package paper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/kdraigo/dev_sdk/exchange/live"
	"github.com/kdraigo/dev_sdk/types"
)

// maxAttempts bounds how often an order is sent again after the connection
// dropped before its answer. Each attempt carries the same client ids, and
// the engine answers a repeat with the orders the first one made.
const maxAttempts = 4

// PlaceOrder sends an order. It is checked here first as an exchange run
// checks it (the order mapping, the market's grid and minimums, the futures
// caps), then by the engine under Binance's rules.
func (c *Client) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	if req == nil {
		return nil, errors.New("paper: order request must not be nil")
	}
	vs, err := c.market(req.Exchange, req.Symbol)
	if err != nil {
		return nil, err
	}
	intent, err := live.MapOrder(req, c.kind)
	if err != nil {
		return nil, err
	}
	if err := c.check(ctx, vs, intent); err != nil {
		return nil, err
	}
	body := c.wire(vs, intent, uuid.NewString())
	if len(req.Reason) > 0 {
		body["reason"] = req.Reason
	}
	if len(req.Logs) > 0 {
		body["logs"] = req.Logs
	}
	res, err := c.send(ctx, "order", body)
	if err != nil {
		return nil, err
	}
	if len(res.Orders) == 0 {
		return nil, errors.New("paper: the engine answered the order with no order")
	}
	w := res.Orders[0]
	c.track(w, req.Type)
	return c.order(w, res.Fills), nil
}

// PlaceBracket places a take-profit and a protective stop as one OCO: when
// one fills the engine cancels the other, with the bot attached or not. On
// futures both legs are reduce-only and the stop triggers on the mark price
// (as the exchange adapter sends it); on spot they are Binance's OCO, a
// LIMIT_MAKER and a STOP_LOSS or STOP_LOSS_LIMIT.
func (c *Client) PlaceBracket(ctx context.Context, req *types.BracketRequest) ([]*types.Order, error) {
	if req == nil {
		return nil, errors.New("paper: bracket request must not be nil")
	}
	if req.TakeProfitPrice <= 0 || req.StopPrice <= 0 {
		return nil, errors.New("paper: a bracket needs a positive TakeProfitPrice and StopPrice")
	}
	vs, err := c.market(req.Exchange, req.Symbol)
	if err != nil {
		return nil, err
	}
	futures := c.kind == live.MarketLinearPerp
	tpReq := &types.OrderRequest{Symbol: req.Symbol, Side: req.Side, Quantity: req.Quantity,
		Type: types.OrderTypeTakeProfitLimit, Price: req.TakeProfitPrice, ReduceOnly: futures}
	slReq := &types.OrderRequest{Symbol: req.Symbol, Side: req.Side, Quantity: req.Quantity,
		Type: types.OrderTypeStopLoss, StopPrice: req.StopPrice, ReduceOnly: futures}
	if req.StopLimitPrice > 0 {
		slReq.Type, slReq.Price = types.OrderTypeStopLossLimit, req.StopLimitPrice
	}
	var legs []map[string]any
	for _, r := range []*types.OrderRequest{tpReq, slReq} {
		intent, err := live.MapOrder(r, c.kind)
		if err != nil {
			return nil, fmt.Errorf("paper bracket: %w", err)
		}
		if err := c.check(ctx, vs, intent); err != nil {
			return nil, fmt.Errorf("paper bracket: %w", err)
		}
		legs = append(legs, c.wire(vs, intent, uuid.NewString()))
	}
	if !futures {
		legs[0]["type"] = "LIMIT_MAKER"
		delete(legs[0], "time_in_force")
	}
	body := map[string]any{"legs": legs}
	if len(req.Reason) > 0 {
		body["reason"] = req.Reason
	}
	if len(req.Logs) > 0 {
		body["logs"] = req.Logs
	}
	res, err := c.send(ctx, "order_bracket", body)
	if err != nil {
		return nil, err
	}
	if len(res.Orders) != 2 {
		return nil, fmt.Errorf("paper: the engine answered the bracket with %d orders", len(res.Orders))
	}
	var out []*types.Order
	for _, w := range res.Orders {
		intent := types.OrderTypeTakeProfitLimit
		if w.StopPrice > 0 {
			intent = slReq.Type
		}
		c.track(w, intent)
		out = append(out, c.order(w, res.Fills))
	}
	// The take-profit first, as the exchange adapters return it.
	if out[0].Type != types.OrderTypeTakeProfitLimit {
		out[0], out[1] = out[1], out[0]
	}
	return out, nil
}

// CancelOrder cancels an open order. One that already ended (it filled, or
// the engine cancelled it as a bracket's other leg) is Binance's -2011, a
// *Rejected wrapping types.ErrOrderClosed.
func (c *Client) CancelOrder(ctx context.Context, exchange, symbol, id string) error {
	n, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return fmt.Errorf("paper: %q is not a paper order id", id)
	}
	for attempt := 1; ; attempt++ {
		_, err := c.call(ctx, "cancel", map[string]any{"order_id": n})
		var rj *Rejected
		switch {
		case err == nil:
			return nil
		case errors.As(err, &rj) && rj.Code == -2011:
			if attempt > 1 {
				// The first attempt may have landed before the drop.
				if o, gerr := c.GetOrder(ctx, exchange, symbol, id); gerr == nil && o.Status == types.OrderStatusCanceled {
					return nil
				}
			}
			return fmt.Errorf("%w: %w", err, types.ErrOrderClosed)
		case !c.retryable(ctx, err, attempt):
			return err
		}
	}
}

// GetOrder reads one order, open or ended.
func (c *Client) GetOrder(ctx context.Context, exchange, symbol, id string) (*types.Order, error) {
	n, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("paper: %q is not a paper order id", id)
	}
	raw, err := c.callRetry(ctx, "get_order", map[string]any{"order_id": n})
	if err != nil {
		return nil, err
	}
	var w orderWire
	if err := json.Unmarshal(raw, &w); err != nil {
		return nil, err
	}
	return c.order(w, nil), nil
}

// ListOpenOrders lists the account's open orders, on one symbol or all.
func (c *Client) ListOpenOrders(ctx context.Context, exchange, symbol string) ([]*types.Order, error) {
	raw, err := c.callRetry(ctx, "open_orders", nil)
	if err != nil {
		return nil, err
	}
	var ws []orderWire
	if err := json.Unmarshal(raw, &ws); err != nil {
		return nil, err
	}
	vs := live.VenueSymbol(symbol)
	out := []*types.Order{}
	for _, w := range ws {
		if symbol == "" || w.Symbol == vs {
			out = append(out, c.order(w, nil))
		}
	}
	return out, nil
}

// SetLeverage sets a futures pair's leverage for new positions. The engine
// refuses a change while a position or an order is open on the pair, except
// raising it with a position open, as Binance does.
func (c *Client) SetLeverage(ctx context.Context, exchange, pair string, leverage float64) error {
	if c.kind != live.MarketLinearPerp {
		return errors.New("paper: leverage applies to paper_binance_futures only")
	}
	if leverage < 1 || leverage != math.Trunc(leverage) {
		return fmt.Errorf("paper: leverage must be a whole number of at least 1, got %v", leverage)
	}
	if o := c.cfg.Live; o != nil && o.MaxLeverage > 0 && leverage > o.MaxLeverage {
		return &live.ErrNotArmed{Gate: "cap", Detail: fmt.Sprintf("requested %vx on %s exceeds MaxLeverage %vx", leverage, pair, o.MaxLeverage)}
	}
	vs, err := c.market(exchange, pair)
	if err != nil {
		return err
	}
	_, err = c.callRetry(ctx, "leverage", map[string]any{"symbol": vs, "leverage": int(leverage)})
	return err
}

// SetOnResume registers what runs when the bot attaches to an account that
// existed before: at start with PaperOptions.AccountID (a cold start), and
// after every reconnect. Candles wait until it returns.
func (c *Client) SetOnResume(fn func(*types.SessionState)) {
	c.mu.Lock()
	c.onResume = fn
	c.mu.Unlock()
}

// market is the venue symbol of an order's market, refused when the account
// does not trade it.
func (c *Client) market(exchange, symbol string) (string, error) {
	if exchange != "" && exchange != c.venue {
		return "", fmt.Errorf("paper: this account trades on %s, not %q", c.venue, exchange)
	}
	vs := live.VenueSymbol(symbol)
	if _, ok := c.symbols[vs]; !ok {
		return "", fmt.Errorf("paper: the run trades %v, not %s (Config.Live.Assets)", c.configured(), symbol)
	}
	return vs, nil
}

func (c *Client) configured() []string {
	var out []string
	for _, s := range c.cfg.Live.Assets {
		out = append(out, s)
	}
	return out
}

// check snaps an intent onto the market's grid and applies the futures caps.
func (c *Client) check(ctx context.Context, vs string, intent *live.OrderIntent) error {
	ref := c.refPrice(ctx, vs)
	if err := c.filters[vs].Apply(intent, ref); err != nil {
		return err
	}
	return c.guard().CheckOrder(intent, ref)
}

// refPrice values a market order: the last close, else the engine's last
// trade or mark.
func (c *Client) refPrice(ctx context.Context, vs string) float64 {
	c.mu.Lock()
	p := c.last[vs]
	c.mu.Unlock()
	if p > 0 {
		return p
	}
	if k, err := c.contract(ctx, vs); err == nil {
		if k.Last > 0 {
			return float64(k.Last)
		}
		return float64(k.Mark)
	}
	return 0
}

// wire is an intent as the engine takes it.
func (c *Client) wire(vs string, intent *live.OrderIntent, clientID string) map[string]any {
	f := c.filters[vs]
	futures := c.kind == live.MarketLinearPerp
	body := map[string]any{"client_order_id": clientID, "symbol": vs, "side": string(intent.Side),
		"quantity": f.FormatQty(intent.Quantity)}
	switch intent.Type {
	case live.IntentMarket:
		body["type"] = "MARKET"
	case live.IntentLimit, live.IntentTakeProfitLimit:
		// The SDK's take-profit is a resting limit; the engine's
		// TAKE_PROFIT_LIMIT would wait for a trigger.
		body["type"] = "LIMIT"
	case live.IntentStopMarket:
		body["type"] = "STOP_LOSS"
		if futures {
			body["type"] = "STOP_MARKET"
		}
	case live.IntentStopLimit:
		body["type"] = "STOP_LOSS_LIMIT"
		if futures {
			body["type"] = "STOP"
		}
	}
	if intent.Price > 0 {
		body["price"] = f.FormatPrice(intent.Price)
	}
	if intent.StopPrice > 0 {
		body["stop_price"] = f.FormatPrice(intent.StopPrice)
		if futures {
			body["working_type"] = "MARK_PRICE"
		}
	}
	if intent.TimeInForce != "" {
		body["time_in_force"] = intent.TimeInForce
	}
	if intent.ReduceOnly {
		body["reduce_only"] = true
	}
	return body
}

// send places an order or an OCO, sending it again after a drop.
func (c *Client) send(ctx context.Context, action string, body map[string]any) (orderResult, error) {
	cids := clientIDs(body)
	c.mu.Lock()
	for _, id := range cids {
		c.placing[id] = true
	}
	c.mu.Unlock()
	var res orderResult
	var err error
	for attempt := 1; ; attempt++ {
		var raw json.RawMessage
		if raw, err = c.call(ctx, action, body); err == nil {
			err = json.Unmarshal(raw, &res)
			break
		}
		if !c.retryable(ctx, err, attempt) {
			break
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var rj *Rejected
	for _, id := range cids {
		if errors.As(err, &rj) {
			// Kept a while: the refused row's push may come after the answer.
			c.refusedFIFO = append(c.refusedFIFO, id)
			if len(c.refusedFIFO) > 256 {
				delete(c.placing, c.refusedFIFO[0])
				c.refusedFIFO = c.refusedFIFO[1:]
			}
			continue
		}
		delete(c.placing, id)
	}
	return res, err
}

func clientIDs(body map[string]any) []string {
	if legs, ok := body["legs"].([]map[string]any); ok {
		var out []string
		for _, l := range legs {
			out = append(out, l["client_order_id"].(string))
		}
		return out
	}
	id, _ := body["client_order_id"].(string)
	return []string{id}
}

// callRetry is a read or an idempotent action, asked again after a drop.
func (c *Client) callRetry(ctx context.Context, action string, data any) (json.RawMessage, error) {
	for attempt := 1; ; attempt++ {
		raw, err := c.call(ctx, action, data)
		if err == nil || !c.retryable(ctx, err, attempt) {
			return raw, err
		}
	}
}

// retryable waits before another attempt and reports whether to make it: a
// dropped connection (once it is back) or a busy engine, up to maxAttempts.
func (c *Client) retryable(ctx context.Context, err error, attempt int) bool {
	if attempt >= maxAttempts || ctx.Err() != nil {
		return false
	}
	var ee *EngineError
	switch {
	case errors.Is(err, ErrNotConnected):
		return c.waitConnected(ctx, 30*time.Second) == nil
	case errors.As(err, &ee) && ee.Code == "engine_busy":
		return sleep(ctx, time.Duration(attempt)*200*time.Millisecond)
	}
	return false
}

// track remembers an order the strategy placed: open, and the type it asked
// for.
func (c *Client) track(w orderWire, asked types.OrderType) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if asked == types.OrderTypeTakeProfitLimit {
		c.intent[w.ID] = asked
	}
	if !isTerminal(sdkStatus(w.Status)) && !c.done[w.ID] {
		c.open[w.ID] = true
	}
}

// order is an engine order as the SDK's, with the fee of the fills that
// came with this change.
func (c *Client) order(w orderWire, fills []fillWire) *types.Order {
	c.mu.Lock()
	intent := c.intent[w.ID]
	c.mu.Unlock()
	fee := 0.0
	for _, f := range fills {
		if f.OrderID == w.ID {
			fee += float64(f.Fee)
		}
	}
	sym := c.symbols[w.Symbol]
	if sym == "" {
		sym = w.Symbol
	}
	o := &types.Order{ID: orderID(w.ID), Symbol: sym, Exchange: c.venue, Side: types.OrderSide(w.Side), Type: sdkType(w, intent),
		Status: sdkStatus(w.Status), Price: float64(w.Price), Quantity: float64(w.Qty), FilledQty: float64(w.FilledQty),
		AveragePrice: float64(w.AvgPrice), Fee: fee, FeeAsset: optString(w.FeeAsset), CreatedAt: w.PlacedAt, UpdatedAt: w.UpdatedAt,
		StopPrice: float64(w.StopPrice), ReduceOnly: w.ReduceOnly, CumulativeFee: float64(w.Fee)}
	if w.GroupID != nil {
		o.GroupID = strconv.FormatInt(*w.GroupID, 10)
	}
	return o
}

// sessionState is the account as the strategy's resume callback sees it.
func (c *Client) sessionState(v view, cold bool) *types.SessionState {
	st := &types.SessionState{SessionID: c.accountID, Resumed: true, ColdStart: cold, Playhead: time.Now().UTC()}
	st.Wallets = []types.Account{c.account(v.Balances, "")}
	for _, p := range v.Positions {
		st.Positions = append(st.Positions, *c.position(p))
	}
	for _, w := range v.Orders {
		o := c.order(w, nil)
		st.OpenOrders = append(st.OpenOrders, types.ResumedOrder{ID: o.ID, Pair: o.Symbol, Side: string(o.Side), Type: string(o.Type),
			Status: string(o.Status), Price: o.Price, Quantity: o.Quantity})
	}
	return st
}
