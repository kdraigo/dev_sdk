package live

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kdraigo/dev_sdk/aggregator"
	"github.com/kdraigo/dev_sdk/types"
)

// BybitFuturesClient trades Bybit USDT perpetuals (V5 category "linear").
//
// Scope matches the backtest engine and the Binance futures adapter exactly:
// isolated margin, one-way positions, linear USDT-margined contracts. Anything
// else is refused at PrepareSession rather than half-supported. Unlike
// Binance, a Bybit Unified Trading Account sets its margin mode for the whole
// account, so the adapter never switches it: it refuses, and says how to
// switch it (ideally on a sub-account kept for the purpose).
//
// It talks to Bybit through bybitV5, never through github.com/hirokisan/bybit,
// whose order parameters lose reduceOnly on the wire (see bybit_v5.go).
// TestBybitFuturesAdapterUsesItsOwnClient enforces that, and that nothing here
// can reach the spot category.
type BybitFuturesClient struct {
	config *types.Config
	api    *bybitV5
	guard  Guard

	// restBase and wsBase override the venue's addresses (tests only).
	restBase string
	wsBase   string

	filters map[string]InstrumentFilter

	mu sync.RWMutex
	// marks is the latest mark price per symbol: from the tickers stream, or
	// read once over REST when a market order needs valuing before the stream
	// has spoken.
	marks map[string]float64
	// brackets links a bracket's two legs: Bybit does not cancel one when the
	// other fills, so the adapter does (see handleOrderUpdate).
	brackets map[string]bracketLeg
	// declared remembers what the strategy called an order, so a
	// TAKE_PROFIT_LIMIT that goes out as a plain Limit is reported back under
	// the same name.
	declared map[string]types.OrderType
	// feeSeen is the cumulative fee last reported per order, so each update
	// carries the new fee in Fee and the running total in CumulativeFee.
	feeSeen map[string]float64

	orderOut chan<- *types.Order

	stopOnce sync.Once
	stopped  chan struct{}
}

var (
	_ interface {
		SetLeverage(ctx context.Context, exchange, pair string, leverage float64) error
	} = (*BybitFuturesClient)(nil)
	_ interface {
		GetPositions(ctx context.Context, exchange string) ([]*types.Position, error)
	} = (*BybitFuturesClient)(nil)
	_ interface {
		PlaceBracket(ctx context.Context, req *types.BracketRequest) ([]*types.Order, error)
	} = (*BybitFuturesClient)(nil)
	_ interface {
		ListOpenOrders(ctx context.Context, exchange, symbol string) ([]*types.Order, error)
		GetOrder(ctx context.Context, exchange, symbol, id string) (*types.Order, error)
	} = (*BybitFuturesClient)(nil)
)

func NewBybitFuturesClient(cfg *types.Config) *BybitFuturesClient {
	return &BybitFuturesClient{
		config:   cfg,
		guard:    Guard{Env: cfg.Environment, Opts: cfg.Live},
		filters:  make(map[string]InstrumentFilter),
		marks:    make(map[string]float64),
		brackets: make(map[string]bracketLeg),
		declared: make(map[string]types.OrderType),
		feeSeen:  make(map[string]float64),
		stopped:  make(chan struct{}),
	}
}

func (b *BybitFuturesClient) assets() []string {
	if b.config == nil || b.config.Live == nil {
		return nil
	}
	return b.config.Live.Assets
}

func (b *BybitFuturesClient) testnet() bool {
	return b.config != nil && b.config.Environment == types.EnvTestBybitFutures
}

func (b *BybitFuturesClient) restURL() string {
	if b.restBase != "" {
		return b.restBase
	}
	if b.testnet() {
		return bybitTestnetREST
	}
	return bybitMainnetREST
}

func (b *BybitFuturesClient) wsURL() string {
	if b.wsBase != "" {
		return b.wsBase
	}
	if b.testnet() {
		return bybitTestnetWS
	}
	return bybitMainnetWS
}

// ensureAPI builds the REST client for calls that may come before
// PrepareSession (history reads).
func (b *BybitFuturesClient) ensureAPI() {
	if b.api == nil {
		b.api = newBybitV5(b.restURL(), b.config.Credentials.APIKey, b.config.Credentials.APISecret)
	}
}

// ── session ──────────────────────────────────────────────────────────────────

func (b *BybitFuturesClient) PrepareSession(ctx context.Context, cfg *types.Config) error {
	b.config = cfg
	b.guard = Guard{Env: cfg.Environment, Opts: cfg.Live}

	if !IsBybitFutures(cfg.Environment) {
		return fmt.Errorf("bybit futures adapter cannot run environment %q; use %s or %s",
			cfg.Environment, types.EnvTestBybitFutures, types.EnvRealBybitFutures)
	}
	if err := b.guard.CheckSession(); err != nil {
		return err
	}
	if strings.Contains(cfg.Credentials.APISecret, "PRIVATE KEY") {
		return fmt.Errorf("bybit futures: the API secret is an RSA private key; this adapter signs with Bybit's system-generated (HMAC) keys only — create one in Bybit's API management")
	}

	if b.testnet() {
		log.Println("Bybit futures: TESTNET")
	} else {
		log.Printf("Bybit futures: MAINNET, armed=%v dry_run=%v max_notional=%v max_leverage=%v",
			cfg.Live.Armed, cfg.Live.DryRun, cfg.Live.MaxOrderNotional, cfg.Live.MaxLeverage)
	}

	b.api = newBybitV5(b.restURL(), cfg.Credentials.APIKey, cfg.Credentials.APISecret)
	if err := b.api.syncTime(ctx); err != nil {
		// Not fatal: a clock that is already right still works, and a wrong one
		// fails loudly with retCode 10002 on the first private call below.
		log.Printf("Bybit futures: server time sync failed: %v", err)
	}

	if err := b.loadFilters(ctx); err != nil {
		return fmt.Errorf("bybit futures: %w", err)
	}
	if err := b.assertAccountShape(ctx); err != nil {
		return err
	}
	if err := b.applyConfiguredLeverage(ctx); err != nil {
		return err
	}
	for _, a := range b.assets() {
		if _, err := b.fetchMark(ctx, VenueSymbol(a)); err != nil {
			log.Printf("Bybit futures: reading the mark price of %s: %v", a, err)
		}
	}
	return nil
}

// loadFilters reads each contract's own trading rules. Fatal, as on Binance: a
// precision rejection after a signal leaves the strategy believing it holds
// exposure it does not.
func (b *BybitFuturesClient) loadFilters(ctx context.Context) error {
	for _, a := range b.assets() {
		sym := VenueSymbol(a)
		var r bybitList[bybitInstrument]
		q := url.Values{"category": {"linear"}, "symbol": {sym}}
		if err := b.api.get(ctx, "/v5/market/instruments-info", q, false, &r); err != nil {
			return fmt.Errorf("instruments-info %s: %w", sym, err)
		}
		if len(r.List) == 0 {
			return fmt.Errorf("no perpetual contract on Bybit for %s", sym)
		}
		in := r.List[0]
		if in.ContractType != "LinearPerpetual" || !strings.EqualFold(in.SettleCoin, "USDT") {
			return fmt.Errorf("%s on Bybit is a %s settled in %s; this adapter trades USDT perpetuals only", sym, in.ContractType, in.SettleCoin)
		}
		if in.Status != "Trading" {
			return fmt.Errorf("%s is not trading on Bybit (status %s)", sym, in.Status)
		}
		f := InstrumentFilter{
			TickSize:    parseFloat(in.PriceFilter.TickSize),
			StepSize:    parseFloat(in.LotSizeFilter.QtyStep),
			MinQty:      parseFloat(in.LotSizeFilter.MinOrderQty),
			MinNotional: parseFloat(in.LotSizeFilter.MinNotionalValue),
		}
		b.filters[sym] = f
		log.Printf("Bybit futures: %s tick=%v step=%v minQty=%v minNotional=%v maxLeverage=%s",
			sym, f.TickSize, f.StepSize, f.MinQty, f.MinNotional, in.LeverageFilter.MaxLeverage)
	}
	return nil
}

// Bybit's account kinds (GET /v5/account/info unifiedMarginStatus): 1 is a
// classic account, 3–6 are Unified Trading Accounts (1.0 and 2.0, pro or not).
const bybitClassicAccount = 1

// assertAccountShape refuses an account set up differently from what the
// backtest engine models: cross or portfolio margin, or hedge mode. Under cross
// margin a losing position draws on the whole wallet, so the isolated
// liquidation price the simulator computed — and every size derived from it —
// is wrong; under hedge mode a reduce-only sell does not reduce a long.
func (b *BybitFuturesClient) assertAccountShape(ctx context.Context) error {
	var info struct {
		MarginMode          string `json:"marginMode"`
		UnifiedMarginStatus int    `json:"unifiedMarginStatus"`
	}
	if err := b.api.get(ctx, "/v5/account/info", nil, true, &info); err != nil {
		return fmt.Errorf("bybit futures: reading the account: %w", explainBybitKeyError(err))
	}
	if info.UnifiedMarginStatus == bybitClassicAccount {
		return fmt.Errorf("bybit futures: this is a classic Bybit account; the adapter supports Unified Trading Accounts only (upgrade the account in Bybit)")
	}
	if info.MarginMode != "ISOLATED_MARGIN" {
		return fmt.Errorf("bybit futures: the account's margin mode is %s; this SDK models isolated margin only. "+
			"On a Unified Trading Account the margin mode covers the whole account, so the adapter does not switch it: "+
			"set Isolated Margin in Bybit (Derivatives, margin mode), on a sub-account kept for this", info.MarginMode)
	}

	for _, a := range b.assets() {
		sym := VenueSymbol(a)
		var r bybitList[bybitPositionRecord]
		q := url.Values{"category": {"linear"}, "symbol": {sym}}
		if err := b.api.get(ctx, "/v5/position/list", q, true, &r); err != nil {
			return fmt.Errorf("bybit futures: reading the %s position: %w", sym, explainBybitKeyError(err))
		}
		for _, p := range r.List {
			if p.PositionIdx != 0 {
				return fmt.Errorf("bybit futures: %s is in hedge mode (positionIdx %d); this SDK models one-way positions only — switch the contract to one-way mode in Bybit", sym, p.PositionIdx)
			}
		}
	}
	return nil
}

// applyConfiguredLeverage sets the starting leverage per pair, leaving pairs
// not named in LiveOptions.Leverage as the account has them.
func (b *BybitFuturesClient) applyConfiguredLeverage(ctx context.Context) error {
	if b.config.Live == nil {
		return nil
	}
	for pair, lev := range b.config.Live.Leverage {
		sym := VenueSymbol(pair)
		if b.guard.DryRun() {
			log.Printf("Bybit futures DRY RUN: would set %s leverage to %vx", sym, lev)
			continue
		}
		if err := b.setLeverage(ctx, sym, lev); err != nil {
			return fmt.Errorf("bybit futures: setting %vx leverage on %s: %w", lev, sym, err)
		}
		log.Printf("Bybit futures: %s leverage set to %vx", sym, lev)
	}
	return nil
}

func (b *BybitFuturesClient) setLeverage(ctx context.Context, sym string, lev float64) error {
	l := strconv.FormatFloat(lev, 'f', -1, 64)
	err := b.api.post(ctx, "/v5/position/set-leverage", map[string]any{
		"category": "linear", "symbol": sym, "buyLeverage": l, "sellLeverage": l,
	}, nil)
	if bybitCode(err) == bybitCodeLeverageUnchanged {
		return nil // already at that leverage
	}
	return explainBybitKeyError(err)
}

// ── orders ───────────────────────────────────────────────────────────────────

func (b *BybitFuturesClient) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	intent, err := MapOrder(req, MarketLinearPerp)
	if err != nil {
		return nil, fmt.Errorf("bybit futures: %w", err)
	}
	return b.place(ctx, req, intent, "")
}

// place is the single path every order takes, bracket legs included, so the
// rounding and the gates cannot be bypassed by adding another entry point.
func (b *BybitFuturesClient) place(ctx context.Context, req *types.OrderRequest, intent *OrderIntent, groupID string) (*types.Order, error) {
	sym := intent.Symbol
	filter, known := b.filters[sym]
	if !known {
		// Without the contract's rules the order can be neither rounded nor
		// checked against its minimums. Refuse rather than let the venue guess.
		return nil, fmt.Errorf("bybit futures: %s was not prepared — add it to LiveOptions.Assets", sym)
	}
	mark := b.markFor(sym)
	if mark <= 0 && b.api != nil {
		if m, err := b.fetchMark(ctx, sym); err == nil {
			mark = m
		}
	}
	if err := filter.Apply(intent, mark); err != nil {
		return nil, fmt.Errorf("bybit futures %s: %w", sym, err)
	}
	if err := b.guard.CheckOrder(intent, mark); err != nil {
		return nil, err
	}

	body, linkID, err := bybitCreateBody(intent, filter, time.Now())
	if err != nil {
		return nil, err
	}

	if b.guard.DryRun() {
		return b.dryRunAck(req, intent, groupID), nil
	}

	var res struct {
		OrderID     string `json:"orderId"`
		OrderLinkID string `json:"orderLinkId"`
	}
	err = b.api.post(ctx, "/v5/order/create", body, &res)
	if errors.Is(err, errBybitUnsure) {
		// The order may or may not exist. Ask by the link id we gave it rather
		// than guess: reporting "failed" for a live order invites a second one,
		// reporting "placed" for a missing stop leaves a position unprotected.
		rec, found, lerr := b.findByLinkID(ctx, sym, linkID)
		switch {
		case lerr != nil:
			return nil, fmt.Errorf("bybit futures %s %s %s: the placement's outcome is unknown and the check failed too — the order MAY exist (orderLinkId %s): %v; check: %w",
				sym, intent.Side, intent.Type, linkID, err, lerr)
		case !found:
			return nil, fmt.Errorf("bybit futures %s %s %s: not placed (checked by orderLinkId %s): %w", sym, intent.Side, intent.Type, linkID, err)
		default:
			log.Printf("Bybit futures: placement of %s looked lost (%v) but the order exists: %s", linkID, err, rec.OrderID)
			res.OrderID = rec.OrderID
			err = nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("bybit futures %s %s %s: %w", sym, intent.Side, intent.Type, explainBybitKeyError(err))
	}

	b.rememberDeclared(res.OrderID, req.Type)
	order := b.readBack(ctx, sym, res.OrderID, req, intent, groupID)
	if intent.Type.Triggered() {
		// A resting stop may stay silent until it fires; announce it as the
		// backtest engine does. The reconciler drops the duplicate if the
		// venue announces it too.
		b.emitSynthetic(order)
	}
	return order, nil
}

// bybitCreateBody is the /v5/order/create body for an intent that has already
// been rounded and checked. Every key is V5's own spelling.
func bybitCreateBody(intent *OrderIntent, filter InstrumentFilter, now time.Time) (map[string]any, string, error) {
	orderType, err := bybitFuturesOrderType(intent.Type)
	if err != nil {
		return nil, "", err
	}
	side := "Buy"
	if intent.Side == types.OrderSideSell {
		side = "Sell"
	}
	linkID := newBybitLinkID(now)
	body := map[string]any{
		"category":    "linear",
		"symbol":      intent.Symbol,
		"side":        side,
		"orderType":   orderType,
		"qty":         filter.FormatQty(intent.Quantity),
		"positionIdx": 0, // one-way; a hedge-mode account fails loudly instead of opening a leg
		"orderLinkId": linkID,
	}
	if orderType == "Limit" {
		body["price"] = filter.FormatPrice(intent.Price)
		body["timeInForce"] = "GTC"
	}
	if intent.Type.Triggered() {
		dir, err := bybitTriggerDirection(intent.Side)
		if err != nil {
			return nil, "", err
		}
		body["triggerPrice"] = filter.FormatPrice(intent.StopPrice)
		body["triggerDirection"] = dir
		// The mark, as the backtest engine triggers and liquidates on it.
		body["triggerBy"] = "MarkPrice"
	}
	if intent.ReduceOnly {
		body["reduceOnly"] = true
		if intent.Type.Triggered() {
			// A protective stop must reduce the position whatever the margin:
			// Bybit cancels a conditional order that finds margin short when it
			// triggers, unless closeOnTrigger is set.
			body["closeOnTrigger"] = true
		}
	}
	return body, linkID, nil
}

// readBack reads the order's state after a placement. V5's create answers
// with ids only, while the backtest engine returns a market order FILLED with
// its price; a strategy branching on the status must see the same thing live.
// A failed read never turns a placed order into an error: the order exists.
func (b *BybitFuturesClient) readBack(ctx context.Context, sym, orderID string, req *types.OrderRequest, intent *OrderIntent, groupID string) *types.Order {
	attempts := 1
	if intent.Type == IntentMarket {
		attempts = 6
	}
	var last *types.Order
	for i := 0; i < attempts; i++ {
		if i > 0 && !sleepCtx(ctx, 150*time.Millisecond) {
			break
		}
		rec, found, err := b.lookup(ctx, sym, url.Values{"orderId": {orderID}})
		if err != nil || !found {
			continue
		}
		last = b.mapRecord(rec, req.Symbol)
		if last.Status != types.OrderStatusNew {
			break
		}
	}
	if last == nil {
		now := time.Now()
		last = &types.Order{
			ID: orderID, Symbol: req.Symbol, Exchange: types.ExchangeBybitFutures, Side: req.Side, Type: req.Type,
			Status: types.OrderStatusNew, Price: intent.Price, Quantity: intent.Quantity, StopPrice: intent.StopPrice,
			ReduceOnly: intent.ReduceOnly, CreatedAt: now, UpdatedAt: now,
		}
	}
	last.Type = req.Type
	if last.StopPrice == 0 {
		last.StopPrice = intent.StopPrice
	}
	last.GroupID = groupID
	return last
}

// lookup finds one order on the open book, then in the history.
func (b *BybitFuturesClient) lookup(ctx context.Context, sym string, by url.Values) (bybitOrderRecord, bool, error) {
	q := url.Values{"category": {"linear"}, "symbol": {sym}}
	for k, v := range by {
		q[k] = v
	}
	var r bybitList[bybitOrderRecord]
	if err := b.api.get(ctx, "/v5/order/realtime", q, true, &r); err != nil {
		return bybitOrderRecord{}, false, err
	}
	if len(r.List) > 0 {
		return r.List[0], true, nil
	}
	r = bybitList[bybitOrderRecord]{}
	if err := b.api.get(ctx, "/v5/order/history", q, true, &r); err != nil {
		return bybitOrderRecord{}, false, err
	}
	if len(r.List) > 0 {
		return r.List[0], true, nil
	}
	return bybitOrderRecord{}, false, nil
}

func (b *BybitFuturesClient) findByLinkID(ctx context.Context, sym, linkID string) (bybitOrderRecord, bool, error) {
	return b.lookup(ctx, sym, url.Values{"orderLinkId": {linkID}})
}

func (b *BybitFuturesClient) dryRunAck(req *types.OrderRequest, intent *OrderIntent, groupID string) *types.Order {
	log.Printf("Bybit futures DRY RUN: %s %s %v %s type=%s price=%v stop=%v reduceOnly=%v",
		intent.Side, intent.Symbol, intent.Quantity, intent.TimeInForce,
		intent.Type, intent.Price, intent.StopPrice, intent.ReduceOnly)
	now := time.Now()
	return &types.Order{
		ID: fmt.Sprintf("dryrun-%d", now.UnixNano()), Symbol: req.Symbol, Exchange: types.ExchangeBybitFutures,
		Side: req.Side, Type: req.Type, Status: types.OrderStatusNew, Price: intent.Price, Quantity: intent.Quantity,
		StopPrice: intent.StopPrice, GroupID: groupID, ReduceOnly: intent.ReduceOnly, CreatedAt: now, UpdatedAt: now,
	}
}

func (b *BybitFuturesClient) CancelOrder(ctx context.Context, exchange, symbol, id string) error {
	if b.guard.DryRun() {
		log.Printf("Bybit futures DRY RUN: cancel %s on %s", id, symbol)
		return nil
	}
	err := b.api.post(ctx, "/v5/order/cancel", map[string]any{
		"category": "linear", "symbol": VenueSymbol(symbol), "orderId": id,
	}, nil)
	switch bybitCode(err) {
	case bybitCodeOrderNotExist, bybitCodeOrderCompleted, bybitCodeOrderCancelled:
		var apiErr *bybitAPIError
		errors.As(err, &apiErr)
		err = fmt.Errorf("%w (venue said: %s)", types.ErrOrderClosed, apiErr.Msg)
	}
	if err == nil || errors.Is(err, types.ErrOrderClosed) {
		b.forgetBracket(id)
	}
	return err
}

// PlaceBracket places a take-profit and a protective stop as a pair. Bybit does
// not link them, so the adapter cancels the survivor when the first leg fills
// (handleOrderUpdate). Both legs are reduce-only and reserve no margin.
func (b *BybitFuturesClient) PlaceBracket(ctx context.Context, req *types.BracketRequest) ([]*types.Order, error) {
	if req == nil {
		return nil, fmt.Errorf("bracket request must not be nil")
	}
	if req.TakeProfitPrice <= 0 || req.StopPrice <= 0 {
		return nil, fmt.Errorf("bracket requires a positive TakeProfitPrice and StopPrice")
	}
	groupID := fmt.Sprintf("bracket-%d", time.Now().UnixNano())

	tpReq := &types.OrderRequest{
		Symbol: req.Symbol, Exchange: req.Exchange, Side: req.Side, Quantity: req.Quantity,
		Type: types.OrderTypeTakeProfitLimit, Price: req.TakeProfitPrice, ReduceOnly: true,
	}
	slType := types.OrderTypeStopLossLimit
	if req.StopLimitPrice <= 0 {
		// A stop that must fill beats one that might.
		slType = types.OrderTypeStopLoss
	}
	slReq := &types.OrderRequest{
		Symbol: req.Symbol, Exchange: req.Exchange, Side: req.Side, Quantity: req.Quantity,
		Type: slType, Price: req.StopLimitPrice, StopPrice: req.StopPrice, ReduceOnly: true,
	}
	tpIntent, err := MapOrder(tpReq, MarketLinearPerp)
	if err != nil {
		return nil, fmt.Errorf("bybit futures bracket take-profit: %w", err)
	}
	slIntent, err := MapOrder(slReq, MarketLinearPerp)
	if err != nil {
		return nil, fmt.Errorf("bybit futures bracket stop: %w", err)
	}

	tp, err := b.place(ctx, tpReq, tpIntent, groupID)
	if err != nil {
		return nil, err
	}
	sl, err := b.place(ctx, slReq, slIntent, groupID)
	if err != nil {
		// A take-profit with no stop is an exit without protection: take it
		// back rather than leave it.
		if cerr := b.CancelOrder(ctx, req.Exchange, req.Symbol, tp.ID); cerr != nil && !errors.Is(cerr, types.ErrOrderClosed) {
			log.Printf("Bybit futures: stop leg failed AND its take-profit %s could not be cancelled: %v", tp.ID, cerr)
		}
		return nil, fmt.Errorf("bybit futures bracket stop leg: %w", err)
	}

	b.mu.Lock()
	b.brackets[tp.ID] = bracketLeg{sibling: sl.ID, groupID: groupID}
	b.brackets[sl.ID] = bracketLeg{sibling: tp.ID, groupID: groupID}
	b.mu.Unlock()
	return []*types.Order{tp, sl}, nil
}

// ── account and positions ────────────────────────────────────────────────────

func (b *BybitFuturesClient) GetAccount(ctx context.Context, exchange string, asset string) (*types.Account, error) {
	coin := asset
	if coin == "" {
		coin = "USDT"
	}
	var r bybitList[struct {
		Coin []struct {
			Coin            string `json:"coin"`
			WalletBalance   string `json:"walletBalance"`
			TotalPositionIM string `json:"totalPositionIM"`
			TotalOrderIM    string `json:"totalOrderIM"`
			Locked          string `json:"locked"`
			Bonus           string `json:"bonus"`
		} `json:"coin"`
	}]
	q := url.Values{"accountType": {"UNIFIED"}, "coin": {coin}}
	if err := b.api.get(ctx, "/v5/account/wallet-balance", q, true, &r); err != nil {
		return nil, err
	}
	acc := &types.Account{Exchange: types.ExchangeBybitFutures}
	for _, l := range r.List {
		for _, c := range l.Coin {
			wallet := parseFloat(c.WalletBalance)
			// Isolated margin: what is posted against positions and resting
			// orders, held or given as bonus is not free (V5 wallet-balance
			// docs); it is reported as locked, as on Binance.
			free := wallet - parseFloat(c.TotalPositionIM) - parseFloat(c.TotalOrderIM) - parseFloat(c.Locked) - parseFloat(c.Bonus)
			if free < 0 {
				free = 0
			}
			if wallet == 0 && free == 0 {
				continue
			}
			acc.Balances = append(acc.Balances, types.Balance{Asset: c.Coin, Free: free, Lock: wallet - free})
		}
	}
	return acc, nil
}

// GetPositions reads open positions from the venue, never from local memory
// (a liquidation, an ADL or a fill missed while a socket was down all make
// local state wrong).
func (b *BybitFuturesClient) GetPositions(ctx context.Context, exchange string) ([]*types.Position, error) {
	if exchange != "" && exchange != types.ExchangeBybitFutures {
		return nil, nil
	}
	var out []*types.Position
	cursor := ""
	for page := 0; page < 50; page++ {
		q := url.Values{"category": {"linear"}, "settleCoin": {"USDT"}, "limit": {"200"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var r bybitList[bybitPositionRecord]
		if err := b.api.get(ctx, "/v5/position/list", q, true, &r); err != nil {
			return nil, err
		}
		for _, p := range r.List {
			if pos := mapBybitPosition(p); pos != nil {
				out = append(out, pos)
			}
		}
		if r.NextPageCursor == "" || len(r.List) == 0 {
			break
		}
		cursor = r.NextPageCursor
	}
	return out, nil
}

func mapBybitPosition(p bybitPositionRecord) *types.Position {
	side := bybitPositionSide(p.Side)
	size := parseFloat(p.Size)
	if side == "" || size == 0 {
		return nil // flat is no position, not a zero-size one
	}
	entry := parseFloat(p.AvgPrice)
	if entry == 0 {
		entry = parseFloat(p.EntryPrice)
	}
	return &types.Position{
		Symbol:         p.Symbol,
		Exchange:       types.ExchangeBybitFutures,
		Side:           side,
		Size:           size,
		EntryPrice:     entry,
		Leverage:       parseFloat(p.Leverage),
		IsolatedMargin: parseFloat(p.PositionIM),
		MarkPrice:      parseFloat(p.MarkPrice),
		UnrealizedPnL:  parseFloat(p.UnrealisedPnl),
		// RealizedPnL and FundingPaid are left at zero, as on Binance: the
		// venue's cumRealisedPnl spans the symbol's whole history, not this
		// position's lifetime.
	}
}

// SetLeverage changes the leverage for new positions on a pair. Refused while a
// position is open, matching the backtest engine.
func (b *BybitFuturesClient) SetLeverage(ctx context.Context, exchange, pair string, leverage float64) error {
	if leverage <= 0 {
		return fmt.Errorf("leverage must be positive, got %v", leverage)
	}
	if b.config.Live != nil && b.config.Live.MaxLeverage > 0 && leverage > b.config.Live.MaxLeverage {
		return &ErrNotArmed{Gate: "cap", Detail: fmt.Sprintf("requested %vx on %s exceeds MaxLeverage %vx", leverage, pair, b.config.Live.MaxLeverage)}
	}
	sym := VenueSymbol(pair)
	positions, err := b.GetPositions(ctx, types.ExchangeBybitFutures)
	if err != nil {
		return fmt.Errorf("bybit futures: checking for an open position before re-levering: %w", err)
	}
	for _, p := range positions {
		if p.Symbol == sym {
			return fmt.Errorf("bybit futures: refusing to change leverage on %s while a %s position of %v is open", sym, p.Side, p.Size)
		}
	}
	if b.guard.DryRun() {
		log.Printf("Bybit futures DRY RUN: set %s leverage to %vx", sym, leverage)
		return nil
	}
	return b.setLeverage(ctx, sym, leverage)
}

func (b *BybitFuturesClient) Next(ctx context.Context) error { return nil }

// ── order reads (the SDK's reconciler) ───────────────────────────────────────

// OrderFeed: Bybit pushes order updates on the private stream; the slow poll
// is the safety net for updates lost while a socket was down.
func (b *BybitFuturesClient) OrderFeed() types.OrderFeed {
	return types.OrderFeed{Push: true, PollEvery: 2 * time.Minute, Latency: time.Second}
}

// ListOpenOrders reads the open book, resting conditional orders
// (Untriggered) included: V5's realtime list returns every kind by default.
func (b *BybitFuturesClient) ListOpenOrders(ctx context.Context, exchange, symbol string) ([]*types.Order, error) {
	sym := VenueSymbol(symbol)
	var out []*types.Order
	cursor := ""
	for page := 0; page < 50; page++ {
		q := url.Values{"category": {"linear"}, "symbol": {sym}, "openOnly": {"0"}, "limit": {"50"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var r bybitList[bybitOrderRecord]
		if err := b.api.get(ctx, "/v5/order/realtime", q, true, &r); err != nil {
			return nil, err
		}
		for _, rec := range r.List {
			out = append(out, b.mapRecord(rec, symbol))
		}
		if r.NextPageCursor == "" || len(r.List) == 0 {
			break
		}
		cursor = r.NextPageCursor
	}
	return out, nil
}

// GetOrder resolves what became of one order: an order gone from the open list
// is filled or cancelled, and only the order itself can say which.
func (b *BybitFuturesClient) GetOrder(ctx context.Context, exchange, symbol, id string) (*types.Order, error) {
	rec, found, err := b.lookup(ctx, VenueSymbol(symbol), url.Values{"orderId": {id}})
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("bybit futures: order %s on %s not found", id, symbol)
	}
	return b.mapRecord(rec, symbol), nil
}

// mapRecord converts a V5 order record (REST or websocket) into the SDK shape.
func (b *BybitFuturesClient) mapRecord(rec bybitOrderRecord, symbol string) *types.Order {
	side := types.OrderSideBuy
	if rec.Side == "Sell" {
		side = types.OrderSideSell
	}
	trigger := parseFloat(rec.TriggerPrice)
	t := b.declaredOrderType(rec.OrderID)
	if t == "" {
		t = sdkTypeFromBybit(rec.OrderType, trigger, rec.StopOrderType)
	}
	if symbol == "" {
		symbol = rec.Symbol
	}
	return &types.Order{
		ID:            rec.OrderID,
		Symbol:        symbol,
		Exchange:      types.ExchangeBybitFutures,
		Side:          side,
		Type:          t,
		Status:        mapBybitFuturesStatus(rec.OrderStatus),
		Price:         parseFloat(rec.Price),
		Quantity:      parseFloat(rec.Qty),
		FilledQty:     parseFloat(rec.CumExecQty),
		AveragePrice:  parseFloat(rec.AvgPrice),
		FeeAsset:      "USDT",
		CumulativeFee: bybitOrderFee(rec),
		RealizedPnL:   parseFloat(rec.ClosedPnl),
		StopPrice:     trigger,
		ReduceOnly:    rec.ReduceOnly || rec.CloseOnTrigger,
		CreatedAt:     parseMillis(rec.CreatedTime),
		UpdatedAt:     parseMillis(rec.UpdatedTime),
	}
}

// bybitOrderFee is an order's fees so far. V5 deprecates cumExecFee for linear
// in favour of cumFeeDetail (fees per coin); the detail wins when present.
func bybitOrderFee(rec bybitOrderRecord) float64 {
	if detail, ok := rec.CumFeeDetail.(map[string]any); ok && len(detail) > 0 {
		total := 0.0
		for _, v := range detail {
			switch x := v.(type) {
			case string:
				total += parseFloat(x)
			case float64:
				total += x
			}
		}
		return total
	}
	return parseFloat(rec.CumExecFee)
}

// ── history ──────────────────────────────────────────────────────────────────

// GetHistoricalCandles reads closed candles in [from, to], oldest first. V5's
// kline answers newest first, so pages are walked backwards from `to`.
func (b *BybitFuturesClient) GetHistoricalCandles(ctx context.Context, exchange, symbol string, from, to time.Time, tf types.Timeframe) ([]*types.Candle, error) {
	b.ensureAPI()
	if from.After(to) {
		return nil, fmt.Errorf("bybit futures: from %s is after to %s", from, to)
	}
	interval, err := bybitInterval(tf)
	if err != nil {
		return nil, err
	}
	sym := VenueSymbol(symbol)
	dur := aggregator.ExtractDuration(tf)
	fromMs, endMs := from.UnixMilli(), to.UnixMilli()
	now := time.Now()

	byStart := map[int64]*types.Candle{}
	const pageLimit = 1000
	for endMs >= fromMs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		var r struct {
			List [][]string `json:"list"`
		}
		q := url.Values{
			"category": {"linear"}, "symbol": {sym}, "interval": {string(interval)},
			"start": {strconv.FormatInt(fromMs, 10)}, "end": {strconv.FormatInt(endMs, 10)},
			"limit": {strconv.Itoa(pageLimit)},
		}
		if err := b.api.get(ctx, "/v5/market/kline", q, false, &r); err != nil {
			return nil, fmt.Errorf("bybit futures kline fetch: %w", err)
		}
		if len(r.List) == 0 {
			break
		}
		oldest := int64(-1)
		for _, row := range r.List {
			if len(row) < 7 {
				continue
			}
			start, err := strconv.ParseInt(row[0], 10, 64)
			if err != nil {
				continue
			}
			if oldest < 0 || start < oldest {
				oldest = start
			}
			open := time.UnixMilli(start)
			// Only closed candles: the newest row of a page reaching now is
			// still forming.
			if start < fromMs || start > to.UnixMilli() || open.Add(dur).After(now) {
				continue
			}
			byStart[start] = &types.Candle{
				Symbol: symbol, Exchange: types.ExchangeBybitFutures, Timeframe: tf,
				OpenTime: open, CloseTime: open.Add(dur),
				Open: parseFloat(row[1]), High: parseFloat(row[2]), Low: parseFloat(row[3]), Close: parseFloat(row[4]),
				Volume: parseFloat(row[5]), QuoteVolume: parseFloat(row[6]), IsComplete: true,
			}
		}
		if len(r.List) < pageLimit || oldest <= fromMs {
			break
		}
		endMs = oldest - 1
	}

	out := make([]*types.Candle, 0, len(byStart))
	for _, c := range byStart {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OpenTime.Before(out[j].OpenTime) })
	return out, nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

// fetchMark reads one contract's mark price over REST and keeps it.
func (b *BybitFuturesClient) fetchMark(ctx context.Context, sym string) (float64, error) {
	var r bybitList[struct {
		MarkPrice string `json:"markPrice"`
	}]
	if err := b.api.get(ctx, "/v5/market/tickers", url.Values{"category": {"linear"}, "symbol": {sym}}, false, &r); err != nil {
		return 0, err
	}
	if len(r.List) == 0 {
		return 0, fmt.Errorf("no ticker for %s", sym)
	}
	p := parseFloat(r.List[0].MarkPrice)
	if p > 0 {
		b.setMark(sym, p)
	}
	return p, nil
}

func (b *BybitFuturesClient) markFor(sym string) float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.marks[sym]
}

func (b *BybitFuturesClient) setMark(sym string, p float64) {
	b.mu.Lock()
	b.marks[sym] = p
	b.mu.Unlock()
}

func (b *BybitFuturesClient) rememberDeclared(id string, t types.OrderType) {
	if t == "" || id == "" {
		return
	}
	b.mu.Lock()
	b.declared[id] = t
	b.mu.Unlock()
}

func (b *BybitFuturesClient) declaredOrderType(id string) types.OrderType {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.declared[id]
}

func (b *BybitFuturesClient) forgetBracket(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if leg, ok := b.brackets[id]; ok {
		delete(b.brackets, leg.sibling)
		delete(b.brackets, id)
	}
}

// forgetOrder drops a finished order's bookkeeping, which would otherwise grow
// for the life of the process.
func (b *BybitFuturesClient) forgetOrder(id string) {
	b.forgetBracket(id)
	b.mu.Lock()
	delete(b.declared, id)
	delete(b.feeSeen, id)
	b.mu.Unlock()
}

// emitSynthetic announces an order the venue may not announce in time, without
// ever blocking a placement on a slow consumer.
func (b *BybitFuturesClient) emitSynthetic(order *types.Order) {
	b.mu.RLock()
	out := b.orderOut
	b.mu.RUnlock()
	if out == nil || order == nil {
		return
	}
	select {
	case out <- order:
	default:
		log.Printf("Bybit futures: order channel full, %s not announced", order.ID)
	}
}

// ErrBybitPermissionDenied reports that the key reaches Bybit but may not do
// what was asked. Separate from every other failure because the fix is a
// setting on the key, not anything in the code or the order.
var ErrBybitPermissionDenied = errors.New("the API key is not allowed to do this on Bybit")

// explainBybitKeyError turns the key errors into what to check.
func explainBybitKeyError(err error) error {
	switch bybitCode(err) {
	case bybitCodePermission:
		return fmt.Errorf("%w: %v. The key needs Contract (Orders and Positions) permission on the Unified Trading Account", ErrBybitPermissionDenied, err)
	case bybitCodeIPNotAllowed:
		return fmt.Errorf("%w: %v. This machine's IP is not in the key's allow-list", ErrBybitPermissionDenied, err)
	case bybitCodeInvalidKey, bybitCodeBadSign:
		return fmt.Errorf("%w: %v. The key or secret is wrong, or belongs to the other network (mainnet keys do not work on testnet)", ErrBybitPermissionDenied, err)
	case bybitCodeTimestamp:
		return fmt.Errorf("%v: this machine's clock is off from Bybit's by more than the 5 s window; sync the clock", err)
	}
	return err
}
