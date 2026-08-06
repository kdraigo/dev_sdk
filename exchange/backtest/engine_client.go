package backtest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/kdraigo/dev_sdk/types"
)

// EngineClient bridges the SDK dynamically directly into the backtest engine via Backend API and Websocket APIs.
type EngineClient struct {
	config    *types.Config
	sessionID string
	wsConn    *websocket.Conn
	writeMu   sync.Mutex

	streamDone atomic.Bool           // set when the engine sends done:true; nextTick becomes a no-op
	streamErr  atomic.Pointer[error] // set when the WS stream ends before done:true (truncation)

	pendingOrders   map[string]chan *orderResponse
	pendingBrackets map[string]chan *bracketResponse
	pendingAccounts map[string]chan *accountResponse
	pendingCancels  map[string]chan error
	pendingHistory  map[string]chan *historyResponse
	pendingMu       sync.Mutex

	smallestTF types.Timeframe
}

// historyResponse carries the result of a "history" WS round-trip.
type historyResponse struct {
	candles []*types.Candle
	err     error
}

type orderResponse struct {
	order *types.Order
	err   error
}

// bracketResponse carries the two legs of an OCO bracket. It has its own type
// because the order channel is single-order by design.
type bracketResponse struct {
	orders []*types.Order
	err    error
}

// orderWire mirrors the engine's on-the-wire order shape (lib/core.Order). The SDK's
// public types.Order deliberately carries no json tags, so we unmarshal into this
// explicit struct and map fields by hand rather than let the engine's wire format
// shape the public type (D2).
type orderWire struct {
	ID         int64   `json:"id"`
	ExchangeID int64   `json:"exchange_id"`
	Pair       string  `json:"pair"`
	Side       string  `json:"side"`
	Type       string  `json:"type"`
	Status     string  `json:"status"`
	Price      float64  `json:"price"`
	Quantity   float64  `json:"quantity"`
	Commission float64  `json:"commission"`
	Stop       *float64 `json:"stop"`
	GroupID    *int64   `json:"group_id"`
}

// quoteAssetOf extracts the quote side of a BASE/QUOTE symbol.
//
// This used to be an unchecked index into strings.Split, which panicked and
// took the whole strategy process down whenever a symbol arrived without a
// slash — a plausible typo, not an exceptional condition.
func quoteAssetOf(symbol string) (string, error) {
	parts := strings.Split(symbol, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", fmt.Errorf("symbol %q must be in BASE/QUOTE form, e.g. BTC/USDT", symbol)
	}
	return parts[1], nil
}

// PlaceBracket places a take-profit and a protective stop as one mutually
// cancelling pair. It implements dev_sdk.BracketPlacer.
func (e *EngineClient) PlaceBracket(ctx context.Context, req *types.BracketRequest) ([]*types.Order, error) {
	if req == nil {
		return nil, fmt.Errorf("bracket request must not be nil")
	}
	if req.TakeProfitPrice <= 0 || req.StopPrice <= 0 {
		return nil, fmt.Errorf("bracket requires a positive TakeProfitPrice and StopPrice")
	}

	quoteAsset, err := quoteAssetOf(req.Symbol)
	if err != nil {
		return nil, err
	}

	reqID := uuid.NewString()
	respChan := make(chan *bracketResponse, 1)

	e.pendingMu.Lock()
	e.pendingBrackets[reqID] = respChan
	e.pendingMu.Unlock()

	cleanup := func() {
		e.pendingMu.Lock()
		delete(e.pendingBrackets, reqID)
		e.pendingMu.Unlock()
	}

	data := map[string]interface{}{
		"exchange":          req.Exchange,
		"asset":             quoteAsset,
		"pair":              req.Symbol,
		"side":              req.Side,
		"quantity":          req.Quantity,
		"take_profit_price": req.TakeProfitPrice,
		"stop_price":        req.StopPrice,
	}
	if req.StopLimitPrice > 0 {
		data["stop_limit_price"] = req.StopLimitPrice
	}
	if req.Reason != nil {
		data["reason"] = req.Reason
	}
	if len(req.Logs) > 0 {
		data["logs"] = req.Logs
	}

	e.writeMu.Lock()
	err = e.wsConn.WriteJSON(map[string]interface{}{
		"action":     "order_bracket",
		"request_id": reqID,
		"data":       data,
	})
	e.writeMu.Unlock()

	if err != nil {
		cleanup()
		return nil, fmt.Errorf("failed to send bracket request: %w", err)
	}

	select {
	case resp := <-respChan:
		if resp.err != nil {
			return nil, resp.err
		}
		for _, o := range resp.orders {
			o.Exchange = req.Exchange
		}
		return resp.orders, nil

	case <-ctx.Done():
		cleanup()
		return nil, ctx.Err()

	case <-time.After(10 * time.Second):
		// Matches PlaceOrder's fail-safe: a lost response must not wedge the
		// strategy loop forever.
		cleanup()
		return nil, fmt.Errorf("timeout waiting for bracket response")
	}
}

// parseOrderAck maps a synchronous PlaceOrder ack into a types.Order. The engine
// omits average_price/filled_qty on the ack, so for a FILLED order they are derived
// from the fill price/quantity — market orders fill at the candle close, limit
// orders at the limit price, both reported in `price` (D2).
func parseOrderAck(data json.RawMessage) *types.Order {
	var w orderWire
	if err := json.Unmarshal(data, &w); err != nil {
		log.Printf("Backtest Engine: failed to decode order ack: %v", err)
		return nil
	}

	id := ""
	if w.ExchangeID != 0 {
		id = fmt.Sprintf("%d", w.ExchangeID)
	} else if w.ID != 0 {
		id = fmt.Sprintf("%d", w.ID)
	}

	order := &types.Order{
		ID:       id,
		Symbol:   w.Pair,
		Side:     types.OrderSide(strings.ToUpper(w.Side)),
		Type:     types.OrderType(strings.ToUpper(w.Type)),
		Status:   types.OrderStatus(strings.ToUpper(w.Status)),
		Price:    w.Price,
		Quantity: w.Quantity,
		Fee:      w.Commission,
	}

	// Carry the bracket fields through, so a strategy handling OnOrderUpdate can
	// tell a stop fill from a limit fill at the same price, and can see which
	// orders belonged to the same mutually-cancelling pair.
	if w.Stop != nil {
		order.StopPrice = *w.Stop
	}
	if w.GroupID != nil {
		order.GroupID = fmt.Sprintf("%d", *w.GroupID)
	}

	// The engine reports a synchronous market/limit fill as already FILLED but does
	// not echo average_price/filled_qty; derive them from the fill so strategies
	// that size or book PnL off AveragePrice/FilledQty read real values.
	if order.Status == types.OrderStatusFilled {
		order.FilledQty = w.Quantity
		order.AveragePrice = w.Price
	}

	return order
}

func NewEngineClient(cfg *types.Config) *EngineClient {
	return &EngineClient{
		config:          cfg,
		pendingOrders:   make(map[string]chan *orderResponse),
		pendingAccounts: make(map[string]chan *accountResponse),
		pendingCancels:  make(map[string]chan error),
		pendingBrackets: make(map[string]chan *bracketResponse),
		pendingHistory:  make(map[string]chan *historyResponse),
	}
}

type startSessionRequestStream struct {
	SessionID uuid.UUID `json:"sessionID"`
	Exchange  string    `json:"exchange"`
	Pair      string    `json:"pair"`
	Timeframe string    `json:"timeframe"`
	From      time.Time `json:"from"`
	To        time.Time `json:"to"`
}

type startSessionRequestWallet struct {
	SessionID     uuid.UUID `json:"session_id"`
	Exchange      string    `json:"exchange"`
	Asset         string    `json:"asset"`
	Balance       float64   `json:"balance"`
	LockedBalance float64   `json:"locked_balance"`
}

type newSessionRequestPayload struct {
	Streams        []startSessionRequestStream `json:"streams"`
	InitialWallets []startSessionRequestWallet `json:"initial_wallets"`
	Simulation     *types.SimulationOptions    `json:"simulation,omitempty"`
}

type sessionResponse struct {
	ID string `json:"id"`
}


// buildWallets turns the configured starting balances into per-exchange wallet
// requests.
//
// Funds belong to an exchange: capital at binance cannot be spent on bybit. The
// previous form applied every Wallets entry to every requested exchange, so a
// two-exchange session started with twice the intended capital and reported
// buying power that never existed.
func buildWallets(sessionID uuid.UUID, opts *types.BacktestOptions) ([]startSessionRequestWallet, error) {
	hasFlat := len(opts.Wallets) > 0
	hasPerExchange := len(opts.WalletsByExchange) > 0

	switch {
	case hasFlat && hasPerExchange:
		return nil, fmt.Errorf("set either Wallets or WalletsByExchange, not both")

	case !hasFlat && !hasPerExchange:
		return nil, fmt.Errorf("no starting balances configured: set Wallets or WalletsByExchange")

	case hasFlat && len(opts.RequestedExchanges) > 1:
		// Ambiguous rather than wrong: we cannot know how the caller wants the
		// balance split, and silently duplicating it is what caused the bug.
		return nil, fmt.Errorf(
			"Wallets is ambiguous across %d exchanges (%v): use WalletsByExchange to say how much sits at each",
			len(opts.RequestedExchanges), opts.RequestedExchanges)
	}

	byExchange := opts.WalletsByExchange
	if hasFlat {
		exchange := ""
		if len(opts.RequestedExchanges) == 1 {
			exchange = opts.RequestedExchanges[0]
		}
		if exchange == "" {
			return nil, fmt.Errorf("Wallets requires exactly one entry in RequestedExchanges")
		}
		byExchange = map[string]map[string]float64{exchange: opts.Wallets}
	}

	requested := make(map[string]struct{}, len(opts.RequestedExchanges))
	for _, ex := range opts.RequestedExchanges {
		requested[ex] = struct{}{}
	}

	// Deterministic ordering: the session request is signed, so an unstable
	// wallet order would change the payload between otherwise identical runs.
	exchanges := make([]string, 0, len(byExchange))
	for ex := range byExchange {
		if _, ok := requested[ex]; !ok {
			return nil, fmt.Errorf("wallet configured for %q, which is not in RequestedExchanges %v", ex, opts.RequestedExchanges)
		}
		exchanges = append(exchanges, ex)
	}
	sort.Strings(exchanges)

	var wallets []startSessionRequestWallet
	for _, ex := range exchanges {
		assets := make([]string, 0, len(byExchange[ex]))
		for asset := range byExchange[ex] {
			assets = append(assets, asset)
		}
		sort.Strings(assets)

		for _, asset := range assets {
			wallets = append(wallets, startSessionRequestWallet{
				SessionID:     sessionID,
				Exchange:      ex,
				Asset:         asset,
				Balance:       byExchange[ex][asset],
				LockedBalance: 0,
			})
		}
	}

	return wallets, nil
}

func (e *EngineClient) PrepareSession(ctx context.Context, cfg *types.Config) error {
	log.Printf("Backtest Engine: Preparing Session...\n")

	if cfg.Backtest.Endpoint == "" {
		cfg.Backtest.Endpoint = "http://localhost:4000"
	}
	if !strings.HasPrefix(cfg.Backtest.Endpoint, "http") {
		cfg.Backtest.Endpoint = "http://" + cfg.Backtest.Endpoint
	}

	// 1. Build Payload
	uid := uuid.New()

	// Create required streams requests for all requested Exchange-Asset pairs
	// The session streams at the shortest requested timeframe, because every
	// longer one is folded up from it. This used to take Timeframes[0], so
	// declaring []{1h, 5m} streamed at 1h and the 5m callback never fired with
	// real 5m data. Ordering is by duration, not by name length.
	e.smallestTF = types.Timeframe1m
	if smallest, ok := types.SmallestTimeframe(cfg.Timeframes); ok {
		e.smallestTF = smallest
	}

	var streams []startSessionRequestStream
	for _, ex := range cfg.Backtest.RequestedExchanges {
		for _, asset := range cfg.Backtest.Assets {
			streams = append(streams, startSessionRequestStream{
				SessionID: uid,
				Exchange:  ex,
				Pair:      asset,
				Timeframe: string(e.smallestTF),
				From:      cfg.Backtest.StartTime,
				To:        cfg.Backtest.EndTime,
			})
		}
	}

	// Format wallets
	wallets, err := buildWallets(uid, cfg.Backtest)
	if err != nil {
		return err
	}

	payload := newSessionRequestPayload{
		Streams:        streams,
		InitialWallets: wallets,
		Simulation:     cfg.Backtest.Simulation,
	}
	body, _ := json.Marshal(payload)

	// 2. Perform HTTP action
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	sig, err := e.generateSignature(http.MethodPost, "/api/v1/dev/session", timestamp, string(body))
	if err != nil {
		return fmt.Errorf("failed to generate signature: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.Backtest.Endpoint+"/api/v1/dev/session", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-API-KEY", e.config.Credentials.KeyID)
	req.Header.Set("X-SIGNATURE", sig)
	req.Header.Set("X-TIMESTAMP", timestamp)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to call engine /session API: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed creating session. status=%d body=%s", resp.StatusCode, b)
	}

	var sessResp sessionResponse
	if err := json.NewDecoder(resp.Body).Decode(&sessResp); err != nil {
		return err
	}

	e.sessionID = sessResp.ID
	log.Printf("Backtest Engine: Successfully initialized Session ID: %s", e.sessionID)
	return nil
}

func (e *EngineClient) ConnectStream(ctx context.Context, candleChan chan<- *types.Candle, orderChan chan<- *types.Order) error {
	log.Printf("Backtest Engine: Establishing WS connection for session %s...", e.sessionID)
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	sig, err := e.generateSignature(http.MethodGet, "/api/v1/dev/session/ws", timestamp, "")
	if err != nil {
		return fmt.Errorf("failed to generate signature: %v", err)
	}

	wsEndpoint := strings.Replace(e.config.Backtest.Endpoint, "http", "ws", 1) +
		"/api/v1/dev/session/ws?id=" + e.sessionID +
		"&key_id=" + e.config.Credentials.KeyID +
		"&signature=" + sig +
		"&timestamp=" + timestamp

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("websocket dial failed: %v", err)
	}
	e.wsConn = conn

	// Use gorilla's default ping handler (sends pong automatically, no mutex needed).
	// Our custom handler was acquiring writeMu inside ReadJSON which could deadlock
	// if writeMu was held by nextTick() at the same moment.
	conn.SetPingHandler(nil)

	// Refresh read deadline after every message so a silent engine is detected quickly.
	const readTimeout = 60 * time.Second
	conn.SetReadDeadline(time.Now().Add(readTimeout))

	// 5. Command the exchange Adapter to begin pumping data into `rawCandleChan` & `orderChan` natives.
	go func() {
		defer conn.Close()
		candleClosed := false
		closeCandleChan := func() {
			if !candleClosed {
				candleClosed = true
				close(candleChan)
			}
		}
		for {
			var resp struct {
				Action    string          `json:"action"`
				Status    string          `json:"status"`
				Data      json.RawMessage `json:"data"`
				Error     string          `json:"error"`
				RequestID string          `json:"request_id"`
			}
			conn.SetReadDeadline(time.Now().Add(readTimeout))
			if err := conn.ReadJSON(&resp); err != nil {
				// A read error after done:true is a normal shutdown; anything before
				// it is a truncated stream and must be surfaced as a terminal error so
				// the SDK does not report partial data as a clean completion (D1).
				if !e.streamDone.Load() {
					wrapped := fmt.Errorf("backtest stream truncated: %w", err)
					e.streamErr.Store(&wrapped)
					log.Printf("Backtest Engine WS disconnected before completion: %v", err)
				} else {
					log.Printf("Backtest Engine WS closed after completion: %v", err)
				}
				closeCandleChan() // unblock dispatch goroutine so the tick loop can shut down cleanly
				return
			}

			// Handle pending PlaceOrder/GetAccount/CancelOrder/History waiters
			if resp.RequestID != "" {
				var orderCh chan *orderResponse
				var bracketCh chan *bracketResponse
				var accountCh chan *accountResponse
				var cancelCh chan error
				var historyCh chan *historyResponse

				e.pendingMu.Lock()
				if ch, ok := e.pendingOrders[resp.RequestID]; ok {
					orderCh = ch
					delete(e.pendingOrders, resp.RequestID)
				} else if ch, ok := e.pendingBrackets[resp.RequestID]; ok {
					bracketCh = ch
					delete(e.pendingBrackets, resp.RequestID)
				} else if ch, ok := e.pendingAccounts[resp.RequestID]; ok {
					accountCh = ch
					delete(e.pendingAccounts, resp.RequestID)
				} else if ch, ok := e.pendingCancels[resp.RequestID]; ok {
					cancelCh = ch
					delete(e.pendingCancels, resp.RequestID)
				} else if ch, ok := e.pendingHistory[resp.RequestID]; ok {
					historyCh = ch
					delete(e.pendingHistory, resp.RequestID)
				}
				e.pendingMu.Unlock()

				if bracketCh != nil {
					var br bracketResponse
					if resp.Status == "error" {
						br.err = fmt.Errorf("%s", resp.Error)
					} else {
						var bp struct {
							Orders []orderWire `json:"orders"`
						}
						if err := json.Unmarshal(resp.Data, &bp); err != nil {
							br.err = fmt.Errorf("bracket decode: %w", err)
						} else {
							for i := range bp.Orders {
								encoded, err := json.Marshal(bp.Orders[i])
								if err != nil {
									continue
								}
								if order := parseOrderAck(encoded); order != nil {
									br.orders = append(br.orders, order)
								}
							}
						}
					}
					bracketCh <- &br
					continue
				}

				if historyCh != nil {
					var hr historyResponse
					if resp.Status == "error" {
						hr.err = fmt.Errorf("%s", resp.Error)
					} else {
						var hp struct {
							Candles []struct {
								Pair                string    `json:"Pair"`
								Time                time.Time `json:"time"`
								UpdatedAt           time.Time `json:"updatedAt"`
								Open                float64   `json:"open"`
								High                float64   `json:"high"`
								Low                 float64   `json:"low"`
								Close               float64   `json:"close"`
								Volume              float64   `json:"volume"`
								Complete            bool      `json:"complete"`
								TradeCount          int64     `json:"tradeCount"`
								QuoteVolume         float64   `json:"quoteVolume"`
								TakerBuyBaseVolume  float64   `json:"takerBuyBaseVolume"`
								TakerBuyQuoteVolume float64   `json:"takerBuyQuoteVolume"`
							} `json:"candles"`
						}
						if err := json.Unmarshal(resp.Data, &hp); err != nil {
							hr.err = fmt.Errorf("history decode: %w", err)
						} else {
							out := make([]*types.Candle, 0, len(hp.Candles))
							for _, c := range hp.Candles {
								out = append(out, &types.Candle{
									Symbol:              c.Pair,
									Timeframe:           e.smallestTF,
									OpenTime:            c.Time,
									CloseTime:           c.UpdatedAt,
									Open:                c.Open,
									High:                c.High,
									Low:                 c.Low,
									Close:               c.Close,
									Volume:              c.Volume,
									IsComplete:          true,
									TradeCount:          c.TradeCount,
									QuoteVolume:         c.QuoteVolume,
									TakerBuyBaseVolume:  c.TakerBuyBaseVolume,
									TakerBuyQuoteVolume: c.TakerBuyQuoteVolume,
								})
							}
							hr.candles = out
						}
					}
					historyCh <- &hr
					close(historyCh)
					continue
				}

				if orderCh != nil {
					var or orderResponse
					if resp.Status == "error" {
						or.err = fmt.Errorf("%s", resp.Error)
						// If the engine attached structured rejection detail, surface
						// it explicitly so the strategy can react (e.g. shrink size).
						var rej struct {
							Code           string  `json:"code"`
							RequiredQuote  float64 `json:"required_quote"`
							AvailableQuote float64 `json:"available_quote"`
							LockedQuote    float64 `json:"locked_quote"`
							FeeEstimate    float64 `json:"fee_estimate"`
						}
						if len(resp.Data) > 0 && json.Unmarshal(resp.Data, &rej) == nil && rej.Code != "" {
							or.err = fmt.Errorf("order rejected [%s]: required_quote=%.8f available_quote=%.8f locked_quote=%.8f fee=%.8f",
								rej.Code, rej.RequiredQuote, rej.AvailableQuote, rej.LockedQuote, rej.FeeEstimate)
						}
					} else {
						or.order = parseOrderAck(resp.Data)
					}
					orderCh <- &or
					close(orderCh)
				} else if accountCh != nil {
					var ar accountResponse
					if resp.Status == "error" {
						ar.err = fmt.Errorf("%s", resp.Error)
					} else {
						json.Unmarshal(resp.Data, &ar.account)
					}
					accountCh <- &ar
					close(accountCh)
				} else if cancelCh != nil {
					if resp.Status == "error" {
						cancelCh <- fmt.Errorf("%s", resp.Error)
					} else {
						cancelCh <- nil
					}
					close(cancelCh)
				}
			}

			if resp.Status != "ok" {
				log.Printf("Engine WS Error on %s: %s", resp.Action, resp.Error)
				continue
			}

			// Server-pushed keepalive/observability frame. Arriving every ~15s, it
			// also refreshes the read deadline above, which is what keeps long/slow
			// backtests from being torn down as "disconnected". Just surface it.
			if resp.Action == "progress" {
				var p struct {
					ProcessedCandles int64   `json:"processed_candles"`
					TotalCandles     int64   `json:"total_candles"`
					Percent          float64 `json:"percent"`
					ETASeconds       float64 `json:"eta_seconds"`
				}
				if json.Unmarshal(resp.Data, &p) == nil && p.TotalCandles > 0 {
					log.Printf("Backtest progress: %d/%d candles (%.1f%%), ETA %.0fs",
						p.ProcessedCandles, p.TotalCandles, p.Percent, p.ETASeconds)
				}
				continue
			}

			if resp.Action == "next" {
				var dataStruct struct {
					Tick *struct {
						Exchange string `json:"Exchange"`
						Pair     string `json:"Pair"`
						Candle   struct {
							Time                time.Time `json:"time"`
							UpdatedAt           time.Time `json:"updatedAt"`
							Open                float64   `json:"open"`
							High                float64   `json:"high"`
							Low                 float64   `json:"low"`
							Close               float64   `json:"close"`
							Volume              float64   `json:"volume"`
							Complete            bool      `json:"complete"`
							TradeCount          int64     `json:"tradeCount"`
							QuoteVolume         float64   `json:"quoteVolume"`
							TakerBuyBaseVolume  float64   `json:"takerBuyBaseVolume"`
							TakerBuyQuoteVolume float64   `json:"takerBuyQuoteVolume"`
						} `json:"Candle"`
					} `json:"tick"`
					Done   bool `json:"done"`
					Orders []struct {
						ID         int64   `json:"id"`
						ExchangeID int64   `json:"exchange_id"`
						Pair       string  `json:"pair"`
						Side       string  `json:"side"`
						Type       string  `json:"type"`
						Status     string  `json:"status"`
						Price      float64 `json:"price"`
						Quantity   float64 `json:"quantity"`
					} `json:"orders"`
				}
				json.Unmarshal(resp.Data, &dataStruct)
				// When done:true the engine sends a sentinel zero-value tick.
				// Skip it — the zero candle would otherwise pollute the SDK
				// pipeline (advance clock backwards by no-op, fire OnCandle
				// with empty data, etc.). The done flag itself is enough.
				if dataStruct.Tick != nil && !dataStruct.Done && !dataStruct.Tick.Candle.Time.IsZero() {
					candle := &types.Candle{
						Symbol:              dataStruct.Tick.Pair,
						Exchange:            dataStruct.Tick.Exchange,
						Timeframe:           e.smallestTF,
						OpenTime:            dataStruct.Tick.Candle.Time,
						CloseTime:           dataStruct.Tick.Candle.UpdatedAt,
						Open:                dataStruct.Tick.Candle.Open,
						High:                dataStruct.Tick.Candle.High,
						Low:                 dataStruct.Tick.Candle.Low,
						Close:               dataStruct.Tick.Candle.Close,
						Volume:              dataStruct.Tick.Candle.Volume,
						IsComplete:          dataStruct.Tick.Candle.Complete,
						TradeCount:          dataStruct.Tick.Candle.TradeCount,
						QuoteVolume:         dataStruct.Tick.Candle.QuoteVolume,
						TakerBuyBaseVolume:  dataStruct.Tick.Candle.TakerBuyBaseVolume,
						TakerBuyQuoteVolume: dataStruct.Tick.Candle.TakerBuyQuoteVolume,
					}
					// log.Printf("[WS] Received Candle: %s", candle.OpenTime.Format("2006-01-02 15:04"))
					candleChan <- candle
				}
				// Dispatch any orders filled during this tick to the order channel.
				for _, o := range dataStruct.Orders {
					id := fmt.Sprintf("%d", o.ExchangeID)
					if id == "0" {
						id = fmt.Sprintf("%d", o.ID)
					}
					if id == "0" {
						continue
					}
					orderChan <- &types.Order{
						ID:        id,
						Symbol:    o.Pair,
						Side:      types.OrderSide(strings.ToUpper(o.Side)),
						Type:      types.OrderType(strings.ToUpper(o.Type)),
						Status:    types.OrderStatus(strings.ToUpper(o.Status)),
						Price:     o.Price,
						Quantity:  o.Quantity,
						FilledQty: o.Quantity, // engine fills fully
						// Fill price for every order type: market orders fill at the
						// current candle's close, limit orders at the limit price. In
						// both cases the engine reports it in o.Price.
						AveragePrice: o.Price,
					}
				}
				if dataStruct.Done {
					log.Println("Backtest Engine: Data stream finished.")
					e.streamDone.Store(true)
					close(candleChan) // Signal aggregator that no more candles are coming
					return
				}
			}
		}
	}()

	return nil
}

// StreamErr reports a terminal error if the candle stream ended before the engine
// signalled done:true (e.g. a dropped websocket). It returns nil on a clean finish.
// The SDK consults this when the candle channel closes to distinguish a truncated
// run from a completed one (D1).
func (e *EngineClient) StreamErr() error {
	if p := e.streamErr.Load(); p != nil {
		return *p
	}
	return nil
}

// nextTick issues a "next" command to step the backtester engine.
// It is a no-op once the engine has signalled that the stream is finished.
func (e *EngineClient) nextTick() error {
	if e.streamDone.Load() {
		return nil
	}
	if e.wsConn == nil {
		return fmt.Errorf("nextTick failed: websocket not connected")
	}

	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	// log.Printf("[WS] Sending Action: next")
	if err := e.wsConn.WriteJSON(map[string]string{"action": "next"}); err != nil {
		return fmt.Errorf("nextTick WriteJSON error: %w", err)
	}
	return nil
}

func (e *EngineClient) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	if e.wsConn == nil {
		return nil, fmt.Errorf("websocket not connected")
	}

	reqID := uuid.New().String()
	respChan := make(chan *orderResponse, 1)

	e.pendingMu.Lock()
	e.pendingOrders[reqID] = respChan
	e.pendingMu.Unlock()

	quoteAsset, err := quoteAssetOf(req.Symbol)
	if err != nil {
		e.pendingMu.Lock()
		delete(e.pendingOrders, reqID)
		e.pendingMu.Unlock()
		return nil, err
	}

	orderData := map[string]interface{}{
		"exchange": req.Exchange,
		"pair":     req.Symbol, // The engine schema wants Pair instead of Symbol
		"side":     req.Side,
		"type":     req.Type,
		"price":    req.Price,
		"quantity": req.Quantity,
		"asset":    quoteAsset,
	}
	if req.StopPrice > 0 {
		orderData["stop_price"] = req.StopPrice
	}
	if req.Reason != nil {
		orderData["reason"] = req.Reason
	}
	if len(req.Logs) > 0 {
		orderData["logs"] = req.Logs
	}
	payload := map[string]interface{}{
		"action":     "order",
		"request_id": reqID,
		"data":       orderData,
	}

	e.writeMu.Lock()
	// log.Printf("[WS] Sending Action: order (reqID: %s)", reqID)
	err = e.wsConn.WriteJSON(payload)
	e.writeMu.Unlock()

	if err != nil {
		e.pendingMu.Lock()
		delete(e.pendingOrders, reqID)
		e.pendingMu.Unlock()
		return nil, err
	}

	// Wait for response or timeout
	select {
	case resp := <-respChan:
		if resp.err != nil {
			return nil, resp.err
		}
		return resp.order, nil
	case <-ctx.Done():
		e.pendingMu.Lock()
		delete(e.pendingOrders, reqID)
		e.pendingMu.Unlock()
		return nil, ctx.Err()
	case <-time.After(10 * time.Second): // Fail-safe timeout
		e.pendingMu.Lock()
		delete(e.pendingOrders, reqID)
		e.pendingMu.Unlock()
		return nil, fmt.Errorf("timeout waiting for order confirmation")
	}
}

func (e *EngineClient) CancelOrder(ctx context.Context, exchange, symbol, orderID string) error {
	if e.wsConn == nil {
		return fmt.Errorf("websocket not connected")
	}

	reqID := uuid.New().String()
	respChan := make(chan error, 1)

	e.pendingMu.Lock()
	e.pendingCancels[reqID] = respChan
	e.pendingMu.Unlock()

	payload := map[string]interface{}{
		"action":     "cancel",
		"request_id": reqID,
		"data": map[string]string{
			"order_id": orderID,
		},
	}

	e.writeMu.Lock()
	err := e.wsConn.WriteJSON(payload)
	e.writeMu.Unlock()

	if err != nil {
		e.pendingMu.Lock()
		delete(e.pendingCancels, reqID)
		e.pendingMu.Unlock()
		return err
	}

	select {
	case err := <-respChan:
		return err
	case <-ctx.Done():
		e.pendingMu.Lock()
		delete(e.pendingCancels, reqID)
		e.pendingMu.Unlock()
		return ctx.Err()
	case <-time.After(5 * time.Second):
		e.pendingMu.Lock()
		delete(e.pendingCancels, reqID)
		e.pendingMu.Unlock()
		return fmt.Errorf("timeout waiting for cancel confirmation")
	}
}

func (e *EngineClient) GetAccount(ctx context.Context, exchange string, asset string) (*types.Account, error) {
	if e.wsConn == nil {
		return nil, fmt.Errorf("websocket not connected")
	}

	reqID := uuid.New().String()
	respChan := make(chan *accountResponse, 1)

	e.pendingMu.Lock()
	e.pendingAccounts[reqID] = respChan
	e.pendingMu.Unlock()

	payload := map[string]interface{}{
		"action":     "account",
		"request_id": reqID,
		"data": map[string]string{
			"exchange": exchange,
			"asset":    asset,
		},
	}

	e.writeMu.Lock()
	err := e.wsConn.WriteJSON(payload)
	e.writeMu.Unlock()

	if err != nil {
		e.pendingMu.Lock()
		delete(e.pendingAccounts, reqID)
		e.pendingMu.Unlock()
		return nil, err
	}

	select {
	case resp := <-respChan:
		if resp.err != nil {
			return nil, resp.err
		}
		return resp.account, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("timeout waiting for account info")
	}
}

func (e *EngineClient) Next(ctx context.Context) error {
	return e.nextTick()
}

// GetHistoricalCandles round-trips a "history" request to the backtester
// engine over the existing WS connection. The engine validates that `to` does
// not exceed the simulated playhead and serves candles from data_provider.
// Pure read — no playhead, wallet, or coordinator state is touched on either side.
func (e *EngineClient) GetHistoricalCandles(ctx context.Context, exchange, symbol string, from, to time.Time, tf types.Timeframe) ([]*types.Candle, error) {
	if e.wsConn == nil {
		return nil, fmt.Errorf("websocket not connected")
	}

	reqID := uuid.New().String()
	respChan := make(chan *historyResponse, 1)

	e.pendingMu.Lock()
	e.pendingHistory[reqID] = respChan
	e.pendingMu.Unlock()

	payload := map[string]interface{}{
		"action":     "history",
		"request_id": reqID,
		"data": map[string]interface{}{
			"exchange":  exchange,
			"pair":      symbol,
			"timeframe": string(tf),
			"from":      from,
			"to":        to,
		},
	}

	e.writeMu.Lock()
	err := e.wsConn.WriteJSON(payload)
	e.writeMu.Unlock()

	if err != nil {
		e.pendingMu.Lock()
		delete(e.pendingHistory, reqID)
		e.pendingMu.Unlock()
		return nil, err
	}

	select {
	case resp := <-respChan:
		if resp.err != nil {
			return nil, resp.err
		}
		// Stamp returned candles with the requested exchange (engine response
		// carries Pair but not Exchange).
		for _, c := range resp.candles {
			c.Exchange = exchange
		}
		return resp.candles, nil
	case <-ctx.Done():
		e.pendingMu.Lock()
		delete(e.pendingHistory, reqID)
		e.pendingMu.Unlock()
		return nil, ctx.Err()
	case <-time.After(30 * time.Second):
		e.pendingMu.Lock()
		delete(e.pendingHistory, reqID)
		e.pendingMu.Unlock()
		return nil, fmt.Errorf("timeout waiting for history response")
	}
}

func (e *EngineClient) generateSignature(method, path, timestamp, body string) (string, error) {
	if e.config.Credentials.PrivateKey == "" {
		return "", fmt.Errorf("platform private key is missing in config")
	}

	privKeyBytes, err := hex.DecodeString(e.config.Credentials.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("failed to decode private key: %v", err)
	}

	if len(privKeyBytes) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("invalid private key size: expected %d, got %d", ed25519.PrivateKeySize, len(privKeyBytes))
	}

	canonical := fmt.Sprintf("%s\n%s\n%s\n%s", method, path, timestamp, body)
	sig := ed25519.Sign(privKeyBytes, []byte(canonical))
	return hex.EncodeToString(sig), nil
}

type accountResponse struct {
	account *types.Account
	err     error
}
