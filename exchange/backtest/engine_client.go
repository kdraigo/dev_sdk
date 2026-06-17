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
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

// EngineClient bridges the SDK dynamically directly into the backtest engine via Backend API and Websocket APIs.
type EngineClient struct {
	config    *types.Config
	sessionID string
	wsConn    *websocket.Conn
	writeMu   sync.Mutex

	streamDone atomic.Bool // set when the engine sends done:true; nextTick becomes a no-op

	pendingOrders   map[string]chan *orderResponse
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

func NewEngineClient(cfg *types.Config) *EngineClient {
	return &EngineClient{
		config:          cfg,
		pendingOrders:   make(map[string]chan *orderResponse),
		pendingAccounts: make(map[string]chan *accountResponse),
		pendingCancels:  make(map[string]chan error),
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
}

type sessionResponse struct {
	ID string `json:"id"`
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
	e.smallestTF = types.Timeframe1m
	if len(cfg.Timeframes) > 0 {
		e.smallestTF = cfg.Timeframes[0]
		// Naive smallest: in this SDK, shorter strings aren't necessarily smaller,
		// but usually the first one in config is the 'base' one.
		// For now, let's just use the first one provided to speed things up significantly.
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
	var wallets []startSessionRequestWallet
	for _, ex := range cfg.Backtest.RequestedExchanges {
		for asset, bal := range cfg.Backtest.Wallets {
			wallets = append(wallets, startSessionRequestWallet{
				SessionID:     uid,
				Exchange:      ex,
				Asset:         asset,
				Balance:       bal,
				LockedBalance: 0,
			})
		}
	}

	payload := newSessionRequestPayload{
		Streams:        streams,
		InitialWallets: wallets,
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
				log.Printf("Backtest Engine WS disconnected: %v", err)
				closeCandleChan() // unblock dispatch goroutine so the tick loop can shut down cleanly
				return
			}

			// Handle pending PlaceOrder/GetAccount/CancelOrder/History waiters
			if resp.RequestID != "" {
				var orderCh chan *orderResponse
				var accountCh chan *accountResponse
				var cancelCh chan error
				var historyCh chan *historyResponse

				e.pendingMu.Lock()
				if ch, ok := e.pendingOrders[resp.RequestID]; ok {
					orderCh = ch
					delete(e.pendingOrders, resp.RequestID)
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

				if historyCh != nil {
					var hr historyResponse
					if resp.Status == "error" {
						hr.err = fmt.Errorf("%s", resp.Error)
					} else {
						var hp struct {
							Candles []struct {
								Pair      string    `json:"Pair"`
								Time      time.Time `json:"time"`
								UpdatedAt time.Time `json:"updatedAt"`
								Open      float64   `json:"open"`
								High      float64   `json:"high"`
								Low       float64   `json:"low"`
								Close     float64   `json:"close"`
								Volume    float64   `json:"volume"`
								Complete  bool      `json:"complete"`
							} `json:"candles"`
						}
						if err := json.Unmarshal(resp.Data, &hp); err != nil {
							hr.err = fmt.Errorf("history decode: %w", err)
						} else {
							out := make([]*types.Candle, 0, len(hp.Candles))
							for _, c := range hp.Candles {
								out = append(out, &types.Candle{
									Symbol:     c.Pair,
									Timeframe:  e.smallestTF,
									OpenTime:   c.Time,
									CloseTime:  c.UpdatedAt,
									Open:       c.Open,
									High:       c.High,
									Low:        c.Low,
									Close:      c.Close,
									Volume:     c.Volume,
									IsComplete: true,
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
					} else {
						var engineOrder struct {
							ExchangeID int64 `json:"exchange_id"`
						}
						json.Unmarshal(resp.Data, &engineOrder)
						json.Unmarshal(resp.Data, &or.order)

						// Map int64 ID to string if it was missing
						if or.order != nil && or.order.ID == "" && engineOrder.ExchangeID != 0 {
							or.order.ID = fmt.Sprintf("%d", engineOrder.ExchangeID)
						}
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

			if resp.Action == "next" {
				var dataStruct struct {
					Tick *struct {
						Exchange string `json:"Exchange"`
						Pair     string `json:"Pair"`
						Candle   struct {
							Time      time.Time `json:"time"`
							UpdatedAt time.Time `json:"updatedAt"`
							Open      float64   `json:"open"`
							High      float64   `json:"high"`
							Low       float64   `json:"low"`
							Close     float64   `json:"close"`
							Volume    float64   `json:"volume"`
							Complete  bool      `json:"complete"`
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
						Symbol:     dataStruct.Tick.Pair,
						Exchange:   dataStruct.Tick.Exchange,
						Timeframe:  e.smallestTF,
						OpenTime:   dataStruct.Tick.Candle.Time,
						CloseTime:  dataStruct.Tick.Candle.UpdatedAt,
						Open:       dataStruct.Tick.Candle.Open,
						High:       dataStruct.Tick.Candle.High,
						Low:        dataStruct.Tick.Candle.Low,
						Close:      dataStruct.Tick.Candle.Close,
						Volume:     dataStruct.Tick.Candle.Volume,
						IsComplete: dataStruct.Tick.Candle.Complete,
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
						ID:           id,
						Symbol:       o.Pair,
						Side:         types.OrderSide(strings.ToUpper(o.Side)),
						Type:         types.OrderType(strings.ToUpper(o.Type)),
						Status:       types.OrderStatus(strings.ToUpper(o.Status)),
						Price:        o.Price,
						Quantity:     o.Quantity,
						FilledQty:    o.Quantity, // engine fills fully
						AveragePrice: o.Price,    // limit order fills at limit price
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

	orderData := map[string]interface{}{
		"exchange": req.Exchange,
		"pair":     req.Symbol, // The engine schema wants Pair instead of Symbol
		"side":     req.Side,
		"type":     req.Type,
		"price":    req.Price,
		"quantity": req.Quantity,
		"asset":    strings.Split(req.Symbol, "/")[1], // Assuming symbol like BTC/USDT needs quote asset
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
	err := e.wsConn.WriteJSON(payload)
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
