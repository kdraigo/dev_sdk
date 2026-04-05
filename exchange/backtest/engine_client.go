package backtest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
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

	pendingOrders   map[string]chan *orderResponse
	pendingAccounts map[string]chan *accountResponse
	pendingMu       sync.Mutex
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
	}
}

type startSessionRequestStream struct {
	SessionID uuid.UUID `json:"sessionID"`
	Pair      string    `json:"pair"`
	Timeframe string    `json:"timeframe"`
	From      string    `json:"from"`
	To        string    `json:"to"`
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
	log.Printf("Backtest Engine: Preparing Session '%s'...\n", cfg.Backtest.SessionName)

	if cfg.Backtest.Endpoint == "" {
		cfg.Backtest.Endpoint = "http://localhost:4000"
	}
	if !strings.HasPrefix(cfg.Backtest.Endpoint, "http") {
		cfg.Backtest.Endpoint = "http://" + cfg.Backtest.Endpoint
	}

	// 1. Build Payload
	uid := uuid.New()

	// Create required streams requests for all requested Exchange-Asset pairs
	var streams []startSessionRequestStream
	for _, asset := range cfg.Backtest.Assets {
		streams = append(streams, startSessionRequestStream{
			SessionID: uid,
			Pair:      asset,
			Timeframe: string(cfg.Timeframe),
			From:      cfg.Backtest.StartTime.Format("2006-01-02T15:04:05Z07:00"),
			To:        cfg.Backtest.EndTime.Format("2006-01-02T15:04:05Z07:00"),
		})
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

	payload := newSessionRequestPayload{Streams: streams, InitialWallets: wallets}
	data, _ := json.Marshal(payload)

	// 2. Perform HTTP action
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.Backtest.Endpoint+"/api/v1/dev/session", bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("X-User-ID", uid.String())
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

	wsEndpoint := strings.Replace(e.config.Backtest.Endpoint, "http", "ws", 1) + "/api/v1/dev/session/ws?id=" + e.sessionID

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsEndpoint, nil)
	if err != nil {
		return fmt.Errorf("websocket dial failed: %v", err)
	}
	e.wsConn = conn

	// Spawn listening routine
	go func() {
		defer conn.Close()
		for {
			var resp struct {
				Action    string          `json:"action"`
				Status    string          `json:"status"`
				Data      json.RawMessage `json:"data"`
				Error     string          `json:"error"`
				RequestID string          `json:"request_id"`
			}
			if err := conn.ReadJSON(&resp); err != nil {
				return
			}

			// Handle pending PlaceOrder/GetAccount waiters
			if resp.RequestID != "" {
				e.pendingMu.Lock()
				if ch, ok := e.pendingOrders[resp.RequestID]; ok {
					var or orderResponse
					if resp.Status == "error" {
						or.err = fmt.Errorf(resp.Error)
					} else {
						var engineOrder struct {
							ExchangeID int64 `json:"exchange_id"`
						}
						json.Unmarshal(resp.Data, &engineOrder)
						json.Unmarshal(resp.Data, &or.order)

						// Map int64 ID to string if it was missing
						if or.order.ID == "" && engineOrder.ExchangeID != 0 {
							or.order.ID = fmt.Sprintf("%d", engineOrder.ExchangeID)
						}
					}
					ch <- &or
					close(ch)
					delete(e.pendingOrders, resp.RequestID)
				} else if ch, ok := e.pendingAccounts[resp.RequestID]; ok {
					var ar accountResponse
					if resp.Status == "error" {
						ar.err = fmt.Errorf(resp.Error)
					} else {
						json.Unmarshal(resp.Data, &ar.account)
					}
					ch <- &ar
					close(ch)
					delete(e.pendingAccounts, resp.RequestID)
				}
				e.pendingMu.Unlock()
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
					Done bool `json:"done"`
				}
				json.Unmarshal(resp.Data, &dataStruct)
				if dataStruct.Tick != nil {
					candleChan <- &types.Candle{
						Symbol:     dataStruct.Tick.Pair,
						Exchange:   dataStruct.Tick.Exchange,
						OpenTime:   dataStruct.Tick.Candle.Time,
						CloseTime:  dataStruct.Tick.Candle.UpdatedAt,
						Open:       dataStruct.Tick.Candle.Open,
						High:       dataStruct.Tick.Candle.High,
						Low:        dataStruct.Tick.Candle.Low,
						Close:      dataStruct.Tick.Candle.Close,
						Volume:     dataStruct.Tick.Candle.Volume,
						IsComplete: dataStruct.Tick.Candle.Complete,
					}
				}
				if dataStruct.Done {
					log.Println("Backtest Engine: Data stream finished.")
					return
				}
			}

			if resp.Action == "order" {
				var engineOrder struct {
					ID         int64 `json:"id"`
					ExchangeID int64 `json:"exchange_id"`
				}
				json.Unmarshal(resp.Data, &engineOrder)

				var order types.Order
				json.Unmarshal(resp.Data, &order)

				// Ensure ID is populated for SDK tracking
				if order.ID == "" {
					if engineOrder.ID != 0 {
						order.ID = fmt.Sprintf("%d", engineOrder.ID)
					} else if engineOrder.ExchangeID != 0 {
						order.ID = fmt.Sprintf("%d", engineOrder.ExchangeID)
					}
				}

				if order.ID != "" {
					orderChan <- &order
				}
			}
		}
	}()

	// Handshake: Initial account state should be fetched by the SDK before/during start
	// The engine waits for the first "next" command from the SDK.

	<-ctx.Done()
	return nil
}

// nextTick issues a "next" command to step the backtester engine.
func (e *EngineClient) nextTick() {
	if e.wsConn != nil {
		e.writeMu.Lock()
		defer e.writeMu.Unlock()
		e.wsConn.WriteJSON(map[string]string{"action": "next"})
	}
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

	payload := map[string]interface{}{
		"action":     "order",
		"request_id": reqID,
		"data": map[string]interface{}{
			"exchange": req.Exchange,
			"pair":     req.Symbol, // The engine schema wants Pair instead of Symbol
			"side":     req.Side,
			"type":     req.Type,
			"price":    req.Price,
			"quantity": req.Quantity,
			"asset":    strings.Split(req.Symbol, "/")[1], // Assuming symbol like BTC/USDT needs quote asset
		},
	}

	e.writeMu.Lock()
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

func (e *EngineClient) CancelOrder(ctx context.Context, orderID string) error {
	log.Printf("Backtest Engine: Canceling Order %s", orderID)
	return nil
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
	e.nextTick()
	return nil
}

type accountResponse struct {
	account *types.Account
	err     error
}
