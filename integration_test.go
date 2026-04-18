package dev_sdk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

var upgrader = websocket.Upgrader{}

type mockEngineServer struct {
	server      *httptest.Server
	mu          sync.Mutex
	nextCalls   int
	orders      int
	accountReqs int
	candlesSent int
}

func newMockEngineServer() *mockEngineServer {
	m := &mockEngineServer{}
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/ws") {
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer conn.Close()
			m.handleWS(conn)
		} else if strings.HasPrefix(r.URL.Path, "/api/v1/dev/session") {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "ok",
				"id":     "test-session-id",
			})
		}
	}))
	return m
}

func (m *mockEngineServer) handleWS(conn *websocket.Conn) {
	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var req struct {
			Action    string          `json:"action"`
			RequestID string          `json:"request_id"`
			Data      json.RawMessage `json:"data"`
		}
		json.Unmarshal(message, &req)

		m.mu.Lock()
		switch req.Action {
		case "account":
			m.accountReqs++
			resp := map[string]interface{}{
				"action":     "account",
				"request_id": req.RequestID,
				"status":     "ok",
				"data": map[string]interface{}{
					"exchange": "binance",
					"balances": []map[string]interface{}{
						{"asset": "USDT", "free": 100000.0, "locked": 0.0},
					},
				},
			}
			conn.WriteJSON(resp)

		case "order":
			m.orders++
			resp := map[string]interface{}{
				"action":     "order",
				"request_id": req.RequestID,
				"status":     "ok",
				"data": map[string]interface{}{
					"id":          m.orders,
					"exchange_id": m.orders,
					"status":      "FILLED",
					"price":       50000.0,
					"quantity":    0.1,
				},
			}
			conn.WriteJSON(resp)

		case "next":
			m.nextCalls++
			if m.nextCalls > 5 {
				// Finish after 5 candles
				conn.WriteJSON(map[string]interface{}{
					"action": "next",
					"status": "ok",
					"data": map[string]interface{}{
						"done": true,
					},
				})
			} else {
				m.candlesSent++
				conn.WriteJSON(map[string]interface{}{
					"action": "next",
					"status": "ok",
					"data": map[string]interface{}{
						"tick": map[string]interface{}{
							"Exchange": "binance",
							"Pair":     "BTC/USDT",
							"Candle": map[string]interface{}{
								"time":      time.Now(),
								"updatedAt": time.Now(),
								"open":      50000.0,
								"high":      51000.0,
								"low":       49000.0,
								"close":     50500.0,
								"volume":    1.0,
								"complete":  true,
							},
						},
						"done": false,
					},
				})
			}
		}
		m.mu.Unlock()
	}
}

func TestSDK_Integration_Flow(t *testing.T) {
	mock := newMockEngineServer()
	defer mock.server.Close()

	// Adjust endpoint to mock server
	config := &types.Config{
		Environment: types.EnvBacktest,
		Timeframes:  []types.Timeframe{types.Timeframe1m}, // Fast test
		Backtest: &types.BacktestOptions{
			Endpoint:           mock.server.URL,
			SessionName:        "Test-Session",
			RequestedExchanges: []string{"binance"},
			Assets:             []string{"BTC/USDT"},
			Wallets:            map[string]float64{"USDT": 100000.0},
		},
		Credentials: types.Credentials{
			KeyID:      "test-key",
			PrivateKey: "385d5c080a1b4140a5ed9ee76d0ef3fcd291cabab4ec6759bc178ad3a8ed837148309e3cb2a3a014c93d68b4f20a0ba5978ab300531c844dcec672925eb8d63a",
		},
	}

	sdk, err := New(config)
	if err != nil {
		t.Fatalf("New SDK err: %v", err)
	}

	// Override adapter endpoint for WebSocket
	// In the real system, PrepareSession sets this up, but here we've pointed it to httptest.

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var candlesReceived int
	var ordersFilled int
	var mu sync.Mutex

	sdk.SetOnCandle(func(sdkCtx *types.Context, candle *types.Candle) {
		mu.Lock()
		candlesReceived++
		mu.Unlock()

		// Place an order on the 2nd candle
		if candlesReceived == 2 {
			req := &types.OrderRequest{
				Symbol:   candle.Symbol,
				Exchange: candle.Exchange,
				Side:     types.OrderSideBuy,
				Type:     types.OrderTypeMarket,
				Quantity: 0.1,
			}
			_, err := sdkCtx.PlaceOrder(req)
			if err != nil {
				t.Errorf("PlaceOrder err: %v", err)
			}
		}
	})

	sdk.SetOnOrderUpdate(func(sdkCtx *types.Context, order *types.Order) {
		if order.Status == types.OrderStatusFilled {
			mu.Lock()
			ordersFilled++
			mu.Unlock()
		}
	})

	// Run SDK
	err = sdk.Start(ctx)
	if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		t.Fatalf("SDK Start err: %v", err)
	}

	// Assertions
	mu.Lock()
	defer mu.Unlock()

	if candlesReceived != 5 {
		t.Errorf("Expected 5 candles, got %d", candlesReceived)
	}
	if ordersFilled != 1 {
		t.Errorf("Expected 1 order fill, got %d", ordersFilled)
	}
	if mock.accountReqs < 1 {
		t.Errorf("Expected at least 1 account handshake, got %d", mock.accountReqs)
	}
	if mock.nextCalls != 6 {
		t.Errorf("Expected exactly 6 Next() calls (1 initial + 5 candles), got %d", mock.nextCalls)
	}
}
