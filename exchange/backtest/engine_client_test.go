package backtest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

func TestEngineClient_PrepareSession(t *testing.T) {
	// 1. Create a test server to mock the engine
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/session" {
			t.Errorf("Expected path /session, got %s", r.URL.Path)
		}

		// Verify User-ID header
		if r.Header.Get("X-User-ID") == "" {
			t.Error("Missing X-User-ID header")
		}

		// Decode payload
		var payload newSessionRequestPayload
		err := json.NewDecoder(r.Body).Decode(&payload)
		if err != nil {
			t.Fatalf("Failed to decode payload: %v", err)
		}

		if len(payload.Streams) == 0 {
			t.Error("Expected at least one stream")
		}

		// Return success
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(sessionResponse{ID: "test-session-id"})
	}))
	defer ts.Close()

	// 2. Configure Client
	cfg := &types.Config{
		Timeframes: []types.Timeframe{types.Timeframe15m},
		Backtest: &types.BacktestOptions{
			Endpoint:           ts.URL,
			SessionName:        "Test-Session",
			RequestedExchanges: []string{"binance"},
			Assets:             []string{"BTC/USDT"},
			StartTime:          time.Now(),
			EndTime:            time.Now().Add(time.Hour),
			Wallets:            map[string]float64{"USDT": 1000},
		},
	}

	client := NewEngineClient(cfg)
	err := client.PrepareSession(context.Background(), cfg)
	if err != nil {
		t.Fatalf("PrepareSession failed: %v", err)
	}

	if client.sessionID != "test-session-id" {
		t.Errorf("Expected sessionID test-session-id, got %s", client.sessionID)
	}
}
