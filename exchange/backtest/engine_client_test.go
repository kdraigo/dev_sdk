package backtest

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/signing"
	"github.com/kdraigo/dev_sdk/types"
)

func TestEngineClient_PrepareSession(t *testing.T) {
	// 1. Create a test server to mock the engine
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/api/v1/dev/session" {
			t.Errorf("Expected path /api/v1/dev/session, got %s", r.URL.Path)
		}

		// Verify API-KEY header
		if r.Header.Get("X-API-KEY") == "" {
			t.Error("Missing X-API-KEY header")
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
		Credentials: types.Credentials{
			KeyID:      "test-key",
			PrivateKey: "385d5c080a1b4140a5ed9ee76d0ef3fcd291cabab4ec6759bc178ad3a8ed837148309e3cb2a3a014c93d68b4f20a0ba5978ab300531c844dcec672925eb8d63a", // Dummy test key
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

// Requests to the engine use signing version 2: the body and the WebSocket's
// ?id= are signed, every request has its own nonce, and no credential travels
// in a URL, where access logs would keep it.
func TestEngineClient_SignsV2(t *testing.T) {
	const keyHex = "385d5c080a1b4140a5ed9ee76d0ef3fcd291cabab4ec6759bc178ad3a8ed837148309e3cb2a3a014c93d68b4f20a0ba5978ab300531c844dcec672925eb8d63a"
	raw, _ := hex.DecodeString(keyHex)
	pub := ed25519.PrivateKey(raw).Public().(ed25519.PublicKey)

	verify := func(r *http.Request, body []byte) {
		t.Helper()
		for _, k := range []string{"key_id", "signature", "timestamp", "nonce"} {
			if r.URL.Query().Has(k) {
				t.Errorf("%s %s: credential %q in the URL", r.Method, r.URL.Path, k)
			}
		}
		sig, _ := hex.DecodeString(r.Header.Get("X-Signature"))
		canonical := signing.Canonical(r.Method, r.URL.Path, r.URL.Query(), r.Header.Get("X-Timestamp"), r.Header.Get("X-Nonce"), body)
		if r.Header.Get("X-API-KEY") != "test-key" || len(r.Header.Get("X-Nonce")) != 32 || !ed25519.Verify(pub, []byte(canonical), sig) {
			t.Errorf("%s %s: not a valid version 2 signature", r.Method, r.URL.Path)
		}
	}
	var dialed bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/dev/session":
			body, _ := io.ReadAll(r.Body)
			verify(r, body)
			json.NewEncoder(w).Encode(sessionResponse{ID: "sess-1"})
		case "/api/v1/dev/session/ws":
			if r.URL.Query().Get("id") != "sess-1" {
				t.Errorf("ws id %q", r.URL.Query().Get("id"))
			}
			verify(r, nil)
			dialed = true
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "Session not found", "code": "session_unknown"})
		}
	}))
	defer ts.Close()

	cfg := &types.Config{
		Timeframes: []types.Timeframe{types.Timeframe15m},
		Backtest: &types.BacktestOptions{
			Endpoint: ts.URL, RequestedExchanges: []string{"binance"}, Assets: []string{"BTC/USDT"},
			StartTime: time.Now(), EndTime: time.Now().Add(time.Hour), Wallets: map[string]float64{"USDT": 1000},
		},
		Credentials: types.Credentials{KeyID: "test-key", PrivateKey: keyHex},
	}
	client := NewEngineClient(cfg)
	if err := client.PrepareSession(context.Background(), cfg); err != nil {
		t.Fatalf("PrepareSession: %v", err)
	}
	if _, err := client.dialOnce(context.Background()); err == nil {
		t.Fatal("the fake engine refuses the WebSocket after checking it")
	}
	if !dialed {
		t.Fatal("the WebSocket was never dialed")
	}
}
