package dev_sdk

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

func newTruncationConfig(endpoint string) *types.Config {
	return &types.Config{
		Environment: types.EnvBacktest,
		Timeframes:  []types.Timeframe{types.Timeframe1m},
		Backtest: &types.BacktestOptions{
			Endpoint:           endpoint,
			SessionName:        "Truncation-Test",
			RequestedExchanges: []string{"binance"},
			Assets:             []string{"BTC/USDT"},
			Wallets:            map[string]float64{"USDT": 100000.0},
		},
		Credentials: types.Credentials{
			KeyID:      "test-key",
			PrivateKey: "385d5c080a1b4140a5ed9ee76d0ef3fcd291cabab4ec6759bc178ad3a8ed837148309e3cb2a3a014c93d68b4f20a0ba5978ab300531c844dcec672925eb8d63a",
		},
	}
}

// TestSDK_TruncatedStream_ReportsError verifies that a dropped websocket mid-run
// surfaces as a terminal error: Start returns non-nil, onError fires, and
// onComplete does NOT fire (D1).
func TestSDK_TruncatedStream_ReportsError(t *testing.T) {
	mock := newMockEngineServer()
	mock.truncateAfter = 2 // drop the WS after 2 candles, before done:true
	defer mock.server.Close()

	sdk, err := New(newTruncationConfig(mock.server.URL))
	if err != nil {
		t.Fatalf("New SDK err: %v", err)
	}

	var mu sync.Mutex
	completeCalled := false
	var errReported error

	sdk.SetOnComplete(func() {
		mu.Lock()
		completeCalled = true
		mu.Unlock()
	})
	sdk.SetOnError(func(e error) {
		mu.Lock()
		errReported = e
		mu.Unlock()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	startErr := sdk.Start(ctx)

	mu.Lock()
	defer mu.Unlock()

	if startErr == nil {
		t.Fatal("expected Start to return a non-nil error on truncated stream, got nil")
	}
	if ctx.Err() != nil {
		t.Fatalf("test timed out waiting for Start to return; ctx err: %v", ctx.Err())
	}
	if completeCalled {
		t.Error("onComplete must NOT fire on a truncated stream")
	}
	if errReported == nil {
		t.Error("onError should have fired with the terminal error")
	}
}

// TestSDK_CleanFinish_ReportsComplete verifies the happy path is unchanged:
// a run reaching done:true fires onComplete, not onError, and Start returns nil.
func TestSDK_CleanFinish_ReportsComplete(t *testing.T) {
	mock := newMockEngineServer() // truncateAfter=0 → clean done:true after 5 candles
	defer mock.server.Close()

	sdk, err := New(newTruncationConfig(mock.server.URL))
	if err != nil {
		t.Fatalf("New SDK err: %v", err)
	}

	var mu sync.Mutex
	completeCalled := false
	var errReported error

	sdk.SetOnComplete(func() {
		mu.Lock()
		completeCalled = true
		mu.Unlock()
	})
	sdk.SetOnError(func(e error) {
		mu.Lock()
		errReported = e
		mu.Unlock()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	startErr := sdk.Start(ctx)

	mu.Lock()
	defer mu.Unlock()

	if startErr != nil {
		t.Fatalf("expected Start to return nil on clean finish, got %v", startErr)
	}
	if !completeCalled {
		t.Error("onComplete should have fired on a clean finish")
	}
	if errReported != nil {
		t.Errorf("onError must NOT fire on a clean finish, got %v", errReported)
	}
}
