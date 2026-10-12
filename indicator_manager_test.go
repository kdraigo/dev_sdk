package dev_sdk

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// TestSDK_IndicatorManagerFor_BeforeStart covers the pattern the website docs
// showed for months: take the manager once, before Start, and use it in the
// callback. Before v1.3.1 the managers were built inside Start, so the captured
// value was nil and the first indicator call panicked.
func TestSDK_IndicatorManagerFor_BeforeStart(t *testing.T) {
	mock := newMockEngineServer()
	defer mock.server.Close()

	cfg := newTruncationConfig(mock.server.URL)
	sdk, err := New(cfg)
	if err != nil {
		t.Fatalf("New SDK err: %v", err)
	}

	im := sdk.IndicatorManagerFor(types.Timeframe1m)
	if im == nil {
		t.Fatal("IndicatorManagerFor returned nil before Start")
	}

	// A config edited between New and Start is caught up in place.
	cfg.Backtest.Assets = append(cfg.Backtest.Assets, "ETH/USDT")

	var mu sync.Mutex
	calls := 0
	sameInstance := true
	sdk.SetOnCandleFor(types.Timeframe1m, func(ctx *types.Context, c *types.Candle) {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if sdk.IndicatorManagerFor(types.Timeframe1m) != im {
			sameInstance = false
		}
		// Must not panic; an error just means the warm-up is not done.
		_, _ = im.SMA(c.Exchange, c.Symbol, "close", 2)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sdk.Start(ctx); err != nil {
		t.Fatalf("Start err: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if calls == 0 {
		t.Fatal("no candle reached the callback")
	}
	if !sameInstance {
		t.Error("Start replaced the indicator manager the strategy captured before it")
	}

	// The asset added after New is tracked by the captured manager.
	feed := sdk.indManagers[types.Timeframe1m]
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		feed.Update(&types.Candle{Exchange: "binance", Symbol: "ETH/USDT", OpenTime: base.Add(time.Duration(i) * time.Minute), Close: float64(i + 1)})
	}
	if vals, err := im.SMA("binance", "ETH/USDT", "close", 2); err != nil || len(vals) != 3 {
		t.Errorf("ETH/USDT added after New: SMA len=%d err=%v, want 3 values", len(vals), err)
	}
}
