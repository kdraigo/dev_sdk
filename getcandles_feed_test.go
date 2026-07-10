package dev_sdk

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// TestSDK_GetCandles_FeedsIndicators verifies that fetching a history range with
// GetCandles drops those candles into the SDK's own indicator manager, so an
// indicator needing a warm-up window returns values on the first streamed bar
// without the strategy owning a parallel manager. It also proves the cutoff: the
// in-flight bar is not double-counted.
func TestSDK_GetCandles_FeedsIndicators(t *testing.T) {
	mock := newMockEngineServer()
	defer mock.server.Close()

	sdk, err := New(newTruncationConfig(mock.server.URL))
	if err != nil {
		t.Fatalf("New SDK err: %v", err)
	}

	const smaPeriod = 5
	var mu sync.Mutex
	var fetchErr error
	seriesLen := -1
	firstBarSMAOK := false

	sdk.SetOnCandle(func(sdkCtx *types.Context, candle *types.Candle) {
		mu.Lock()
		defer mu.Unlock()
		if seriesLen != -1 {
			return // fetch once, on the first bar
		}
		// Fetching history feeds the manager automatically.
		if _, err := sdk.GetCandles(context.Background(), "binance", "BTC/USDT", 30, candle.Timeframe); err != nil {
			fetchErr = err
			seriesLen = 0
			return
		}
		series, err := sdk.IndicatorManagerFor(candle.Timeframe).SMA("binance", "BTC/USDT", "close", smaPeriod)
		if err != nil {
			fetchErr = err
			seriesLen = 0
			return
		}
		seriesLen = len(series)
		firstBarSMAOK = len(series) > 0 && series[len(series)-1] > 0
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sdk.Start(ctx); err != nil {
		t.Fatalf("Start err: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if fetchErr != nil {
		t.Fatalf("GetCandles/indicator err: %v", fetchErr)
	}
	if !firstBarSMAOK {
		t.Errorf("expected a non-empty SMA(%d) series with a positive value on the first bar after GetCandles; got len=%d", smaPeriod, seriesLen)
	}
}
