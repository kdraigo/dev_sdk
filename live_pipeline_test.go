package dev_sdk

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// candlePump is a live adapter that emits n one-minute candles as fast as it
// can, standing in for a venue's websocket.
type candlePump struct {
	spotOnlyAdapter
	n int
}

func (p candlePump) ConnectStream(ctx context.Context, candleChan chan<- *types.Candle, _ chan<- *types.Order) error {
	go func() {
		base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		for i := 0; i < p.n; i++ {
			open := base.Add(time.Duration(i) * time.Minute)
			select {
			case candleChan <- &types.Candle{
				Symbol: "BTCUSDT", Exchange: "binance", Timeframe: types.Timeframe1m,
				OpenTime: open, CloseTime: open.Add(time.Minute - time.Millisecond),
				Open: 100, High: 101, Low: 99, Close: 100.5, Volume: 1, IsComplete: true,
			}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (candlePump) OrderFeed() types.OrderFeed {
	return types.OrderFeed{Push: true}
}

// TestLiveSessionKeepsDispatchingCandles
//
// A live session used to dispatch exactly two candles and then go deaf for the
// rest of its life. syncChan is buffered at 1 and its only reader is the
// backtest ticking loop, so in live mode the second send blocked the aggregator
// goroutine permanently — websockets still connected, no error, no log line,
// the venue still sending. It was found by watching a live futures session sit
// at candles=2 while its per-minute pulse kept ticking.
//
// Two is the magic number: one send fills the buffer, the next blocks. Any
// assertion below three would have passed against the bug.
func TestLiveSessionKeepsDispatchingCandles(t *testing.T) {
	const emitted = 30

	cfg := &types.Config{
		Environment: types.EnvRealBinance,
		Timeframes:  []types.Timeframe{types.Timeframe1m},
		Live: &types.LiveOptions{
			RequestedExchanges: []string{"binance"},
			Assets:             []string{"BTC/USDT"},
		},
	}
	s, err := New(cfg)
	require.NoError(t, err)
	s.adapter = candlePump{n: emitted}

	var seen atomic.Int64
	s.SetOnCandle(func(_ *types.Context, _ *types.Candle) { seen.Add(1) })

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	_ = s.Start(ctx)

	got := seen.Load()
	assert.Greater(t, got, int64(2),
		"a live session stopped at 2 candles: syncChan filled and the aggregator goroutine blocked forever")
	// The aggregator emits a bar when the next one opens, so the final candle
	// is still in flight — everything before it must have been dispatched.
	assert.GreaterOrEqual(t, got, int64(emitted-1),
		"every closed candle the venue sent must reach the strategy")
}

// TestBacktestStillPacesThroughSyncChan guards the other side of the fix: the
// signal must survive in backtest, where the tick loop depends on it to request
// the next bar. Removing it outright would silently stall every backtest.
func TestBacktestStillPacesThroughSyncChan(t *testing.T) {
	raw, err := os.ReadFile("client.go")
	require.NoError(t, err)
	src := string(raw)
	assert.Contains(t, src, "if s.config.Environment == types.EnvBacktest {\n\t\t\t\tsyncChan <- true",
		"the pacing signal must remain, gated to backtest rather than deleted")
}
