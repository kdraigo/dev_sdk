package dev_sdk

import (
	"context"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/plots"
	"github.com/kdraigo/dev_sdk/types"
)

// runPlotting drives a backtest whose strategy adds one value per candle, plus
// extra values on the first candle, and returns what the engine stored.
func runPlotting(t *testing.T, mock *mockEngineServer, extra int) ([]plots.Def, []plots.Point, int) {
	t.Helper()
	sdk, err := New(newTruncationConfig(mock.server.URL))
	if err != nil {
		t.Fatal(err)
	}
	ema, err := sdk.Plot(plots.Def{Key: "ema", Title: "EMA 3"})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	sdk.SetOnCandle(func(ctx *types.Context, c *types.Candle) {
		n++
		ema.Add(c, float64(n))
		if n == 1 {
			for i := range extra {
				ema.AddAt(c.Exchange, c.Symbol, c.OpenTime.Add(-time.Duration(i+1)*time.Minute), -float64(i+1))
			}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := sdk.Start(ctx); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	return mock.plotStats()
}

// Each candle's value reaches the engine once, declared, with its timeframe.
func TestPlots_RideOnTicks(t *testing.T) {
	mock := newMockEngineServer()
	defer mock.server.Close()

	defs, points, frames := runPlotting(t, mock, 0)
	if len(points) != 5 {
		t.Fatalf("engine stored %d points, want 5: %+v", len(points), points)
	}
	for i, p := range points {
		if p.Value != float64(i+1) || p.Key != "ema" || p.Exchange != "binance" || p.Pair != "BTC/USDT" {
			t.Fatalf("point %d = %+v", i, p)
		}
	}
	if frames != 0 {
		t.Fatalf("%d separate plot frames; ticks should carry these", frames)
	}
	last := defs[len(defs)-1]
	if last.Key != "ema" || last.Timeframe != types.Timeframe1m || last.Title != "EMA 3" {
		t.Fatalf("declaration %+v", last)
	}
}

// A tick lost with a dropped connection is re-sent with the same plots, and
// the engine's replay of a served tick stores nothing twice.
func TestPlots_ExactlyOnceAcrossReconnect(t *testing.T) {
	mock := newMockEngineServer()
	mock.dropAfter = 2
	defer mock.server.Close()

	_, points, _ := runPlotting(t, mock, 0)
	seen := map[float64]int{}
	for _, p := range points {
		seen[p.Value]++
	}
	for v := 1.0; v <= 5; v++ {
		if seen[v] != 1 {
			t.Fatalf("value %v stored %d times: %+v", v, seen[v], points)
		}
	}
}

// More than ticks carry: the rest is sent before close, and nothing is lost.
func TestPlots_BacklogFlushedAtEnd(t *testing.T) {
	mock := newMockEngineServer()
	defer mock.server.Close()

	const extra = 3000
	_, points, frames := runPlotting(t, mock, extra)
	if len(points) != 5+extra {
		t.Fatalf("engine stored %d points, want %d", len(points), 5+extra)
	}
	if frames == 0 {
		t.Fatal("expected the backlog to need plot frames at the end")
	}
	seen := map[float64]bool{}
	for _, p := range points {
		if seen[p.Value] {
			t.Fatalf("value %v stored twice", p.Value)
		}
		seen[p.Value] = true
	}
}
