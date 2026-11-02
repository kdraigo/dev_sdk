package dev_sdk

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// fillAdapter is an engine that, on every even step, fills the strategy's
// resting stop inside the bar: the fill and the candle come back in the same
// answer, the fill queued first, as the backtest adapter queues them. After
// steps candles the stream ends.
type fillAdapter struct {
	mockAdapter
	steps int

	mu      sync.Mutex
	n       int
	resting string
	nextID  int
	candles chan<- *types.Candle
	orders  chan<- *types.Order
}

func (a *fillAdapter) PrepareSession(context.Context, *types.Config) error { return nil }

func (a *fillAdapter) ConnectStream(_ context.Context, candles chan<- *types.Candle, orders chan<- *types.Order) error {
	a.mu.Lock()
	a.candles, a.orders = candles, orders
	a.mu.Unlock()
	return nil
}

func (a *fillAdapter) PlaceOrder(_ context.Context, req *types.OrderRequest) (*types.Order, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nextID++
	a.resting = fmt.Sprintf("stop-%d", a.nextID)
	return &types.Order{ID: a.resting, Exchange: req.Exchange, Symbol: req.Symbol, Side: req.Side, Type: req.Type, Status: types.OrderStatusNew}, nil
}

func (a *fillAdapter) Next(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.n++
	if a.n > a.steps {
		if a.n == a.steps+1 {
			close(a.candles)
		}
		return nil
	}
	if a.n%2 == 0 && a.resting != "" {
		a.orders <- &types.Order{ID: a.resting, Exchange: "binance", Symbol: "BTC/USDT", Side: types.OrderSideSell,
			Type: types.OrderTypeStopLoss, Status: types.OrderStatusFilled, FilledQty: 1, AveragePrice: 1}
		a.resting = ""
	}
	open := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(a.n) * time.Minute)
	a.candles <- &types.Candle{Symbol: "BTC/USDT", Exchange: "binance", Timeframe: types.Timeframe1m,
		OpenTime: open, CloseTime: open.Add(time.Minute - time.Millisecond), Open: 1, High: 1, Low: 1, Close: 1, IsComplete: true}
	return nil
}

// A strategy that keeps its position in OnOrderUpdate, as the RSI template
// does, sees a fill from inside a bar before that bar's OnCandle, every run.
// The two used to be dispatched on separate goroutines, so the same code on
// the same data traded differently from run to run (bug #16: two cloud runs
// of one workspace parted on the first bar where a stop filled).
func TestSDK_BacktestHandsFillsBeforeTheirBarEveryRun(t *testing.T) {
	const steps, runs = 120, 12
	want := make([]string, 0, steps*2)
	long := false
	for n := 1; n <= steps; n++ {
		if n%2 == 0 && long {
			want = append(want, "fill")
			long = false
		}
		want = append(want, fmt.Sprintf("candle %d long=%v", n, long))
		if !long {
			long = true
		}
	}

	for run := 0; run < runs; run++ {
		a := &fillAdapter{steps: steps}
		s := newTickSDK(&tickAdapter{})
		s.adapter = a
		s.config.Backtest.RequestedExchanges = []string{"binance"}
		s.config.Backtest.Assets = []string{"BTC/USDT"}

		var mu sync.Mutex
		var trace []string
		long := false
		s.SetOnOrderUpdate(func(_ *types.Context, o *types.Order) {
			if o.Status != types.OrderStatusFilled {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			trace = append(trace, "fill")
			long = false
		})
		n := 0
		s.SetOnCandle(func(ctx *types.Context, c *types.Candle) {
			mu.Lock()
			n++
			trace = append(trace, fmt.Sprintf("candle %d long=%v", n, long))
			place := !long
			if place {
				long = true
			}
			mu.Unlock()
			if place {
				if _, err := ctx.PlaceOrder(&types.OrderRequest{Exchange: "binance", Symbol: "BTC/USDT", Side: types.OrderSideSell,
					Type: types.OrderTypeStopLoss, Quantity: 1, StopPrice: 0.9}); err != nil {
					t.Errorf("place: %v", err)
				}
			}
		})

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		err := s.Start(ctx)
		cancel()
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		mu.Lock()
		got := strings.Join(trace, "\n")
		mu.Unlock()
		if got != strings.Join(want, "\n") {
			t.Fatalf("run %d differs from the expected order:\n%s", run, firstDiff(want, trace))
		}
	}
}

func firstDiff(want, got []string) string {
	for i := range want {
		if i >= len(got) {
			return fmt.Sprintf("ended after %d events; next wanted %q", len(got), want[i])
		}
		if want[i] != got[i] {
			return fmt.Sprintf("event %d: got %q, want %q", i, got[i], want[i])
		}
	}
	if len(got) > len(want) {
		return fmt.Sprintf("extra events from %d: %q", len(want), got[len(want)])
	}
	return "same"
}
