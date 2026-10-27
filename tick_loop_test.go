package dev_sdk

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// tickAdapter answers each Next with a 1m candle, except the first silent
// ones, which it leaves unanswered like an engine gone quiet.
type tickAdapter struct {
	mockAdapter
	silent int

	mu      sync.Mutex
	calls   int
	candles chan<- *types.Candle
}

func (a *tickAdapter) PrepareSession(context.Context, *types.Config) error { return nil }

func (a *tickAdapter) ConnectStream(_ context.Context, candles chan<- *types.Candle, _ chan<- *types.Order) error {
	a.mu.Lock()
	a.candles = candles
	a.mu.Unlock()
	return nil
}

func (a *tickAdapter) Next(context.Context) error {
	a.mu.Lock()
	a.calls++
	n, candles := a.calls, a.candles
	a.mu.Unlock()
	if n <= a.silent {
		return nil
	}
	open := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(n) * time.Minute)
	go func() {
		candles <- &types.Candle{Symbol: "BTC/USDT", Exchange: "binance", Timeframe: types.Timeframe1m,
			OpenTime: open, CloseTime: open.Add(time.Minute - time.Millisecond), Open: 1, High: 1, Low: 1, Close: 1, IsComplete: true}
	}()
	return nil
}

func (a *tickAdapter) nexts() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls
}

// syncBuffer is a log destination the test can read while the SDK writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// shortTickTiming shortens the tick loop's timers and captures the log.
func shortTickTiming(t *testing.T) *syncBuffer {
	t.Helper()
	timeout, slow := nextTimeout, slowCallbackAfter
	nextTimeout, slowCallbackAfter = 20*time.Millisecond, 100*time.Millisecond
	logs := &syncBuffer{}
	log.SetOutput(logs)
	t.Cleanup(func() {
		nextTimeout, slowCallbackAfter = timeout, slow
		log.SetOutput(os.Stderr)
	})
	return logs
}

func newTickSDK(a *tickAdapter) *SDK {
	return &SDK{
		config: &types.Config{Environment: types.EnvBacktest, Timeframes: []types.Timeframe{types.Timeframe1m},
			Backtest: &types.BacktestOptions{}},
		adapter:       a,
		rawCandleChan: make(chan *types.Candle, 100),
		orderChan:     make(chan *types.Order, 100),
		reconciler:    newOrderReconciler(),
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A callback that has not returned holds the run: the engine answered, so
// another "next" would step past the strategy. A deadlock used to crawl one
// candle per timeout instead of stopping.
func TestSDK_WaitsForACallbackInsteadOfSteppingPastIt(t *testing.T) {
	logs := shortTickTiming(t)
	a := &tickAdapter{}
	s := newTickSDK(a)
	blocked, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s.SetOnCandle(func(*types.Context, *types.Candle) {
		once.Do(func() {
			close(blocked)
			<-release
		})
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Start(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	<-blocked
	time.Sleep(15 * nextTimeout) // well past the slow-callback warning too
	if n := a.nexts(); n != 1 {
		release <- struct{}{}
		t.Fatalf("%d next requests while the first candle's callback was running, want 1", n)
	}
	if got := strings.Count(logs.String(), "have not returned after"); got != 1 {
		t.Errorf("%d slow-callback warnings, want exactly 1:\n%s", got, logs.String())
	}
	if strings.Contains(logs.String(), "re-issuing next") {
		t.Errorf("re-issued next while the strategy was busy:\n%s", logs.String())
	}

	close(release)
	waitFor(t, "the next step after the callback returned", func() bool { return a.nexts() >= 3 })
}

// With nothing in the pipeline, silence is the engine's, and "next" is
// re-issued after the timeout.
func TestSDK_ReissuesNextWhenTheEngineIsSilent(t *testing.T) {
	logs := shortTickTiming(t)
	a := &tickAdapter{silent: 1}
	s := newTickSDK(a)
	got := make(chan struct{}, 10)
	s.SetOnCandle(func(*types.Context, *types.Candle) { got <- struct{}{} })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = s.Start(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("no candle after the engine went silent")
	}
	if !strings.Contains(logs.String(), "re-issuing next") {
		t.Errorf("no re-issue logged:\n%s", logs.String())
	}
}

// A progress frame that reports no new candles prints nothing: the engine
// sends one every 15 s, and repeated lines made a strategy stuck in a
// callback look busy to the cloud workspace's stall watchdog.
func TestSDK_LogsProgressOnlyWhenItMoved(t *testing.T) {
	logs := shortTickTiming(t)
	mock := newMockEngineServer()
	mock.progress = []int64{3, 3, 3, 4}
	defer mock.server.Close()

	s, err := New(newTruncationConfig(mock.server.URL))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := strings.Count(logs.String(), "Backtest progress:"); got != 2 {
		t.Errorf("%d progress lines for counts 3, 3, 3, 4, want 2:\n%s", got, logs.String())
	}
}
