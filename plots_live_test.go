package dev_sdk

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/plots"
	"github.com/kdraigo/dev_sdk/telemetry"
	"github.com/kdraigo/dev_sdk/types"
)

// fakePlotPublisher records what arrives; fail makes the next calls fail.
type fakePlotPublisher struct {
	mu      sync.Mutex
	batches []*plots.Batch
	fail    error
	calls   int
}

func (f *fakePlotPublisher) PublishPlots(_ context.Context, b *plots.Batch) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail != nil {
		return "", f.fail
	}
	f.batches = append(f.batches, b)
	return "", nil
}

func (f *fakePlotPublisher) points() (n int, defs int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, b := range f.batches {
		n += len(b.Points)
		defs += len(b.Defs)
	}
	return
}

func (f *fakePlotPublisher) setFail(err error) {
	f.mu.Lock()
	f.fail = err
	f.mu.Unlock()
}

var fastTiming = livePlotTiming{
	every: 5 * time.Millisecond, maxBackoff: 20 * time.Millisecond, redeclare: time.Hour, finalWait: time.Second,
}

// runLoop runs the loop until the test ends, and returns a stop that ends it
// and waits until it has returned (and done its last flush).
func runLoop(t *testing.T, s *SDK, pub *fakePlotPublisher, tm livePlotTiming) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.sendPlotsLoop(ctx, pub, tm) }()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

func liveSDK(t *testing.T) (*SDK, *plots.Series) {
	t.Helper()
	s := &SDK{plots: plots.NewRecorder()}
	s.plots.Activate()
	ema, err := s.Plot(plots.Def{Key: "ema", Timeframe: types.Timeframe1h})
	if err != nil {
		t.Fatal(err)
	}
	return s, ema
}

func addN(ema *plots.Series, from, n int) {
	for i := from; i < from+n; i++ {
		ema.AddAt("binance_futures", "BTC/USDT", time.Unix(int64(i)*3600, 0), float64(i))
	}
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestLivePlots_SentInBatchesAndOnShutdown(t *testing.T) {
	s, ema := liveSDK(t)
	pub := &fakePlotPublisher{}
	stop := runLoop(t, s, pub, fastTiming)

	addN(ema, 0, 30)
	eventually(t, "the first 30", func() bool { n, _ := pub.points(); return n == 30 })
	if _, defs := pub.points(); defs != 1 {
		t.Fatalf("declarations sent %d times, want once", defs)
	}

	// Added just before shutdown: the last flush still sends it.
	addN(ema, 30, 5)
	stop()
	if n, _ := pub.points(); n != 35 {
		t.Fatalf("sent %d values, want 35", n)
	}
}

func TestLivePlots_KeepsWhatTheOutageStopped(t *testing.T) {
	s, ema := liveSDK(t)
	pub := &fakePlotPublisher{}
	pub.setFail(fmt.Errorf("connection refused"))
	runLoop(t, s, pub, fastTiming)

	addN(ema, 0, 10)
	eventually(t, "a few failed attempts", func() bool { pub.mu.Lock(); defer pub.mu.Unlock(); return pub.calls >= 3 })
	pub.setFail(nil)
	eventually(t, "every value after the outage", func() bool { n, _ := pub.points(); return n == 10 })
}

func TestLivePlots_DropsWhatIsRefused(t *testing.T) {
	s, ema := liveSDK(t)
	pub := &fakePlotPublisher{}
	pub.setFail(fmt.Errorf("%w: status 413", telemetry.ErrPlotsRejected))
	runLoop(t, s, pub, fastTiming)

	addN(ema, 0, 10)
	eventually(t, "the refusal", func() bool { pub.mu.Lock(); defer pub.mu.Unlock(); return pub.calls >= 1 })
	pub.setFail(nil)
	addN(ema, 10, 1)
	eventually(t, "the next value", func() bool { n, _ := pub.points(); return n == 1 })
	time.Sleep(30 * time.Millisecond)
	if n, _ := pub.points(); n != 1 {
		t.Fatalf("the refused batch came back: %d values", n)
	}
}

func TestLivePlots_RedeclaresNowAndThen(t *testing.T) {
	s, _ := liveSDK(t)
	pub := &fakePlotPublisher{}
	tm := fastTiming
	tm.redeclare = 10 * time.Millisecond
	runLoop(t, s, pub, tm)
	eventually(t, "the declaration again", func() bool { _, defs := pub.points(); return defs >= 2 })
}

// New activates plots in live mode only when telemetry can carry them.
func TestNew_LivePlotsNeedTelemetry(t *testing.T) {
	cfg := func(url string) *types.Config {
		return &types.Config{
			Environment: types.EnvTestBinanceFutures,
			Timeframes:  []types.Timeframe{types.Timeframe1h},
			Live: &types.LiveOptions{
				RequestedExchanges: []string{"binance_futures"}, Assets: []string{"BTC/USDT"},
				TelemetryURL: url,
			},
		}
	}
	with, err := New(cfg("http://localhost:5001"))
	if err != nil {
		t.Fatal(err)
	}
	if !with.plots.Active() {
		t.Error("plots inactive with telemetry configured")
	}
	without, err := New(cfg(""))
	if err != nil {
		t.Fatal(err)
	}
	if without.plots.Active() {
		t.Error("plots active with nowhere to send them")
	}
	if _, ok := with.publisher.(telemetry.PlotPublisher); !ok {
		t.Error("the HTTP publisher does not carry plots")
	}
}
