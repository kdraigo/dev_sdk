package dev_sdk

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

func newResumeConfig(endpoint, sessionID string) *types.Config {
	cfg := newTruncationConfig(endpoint)
	cfg.Backtest.SessionName = "Resume-Test"
	cfg.Backtest.SessionID = sessionID
	return cfg
}

// runResume drives a backtest to completion (or failure) and reports what the
// strategy saw.
func runResume(t *testing.T, cfg *types.Config, onResume func(*types.Context, *types.SessionState)) (candles int, completed bool, runErr error) {
	t.Helper()

	sdk, err := New(cfg)
	if err != nil {
		t.Fatalf("New SDK err: %v", err)
	}

	var mu sync.Mutex
	sdk.SetOnCandle(func(ctx *types.Context, c *types.Candle) {
		mu.Lock()
		candles++
		mu.Unlock()
	})
	sdk.SetOnComplete(func() {
		mu.Lock()
		completed = true
		mu.Unlock()
	})
	if onResume != nil {
		sdk.SetOnResume(onResume)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	runErr = sdk.Start(ctx)

	mu.Lock()
	defer mu.Unlock()
	return candles, completed, runErr
}

// The core of the feature: a connection that drops mid-run is redialed, the
// session resumes where it stopped, and the run finishes normally.
func TestSDK_ReconnectResumesStream(t *testing.T) {
	mock := newMockEngineServer()
	mock.dropAfter = 2 // drop once, then stay resumable
	defer mock.server.Close()

	candles, completed, err := runResume(t, newTruncationConfig(mock.server.URL), nil)
	if err != nil {
		t.Fatalf("expected the run to survive a dropped connection, got: %v", err)
	}
	if !completed {
		t.Fatal("expected onComplete to fire after a successful resume")
	}
	if candles != 5 {
		t.Fatalf("expected all 5 candles exactly once, got %d", candles)
	}

	conns, posts, seqs, closed := mock.stats()
	if conns < 2 {
		t.Fatalf("expected a redial, saw %d connection(s)", conns)
	}
	if posts != 1 {
		t.Fatalf("a resume must not create a new session; saw %d POSTs", posts)
	}
	_ = closed

	// The tick lost with the connection must have been re-requested under its
	// original sequence rather than skipped.
	seen := map[uint64]int{}
	for _, s := range seqs {
		seen[s]++
	}
	repeated := false
	for _, n := range seen {
		if n > 1 {
			repeated = true
		}
	}
	if !repeated {
		t.Fatalf("expected the in-flight sequence to be re-requested, saw %v", seqs)
	}
}

// Sequences must never skip. A gap would mean a candle the engine served and
// the strategy never saw, with nothing recording that it had.
func TestSDK_ReconnectRequestsContiguousSequences(t *testing.T) {
	mock := newMockEngineServer()
	mock.dropAfter = 3
	defer mock.server.Close()

	if _, _, err := runResume(t, newTruncationConfig(mock.server.URL), nil); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	_, _, seqs, _ := mock.stats()
	if len(seqs) == 0 {
		t.Fatal("no sequences recorded — the client is not sending them")
	}

	var highest uint64
	for _, s := range seqs {
		if s == 0 {
			t.Fatalf("an unsequenced tick was sent: %v", seqs)
		}
		if s > highest+1 {
			t.Fatalf("sequence jumped from %d to %d: %v", highest, s, seqs)
		}
		if s > highest {
			highest = s
		}
	}
}

// A cold resume attaches to a session this process did not create: no POST is
// made, and the strategy is handed the engine's view before any candle.
func TestSDK_ColdResume(t *testing.T) {
	mock := newMockEngineServer()
	defer mock.server.Close()

	var mu sync.Mutex
	var state *types.SessionState
	var sawCandleBeforeResume bool
	resumeFired := false

	cfg := newResumeConfig(mock.server.URL, "test-session-id")
	sdk, err := New(cfg)
	if err != nil {
		t.Fatalf("New SDK err: %v", err)
	}
	sdk.SetOnResume(func(ctx *types.Context, s *types.SessionState) {
		mu.Lock()
		defer mu.Unlock()
		resumeFired = true
		state = s
	})
	sdk.SetOnCandle(func(ctx *types.Context, c *types.Candle) {
		mu.Lock()
		defer mu.Unlock()
		if !resumeFired {
			sawCandleBeforeResume = true
		}
		// The timeframe must survive a resume: it is what the aggregators and
		// indicator managers are keyed by, and an empty one makes the run
		// silently deaf.
		if c.Timeframe == "" {
			t.Errorf("resumed candle carries no timeframe")
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := sdk.Start(ctx); err != nil {
		t.Fatalf("cold resume failed: %v", err)
	}

	_, posts, _, _ := mock.stats()
	if posts != 0 {
		t.Fatalf("a cold resume must not create a session; saw %d POSTs", posts)
	}

	mu.Lock()
	defer mu.Unlock()
	if !resumeFired {
		t.Fatal("OnResume did not fire on a cold resume")
	}
	if sawCandleBeforeResume {
		t.Fatal("a candle reached the strategy before OnResume — indicators could not have been warmed")
	}
	if state == nil || !state.ColdStart {
		t.Fatalf("expected ColdStart to be set, got %+v", state)
	}
}

// A resume that finds the session gone must fail loudly. Silently starting a
// fresh session would hand back results the caller reads as a continuation of
// a run that no longer exists.
func TestSDK_ResumeUnknownSession_IsFatal(t *testing.T) {
	mock := newMockEngineServer()
	mock.refuse = true
	defer mock.server.Close()

	_, completed, err := runResume(t, newResumeConfig(mock.server.URL, "gone-session"), nil)
	if err == nil {
		t.Fatal("expected an error when the session no longer exists")
	}
	if completed {
		t.Fatal("onComplete must not fire when the session is gone")
	}

	_, posts, _, _ := mock.stats()
	if posts != 0 {
		t.Fatalf("the SDK must not silently start a fresh session; saw %d POSTs", posts)
	}
}

// An engine that predates the session_state frame must still work: it sends
// nothing on attach, because the client has not asked for anything yet, and
// waiting the full read timeout for it would stall every connect.
func TestSDK_WorksAgainstEngineWithoutSessionState(t *testing.T) {
	mock := newMockEngineServer()
	mock.suppressSessionState = true
	defer mock.server.Close()

	start := time.Now()
	candles, completed, err := runResume(t, newTruncationConfig(mock.server.URL), nil)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("run against an older engine failed: %v", err)
	}
	if !completed || candles != 5 {
		t.Fatalf("expected a clean 5-candle run, got %d candles completed=%v", candles, completed)
	}
	// Generous, but it must not be the 60s read timeout.
	if elapsed > 20*time.Second {
		t.Fatalf("connect stalled waiting for a frame that never comes: %s", elapsed)
	}
}
