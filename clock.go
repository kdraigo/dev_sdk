package dev_sdk

import (
	"sync"
	"time"
)

// Clock returns the current time in the strategy's frame of reference.
//
// In live mode the clock returns wall time. In backtest mode it returns the
// close time of the most recently dispatched candle, so strategies see a
// monotonically advancing simulated clock instead of real time.
type Clock interface {
	Now() time.Time
}

// wallClock returns real time.
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// backtestClock returns simulated time. It is initialized to the session start
// time (so sdk.Now() is meaningful before the first candle is dispatched) and
// is advanced strictly monotonically as candles flow through the SDK.
type backtestClock struct {
	mu  sync.RWMutex
	cur time.Time
}

func newBacktestClock(start time.Time) *backtestClock {
	return &backtestClock{cur: start}
}

func (b *backtestClock) Now() time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.cur
}

// Advance moves the clock forward to t if t is after the current time.
// Calls with earlier or equal times are ignored.
func (b *backtestClock) Advance(t time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if t.After(b.cur) {
		b.cur = t
	}
}
