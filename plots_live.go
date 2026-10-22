package dev_sdk

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/kdraigo/dev_sdk/telemetry"
)

// Live plots. A backtest sends plots with its ticks (see the engine client);
// a live session has no ticks, so a loop posts what the strategy added through
// the telemetry publisher. Batching on a timer rather than per candle is what
// keeps a portfolio cheap: thirty pairs closing their hour together go out as
// one signed request, not thirty.

// livePlotTiming paces the loop. A value, not package variables, so tests can
// run it fast without racing each other.
type livePlotTiming struct {
	// every is how often waiting values are sent.
	every time.Duration
	// maxBackoff caps the wait between attempts while the platform cannot be
	// reached.
	maxBackoff time.Duration
	// redeclare re-sends every declaration, so one lost on the way (or a
	// platform that lost it) is back within this long.
	redeclare time.Duration
	// finalWait bounds the last flush at shutdown.
	finalWait time.Duration
}

var defaultLivePlotTiming = livePlotTiming{
	every:      2 * time.Second,
	maxBackoff: time.Minute,
	redeclare:  10 * time.Minute,
	finalWait:  5 * time.Second,
}

const (
	// livePlotBytes bounds one request's batch, well under the gateway's
	// 1 MB body limit.
	livePlotBytes = 256 << 10
	// livePlotBatches bounds how many requests one flush sends, so a long
	// backlog drains over several flushes instead of in one burst.
	livePlotBatches = 8
)

// sendPlotsLoop posts the strategy's plots until ctx ends, then once more.
func (s *SDK) sendPlotsLoop(ctx context.Context, pub telemetry.PlotPublisher, tm livePlotTiming) {
	f := &livePlotFlusher{sdk: s, pub: pub, warned: map[string]bool{}}
	wait := tm.every
	timer := time.NewTimer(wait)
	defer timer.Stop()
	declared := time.Now()

	for {
		select {
		case <-ctx.Done():
			final, cancel := context.WithTimeout(context.Background(), tm.finalWait)
			f.flush(final, 64)
			cancel()
			return
		case <-timer.C:
		}
		if time.Since(declared) >= tm.redeclare {
			s.plots.Redeclare()
			declared = time.Now()
		}
		if f.flush(ctx, livePlotBatches) {
			wait = tm.every
		} else {
			wait = min(2*wait, tm.maxBackoff)
		}
		timer.Reset(wait)
	}
}

type livePlotFlusher struct {
	sdk     *SDK
	pub     telemetry.PlotPublisher
	failing bool
	warned  map[string]bool
}

// flush sends at most n batches. It reports false when the platform could not
// be reached; the batch is kept for the next attempt. A refused batch is
// dropped, since sending it again cannot succeed.
func (f *livePlotFlusher) flush(ctx context.Context, n int) bool {
	for range n {
		b := f.sdk.plots.Take(livePlotBytes)
		if b == nil {
			break
		}
		warning, err := f.pub.PublishPlots(ctx, b)
		switch {
		case errors.Is(err, telemetry.ErrPlotsRejected):
			f.warnOnce("plots: the platform refused a batch, dropped: " + err.Error())
		case err != nil:
			f.sdk.plots.Return(b)
			if !f.failing {
				log.Printf("plots: not sent, retrying with backoff: %v", err)
				f.failing = true
			}
			return false
		}
		if f.failing {
			log.Printf("plots: sending again")
			f.failing = false
		}
		if warning != "" {
			f.warnOnce("plots: " + warning)
		}
	}
	return true
}

// warnOnce logs each distinct message once: the same limit is reported on
// every batch it applies to.
func (f *livePlotFlusher) warnOnce(msg string) {
	if !f.warned[msg] {
		f.warned[msg] = true
		log.Print(msg)
	}
}
