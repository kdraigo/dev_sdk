package aggregator

import (
	"sync"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// TimeframeAggregator processes high-frequency (e.g. 1m) candle streams
// and aggregates them into the user-requested Timeframe (e.g. 15m), only emitting when complete.
type TimeframeAggregator struct {
	targetTimeframe types.Timeframe
	spec            types.TimeframeSpec

	mu      sync.Mutex
	current *types.Candle
}

// NewTimeframeAggregator builds an aggregator for the given target timeframe.
func NewTimeframeAggregator(tf types.Timeframe) *TimeframeAggregator {
	// An unknown timeframe falls back to one minute, which makes the aggregator
	// a passthrough. That is the safe degradation here: the engine validates
	// timeframes at session creation, so an unknown value cannot reach a live
	// run, and silently bucketing it wrongly would be worse.
	spec, err := types.ParseTimeframe(tf)
	if err != nil {
		spec, _ = types.ParseTimeframe(types.Timeframe1m)
		tf = types.Timeframe1m
	}

	return &TimeframeAggregator{
		targetTimeframe: tf,
		spec:            spec,
	}
}

// Process handles a single high-frequency candle and returns a completed aggregated candle
// when a timeframe boundary is crossed, or nil if the boundary has not yet been reached.
// The caller is responsible for acting on the returned candle (updating indicators, firing OnCandle)
// before requesting the next raw candle — this keeps the pipeline fully sequential and deterministic.
func (ta *TimeframeAggregator) Process(raw *types.Candle) *types.Candle {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// If the user requested the same timeframe as the raw feed (e.g., Engine gives 1m, User requested 1m)
	// We pass it directly if it's completed.
	if ta.targetTimeframe == types.Timeframe1m || ta.targetTimeframe == raw.Timeframe {
		return raw
	}

	// This is a naive check; full implementation will use time/math to determine explicit boundary crossings
	// depending on the targetTimeframe (e.g. modulo 15 minutes for 15m).
	ta.aggregate(raw)

	// Emit if boundary crossed
	if ta.isBoundaryCrossed(raw) {
		ta.current.IsComplete = true
		ta.current.CloseTime = raw.CloseTime

		// Return a copy; reset for the next period.
		completedCandle := *ta.current
		ta.current = nil
		return &completedCandle
	}

	return nil
}

func (ta *TimeframeAggregator) aggregate(raw *types.Candle) {
	if ta.current == nil {
		ta.current = &types.Candle{
			Symbol:              raw.Symbol,
			Exchange:            raw.Exchange,
			Timeframe:           ta.targetTimeframe,
			OpenTime:            ta.spec.BucketStart(raw.OpenTime),
			Open:                raw.Open,
			High:                raw.High,
			Low:                 raw.Low,
			Volume:              raw.Volume,
			Close:               raw.Close,
			TradeCount:          raw.TradeCount,
			QuoteVolume:         raw.QuoteVolume,
			TakerBuyBaseVolume:  raw.TakerBuyBaseVolume,
			TakerBuyQuoteVolume: raw.TakerBuyQuoteVolume,
		}
		return
	}

	// Update running highs/lows
	if raw.High > ta.current.High {
		ta.current.High = raw.High
	}
	if raw.Low < ta.current.Low {
		ta.current.Low = raw.Low
	}
	ta.current.Close = raw.Close
	ta.current.Volume += raw.Volume
	ta.current.TradeCount += raw.TradeCount
	ta.current.QuoteVolume += raw.QuoteVolume
	ta.current.TakerBuyBaseVolume += raw.TakerBuyBaseVolume
	ta.current.TakerBuyQuoteVolume += raw.TakerBuyQuoteVolume
}

// isBoundaryCrossed reports whether the raw candle completes the current bucket.
//
// It compares bucket membership rather than subtracting timestamps, because a
// fixed duration cannot express a calendar month. (Weeks were already correct
// under the previous Time.Truncate, which anchors at Go's zero time — year 1,
// a Monday — rather than at the Unix epoch, a Thursday.)
func (ta *TimeframeAggregator) isBoundaryCrossed(raw *types.Candle) bool {
	return !ta.spec.SameBucket(raw.CloseTime, ta.current.OpenTime)
}

// ExtractDuration returns the nominal length of a timeframe, for client-side
// start-time rounding.
//
// It replaces a local switch whose default silently returned one minute — the
// same class of defect that made unsupported timeframes serve 1m data. Unknown
// values still fall back to a minute here because callers use this only for
// rounding, but the table now covers every supported timeframe including 2m,
// 1w and 1M. A calendar month reports a nominal 31 days.
func ExtractDuration(tf types.Timeframe) time.Duration {
	spec, err := types.ParseTimeframe(tf)
	if err != nil {
		return time.Minute
	}
	return spec.Duration()
}
