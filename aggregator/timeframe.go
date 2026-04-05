package aggregator

import (
	"sync"
	"time"

	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

// TimeframeAggregator processes high-frequency (e.g. 1m) candle streams
// and aggregates them into the user-requested Timeframe (e.g. 15m), only emitting when complete.
type TimeframeAggregator struct {
	targetTimeframe types.Timeframe
	outChan         chan<- *types.Candle

	mu      sync.Mutex
	current *types.Candle
}

// NewTimeframeAggregator builds an aggregator that pumps completed candles to outChan.
func NewTimeframeAggregator(tf types.Timeframe, outChan chan<- *types.Candle) *TimeframeAggregator {
	return &TimeframeAggregator{
		targetTimeframe: tf,
		outChan:         outChan,
	}
}

// Run loop listens to the raw channel and aggregates.
func (ta *TimeframeAggregator) Run(rawChan <-chan *types.Candle, doneChan chan<- bool) {
	for rawCandle := range rawChan {
		ta.Process(rawCandle)
		if doneChan != nil {
			doneChan <- true
		}
	}
}

// Process handles a single high-frequency candle.
func (ta *TimeframeAggregator) Process(raw *types.Candle) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// If the user requested the same timeframe as the raw feed (e.g., Engine gives 1m, User requested 1m)
	// We pass it directly if it's completed.
	if ta.targetTimeframe == types.Timeframe1m || ta.targetTimeframe == raw.Timeframe {
		ta.outChan <- raw
		return
	}

	// This is a naive check; full implementation will use time/math to determine explicit boundary crossings
	// depending on the targetTimeframe (e.g. modulo 15 minutes for 15m).
	ta.aggregate(raw)

	// Emit if boundary crossed
	if ta.isBoundaryCrossed(raw) {
		ta.current.IsComplete = true
		ta.current.CloseTime = raw.CloseTime

		// Send a copy to the output channel
		completedCandle := *ta.current
		ta.outChan <- &completedCandle

		// Reset for next period
		ta.current = nil
	}
}

func (ta *TimeframeAggregator) aggregate(raw *types.Candle) {
	if ta.current == nil {
		ta.current = &types.Candle{
			Symbol:    raw.Symbol,
			Exchange:  raw.Exchange,
			Timeframe: ta.targetTimeframe,
			OpenTime:  raw.OpenTime, // Round down to explicit boundary internally
			Open:      raw.Open,
			High:      raw.High,
			Low:       raw.Low,
			Volume:    raw.Volume,
			Close:     raw.Close,
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
}

func (ta *TimeframeAggregator) isBoundaryCrossed(raw *types.Candle) bool {
	// For architecture structural purposes:
	// Calculate if raw.CloseTime signifies the end of the `targetTimeframe` window.
	// Assume an external math/duration calculation here against Timeframe string.
	// Returning true will trigger the aggregated candle emit.

	duration := extractDuration(ta.targetTimeframe)
	// Example boundary check
	return raw.CloseTime.Sub(ta.current.OpenTime) >= duration
}

func extractDuration(tf types.Timeframe) time.Duration {
	// Simple mapping stub
	switch tf {
	case types.Timeframe5m:
		return 5 * time.Minute
	case types.Timeframe15m:
		return 15 * time.Minute
	case types.Timeframe1h:
		return time.Hour
	case types.Timeframe1d:
		return 24 * time.Hour
	default:
		return time.Minute
	}
}
