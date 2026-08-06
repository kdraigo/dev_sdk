package aggregator

import (
	"sync"

	"github.com/kdraigo/dev_sdk/types"
)

// MultiAggregator keeps one single-series aggregator per (exchange, symbol) for
// a single target timeframe.
//
// A session may stream several symbols, and their raw candles arrive
// interleaved. Folding them through one shared aggregator produced bars that
// carried one symbol's label and another's highs — so an indicator computed on
// a multi-symbol session was reading a blend of instruments.
//
// TimeframeAggregator stays deliberately single-series: partitioning here keeps
// the folding logic in one place and its existing tests meaningful.
type MultiAggregator struct {
	targetTimeframe types.Timeframe

	mu     sync.Mutex
	series map[string]*TimeframeAggregator
}

// NewMultiAggregator builds a symbol-partitioned aggregator for one timeframe.
func NewMultiAggregator(tf types.Timeframe) *MultiAggregator {
	return &MultiAggregator{
		targetTimeframe: tf,
		series:          make(map[string]*TimeframeAggregator),
	}
}

// seriesKey identifies one instrument's stream.
func seriesKey(exchange, symbol string) string { return exchange + "|" + symbol }

// Process folds a raw candle into its own instrument's series and returns a
// completed bar when that series crosses a boundary, or nil otherwise.
//
// Series are created on demand. Doing it lazily rather than from the configured
// asset list keeps this correct when the engine streams a symbol the SDK was not
// explicitly told about.
func (m *MultiAggregator) Process(raw *types.Candle) *types.Candle {
	if raw == nil {
		return nil
	}

	m.mu.Lock()
	key := seriesKey(raw.Exchange, raw.Symbol)
	series, ok := m.series[key]
	if !ok {
		series = NewTimeframeAggregator(m.targetTimeframe)
		m.series[key] = series
	}
	m.mu.Unlock()

	return series.Process(raw)
}

// Series reports how many instruments this aggregator is tracking. Intended for
// tests and diagnostics.
func (m *MultiAggregator) Series() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.series)
}
