package indicators

import (
	"sync"

	"github.com/kdraigo/dev_sdk/types"
)

type IndicatorsCalculator interface {
	IndicatorsCalculatorGen
}

type IndicatorManager interface {
	IndicatorsCalculator
	IndicatorsCalculatorRun(
		ctx *types.Context,
		aggregatedChan <-chan *types.Candle,
		onCandleCallback types.OnCandleFunc,
	)
	Update(candle *types.Candle)
}

// defaultMaxPoints is the rolling history retained per price series when the
// caller does not override it. Generous enough that even long-period indicators
// (e.g. EMA200) keep many multiples of their period for TA-Lib's unstable-period
// warm-up, while keeping memory and per-call compute bounded.
const defaultMaxPoints = 1500

// Option configures an indicatorManager at construction time.
type Option func(*indicatorManager)

// WithMaxPoints overrides the rolling history window kept per price series.
// A value <= 0 is ignored (the default is kept). Raise it only if a strategy
// requests indicator periods approaching the default window.
func WithMaxPoints(n int) Option {
	return func(im *indicatorManager) {
		if n > 0 {
			im.maxPoints = n
		}
	}
}

// IndicatorManager listens to aggregated candles, updates its internal mathematical states,
// and invokes the user's strategy callback with the fully formulated indicators map.
type indicatorManager struct {
	// The time frame is constant. We only need to store the points for each pair.
	// exhcange -> pait-> points
	pairCandlePoints map[string]map[string]*pairCandlePoints
	guard            sync.RWMutex

	// maxPoints caps the rolling history kept per price series so a long-running
	// session does not grow memory (or per-call TA-Lib compute) without bound.
	maxPoints int
}

// NewIndicatorManager prepares the calculator for requested inputs.
// History retention defaults to defaultMaxPoints; override with WithMaxPoints.
func NewIndicatorManager(exchanges []string, pairs []string, opts ...Option) IndicatorManager {
	exchngePairs := make(map[string]map[string]*pairCandlePoints)

	for _, exchange := range exchanges {
		exchngePairs[exchange] = make(map[string]*pairCandlePoints)
		for _, pair := range pairs {
			exchngePairs[exchange][pair] = &pairCandlePoints{}
		}
	}

	im := &indicatorManager{
		pairCandlePoints: exchngePairs,
		maxPoints:        defaultMaxPoints,
	}
	for _, opt := range opts {
		opt(im)
	}
	return im
}

// Run listens to aggregated candles emitted by the TimeframeAggregator
func (im *indicatorManager) IndicatorsCalculatorRun(
	ctx *types.Context,
	aggregatedChan <-chan *types.Candle,
	onCandleCallback types.OnCandleFunc,
) {
	for {
		select {
		case <-ctx.Ctx.Done():
			return

		case candle, ok := <-aggregatedChan:
			if !ok {
				return
			}

			im.Update(candle)

			if onCandleCallback != nil {
				onCandleCallback(ctx, candle)
			}
		}
	}
}

// Update adds a completed aggregated candle to the indicator history.
// Must be called before the OnCandle callback so RSI/etc. reflect the new candle.
func (im *indicatorManager) Update(candle *types.Candle) {
	im.guard.Lock()
	defer im.guard.Unlock()

	exchange := im.pairCandlePoints[candle.Exchange]
	if exchange == nil {
		return
	}

	pair := exchange[candle.Symbol]
	if pair == nil {
		return
	}

	pair.High = append(pair.High, candle.High)
	pair.Low = append(pair.Low, candle.Low)
	pair.Close = append(pair.Close, candle.Close)
	pair.Open = append(pair.Open, candle.Open)
	pair.Volume = append(pair.Volume, candle.Volume)

	// Prune oldest points once history overflows. Trigger at 2x the window and
	// trim back to maxPoints so the copy cost amortises to O(1) per candle while
	// the indicators always retain at least maxPoints of history.
	if im.maxPoints > 0 && len(pair.Close) >= 2*im.maxPoints {
		pair.High = keepLast(pair.High, im.maxPoints)
		pair.Low = keepLast(pair.Low, im.maxPoints)
		pair.Close = keepLast(pair.Close, im.maxPoints)
		pair.Open = keepLast(pair.Open, im.maxPoints)
		pair.Volume = keepLast(pair.Volume, im.maxPoints)
	}
}

// keepLast returns the last n elements of s in a freshly allocated slice. The
// copy (rather than a reslice) drops the reference to the large backing array so
// the pruned head can be garbage collected.
func keepLast(s []float64, n int) []float64 {
	if len(s) <= n {
		return s
	}
	out := make([]float64, n)
	copy(out, s[len(s)-n:])
	return out
}

type pairCandlePoints struct {
	High   []float64
	Low    []float64
	Close  []float64
	Open   []float64
	Volume []float64
}
