package indicators

import (
	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

// IndicatorManager listens to aggregated candles, updates its internal mathematical states,
// and invokes the user's strategy callback with the fully formulated indicators map.
type IndicatorManager struct {
	requested []string

	// Dataframe stores rolling state of candles for mathematical formulas that need history
	// df *core.Dataframe

	// internal storage for calculated values at the current tick
	currentValues map[string]float64
}

// NewIndicatorManager prepares the calculator for requested inputs
func NewIndicatorManager(requestedIndicators []string) *IndicatorManager {
	return &IndicatorManager{
		requested:     requestedIndicators,
		currentValues: make(map[string]float64),
	}
}

// Run listens to aggregated candles emitted by the TimeframeAggregator
func (im *IndicatorManager) Run(
	ctx *types.Context,
	aggregatedChan <-chan *types.Candle,
	onCandleCallback types.OnCandleFunc,
) {
	for candle := range aggregatedChan {
		im.update(candle)

		// Update the Context to pass injected indicator values reliably to strategy logic
		ctx.SetIndicators(im.currentValues)

		// Trigger the user's strategy algorithm
		onCandleCallback(ctx, candle)
	}
}

func (im *IndicatorManager) update(candle *types.Candle) {
	// Internally, push `candle` into the `core.Dataframe`.
	// Loop over im.requested (e.g. "EMA15", "RSI14")
	// Calculate and store latest calculation in `im.currentValues`

	// Stub architecture:
	for _, indName := range im.requested {
		im.currentValues[indName] = im.calculate(indName, candle)
	}
}

// calculate extracts the indicator from the `lib/core` standard functions
func (im *IndicatorManager) calculate(indName string, candle *types.Candle) float64 {
	// E.g., if indName == "EMA200", extract 200, find standard lib EMA function vs Dataframe Close history
	return 0.0 // Stub calculation
}
