package indicators

import (
	"errors"
	"sync"

	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

type IndicatorsCalculator interface {
	RSI(exchange string, symbol string, period int, pointType string) ([]float64, error)
}

// IndicatorManager listens to aggregated candles, updates its internal mathematical states,
// and invokes the user's strategy callback with the fully formulated indicators map.
type IndicatorManager struct {
	// The time frame is constant. We only need to store the points for each pair.
	// exhcange -> pait-> points
	pairCandlePoints map[string]map[string]*pairCandlePoints
	guard            sync.RWMutex
}

// NewIndicatorManager prepares the calculator for requested inputs
func NewIndicatorManager(exchanges []string, pairs []string) *IndicatorManager {
	exchngePairs := make(map[string]map[string]*pairCandlePoints)

	for _, exchange := range exchanges {
		exchngePairs[exchange] = make(map[string]*pairCandlePoints)
		for _, pair := range pairs {
			exchngePairs[exchange][pair] = &pairCandlePoints{}
		}
	}

	return &IndicatorManager{
		pairCandlePoints: exchngePairs,
	}
}

// Run listens to aggregated candles emitted by the TimeframeAggregator
func (im *IndicatorManager) Run(
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
func (im *IndicatorManager) Update(candle *types.Candle) {
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
}

func (im *IndicatorManager) RSI(exchange string, symbol string, period int, pointType string) ([]float64, error) {
	im.guard.RLock()
	defer im.guard.RUnlock()

	exhangeSymbols, exists := im.pairCandlePoints[exchange]
	if !exists {
		return nil, errors.New("invalid exchange")
	}

	symbolPoints, exists := exhangeSymbols[symbol]
	if !exists {
		return nil, errors.New("invalid symbol")
	}

	if len(symbolPoints.Close) < period {
		return nil, errors.New("not enough data for RSI calculation")
	}

	points := symbolPoints.Close
	if pointType == "high" {
		points = symbolPoints.High
	} else if pointType == "low" {
		points = symbolPoints.Low
	} else if pointType == "open" {
		points = symbolPoints.Open
	}
	if len(points) <= period {
		return nil, errors.New("not enough data for RSI calculation")
	}

	return RSI(points, period), nil
}

type pairCandlePoints struct {
	High  []float64
	Low   []float64
	Close []float64
	Open  []float64
}
