package aggregator

import (
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

func TestTimeframeAggregator_Process(t *testing.T) {
	agg := NewTimeframeAggregator(types.Timeframe5m)

	now := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)

	var aggregated *types.Candle
	// Feed 5 x 1m candles; the 5th should produce a completed 5m candle.
	for i := 0; i < 5; i++ {
		candle := &types.Candle{
			Symbol:    "BTC/USDT",
			Exchange:  "binance",
			Timeframe: types.Timeframe1m,
			OpenTime:  now.Add(time.Duration(i) * time.Minute),
			CloseTime: now.Add(time.Duration(i+1) * time.Minute),
			Open:      100 + float64(i),
			High:      110 + float64(i),
			Low:       90 + float64(i),
			Close:     101 + float64(i),
			Volume:    10,
		}
		aggregated = agg.Process(candle)
	}

	if aggregated == nil {
		t.Fatal("Expected an aggregated candle, but got none")
	}
	if aggregated.Timeframe != types.Timeframe5m {
		t.Errorf("Expected timeframe 5m, got %v", aggregated.Timeframe)
	}
	if aggregated.Open != 100 {
		t.Errorf("Expected open 100, got %f", aggregated.Open)
	}
	if aggregated.Close != 105 {
		t.Errorf("Expected close 105, got %f", aggregated.Close)
	}
	if aggregated.High != 114 {
		t.Errorf("Expected high 114, got %f", aggregated.High)
	}
	if aggregated.Low != 90 {
		t.Errorf("Expected low 90, got %f", aggregated.Low)
	}
	if aggregated.Volume != 50 {
		t.Errorf("Expected volume 50, got %f", aggregated.Volume)
	}
	if !aggregated.IsComplete {
		t.Error("Expected candle to be complete")
	}
}

func TestTimeframeAggregator_SameTimeframe(t *testing.T) {
	agg := NewTimeframeAggregator(types.Timeframe1m)

	now := time.Now()
	candle := &types.Candle{
		Symbol:    "BTC/USDT",
		Exchange:  "binance",
		Timeframe: types.Timeframe1m,
		OpenTime:  now,
		CloseTime: now.Add(time.Minute),
		Open:      100,
		Close:     101,
	}

	out := agg.Process(candle)
	if out == nil {
		t.Fatal("Expected candle returned for same timeframe, got nil")
	}
	if out != candle {
		t.Error("Expected same candle pointer returned for 1m pass-through")
	}
}

func TestTimeframeAggregator_BoundaryAlignment(t *testing.T) {
	agg := NewTimeframeAggregator(types.Timeframe15m)

	// A candle starting at 10:07 should result in an aggregated candle starting at 10:00
	startTime := time.Date(2023, 1, 1, 10, 7, 0, 0, time.UTC)
	candle := &types.Candle{
		Symbol:    "BTC/USDT",
		Exchange:  "binance",
		Timeframe: types.Timeframe1m,
		OpenTime:  startTime,
		CloseTime: startTime.Add(time.Minute),
		Open:      100,
		High:      101,
		Low:       99,
		Close:     100.5,
	}

	agg.Process(candle) // First candle starts the aggregation

	if agg.current == nil {
		t.Fatal("Expected current candle to be initialized")
	}

	expectedOpenTime := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)
	if !agg.current.OpenTime.Equal(expectedOpenTime) {
		t.Errorf("Expected aligned OpenTime %v, got %v", expectedOpenTime, agg.current.OpenTime)
	}
}
