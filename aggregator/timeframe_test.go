package aggregator

import (
	"testing"
	"time"

	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

func TestTimeframeAggregator_Process(t *testing.T) {
	outChan := make(chan *types.Candle, 10)
	agg := NewTimeframeAggregator(types.Timeframe5m, outChan)

	now := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)

	// Feed 5 x 1m candles
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
		agg.Process(candle)
	}

	// We expect 1 candle in outChan
	select {
	case aggregated := <-outChan:
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
	default:
		t.Fatal("Expected an aggregated candle, but got none")
	}
}

func TestTimeframeAggregator_SameTimeframe(t *testing.T) {
	outChan := make(chan *types.Candle, 10)
	agg := NewTimeframeAggregator(types.Timeframe1m, outChan)

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

	agg.Process(candle)

	select {
	case out := <-outChan:
		if out != candle {
			t.Error("Expected same candle returned for same timeframe")
		}
	default:
		t.Fatal("Expected candle in outChan")
	}
}
