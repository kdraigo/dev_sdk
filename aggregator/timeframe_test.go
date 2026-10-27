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

func TestTimeframeAggregator_SumsOrderFlowMetrics(t *testing.T) {
	agg := NewTimeframeAggregator(types.Timeframe5m)

	now := time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC)

	var aggregated *types.Candle
	for i := 0; i < 5; i++ {
		candle := &types.Candle{
			Symbol:              "BTC/USDT",
			Exchange:            "binance",
			Timeframe:           types.Timeframe1m,
			OpenTime:            now.Add(time.Duration(i) * time.Minute),
			CloseTime:           now.Add(time.Duration(i+1) * time.Minute),
			Open:                100,
			High:                110,
			Low:                 90,
			Close:               101,
			Volume:              10,
			TradeCount:          3,
			QuoteVolume:         1000,
			TakerBuyBaseVolume:  4,
			TakerBuyQuoteVolume: 400,
		}
		aggregated = agg.Process(candle)
	}

	if aggregated == nil {
		t.Fatal("Expected an aggregated candle, but got none")
	}
	if aggregated.TradeCount != 15 {
		t.Errorf("Expected TradeCount 15, got %d", aggregated.TradeCount)
	}
	if aggregated.QuoteVolume != 5000 {
		t.Errorf("Expected QuoteVolume 5000, got %f", aggregated.QuoteVolume)
	}
	if aggregated.TakerBuyBaseVolume != 20 {
		t.Errorf("Expected TakerBuyBaseVolume 20, got %f", aggregated.TakerBuyBaseVolume)
	}
	if aggregated.TakerBuyQuoteVolume != 2000 {
		t.Errorf("Expected TakerBuyQuoteVolume 2000, got %f", aggregated.TakerBuyQuoteVolume)
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

// Candles as venues and the backtest stream send them: CloseTime is the last
// millisecond (open + d − 1 ms). A bucket's last candle completes it; folded
// bars used to run one raw candle late, from the next bucket's first candle.
func TestTimeframeAggregator_InclusiveCloseTimes(t *testing.T) {
	feed := func(target, raw types.Timeframe, d time.Duration, start time.Time, n int) (out []*types.Candle) {
		agg := NewTimeframeAggregator(target)
		for i := 0; i < n; i++ {
			open := start.Add(time.Duration(i) * d)
			c := &types.Candle{Symbol: "BTC/USDT", Exchange: "binance", Timeframe: raw,
				OpenTime: open, CloseTime: open.Add(d - time.Millisecond),
				Open: float64(100 + i), High: float64(101 + i), Low: float64(99 + i), Close: float64(100 + i), Volume: 1}
			if bar := agg.Process(c); bar != nil {
				out = append(out, bar)
			}
		}
		return out
	}

	t.Run("4h from 1h", func(t *testing.T) {
		start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
		bars := feed(types.Timeframe4h, types.Timeframe1h, time.Hour, start, 9)
		if len(bars) != 2 {
			t.Fatalf("want 2 complete 4h bars from 9 hours, got %d", len(bars))
		}
		for i, b := range bars {
			wantOpen := start.Add(time.Duration(4*i) * time.Hour)
			if !b.OpenTime.Equal(wantOpen) {
				t.Errorf("bar %d opens %s, want %s", i, b.OpenTime, wantOpen)
			}
			if b.Open != float64(100+4*i) || b.Close != float64(103+4*i) || b.Volume != 4 {
				t.Errorf("bar %d is O %.0f C %.0f V %.0f, want the four hours %d–%d", i, b.Open, b.Close, b.Volume, 4*i, 4*i+3)
			}
			if wantClose := wantOpen.Add(4*time.Hour - time.Millisecond); !b.CloseTime.Equal(wantClose) {
				t.Errorf("bar %d closes %s, want %s", i, b.CloseTime, wantClose)
			}
		}
	})

	t.Run("1w from 1d, weeks from Monday", func(t *testing.T) {
		monday := time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)
		bars := feed(types.Timeframe1w, types.Timeframe1d, 24*time.Hour, monday, 14)
		if len(bars) != 2 {
			t.Fatalf("want 2 weeks from 14 days, got %d", len(bars))
		}
		if !bars[1].OpenTime.Equal(monday.AddDate(0, 0, 7)) || bars[1].Open != 107 || bars[1].Close != 113 {
			t.Errorf("second week opens %s O %.0f C %.0f, want Monday Sep 7, days 7–13", bars[1].OpenTime, bars[1].Open, bars[1].Close)
		}
	})

	t.Run("5m from 1m", func(t *testing.T) {
		start := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
		bars := feed(types.Timeframe5m, types.Timeframe1m, time.Minute, start, 5)
		if len(bars) != 1 || bars[0].Close != 104 {
			t.Fatalf("want the 5th minute to complete the bar, got %d bars", len(bars))
		}
	})
}
