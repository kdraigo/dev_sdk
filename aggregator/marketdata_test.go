package aggregator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kdraigo/dev_sdk/types"
)

var mdBase = time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

// minute is the i-th 1m candle, with flow scaled by i+1 and a positioning
// snapshot of open interest 100+i.
func minute(i int, withFlow, withPos bool) *types.Candle {
	open := mdBase.Add(time.Duration(i) * time.Minute)
	n := float64(i + 1)
	c := &types.Candle{
		Symbol: "BTC/USDT", Exchange: "binance_futures", Timeframe: types.Timeframe1m,
		OpenTime: open, CloseTime: open.Add(time.Minute - time.Millisecond),
		Open: 1, High: 1, Low: 1, Close: 1, Volume: 1, IsComplete: true,
	}
	if withFlow {
		c.Flow = &types.Flow{
			OpenTime: open, BuyVolume: n, SellVolume: 1, BuyQuote: 10 * n, SellQuote: 10,
			BuyTrades: 2, SellTrades: 1, MaxPrintQuote: 1000 * n, LargePrints10k: 1, LargeQuote10k: 10_000,
			Minutes: 1, Complete: true,
		}
		c.Flow.Delta = c.Flow.BuyVolume - c.Flow.SellVolume
		c.Flow.VWAP = (c.Flow.BuyQuote + c.Flow.SellQuote) / (c.Flow.BuyVolume + c.Flow.SellVolume)
	}
	if withPos {
		c.Positioning = &types.Positioning{OpenInterest: 100 + float64(i), Time: open}
	}
	return c
}

func foldFive(t *testing.T, candles ...*types.Candle) *types.Candle {
	t.Helper()
	agg := NewTimeframeAggregator(types.Timeframe5m)
	var out *types.Candle
	for _, c := range candles {
		if bar := agg.Process(c); bar != nil {
			require.Nil(t, out, "one bar")
			out = bar
		}
	}
	require.NotNil(t, out)
	return out
}

func TestFold_FlowSumsAndPositioningIsTheNewest(t *testing.T) {
	var in []*types.Candle
	for i := 0; i < 5; i++ {
		in = append(in, minute(i, true, true))
	}
	bar := foldFive(t, in...)

	f := bar.Flow
	require.NotNil(t, f)
	assert.Equal(t, mdBase, f.OpenTime, "the bar's open, not its last minute's")
	assert.Equal(t, 15.0, f.BuyVolume) // 1+2+3+4+5
	assert.Equal(t, 5.0, f.SellVolume)
	assert.Equal(t, 10.0, f.Delta)
	assert.Equal(t, 200.0, f.BuyQuote+f.SellQuote)
	assert.InDelta(t, 10.0, f.VWAP, 1e-9, "re-weighted over the bar, not the last minute's")
	assert.Equal(t, int64(10), f.BuyTrades)
	assert.Equal(t, 5000.0, f.MaxPrintQuote, "the largest print of any minute")
	assert.Equal(t, int64(5), f.LargePrints10k)
	assert.Equal(t, 5, f.Minutes)
	assert.True(t, f.Complete)

	require.NotNil(t, bar.Positioning)
	assert.Equal(t, 104.0, bar.Positioning.OpenInterest, "the newest snapshot")

	// The raw candles were not changed by folding.
	assert.Equal(t, 1.0, in[0].Flow.BuyVolume)
	assert.Equal(t, mdBase, in[0].Flow.OpenTime)
}

func TestFold_AMinuteWithoutFlowMakesTheBarIncomplete(t *testing.T) {
	bar := foldFive(t, minute(0, false, false), minute(1, true, false), minute(2, true, false), minute(3, true, false), minute(4, true, false))
	require.NotNil(t, bar.Flow)
	assert.False(t, bar.Flow.Complete)
	assert.Equal(t, 4, bar.Flow.Minutes)
	assert.Equal(t, mdBase, bar.Flow.OpenTime)

	bar = foldFive(t, minute(0, true, false), minute(1, true, false), minute(2, false, false), minute(3, true, false), minute(4, true, false))
	assert.False(t, bar.Flow.Complete)
}

func TestFold_NoMarketDataStaysNil(t *testing.T) {
	var in []*types.Candle
	for i := 0; i < 5; i++ {
		in = append(in, minute(i, false, false))
	}
	bar := foldFive(t, in...)
	assert.Nil(t, bar.Flow)
	assert.Nil(t, bar.Positioning)

	// A later bar starts afresh: a gap in one bar does not mark the next.
	agg := NewTimeframeAggregator(types.Timeframe5m)
	for i := 0; i < 5; i++ {
		agg.Process(minute(i, i != 2, false))
	}
	var next *types.Candle
	for i := 5; i < 10; i++ {
		if b := agg.Process(minute(i, true, false)); b != nil {
			next = b
		}
	}
	require.NotNil(t, next)
	assert.True(t, next.Flow.Complete)
}

func TestFold_PositioningKeptWhenTheLastMinuteHasNone(t *testing.T) {
	bar := foldFive(t, minute(0, false, true), minute(1, false, true), minute(2, false, false), minute(3, false, false), minute(4, false, false))
	require.NotNil(t, bar.Positioning)
	assert.Equal(t, 101.0, bar.Positioning.OpenInterest)
}
