package types

import "time"

// MarketData names a series the platform attaches to every candle, beside
// its prices. Ask for them in Config.MarketData; a strategy that asks for none
// receives none and pays nothing for them.
//
// They follow the rule GetCandles follows: a candle carries only what a live
// bot could have known at its close. The same series over longer windows come
// from SDK.GetPositioning and SDK.GetFlow, under the same rule.
type MarketData string

const (
	// MarketDataPositioning attaches a perpetual's open interest and long/short
	// ratios to each candle (Candle.Positioning): the newest 5-minute period
	// published by the candle's close. Perpetual exchanges only (binance_futures,
	// bybit_futures); history back to 2020-09.
	MarketDataPositioning MarketData = "positioning"

	// MarketDataFlow attaches what executed in the candle's own bar, by taker
	// side (Candle.Flow). Every collected market, spot and perpetual; history
	// back to 2025-03-01.
	MarketDataFlow MarketData = "flow"
)

// FundingRate is one funding settlement of a perpetual (SDK.GetFundingRates).
type FundingRate struct {
	Time          time.Time // the settlement instant, UTC (often a few ms after the hour)
	Rate          float64   // positive: longs pay shorts
	MarkPrice     float64   // the mark price the venue settled at; 0 if unknown
	IntervalHours int       // hours between settlements: 8 on most markets, 4 or 1 on some
}

// Positioning is a perpetual's open interest and who is long, from the venue's
// 5-minute periods.
//
// On a candle it is the newest period published by the candle's close: a
// period is published 90 seconds after it ends, so a candle closing at 12:00
// carries the period that ended at 11:55, never the one ending at 12:00. Time
// says which period it is. From SDK.GetPositioning it is one bar of the asked
// timeframe, its snapshots as they stood at the bar's end.
//
// A value the venue does not publish is 0: Bybit has no top-trader or taker
// series and no open interest in quote. A real ratio or open interest is
// never 0.
type Positioning struct {
	Time     time.Time // the instant the values are as of: the end of the period or bar
	OpenTime time.Time // the start of that period or bar

	OpenInterest      float64 // contracts open, base units, one side (comparable across venues)
	OpenInterestValue float64 // the same in quote; 0 on Bybit
	LongShortRatio    float64 // accounts long ÷ accounts short, all accounts
	LongAccountShare  float64 // share of accounts that are long, 0–1
	TopAccountRatio   float64 // the top 20% of accounts by margin, long ÷ short by account; Binance only
	TopPositionRatio  float64 // the same top accounts, long ÷ short by position size; Binance only
	TakerBuySellRatio float64 // taker buy ÷ sell volume; over a bar, the geometric mean of its periods; Binance only
	Periods           int     // 5-minute periods in the bar (1 on a candle)
}

// Flow is what executed in one bar, by taker side: a buy is a taker buying
// (a market order lifting the ask), a sell a taker selling. On a candle it is
// the candle's own bar, summed over its minutes.
type Flow struct {
	OpenTime time.Time // the bar's open

	BuyVolume  float64 // base
	SellVolume float64 // base
	Delta      float64 // BuyVolume − SellVolume
	BuyQuote   float64 // quote
	SellQuote  float64 // quote
	VWAP       float64 // (BuyQuote + SellQuote) ÷ (BuyVolume + SellVolume); 0 without volume

	BuyTrades  int64 // taker prints that bought
	SellTrades int64 // taker prints that sold

	MaxPrintQuote   float64 // the largest single taker print, in quote
	LargePrints10k  int64   // prints of at least 10,000 quote
	LargeQuote10k   float64 // their quote volume
	LargePrints100k int64   // prints of at least 100,000 quote
	LargeQuote100k  float64 // their quote volume

	Minutes  int  // minutes of the bar the platform stored
	Complete bool // every minute of the bar was seen whole; false = a gap, the sums are short
}
