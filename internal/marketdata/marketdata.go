// Package marketdata carries the platform's market data — funding
// settlements, positioning and trade flow — between its wire forms (the
// backtest engine's frames and the data API's rows) and the SDK's types, and
// holds the rule both modes share: what is known at an instant.
package marketdata

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

const (
	// PublishLag is how long after a 5-minute positioning period ends its
	// values count as known. The venues publish a period shortly after it
	// ends and the platform's collector reads it 90 s after; the backtest
	// engine applies the same lag, so a backtest never sees a period sooner
	// than a live bot would.
	PublishLag = 90 * time.Second

	// MaxAge bounds how old the period on a candle may be. Older means the
	// venue published nothing for a while: the candle carries nothing rather
	// than a stale value that reads as current.
	MaxAge = 30 * time.Minute

	// Period is the venues' positioning period.
	Period = 5 * time.Minute

	// DefaultURL is the hosted data API.
	DefaultURL = "https://api.kdraigo.com/data"
)

// Perpetual reports whether exchange trades perpetuals, the only markets with
// funding and positioning.
func Perpetual(exchange string) bool {
	return exchange == types.ExchangeBinanceFutures || exchange == "bybit_futures"
}

// PerpetualsOnly is the error for funding or positioning asked of another market.
func PerpetualsOnly(what, exchange string) error {
	return fmt.Errorf("%s exists only on perpetual exchanges (binance_futures, bybit_futures), not %q", what, exchange)
}

// FundingWire is a settlement as the engine and the data API send it.
type FundingWire struct {
	FundingTime   int64   `json:"funding_time"`
	Rate          float64 `json:"funding_rate"`
	MarkPrice     float64 `json:"mark_price"`
	IntervalHours int     `json:"interval_hours"`
}

// FundingRate converts a wire settlement.
func (w FundingWire) FundingRate() types.FundingRate {
	return types.FundingRate{
		Time:          time.UnixMilli(w.FundingTime).UTC(),
		Rate:          w.Rate,
		MarkPrice:     w.MarkPrice,
		IntervalHours: w.IntervalHours,
	}
}

// PositioningWire is a positioning bar. Time (the end of the bar, Unix ms)
// comes from the engine; the data API leaves it out and End supplies it. A
// null (not published) decodes as 0.
type PositioningWire struct {
	Time              int64   `json:"time"`
	OpenTime          int64   `json:"open_time"`
	OpenInterest      float64 `json:"open_interest"`
	OpenInterestValue float64 `json:"open_interest_value"`
	LongShortRatio    float64 `json:"long_short_ratio"`
	LongAccountShare  float64 `json:"long_account_share"`
	TopAccountRatio   float64 `json:"top_account_ratio"`
	TopPositionRatio  float64 `json:"top_position_ratio"`
	TakerBuySellRatio float64 `json:"taker_buy_sell_ratio"`
	Periods           int     `json:"periods"`
}

// Positioning converts a wire bar; end is the bar's end when the wire has none.
func (w PositioningWire) Positioning(end time.Time) *types.Positioning {
	t := end
	if w.Time > 0 {
		t = time.UnixMilli(w.Time).UTC()
	}
	return &types.Positioning{
		Time:              t,
		OpenTime:          time.UnixMilli(w.OpenTime).UTC(),
		OpenInterest:      w.OpenInterest,
		OpenInterestValue: w.OpenInterestValue,
		LongShortRatio:    w.LongShortRatio,
		LongAccountShare:  w.LongAccountShare,
		TopAccountRatio:   w.TopAccountRatio,
		TopPositionRatio:  w.TopPositionRatio,
		TakerBuySellRatio: w.TakerBuySellRatio,
		Periods:           w.Periods,
	}
}

// FlowWire is a trade-flow bar as the engine and the data API send it.
type FlowWire struct {
	OpenTime        int64   `json:"open_time"`
	BuyVolume       float64 `json:"buy_volume"`
	SellVolume      float64 `json:"sell_volume"`
	Delta           float64 `json:"delta"`
	BuyQuote        float64 `json:"buy_quote"`
	SellQuote       float64 `json:"sell_quote"`
	VWAP            float64 `json:"vwap"`
	BuyTrades       int64   `json:"buy_trades"`
	SellTrades      int64   `json:"sell_trades"`
	MaxPrintQuote   float64 `json:"max_print_quote"`
	LargePrints10k  int64   `json:"large_prints_10k"`
	LargeQuote10k   float64 `json:"large_quote_10k"`
	LargePrints100k int64   `json:"large_prints_100k"`
	LargeQuote100k  float64 `json:"large_quote_100k"`
	Minutes         int     `json:"minutes"`
	Complete        bool    `json:"complete"`
}

// Flow converts a wire bar.
func (w FlowWire) Flow() *types.Flow {
	return &types.Flow{
		OpenTime:        time.UnixMilli(w.OpenTime).UTC(),
		BuyVolume:       w.BuyVolume,
		SellVolume:      w.SellVolume,
		Delta:           w.Delta,
		BuyQuote:        w.BuyQuote,
		SellQuote:       w.SellQuote,
		VWAP:            w.VWAP,
		BuyTrades:       w.BuyTrades,
		SellTrades:      w.SellTrades,
		MaxPrintQuote:   w.MaxPrintQuote,
		LargePrints10k:  w.LargePrints10k,
		LargeQuote10k:   w.LargeQuote10k,
		LargePrints100k: w.LargePrints100k,
		LargeQuote100k:  w.LargeQuote100k,
		Minutes:         w.Minutes,
		Complete:        w.Complete,
	}
}

// FlowFields are the trade-flow columns asked of the data API. The perpetual
// columns (liquidations, minute open interest, current funding) exist from
// 2026-11-06 only and are not part of the SDK.
const FlowFields = "buy_volume,sell_volume,delta,buy_quote,sell_quote,vwap,buy_trades,sell_trades,max_print_quote," +
	"large_prints_10k,large_quote_10k,large_prints_100k,large_quote_100k,minutes,complete"

// BarEnd is the end of the bar of spec opening at open: the next bar's open.
// A calendar month's nominal 31 days always land in the next month.
func BarEnd(spec types.TimeframeSpec, open time.Time) time.Time {
	start := spec.BucketStart(open)
	return spec.BucketStart(start.Add(spec.Duration()))
}

// KnownBy is the latest period end known at instant t (a candle's close, or
// now): periods that ended PublishLag before it or earlier. A close is a bar's
// last millisecond, so the bar itself ends 1 ms after it.
func KnownBy(t time.Time) time.Time {
	return t.Add(time.Millisecond).Add(-PublishLag)
}

// NewestKnown is the newest of bars (5-minute periods, ascending) known at t
// and not older than MaxAge, or nil.
func NewestKnown(bars []PositioningWire, t time.Time) *types.Positioning {
	knownBy := KnownBy(t)
	var best *PositioningWire
	for i := range bars {
		end := time.UnixMilli(bars[i].OpenTime).Add(Period)
		if end.After(knownBy) {
			break
		}
		best = &bars[i]
	}
	if best == nil {
		return nil
	}
	end := time.UnixMilli(best.OpenTime).Add(Period).UTC()
	if end.Before(knownBy.Add(-MaxAge)) {
		return nil
	}
	return best.Positioning(end)
}

// Client reads the platform's public data API.
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// NewClient returns a client for baseURL, DefaultURL when empty.
func NewClient(baseURL string) *Client {
	if baseURL == "" {
		baseURL = DefaultURL
	}
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), HTTP: &http.Client{Timeout: 20 * time.Second}}
}

// Funding returns settlements in [from, to] (the API answers with a few ms of slack; callers filter).
func (c *Client) Funding(ctx context.Context, exchange, symbol string, from, to time.Time) ([]FundingWire, error) {
	var out []FundingWire
	return out, c.get(ctx, "/funding", exchange, symbol, "", from, to, "", &out)
}

// Positioning returns positioning bars of tf overlapping [from, to].
func (c *Client) Positioning(ctx context.Context, exchange, symbol string, tf types.Timeframe, from, to time.Time) ([]PositioningWire, error) {
	var out []PositioningWire
	return out, c.get(ctx, "/positioning", exchange, symbol, tf, from, to, "", &out)
}

// Flow returns trade-flow bars of tf overlapping [from, to].
func (c *Client) Flow(ctx context.Context, exchange, symbol string, tf types.Timeframe, from, to time.Time) ([]FlowWire, error) {
	var out []FlowWire
	return out, c.get(ctx, "/flow", exchange, symbol, tf, from, to, FlowFields, &out)
}

func (c *Client) get(ctx context.Context, path, exchange, symbol string, tf types.Timeframe, from, to time.Time, fields string, out any) error {
	q := url.Values{}
	q.Set("exchange", exchange)
	q.Set("symbol", symbol)
	q.Set("from", strconv.FormatInt(from.UnixMilli(), 10))
	q.Set("to", strconv.FormatInt(to.UnixMilli(), 10))
	if tf != "" {
		q.Set("timeframe", string(tf))
	}
	if fields != "" {
		q.Set("fields", fields)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("data API %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(body, &e) == nil && e.Error != "" {
			return fmt.Errorf("data API %s: %s (status %d)", path, e.Error, resp.StatusCode)
		}
		return fmt.Errorf("data API %s: status %d", path, resp.StatusCode)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("data API %s: decode: %w", path, err)
	}
	return nil
}
