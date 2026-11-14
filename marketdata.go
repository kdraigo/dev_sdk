package dev_sdk

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/kdraigo/dev_sdk/internal/marketdata"
	"github.com/kdraigo/dev_sdk/types"
)

// MarketDataReader is an optional capability: an adapter that serves the
// platform's market data itself. The backtest engine does, under its rule
// that nothing past the simulated playhead is readable. Live adapters do not;
// a live run reads the platform's data API instead (LiveOptions.DataURL).
type MarketDataReader interface {
	GetFundingRates(ctx context.Context, exchange, symbol string, from, to time.Time) ([]types.FundingRate, error)
	GetPositioning(ctx context.Context, exchange, symbol string, tf types.Timeframe, from, to time.Time) ([]types.Positioning, error)
	GetFlow(ctx context.Context, exchange, symbol string, tf types.Timeframe, from, to time.Time) ([]types.Flow, error)
}

// GetFundingRates returns a perpetual's funding settlements with
// from ≤ Time ≤ to, oldest first. An empty slice with a nil error means none in
// the range. History starts 2024-01-01, or at the market's listing.
//
// In a backtest, a to after s.Now() is an error, exactly as for candles: a
// strategy never sees a settlement that has not happened at the simulated
// time. Live, a to after the wall clock is read as now.
//
// Spot exchanges have no funding and return an error.
func (s *SDK) GetFundingRates(ctx context.Context, exchange, symbol string, from, to time.Time) ([]types.FundingRate, error) {
	if s.adapter == nil {
		return nil, fmt.Errorf("sdk has no adapter")
	}
	if r, ok := s.adapter.(MarketDataReader); ok {
		return r.GetFundingRates(ctx, exchange, symbol, from, to)
	}
	return s.liveMarketData().fundingRates(ctx, exchange, symbol, from, to)
}

// GetPositioning returns a perpetual's positioning bars of tf (5m or longer;
// empty means 5m) overlapping [from, to] whose values were published by to: a
// 5-minute period is published 90 s after it ends, so a bar ending at to is
// not there yet. Oldest first. History back to 2020-09.
//
// The same backtest and live rules as GetFundingRates.
func (s *SDK) GetPositioning(ctx context.Context, exchange, symbol string, tf types.Timeframe, from, to time.Time) ([]types.Positioning, error) {
	if s.adapter == nil {
		return nil, fmt.Errorf("sdk has no adapter")
	}
	if r, ok := s.adapter.(MarketDataReader); ok {
		return r.GetPositioning(ctx, exchange, symbol, tf, from, to)
	}
	return s.liveMarketData().positioningBars(ctx, exchange, symbol, tf, from, to)
}

// GetFlow returns trade-flow bars of tf (empty means 1m) overlapping
// [from, to] that had closed by to, oldest first. Every collected market,
// spot and perpetual; history back to 2025-03-01.
//
// The same backtest and live rules as GetFundingRates.
func (s *SDK) GetFlow(ctx context.Context, exchange, symbol string, tf types.Timeframe, from, to time.Time) ([]types.Flow, error) {
	if s.adapter == nil {
		return nil, fmt.Errorf("sdk has no adapter")
	}
	if r, ok := s.adapter.(MarketDataReader); ok {
		return r.GetFlow(ctx, exchange, symbol, tf, from, to)
	}
	return s.liveMarketData().flowBars(ctx, exchange, symbol, tf, from, to)
}

// validateMarketData refuses a series the platform does not attach.
func validateMarketData(series []types.MarketData) error {
	for _, m := range series {
		switch m {
		case types.MarketDataPositioning, types.MarketDataFlow:
		default:
			return fmt.Errorf("config.MarketData: unknown %q (supported: %q, %q)", m, types.MarketDataPositioning, types.MarketDataFlow)
		}
	}
	return nil
}

// liveMarketData returns the live reader, made on first use.
func (s *SDK) liveMarketData() *liveData {
	s.liveDataOnce.Do(func() {
		base := ""
		if s.config.Live != nil {
			base = s.config.Live.DataURL
		}
		s.liveDataReader = newLiveData(marketdata.NewClient(base), s.config.MarketData)
	})
	return s.liveDataReader
}

// liveData serves a live run's market data from the platform's data API: the
// read calls, and the series of Config.MarketData attached to each candle as
// it closes. It applies the backtest engine's rules, so a strategy sees the
// same numbers live as in a backtest.
type liveData struct {
	client *marketdata.Client
	// The series Config.MarketData attaches to each candle.
	wantPositioning bool
	wantFlow        bool

	// now is the wall clock; flowWaits is how long a closed candle waits for
	// its flow bar (the platform writes a minute ~3 s after it ends). Fields so
	// tests can replace them.
	now       func() time.Time
	flowWaits []time.Duration

	mu         sync.Mutex
	pos        map[string][]marketdata.PositioningWire
	flowMisses map[string]int
	warned     map[string]bool
}

func newLiveData(client *marketdata.Client, series []types.MarketData) *liveData {
	l := &liveData{
		client:     client,
		now:        time.Now,
		flowWaits:  []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second},
		pos:        map[string][]marketdata.PositioningWire{},
		flowMisses: map[string]int{},
		warned:     map[string]bool{},
	}
	for _, m := range series {
		switch m {
		case types.MarketDataPositioning:
			l.wantPositioning = true
		case types.MarketDataFlow:
			l.wantFlow = true
		}
	}
	return l
}

// flowMissesBeforeNoWait is how many candles in a row may find no flow bar
// before the SDK stops waiting for one: a market the platform does not
// collect must not delay every candle by the whole wait.
const flowMissesBeforeNoWait = 3

// warn logs a problem once per key.
func (l *liveData) warn(key, format string, args ...any) {
	l.mu.Lock()
	seen := l.warned[key]
	l.warned[key] = true
	l.mu.Unlock()
	if !seen {
		log.Printf("DevSDK: market data: "+format, args...)
	}
}

// enrich attaches the configured series to a closed live candle.
func (l *liveData) enrich(ctx context.Context, c *types.Candle) {
	if c == nil || !c.IsComplete {
		return
	}
	key := c.Exchange + "|" + c.Symbol
	if l.wantPositioning && marketdata.Perpetual(c.Exchange) {
		c.Positioning = l.positioningAt(ctx, key, c)
	}
	if l.wantFlow {
		c.Flow = l.flowAt(ctx, key, c)
	}
}

// positioningAt is the newest period known at the candle's close. It asks
// again only when a newer period could be known than the newest held.
func (l *liveData) positioningAt(ctx context.Context, key string, c *types.Candle) *types.Positioning {
	knownBy := marketdata.KnownBy(c.CloseTime)
	l.mu.Lock()
	held := l.pos[key]
	l.mu.Unlock()
	stale := len(held) == 0 || !time.UnixMilli(held[len(held)-1].OpenTime).Add(2*marketdata.Period).After(knownBy)
	if stale {
		from := knownBy.Add(-marketdata.MaxAge - marketdata.Period)
		bars, err := l.client.Positioning(ctx, c.Exchange, c.Symbol, types.Timeframe5m, from, c.CloseTime)
		if err != nil {
			l.warn("positioning|"+key, "positioning for %s on %s: %v (candles carry none until it answers)", c.Symbol, c.Exchange, err)
		} else {
			held = bars
			l.mu.Lock()
			l.pos[key] = bars
			l.mu.Unlock()
		}
	}
	return marketdata.NewestKnown(held, c.CloseTime)
}

// flowAt is the flow bar of the candle's own bar, waiting briefly for the
// platform to write it.
func (l *liveData) flowAt(ctx context.Context, key string, c *types.Candle) *types.Flow {
	tf := c.Timeframe
	if tf == "" {
		tf = types.Timeframe1m
	}
	spec, err := types.ParseTimeframe(tf)
	if err != nil {
		return nil
	}
	open := spec.BucketStart(c.OpenTime)
	wantMinutes := int(marketdata.BarEnd(spec, open).Sub(open) / time.Minute)

	l.mu.Lock()
	misses := l.flowMisses[key]
	l.mu.Unlock()
	waits := l.flowWaits
	if misses >= flowMissesBeforeNoWait {
		waits = nil
	}

	var bar *marketdata.FlowWire
	for attempt := 0; ; attempt++ {
		bars, err := l.client.Flow(ctx, c.Exchange, c.Symbol, tf, open, open)
		if err != nil {
			l.warn("flow|"+key, "trade flow for %s on %s: %v (candles carry none until it answers)", c.Symbol, c.Exchange, err)
			bar = nil
			misses = flowMissesBeforeNoWait
			break
		}
		bar = nil
		for i := range bars {
			if bars[i].OpenTime == open.UnixMilli() {
				bar = &bars[i]
			}
		}
		if (bar != nil && bar.Minutes >= wantMinutes) || attempt >= len(waits) {
			break
		}
		select {
		case <-time.After(waits[attempt]):
		case <-ctx.Done():
			return nil
		}
	}

	l.mu.Lock()
	if bar == nil {
		l.flowMisses[key] = misses + 1
	} else {
		l.flowMisses[key] = 0
	}
	l.mu.Unlock()
	if bar == nil {
		return nil
	}
	return bar.Flow()
}

// window clamps a live read to the wall clock and checks it.
func (l *liveData) window(from, to time.Time) (time.Time, error) {
	if now := l.now(); to.After(now) {
		to = now
	}
	if from.After(to) {
		return to, fmt.Errorf("from %s is after to %s", from.Format(time.RFC3339), to.Format(time.RFC3339))
	}
	return to, nil
}

func (l *liveData) fundingRates(ctx context.Context, exchange, symbol string, from, to time.Time) ([]types.FundingRate, error) {
	to, err := l.window(from, to)
	if err != nil {
		return nil, err
	}
	if !marketdata.Perpetual(exchange) {
		return nil, marketdata.PerpetualsOnly("funding", exchange)
	}
	rates, err := l.client.Funding(ctx, exchange, symbol, from, to)
	if err != nil {
		return nil, err
	}
	out := make([]types.FundingRate, 0, len(rates))
	for _, r := range rates {
		if f := r.FundingRate(); !f.Time.Before(from) && !f.Time.After(to) {
			out = append(out, f)
		}
	}
	return out, nil
}

func (l *liveData) positioningBars(ctx context.Context, exchange, symbol string, tf types.Timeframe, from, to time.Time) ([]types.Positioning, error) {
	to, err := l.window(from, to)
	if err != nil {
		return nil, err
	}
	if !marketdata.Perpetual(exchange) {
		return nil, marketdata.PerpetualsOnly("positioning", exchange)
	}
	if tf == "" {
		tf = types.Timeframe5m
	}
	spec, err := types.ParseTimeframe(tf)
	if err != nil {
		return nil, err
	}
	bars, err := l.client.Positioning(ctx, exchange, symbol, tf, from, to)
	if err != nil {
		return nil, err
	}
	knownBy := marketdata.KnownBy(to)
	out := make([]types.Positioning, 0, len(bars))
	for _, b := range bars {
		if end := marketdata.BarEnd(spec, time.UnixMilli(b.OpenTime)); !end.After(knownBy) {
			out = append(out, *b.Positioning(end))
		}
	}
	return out, nil
}

func (l *liveData) flowBars(ctx context.Context, exchange, symbol string, tf types.Timeframe, from, to time.Time) ([]types.Flow, error) {
	to, err := l.window(from, to)
	if err != nil {
		return nil, err
	}
	if tf == "" {
		tf = types.Timeframe1m
	}
	spec, err := types.ParseTimeframe(tf)
	if err != nil {
		return nil, err
	}
	bars, err := l.client.Flow(ctx, exchange, symbol, tf, from, to)
	if err != nil {
		return nil, err
	}
	closedBy := to.Add(time.Millisecond)
	out := make([]types.Flow, 0, len(bars))
	for _, b := range bars {
		if end := marketdata.BarEnd(spec, time.UnixMilli(b.OpenTime)); !end.After(closedBy) {
			out = append(out, *b.Flow())
		}
	}
	return out, nil
}
