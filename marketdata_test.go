package dev_sdk

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kdraigo/dev_sdk/internal/marketdata"
	"github.com/kdraigo/dev_sdk/types"
)

var liveDay = time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

func lhm(h, m int) time.Time {
	return liveDay.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute)
}

// fakeDataAPI serves /positioning (5-minute periods from liveDay), /flow (1m
// bars that appear only once `ready` says so) and /funding, the way the data
// API answers: whole bars whose open lies in [from, to].
type fakeDataAPI struct {
	mu          sync.Mutex
	periods     int
	flowReady   func(open int64, calls int) bool
	unknown     map[string]bool
	funding     []map[string]any
	posCalls    atomic.Int32
	flowCalls   atomic.Int32
	lastFlowSym string
}

func (f *fakeDataAPI) server(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		from, _ := strconv.ParseInt(q.Get("from"), 10, 64)
		to, _ := strconv.ParseInt(q.Get("to"), 10, 64)
		if f.unknown[q.Get("symbol")] {
			if r.URL.Path == "/flow" {
				f.flowCalls.Add(1)
			}
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"symbol not collected"}`))
			return
		}
		var out []map[string]any
		switch r.URL.Path {
		case "/positioning":
			f.posCalls.Add(1)
			step := int64(5 * 60_000)
			if q.Get("timeframe") == "1h" {
				step = 3_600_000
			}
			for i := 0; i < f.periods; i++ {
				open := liveDay.Add(time.Duration(i) * 5 * time.Minute).UnixMilli()
				if open%step != 0 || open < from-from%step || open > to {
					continue
				}
				out = append(out, map[string]any{"open_time": open, "open_interest": float64(i), "periods": 1})
			}
		case "/flow":
			calls := int(f.flowCalls.Add(1))
			f.mu.Lock()
			f.lastFlowSym = q.Get("symbol")
			f.mu.Unlock()
			step := int64(60_000)
			if q.Get("timeframe") == "1h" {
				step = 3_600_000
			}
			for open := from - from%step; open <= to; open += step {
				if f.flowReady == nil || f.flowReady(open, calls) {
					out = append(out, map[string]any{"open_time": open, "buy_volume": 2.0, "sell_volume": 1.0, "delta": 1.0, "minutes": int(step / 60_000), "complete": true})
				}
			}
		case "/funding":
			for _, r := range f.funding {
				if ft := r["funding_time"].(int64); ft >= from && ft <= to {
					out = append(out, r)
				}
			}
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if out == nil {
			out = []map[string]any{}
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestLiveData(t *testing.T, api *fakeDataAPI, now time.Time, series ...types.MarketData) *liveData {
	t.Helper()
	l := newLiveData(marketdata.NewClient(api.server(t).URL), series)
	l.now = func() time.Time { return now }
	l.flowWaits = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	return l
}

func liveCandle(exchange string, open time.Time, tf types.Timeframe, d time.Duration) *types.Candle {
	return &types.Candle{Symbol: "BTC/USDT", Exchange: exchange, Timeframe: tf, OpenTime: open, CloseTime: open.Add(d - time.Millisecond), IsComplete: true}
}

func TestLive_CandleCarriesTheSameMarketDataAsABacktest(t *testing.T) {
	api := &fakeDataAPI{periods: 12 * 24}
	l := newTestLiveData(t, api, lhm(13, 0), types.MarketDataPositioning, types.MarketDataFlow)

	c := liveCandle("binance_futures", lhm(12, 59), types.Timeframe1m, time.Minute)
	l.enrich(context.Background(), c)
	require.NotNil(t, c.Positioning)
	assert.Equal(t, lhm(12, 55), c.Positioning.Time, "closing 12:59:59.999: the period ending 13:00 is not published until 13:01:30")
	require.NotNil(t, c.Flow)
	assert.Equal(t, lhm(12, 59), c.Flow.OpenTime)
	assert.Equal(t, 1.0, c.Flow.Delta)

	// The next minute needs no new positioning request: no newer period can be known yet.
	before := api.posCalls.Load()
	c = liveCandle("binance_futures", lhm(13, 0), types.Timeframe1m, time.Minute)
	l.enrich(context.Background(), c)
	assert.Equal(t, before, api.posCalls.Load())
	assert.Equal(t, lhm(12, 55), c.Positioning.Time)

	// 13:01 closes at 13:01:59.999, after 13:01:30: the 13:00 period is known.
	c = liveCandle("binance_futures", lhm(13, 1), types.Timeframe1m, time.Minute)
	l.enrich(context.Background(), c)
	assert.Equal(t, lhm(13, 0), c.Positioning.Time)
}

func TestLive_SpotHasNoPositioningAndFormingCandlesAreLeftAlone(t *testing.T) {
	api := &fakeDataAPI{periods: 100}
	l := newTestLiveData(t, api, lhm(5, 0), types.MarketDataPositioning, types.MarketDataFlow)

	c := liveCandle("binance", lhm(4, 0), types.Timeframe1m, time.Minute)
	l.enrich(context.Background(), c)
	assert.Nil(t, c.Positioning)
	assert.NotNil(t, c.Flow)
	assert.Zero(t, api.posCalls.Load())

	forming := liveCandle("binance_futures", lhm(4, 1), types.Timeframe1m, time.Minute)
	forming.IsComplete = false
	l.enrich(context.Background(), forming)
	assert.Nil(t, forming.Flow)
	assert.Nil(t, forming.Positioning)
}

// The platform writes a minute's flow a few seconds after it ends: the candle
// waits for it.
func TestLive_FlowWaitsForItsBar(t *testing.T) {
	api := &fakeDataAPI{flowReady: func(_ int64, calls int) bool { return calls >= 3 }}
	l := newTestLiveData(t, api, lhm(1, 0), types.MarketDataFlow)
	c := liveCandle("binance", lhm(0, 59), types.Timeframe1m, time.Minute)
	l.enrich(context.Background(), c)
	require.NotNil(t, c.Flow)
	assert.Equal(t, int32(3), api.flowCalls.Load())
}

// A market without flow stops costing the wait after a few candles.
func TestLive_FlowMissesStopTheWait(t *testing.T) {
	api := &fakeDataAPI{flowReady: func(int64, int) bool { return false }}
	l := newTestLiveData(t, api, lhm(1, 0), types.MarketDataFlow)
	for i := 0; i < flowMissesBeforeNoWait; i++ {
		c := liveCandle("binance", lhm(0, i), types.Timeframe1m, time.Minute)
		l.enrich(context.Background(), c)
		assert.Nil(t, c.Flow)
	}
	waited := api.flowCalls.Load()
	assert.Equal(t, int32(flowMissesBeforeNoWait*(1+len(l.flowWaits))), waited)

	c := liveCandle("binance", lhm(0, 10), types.Timeframe1m, time.Minute)
	l.enrich(context.Background(), c)
	assert.Equal(t, waited+1, api.flowCalls.Load(), "one try, no waiting")
}

func TestLive_UnknownMarketIsWarnedNotWaitedFor(t *testing.T) {
	api := &fakeDataAPI{unknown: map[string]bool{"BTC/USDT": true}}
	l := newTestLiveData(t, api, lhm(1, 0), types.MarketDataFlow)
	c := liveCandle("binance", lhm(0, 59), types.Timeframe1m, time.Minute)
	l.enrich(context.Background(), c)
	assert.Nil(t, c.Flow)
	assert.Equal(t, int32(1), api.flowCalls.Load(), "an error is an answer: no retries")
	assert.True(t, l.warned["flow|binance|BTC/USDT"])
}

func TestLive_Reads(t *testing.T) {
	api := &fakeDataAPI{periods: 12 * 24, funding: []map[string]any{
		{"symbol": "BTCUSDT", "funding_time": lhm(0, 0).UnixMilli() + 1, "funding_rate": 0.0001, "mark_price": 1.0, "interval_hours": 8},
		{"symbol": "BTCUSDT", "funding_time": lhm(8, 0).UnixMilli() + 1, "funding_rate": 0.0002, "mark_price": 1.0, "interval_hours": 8},
	}}
	now := lhm(13, 0).Add(30 * time.Second)
	l := newTestLiveData(t, api, now)
	ctx := context.Background()

	// A to in the future is read as now.
	rates, err := l.fundingRates(ctx, "binance_futures", "BTC/USDT", liveDay, liveDay.Add(48*time.Hour))
	require.NoError(t, err)
	require.Len(t, rates, 2)
	assert.Equal(t, 0.0002, rates[1].Rate)
	assert.Equal(t, 8, rates[1].IntervalHours)

	_, err = l.fundingRates(ctx, "binance", "BTC/USDT", liveDay, now)
	assert.ErrorContains(t, err, "funding exists only on perpetual exchanges")
	_, err = l.positioningBars(ctx, "bybit", "BTC/USDT", "", liveDay, now)
	assert.ErrorContains(t, err, "positioning exists only on perpetual exchanges")
	_, err = l.flowBars(ctx, "binance", "BTC/USDT", "", now.Add(time.Hour), now.Add(2*time.Hour))
	assert.ErrorContains(t, err, "is after")

	// Known by now (13:00:30): periods ending by 12:59:00, i.e. up to the one ending 12:55.
	pos, err := l.positioningBars(ctx, "binance_futures", "BTC/USDT", "", lhm(12, 0), now)
	require.NoError(t, err)
	require.NotEmpty(t, pos)
	assert.Equal(t, lhm(12, 55), pos[len(pos)-1].Time)
	hourly, err := l.positioningBars(ctx, "binance_futures", "BTC/USDT", types.Timeframe1h, lhm(10, 0), now)
	require.NoError(t, err)
	require.Len(t, hourly, 2, "10:00 and 11:00; the 12:00 hour ended at 13:00, not yet published")
	assert.Equal(t, lhm(12, 0), hourly[1].Time)

	// Closed by now: the 13:00 bar has not closed.
	flow, err := l.flowBars(ctx, "binance", "BTC/USDT", types.Timeframe1h, lhm(10, 0), now)
	require.NoError(t, err)
	require.Len(t, flow, 3)
	assert.Equal(t, lhm(12, 0), flow[2].OpenTime)
}

func TestValidateMarketData(t *testing.T) {
	assert.NoError(t, validateMarketData(nil))
	assert.NoError(t, validateMarketData([]types.MarketData{types.MarketDataFlow, types.MarketDataPositioning}))
	assert.ErrorContains(t, validateMarketData([]types.MarketData{"liquidations"}), `unknown "liquidations"`)
}

// The SDK sends a backtest's reads to the engine and a live run's to the data API.
func TestGetFundingRates_LiveGoesToTheDataAPI(t *testing.T) {
	api := &fakeDataAPI{funding: []map[string]any{
		{"symbol": "BTCUSDT", "funding_time": time.Now().Add(-time.Hour).UnixMilli(), "funding_rate": 0.0003, "mark_price": 1.0, "interval_hours": 8},
	}}
	srv := api.server(t)
	s := &SDK{config: &types.Config{Live: &types.LiveOptions{DataURL: srv.URL}}, adapter: stubAdapter{}}
	rates, err := s.GetFundingRates(context.Background(), "binance_futures", "BTC/USDT", time.Now().Add(-24*time.Hour), time.Now())
	require.NoError(t, err)
	require.Len(t, rates, 1)
	assert.Equal(t, 0.0003, rates[0].Rate)
}

// stubAdapter is a live adapter: no MarketDataReader.
type stubAdapter struct{ Adapter }
