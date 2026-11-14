package marketdata

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kdraigo/dev_sdk/types"
)

var day = time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

func hm(h, m int) time.Time {
	return day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute)
}

func TestBarEnd(t *testing.T) {
	month, err := types.ParseTimeframe(types.Timeframe("1M"))
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC), BarEnd(month, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)), "February's 28 days")
	assert.Equal(t, time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC), BarEnd(month, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))

	hour, _ := types.ParseTimeframe(types.Timeframe1h)
	assert.Equal(t, hm(13, 0), BarEnd(hour, hm(12, 0)))
}

func periods(from time.Time, n int) []PositioningWire {
	out := make([]PositioningWire, n)
	for i := range out {
		open := from.Add(time.Duration(i) * Period)
		out[i] = PositioningWire{OpenTime: open.UnixMilli(), OpenInterest: float64(i)}
	}
	return out
}

// The same rule as the backtest engine: a period counts from 90 s after its end.
func TestNewestKnown(t *testing.T) {
	bars := periods(hm(12, 0), 24)
	close := func(h, m int) time.Time { return hm(h, m).Add(-time.Millisecond) } // the last ms of the bar ending at h:m

	p := NewestKnown(bars, close(13, 0))
	require.NotNil(t, p)
	assert.Equal(t, hm(12, 55), p.Time, "a 1h candle closing at 13:00 carries the period ending 12:55")
	assert.Equal(t, hm(12, 50), p.OpenTime)

	p = NewestKnown(bars, close(13, 2))
	require.NotNil(t, p)
	assert.Equal(t, hm(13, 0), p.Time, "known from 13:01:30")

	assert.Nil(t, NewestKnown(bars, close(12, 6)), "nothing has been published yet at 12:06")
	assert.Nil(t, NewestKnown(bars, close(15, 0)), "the newest period (13:55) is over 30 minutes old")
	assert.Nil(t, NewestKnown(nil, close(13, 0)))
}

func TestPerpetual(t *testing.T) {
	assert.True(t, Perpetual("binance_futures"))
	assert.True(t, Perpetual("bybit_futures"))
	assert.False(t, Perpetual("binance"))
	assert.ErrorContains(t, PerpetualsOnly("funding", "bybit"), `not "bybit"`)
}
