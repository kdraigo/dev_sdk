package live

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A contract read for any symbol: rules, the lowest risk tier and the mark.
func TestBybitContract(t *testing.T) {
	f := newFakeBybit(t)
	f.routes["/v5/market/risk-limit"] = func(c fakeCall) fakeAnswer {
		return ok(map[string]any{"list": []any{
			map[string]any{"maintenanceMargin": "0.01", "maxLeverage": "50.00", "isLowestRisk": 0},
			map[string]any{"maintenanceMargin": "0.005", "maxLeverage": "75.00", "isLowestRisk": 1},
		}})
	}
	b := preparedBybit(t, f, bybitTestConfig(f))
	ctx := context.Background()

	c, err := b.Contract(ctx, "bybit_futures", "eth/usdt")
	require.NoError(t, err)
	assert.Equal(t, types.Contract{Exchange: "bybit_futures", Symbol: "ETHUSDT", TickSize: 0.1, StepSize: 0.001, MinQty: 0.001, MinNotional: 5,
		MaxLeverage: 75, MaintenanceMargin: 0.005, Mark: 100000}, *c, "the lowest tier's rate, and its max leverage under the contract's")

	// Without the tiers the rate is unknown, not invented.
	f.routes["/v5/market/risk-limit"] = func(fakeCall) fakeAnswer { return fakeAnswer{RetCode: 10001, RetMsg: "down"} }
	c, err = b.Contract(ctx, "bybit_futures", "ETHUSDT")
	require.NoError(t, err)
	assert.Zero(t, c.MaintenanceMargin)
	assert.Equal(t, 100.0, c.MaxLeverage)

	f.routes["/v5/market/instruments-info"] = func(c fakeCall) fakeAnswer {
		return ok(map[string]any{"list": []any{map[string]any{"symbol": "XUSDT", "status": "Settling", "contractType": "LinearPerpetual", "settleCoin": "USDT"}}})
	}
	_, err = b.Contract(ctx, "bybit_futures", "XUSDT")
	assert.ErrorContains(t, err, "not trading on Bybit")
}

// Closed P&L since a moment, over every page, signed.
func TestBybitRealizedPnL(t *testing.T) {
	f := newFakeBybit(t)
	since := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	f.routes["/v5/position/closed-pnl"] = func(c fakeCall) fakeAnswer {
		assert.Equal(t, "1794873600000", c.Query.Get("startTime"))
		if c.Query.Get("cursor") == "" {
			return ok(map[string]any{"list": []any{map[string]string{"closedPnl": "-12.5"}, map[string]string{"closedPnl": "4"}}, "nextPageCursor": "p2"})
		}
		return ok(map[string]any{"list": []any{map[string]string{"closedPnl": "-1.5"}}, "nextPageCursor": ""})
	}
	b := preparedBybit(t, f, bybitTestConfig(f))
	got, err := b.RealizedPnL(context.Background(), "bybit_futures", since)
	require.NoError(t, err)
	assert.InDelta(t, -10.0, got, 1e-9)
}

// The same reads on Binance: exchangeInfo, the mark, the first leverage
// bracket; income summed over closed trades, commissions and funding.
func TestBinanceContractAndRealized(t *testing.T) {
	brackets := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body any
		switch r.URL.Path {
		case "/fapi/v1/exchangeInfo":
			body = map[string]any{"symbols": []any{map[string]any{
				"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL",
				"filters": []any{
					map[string]any{"filterType": "PRICE_FILTER", "tickSize": "0.10", "minPrice": "0.1", "maxPrice": "1000000"},
					map[string]any{"filterType": "LOT_SIZE", "stepSize": "0.001", "minQty": "0.001", "maxQty": "1000"},
					map[string]any{"filterType": "MIN_NOTIONAL", "notional": "100"},
				},
			}}}
		case "/fapi/v1/premiumIndex":
			body = map[string]any{"symbol": r.URL.Query().Get("symbol"), "markPrice": "64998.5"}
		case "/fapi/v1/leverageBracket":
			if !brackets {
				w.WriteHeader(http.StatusUnauthorized)
				body = map[string]any{"code": -2015, "msg": "Invalid API-key"}
				break
			}
			body = map[string]any{"symbol": "BTCUSDT", "brackets": []any{
				map[string]any{"bracket": 2, "initialLeverage": 100, "maintMarginRatio": 0.005},
				map[string]any{"bracket": 1, "initialLeverage": 125, "maintMarginRatio": 0.004},
			}}
		case "/fapi/v1/income":
			body = []any{
				map[string]any{"asset": "USDT", "income": "-20", "incomeType": "REALIZED_PNL", "time": 1},
				map[string]any{"asset": "USDT", "income": "-0.8", "incomeType": "COMMISSION", "time": 2},
				map[string]any{"asset": "USDT", "income": "0.3", "incomeType": "FUNDING_FEE", "time": 3},
				map[string]any{"asset": "USDT", "income": "50", "incomeType": "TRANSFER", "time": 4},
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(srv.Close)

	b := NewBinanceFuturesClient(&types.Config{Environment: types.EnvTestBinanceFutures})
	ctx := context.Background()
	_, err := b.Contract(ctx, "binance_futures", "BTCUSDT")
	assert.ErrorIs(t, err, errNotPrepared)

	b.client = newFuturesClient(types.Credentials{APIKey: "k", APISecret: "s"})
	b.client.BaseURL = srv.URL
	c, err := b.Contract(ctx, "binance_futures", "BTC/USDT")
	require.NoError(t, err)
	assert.Equal(t, types.Contract{Exchange: "binance_futures", Symbol: "BTCUSDT", TickSize: 0.1, StepSize: 0.001, MinQty: 0.001, MinNotional: 100,
		MaxLeverage: 125, MaintenanceMargin: 0.004, Mark: 64998.5}, *c)

	brackets = false
	c, err = b.Contract(ctx, "binance_futures", "BTCUSDT")
	require.NoError(t, err, "the brackets are optional")
	assert.Zero(t, c.MaintenanceMargin)

	_, err = b.Contract(ctx, "binance_futures", "ETHUSDT")
	assert.ErrorContains(t, err, "no perpetual contract on Binance futures for ETHUSDT")

	got, err := b.RealizedPnL(ctx, "binance_futures", time.Now())
	require.NoError(t, err)
	assert.InDelta(t, -20.5, got, 1e-9, "transfers are not trading")
}
