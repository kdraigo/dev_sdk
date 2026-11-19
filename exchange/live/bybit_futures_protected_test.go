package live

import (
	"context"
	"testing"

	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A limit entry carries its stop and target to Bybit, on the grid, for the
// whole position at the fill, triggered on the mark and closed at market.
func TestBybitProtectedLimit(t *testing.T) {
	f := newFakeBybit(t)
	b := preparedBybit(t, f, bybitTestConfig(f))
	ctx := context.Background()

	_, err := b.PlaceProtectedLimit(ctx, &types.OrderRequest{Symbol: "BTC/USDT", Exchange: "bybit_futures", Side: types.OrderSideBuy,
		Type: types.OrderTypeLimit, Price: 99500.04, Quantity: 0.0104}, 98000.06, 103000.01)
	require.NoError(t, err)
	creates := f.callsTo("/v5/order/create")
	require.Len(t, creates, 1)
	body := creates[0].Body
	assert.Equal(t, "Limit", body["orderType"])
	assert.Equal(t, "Buy", body["side"])
	assert.Equal(t, "99500.0", body["price"])
	assert.Equal(t, "0.010", body["qty"])
	assert.Equal(t, "GTC", body["timeInForce"])
	assert.Equal(t, "98000.1", body["stopLoss"])
	assert.Equal(t, "103000.0", body["takeProfit"])
	assert.Equal(t, "Full", body["tpslMode"])
	assert.Equal(t, "MarkPrice", body["slTriggerBy"])
	assert.Equal(t, "MarkPrice", body["tpTriggerBy"])
	assert.Equal(t, "Market", body["slOrderType"])
	assert.Equal(t, "Market", body["tpOrderType"])
	assert.NotContains(t, body, "reduceOnly")

	// Refused before the venue: a stop on the wrong side, a missing target,
	// anything but an opening limit.
	for name, call := range map[string]func() error{
		"stop above a long's entry": func() error {
			_, err := b.PlaceProtectedLimit(ctx, &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Type: types.OrderTypeLimit, Price: 99500, Quantity: 0.01}, 99600, 103000)
			return err
		},
		"short's target above": func() error {
			_, err := b.PlaceProtectedLimit(ctx, &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideSell, Type: types.OrderTypeLimit, Price: 99500, Quantity: 0.01}, 101000, 99600)
			return err
		},
		"no target": func() error {
			_, err := b.PlaceProtectedLimit(ctx, &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Type: types.OrderTypeLimit, Price: 99500, Quantity: 0.01}, 98000, 0)
			return err
		},
		"market": func() error {
			_, err := b.PlaceProtectedLimit(ctx, &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Type: types.OrderTypeMarket, Quantity: 0.01}, 98000, 103000)
			return err
		},
	} {
		assert.Error(t, call(), name)
	}
	assert.Len(t, f.callsTo("/v5/order/create"), 1, "nothing more reached Bybit")
}

// The stop Bybit holds on a position: read from the position, moved with
// trading-stop (Full mode, on the mark, at market), on the grid.
func TestBybitPositionStop(t *testing.T) {
	f := newFakeBybit(t)
	f.routes["/v5/position/list"] = func(c fakeCall) fakeAnswer {
		return ok(map[string]any{"list": []any{map[string]any{"symbol": "BTCUSDT", "side": "Buy", "size": "0.01", "avgPrice": "64000",
			"liqPrice": "43000.5", "stopLoss": "63000", "positionIdx": 0}}})
	}
	f.routes["/v5/position/trading-stop"] = func(fakeCall) fakeAnswer { return ok(map[string]any{}) }
	b := preparedBybit(t, f, bybitTestConfig(f))
	ctx := context.Background()

	stop, err := b.PositionStop(ctx, "BTC/USDT")
	require.NoError(t, err)
	assert.Equal(t, 63000.0, stop)
	ps, err := b.GetPositions(ctx, "")
	require.NoError(t, err)
	require.Len(t, ps, 1)
	assert.Equal(t, 43000.5, ps[0].LiquidationPrice)

	require.NoError(t, b.SetPositionStop(ctx, "BTC/USDT", 64000.04))
	calls := f.callsTo("/v5/position/trading-stop")
	require.Len(t, calls, 1)
	assert.Equal(t, "64000.0", calls[0].Body["stopLoss"])
	assert.Equal(t, "Full", calls[0].Body["tpslMode"])
	assert.Equal(t, "MarkPrice", calls[0].Body["slTriggerBy"])
	assert.Equal(t, "Market", calls[0].Body["slOrderType"])

	assert.Error(t, b.SetPositionStop(ctx, "ETHUSDT", 3000), "not prepared")
}

// The target Bybit holds on a position: read from the position, moved with
// trading-stop (Full mode, on the mark, at market), on the grid, the stop
// left as it is.
func TestBybitPositionTarget(t *testing.T) {
	f := newFakeBybit(t)
	f.routes["/v5/position/list"] = func(c fakeCall) fakeAnswer {
		return ok(map[string]any{"list": []any{map[string]any{"symbol": "BTCUSDT", "side": "Buy", "size": "0.01", "avgPrice": "64000",
			"liqPrice": "43000.5", "stopLoss": "63000", "takeProfit": "67000", "positionIdx": 0}}})
	}
	f.routes["/v5/position/trading-stop"] = func(fakeCall) fakeAnswer { return ok(map[string]any{}) }
	b := preparedBybit(t, f, bybitTestConfig(f))
	ctx := context.Background()

	target, err := b.PositionTarget(ctx, "BTC/USDT")
	require.NoError(t, err)
	assert.Equal(t, 67000.0, target)

	require.NoError(t, b.SetPositionTarget(ctx, "BTC/USDT", 66500.04))
	calls := f.callsTo("/v5/position/trading-stop")
	require.Len(t, calls, 1)
	assert.Equal(t, "66500.0", calls[0].Body["takeProfit"])
	assert.Equal(t, "Full", calls[0].Body["tpslMode"])
	assert.Equal(t, "MarkPrice", calls[0].Body["tpTriggerBy"])
	assert.Equal(t, "Market", calls[0].Body["tpOrderType"])
	assert.NotContains(t, calls[0].Body, "stopLoss", "the stop is left as it is")

	assert.Error(t, b.SetPositionTarget(ctx, "ETHUSDT", 3000), "not prepared")
	assert.Error(t, b.SetPositionTarget(ctx, "BTCUSDT", 0))
}
