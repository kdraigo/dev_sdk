package dev_sdk

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/exchange/backtest"
	"github.com/kdraigo/dev_sdk/exchange/live"
	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEngineClientSatisfiesFuturesCapabilities is a compile-time contract.
//
// SetLeverage and GetPositions reach the engine through an interface assertion,
// so a signature drift would not fail the build — it would silently degrade to
// ErrUnsupportedByAdapter at runtime, and a strategy would read that as "this
// backend has no leverage" rather than "the SDK is broken".
func TestEngineClientSatisfiesFuturesCapabilities(t *testing.T) {
	var adapter interface{} = &backtest.EngineClient{}

	_, isSetter := adapter.(LeverageSetter)
	assert.True(t, isSetter, "EngineClient must implement LeverageSetter")

	_, isReader := adapter.(PositionReader)
	assert.True(t, isReader, "EngineClient must implement PositionReader")

	_, isBracket := adapter.(BracketPlacer)
	assert.True(t, isBracket, "and must keep implementing BracketPlacer")
}

// spotOnlyAdapter implements Adapter and nothing else — the shape every live
// adapter has today.
type spotOnlyAdapter struct{}

func (spotOnlyAdapter) PrepareSession(context.Context, *types.Config) error { return nil }
func (spotOnlyAdapter) ConnectStream(context.Context, chan<- *types.Candle, chan<- *types.Order) error {
	return nil
}
func (spotOnlyAdapter) PlaceOrder(context.Context, *types.OrderRequest) (*types.Order, error) {
	return nil, nil
}
func (spotOnlyAdapter) CancelOrder(context.Context, string, string, string) error { return nil }
func (spotOnlyAdapter) GetAccount(context.Context, string, string) (*types.Account, error) {
	return nil, nil
}
func (spotOnlyAdapter) Next(context.Context) error { return nil }
func (spotOnlyAdapter) GetHistoricalCandles(context.Context, string, string, time.Time, time.Time, types.Timeframe) ([]*types.Candle, error) {
	return nil, nil
}

// TestFuturesCapabilities_UnsupportedAdapter: an adapter without the capability
// gets a checkable error rather than a panic or a silent no-op, so a strategy
// can fall back deliberately.
func TestFuturesCapabilities_UnsupportedAdapter(t *testing.T) {
	sdk := &SDK{adapter: spotOnlyAdapter{}}
	ctx := context.Background()

	err := sdk.SetLeverage(ctx, "binance", "BTCUSDT", 10)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnsupportedByAdapter),
		"the caller must be able to test for it, not string-match it")

	positions, err := sdk.GetPositions(ctx, "binance")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnsupportedByAdapter))
	assert.Nil(t, positions)
}

// TestOrderRequest_ReduceOnlyDefaultsOff guards the default: reduce-only must
// be opt-in, or every existing strategy's entry orders would start refusing to
// open positions.
func TestOrderRequest_ReduceOnlyDefaultsOff(t *testing.T) {
	req := types.OrderRequest{Symbol: "BTCUSDT", Quantity: 1}
	assert.False(t, req.ReduceOnly)
}

// TestBinanceFuturesSatisfiesFuturesCapabilities is the live half of the
// contract above, and it matters more than the backtest half.
//
// SetLeverage and GetPositions reach the adapter through an interface
// assertion. A signature drift would not fail the build: SDK.SetLeverage would
// quietly start returning ErrUnsupportedByAdapter, and a strategy would read
// that as "Binance futures has no leverage" rather than "the SDK is broken" —
// then size every position as though it were unleveraged.
func TestBinanceFuturesSatisfiesFuturesCapabilities(t *testing.T) {
	var adapter interface{} = live.NewBinanceFuturesClient(&types.Config{})

	_, isSetter := adapter.(LeverageSetter)
	assert.True(t, isSetter, "BinanceFuturesClient must implement LeverageSetter")

	_, isReader := adapter.(PositionReader)
	assert.True(t, isReader, "BinanceFuturesClient must implement PositionReader")

	_, isBracket := adapter.(BracketPlacer)
	assert.True(t, isBracket, "BinanceFuturesClient must implement BracketPlacer")

	_, isAdapter := adapter.(Adapter)
	assert.True(t, isAdapter, "and must satisfy the base Adapter contract")
}

// TestSpotAdaptersStillDeclareNoFuturesCapabilities: spot must keep returning a
// checkable error rather than pretending. A live spot wallet has no leverage
// and no positions, and saying otherwise would be worse than saying nothing.
func TestSpotAdaptersStillDeclareNoFuturesCapabilities(t *testing.T) {
	for name, adapter := range map[string]interface{}{
		"binance spot": live.NewBinanceClient(&types.Config{}),
		"bybit spot":   live.NewBybitClient(&types.Config{}),
	} {
		_, isSetter := adapter.(LeverageSetter)
		assert.False(t, isSetter, "%s must not claim LeverageSetter", name)
		_, isReader := adapter.(PositionReader)
		assert.False(t, isReader, "%s must not claim PositionReader", name)
	}
}

// TestFuturesEnvironmentsSelectTheFuturesAdapter guards the switch in New: a
// futures environment falling through to the spot adapter would place spot
// orders on an account that also holds real spot funds.
func TestFuturesEnvironmentsSelectTheFuturesAdapter(t *testing.T) {
	for _, env := range []types.Environment{types.EnvRealBinanceFutures, types.EnvTestBinanceFutures} {
		sdk, err := New(&types.Config{
			Environment: env,
			Timeframes:  []types.Timeframe{types.Timeframe1m},
			Live:        &types.LiveOptions{Assets: []string{"BTCUSDT"}},
		})
		require.NoError(t, err, env)
		_, ok := sdk.adapter.(*live.BinanceFuturesClient)
		assert.True(t, ok, "%s must select the futures adapter, got %T", env, sdk.adapter)
	}

	spot, err := New(&types.Config{
		Environment: types.EnvRealBinance,
		Timeframes:  []types.Timeframe{types.Timeframe1m},
		Live:        &types.LiveOptions{Assets: []string{"BTCUSDT"}},
	})
	require.NoError(t, err)
	_, ok := spot.adapter.(*live.BinanceClient)
	assert.True(t, ok, "and real_binance must still select the spot adapter")
}
