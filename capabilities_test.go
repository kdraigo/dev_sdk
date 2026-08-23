package dev_sdk

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/exchange/backtest"
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
