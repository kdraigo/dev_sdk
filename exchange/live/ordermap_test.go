package live

import (
	"math"
	"testing"

	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMapOrder_EveryTypeIsDistinct is the regression test for the defect this
// package exists to prevent: both live adapters used to send every non-LIMIT
// order type as an immediate MARKET order, so a protective stop executed on
// placement. No two order types may resolve to the same intent shape.
func TestMapOrder_EveryTypeIsDistinct(t *testing.T) {
	cases := []struct {
		name      string
		req       types.OrderRequest
		wantType  IntentType
		wantPrice float64
		wantStop  float64
	}{
		{
			name:     "market",
			req:      types.OrderRequest{Type: types.OrderTypeMarket},
			wantType: IntentMarket,
		},
		{
			name:      "limit rests at price",
			req:       types.OrderRequest{Type: types.OrderTypeLimit, Price: 100},
			wantType:  IntentLimit,
			wantPrice: 100,
		},
		{
			name:     "stop loss is market-on-trigger, never immediate",
			req:      types.OrderRequest{Type: types.OrderTypeStopLoss, StopPrice: 90},
			wantType: IntentStopMarket,
			wantStop: 90,
		},
		{
			name:      "stop loss limit keeps both prices",
			req:       types.OrderRequest{Type: types.OrderTypeStopLossLimit, StopPrice: 90, Price: 89},
			wantType:  IntentStopLimit,
			wantPrice: 89,
			wantStop:  90,
		},
		{
			name:      "take profit limit rests",
			req:       types.OrderRequest{Type: types.OrderTypeTakeProfitLimit, Price: 120},
			wantType:  IntentTakeProfitLimit,
			wantPrice: 120,
		},
	}

	seen := map[IntentType]string{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := tc.req
			req.Symbol = "BTC/USDT"
			req.Side = types.OrderSideSell
			req.Quantity = 1

			intent, err := MapOrder(&req, MarketSpot)
			require.NoError(t, err)
			assert.Equal(t, tc.wantType, intent.Type)
			assert.Equal(t, tc.wantPrice, intent.Price, "resting price")
			assert.Equal(t, tc.wantStop, intent.StopPrice, "trigger price")
			assert.Equal(t, "BTCUSDT", intent.Symbol)

			if prev, dup := seen[intent.Type]; dup {
				t.Fatalf("%s and %s both resolve to %s — collapsing two order types into one is the bug this test guards", prev, tc.name, intent.Type)
			}
			seen[intent.Type] = tc.name
		})
	}
	assert.Len(t, seen, len(cases), "every SDK order type must have its own intent")
}

// TestMapOrder_StopPriceIsNeverDropped: StopPrice appeared nowhere in either
// live adapter, so a stop was placed with no trigger at all.
func TestMapOrder_StopPriceIsNeverDropped(t *testing.T) {
	for _, ot := range []types.OrderType{types.OrderTypeStopLoss, types.OrderTypeStopLossLimit} {
		req := &types.OrderRequest{
			Symbol: "BTCUSDT", Side: types.OrderSideSell, Quantity: 1,
			Type: ot, StopPrice: 55_000, Price: 54_900,
		}
		intent, err := MapOrder(req, MarketLinearPerp)
		require.NoError(t, err, ot)
		assert.Equal(t, 55_000.0, intent.StopPrice, "%s must carry its trigger", ot)
		assert.True(t, intent.Type.Triggered(), "%s must be a triggered intent", ot)
		assert.NotEqual(t, IntentMarket, intent.Type, "%s must never become an immediate market order", ot)
	}
}

// TestMapOrder_UnknownTypeIsRefused: the replaced code defaulted anything it did
// not recognise to MARKET. Refusing is the whole point.
func TestMapOrder_UnknownTypeIsRefused(t *testing.T) {
	req := &types.OrderRequest{
		Symbol: "BTCUSDT", Side: types.OrderSideBuy, Quantity: 1,
		Type: types.OrderType("TRAILING_STOP"),
	}
	_, err := MapOrder(req, MarketLinearPerp)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported order type")
}

func TestMapOrder_Validation(t *testing.T) {
	base := func() *types.OrderRequest {
		return &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Quantity: 1}
	}

	t.Run("resting order without a price", func(t *testing.T) {
		req := base()
		req.Type = types.OrderTypeLimit
		_, err := MapOrder(req, MarketSpot)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "positive Price")
	})

	t.Run("triggered order without a stop price", func(t *testing.T) {
		req := base()
		req.Type = types.OrderTypeStopLoss
		_, err := MapOrder(req, MarketSpot)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "positive StopPrice")
	})

	t.Run("zero quantity", func(t *testing.T) {
		req := base()
		req.Quantity = 0
		_, err := MapOrder(req, MarketSpot)
		require.Error(t, err)
	})

	t.Run("missing side", func(t *testing.T) {
		req := base()
		req.Side = ""
		_, err := MapOrder(req, MarketSpot)
		require.Error(t, err)
	})

	t.Run("reduce-only is refused on spot but accepted on perps", func(t *testing.T) {
		req := base()
		req.ReduceOnly = true
		_, err := MapOrder(req, MarketSpot)
		require.Error(t, err, "a cash balance has no position to reduce")

		intent, err := MapOrder(req, MarketLinearPerp)
		require.NoError(t, err)
		assert.True(t, intent.ReduceOnly)
	})

	t.Run("empty type stays a market order", func(t *testing.T) {
		req := base()
		req.Type = ""
		intent, err := MapOrder(req, MarketSpot)
		require.NoError(t, err)
		assert.Equal(t, IntentMarket, intent.Type)
	})
}

func TestMapOrder_TimeInForce(t *testing.T) {
	market, err := MapOrder(&types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Quantity: 1, Type: types.OrderTypeMarket}, MarketSpot)
	require.NoError(t, err)
	assert.Empty(t, market.TimeInForce, "a market order carries no TIF")

	limit, err := MapOrder(&types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Quantity: 1, Type: types.OrderTypeLimit, Price: 10}, MarketSpot)
	require.NoError(t, err)
	assert.Equal(t, "GTC", limit.TimeInForce)
}

func TestVenueSymbol(t *testing.T) {
	for in, want := range map[string]string{
		"BTC/USDT": "BTCUSDT",
		"BTCUSDT":  "BTCUSDT",
		"btc/usdt": "BTCUSDT",
		"eth/usdc": "ETHUSDC",
		"":         "",
	} {
		assert.Equal(t, want, VenueSymbol(in), in)
	}
}

// TestInstrumentFilter_QuantityFloors: rounding a size up trades more than the
// strategy asked for, and on a reduce-only exit the surplus is what flips a
// position instead of closing it.
func TestInstrumentFilter_QuantityFloors(t *testing.T) {
	f := InstrumentFilter{StepSize: 0.001, TickSize: 0.01}
	intent := &OrderIntent{Type: IntentMarket, Quantity: 1.23456}
	require.NoError(t, f.Apply(intent, 100))
	assert.InDelta(t, 1.234, intent.Quantity, 1e-12)
}

// TestInstrumentFilter_ExactStepSurvives guards the floating-point trap:
// 0.1/0.1 is 0.9999999999999999 in binary, and a naive Floor turns a quantity
// of exactly one step into zero.
func TestInstrumentFilter_ExactStepSurvives(t *testing.T) {
	for _, step := range []float64{0.1, 0.001, 0.00001, 1, 10} {
		intent := &OrderIntent{Type: IntentMarket, Quantity: step}
		f := InstrumentFilter{StepSize: step}
		require.NoError(t, f.Apply(intent, 1e9), "step %v", step)
		assert.InDelta(t, step, intent.Quantity, step*1e-9, "a quantity of exactly one step must survive")
	}
}

// TestInstrumentFilter_PricesRoundToNearest: flooring a protective SELL stop
// would push every stop further from the position it guards.
func TestInstrumentFilter_PricesRoundToNearest(t *testing.T) {
	f := InstrumentFilter{TickSize: 0.5, StepSize: 0.001}
	intent := &OrderIntent{Type: IntentStopLimit, Quantity: 1, Price: 100.4, StopPrice: 100.6}
	require.NoError(t, f.Apply(intent, 0))
	assert.InDelta(t, 100.5, intent.Price, 1e-9)
	assert.InDelta(t, 100.5, intent.StopPrice, 1e-9)
}

func TestInstrumentFilter_Minimums(t *testing.T) {
	t.Run("below min quantity", func(t *testing.T) {
		f := InstrumentFilter{StepSize: 0.001, MinQty: 0.01}
		intent := &OrderIntent{Type: IntentMarket, Quantity: 0.005}
		err := f.Apply(intent, 100)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "below the instrument minimum")
	})

	t.Run("below min notional, valued from the reference price", func(t *testing.T) {
		f := InstrumentFilter{StepSize: 0.001, MinNotional: 100}
		intent := &OrderIntent{Type: IntentMarket, Quantity: 0.001}
		err := f.Apply(intent, 50_000)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "notional")
	})

	t.Run("min notional valued from the limit price, not the reference", func(t *testing.T) {
		f := InstrumentFilter{StepSize: 0.001, TickSize: 0.01, MinNotional: 100}
		intent := &OrderIntent{Type: IntentLimit, Quantity: 0.01, Price: 50_000}
		// refPrice of 1 would value this at 0.01 and fail; the limit price wins.
		assert.NoError(t, f.Apply(intent, 1))
	})

	t.Run("rounding to zero is an error, not a zero-size order", func(t *testing.T) {
		f := InstrumentFilter{StepSize: 1}
		intent := &OrderIntent{Type: IntentMarket, Quantity: 0.4}
		err := f.Apply(intent, 100)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rounds to zero")
	})

	t.Run("no filters is a pass-through", func(t *testing.T) {
		intent := &OrderIntent{Type: IntentLimit, Quantity: 1.23456789, Price: 99.987654}
		require.NoError(t, InstrumentFilter{}.Apply(intent, 0))
		assert.Equal(t, 1.23456789, intent.Quantity)
		assert.Equal(t, 99.987654, intent.Price)
	})
}

// TestInstrumentFilter_Format: the spot adapters sent fmt.Sprintf("%f", …),
// which is six decimals whatever the instrument's precision is — too many for a
// symbol that steps in whole units, and the venue rejects it.
func TestInstrumentFilter_Format(t *testing.T) {
	f := InstrumentFilter{StepSize: 1, TickSize: 0.01}
	assert.Equal(t, "3", f.FormatQty(3))
	assert.Equal(t, "50000.00", f.FormatPrice(50000))
	assert.NotContains(t, f.FormatQty(3), ".", "a whole-unit step must not gain decimals")

	fine := InstrumentFilter{StepSize: 0.00001}
	assert.Equal(t, "0.00123", fine.FormatQty(0.00123))

	unknown := InstrumentFilter{}
	assert.Equal(t, "1.5", unknown.FormatQty(1.5), "an unknown step formats as needed, not at an invented precision")
}

func TestFloorAndNearest_NoDriftAcrossASweep(t *testing.T) {
	const step = 0.001
	for i := 1; i <= 500; i++ {
		v := float64(i) * step
		got := floorToStep(v, step)
		assert.InDelta(t, v, got, step*1e-6, "a value already on the grid must be unchanged (i=%d)", i)
		assert.False(t, math.IsNaN(got))
	}
}

// TestInstrumentFilter_ReduceOnlyIsExemptFromMinNotional
//
// Found by the first mainnet dry run: a reduce-only protective stop on a small
// position was refused locally for being below the instrument's 5 USDT
// minimum. The minimum exists to stop dust positions being *opened*; a
// reduce-only order can only shrink one. Refusing it leaves the position with
// no stop at all, which is the outcome the stop existed to prevent.
func TestInstrumentFilter_ReduceOnlyIsExemptFromMinNotional(t *testing.T) {
	f := InstrumentFilter{StepSize: 0.01, TickSize: 0.01, MinNotional: 5}

	entry := &OrderIntent{Type: IntentMarket, Quantity: 0.02}
	require.Error(t, f.Apply(entry, 100), "an opening order under the minimum is still refused")

	stop := &OrderIntent{Type: IntentStopMarket, Quantity: 0.02, StopPrice: 95, ReduceOnly: true}
	require.NoError(t, f.Apply(stop, 100),
		"a reduce-only stop guarding a small position must reach the venue")
	assert.InDelta(t, 95.0, stop.StopPrice, 1e-9, "and keep its trigger")
}
