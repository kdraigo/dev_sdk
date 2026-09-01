package live

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/adshao/go-binance/v2/common"

	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// futuresAdapterFiles are every file the futures adapter is made of. A new one
// added without being listed here escapes the check below, which is why the
// list is asserted non-empty and each file is required to exist.
var futuresAdapterFiles = []string{
	"binance_futures.go",
	"binance_futures_stream.go",
}

// TestFuturesAdapterNeverConstructsASpotClient is the mechanical guarantee
// behind a claim that would otherwise be a promise.
//
// The Binance account this runs against holds real funds and open orders on
// spot. The futures wallet is a separate balance reached through /fapi, and the
// single mistake that could cross between them is a spot client constructed
// somewhere on the futures path. Reviewers miss that; grep does not.
func TestFuturesAdapterNeverConstructsASpotClient(t *testing.T) {
	require.NotEmpty(t, futuresAdapterFiles)

	for _, name := range futuresAdapterFiles {
		raw, err := os.ReadFile(name)
		require.NoError(t, err, "the futures adapter file %s must exist for this check to mean anything", name)
		src := string(raw)

		for _, forbidden := range []string{
			`"github.com/adshao/go-binance/v2"`,
			"binance.NewClient",
			"binance.UseTestnet",
			"newBinanceClient(",
		} {
			assert.NotContains(t, src, forbidden,
				"%s must never construct or configure a spot client (%s): the spot wallet holds real funds", name, forbidden)
		}
	}

	// And it must actually be built on the futures package, or the check above
	// would pass trivially for a file that does nothing.
	core, err := os.ReadFile("binance_futures.go")
	require.NoError(t, err)
	assert.Contains(t, string(core), `"github.com/adshao/go-binance/v2/futures"`)
}

// TestFuturesOrderTypesAreDistinctOnTheWire: the venue vocabulary must preserve
// the distinctions MapOrder made. Collapsing them here would reintroduce the
// original defect one layer lower down.
func TestFuturesOrderTypesAreDistinctOnTheWire(t *testing.T) {
	seen := map[string]IntentType{}
	for _, intent := range []IntentType{IntentMarket, IntentLimit, IntentStopMarket, IntentStopLimit} {
		got, err := binanceFuturesOrderType(intent)
		require.NoError(t, err, intent)
		wire := string(got)
		if prev, dup := seen[wire]; dup {
			t.Fatalf("%s and %s both send %q", prev, intent, wire)
		}
		seen[wire] = intent
	}

	assert.Equal(t, "STOP_MARKET", mustWire(t, IntentStopMarket),
		"go-binance declares no STOP_MARKET constant, but the venue accepts the string; falling back to MARKET here is the bug")
	assert.Equal(t, "STOP", mustWire(t, IntentStopLimit))
	assert.Equal(t, "MARKET", mustWire(t, IntentMarket))
}

func mustWire(t *testing.T, intent IntentType) string {
	t.Helper()
	got, err := binanceFuturesOrderType(intent)
	require.NoError(t, err)
	return string(got)
}

// TestSpotOrderTypesAreDistinctOnTheWire is the same guarantee for the spot
// adapters, which is where the defect actually lived.
func TestSpotOrderTypesAreDistinctOnTheWire(t *testing.T) {
	binMarket := mustSpotWire(t, IntentMarket)
	binStop := mustSpotWire(t, IntentStopMarket)
	binStopLimit := mustSpotWire(t, IntentStopLimit)

	assert.NotEqual(t, binMarket, binStop, "a STOP_LOSS must not go out as a MARKET order")
	assert.NotEqual(t, binMarket, binStopLimit)
	assert.Equal(t, "STOP_LOSS", binStop)
	assert.Equal(t, "STOP_LOSS_LIMIT", binStopLimit)

	bybitStop, err := bybitSpotOrderType(IntentStopMarket)
	require.NoError(t, err)
	// Bybit expresses the trigger through orderFilter=StopOrder rather than a
	// distinct order type, so the type is legitimately Market here; the
	// distinction lives in the params PlaceOrder sets alongside it.
	assert.Equal(t, "Market", string(bybitStop))
}

func mustSpotWire(t *testing.T, intent IntentType) string {
	t.Helper()
	got, err := binanceSpotOrderType(intent)
	require.NoError(t, err)
	return string(got)
}

// TestFuturesPrepareSessionRefusesBeforeTouchingTheNetwork: an unarmed mainnet
// config must fail at the gate, not after it has authenticated and started
// changing account settings.
func TestFuturesPrepareSessionRefusesBeforeTouchingTheNetwork(t *testing.T) {
	cfg := &types.Config{
		Environment: types.EnvRealBinanceFutures,
		Live: &types.LiveOptions{
			Assets:           []string{"BTCUSDT"},
			MaxOrderNotional: 100,
			MaxLeverage:      5,
			// Armed deliberately false.
		},
		Credentials: types.Credentials{APIKey: "unused", APISecret: "unused"},
	}
	c := NewBinanceFuturesClient(cfg)

	err := c.PrepareSession(t.Context(), cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Armed")
	assert.Nil(t, c.client, "it must not have built a client, let alone called the venue")
}

// TestFuturesPlaceOrderRefusesUnmappableRequests exercises the order path down
// to the point where it would hit the network, with no client configured — so
// reaching the network at all would panic and fail the test.
func TestFuturesPlaceOrderRefusesUnmappableRequests(t *testing.T) {
	cfg := &types.Config{
		Environment: types.EnvTestBinanceFutures,
		Live: &types.LiveOptions{
			Assets: []string{"BTCUSDT"}, MaxOrderNotional: 1_000_000, MaxLeverage: 20,
		},
	}
	c := NewBinanceFuturesClient(cfg)

	cases := map[string]*types.OrderRequest{
		"stop with no trigger": {
			Symbol: "BTCUSDT", Side: types.OrderSideSell, Quantity: 1, Type: types.OrderTypeStopLoss,
		},
		"limit with no price": {
			Symbol: "BTCUSDT", Side: types.OrderSideBuy, Quantity: 1, Type: types.OrderTypeLimit,
		},
		"unknown type": {
			Symbol: "BTCUSDT", Side: types.OrderSideBuy, Quantity: 1, Type: types.OrderType("TWAP"),
		},
		"zero quantity": {
			Symbol: "BTCUSDT", Side: types.OrderSideBuy, Quantity: 0, Type: types.OrderTypeMarket,
		},
	}

	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := c.PlaceOrder(t.Context(), req)
			require.Error(t, err)
			assert.True(t, strings.HasPrefix(err.Error(), "binance futures:"), err.Error())
		})
	}
}

// TestFuturesMarketOrderWithoutAMarkIsRefused: a market order is the one type
// with no price of its own, so with no mark the notional cap cannot be applied
// — and that is precisely the order the cap exists for.
func TestFuturesMarketOrderWithoutAMarkIsRefused(t *testing.T) {
	cfg := &types.Config{
		Environment: types.EnvTestBinanceFutures,
		Live: &types.LiveOptions{
			Assets: []string{"BTCUSDT"}, MaxOrderNotional: 1000, MaxLeverage: 20,
		},
	}
	c := NewBinanceFuturesClient(cfg)

	_, err := c.PlaceOrder(t.Context(), &types.OrderRequest{
		Symbol: "BTCUSDT", Side: types.OrderSideBuy, Quantity: 0.001, Type: types.OrderTypeMarket,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MaxOrderNotional cannot be enforced")

	// With a mark it is priced, and a small order passes the cap while a large
	// one does not.
	c.setMark("BTCUSDT", 50_000)
	_, err = c.PlaceOrder(t.Context(), &types.OrderRequest{
		Symbol: "BTCUSDT", Side: types.OrderSideBuy, Quantity: 1, Type: types.OrderTypeMarket,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds MaxOrderNotional")
}

// TestFuturesDryRunNeverReachesTheVenue: the client is nil, so any call that
// actually tried to send would panic.
func TestFuturesDryRunNeverReachesTheVenue(t *testing.T) {
	cfg := &types.Config{
		Environment: types.EnvTestBinanceFutures,
		Live: &types.LiveOptions{
			Assets: []string{"BTCUSDT"}, MaxOrderNotional: 1_000_000, MaxLeverage: 20, DryRun: true,
		},
	}
	c := NewBinanceFuturesClient(cfg)
	c.setMark("BTCUSDT", 50_000)

	order, err := c.PlaceOrder(t.Context(), &types.OrderRequest{
		Symbol: "BTC/USDT", Side: types.OrderSideSell, Quantity: 0.01,
		Type: types.OrderTypeStopLoss, StopPrice: 49_000, ReduceOnly: true,
	})
	require.NoError(t, err)
	assert.Contains(t, order.ID, "dryrun-")
	assert.Equal(t, types.OrderTypeStopLoss, order.Type, "the strategy's stated type survives")
	assert.Equal(t, 49_000.0, order.StopPrice)
	assert.Equal(t, types.ExchangeBinanceFutures, order.Exchange)

	require.NoError(t, c.CancelOrder(t.Context(), types.ExchangeBinanceFutures, "BTC/USDT", order.ID))
}

// TestFuturesReduceOnlySurvivesToTheIntent: reduce-only is what stops an
// oversized protective stop from flipping a position instead of closing it.
func TestFuturesReduceOnlySurvivesToTheIntent(t *testing.T) {
	intent, err := MapOrder(&types.OrderRequest{
		Symbol: "BTC/USDT", Side: types.OrderSideSell, Quantity: 5,
		Type: types.OrderTypeStopLoss, StopPrice: 100, ReduceOnly: true,
	}, MarketLinearPerp)
	require.NoError(t, err)
	assert.True(t, intent.ReduceOnly)
	assert.Equal(t, IntentStopMarket, intent.Type)
}

// TestSdkOrderTypeRoundTrip: a fill arriving on the user data stream must be
// described the same way the PlaceOrder that created it was, or a strategy
// matching on Type in OnOrderUpdate never recognises its own order.
func TestSdkOrderTypeRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		wire string
		want types.OrderType
	}{
		{"MARKET", types.OrderTypeMarket},
		{"LIMIT", types.OrderTypeLimit},
		{"STOP_MARKET", types.OrderTypeStopLoss},
		{"STOP", types.OrderTypeStopLossLimit},
		{"LIQUIDATION", types.OrderTypeMarket},
	} {
		assert.Equal(t, tc.want, sdkOrderType(tc.wire, false), tc.wire)
	}
}

func TestBracketGroupIDIsStableWhicheverLegReports(t *testing.T) {
	assert.Equal(t, bracketGroupID("100", "200"), bracketGroupID("200", "100"),
		"both legs of one bracket must report the same GroupID")
}

// TestExplainOrderError_ConditionalRejection: Binance answers a refused stop
// with "Please use the Algo Order API endpoints instead", which points at a
// TWAP/VP service that does not place protective stops. Passing that through
// verbatim would send whoever reads the log in the wrong direction.
func TestExplainOrderError_ConditionalRejection(t *testing.T) {
	venueErr := &common.APIError{Code: -4120, Message: "Order type not supported for this endpoint. Please use the Algo Order API endpoints instead."}

	stop := &OrderIntent{Type: IntentStopMarket}
	got := explainOrderError(venueErr, stop)
	require.Error(t, got)
	assert.True(t, errors.Is(got, ErrConditionalOrdersUnavailable),
		"a caller must be able to test for this and fall back deliberately")
	assert.Contains(t, got.Error(), "NOT converted to a market order")

	// The same code on a non-triggered order is not this condition, and must
	// not be relabelled as it.
	market := &OrderIntent{Type: IntentMarket}
	assert.False(t, errors.Is(explainOrderError(venueErr, market), ErrConditionalOrdersUnavailable))

	// An unrelated error passes through untouched.
	other := &common.APIError{Code: -2019, Message: "Margin is insufficient."}
	assert.False(t, errors.Is(explainOrderError(other, stop), ErrConditionalOrdersUnavailable))
}

// TestFuturesAdapterRefusesForeignEnvironments: the adapter is exported, so a
// direct caller must not be able to run it under an environment the guard
// treats as out of scope — which would skip every gate.
func TestFuturesAdapterRefusesForeignEnvironments(t *testing.T) {
	for _, env := range []types.Environment{types.EnvBacktest, types.EnvRealBinance, types.EnvTestBybit, ""} {
		cfg := &types.Config{Environment: env} // Live deliberately nil
		err := NewBinanceFuturesClient(cfg).PrepareSession(t.Context(), cfg)
		require.Error(t, err, "environment %q", env)
		assert.Contains(t, err.Error(), "cannot run environment")
	}
}
