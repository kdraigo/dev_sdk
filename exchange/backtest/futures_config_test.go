package backtest

import (
	"testing"

	"github.com/google/uuid"
	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func perpOpts() *types.BacktestOptions {
	return &types.BacktestOptions{
		RequestedExchanges: []string{"binance_futures"},
		WalletsByExchange: map[string]map[string]float64{
			"binance_futures": {"USDT": 10_000},
		},
		MarketTypeByExchange: map[string]string{"binance_futures": types.MarketTypeLinearPerp},
		LeverageByExchange:   map[string]float64{"binance_futures": 10},
	}
}

// TestBuildWallets_CarriesMarketTypeAndLeverage: without these on the wire the
// engine builds a spot wallet, and the run silently has no leverage, no
// funding and nothing that can ever liquidate.
func TestBuildWallets_CarriesMarketTypeAndLeverage(t *testing.T) {
	got, err := buildWallets(uuid.New(), perpOpts())
	require.NoError(t, err)
	require.Len(t, got, 1)

	assert.Equal(t, types.MarketTypeLinearPerp, got[0].MarketType)
	assert.InDelta(t, 10.0, got[0].Leverage, 1e-9)
	assert.Equal(t, "USDT", got[0].Asset)
}

// TestBuildWallets_SpotPayloadIsUnchanged is the compatibility guard: both new
// fields are omitempty, so a spot session must serialise exactly as before and
// keep working against an engine that predates them.
func TestBuildWallets_SpotPayloadIsUnchanged(t *testing.T) {
	got, err := buildWallets(uuid.New(), &types.BacktestOptions{
		RequestedExchanges: []string{"binance"},
		Wallets:            map[string]float64{"USDT": 1_000},
	})
	require.NoError(t, err)
	require.Len(t, got, 1)

	assert.Empty(t, got[0].MarketType, "an unset market type must not appear on the wire")
	assert.Zero(t, got[0].Leverage)
}

// TestBuildWallets_MixedSession: market type is per exchange, so one session
// can hold spot and futures at once.
func TestBuildWallets_MixedSession(t *testing.T) {
	got, err := buildWallets(uuid.New(), &types.BacktestOptions{
		RequestedExchanges: []string{"binance", "binance_futures"},
		WalletsByExchange: map[string]map[string]float64{
			"binance":         {"USDT": 5_000},
			"binance_futures": {"USDT": 2_000},
		},
		MarketTypeByExchange: map[string]string{"binance_futures": types.MarketTypeLinearPerp},
		LeverageByExchange:   map[string]float64{"binance_futures": 5},
	})
	require.NoError(t, err)
	require.Len(t, got, 2)

	// Sorted by exchange, which is what keeps the signed payload stable.
	assert.Equal(t, "binance", got[0].Exchange)
	assert.Empty(t, got[0].MarketType, "the spot leg stays spot")
	assert.Zero(t, got[0].Leverage)

	assert.Equal(t, "binance_futures", got[1].Exchange)
	assert.Equal(t, types.MarketTypeLinearPerp, got[1].MarketType)
	assert.InDelta(t, 5.0, got[1].Leverage, 1e-9)
}

// TestFuturesExchanges_SkipsSpot: a spot exchange holds no positions by
// construction, so querying it every bar would be pure waste.
func TestFuturesExchanges_SkipsSpot(t *testing.T) {
	e := &EngineClient{config: &types.Config{Backtest: &types.BacktestOptions{
		RequestedExchanges: []string{"binance", "bybit_futures", "binance_futures"},
		MarketTypeByExchange: map[string]string{
			"binance_futures": types.MarketTypeLinearPerp,
			"bybit_futures":   types.MarketTypeLinearPerp,
		},
	}}}

	assert.Equal(t, []string{"binance_futures", "bybit_futures"}, e.futuresExchanges(),
		"only perpetual wallets, in a stable order")
}

func TestFuturesExchanges_NoConfigIsEmpty(t *testing.T) {
	assert.Empty(t, (&EngineClient{}).futuresExchanges())
	assert.Empty(t, (&EngineClient{config: &types.Config{}}).futuresExchanges())
}

// TestResolveMarketType_DefaultsFromExchangeName guards the misconfiguration
// that produced no error: naming binance_futures in RequestedExchanges but
// forgetting MarketTypeByExchange gave a spot wallet running on perpetual data,
// with the wrong fees, no leverage and nothing that could liquidate.
func TestResolveMarketType_DefaultsFromExchangeName(t *testing.T) {
	cases := []struct {
		name       string
		exchange   string
		byExchange map[string]string
		want       string
	}{
		{
			name:     "a _futures venue defaults to perpetual",
			exchange: "binance_futures",
			want:     types.MarketTypeLinearPerp,
		},
		{
			name:     "so does bybit_futures",
			exchange: "bybit_futures",
			want:     types.MarketTypeLinearPerp,
		},
		{
			name:     "a spot venue stays unset, which the engine reads as spot",
			exchange: "binance",
			want:     "",
		},
		{
			name:       "an explicit entry always wins",
			exchange:   "binance_futures",
			byExchange: map[string]string{"binance_futures": types.MarketTypeSpot},
			want:       types.MarketTypeSpot,
		},
		{
			name:       "an explicit perp on a spot-named venue is still honoured",
			exchange:   "binance",
			byExchange: map[string]string{"binance": types.MarketTypeLinearPerp},
			want:       types.MarketTypeLinearPerp,
		},
		{
			name:       "an empty explicit entry does not override the default",
			exchange:   "binance_futures",
			byExchange: map[string]string{"binance_futures": ""},
			want:       types.MarketTypeLinearPerp,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, resolveMarketType(tc.exchange, tc.byExchange))
		})
	}
}
