package live

import (
	"errors"
	"testing"

	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// armedEnv is a Getenv that reports the machine as armed.
func armedEnv(key string) string {
	if key == ArmedEnvVar {
		return "1"
	}
	return ""
}

func emptyEnv(string) string { return "" }

// fullyArmedOpts is a configuration that passes every gate. Each test below
// breaks exactly one thing about it, so a test failing means that one gate
// stopped working — not that the fixture drifted.
func fullyArmedOpts() *types.LiveOptions {
	return &types.LiveOptions{
		RequestedExchanges: []string{types.ExchangeBinanceFutures},
		Assets:             []string{"BTCUSDT"},
		Armed:              true,
		MaxOrderNotional:   1000,
		MaxLeverage:        10,
	}
}

func smallIntent() *OrderIntent {
	return &OrderIntent{Type: IntentMarket, Symbol: "BTCUSDT", Side: types.OrderSideBuy, Quantity: 0.001}
}

// TestGuard_FullyArmedPasses establishes the baseline. Without it, a gate test
// that passes proves nothing — everything would be refused anyway.
func TestGuard_FullyArmedPasses(t *testing.T) {
	g := Guard{Env: types.EnvRealBinanceFutures, Opts: fullyArmedOpts(), Getenv: armedEnv}
	require.NoError(t, g.CheckSession())
	require.NoError(t, g.CheckOrder(smallIntent(), 50_000))
}

// Each subtest arms the other three gates and breaks one. A gate nobody has
// tried to bypass is not known to work.
func TestGuard_EachGateRefusesAlone(t *testing.T) {
	t.Run("gate 1: environment is not mainnet futures", func(t *testing.T) {
		// Testnet skips the arming gates by design, so the proof here is the
		// inverse: an unarmed config that mainnet refuses, testnet accepts.
		opts := fullyArmedOpts()
		opts.Armed = false
		g := Guard{Env: types.EnvTestBinanceFutures, Opts: opts, Getenv: emptyEnv}
		assert.NoError(t, g.CheckSession(), "testnet must not require arming")

		g.Env = types.EnvRealBinanceFutures
		assert.Error(t, g.CheckSession(), "the same config must be refused on mainnet")
	})

	t.Run("gate 2: Armed is false", func(t *testing.T) {
		opts := fullyArmedOpts()
		opts.Armed = false
		g := Guard{Env: types.EnvRealBinanceFutures, Opts: opts, Getenv: armedEnv}

		err := g.CheckOrder(smallIntent(), 50_000)
		require.Error(t, err)
		var notArmed *ErrNotArmed
		require.True(t, errors.As(err, &notArmed))
		assert.Equal(t, "armed", notArmed.Gate)
	})

	t.Run("gate 3: KDRAIGO_LIVE_ARMED absent", func(t *testing.T) {
		g := Guard{Env: types.EnvRealBinanceFutures, Opts: fullyArmedOpts(), Getenv: emptyEnv}

		err := g.CheckOrder(smallIntent(), 50_000)
		require.Error(t, err)
		var notArmed *ErrNotArmed
		require.True(t, errors.As(err, &notArmed))
		assert.Equal(t, "environment", notArmed.Gate)
	})

	t.Run("gate 3: KDRAIGO_LIVE_ARMED set to something other than 1", func(t *testing.T) {
		g := Guard{
			Env: types.EnvRealBinanceFutures, Opts: fullyArmedOpts(),
			Getenv: func(string) string { return "true" },
		}
		assert.Error(t, g.CheckSession(), "only the exact value 1 arms a machine")
	})

	t.Run("gate 4: notional over the cap", func(t *testing.T) {
		g := Guard{Env: types.EnvRealBinanceFutures, Opts: fullyArmedOpts(), Getenv: armedEnv}

		big := smallIntent()
		big.Quantity = 1 // 1 BTC at 50k is 50,000, well over the 1,000 cap
		err := g.CheckOrder(big, 50_000)
		require.Error(t, err)
		var notArmed *ErrNotArmed
		require.True(t, errors.As(err, &notArmed))
		assert.Equal(t, "cap", notArmed.Gate)
	})
}

// TestGuard_CapsAreRequiredNotDefaulted: a cap that defaults to something
// permissive means "I forgot to set one" and "I chose a high limit" are
// indistinguishable, and only one of them is safe.
func TestGuard_CapsAreRequiredNotDefaulted(t *testing.T) {
	t.Run("MaxOrderNotional unset", func(t *testing.T) {
		opts := fullyArmedOpts()
		opts.MaxOrderNotional = 0
		g := Guard{Env: types.EnvRealBinanceFutures, Opts: opts, Getenv: armedEnv}
		err := g.CheckSession()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "MaxOrderNotional")
	})

	t.Run("MaxLeverage unset", func(t *testing.T) {
		opts := fullyArmedOpts()
		opts.MaxLeverage = 0
		g := Guard{Env: types.EnvRealBinanceFutures, Opts: opts, Getenv: armedEnv}
		err := g.CheckSession()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "MaxLeverage")
	})

	t.Run("caps are enforced on testnet too", func(t *testing.T) {
		opts := fullyArmedOpts()
		opts.MaxOrderNotional = 0
		g := Guard{Env: types.EnvTestBinanceFutures, Opts: opts, Getenv: emptyEnv}
		assert.Error(t, g.CheckSession(),
			"caps only exercised on mainnet are caps nobody has tested")
	})

	t.Run("nil LiveOptions", func(t *testing.T) {
		g := Guard{Env: types.EnvTestBinanceFutures, Getenv: emptyEnv}
		assert.Error(t, g.CheckSession())
	})
}

func TestGuard_ConfiguredLeverageIsCappedAtSessionStart(t *testing.T) {
	opts := fullyArmedOpts()
	opts.Leverage = map[string]float64{"BTCUSDT": 25}
	g := Guard{Env: types.EnvTestBinanceFutures, Opts: opts, Getenv: emptyEnv}

	err := g.CheckSession()
	require.Error(t, err, "leverage over the cap must fail before a candle is streamed, not at the first order")
	assert.Contains(t, err.Error(), "MaxLeverage")
}

// TestGuard_UnvaluableOrderIsRefused: an order whose size cannot be checked is
// exactly the one the cap exists for, so it is refused rather than waved past.
func TestGuard_UnvaluableOrderIsRefused(t *testing.T) {
	g := Guard{Env: types.EnvTestBinanceFutures, Opts: fullyArmedOpts(), Getenv: emptyEnv}

	err := g.CheckOrder(smallIntent(), 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MaxOrderNotional cannot be enforced")
}

// TestGuard_TriggerPriceValuesAStop: a stop has no limit price, but it does
// have a trigger, and that is a fair valuation of the order.
func TestGuard_TriggerPriceValuesAStop(t *testing.T) {
	g := Guard{Env: types.EnvTestBinanceFutures, Opts: fullyArmedOpts(), Getenv: emptyEnv}

	intent := &OrderIntent{Type: IntentStopMarket, Symbol: "BTCUSDT", Quantity: 0.001, StopPrice: 50_000}
	assert.NoError(t, g.CheckOrder(intent, 0), "the trigger prices the order even with no reference")

	intent.Quantity = 1
	assert.Error(t, g.CheckOrder(intent, 0), "and it is still subject to the cap")
}

// TestGuard_SpotIsUntouched: this work fixes the spot order mapping, it does
// not add arming to spot. Requiring caps there would break every live spot
// strategy running today.
func TestGuard_SpotIsUntouched(t *testing.T) {
	for _, env := range []types.Environment{types.EnvRealBinance, types.EnvTestBinance, types.EnvRealBybit, types.EnvTestBybit} {
		g := Guard{Env: env, Opts: &types.LiveOptions{}, Getenv: emptyEnv}
		assert.NoError(t, g.CheckSession(), env)
		assert.NoError(t, g.CheckOrder(smallIntent(), 50_000), env)
	}
}

func TestIsMainnetAndIsFutures(t *testing.T) {
	assert.True(t, IsMainnet(types.EnvRealBinanceFutures))
	assert.True(t, IsMainnet(types.EnvRealBinance))
	assert.False(t, IsMainnet(types.EnvTestBinanceFutures))
	assert.False(t, IsMainnet(types.EnvBacktest))

	assert.True(t, IsFutures(types.EnvRealBinanceFutures))
	assert.True(t, IsFutures(types.EnvTestBinanceFutures))
	assert.False(t, IsFutures(types.EnvRealBinance))
}

func TestGuard_DryRun(t *testing.T) {
	assert.False(t, Guard{}.DryRun())
	assert.False(t, Guard{Opts: &types.LiveOptions{}}.DryRun())
	assert.True(t, Guard{Opts: &types.LiveOptions{DryRun: true}}.DryRun())
}
