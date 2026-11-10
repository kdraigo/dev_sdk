package backtest

import (
	"testing"

	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithStress(t *testing.T) {
	maker := 0.0004
	own := &types.SimulationOptions{FillPolicy: "pessimistic", MakerFee: &maker}

	assert.Same(t, own, withStress(own), "no stress settings: the strategy's own options, untouched")
	assert.Nil(t, withStress(nil))

	t.Setenv(EnvSlippageBps, "5")
	t.Setenv(EnvFeeMult, "2")
	t.Setenv(EnvFunding, "off")
	got := withStress(own)
	require.NotNil(t, got)
	assert.Equal(t, 5.0, *got.SlippageBps)
	assert.Equal(t, 2.0, *got.FeeMult)
	assert.False(t, *got.FundingEnabled)
	assert.Equal(t, "pessimistic", got.FillPolicy, "the rest of the strategy's options stay")
	assert.Equal(t, 0.0004, *got.MakerFee)
	assert.Nil(t, own.SlippageBps, "the strategy's config itself is not changed")

	t.Setenv(EnvFeeMult, "not a number")
	t.Setenv(EnvFunding, "")
	got = withStress(nil)
	assert.Nil(t, got.FeeMult, "an unreadable value is ignored")
	assert.Equal(t, 5.0, *got.SlippageBps)
}
