package live

import (
	"strings"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBybitFuturesStatusNeverInventsAFill(t *testing.T) {
	cases := map[string]types.OrderStatus{
		"Filled":                  types.OrderStatusFilled,
		"PartiallyFilled":         types.OrderStatusPartiallyFilled,
		"Cancelled":               types.OrderStatusCanceled,
		"Deactivated":             types.OrderStatusCanceled, // a stop cancelled before it fired
		"PartiallyFilledCanceled": types.OrderStatusCanceled,
		"Rejected":                types.OrderStatusRejected,
		"New":                     types.OrderStatusNew,
		"Created":                 types.OrderStatusNew,
		"Untriggered":             types.OrderStatusNew, // a resting stop
		"Triggered":               types.OrderStatusNew,
		"SomethingNew":            types.OrderStatusNew, // unknown: still live, never a fill
	}
	for in, want := range cases {
		assert.Equal(t, want, mapBybitFuturesStatus(in), in)
	}
}

func TestBybitTriggerDirectionFollowsTheSDKsStop(t *testing.T) {
	// A SELL stop guards a long: it fires as the price falls to it (2).
	d, err := bybitTriggerDirection(types.OrderSideSell)
	require.NoError(t, err)
	assert.Equal(t, 2, d)
	// A BUY stop guards a short: it fires as the price rises to it (1).
	d, err = bybitTriggerDirection(types.OrderSideBuy)
	require.NoError(t, err)
	assert.Equal(t, 1, d)
	_, err = bybitTriggerDirection("")
	assert.Error(t, err)
}

func TestBybitFuturesOrderTypes(t *testing.T) {
	for intent, want := range map[IntentType]string{
		IntentMarket:          "Market",
		IntentStopMarket:      "Market",
		IntentLimit:           "Limit",
		IntentStopLimit:       "Limit",
		IntentTakeProfitLimit: "Limit",
	} {
		got, err := bybitFuturesOrderType(intent)
		require.NoError(t, err)
		assert.Equal(t, want, got, intent)
	}
	_, err := bybitFuturesOrderType("TRAILING")
	assert.Error(t, err)
}

func TestSdkTypeFromBybit(t *testing.T) {
	assert.Equal(t, types.OrderTypeMarket, sdkTypeFromBybit("Market", 0, ""))
	assert.Equal(t, types.OrderTypeLimit, sdkTypeFromBybit("Limit", 0, ""))
	assert.Equal(t, types.OrderTypeStopLoss, sdkTypeFromBybit("Market", 95, "Stop"))
	assert.Equal(t, types.OrderTypeStopLossLimit, sdkTypeFromBybit("Limit", 95, "Stop"))
	assert.Equal(t, types.OrderTypeStopLoss, sdkTypeFromBybit("Market", 0, "StopLoss"))
}

func TestBybitLinkIDsAreUniqueAndShort(t *testing.T) {
	now := time.Unix(1_800_000_000, 123)
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := newBybitLinkID(now) // the same instant every time
		assert.LessOrEqual(t, len(id), bybitLinkIDMax)
		assert.False(t, seen[id], "duplicate %s", id)
		assert.False(t, strings.ContainsAny(id, " /:"), id)
		seen[id] = true
	}
}

func TestBybitPositionSide(t *testing.T) {
	assert.Equal(t, "LONG", bybitPositionSide("Buy"))
	assert.Equal(t, "SHORT", bybitPositionSide("Sell"))
	assert.Equal(t, "", bybitPositionSide(""))
	assert.Equal(t, "", bybitPositionSide("None"))
}

func TestBybitFuturesEnvironmentsAreGuarded(t *testing.T) {
	assert.True(t, IsFutures(types.EnvRealBybitFutures))
	assert.True(t, IsFutures(types.EnvTestBybitFutures))
	assert.True(t, IsMainnet(types.EnvRealBybitFutures))
	assert.False(t, IsMainnet(types.EnvTestBybitFutures))
	assert.False(t, IsBinanceFutures(types.EnvRealBybitFutures), "each adapter accepts only its own venue")
	assert.False(t, IsBybitFutures(types.EnvRealBinanceFutures))

	// The caps have no defaults on Bybit either.
	err := Guard{Env: types.EnvTestBybitFutures, Opts: &types.LiveOptions{}}.CheckSession()
	var notArmed *ErrNotArmed
	require.ErrorAs(t, err, &notArmed)
	assert.Equal(t, "cap", notArmed.Gate)

	// Mainnet needs Armed and KDRAIGO_LIVE_ARMED=1.
	opts := &types.LiveOptions{MaxOrderNotional: 100, MaxLeverage: 2}
	err = Guard{Env: types.EnvRealBybitFutures, Opts: opts, Getenv: func(string) string { return "1" }}.CheckSession()
	require.ErrorAs(t, err, &notArmed)
	assert.Equal(t, "armed", notArmed.Gate)
	opts.Armed = true
	assert.NoError(t, Guard{Env: types.EnvRealBybitFutures, Opts: opts, Getenv: func(string) string { return "1" }}.CheckSession())
	assert.Error(t, Guard{Env: types.EnvRealBybitFutures, Opts: opts, Getenv: func(string) string { return "" }}.CheckSession())
}
