package orderplan

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var actMarket = Market{TickSize: 0.1, StepSize: 0.001, MinQty: 0.001, MinNotional: 5, MaxLeverage: 100, Mark: 65000}
var actLimits = Limits{MinStopGap: 0.002}

func TestBuildClose(t *testing.T) {
	h := Held{Side: Long, Qty: 0.01, Entry: 64000, Stop: 63000}
	a := BuildClose("bybit_futures", "btcusdt", h, actMarket)
	require.True(t, a.OK, "%v", a.Checks)
	assert.Equal(t, ActClose, a.Kind)
	assert.Equal(t, "BTCUSDT", a.Symbol)
	assert.InDelta(t, 10.0, a.PnL, 1e-9, "(65,000 − 64,000) × 0.01")
	assert.Equal(t, ActionHash(a), a.Hash)
	assert.True(t, Passed(VerifyAction(a)))

	short := BuildClose("bybit_futures", "BTCUSDT", Held{Side: Short, Qty: 0.02, Entry: 66000}, actMarket)
	assert.InDelta(t, 20.0, short.PnL, 1e-9)

	flat := BuildClose("bybit_futures", "BTCUSDT", Held{}, actMarket)
	assert.False(t, flat.OK)
	assert.Equal(t, []string{"position"}, failedNames(flat.Checks))
}

func TestBuildCancel(t *testing.T) {
	a := BuildCancel("bybit_futures", "BTCUSDT", Resting{ID: "o-9", Side: Long, Qty: 0.01, Price: 64500}, actMarket)
	require.True(t, a.OK)
	assert.Equal(t, "o-9", a.OrderID)
	assert.True(t, Passed(VerifyAction(a)))
	assert.False(t, BuildCancel("bybit_futures", "BTCUSDT", Resting{}, actMarket).OK)
}

func TestBuildMoveStop(t *testing.T) {
	h := Held{Side: Long, Qty: 0.01, Entry: 64000, Stop: 63000, Liquidation: 43000}
	a := BuildMoveStop("bybit_futures", "BTCUSDT", h, 64000.04, actMarket, actLimits)
	require.True(t, a.OK, "%v", a.Checks)
	assert.Equal(t, 64000.0, a.Stop, "on the grid")
	assert.Equal(t, 63000.0, a.OldStop)
	assert.Equal(t, 0.0, a.PnL, "to the entry: nothing lost if it fills")
	assert.True(t, Passed(VerifyAction(a)))

	for name, c := range map[string]struct {
		h    Held
		stop float64
		want []string
	}{
		"above the price for a long": {h, 65100, []string{"side", "gap"}},
		"inside the gap":             {h, 64950, []string{"gap"}},
		"after the liquidation":      {h, 42000, []string{"liquidation"}},
		"not moved":                  {h, 63000, []string{"changed"}},
		"short below the price":      {Held{Side: Short, Qty: 0.01, Entry: 66000, Stop: 67000}, 64900, []string{"side", "gap"}},
	} {
		a := BuildMoveStop("bybit_futures", "BTCUSDT", c.h, c.stop, actMarket, actLimits)
		assert.False(t, a.OK, name)
		assert.Equal(t, c.want, failedNames(a.Checks), name)
	}
	short := BuildMoveStop("bybit_futures", "BTCUSDT", Held{Side: Short, Qty: 0.01, Entry: 66000, Stop: 67000}, 65800, actMarket, actLimits)
	require.True(t, short.OK, "%v", short.Checks)
	assert.InDelta(t, 2.0, short.PnL, 1e-9, "a short stopped at 65,800 from 66,000 makes 2")
}

// An action changed after it was built is not the one confirmed.
func TestVerifyAction(t *testing.T) {
	a := BuildClose("bybit_futures", "BTCUSDT", Held{Side: Long, Qty: 0.01, Entry: 64000}, actMarket)
	a.Qty = 1
	assert.Equal(t, []string{"unchanged"}, failedNames(VerifyAction(a)))
	b := BuildMoveStop("bybit_futures", "BTCUSDT", Held{Side: Long, Qty: 0.01, Entry: 64000}, 64500, actMarket, actLimits)
	b.Kind = "flip"
	b.Hash = ActionHash(b)
	assert.Contains(t, failedNames(VerifyAction(b)), "action")
}

// At carrying out: the position or the order must still be there, and a
// move's stop still on the protective side of the price now.
func TestRecheckAction(t *testing.T) {
	h := &Held{Side: Long, Qty: 0.01, Entry: 64000, Stop: 63000}
	closing := BuildClose("bybit_futures", "BTCUSDT", *h, actMarket)
	assert.True(t, Passed(RecheckAction(closing, h, nil, actMarket, actLimits)))
	assert.Equal(t, []string{"position"}, failedNames(RecheckAction(closing, nil, nil, actMarket, actLimits)), "already flat")
	assert.Equal(t, []string{"position"}, failedNames(RecheckAction(closing, &Held{Side: Short, Qty: 0.01}, nil, actMarket, actLimits)), "flipped")

	cancel := BuildCancel("bybit_futures", "BTCUSDT", Resting{ID: "o-9", Side: Long, Qty: 0.01, Price: 64500}, actMarket)
	assert.True(t, Passed(RecheckAction(cancel, nil, &Resting{ID: "o-9"}, actMarket, actLimits)))
	assert.Equal(t, []string{"order"}, failedNames(RecheckAction(cancel, nil, nil, actMarket, actLimits)))

	move := BuildMoveStop("bybit_futures", "BTCUSDT", *h, 64500, actMarket, actLimits)
	require.True(t, move.OK)
	assert.True(t, Passed(RecheckAction(move, h, nil, actMarket, actLimits)))
	fell := actMarket
	fell.Mark = 64450 // under the new stop now: it would fire at once
	assert.Equal(t, []string{"side", "gap"}, failedNames(RecheckAction(move, h, nil, fell, actLimits)))
	moved := move
	moved.Stop = 64400
	assert.Equal(t, []string{"unchanged"}, failedNames(RecheckAction(moved, h, nil, actMarket, actLimits)))
}

func TestBuildMoveTarget(t *testing.T) {
	h := Held{Side: Long, Qty: 0.01, Entry: 64000, Stop: 63000, Target: 66000}
	a := BuildMoveTarget("bybit_futures", "BTCUSDT", h, 66500.04, actMarket, actLimits)
	require.True(t, a.OK, "%v", a.Checks)
	assert.Equal(t, ActMoveTarget, a.Kind)
	assert.Equal(t, 66500.0, a.Target, "on the tick")
	assert.Equal(t, 66000.0, a.OldTarget)
	assert.InDelta(t, 25.0, a.PnL, 1e-9, "(66,500 − 64,000) × 0.01")
	assert.True(t, Passed(VerifyAction(a)))

	// A position with no target gets one.
	set := BuildMoveTarget("bybit_futures", "BTCUSDT", Held{Side: Short, Qty: 0.02, Entry: 66000}, 63000, actMarket, actLimits)
	require.True(t, set.OK, "%v", set.Checks)
	assert.Equal(t, 0.0, set.OldTarget)
	assert.InDelta(t, 60.0, set.PnL, 1e-9)

	// Refused: the wrong side of the price, at the price, unchanged, flat.
	assert.Equal(t, []string{"side", "gap"}, failedNames(BuildMoveTarget("bybit_futures", "BTCUSDT", h, 64900, actMarket, actLimits).Checks))
	assert.Equal(t, []string{"gap"}, failedNames(BuildMoveTarget("bybit_futures", "BTCUSDT", h, 65050, actMarket, actLimits).Checks), "0.08% away, under the 0.2% gap")
	assert.Equal(t, []string{"changed"}, failedNames(BuildMoveTarget("bybit_futures", "BTCUSDT", h, 66000, actMarket, actLimits).Checks))
	assert.Equal(t, []string{"side", "gap"}, failedNames(BuildMoveTarget("bybit_futures", "BTCUSDT", Held{Side: Short, Qty: 0.02, Entry: 66000}, 65100, actMarket, actLimits).Checks))
	assert.False(t, BuildMoveTarget("bybit_futures", "BTCUSDT", Held{}, 66500, actMarket, actLimits).OK)

	// The target is in the hash: a changed one is not the action confirmed.
	moved := a
	moved.Target = 67000
	assert.False(t, Passed(VerifyAction(moved)))
	assert.Equal(t, []string{"unchanged"}, failedNames(RecheckAction(moved, &h, nil, actMarket, actLimits)))

	// At acting: still open on the same side, still on the profit side of the price now.
	assert.True(t, Passed(RecheckAction(a, &Held{Side: Long, Qty: 0.01, Entry: 64000, Target: 66500}, nil, actMarket, actLimits)), "the target already there is fine")
	assert.Equal(t, []string{"position"}, failedNames(RecheckAction(a, nil, nil, actMarket, actLimits)))
	up := actMarket
	up.Mark = 66600
	assert.Contains(t, failedNames(RecheckAction(a, &h, nil, up, actLimits)), "side", "the price went past it")
}

// The other actions hash as before the target was added: an executor on an
// older SDK and a server on this one agree.
func TestActionHashUnchangedForOldKinds(t *testing.T) {
	old := func(a Action) string {
		s := strings.Join([]string{"a1", string(a.Kind), a.Venue, a.Symbol, string(a.Side), fmtStep(a.Qty, a.StepSize), a.OrderID,
			fmtStep(a.Price, a.TickSize), fmtStep(a.Stop, a.TickSize), fmtStep(a.Mark, a.TickSize)}, "|")
		sum := sha256.Sum256([]byte(s))
		return hex.EncodeToString(sum[:])
	}
	h := Held{Side: Long, Qty: 0.01, Entry: 64000, Stop: 63000, Target: 66000}
	for _, a := range []Action{
		BuildClose("bybit_futures", "BTCUSDT", h, actMarket),
		BuildCancel("bybit_futures", "BTCUSDT", Resting{ID: "o-9", Side: Long, Qty: 0.01, Price: 64500}, actMarket),
		BuildMoveStop("bybit_futures", "BTCUSDT", h, 64000, actMarket, actLimits),
	} {
		assert.Equal(t, old(a), a.Hash, string(a.Kind))
	}
	assert.NotEqual(t, old(BuildMoveTarget("bybit_futures", "BTCUSDT", h, 66500, actMarket, actLimits)), BuildMoveTarget("bybit_futures", "BTCUSDT", h, 66500, actMarket, actLimits).Hash)
}
