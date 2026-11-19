package orderexec

import (
	"context"
	"errors"
	"testing"

	"github.com/kdraigo/dev_sdk/orderplan"
	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var actMarket = orderplan.Market{TickSize: 0.1, StepSize: 0.001, MinQty: 0.001, MinNotional: 5, MaxLeverage: 100, Mark: 65000}

func holding() *venue {
	return &venue{
		positions: []*types.Position{{Symbol: "BTCUSDT", Side: "LONG", Size: 0.01, EntryPrice: 64000, LiquidationPrice: 43000}},
		open: []*types.Order{
			{ID: "tp-1", Type: types.OrderTypeTakeProfitLimit, Side: types.OrderSideSell, Price: 67000, ReduceOnly: true},
			{ID: "sl-1", Type: types.OrderTypeStopLoss, Side: types.OrderSideSell, StopPrice: 63000, ReduceOnly: true},
			{ID: "e-2", Type: types.OrderTypeLimit, Side: types.OrderSideBuy, Quantity: 0.02, FilledQty: 0.005, Price: 62000},
		},
	}
}

func TestHoldings(t *testing.T) {
	v := holding()
	h, resting, exits, err := Holdings(context.Background(), v, "bybit_futures", "BTC/USDT")
	require.NoError(t, err)
	assert.Equal(t, &orderplan.Held{Side: orderplan.Long, Qty: 0.01, Entry: 64000, Stop: 63000, Target: 67000, Liquidation: 43000}, h)
	assert.Equal(t, []orderplan.Resting{{ID: "e-2", Side: orderplan.Long, Qty: 0.015, Price: 62000}}, resting)
	assert.Len(t, exits, 2)

	// A stop held on the position itself (Bybit Full mode).
	v = holding()
	v.open = nil
	s := stopperVenue{venue: v, stop: 63500}
	h, _, _, err = Holdings(context.Background(), s, "bybit_futures", "BTCUSDT")
	require.NoError(t, err)
	assert.Equal(t, 63500.0, h.Stop)

	v.positions = nil
	h, _, _, err = Holdings(context.Background(), v, "bybit_futures", "BTCUSDT")
	require.NoError(t, err)
	assert.Nil(t, h, "flat")
}

func closeAction() orderplan.Action {
	return orderplan.BuildClose("bybit_futures", "BTCUSDT", orderplan.Held{Side: orderplan.Long, Qty: 0.01, Entry: 64000}, actMarket)
}

// Closing: a reduce-only market order, then, once flat, the exits taken back.
func TestAct_Close(t *testing.T) {
	v := holding()
	v.flatOnClose = true
	res, err := Act(context.Background(), v, closeAction(), fast())
	require.NoError(t, err)
	assert.Equal(t, "close-1", res.OrderID)
	assert.Equal(t, 0.01, res.Filled)
	assert.Equal(t, []string{"tp-1", "sl-1"}, res.Cancelled)
	assert.Equal(t, []string{"close SELL MARKET 0.01 reduce-only", "cancel tp-1", "cancel sl-1"}, v.calls)

	// Still open after the order (it did not close it all): the exits stay.
	v = holding()
	_, err = Act(context.Background(), v, closeAction(), fast())
	assert.ErrorContains(t, err, "0.01 of BTCUSDT is still open after the closing order (it keeps its stop)")
	assert.Equal(t, []string{"close SELL MARKET 0.01 reduce-only"}, v.calls)

	v = holding()
	v.positions = nil
	_, err = Act(context.Background(), v, closeAction(), fast())
	assert.ErrorContains(t, err, "no position is open on BTCUSDT any more: nothing closed")
	assert.Empty(t, v.calls)
}

func TestAct_Cancel(t *testing.T) {
	a := orderplan.BuildCancel("bybit_futures", "BTCUSDT", orderplan.Resting{ID: "e-2", Side: orderplan.Long, Qty: 0.015, Price: 62000}, actMarket)
	v := holding()
	res, err := Act(context.Background(), v, a, fast())
	require.NoError(t, err)
	assert.Equal(t, []string{"e-2"}, res.Cancelled)

	v.cancelErr = map[string]error{"e-2": types.ErrOrderClosed}
	_, err = Act(context.Background(), v, a, fast())
	assert.ErrorContains(t, err, "the order e-2 no longer rests (it filled or was cancelled already)")
}

func moveAction(stop float64) orderplan.Action {
	return orderplan.BuildMoveStop("bybit_futures", "BTCUSDT", orderplan.Held{Side: orderplan.Long, Qty: 0.01, Entry: 64000, Stop: 63000}, stop, actMarket, orderplan.Limits{MinStopGap: 0.002})
}

// The new stop is placed before the old one goes: never without a stop.
func TestAct_MoveStop(t *testing.T) {
	v := holding()
	res, err := Act(context.Background(), v, moveAction(64000), fast())
	require.NoError(t, err)
	assert.Equal(t, "sl-new", res.OrderID)
	assert.Equal(t, []string{"sl-1"}, res.Cancelled)
	assert.Equal(t, []string{"stop SELL 0.01 @ 64000", "cancel sl-1"}, v.calls, "new first, then the old")

	// The new one fails: the old one stays.
	v = holding()
	v.stopErr = errors.New("would trigger immediately")
	_, err = Act(context.Background(), v, moveAction(64000), fast())
	assert.ErrorContains(t, err, "the new stop could not be placed (would trigger immediately): the old stop stays")
	assert.Equal(t, []string{"stop SELL 0.01 @ 64000"}, v.calls)

	// The old one cannot be taken back: said, not hidden.
	v = holding()
	v.cancelErr = map[string]error{"sl-1": errors.New("timeout")}
	res, err = Act(context.Background(), v, moveAction(64000), fast())
	require.NoError(t, err)
	assert.Contains(t, res.Notes[0], "two stops rest, and the nearer fires first")

	// No stop order: one held on the position is moved in place; without that, nothing.
	v = holding()
	v.open = nil
	s := stopperVenue{venue: v, stop: 63000}
	res, err = Act(context.Background(), s, moveAction(64000), fast())
	require.NoError(t, err)
	assert.Equal(t, []string{"set position stop BTCUSDT 64000"}, v.calls)
	assert.Contains(t, res.Notes[0], "moved the stop the exchange holds on the position")

	v = holding()
	v.open = nil
	_, err = Act(context.Background(), v, moveAction(64000), fast())
	assert.ErrorContains(t, err, "no stop rests on the exchange for the BTCUSDT position: nothing moved")
}

func TestAct_Refused(t *testing.T) {
	bad := moveAction(65500) // above the price for a long
	require.False(t, bad.OK)
	v := holding()
	_, err := Act(context.Background(), v, bad, fast())
	assert.ErrorContains(t, err, "did not pass its checks: nothing done")
	assert.Empty(t, v.calls)
}

// stopperVenue holds a stop on the position (Bybit Full mode).
type stopperVenue struct {
	*venue
	stop float64
}

func (s stopperVenue) PositionStop(context.Context, string) (float64, error) { return s.stop, nil }
func (s stopperVenue) SetPositionStop(_ context.Context, symbol string, stop float64) error {
	s.log("set position stop %s %g", symbol, stop)
	return nil
}

func targetAction(h orderplan.Held, target float64) orderplan.Action {
	return orderplan.BuildMoveTarget("bybit_futures", "BTCUSDT", h, target, actMarket, orderplan.Limits{MinStopGap: 0.002})
}

// The new target is placed before the old one goes: a failure leaves the old.
func TestAct_MoveTarget(t *testing.T) {
	held := orderplan.Held{Side: orderplan.Long, Qty: 0.01, Entry: 64000, Stop: 63000, Target: 67000}
	v := holding()
	res, err := Act(context.Background(), v, targetAction(held, 66000), fast())
	require.NoError(t, err)
	assert.Equal(t, "tp-new", res.OrderID)
	assert.Equal(t, []string{"tp-1"}, res.Cancelled)
	assert.Equal(t, []string{"target SELL 0.01 @ 66000", "cancel tp-1"}, v.calls, "new first, then the old")

	v = holding()
	v.targetErr = errors.New("price out of range")
	_, err = Act(context.Background(), v, targetAction(held, 66000), fast())
	assert.ErrorContains(t, err, "the new target could not be placed (price out of range): the old target stays")
	assert.Equal(t, []string{"target SELL 0.01 @ 66000"}, v.calls)

	v = holding()
	v.cancelErr = map[string]error{"tp-1": errors.New("timeout")}
	res, err = Act(context.Background(), v, targetAction(held, 66000), fast())
	require.NoError(t, err)
	assert.Contains(t, res.Notes[0], "two targets rest, and the nearer fills first")

	// No target at all: one is placed for the whole position.
	v = holding()
	v.open = v.open[1:] // the stop and the resting entry only
	res, err = Act(context.Background(), v, targetAction(orderplan.Held{Side: orderplan.Long, Qty: 0.01, Entry: 64000, Stop: 63000}, 66000), fast())
	require.NoError(t, err)
	assert.Equal(t, []string{"target SELL 0.01 @ 66000"}, v.calls)
	assert.Contains(t, res.Notes, "the position had no target: one was placed")

	// A target held on the position (Bybit Full mode) is moved in place.
	v = holding()
	v.open = nil
	tv := targeterVenue{venue: v, target: 67000}
	res, err = Act(context.Background(), tv, targetAction(held, 66000), fast())
	require.NoError(t, err)
	assert.Equal(t, []string{"set position target BTCUSDT 66000"}, v.calls)
	assert.Contains(t, res.Notes[0], "moved the target the exchange holds on the position")

	// A short's target goes below; flat, nothing is done.
	v = holding()
	v.positions = []*types.Position{{Symbol: "BTCUSDT", Side: "SHORT", Size: 0.02, EntryPrice: 66000}}
	v.open = nil
	_, err = Act(context.Background(), v, targetAction(orderplan.Held{Side: orderplan.Short, Qty: 0.02, Entry: 66000}, 63000), fast())
	require.NoError(t, err)
	assert.Equal(t, []string{"target BUY 0.02 @ 63000"}, v.calls)
	v = holding()
	v.positions = nil
	_, err = Act(context.Background(), v, targetAction(held, 66000), fast())
	assert.ErrorContains(t, err, "no position is open on BTCUSDT any more: no target set")
}

// targeterVenue holds a target on the position (Bybit Full mode).
type targeterVenue struct {
	*venue
	target float64
}

func (s targeterVenue) PositionTarget(context.Context, string) (float64, error) { return s.target, nil }
func (s targeterVenue) SetPositionTarget(_ context.Context, symbol string, target float64) error {
	s.log("set position target %s %g", symbol, target)
	return nil
}
