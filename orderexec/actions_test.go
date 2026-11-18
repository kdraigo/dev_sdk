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
	assert.Equal(t, &orderplan.Held{Side: orderplan.Long, Qty: 0.01, Entry: 64000, Stop: 63000, Liquidation: 43000}, h)
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
