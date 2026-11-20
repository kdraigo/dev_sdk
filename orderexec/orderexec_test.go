package orderexec

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/orderplan"
	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// venue is an in-memory futures venue. Each step can be told to fail, and
// every call is logged in order.
type venue struct {
	mu        sync.Mutex
	calls     []string
	contract  types.Contract
	free      float64
	positions []*types.Position
	realized  float64
	since     time.Time

	leverageErr error
	entryErr    error
	bracketErr  error
	closeErr    error
	// entry is what the entry answers; reads is what each later read of it
	// answers (the last one repeats).
	entry types.Order
	reads []types.Order
	n     int

	// open is what rests on the contract; a filled reduce-only market order
	// flattens the position when flatOnClose is set.
	open        []*types.Order
	flatOnClose bool
	cancelErr   map[string]error
	stopErr     error
	targetErr   error
}

func (v *venue) ListOpenOrders(context.Context, string, string) ([]*types.Order, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]*types.Order(nil), v.open...), nil
}

func (v *venue) log(format string, a ...any) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.calls = append(v.calls, fmt.Sprintf(format, a...))
}

func (v *venue) PlaceOrder(_ context.Context, r *types.OrderRequest) (*types.Order, error) {
	if r.ReduceOnly && r.Type == types.OrderTypeStopLoss {
		v.log("stop %s %g @ %g", r.Side, r.Quantity, r.StopPrice)
		if v.stopErr != nil {
			return nil, v.stopErr
		}
		return &types.Order{ID: "sl-new", Type: types.OrderTypeStopLoss, StopPrice: r.StopPrice, ReduceOnly: true}, nil
	}
	if r.ReduceOnly && r.Type == types.OrderTypeTakeProfitLimit {
		v.log("target %s %g @ %g", r.Side, r.Quantity, r.Price)
		if v.targetErr != nil {
			return nil, v.targetErr
		}
		return &types.Order{ID: "tp-new", Type: types.OrderTypeTakeProfitLimit, Price: r.Price, ReduceOnly: true}, nil
	}
	if r.ReduceOnly {
		v.log("close %s %s %g reduce-only", r.Side, r.Type, r.Quantity)
		if v.closeErr != nil {
			return nil, v.closeErr
		}
		if v.flatOnClose {
			v.mu.Lock()
			v.positions = nil
			v.mu.Unlock()
		}
		return &types.Order{ID: "close-1", Status: types.OrderStatusFilled, Quantity: r.Quantity, FilledQty: r.Quantity, AveragePrice: 65500}, nil
	}
	v.log("entry %s %s %g %s", r.Side, r.Type, r.Quantity, r.Symbol)
	if v.entryErr != nil {
		return nil, v.entryErr
	}
	o := v.entry
	return &o, nil
}

func (v *venue) CancelOrder(_ context.Context, _, _, id string) error {
	v.log("cancel %s", id)
	if err := v.cancelErr[id]; err != nil {
		return err
	}
	return nil
}

func (v *venue) GetOrder(_ context.Context, _, _, id string) (*types.Order, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.reads) == 0 {
		o := v.entry
		return &o, nil
	}
	o := v.reads[min(v.n, len(v.reads)-1)]
	v.n++
	return &o, nil
}

func (v *venue) GetAccount(_ context.Context, exchange, asset string) (*types.Account, error) {
	return &types.Account{Exchange: exchange, Balances: []types.Balance{{Asset: "BNB", Free: 3}, {Asset: "USDT", Free: v.free}}}, nil
}

func (v *venue) GetPositions(context.Context, string) ([]*types.Position, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.positions, nil
}

func (v *venue) SetLeverage(_ context.Context, _, pair string, lev float64) error {
	v.log("leverage %s %g", pair, lev)
	return v.leverageErr
}

func (v *venue) PlaceBracket(_ context.Context, r *types.BracketRequest) ([]*types.Order, error) {
	v.log("bracket %s %g tp %g sl %g", r.Side, r.Quantity, r.TakeProfitPrice, r.StopPrice)
	if v.bracketErr != nil {
		return nil, v.bracketErr
	}
	return []*types.Order{{ID: "tp-1", Type: types.OrderTypeTakeProfitLimit}, {ID: "sl-1", Type: types.OrderTypeStopLoss, StopPrice: r.StopPrice}}, nil
}

func (v *venue) Contract(_ context.Context, exchange, symbol string) (*types.Contract, error) {
	c := v.contract
	return &c, nil
}

func (v *venue) RealizedPnL(_ context.Context, _ string, since time.Time) (float64, error) {
	v.since = since
	return v.realized, nil
}

func ticket(side orderplan.Side) orderplan.Ticket {
	t := orderplan.Ticket{Venue: "bybit_futures", Symbol: "BTCUSDT", Side: side, EntryType: orderplan.AtMarket,
		Qty: 0.01, Stop: 64000, Target: 67000, Leverage: 3, OK: true}
	if side == orderplan.Short {
		t.Stop, t.Target = 66000, 63000
	}
	return t
}

var filled = types.Order{ID: "e-1", Status: types.OrderStatusFilled, Quantity: 0.01, FilledQty: 0.01, AveragePrice: 65010}

func fast() Options { return Options{FillWait: 30 * time.Millisecond, Poll: 5 * time.Millisecond} }

func TestSnapshot(t *testing.T) {
	v := &venue{
		contract: types.Contract{TickSize: 0.1, StepSize: 0.001, MinQty: 0.001, MinNotional: 5, MaxLeverage: 100, MaintenanceMargin: 0.005, Mark: 65000},
		free:     480,
		positions: []*types.Position{
			{Symbol: "BTCUSDT", Side: "LONG", Size: 0.01},
			{Symbol: "ETHUSDT", Side: "SHORT", Size: 0.2},
			{Symbol: "SOLUSDT", Size: 0},
		},
		realized: -30,
	}
	now := time.Date(2026, 11, 17, 15, 4, 5, 0, time.UTC)
	m, a, err := Snapshot(context.Background(), v, "bybit_futures", "BTC/USDT", now)
	require.NoError(t, err)
	assert.Equal(t, orderplan.Market{TickSize: 0.1, StepSize: 0.001, MinQty: 0.001, MinNotional: 5, MaxLeverage: 100, MaintenanceMargin: 0.005, Mark: 65000}, m)
	assert.Equal(t, orderplan.Account{Available: 480, OpenPositions: 2, PositionOnSymbol: true, LossToday: 30}, a)
	assert.Equal(t, time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC), v.since, "today is since 00:00 UTC")

	v.realized, v.positions = 12, nil
	_, a, err = Snapshot(context.Background(), v, "bybit_futures", "BTCUSDT", now)
	require.NoError(t, err)
	assert.Equal(t, orderplan.Account{Available: 480}, a, "a gain is no loss")
}

func TestOpen_Protected(t *testing.T) {
	v := &venue{entry: filled}
	res, err := Open(context.Background(), v, ticket(orderplan.Long), fast())
	require.NoError(t, err)
	assert.Equal(t, &Result{EntryID: "e-1", Filled: 0.01, AvgPrice: 65010, StopID: "sl-1", TargetID: "tp-1", Protected: true}, res)
	assert.Equal(t, []string{"leverage BTCUSDT 3", "entry BUY MARKET 0.01 BTCUSDT", "bracket SELL 0.01 tp 67000 sl 64000"}, v.calls)
}

func TestOpen_Short(t *testing.T) {
	v := &venue{entry: filled}
	_, err := Open(context.Background(), v, ticket(orderplan.Short), fast())
	require.NoError(t, err)
	assert.Equal(t, []string{"leverage BTCUSDT 3", "entry SELL MARKET 0.01 BTCUSDT", "bracket BUY 0.01 tp 63000 sl 66000"}, v.calls)
}

// What filled is protected; the rest is cancelled.
func TestOpen_PartialFill(t *testing.T) {
	v := &venue{
		entry: types.Order{ID: "e-1", Status: types.OrderStatusNew, Quantity: 0.01},
		reads: []types.Order{
			{ID: "e-1", Status: types.OrderStatusPartiallyFilled, Quantity: 0.01, FilledQty: 0.004, AveragePrice: 65000},
		},
	}
	res, err := Open(context.Background(), v, ticket(orderplan.Long), fast())
	require.NoError(t, err)
	assert.True(t, res.Protected)
	assert.Equal(t, 0.004, res.Filled)
	assert.Contains(t, res.Notes[0], "the entry filled 0.004 of 0.01")
	assert.Equal(t, []string{"leverage BTCUSDT 3", "entry BUY MARKET 0.01 BTCUSDT", "cancel e-1", "bracket SELL 0.004 tp 67000 sl 64000"}, v.calls)
}

func TestOpen_NothingFilled(t *testing.T) {
	v := &venue{entry: types.Order{ID: "e-1", Status: types.OrderStatusRejected, Quantity: 0.01}}
	res, err := Open(context.Background(), v, ticket(orderplan.Long), fast())
	assert.ErrorContains(t, err, "the entry was rejected: nothing filled, no position opened")
	assert.Zero(t, res.Filled)
	assert.NotContains(t, v.calls, "bracket SELL 0.01 tp 67000 sl 64000")

	v = &venue{entry: types.Order{ID: "e-1", Status: types.OrderStatusNew, Quantity: 0.01}}
	_, err = Open(context.Background(), v, ticket(orderplan.Long), fast())
	assert.ErrorContains(t, err, "did not fill in 30ms and was cancelled, no position opened")
	assert.Equal(t, []string{"leverage BTCUSDT 3", "entry BUY MARKET 0.01 BTCUSDT", "cancel e-1"}, v.calls)
}

// No stop, no position: protection failing closes it at market.
func TestOpen_ProtectionFails(t *testing.T) {
	v := &venue{entry: filled, bracketErr: errors.New("stop leg: price would trigger immediately")}
	res, err := Open(context.Background(), v, ticket(orderplan.Long), fast())
	assert.ErrorContains(t, err, "the stop and target could not be placed (stop leg: price would trigger immediately), so the position was closed at market")
	assert.False(t, errors.Is(err, ErrUnprotected))
	assert.True(t, res.Flattened)
	assert.Equal(t, "close-1", res.CloseID, "the closing order, for a caller that reports orders")
	assert.False(t, res.Protected)
	assert.Equal(t, "close SELL MARKET 0.01 reduce-only", v.calls[len(v.calls)-1])

	v = &venue{entry: filled, bracketErr: errors.New("timeout"), closeErr: errors.New("timeout")}
	_, err = Open(context.Background(), v, ticket(orderplan.Long), fast())
	assert.ErrorIs(t, err, ErrUnprotected)
	assert.ErrorContains(t, err, "POSITION OPEN WITHOUT A STOP: 0.01 BTCUSDT on bybit_futures")
	assert.ErrorContains(t, err, "close it on the exchange now")
}

// An entry that answered an error may still have opened a position: it is
// protected all the same.
func TestOpen_EntryErrorButOpened(t *testing.T) {
	v := &venue{entryErr: errors.New("context deadline exceeded"), positions: []*types.Position{{Symbol: "BTCUSDT", Side: "LONG", Size: 0.01}}}
	res, err := Open(context.Background(), v, ticket(orderplan.Long), fast())
	require.NoError(t, err)
	assert.True(t, res.Protected)
	assert.Contains(t, res.Notes[0], "but a position of 0.01 is open: protecting it")

	v = &venue{entryErr: errors.New("insufficient margin")}
	_, err = Open(context.Background(), v, ticket(orderplan.Long), fast())
	assert.ErrorContains(t, err, "entry: insufficient margin; no position opened")
	assert.Equal(t, []string{"leverage BTCUSDT 3", "entry BUY MARKET 0.01 BTCUSDT"}, v.calls)
}

func TestOpen_Refusals(t *testing.T) {
	v := &venue{entry: filled}
	bad := ticket(orderplan.Long)
	bad.OK = false
	_, err := Open(context.Background(), v, bad, fast())
	assert.ErrorContains(t, err, "did not pass its checks")

	limit := ticket(orderplan.Long)
	limit.EntryType, limit.Entry = orderplan.AtLimit, 64500
	_, err = Open(context.Background(), v, limit, fast())
	assert.ErrorIs(t, err, ErrLimitEntry, "a venue that cannot hold the stop before the fill")
	assert.Empty(t, v.calls, "nothing reaches the venue")

	v.leverageErr = errors.New("position open")
	_, err = Open(context.Background(), v, ticket(orderplan.Long), fast())
	assert.ErrorContains(t, err, "leverage 3x on BTCUSDT: position open; nothing placed")
	assert.Equal(t, []string{"leverage BTCUSDT 3"}, v.calls)
}

// protectedVenue holds a limit entry's stop and target with the order (Bybit).
type protectedVenue struct {
	*venue
	err error
}

func (p protectedVenue) PlaceProtectedLimit(_ context.Context, r *types.OrderRequest, stop, target float64) (*types.Order, error) {
	p.log("limit %s %g @ %g sl %g tp %g", r.Side, r.Quantity, r.Price, stop, target)
	if p.err != nil {
		return nil, p.err
	}
	return &types.Order{ID: "l-1", Status: types.OrderStatusNew, Quantity: r.Quantity, Price: r.Price}, nil
}

// A limit entry rests with its stop and target attached: protected from the
// fill on, with no bracket placed after.
func TestOpen_Limit(t *testing.T) {
	v := protectedVenue{venue: &venue{}}
	tk := ticket(orderplan.Short)
	tk.EntryType, tk.Entry = orderplan.AtLimit, 65500
	res, err := Open(context.Background(), v, tk, fast())
	require.NoError(t, err)
	assert.Equal(t, "l-1", res.EntryID)
	assert.True(t, res.Protected)
	assert.Zero(t, res.Filled, "resting, not filled")
	assert.Contains(t, res.Notes[0], "held by the exchange and take effect at the fill")
	assert.Equal(t, []string{"leverage BTCUSDT 3", "limit SELL 0.01 @ 65500 sl 66000 tp 63000"}, v.calls)

	v = protectedVenue{venue: &venue{}, err: errors.New("price out of range")}
	_, err = Open(context.Background(), v, tk, fast())
	assert.ErrorContains(t, err, "limit entry: price out of range")

	tk.Entry = 0
	_, err = Open(context.Background(), protectedVenue{venue: &venue{}}, tk, fast())
	assert.ErrorContains(t, err, "without its price")
}
