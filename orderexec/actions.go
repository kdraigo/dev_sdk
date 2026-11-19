package orderexec

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/kdraigo/dev_sdk/orderplan"
	"github.com/kdraigo/dev_sdk/types"
)

// Actions on what is open (orderplan.Action): close a position, cancel a
// resting entry, move a stop, set or move a target. Each is carried out only
// after orderplan.RecheckAction passed on a fresh Holdings read.

// PositionStopper is a venue that holds a stop on the position itself (Bybit,
// tpslMode Full: a limit entry's attached stop becomes one at the fill).
type PositionStopper interface {
	PositionStop(ctx context.Context, symbol string) (float64, error)
	SetPositionStop(ctx context.Context, symbol string, stop float64) error
}

// PositionTargeter is a venue that holds a target on the position itself
// (Bybit, tpslMode Full: a limit entry's attached target becomes one at the
// fill).
type PositionTargeter interface {
	PositionTarget(ctx context.Context, symbol string) (float64, error)
	SetPositionTarget(ctx context.Context, symbol string, target float64) error
}

// Holdings is what the venue holds on a contract now: the position (nil when
// flat) with its stop and target and the venue's liquidation estimate, the
// resting entries, and the resting exits (reduce-only orders).
func Holdings(ctx context.Context, v Venue, exchange, symbol string) (*orderplan.Held, []orderplan.Resting, []*types.Order, error) {
	positions, err := v.GetPositions(ctx, exchange)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("positions: %w", err)
	}
	var h *orderplan.Held
	for _, p := range positions {
		if p == nil || p.Size == 0 || !sameSymbol(p.Symbol, symbol) {
			continue
		}
		side := orderplan.Long
		if strings.EqualFold(p.Side, "SHORT") {
			side = orderplan.Short
		}
		h = &orderplan.Held{Side: side, Qty: p.Size, Entry: p.EntryPrice, Liquidation: p.LiquidationPrice}
		break
	}
	open, err := v.ListOpenOrders(ctx, exchange, symbol)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("open orders: %w", err)
	}
	var resting []orderplan.Resting
	var exits []*types.Order
	for _, o := range open {
		if o == nil {
			continue
		}
		if o.ReduceOnly {
			exits = append(exits, o)
			if h != nil && h.Stop == 0 && isStop(o) {
				h.Stop = o.StopPrice
			}
			if h != nil && h.Target == 0 && isTarget(o) {
				h.Target = o.Price
			}
			continue
		}
		side := orderplan.Long
		if o.Side == types.OrderSideSell {
			side = orderplan.Short
		}
		resting = append(resting, orderplan.Resting{ID: o.ID, Side: side, Qty: o.Quantity - o.FilledQty, Price: o.Price})
	}
	if h != nil && h.Stop == 0 {
		if ps, ok := v.(PositionStopper); ok {
			if stop, err := ps.PositionStop(ctx, symbol); err == nil {
				h.Stop = stop
			}
		}
	}
	if h != nil && h.Target == 0 {
		if pt, ok := v.(PositionTargeter); ok {
			if target, err := pt.PositionTarget(ctx, symbol); err == nil {
				h.Target = target
			}
		}
	}
	return h, resting, exits, nil
}

func isStop(o *types.Order) bool {
	return o.Type == types.OrderTypeStopLoss || o.Type == types.OrderTypeStopLossLimit
}

// isTarget: a resting reduce-only exit at a price (both venues send a target
// as a plain limit, and read it back as one).
func isTarget(o *types.Order) bool {
	return o.ReduceOnly && (o.Type == types.OrderTypeTakeProfitLimit || o.Type == types.OrderTypeLimit) && o.Price > 0
}

// ActionResult is what an action did on the venue, whatever the outcome.
type ActionResult struct {
	// OrderID is the closing order, or a move's new stop.
	OrderID   string   `json:"order_id,omitempty"`
	Filled    float64  `json:"filled,omitempty"`
	AvgPrice  float64  `json:"avg_price,omitempty"`
	Cancelled []string `json:"cancelled,omitempty"`
	Notes     []string `json:"notes,omitempty"`
}

// Act carries out a confirmed action. Call it only after
// orderplan.RecheckAction passed on a fresh Holdings read.
func Act(ctx context.Context, v Venue, a orderplan.Action, o Options) (*ActionResult, error) {
	if !a.OK {
		return nil, errors.New("the action did not pass its checks: nothing done")
	}
	if o.FillWait <= 0 {
		o.FillWait = defaultFillWait
	}
	if o.Poll <= 0 {
		o.Poll = defaultPoll
	}
	switch a.Kind {
	case orderplan.ActClose:
		return closePosition(ctx, v, a, o)
	case orderplan.ActCancel:
		return cancelEntry(ctx, v, a)
	case orderplan.ActMoveStop:
		return moveStop(ctx, v, a, o)
	case orderplan.ActMoveTarget:
		return moveTarget(ctx, v, a, o)
	}
	return nil, fmt.Errorf("unknown action %q: nothing done", a.Kind)
}

func exitSide(s orderplan.Side) types.OrderSide {
	if s == orderplan.Short {
		return types.OrderSideBuy
	}
	return types.OrderSideSell
}

// closePosition closes the position at market (reduce-only, so it never
// flips it), and once flat takes back the exits left resting.
func closePosition(ctx context.Context, v Venue, a orderplan.Action, o Options) (*ActionResult, error) {
	res := &ActionResult{}
	h, _, _, err := Holdings(ctx, v, a.Venue, a.Symbol)
	if err != nil {
		return res, err
	}
	if h == nil {
		return res, fmt.Errorf("no position is open on %s any more: nothing closed", a.Symbol)
	}
	ord, err := v.PlaceOrder(ctx, &types.OrderRequest{
		Symbol: a.Symbol, Exchange: a.Venue, Side: exitSide(h.Side), Type: types.OrderTypeMarket, Quantity: h.Qty, ReduceOnly: true, Reason: o.Reason,
	})
	if err != nil {
		return res, fmt.Errorf("closing order: %w; the position stays open with its stop", err)
	}
	res.OrderID = ord.ID
	filled, avg, werr := waitOrder(ctx, v, a.Venue, a.Symbol, h.Qty, "closing order", ord, o)
	res.Filled, res.AvgPrice = filled, avg
	if werr != nil {
		res.Notes = append(res.Notes, werr.Error())
	}

	// Flat now? Then nothing should rest to act on a later position.
	after, _, exits, err := Holdings(context.WithoutCancel(ctx), v, a.Venue, a.Symbol)
	if err != nil {
		return res, fmt.Errorf("closed %g, but the position could not be read again (%v): check %s on the exchange", filled, err, a.Symbol)
	}
	if after != nil {
		return res, fmt.Errorf("%g of %s is still open after the closing order (it keeps its stop)", after.Qty, a.Symbol)
	}
	for _, x := range exits {
		if err := v.CancelOrder(context.WithoutCancel(ctx), a.Venue, a.Symbol, x.ID); err != nil && !errors.Is(err, types.ErrOrderClosed) {
			res.Notes = append(res.Notes, fmt.Sprintf("the exit %s could not be taken back (%v); it cannot open a position", x.ID, err))
			continue
		}
		res.Cancelled = append(res.Cancelled, x.ID)
	}
	return res, nil
}

// cancelEntry takes back a resting entry; on Bybit its attached stop and
// target go with it.
func cancelEntry(ctx context.Context, v Venue, a orderplan.Action) (*ActionResult, error) {
	res := &ActionResult{}
	err := v.CancelOrder(ctx, a.Venue, a.Symbol, a.OrderID)
	if errors.Is(err, types.ErrOrderClosed) {
		return res, fmt.Errorf("the order %s no longer rests (it filled or was cancelled already)", a.OrderID)
	}
	if err != nil {
		return res, fmt.Errorf("cancel %s: %w", a.OrderID, err)
	}
	res.Cancelled = []string{a.OrderID}
	return res, nil
}

// moveStop puts the new stop in place before the old one goes, so the
// position is never without one. A stop the venue holds on the position
// (Bybit Full mode) is moved in place.
func moveStop(ctx context.Context, v Venue, a orderplan.Action, o Options) (*ActionResult, error) {
	res := &ActionResult{}
	h, _, exits, err := Holdings(ctx, v, a.Venue, a.Symbol)
	if err != nil {
		return res, err
	}
	if h == nil {
		return res, fmt.Errorf("no position is open on %s any more: no stop moved", a.Symbol)
	}
	var stops []*types.Order
	for _, x := range exits {
		if isStop(x) {
			stops = append(stops, x)
		}
	}
	if len(stops) == 0 {
		ps, ok := v.(PositionStopper)
		if !ok {
			return res, fmt.Errorf("no stop rests on the exchange for the %s position: nothing moved", a.Symbol)
		}
		if err := ps.SetPositionStop(ctx, a.Symbol, a.Stop); err != nil {
			return res, fmt.Errorf("moving the position's stop: %w; the old stop stays", err)
		}
		res.Notes = append(res.Notes, "moved the stop the exchange holds on the position")
		return res, nil
	}
	ord, err := v.PlaceOrder(ctx, &types.OrderRequest{
		Symbol: a.Symbol, Exchange: a.Venue, Side: exitSide(h.Side), Type: types.OrderTypeStopLoss, Quantity: h.Qty,
		StopPrice: a.Stop, ReduceOnly: true, Reason: o.Reason,
	})
	if err != nil {
		return res, fmt.Errorf("the new stop could not be placed (%v): the old stop stays", err)
	}
	res.OrderID = ord.ID
	for _, old := range stops {
		if err := v.CancelOrder(context.WithoutCancel(ctx), a.Venue, a.Symbol, old.ID); err != nil && !errors.Is(err, types.ErrOrderClosed) {
			res.Notes = append(res.Notes, fmt.Sprintf("the old stop %s could not be taken back (%v): two stops rest, and the nearer fires first", old.ID, err))
			continue
		}
		res.Cancelled = append(res.Cancelled, old.ID)
	}
	return res, nil
}

// moveTarget puts the new target in place before the old one goes, so a
// failed placement leaves the position with the target it had. A target the
// venue holds on the position (Bybit Full mode) is moved in place. A position
// with no target gets one: a reduce-only limit for all of it.
func moveTarget(ctx context.Context, v Venue, a orderplan.Action, o Options) (*ActionResult, error) {
	res := &ActionResult{}
	h, _, exits, err := Holdings(ctx, v, a.Venue, a.Symbol)
	if err != nil {
		return res, err
	}
	if h == nil {
		return res, fmt.Errorf("no position is open on %s any more: no target set", a.Symbol)
	}
	var targets []*types.Order
	for _, x := range exits {
		if isTarget(x) {
			targets = append(targets, x)
		}
	}
	if len(targets) == 0 {
		if pt, ok := v.(PositionTargeter); ok {
			if cur, err := pt.PositionTarget(ctx, a.Symbol); err == nil && cur > 0 {
				if err := pt.SetPositionTarget(ctx, a.Symbol, a.Target); err != nil {
					return res, fmt.Errorf("moving the position's target: %w; the old target stays", err)
				}
				res.Notes = append(res.Notes, "moved the target the exchange holds on the position")
				return res, nil
			}
		}
	}
	ord, err := v.PlaceOrder(ctx, &types.OrderRequest{
		Symbol: a.Symbol, Exchange: a.Venue, Side: exitSide(h.Side), Type: types.OrderTypeTakeProfitLimit, Quantity: h.Qty,
		Price: a.Target, ReduceOnly: true, Reason: o.Reason,
	})
	if err != nil {
		if len(targets) > 0 {
			return res, fmt.Errorf("the new target could not be placed (%v): the old target stays", err)
		}
		return res, fmt.Errorf("the target could not be placed: %w", err)
	}
	res.OrderID = ord.ID
	if len(targets) == 0 {
		res.Notes = append(res.Notes, "the position had no target: one was placed")
	}
	for _, old := range targets {
		if err := v.CancelOrder(context.WithoutCancel(ctx), a.Venue, a.Symbol, old.ID); err != nil && !errors.Is(err, types.ErrOrderClosed) {
			res.Notes = append(res.Notes, fmt.Sprintf("the old target %s could not be taken back (%v): two targets rest, and the nearer fills first", old.ID, err))
			continue
		}
		res.Cancelled = append(res.Cancelled, old.ID)
	}
	return res, nil
}
