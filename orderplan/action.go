package orderplan

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strings"
)

// Actions on what is already open: close a position, cancel a resting entry,
// move a stop (the user's D5, for orders from their AI). Like a ticket, an
// action is built from the venue's numbers now, checked, hashed, confirmed by
// the user, and checked again before it is carried out.

// ActionKind is what an action does.
type ActionKind string

const (
	// ActClose closes the position on a contract, all of it, at market.
	ActClose ActionKind = "close"
	// ActCancel cancels a resting entry order (an exit is not cancelled on
	// its own: that is closing, or moving the stop).
	ActCancel ActionKind = "cancel"
	// ActMoveStop moves an open position's stop.
	ActMoveStop ActionKind = "move_stop"
)

// Held is an open position on a contract, as the venue reports it.
type Held struct {
	Side  Side
	Qty   float64
	Entry float64
	// Stop is the resting stop's trigger now; 0 when none was found.
	Stop float64
	// Liquidation is the venue's estimate; 0 when unknown.
	Liquidation float64
}

// Resting is a resting entry order on a contract.
type Resting struct {
	ID    string
	Side  Side // the side it opens
	Qty   float64
	Price float64
}

// Action is the exact change and its checks; Hash binds a confirmation to it.
type Action struct {
	Kind   ActionKind `json:"kind"`
	Venue  string     `json:"venue"`
	Symbol string     `json:"symbol"`
	// Side is the position's, or the side the cancelled order would open.
	Side Side    `json:"side"`
	Qty  float64 `json:"qty"`
	// OrderID and Price: the order a cancel takes back.
	OrderID string  `json:"order_id,omitempty"`
	Price   float64 `json:"price,omitempty"`
	// Entry and OldStop: the position as it is.
	Entry   float64 `json:"entry,omitempty"`
	OldStop float64 `json:"old_stop,omitempty"`
	// Stop is a move's new stop.
	Stop float64 `json:"stop,omitempty"`
	Mark float64 `json:"mark"`
	// PnL is, for a close, the position's P&L at the price now (before fees);
	// for a move, what the position makes if the new stop fills.
	PnL      float64 `json:"pnl"`
	TickSize float64 `json:"tick_size"`
	StepSize float64 `json:"step_size"`

	Checks []Check `json:"checks"`
	OK     bool    `json:"ok"`
	Hash   string  `json:"hash"`
}

// BuildClose closes the position held on the contract.
func BuildClose(venue, symbol string, h Held, m Market) Action {
	a := newAction(ActClose, venue, symbol, m)
	a.Side, a.Qty, a.Entry, a.OldStop = h.Side, h.Qty, h.Entry, h.Stop
	c := &checks{}
	if !held(c, h, symbol) || !marketKnown(c, m) {
		return a.finish(c)
	}
	a.PnL = pnl(h.Side, h.Entry, m.Mark, h.Qty)
	return a.finish(c)
}

// BuildCancel takes back a resting entry order.
func BuildCancel(venue, symbol string, r Resting, m Market) Action {
	a := newAction(ActCancel, venue, symbol, m)
	a.Side, a.Qty, a.OrderID, a.Price = r.Side, r.Qty, r.ID, r.Price
	c := &checks{}
	c.add("order", r.ID != "" && (r.Side == Long || r.Side == Short) && r.Qty > 0 && r.Price > 0,
		fmt.Sprintf("a %s entry rests on %s", r.Side, a.Symbol))
	return a.finish(c)
}

// BuildMoveStop moves the position's stop to a new price, on the contract's
// grid, between the price now and the liquidation, at least the user's
// minimum gap from the price.
func BuildMoveStop(venue, symbol string, h Held, stop float64, m Market, l Limits) Action {
	a := newAction(ActMoveStop, venue, symbol, m)
	a.Side, a.Qty, a.Entry, a.OldStop = h.Side, h.Qty, h.Entry, h.Stop
	c := &checks{}
	if !held(c, h, symbol) || !marketKnown(c, m) {
		return a.finish(c)
	}
	a.Stop = nearest(stop, m.TickSize)
	stopChecks(c, a, h, m, l)
	a.PnL = pnl(h.Side, h.Entry, a.Stop, h.Qty)
	return a.finish(c)
}

func newAction(kind ActionKind, venue, symbol string, m Market) Action {
	return Action{Kind: kind, Venue: venue, Symbol: strings.ToUpper(symbol), Mark: m.Mark, TickSize: m.TickSize, StepSize: m.StepSize}
}

func held(c *checks, h Held, symbol string) bool {
	return c.add("position", (h.Side == Long || h.Side == Short) && h.Qty > 0, "a position is open on "+strings.ToUpper(symbol))
}

func marketKnown(c *checks, m Market) bool {
	return c.add("market", m.Mark > 0 && m.TickSize > 0 && m.StepSize > 0, "the contract's price, tick size and quantity step are known")
}

// stopChecks: a stop that would fire at once, sit inside the spread, or come
// after the liquidation protects nothing.
func stopChecks(c *checks, a Action, h Held, m Market, l Limits) {
	long := h.Side == Long
	if long {
		c.add("side", a.Stop > 0 && a.Stop < m.Mark, fmt.Sprintf("the new stop %s is below the price now, %s", px(a.Stop, m), px(m.Mark, m)))
	} else {
		c.add("side", a.Stop > m.Mark, fmt.Sprintf("the new stop %s is above the price now, %s", px(a.Stop, m), px(m.Mark, m)))
	}
	gap := math.Abs(m.Mark-a.Stop) / m.Mark
	c.add("gap", gap >= l.MinStopGap && gap > 0, fmt.Sprintf("the new stop is %.2f%% from the price now (at least %.2f%%)", 100*gap, 100*l.MinStopGap))
	if h.Liquidation > 0 {
		if long {
			c.add("liquidation", a.Stop > h.Liquidation, fmt.Sprintf("the new stop %s comes before the liquidation, %s", px(a.Stop, m), px(h.Liquidation, m)))
		} else {
			c.add("liquidation", a.Stop < h.Liquidation, fmt.Sprintf("the new stop %s comes before the liquidation, %s", px(a.Stop, m), px(h.Liquidation, m)))
		}
	}
	c.add("changed", h.Stop == 0 || math.Abs(a.Stop-h.Stop) >= m.TickSize/2, fmt.Sprintf("the stop moves (from %s to %s)", px(h.Stop, m), px(a.Stop, m)))
}

func pnl(side Side, entry, exit, qty float64) float64 {
	if entry <= 0 || exit <= 0 {
		return 0
	}
	d := exit - entry
	if side == Short {
		d = -d
	}
	return roundTiny(d * qty)
}

func (a Action) finish(c *checks) Action {
	a.Checks = c.list
	a.OK = c.ok()
	a.Hash = ActionHash(a)
	return a
}

// ActionHash binds a confirmation to the exact action.
func ActionHash(a Action) string {
	s := strings.Join([]string{
		"a1", string(a.Kind), a.Venue, a.Symbol, string(a.Side), fmtStep(a.Qty, a.StepSize), a.OrderID,
		fmtStep(a.Price, a.TickSize), fmtStep(a.Stop, a.TickSize), fmtStep(a.Mark, a.TickSize),
	}, "|")
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// VerifyAction is the server's check of an action an executor sends: the
// hash, the fields its kind needs, and every check passed.
func VerifyAction(a Action) []Check {
	c := &checks{}
	c.add("unchanged", a.Hash != "" && ActionHash(a) == a.Hash, "the hash matches the action")
	c.add("action", (a.Kind == ActClose || a.Kind == ActCancel || a.Kind == ActMoveStop) && (a.Side == Long || a.Side == Short) && a.Qty > 0 &&
		a.Venue != "" && a.Symbol != "" && a.Mark > 0, "a known action on a position or an order, with its side, size and the price now")
	switch a.Kind {
	case ActCancel:
		c.add("order", a.OrderID != "" && a.Price > 0, "the order to cancel is named")
	case ActMoveStop:
		c.add("stop", a.Stop > 0 && (a.Side == Long && a.Stop < a.Mark || a.Side == Short && a.Stop > a.Mark), "the new stop is on the protective side of the price")
	}
	c.add("checks", Passed(a.Checks), "every check passed when the action was built")
	return c.list
}

// RecheckAction runs the checks again just before the action is carried out,
// on what the venue holds now: the position is still open on the same side
// (close, move), the order still rests (cancel), and a move's stop is still
// on the right side of the price now. h is nil when flat; r is nil when the
// order is gone.
func RecheckAction(a Action, h *Held, r *Resting, m Market, l Limits) []Check {
	c := &checks{}
	if !c.add("unchanged", ActionHash(a) == a.Hash, "the action is the one confirmed") {
		return c.list
	}
	switch a.Kind {
	case ActClose:
		c.add("position", h != nil && h.Side == a.Side && h.Qty > 0, fmt.Sprintf("the %s position on %s is still open", a.Side, a.Symbol))
	case ActCancel:
		c.add("order", r != nil && r.ID == a.OrderID, fmt.Sprintf("the order %s still rests", a.OrderID))
	case ActMoveStop:
		if !c.add("position", h != nil && h.Side == a.Side && h.Qty > 0, fmt.Sprintf("the %s position on %s is still open", a.Side, a.Symbol)) {
			return c.list
		}
		if !marketKnown(c, m) {
			return c.list
		}
		now := *h
		now.Stop = 0 // the move is the one confirmed, whatever the stop is now
		stopChecks(c, a, now, m, l)
	default:
		c.add("action", false, "a known action")
	}
	return c.list
}
