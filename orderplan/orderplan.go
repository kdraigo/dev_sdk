// Package orderplan turns a trade plan drawn on a chart (entry, stop, target)
// into the exact order that would be placed, and checks it, deterministically.
//
// It is the checker between a user's AI and an exchange: kdraigo's server and
// the executor on the user's machine run this same code, so they cannot
// disagree about an order. Nothing here talks to a network or reads a clock;
// every input is passed in, so the same inputs always give the same ticket,
// and the tests pin each rule.
//
// The AI never supplies an order. It points at a drawing; the drawing gives the
// prices; the user chooses how to enter (at market now, or with a limit at the
// drawn entry); the user's limits give the size; the exchange's rules give the
// rounding. A price the AI made up can at most become a drawing the user sees,
// and the checks below refuse one that is far from the market.
//
// Scope, as the SDK's perpetuals adapters: linear USDT-margined perpetuals,
// isolated margin, one-way positions, one position per contract, a
// reduce-only stop and target with every entry.
package orderplan

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Side of the position the plan opens.
type Side string

const (
	Long  Side = "long"
	Short Side = "short"
)

// EntryType is how the position is entered. The user chooses it for each order;
// there is no default.
type EntryType string

const (
	// AtMarket enters now, at the market. The drawn entry must be near the
	// price now, and the order is sized at that price.
	AtMarket EntryType = "market"
	// AtLimit rests at the drawn entry until the price comes to it: at or below
	// the price now for a long, at or above for a short. It is sized at the
	// entry, so its risk does not move with the market.
	AtLimit EntryType = "limit"
)

// Plan is what the drawing says, and how the user chose to enter. Prices are
// the drawing's own, unrounded.
type Plan struct {
	Venue     string // binance_futures, bybit_futures
	Symbol    string // BTCUSDT
	Side      Side
	EntryType EntryType
	Entry     float64
	Stop      float64
	Target    float64
	// Risk is the most the user wants to lose on this trade if the stop
	// fills, in USDT. Zero means Limits.RiskPerTrade. It can only lower the
	// limit, never raise it.
	Risk float64
}

// Market is the contract's rules and price now, read from the exchange.
type Market struct {
	TickSize    float64
	StepSize    float64
	MinQty      float64
	MinNotional float64
	MaxLeverage float64 // the exchange's; 0 = unknown
	// MaintenanceMargin is the maintenance margin rate (0.005 = 0.5%) used
	// to estimate the liquidation price. 0 uses DefaultMaintenanceMargin.
	MaintenanceMargin float64
	Mark              float64
}

// DefaultMaintenanceMargin is used when the contract's rate is not known. It
// is at the high end of the first tier on both venues, so the estimated
// liquidation price is nearer the entry than the real one, never further.
const DefaultMaintenanceMargin = 0.01

// Account is the futures wallet now.
type Account struct {
	Available        float64 // free USDT
	OpenPositions    int
	PositionOnSymbol bool    // a position is already open on this contract
	LossToday        float64 // realized loss today, as a positive number
}

// Limits are the user's own, kept on their machine. None has a default: an
// unset limit is a refused plan, never an unlimited one.
type Limits struct {
	RiskPerTrade     float64 // most lost per trade if the stop fills, USDT
	MaxNotional      float64 // largest position value, USDT
	Leverage         float64 // leverage to open with
	MaxLeverage      float64
	MaxOpenPositions int
	MaxDailyLoss     float64 // no new position once today's realized loss reaches this, USDT
	// MaxEntryGap is how far the drawn entry may be from the price now for an
	// entry at market, as a fraction (0.005 = 0.5%). It catches a plan drawn
	// at a price the market is not at: stale, or made up.
	MaxEntryGap float64
	// MaxLimitGap is how far a limit entry may rest from the price now, as a
	// fraction (0.1 = 10%). Needed only for limit entries.
	MaxLimitGap float64
	// MinStopGap is the nearest a stop may be to the entry, as a fraction. A
	// stop inside the spread and the fees is a loss, not a protection.
	MinStopGap float64
}

// Check is one rule's verdict, in words the user reads on the ticket.
type Check struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Detail string `json:"detail"`
}

// Ticket is the exact order and its checks. Hash binds a confirmation to it:
// the executor places only the ticket whose hash the user confirmed.
type Ticket struct {
	Venue     string    `json:"venue"`
	Symbol    string    `json:"symbol"`
	Side      Side      `json:"side"`
	EntryType EntryType `json:"entry_type"`
	// Entry is the limit price for a limit entry, 0 at market.
	Entry    float64 `json:"entry"`
	Qty      float64 `json:"qty"`
	Stop     float64 `json:"stop"`
	Target   float64 `json:"target"`
	Leverage float64 `json:"leverage"`

	// Mark is the price when the ticket was built; Price is what it was
	// sized at (the mark at market, the entry with a limit).
	Mark        float64 `json:"mark"`
	Price       float64 `json:"price"`
	Notional    float64 `json:"notional"`
	Margin      float64 `json:"margin"`
	Risk        float64 `json:"risk"`
	Reward      float64 `json:"reward"`
	RewardRisk  float64 `json:"reward_risk"`
	Liquidation float64 `json:"liquidation"`
	TickSize    float64 `json:"tick_size"`
	StepSize    float64 `json:"step_size"`

	Checks []Check `json:"checks"`
	OK     bool    `json:"ok"`
	Hash   string  `json:"hash"`
}

// Build sizes the plan and checks it. A ticket with OK false must not be
// placed; its failed checks say why.
func Build(p Plan, m Market, a Account, l Limits) Ticket {
	t := Ticket{
		Venue: p.Venue, Symbol: strings.ToUpper(p.Symbol), Side: p.Side, EntryType: p.EntryType,
		Leverage: l.Leverage, Mark: m.Mark, TickSize: m.TickSize, StepSize: m.StepSize,
	}
	c := &checks{}

	// Limits first: without them nothing below means anything.
	if !c.add("limits", l.RiskPerTrade > 0 && l.MaxNotional > 0 && l.Leverage > 0 && l.MaxLeverage > 0 && l.MaxOpenPositions > 0 && l.MaxDailyLoss > 0 && l.MaxEntryGap > 0,
		"risk per trade, max position value, leverage, max leverage, max open positions, max daily loss and max entry gap are all set") {
		return t.finish(c)
	}
	if !c.add("entry type", p.EntryType == AtMarket || p.EntryType == AtLimit, "the entry is at market or with a limit, as chosen for this order") {
		return t.finish(c)
	}
	if p.EntryType == AtLimit && !c.add("limits", l.MaxLimitGap > 0, "the max distance of a limit entry from the price now is set") {
		return t.finish(c)
	}
	if !c.add("plan", (p.Side == Long || p.Side == Short) && p.Entry > 0 && p.Stop > 0 && p.Target > 0 && p.Venue != "" && p.Symbol != "",
		fmt.Sprintf("a %s plan on %s %s with entry, stop and target above 0", p.Side, p.Venue, p.Symbol)) {
		return t.finish(c)
	}
	if !c.add("market", m.Mark > 0 && m.TickSize > 0 && m.StepSize > 0, "the contract's price, tick size and quantity step are known") {
		return t.finish(c)
	}

	t.Stop = nearest(p.Stop, m.TickSize)
	t.Target = nearest(p.Target, m.TickSize)
	long := p.Side == Long
	entry := p.Entry
	if p.EntryType == AtLimit {
		entry = nearest(p.Entry, m.TickSize)
		t.Entry = entry
		t.Price = entry
	} else {
		t.Price = m.Mark
	}

	// The drawing's shape.
	if long {
		c.add("shape", t.Stop < entry && entry < t.Target, fmt.Sprintf("stop %s < entry %s < target %s", px(t.Stop, m), px(entry, m), px(t.Target, m)))
	} else {
		c.add("shape", t.Target < entry && entry < t.Stop, fmt.Sprintf("target %s < entry %s < stop %s", px(t.Target, m), px(entry, m), px(t.Stop, m)))
	}

	// Where the entry stands against the price now.
	gap := math.Abs(entry-m.Mark) / m.Mark
	if p.EntryType == AtMarket {
		// A stop or target the price has already passed would fire at once.
		if long {
			c.add("price now", t.Stop < m.Mark && m.Mark < t.Target, fmt.Sprintf("the price now, %s, is between the stop and the target", px(m.Mark, m)))
		} else {
			c.add("price now", t.Target < m.Mark && m.Mark < t.Stop, fmt.Sprintf("the price now, %s, is between the target and the stop", px(m.Mark, m)))
		}
		c.add("entry", gap <= l.MaxEntryGap, fmt.Sprintf("the drawn entry %s is %.2f%% from the price now (at most %.2f%% for an entry at market)", px(entry, m), 100*gap, 100*l.MaxEntryGap))
	} else {
		// A limit above the price for a long (below for a short) would fill at
		// once: that is a market entry, or a breakout, neither of which this
		// plan says.
		if long {
			c.add("limit side", entry <= m.Mark, fmt.Sprintf("a buy limit at %s rests at or below the price now, %s", px(entry, m), px(m.Mark, m)))
		} else {
			c.add("limit side", entry >= m.Mark, fmt.Sprintf("a sell limit at %s rests at or above the price now, %s", px(entry, m), px(m.Mark, m)))
		}
		c.add("entry", gap <= l.MaxLimitGap, fmt.Sprintf("the limit %s is %.2f%% from the price now (at most %.2f%%)", px(entry, m), 100*gap, 100*l.MaxLimitGap))
	}
	stopGap := math.Abs(t.Price-t.Stop) / t.Price
	c.add("stop distance", stopGap >= l.MinStopGap && t.Stop != t.Price, fmt.Sprintf("the stop is %.2f%% from the entry (at least %.2f%%)", 100*stopGap, 100*l.MinStopGap))
	if !c.ok() {
		return t.finish(c)
	}

	// Size from the risk: the quantity that loses Risk if the stop fills at
	// its price. Capped by the position value limit, floored to the step.
	risk := l.RiskPerTrade
	if p.Risk > 0 && p.Risk < risk {
		risk = p.Risk
	}
	perUnit := math.Abs(t.Price - t.Stop)
	qty := risk / perUnit
	capped := false
	if qty*t.Price > l.MaxNotional {
		qty = l.MaxNotional / t.Price
		capped = true
	}
	t.Qty = floorTo(qty, m.StepSize)
	t.Notional = t.Qty * t.Price
	t.Risk = t.Qty * perUnit
	t.Reward = t.Qty * math.Abs(t.Target-t.Price)
	if t.Risk > 0 {
		t.RewardRisk = t.Reward / t.Risk
	}
	sizeDetail := fmt.Sprintf("%s %s, worth %.2f USDT: %.2f USDT lost if the stop fills (at most %.2f)", fmtStep(t.Qty, m.StepSize), t.Symbol, t.Notional, t.Risk, risk)
	if capped {
		sizeDetail += fmt.Sprintf("; cut to the %.2f USDT position limit", l.MaxNotional)
	}
	c.add("size", t.Qty > 0 && t.Qty >= m.MinQty && (m.MinNotional <= 0 || t.Notional >= m.MinNotional), sizeDetail+minimums(m))

	// Leverage, margin and liquidation, on isolated margin, at the entry.
	levOK := l.Leverage <= l.MaxLeverage && (m.MaxLeverage <= 0 || l.Leverage <= m.MaxLeverage) && l.Leverage >= 1
	c.add("leverage", levOK, fmt.Sprintf("%vx (your limit %vx%s)", l.Leverage, l.MaxLeverage, maxLev(m)))
	t.Margin = t.Notional / l.Leverage
	c.add("margin", t.Margin <= a.Available, fmt.Sprintf("%.2f USDT of margin, %.2f USDT free", t.Margin, a.Available))
	mmr := m.MaintenanceMargin
	if mmr <= 0 {
		mmr = DefaultMaintenanceMargin
	}
	if long {
		t.Liquidation = t.Price * (1 - 1/l.Leverage + mmr)
		c.add("liquidation", t.Liquidation < t.Stop, fmt.Sprintf("the stop %s comes before the estimated liquidation at %s", px(t.Stop, m), px(t.Liquidation, m)))
	} else {
		t.Liquidation = t.Price * (1 + 1/l.Leverage - mmr)
		c.add("liquidation", t.Liquidation > t.Stop, fmt.Sprintf("the stop %s comes before the estimated liquidation at %s", px(t.Stop, m), px(t.Liquidation, m)))
	}

	// The account.
	c.add("one position per contract", !a.PositionOnSymbol, "no position is open on "+t.Symbol+" yet")
	c.add("open positions", a.OpenPositions < l.MaxOpenPositions, fmt.Sprintf("%d open, at most %d", a.OpenPositions, l.MaxOpenPositions))
	c.add("daily loss", a.LossToday < l.MaxDailyLoss, fmt.Sprintf("%.2f USDT lost today, new positions stop at %.2f", a.LossToday, l.MaxDailyLoss))

	return t.finish(c)
}

// Tolerance is how far the moment of placing may differ from the moment the
// ticket was built and confirmed.
type Tolerance struct {
	// PriceDrift is the most the price may have moved, as a fraction of the
	// ticket's price (0.003 = 0.3%). It applies to entries at market; a limit
	// is placed at its own price whatever the market did.
	PriceDrift float64
	// RiskSlack is how far over the risk limit the loss at the stop may be at
	// placing, as a fraction (0.1 = 10%). At market a price move changes the
	// distance to the stop, and with it the risk, many times more than the
	// price itself: 0.05% on a 1% stop is 5% more risk.
	RiskSlack float64
}

// Recheck re-runs the checks for a confirmed ticket against the market and the
// account at the moment of placing. The entry, quantity, stop, target and
// leverage stay as confirmed: only whether they are still safe is asked. An
// entry at market whose price has moved past the tolerance is refused, so what
// is placed is what the user saw.
func Recheck(t Ticket, m Market, a Account, l Limits, tol Tolerance) []Check {
	c := &checks{}
	if !c.add("unchanged", Hash(t) == t.Hash, "the ticket is the one confirmed") {
		return c.list
	}
	if !c.add("market", m.Mark > 0 && t.Mark > 0, "the price now is known") {
		return c.list
	}
	price := t.Entry
	if t.EntryType == AtMarket {
		price = m.Mark
		drift := math.Abs(m.Mark-t.Mark) / t.Mark
		c.add("price drift", drift <= tol.PriceDrift, fmt.Sprintf("the price moved %.2f%% since the ticket (at most %.2f%%)", 100*drift, 100*tol.PriceDrift))
		if t.Side == Long {
			c.add("price now", t.Stop < m.Mark && m.Mark < t.Target, "the price is still between the stop and the target")
		} else {
			c.add("price now", t.Target < m.Mark && m.Mark < t.Stop, "the price is still between the target and the stop")
		}
	} else {
		// A limit still has to be safe to rest: the price must not have gone
		// through the stop already.
		if t.Side == Long {
			c.add("price now", m.Mark > t.Stop, "the price has not gone through the stop")
		} else {
			c.add("price now", m.Mark < t.Stop, "the price has not gone through the stop")
		}
	}
	notional := t.Qty * price
	risk := t.Qty * math.Abs(price-t.Stop)
	c.add("size", notional <= l.MaxNotional*(1+tol.PriceDrift) && risk <= l.RiskPerTrade*(1+tol.RiskSlack),
		fmt.Sprintf("worth %.2f USDT, %.2f USDT lost if the stop fills (limit %.2f, %.0f%% slack)", notional, risk, l.RiskPerTrade, 100*tol.RiskSlack))
	c.add("leverage", t.Leverage <= l.MaxLeverage && (m.MaxLeverage <= 0 || t.Leverage <= m.MaxLeverage), fmt.Sprintf("%vx", t.Leverage))
	c.add("margin", notional/t.Leverage <= a.Available, fmt.Sprintf("%.2f USDT of margin, %.2f USDT free", notional/t.Leverage, a.Available))
	c.add("one position per contract", !a.PositionOnSymbol, "no position is open on "+t.Symbol+" yet")
	c.add("open positions", a.OpenPositions < l.MaxOpenPositions, fmt.Sprintf("%d open, at most %d", a.OpenPositions, l.MaxOpenPositions))
	c.add("daily loss", a.LossToday < l.MaxDailyLoss, fmt.Sprintf("%.2f USDT lost today", a.LossToday))
	return c.list
}

// Passed reports whether every check passed.
func Passed(cs []Check) bool {
	if len(cs) == 0 {
		return false
	}
	for _, c := range cs {
		if !c.OK {
			return false
		}
	}
	return true
}

// Hash is the ticket's identity: what will be placed and the price it was
// sized at. Numbers are written at the contract's own precision, so the
// server and the executor hash the same string.
func Hash(t Ticket) string {
	s := strings.Join([]string{
		"v1", t.Venue, t.Symbol, string(t.Side), string(t.EntryType), fmtStep(t.Entry, t.TickSize),
		fmtStep(t.Qty, t.StepSize), fmtStep(t.Stop, t.TickSize), fmtStep(t.Target, t.TickSize),
		strconv.FormatFloat(t.Leverage, 'f', -1, 64), fmtStep(t.Mark, t.TickSize),
	}, "|")
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// VerifyShape is what can be checked without the account or the exchange: the
// numbers are coherent and the hash is the ticket's. kdraigo's server runs it
// on a ticket an executor sends, before anyone is asked to confirm it.
func VerifyShape(t Ticket) []Check {
	c := &checks{}
	c.add("unchanged", t.Hash != "" && Hash(t) == t.Hash, "the hash matches the ticket")
	entryOK := (t.EntryType == AtMarket && t.Entry == 0 && t.Price == t.Mark) || (t.EntryType == AtLimit && t.Entry > 0 && t.Price == t.Entry)
	c.add("plan", (t.Side == Long || t.Side == Short) && entryOK && t.Qty > 0 && t.Stop > 0 && t.Target > 0 && t.Leverage >= 1 && t.Mark > 0,
		"side, entry, quantity, stop, target, leverage and price are set")
	if t.Side == Long {
		c.add("shape", t.Stop < t.Price && t.Price < t.Target, "stop < entry < target")
	} else {
		c.add("shape", t.Target < t.Price && t.Price < t.Stop, "target < entry < stop")
	}
	c.add("checks", Passed(t.Checks), "every check passed when the ticket was built")
	return c.list
}

// ── helpers ──────────────────────────────────────────────────────────────────

type checks struct{ list []Check }

func (c *checks) add(name string, ok bool, detail string) bool {
	c.list = append(c.list, Check{Name: name, OK: ok, Detail: detail})
	return ok
}

func (c *checks) ok() bool { return Passed(c.list) }

func (t Ticket) finish(c *checks) Ticket {
	t.Checks = c.list
	t.OK = c.ok()
	t.Hash = Hash(t)
	return t
}

func minimums(m Market) string {
	parts := []string{}
	if m.MinQty > 0 {
		parts = append(parts, "at least "+fmtStep(m.MinQty, m.StepSize))
	}
	if m.MinNotional > 0 {
		parts = append(parts, fmt.Sprintf("worth at least %.2f USDT", m.MinNotional))
	}
	if len(parts) == 0 {
		return ""
	}
	return "; the contract takes " + strings.Join(parts, ", ")
}

func maxLev(m Market) string {
	if m.MaxLeverage > 0 {
		return fmt.Sprintf(", the contract's %vx", m.MaxLeverage)
	}
	return ""
}

func px(v float64, m Market) string { return fmtStep(v, m.TickSize) }

// fmtStep writes v at the precision its step implies.
func fmtStep(v, step float64) string {
	d := -1
	if step > 0 {
		d = int(math.Round(-math.Log10(step)))
		if d < 0 {
			d = 0
		}
		if d > 12 {
			d = 12
		}
	}
	return strconv.FormatFloat(v, 'f', d, 64)
}

// floorTo floors v to the step in whole steps, so 0.3/0.1 does not become 2.
func floorTo(v, step float64) float64 {
	if step <= 0 {
		return v
	}
	units := math.Floor(v/step + 1e-9)
	return roundTiny(units * step)
}

func nearest(v, step float64) float64 {
	if step <= 0 {
		return v
	}
	return roundTiny(math.Round(v/step) * step)
}

func roundTiny(v float64) float64 {
	scaled := v * 1e10
	if math.Abs(scaled) < 1e15 {
		return math.Round(scaled) / 1e10
	}
	return v
}
