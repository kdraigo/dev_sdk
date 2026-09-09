package live

import (
	"fmt"
	"math"
	"strconv"

	"github.com/kdraigo/dev_sdk/types"
)

// This file is the single place a types.OrderRequest becomes something an
// exchange can be asked for. It exists because the previous per-adapter mapping
// was wrong in the same way on both venues:
//
//	orderType := binance.OrderTypeMarket
//	if req.Type == types.OrderTypeLimit { orderType = binance.OrderTypeLimit }
//
// Everything that was not LIMIT fell through to MARKET, so a protective
// STOP_LOSS was sent as an immediate market order and executed at once instead
// of resting, and StopPrice was never read at all. Nothing errored. A strategy
// that believed it was protected simply was not.
//
// Keeping the mapping here, pure and exhaustively tested, is what stops that
// from being reintroduced the next time an adapter is added.

// MarketKind is what the intent is destined for. It changes which requests are
// legal — ReduceOnly means nothing on a cash balance — not just which endpoint
// is called.
type MarketKind int

const (
	MarketSpot MarketKind = iota
	MarketLinearPerp
)

func (m MarketKind) String() string {
	if m == MarketLinearPerp {
		return "linear_perp"
	}
	return "spot"
}

// IntentType is the venue-neutral order family. It is deliberately finer than
// types.OrderType: the SDK's STOP_LOSS is market-on-trigger while
// STOP_LOSS_LIMIT rests at a price, and collapsing the two is exactly the bug
// this file exists to prevent.
type IntentType string

const (
	// IntentMarket executes immediately at the book.
	IntentMarket IntentType = "MARKET"

	// IntentLimit rests at Price.
	IntentLimit IntentType = "LIMIT"

	// IntentStopMarket rests until the trigger, then executes at the book.
	IntentStopMarket IntentType = "STOP_MARKET"

	// IntentStopLimit rests until the trigger, then rests at Price.
	IntentStopLimit IntentType = "STOP_LIMIT"

	// IntentTakeProfitLimit rests at Price. Mechanically a limit order; the
	// distinct intent preserves the strategy's stated purpose so a run can be
	// read back and understood.
	IntentTakeProfitLimit IntentType = "TAKE_PROFIT_LIMIT"
)

// Triggered reports whether the intent waits on a trigger price before it can
// execute. An intent that is triggered but carries no StopPrice is malformed,
// and MapOrder refuses it rather than sending a market order.
func (t IntentType) Triggered() bool {
	return t == IntentStopMarket || t == IntentStopLimit
}

// Resting reports whether the intent books at a limit price rather than at the
// market once it is live.
func (t IntentType) Resting() bool {
	return t == IntentLimit || t == IntentStopLimit || t == IntentTakeProfitLimit
}

// OrderIntent is a validated, venue-neutral order. Adapters translate it into
// their own client's vocabulary; they do not re-derive it from the request.
type OrderIntent struct {
	Type     IntentType
	Side     types.OrderSide
	Symbol   string // venue form, uppercase, no slash
	Quantity float64

	// Price is the resting price, zero when the intent does not rest.
	Price float64

	// StopPrice is the trigger, zero when the intent has no trigger.
	StopPrice float64

	// ReduceOnly is only ever true for MarketLinearPerp; MapOrder rejects it
	// on spot rather than dropping it silently.
	ReduceOnly bool

	// TimeInForce is "GTC" for resting intents and empty for market ones,
	// mirroring what both venues require.
	TimeInForce string
}

// MapOrder resolves a strategy's OrderRequest into a venue-neutral intent.
//
// Every rejection here is a request the venue would have mishandled or
// misunderstood. Returning an error is the point: the alternative, and the
// behaviour being replaced, was to send something plausible instead.
func MapOrder(req *types.OrderRequest, market MarketKind) (*OrderIntent, error) {
	if req == nil {
		return nil, fmt.Errorf("order request must not be nil")
	}
	if req.Quantity <= 0 {
		return nil, fmt.Errorf("order quantity must be positive, got %v", req.Quantity)
	}
	if req.Side != types.OrderSideBuy && req.Side != types.OrderSideSell {
		return nil, fmt.Errorf("order side must be BUY or SELL, got %q", req.Side)
	}

	var intentType IntentType
	switch req.Type {
	case types.OrderTypeMarket, "":
		// An empty type is a market order: that is what the previous code did
		// by accident, and enough callers rely on it that changing it now
		// would break them. Every other unset-looking value is an error.
		intentType = IntentMarket
	case types.OrderTypeLimit:
		intentType = IntentLimit
	case types.OrderTypeStopLoss:
		intentType = IntentStopMarket
	case types.OrderTypeStopLossLimit:
		intentType = IntentStopLimit
	case types.OrderTypeTakeProfitLimit:
		intentType = IntentTakeProfitLimit
	default:
		// Never fall back to MARKET. An unknown type sent as an immediate
		// market order is the defect this file replaces.
		return nil, fmt.Errorf("unsupported order type %q", req.Type)
	}

	if intentType.Resting() && req.Price <= 0 {
		return nil, fmt.Errorf("%s order requires a positive Price, got %v", req.Type, req.Price)
	}
	if intentType.Triggered() && req.StopPrice <= 0 {
		return nil, fmt.Errorf("%s order requires a positive StopPrice, got %v", req.Type, req.StopPrice)
	}
	if req.ReduceOnly && market != MarketLinearPerp {
		return nil, fmt.Errorf("ReduceOnly is a futures concept and has no meaning on %s: a cash balance has no position to reduce", market)
	}

	intent := &OrderIntent{
		Type:       intentType,
		Side:       req.Side,
		Symbol:     VenueSymbol(req.Symbol),
		Quantity:   req.Quantity,
		ReduceOnly: req.ReduceOnly,
	}
	if intentType.Resting() {
		intent.Price = req.Price
		intent.TimeInForce = "GTC"
	}
	if intentType.Triggered() {
		intent.StopPrice = req.StopPrice
	}
	return intent, nil
}

// VenueSymbol converts the SDK's BASE/QUOTE form to the concatenated uppercase
// form both Binance and Bybit expect. Already-concatenated input passes through.
func VenueSymbol(symbol string) string {
	out := make([]rune, 0, len(symbol))
	for _, r := range symbol {
		if r == '/' {
			continue
		}
		if r >= 'a' && r <= 'z' {
			r -= 'a' - 'A'
		}
		out = append(out, r)
	}
	return string(out)
}

// InstrumentFilter is an instrument's own trading rules, read from the venue at
// PrepareSession. Never hardcode these: they differ per symbol, they change,
// and a wrong step size is rejected by the exchange with an opaque code long
// after the strategy thought the order was placed.
type InstrumentFilter struct {
	TickSize    float64 // price increment
	StepSize    float64 // quantity increment
	MinQty      float64
	MinNotional float64
}

// Apply snaps the intent onto the instrument's grid and then checks it still
// clears the venue's minimums.
//
// refPrice values a market order, which has no price of its own; pass the last
// trade or mark. Zero skips the notional check rather than treating the order
// as worthless.
//
// Quantity floors: rounding a size up would trade more than the strategy asked
// for, and on a reduce-only exit that surplus is what flips a position instead
// of closing it. Prices round to nearest, because a price only has to sit on
// the grid and nearest moves it least — flooring a protective SELL stop would
// systematically push every stop further from the position it guards.
func (f InstrumentFilter) Apply(intent *OrderIntent, refPrice float64) error {
	if intent == nil {
		return fmt.Errorf("intent must not be nil")
	}

	intent.Quantity = floorToStep(intent.Quantity, f.StepSize)
	if intent.Price > 0 {
		intent.Price = nearestToStep(intent.Price, f.TickSize)
	}
	if intent.StopPrice > 0 {
		intent.StopPrice = nearestToStep(intent.StopPrice, f.TickSize)
	}

	if intent.Quantity <= 0 {
		return fmt.Errorf("quantity rounds to zero at step size %v", f.StepSize)
	}
	if f.MinQty > 0 && intent.Quantity < f.MinQty {
		return fmt.Errorf("quantity %v is below the instrument minimum %v", intent.Quantity, f.MinQty)
	}

	// A reduce-only order is exempt from the notional minimum here, and the
	// venue is left to decide.
	//
	// The minimum exists to stop dust *positions* being opened. A reduce-only
	// order cannot open one — it only shrinks what is already there — and a
	// protective stop guarding a small position is legitimately small by
	// definition. Enforcing the minimum locally would refuse to place that
	// stop, leaving the position unprotected, which is far worse than letting
	// Binance reject an order it would have rejected anyway. Refusing to
	// protect a position is not a safe default.
	if f.MinNotional > 0 && !intent.ReduceOnly {
		price := intent.Price
		if price <= 0 {
			price = intent.StopPrice
		}
		if price <= 0 {
			price = refPrice
		}
		if price > 0 {
			if notional := price * intent.Quantity; notional < f.MinNotional {
				return fmt.Errorf("notional %v is below the instrument minimum %v", notional, f.MinNotional)
			}
		}
	}
	return nil
}

// FormatQty and FormatPrice render a value for the wire at the precision the
// instrument's own step implies.
//
// The spot adapters used fmt.Sprintf("%f", …), which is six decimals whatever
// the instrument's precision is: too many for a symbol that steps in whole
// units, and the venue rejects it.
func (f InstrumentFilter) FormatQty(v float64) string   { return formatStep(v, f.StepSize) }
func (f InstrumentFilter) FormatPrice(v float64) string { return formatStep(v, f.TickSize) }

func formatStep(v, step float64) string {
	return strconv.FormatFloat(v, 'f', decimalsFor(step), 64)
}

// decimalsFor returns the number of decimal places a step implies. -1 means
// "as many as the value needs", which is the honest answer when the step is
// unknown — better than inventing a precision the instrument may not accept.
func decimalsFor(step float64) int {
	if step <= 0 {
		return -1
	}
	d := int(math.Round(-math.Log10(step)))
	if d < 0 {
		return 0
	}
	if d > 12 {
		return 12
	}
	return d
}

// floorToStep floors v to a multiple of step, working in integer units of the
// step to keep floating-point drift from producing 0.09999999999 for 0.1.
func floorToStep(v, step float64) float64 {
	if step <= 0 {
		return v
	}
	units := math.Floor(roundTiny(v / step))
	return roundTiny(units * step)
}

// nearestToStep rounds v to the closest multiple of step.
func nearestToStep(v, step float64) float64 {
	if step <= 0 {
		return v
	}
	units := math.Round(v / step)
	return roundTiny(units * step)
}

// roundTiny snaps a value that is within a hair of an integer multiple onto it.
// 0.1/0.1 is 0.9999999999999999 in binary floating point, and Floor would turn
// a quantity of exactly one step into zero.
func roundTiny(v float64) float64 {
	const epsilon = 1e-9
	if r := math.Round(v); math.Abs(v-r) < epsilon*math.Max(1, math.Abs(v)) {
		return r
	}
	// Trim the representation error that multiplication reintroduces without
	// disturbing genuinely fine-grained values.
	scaled := v * 1e10
	if math.Abs(scaled) < 1e15 {
		return math.Round(scaled) / 1e10
	}
	return v
}
