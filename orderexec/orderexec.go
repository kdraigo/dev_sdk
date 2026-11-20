// Package orderexec places an order ticket that dev_sdk/orderplan checked and
// the user confirmed, on a futures venue: the venue side of orders from the
// user's AI (kdraigo-mcp's executor). It reads what the checker needs
// (Snapshot), and opens a position with its stop and target (Open).
//
// The rule it keeps above all: a position is never left without its stop. If
// the stop and target cannot be placed once the entry has filled, the position
// is closed at market at once, and the error says so; if even that fails, the
// error says the position is open without a stop, in those words.
package orderexec

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/kdraigo/dev_sdk/orderplan"
	"github.com/kdraigo/dev_sdk/types"
)

// Venue is what placing needs from a futures adapter. dev_sdk's
// exchange/live BinanceFuturesClient and BybitFuturesClient have it, once
// PrepareSession ran with the ticket's symbol among the assets and caps that
// cover the ticket (the guard checks every order against them).
type Venue interface {
	PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error)
	CancelOrder(ctx context.Context, exchange, symbol, id string) error
	GetOrder(ctx context.Context, exchange, symbol, id string) (*types.Order, error)
	ListOpenOrders(ctx context.Context, exchange, symbol string) ([]*types.Order, error)
	GetAccount(ctx context.Context, exchange, asset string) (*types.Account, error)
	GetPositions(ctx context.Context, exchange string) ([]*types.Position, error)
	SetLeverage(ctx context.Context, exchange, pair string, leverage float64) error
	PlaceBracket(ctx context.Context, req *types.BracketRequest) ([]*types.Order, error)
	Contract(ctx context.Context, exchange, symbol string) (*types.Contract, error)
	RealizedPnL(ctx context.Context, exchange string, since time.Time) (float64, error)
}

// Settlement is the asset the perpetuals here are margined in.
const Settlement = "USDT"

// Snapshot reads, now, what orderplan.Build and orderplan.Recheck take: the
// contract's rules and mark, and the futures wallet (free margin, open
// positions, a position on this contract, today's realized loss since 00:00
// UTC).
func Snapshot(ctx context.Context, v Venue, exchange, symbol string, now time.Time) (orderplan.Market, orderplan.Account, error) {
	var m orderplan.Market
	var a orderplan.Account
	c, err := v.Contract(ctx, exchange, symbol)
	if err != nil {
		return m, a, err
	}
	m = orderplan.Market{
		TickSize: c.TickSize, StepSize: c.StepSize, MinQty: c.MinQty, MinNotional: c.MinNotional,
		MaxLeverage: c.MaxLeverage, MaintenanceMargin: c.MaintenanceMargin, Mark: c.Mark,
	}

	acct, err := v.GetAccount(ctx, exchange, Settlement)
	if err != nil {
		return m, a, fmt.Errorf("account: %w", err)
	}
	found := false
	for _, b := range acct.Balances {
		if strings.EqualFold(b.Asset, Settlement) {
			a.Available, found = b.Free, true
		}
	}
	if !found {
		return m, a, fmt.Errorf("account: no %s balance on %s", Settlement, exchange)
	}

	positions, err := v.GetPositions(ctx, exchange)
	if err != nil {
		return m, a, fmt.Errorf("positions: %w", err)
	}
	for _, p := range positions {
		if p == nil || p.Size == 0 {
			continue
		}
		a.OpenPositions++
		if sameSymbol(p.Symbol, symbol) {
			a.PositionOnSymbol = true
		}
	}

	day := now.UTC().Truncate(24 * time.Hour)
	pnl, err := v.RealizedPnL(ctx, exchange, day)
	if err != nil {
		return m, a, fmt.Errorf("today's realized P&L: %w", err)
	}
	a.LossToday = math.Max(0, -pnl)
	return m, a, nil
}

// Options shape Open.
type Options struct {
	// FillWait is how long the entry may take to fill; what is left after it
	// is cancelled and the filled part protected. Zero is 10 s.
	FillWait time.Duration
	// Poll is how often the entry is read while it fills. Zero is 250 ms.
	Poll time.Duration
	// Reason annotates the orders for live_trades (never sent to the venue).
	Reason map[string]any
}

// The defaults of Options.
const (
	defaultFillWait = 10 * time.Second
	defaultPoll     = 250 * time.Millisecond
)

// Result is what was done on the venue, whatever the outcome.
type Result struct {
	EntryID  string  `json:"entry_id,omitempty"`
	Filled   float64 `json:"filled"`
	AvgPrice float64 `json:"avg_price,omitempty"`
	StopID   string  `json:"stop_id,omitempty"`
	TargetID string  `json:"target_id,omitempty"`
	// Protected: the stop and target rest on the venue for the filled size.
	Protected bool `json:"protected"`
	// Flattened: protection failed and the position was closed at market,
	// by order CloseID (a caller that reports orders reports it too: without
	// it a live console keeps a position the venue no longer holds, bug #69).
	Flattened bool     `json:"flattened"`
	CloseID   string   `json:"close_id,omitempty"`
	Notes     []string `json:"notes,omitempty"`
}

// ProtectedLimitPlacer rests a limit entry whose stop and target the venue
// holds with the order and applies at the fill (Bybit). Only such a venue
// takes limit entries: a fill while the user's computer sleeps must find its
// stop already there.
type ProtectedLimitPlacer interface {
	PlaceProtectedLimit(ctx context.Context, req *types.OrderRequest, stop, target float64) (*types.Order, error)
}

// ErrLimitEntry refuses a limit entry on a venue that cannot hold its stop
// and target before the fill.
var ErrLimitEntry = errors.New("a limit entry is not placed on this exchange yet: its stop and target must rest on the exchange before it fills; enter at market, or use Bybit")

// ErrUnprotected is in the error chain when a position is open without its
// stop: the one outcome the user must hear about at once.
var ErrUnprotected = errors.New("POSITION OPEN WITHOUT A STOP")

// Open places a confirmed ticket: the leverage, the entry at market, then the
// stop and target for what filled. Call it only after orderplan.Recheck
// passed on a fresh Snapshot.
func Open(ctx context.Context, v Venue, t orderplan.Ticket, o Options) (*Result, error) {
	if !t.OK {
		return nil, errors.New("the ticket did not pass its checks: nothing placed")
	}
	limit := t.EntryType == orderplan.AtLimit
	var pl ProtectedLimitPlacer
	if limit {
		var ok bool
		if pl, ok = v.(ProtectedLimitPlacer); !ok {
			return nil, ErrLimitEntry
		}
		if t.Entry <= 0 {
			return nil, errors.New("the ticket is incomplete: a limit entry without its price; nothing placed")
		}
	} else if t.EntryType != orderplan.AtMarket {
		return nil, errors.New("the ticket is incomplete: nothing placed")
	}
	if t.Qty <= 0 || t.Stop <= 0 || t.Target <= 0 || t.Leverage <= 0 {
		return nil, errors.New("the ticket is incomplete: nothing placed")
	}
	if o.FillWait <= 0 {
		o.FillWait = defaultFillWait
	}
	if o.Poll <= 0 {
		o.Poll = defaultPoll
	}
	open, exit := types.OrderSideBuy, types.OrderSideSell
	if t.Side == orderplan.Short {
		open, exit = types.OrderSideSell, types.OrderSideBuy
	}
	res := &Result{}

	if err := v.SetLeverage(ctx, t.Venue, t.Symbol, t.Leverage); err != nil {
		return res, fmt.Errorf("leverage %gx on %s: %w; nothing placed", t.Leverage, t.Symbol, err)
	}

	if limit {
		order, err := pl.PlaceProtectedLimit(ctx, &types.OrderRequest{
			Symbol: t.Symbol, Exchange: t.Venue, Side: open, Type: types.OrderTypeLimit, Price: t.Entry, Quantity: t.Qty, Reason: o.Reason,
		}, t.Stop, t.Target)
		if err != nil {
			return res, fmt.Errorf("limit entry: %w", err)
		}
		res.EntryID = order.ID
		res.Filled, res.AvgPrice = order.FilledQty, order.AveragePrice
		res.Protected = true
		res.Notes = append(res.Notes, fmt.Sprintf("rests at %g; its stop (%g) and target (%g) are held by the exchange and take effect at the fill", t.Entry, t.Stop, t.Target))
		return res, nil
	}

	entry, err := v.PlaceOrder(ctx, &types.OrderRequest{
		Symbol: t.Symbol, Exchange: t.Venue, Side: open, Type: types.OrderTypeMarket, Quantity: t.Qty, Reason: o.Reason,
	})
	if err != nil {
		// The entry may have reached the venue all the same (a timeout, a
		// dropped answer). A position on the contract now is ours, and it is
		// protected like a filled entry.
		size, _, perr := positionOn(ctx, v, t.Venue, t.Symbol)
		if perr != nil || size == 0 {
			if perr != nil {
				return res, fmt.Errorf("entry: %w; and the position could not be read (%v): check %s %s on the exchange", err, perr, t.Venue, t.Symbol)
			}
			return res, fmt.Errorf("entry: %w; no position opened", err)
		}
		res.Notes = append(res.Notes, fmt.Sprintf("the entry answered an error (%v), but a position of %g is open: protecting it", err, size))
		res.Filled = size
		return protect(ctx, v, t, exit, o, res)
	}
	res.EntryID = entry.ID
	filled, avg, err := waitFill(ctx, v, t, entry, o)
	res.Filled, res.AvgPrice = filled, avg
	if err != nil && filled == 0 {
		return res, err
	}
	if err != nil {
		res.Notes = append(res.Notes, err.Error())
	}
	return protect(ctx, v, t, exit, o, res)
}

// waitFill reads the entry until it fills or FillWait passes; then the rest
// is cancelled. It returns what filled.
func waitFill(ctx context.Context, v Venue, t orderplan.Ticket, entry *types.Order, o Options) (float64, float64, error) {
	return waitOrder(ctx, v, t.Venue, t.Symbol, t.Qty, "entry", entry, o)
}

// waitOrder reads a market order until it fills or FillWait passes; then the
// rest is cancelled. It returns what filled; what names the order in errors.
func waitOrder(ctx context.Context, v Venue, venue, symbol string, qty float64, what string, entry *types.Order, o Options) (float64, float64, error) {
	t := struct {
		Venue, Symbol string
		Qty           float64
	}{venue, symbol, qty}
	cur := entry
	deadline := time.Now().Add(o.FillWait)
	for {
		switch cur.Status {
		case types.OrderStatusFilled:
			q := cur.FilledQty
			if q == 0 {
				q = cur.Quantity
			}
			return q, cur.AveragePrice, nil
		case types.OrderStatusCanceled, types.OrderStatusRejected:
			if cur.FilledQty > 0 {
				return cur.FilledQty, cur.AveragePrice, fmt.Errorf("the "+what+" was %s after %g of %g filled", strings.ToLower(string(cur.Status)), cur.FilledQty, t.Qty)
			}
			return 0, 0, fmt.Errorf("the "+what+" was %s: nothing filled"+noneOpened(what), strings.ToLower(string(cur.Status)))
		}
		if time.Now().After(deadline) {
			_ = v.CancelOrder(ctx, t.Venue, t.Symbol, entry.ID)
			if last, err := v.GetOrder(ctx, t.Venue, t.Symbol, entry.ID); err == nil && last != nil {
				cur = last
			}
			if cur.FilledQty > 0 || cur.Status == types.OrderStatusFilled {
				q := cur.FilledQty
				if q == 0 {
					q = cur.Quantity
				}
				return q, cur.AveragePrice, fmt.Errorf("the "+what+" filled %g of %g in %s; the rest was cancelled", q, t.Qty, o.FillWait)
			}
			return 0, 0, fmt.Errorf("the "+what+" did not fill in %s and was cancelled"+noneOpened(what), o.FillWait)
		}
		select {
		case <-ctx.Done():
			// Going away mid-fill: what filled must still be protected.
			if last, err := v.GetOrder(context.WithoutCancel(ctx), t.Venue, t.Symbol, entry.ID); err == nil && last != nil && last.FilledQty > 0 {
				return last.FilledQty, last.AveragePrice, fmt.Errorf("interrupted while the "+what+" filled: %w", ctx.Err())
			}
			return 0, 0, ctx.Err()
		case <-time.After(o.Poll):
		}
		next, err := v.GetOrder(ctx, t.Venue, t.Symbol, entry.ID)
		if err == nil && next != nil {
			cur = next
		}
	}
}

// protect places the stop and target for what filled, or closes it.
func protect(ctx context.Context, v Venue, t orderplan.Ticket, exit types.OrderSide, o Options, res *Result) (*Result, error) {
	// Protection is not given up because the caller went away.
	ctx = context.WithoutCancel(ctx)
	legs, err := v.PlaceBracket(ctx, &types.BracketRequest{
		Symbol: t.Symbol, Exchange: t.Venue, Side: exit, Quantity: res.Filled,
		TakeProfitPrice: t.Target, StopPrice: t.Stop, Reason: o.Reason,
	})
	if err == nil {
		for _, l := range legs {
			if l == nil {
				continue
			}
			switch l.Type {
			case types.OrderTypeStopLoss, types.OrderTypeStopLossLimit:
				res.StopID = l.ID
			case types.OrderTypeTakeProfitLimit, types.OrderTypeLimit:
				res.TargetID = l.ID
			}
		}
		res.Protected = res.StopID != ""
		if res.Protected {
			return res, nil
		}
		err = errors.New("the venue answered without a stop leg")
	}

	closing, cerr := v.PlaceOrder(ctx, &types.OrderRequest{
		Symbol: t.Symbol, Exchange: t.Venue, Side: exit, Type: types.OrderTypeMarket, Quantity: res.Filled, ReduceOnly: true, Reason: o.Reason,
	})
	if cerr != nil {
		return res, fmt.Errorf("%w: %g %s on %s; the stop and target failed (%v) and closing at market failed too (%v): close it on the exchange now",
			ErrUnprotected, res.Filled, t.Symbol, t.Venue, err, cerr)
	}
	res.Flattened = true
	res.CloseID = closing.ID
	res.Notes = append(res.Notes, "closed at market by order "+closing.ID)
	return res, fmt.Errorf("the stop and target could not be placed (%v), so the position was closed at market", err)
}

// positionOn is the open size on a contract (0 when flat), and its side.
func positionOn(ctx context.Context, v Venue, exchange, symbol string) (float64, string, error) {
	ps, err := v.GetPositions(ctx, exchange)
	if err != nil {
		return 0, "", err
	}
	for _, p := range ps {
		if p != nil && p.Size != 0 && sameSymbol(p.Symbol, symbol) {
			return p.Size, p.Side, nil
		}
	}
	return 0, "", nil
}

func sameSymbol(a, b string) bool {
	norm := func(s string) string { return strings.ToUpper(strings.ReplaceAll(s, "/", "")) }
	return norm(a) == norm(b)
}

func noneOpened(what string) string {
	if what == "entry" {
		return ", no position opened"
	}
	return ""
}
