package orderplan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	btc = Market{TickSize: 0.1, StepSize: 0.001, MinQty: 0.001, MinNotional: 5, MaxLeverage: 100, MaintenanceMargin: 0.005, Mark: 100050}
	lim = Limits{RiskPerTrade: 50, MaxNotional: 10000, Leverage: 3, MaxLeverage: 5, MaxOpenPositions: 3, MaxDailyLoss: 200, MaxEntryGap: 0.005, MaxLimitGap: 0.1, MinStopGap: 0.001}
	acc = Account{Available: 2000}
)

func longPlan() Plan {
	return Plan{Venue: "bybit_futures", Symbol: "btcusdt", Side: Long, EntryType: AtMarket, Entry: 100000, Stop: 99000.04, Target: 103000.06}
}

func failed(t Ticket) []string {
	var out []string
	for _, c := range t.Checks {
		if !c.OK {
			out = append(out, c.Name)
		}
	}
	return out
}

func TestBuildALongPlan(t *testing.T) {
	tk := Build(longPlan(), btc, acc, lim)
	require.True(t, tk.OK, "%v", tk.Checks)
	assert.Equal(t, "BTCUSDT", tk.Symbol)
	assert.Equal(t, 99000.0, tk.Stop, "rounded to the tick")
	assert.Equal(t, 103000.1, tk.Target)
	// 50 USDT over a 1050 stop distance is 0.0476…, floored to the 0.001 step.
	assert.Equal(t, 0.047, tk.Qty)
	assert.InDelta(t, 49.35, tk.Risk, 1e-9)
	assert.InDelta(t, 4702.35, tk.Notional, 1e-9)
	assert.InDelta(t, 4702.35/3, tk.Margin, 1e-9)
	assert.Less(t, tk.Liquidation, tk.Stop)
	assert.InDelta(t, tk.Reward/tk.Risk, tk.RewardRisk, 1e-12)
	assert.Len(t, tk.Hash, 64)
	assert.Equal(t, tk, Build(longPlan(), btc, acc, lim), "deterministic")
}

func TestBuildAShortPlan(t *testing.T) {
	p := Plan{Venue: "binance_futures", Symbol: "BTCUSDT", Side: Short, EntryType: AtMarket, Entry: 100100, Stop: 101000, Target: 97000}
	tk := Build(p, btc, acc, lim)
	require.True(t, tk.OK, "%v", tk.Checks)
	assert.Greater(t, tk.Liquidation, tk.Stop)
	assert.Equal(t, 0.052, tk.Qty) // 50 / 950
}

func TestBuildRefuses(t *testing.T) {
	cases := map[string]struct {
		edit func(p *Plan, m *Market, a *Account, l *Limits)
		want string
	}{
		"no limits":               {func(p *Plan, m *Market, a *Account, l *Limits) { *l = Limits{} }, "limits"},
		"no daily loss limit":     {func(p *Plan, m *Market, a *Account, l *Limits) { l.MaxDailyLoss = 0 }, "limits"},
		"unknown side":            {func(p *Plan, m *Market, a *Account, l *Limits) { p.Side = "up" }, "plan"},
		"no entry type chosen":    {func(p *Plan, m *Market, a *Account, l *Limits) { p.EntryType = "" }, "entry type"},
		"no price":                {func(p *Plan, m *Market, a *Account, l *Limits) { m.Mark = 0 }, "market"},
		"stop above entry":        {func(p *Plan, m *Market, a *Account, l *Limits) { p.Stop = 100500 }, "shape"},
		"made-up entry":           {func(p *Plan, m *Market, a *Account, l *Limits) { p.Entry, p.Stop, p.Target = 64250, 63600, 65900 }, "entry"},
		"price already past stop": {func(p *Plan, m *Market, a *Account, l *Limits) { m.Mark = 98900 }, "price now"},
		"stop inside the spread":  {func(p *Plan, m *Market, a *Account, l *Limits) { p.Stop = 100000 }, "stop distance"},
		"stop past liquidation": {func(p *Plan, m *Market, a *Account, l *Limits) {
			p.Stop, l.Leverage, l.MaxLeverage, l.MaxEntryGap = 79000, 5, 5, 0.01
		}, "liquidation"},
		"leverage over the limit": {func(p *Plan, m *Market, a *Account, l *Limits) { l.Leverage = 10 }, "leverage"},
		"leverage over the venue": {func(p *Plan, m *Market, a *Account, l *Limits) { l.Leverage, l.MaxLeverage, m.MaxLeverage = 3, 5, 2 }, "leverage"},
		"too small":               {func(p *Plan, m *Market, a *Account, l *Limits) { l.RiskPerTrade = 0.5 }, "size"},
		"no margin":               {func(p *Plan, m *Market, a *Account, l *Limits) { a.Available = 100 }, "margin"},
		"position already open":   {func(p *Plan, m *Market, a *Account, l *Limits) { a.PositionOnSymbol = true }, "one position per contract"},
		"too many positions":      {func(p *Plan, m *Market, a *Account, l *Limits) { a.OpenPositions = 3 }, "open positions"},
		"daily loss reached":      {func(p *Plan, m *Market, a *Account, l *Limits) { a.LossToday = 200 }, "daily loss"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			p, m, a, l := longPlan(), btc, acc, lim
			c.edit(&p, &m, &a, &l)
			tk := Build(p, m, a, l)
			assert.False(t, tk.OK)
			assert.Contains(t, failed(tk), c.want)
		})
	}
}

func TestSizeRespectsTheLimits(t *testing.T) {
	// A large risk is cut to the position value limit, and says so.
	l := lim
	l.RiskPerTrade = 1000
	tk := Build(longPlan(), btc, Account{Available: 5000}, l)
	require.True(t, tk.OK, "%v", tk.Checks)
	assert.LessOrEqual(t, tk.Notional, l.MaxNotional)
	assert.Equal(t, 0.099, tk.Qty) // 10000 / 100050, floored
	for _, c := range tk.Checks {
		if c.Name == "size" {
			assert.Contains(t, c.Detail, "position limit")
		}
	}

	// The plan's own risk can lower the limit, never raise it.
	p := longPlan()
	p.Risk = 10
	assert.Equal(t, 0.009, Build(p, btc, acc, lim).Qty)
	p.Risk = 500
	assert.Equal(t, 0.047, Build(p, btc, acc, lim).Qty)
}

func TestHashBindsWhatIsPlaced(t *testing.T) {
	tk := Build(longPlan(), btc, acc, lim)
	assert.True(t, Passed(VerifyShape(tk)))

	for name, edit := range map[string]func(*Ticket){
		"qty":      func(x *Ticket) { x.Qty = 0.5 },
		"stop":     func(x *Ticket) { x.Stop = 90000 },
		"target":   func(x *Ticket) { x.Target = 200000 },
		"leverage": func(x *Ticket) { x.Leverage = 20 },
		"side":     func(x *Ticket) { x.Side = Short },
		"symbol":   func(x *Ticket) { x.Symbol = "ETHUSDT" },
	} {
		x := tk
		edit(&x)
		assert.NotEqual(t, tk.Hash, Hash(x), name)
		assert.False(t, Passed(VerifyShape(x)), name)
	}

	// A failed ticket fails the server's check too.
	bad := Build(Plan{Venue: "bybit_futures", Symbol: "BTCUSDT", Side: Long, EntryType: AtMarket, Entry: 64250, Stop: 63600, Target: 65900}, btc, acc, lim)
	assert.False(t, Passed(VerifyShape(bad)))
}

func TestRecheckAtPlacing(t *testing.T) {
	tk := Build(longPlan(), btc, acc, lim)
	require.True(t, tk.OK)
	tol := Tolerance{PriceDrift: 0.003, RiskSlack: 0.1}

	m := btc
	m.Mark = 100100 // 0.05% away: 51.7 USDT at risk, inside the 10% slack
	assert.True(t, Passed(Recheck(tk, m, acc, lim, tol)))
	assert.Contains(t, failedNames(Recheck(tk, m, acc, lim, Tolerance{PriceDrift: 0.003})), "size", "no slack: over the 50 USDT limit")

	m.Mark = 101000 // 0.95% away
	cs := Recheck(tk, m, acc, lim, tol)
	assert.False(t, Passed(cs))
	assert.Contains(t, failedNames(cs), "price drift")

	m.Mark = 98900 // through the stop
	assert.Contains(t, failedNames(Recheck(tk, m, acc, lim, Tolerance{PriceDrift: 0.05, RiskSlack: 0.1})), "price now")

	a := acc
	a.PositionOnSymbol = true
	assert.Contains(t, failedNames(Recheck(tk, btc, a, lim, tol)), "one position per contract")

	// The user lowered a limit after confirming: the limit at placing wins.
	l := lim
	l.MaxNotional = 1000
	assert.Contains(t, failedNames(Recheck(tk, btc, acc, l, tol)), "size")

	x := tk
	x.Qty = 1
	assert.Equal(t, []string{"unchanged"}, failedNames(Recheck(x, btc, acc, lim, tol)))
}

func failedNames(cs []Check) []string {
	var out []string
	for _, c := range cs {
		if !c.OK {
			out = append(out, c.Name)
		}
	}
	return out
}

func TestPassedNeedsChecks(t *testing.T) {
	assert.False(t, Passed(nil))
	assert.True(t, Passed([]Check{{OK: true}}))
	assert.False(t, Passed([]Check{{OK: true}, {OK: false}}))
}

func limitPlan() Plan {
	return Plan{Venue: "bybit_futures", Symbol: "BTCUSDT", Side: Long, EntryType: AtLimit, Entry: 99500.04, Stop: 99000, Target: 103000}
}

func TestBuildALimitEntry(t *testing.T) {
	rich := Account{Available: 5000}
	tk := Build(limitPlan(), btc, rich, lim)
	require.True(t, tk.OK, "%v", tk.Checks)
	assert.Equal(t, AtLimit, tk.EntryType)
	assert.Equal(t, 99500.0, tk.Entry, "the limit is the drawn entry on the tick grid")
	assert.Equal(t, 99500.0, tk.Price, "sized at its own price, not the market's")
	assert.Equal(t, 0.1, tk.Qty) // 50 / 500
	assert.InDelta(t, 50, tk.Risk, 1e-9)
	assert.Less(t, tk.Liquidation, tk.Stop)
	assert.True(t, Passed(VerifyShape(tk)))

	market := Build(longPlan(), btc, rich, lim)
	assert.NotEqual(t, market.Hash, tk.Hash)
	x := tk
	x.EntryType = AtMarket
	assert.NotEqual(t, tk.Hash, Hash(x), "the entry type is part of what is confirmed")
	x = tk
	x.Entry = 99400
	assert.NotEqual(t, tk.Hash, Hash(x))
}

func TestBuildRefusesALimit(t *testing.T) {
	rich := Account{Available: 5000}
	cases := map[string]struct {
		edit func(p *Plan, l *Limits)
		want string
	}{
		"buy limit above the price": {func(p *Plan, l *Limits) { p.Entry, p.Target = 100500, 104000 }, "limit side"},
		"far away, made up":         {func(p *Plan, l *Limits) { p.Entry, p.Stop, p.Target = 64250, 63600, 65900 }, "entry"},
		"no limit gap set":          {func(p *Plan, l *Limits) { l.MaxLimitGap = 0 }, "limits"},
		"stop above the limit":      {func(p *Plan, l *Limits) { p.Stop = 99600 }, "shape"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			p, l := limitPlan(), lim
			c.edit(&p, &l)
			tk := Build(p, btc, rich, l)
			assert.False(t, tk.OK)
			assert.Contains(t, failed(tk), c.want)
		})
	}

	// A sell limit rests at or above the price.
	short := Plan{Venue: "bybit_futures", Symbol: "BTCUSDT", Side: Short, EntryType: AtLimit, Entry: 99000, Stop: 99500, Target: 97000}
	assert.Contains(t, failed(Build(short, btc, rich, lim)), "limit side")
	short.Entry, short.Stop = 100600, 101200
	assert.True(t, Build(short, btc, rich, lim).OK)
}

func TestRecheckALimit(t *testing.T) {
	rich := Account{Available: 5000}
	tk := Build(limitPlan(), btc, rich, lim)
	require.True(t, tk.OK)
	tol := Tolerance{PriceDrift: 0.003, RiskSlack: 0.1}

	// The market moved 2% up: a limit still rests at its own price, same risk.
	m := btc
	m.Mark = 102050
	assert.True(t, Passed(Recheck(tk, m, rich, lim, tol)), "%v", Recheck(tk, m, rich, lim, tol))

	// Through the stop: nothing to rest any more.
	m.Mark = 98900
	assert.Contains(t, failedNames(Recheck(tk, m, rich, lim, tol)), "price now")
}
