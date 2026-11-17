package live

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// Contract reads and realized P&L: what code that checks an order before
// placing it needs from the venue (the AI-orders executor, dev_sdk/orderexec).
// They read only; nothing here places, cancels or changes a setting.

// Contract reads a Binance USDⓈ-M perpetual's rules, its first leverage
// bracket (max leverage, maintenance margin rate) and its mark price now.
func (b *BinanceFuturesClient) Contract(ctx context.Context, exchange, symbol string) (*types.Contract, error) {
	if b.client == nil {
		return nil, errNotPrepared
	}
	sym := VenueSymbol(symbol)
	info, err := b.client.NewExchangeInfoService().Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("exchangeInfo: %w", err)
	}
	c := &types.Contract{Exchange: exchange, Symbol: sym}
	found := false
	for i := range info.Symbols {
		s := info.Symbols[i]
		if s.Symbol != sym {
			continue
		}
		if string(s.ContractType) != "PERPETUAL" {
			return nil, fmt.Errorf("%s on Binance futures is a %s contract; orders here are on perpetuals only", sym, s.ContractType)
		}
		if s.Status != "TRADING" {
			return nil, fmt.Errorf("%s is not trading on Binance futures (status %s)", sym, s.Status)
		}
		if lot := s.LotSizeFilter(); lot != nil {
			c.StepSize = parseFloat(lot.StepSize)
			c.MinQty = parseFloat(lot.MinQuantity)
		}
		if pf := s.PriceFilter(); pf != nil {
			c.TickSize = parseFloat(pf.TickSize)
		}
		if mn := s.MinNotionalFilter(); mn != nil {
			c.MinNotional = parseFloat(mn.Notional)
		}
		found = true
		break
	}
	if !found {
		return nil, fmt.Errorf("no perpetual contract on Binance futures for %s", sym)
	}

	idx, err := b.client.NewPremiumIndexService().Symbol(sym).Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("mark price %s: %w", sym, err)
	}
	if len(idx) == 0 || parseFloat(idx[0].MarkPrice) <= 0 {
		return nil, fmt.Errorf("no mark price for %s", sym)
	}
	c.Mark = parseFloat(idx[0].MarkPrice)

	// The leverage brackets are a signed read. Without them the checker
	// assumes a higher maintenance rate, so a failure here costs precision,
	// never safety.
	if brackets, err := b.client.NewGetLeverageBracketService().Symbol(sym).Do(ctx); err == nil {
		for _, lb := range brackets {
			if lb == nil || lb.Symbol != sym || len(lb.Brackets) == 0 {
				continue
			}
			first := lb.Brackets[0]
			for _, br := range lb.Brackets {
				if br.Bracket < first.Bracket {
					first = br
				}
			}
			c.MaxLeverage = float64(first.InitialLeverage)
			c.MaintenanceMargin = first.MaintMarginRatio
		}
	}
	return c, nil
}

// RealizedPnL is what the futures wallet realized since a moment: closed
// trades, commissions and funding, in USDT (negative is a loss).
func (b *BinanceFuturesClient) RealizedPnL(ctx context.Context, exchange string, since time.Time) (float64, error) {
	if b.client == nil {
		return 0, errNotPrepared
	}
	const page = 1000
	total := 0.0
	start := since.UnixMilli()
	for range 20 {
		rows, err := b.client.NewGetIncomeHistoryService().StartTime(start).Limit(page).Do(ctx)
		if err != nil {
			return 0, fmt.Errorf("income history: %w", err)
		}
		last := start
		for _, r := range rows {
			switch r.IncomeType {
			case "REALIZED_PNL", "COMMISSION", "FUNDING_FEE":
				if strings.EqualFold(r.Asset, "USDT") {
					total += parseFloat(r.Income)
				}
			}
			if r.Time > last {
				last = r.Time
			}
		}
		if len(rows) < page {
			return total, nil
		}
		start = last + 1
	}
	return 0, fmt.Errorf("income history: more than %d rows since %s", 20*page, since.UTC().Format(time.RFC3339))
}

// errNotPrepared: the Binance futures client exists once PrepareSession ran
// (it picks testnet or mainnet, process-wide).
var errNotPrepared = errors.New("binance futures: PrepareSession first")

// bybitRiskTier is one row of GET /v5/market/risk-limit.
type bybitRiskTier struct {
	MaintenanceMargin string `json:"maintenanceMargin"`
	MaxLeverage       string `json:"maxLeverage"`
	IsLowestRisk      int    `json:"isLowestRisk"`
}

// Contract reads a Bybit USDT perpetual's rules, its lowest risk tier and its
// mark price now.
func (b *BybitFuturesClient) Contract(ctx context.Context, exchange, symbol string) (*types.Contract, error) {
	b.ensureAPI()
	sym := VenueSymbol(symbol)
	var r bybitList[bybitInstrument]
	if err := b.api.get(ctx, "/v5/market/instruments-info", url.Values{"category": {"linear"}, "symbol": {sym}}, false, &r); err != nil {
		return nil, fmt.Errorf("instruments-info %s: %w", sym, err)
	}
	if len(r.List) == 0 {
		return nil, fmt.Errorf("no perpetual contract on Bybit for %s", sym)
	}
	in := r.List[0]
	if in.ContractType != "LinearPerpetual" || !strings.EqualFold(in.SettleCoin, "USDT") {
		return nil, fmt.Errorf("%s on Bybit is a %s settled in %s; orders here are on USDT perpetuals only", sym, in.ContractType, in.SettleCoin)
	}
	if in.Status != "Trading" {
		return nil, fmt.Errorf("%s is not trading on Bybit (status %s)", sym, in.Status)
	}
	c := &types.Contract{
		Exchange:    exchange,
		Symbol:      sym,
		TickSize:    parseFloat(in.PriceFilter.TickSize),
		StepSize:    parseFloat(in.LotSizeFilter.QtyStep),
		MinQty:      parseFloat(in.LotSizeFilter.MinOrderQty),
		MinNotional: parseFloat(in.LotSizeFilter.MinNotionalValue),
		MaxLeverage: parseFloat(in.LeverageFilter.MaxLeverage),
	}
	mark, err := b.fetchMark(ctx, sym)
	if err != nil {
		return nil, fmt.Errorf("mark price %s: %w", sym, err)
	}
	if mark <= 0 {
		return nil, fmt.Errorf("no mark price for %s", sym)
	}
	c.Mark = mark

	// As on Binance, a failed tier read costs precision, never safety.
	var tiers bybitList[bybitRiskTier]
	if err := b.api.get(ctx, "/v5/market/risk-limit", url.Values{"category": {"linear"}, "symbol": {sym}}, false, &tiers); err == nil && len(tiers.List) > 0 {
		low := tiers.List[0]
		for _, t := range tiers.List {
			if t.IsLowestRisk == 1 {
				low = t
				break
			}
		}
		mmr := parseFloat(low.MaintenanceMargin)
		if mmr >= 1 { // a percent, not a fraction
			mmr /= 100
		}
		c.MaintenanceMargin = mmr
		if lev := parseFloat(low.MaxLeverage); lev > 0 && (c.MaxLeverage == 0 || lev < c.MaxLeverage) {
			c.MaxLeverage = lev
		}
	}
	return c, nil
}

// RealizedPnL is what the account closed since a moment on USDT perpetuals
// (Bybit's closed P&L, net of the trades' fees), in USDT.
func (b *BybitFuturesClient) RealizedPnL(ctx context.Context, exchange string, since time.Time) (float64, error) {
	b.ensureAPI()
	total := 0.0
	cursor := ""
	for range 20 {
		q := url.Values{"category": {"linear"}, "startTime": {fmt.Sprint(since.UnixMilli())}, "limit": {"100"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var r bybitList[struct {
			ClosedPnl string `json:"closedPnl"`
		}]
		if err := b.api.get(ctx, "/v5/position/closed-pnl", q, true, &r); err != nil {
			return 0, fmt.Errorf("closed-pnl: %w", err)
		}
		for _, row := range r.List {
			total += parseFloat(row.ClosedPnl)
		}
		if r.NextPageCursor == "" || len(r.List) == 0 {
			return total, nil
		}
		cursor = r.NextPageCursor
	}
	return 0, fmt.Errorf("closed-pnl: more than 2000 closed trades since %s", since.UTC().Format(time.RFC3339))
}
