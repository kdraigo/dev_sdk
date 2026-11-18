package paper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/kdraigo/dev_sdk/aggregator"
	"github.com/kdraigo/dev_sdk/exchange/live"
	"github.com/kdraigo/dev_sdk/types"
)

// GetAccount is the account's funds in an asset, or in every asset when
// asset is empty. Free is what new orders can use; Lock is what open orders
// hold and, on futures, the margin of open positions.
func (c *Client) GetAccount(ctx context.Context, exchange, asset string) (*types.Account, error) {
	v, err := c.view(ctx)
	if err != nil {
		return nil, err
	}
	acc := c.account(v.Balances, strings.ToUpper(asset))
	return &acc, nil
}

func (c *Client) account(balances []balanceWire, asset string) types.Account {
	acc := types.Account{Exchange: c.venue}
	at := map[string]int{}
	for _, b := range balances {
		// The ledger's other buckets (deposit, fees, funding, realized P&L
		// ...) are its sinks: money outside the account.
		held := b.Bucket == "locked" || strings.HasPrefix(b.Bucket, "position_margin:")
		if (asset != "" && b.Asset != asset) || (b.Bucket != "free" && !held) {
			continue
		}
		i, ok := at[b.Asset]
		if !ok {
			i = len(acc.Balances)
			at[b.Asset] = i
			acc.Balances = append(acc.Balances, types.Balance{Asset: b.Asset})
		}
		if held {
			acc.Balances[i].Lock += float64(b.Amount)
		} else {
			acc.Balances[i].Free += float64(b.Amount)
		}
	}
	return acc
}

// GetPositions is the account's open futures positions, with the engine's
// marks: unrealized P&L and the liquidation price at the mark, and the
// realized P&L, fees and funding of each position so far.
func (c *Client) GetPositions(ctx context.Context, exchange string) ([]*types.Position, error) {
	if exchange != "" && exchange != c.venue {
		return nil, nil
	}
	raw, err := c.callRetry(ctx, "positions", nil)
	if err != nil {
		return nil, err
	}
	var ps []positionWire
	if err := json.Unmarshal(raw, &ps); err != nil {
		return nil, err
	}
	out := []*types.Position{}
	for _, p := range ps {
		out = append(out, c.position(p))
	}
	return out, nil
}

func (c *Client) position(p positionWire) *types.Position {
	sym := c.symbols[p.Symbol]
	if sym == "" {
		sym = p.Symbol
	}
	return &types.Position{Symbol: sym, Exchange: c.venue, Side: p.Side, Size: float64(p.Size), EntryPrice: float64(p.EntryPrice),
		Leverage: float64(p.Leverage), IsolatedMargin: float64(p.IsolatedMargin), RealizedPnL: float64(p.RealizedPnL),
		FundingPaid: float64(p.Funding), MarkPrice: float64(p.Mark), UnrealizedPnL: float64(p.UnrealizedPnL),
		LiquidationPrice: float64(p.LiquidationPrice)}
}

func (c *Client) view(ctx context.Context) (view, error) {
	var v view
	raw, err := c.callRetry(ctx, "account", nil)
	if err != nil {
		return v, err
	}
	return v, json.Unmarshal(raw, &v)
}

// Contract is a market's rules and its mark now (spot: the last trade), with
// the first margin tier's leverage and maintenance rate on futures.
func (c *Client) Contract(ctx context.Context, exchange, symbol string) (*types.Contract, error) {
	vs, err := c.market(exchange, symbol)
	if err != nil {
		return nil, err
	}
	k, err := c.contract(ctx, vs)
	if err != nil {
		return nil, err
	}
	mark := float64(k.Mark)
	if mark <= 0 {
		mark = float64(k.Last)
	}
	return &types.Contract{Exchange: c.venue, Symbol: vs, TickSize: float64(k.TickSize), StepSize: float64(k.StepSize),
		MinQty: float64(k.MinQty), MinNotional: float64(k.MinNotional), MaxLeverage: float64(k.MaxLeverage),
		MaintenanceMargin: float64(k.MMR), Mark: mark}, nil
}

func (c *Client) contract(ctx context.Context, vs string) (contractWire, error) {
	var k contractWire
	raw, err := c.callRetry(ctx, "contract", map[string]any{"symbol": vs})
	if err != nil {
		return k, err
	}
	return k, json.Unmarshal(raw, &k)
}

// RealizedPnL is what the account realized since a moment, in the quote
// asset: closed P&L less fees and funding, over the positions open or closed
// since then (negative is a loss).
func (c *Client) RealizedPnL(ctx context.Context, exchange string, since time.Time) (float64, error) {
	raw, err := c.callRetry(ctx, "realized_pnl", map[string]any{"since": since.UnixMilli()})
	if err != nil {
		return 0, err
	}
	var r struct {
		RealizedPnL num `json:"realized_pnl"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return 0, err
	}
	return float64(r.RealizedPnL), nil
}

// OrderFeed: the engine pushes every order change, and a reconnect reports
// what changed while away.
func (c *Client) OrderFeed() types.OrderFeed {
	return types.OrderFeed{Push: true, Latency: 200 * time.Millisecond}
}

// Next is a backtest's step; a paper run follows the clock.
func (c *Client) Next(ctx context.Context) error { return nil }

// historyPage is how many bars one data API read asks for.
const historyPage = 10_000

// GetHistoricalCandles reads closed candles in [from, to] from the
// platform's data API (Config.Live.DataURL), the venue's own bars.
func (c *Client) GetHistoricalCandles(ctx context.Context, exchange, symbol string, from, to time.Time, tf types.Timeframe) ([]*types.Candle, error) {
	if exchange != "" && exchange != c.venue {
		return nil, fmt.Errorf("paper: this run trades on %s, not %q", c.venue, exchange)
	}
	if from.After(to) {
		return nil, fmt.Errorf("paper: from %s is after to %s", from, to)
	}
	dur := aggregator.ExtractDuration(tf)
	if dur <= 0 {
		return nil, fmt.Errorf("paper: unsupported timeframe %q", tf)
	}
	vs := live.VenueSymbol(symbol)
	now := time.Now()
	var out []*types.Candle
	for start := from; !start.After(to); {
		end := start.Add(time.Duration(historyPage-1) * dur)
		if end.After(to) {
			end = to
		}
		var rows []struct {
			OpenTime   int64   `json:"open_time"`
			Open       float64 `json:"open"`
			High       float64 `json:"high"`
			Low        float64 `json:"low"`
			Close      float64 `json:"close"`
			Volume     float64 `json:"volume"`
			QuoteVol   float64 `json:"quote_asset_volume"`
			TakerBase  float64 `json:"taker_buy_base_asset_volume"`
			TakerQuote float64 `json:"taker_buy_quote_asset_volume"`
			Trades     int64   `json:"trade_num"`
		}
		q := url.Values{"exchange": {c.venue}, "symbol": {vs}, "timeframe": {string(tf)},
			"from": {strconv.FormatInt(start.UnixMilli(), 10)}, "to": {strconv.FormatInt(end.UnixMilli(), 10)}}
		if err := c.data(ctx, "/candles", q, &rows); err != nil {
			return nil, err
		}
		for _, r := range rows {
			open := time.UnixMilli(r.OpenTime)
			if open.Add(dur).After(now) {
				continue // still forming
			}
			out = append(out, &types.Candle{Symbol: symbol, Exchange: c.venue, Timeframe: tf, OpenTime: open, CloseTime: open.Add(dur),
				Open: r.Open, High: r.High, Low: r.Low, Close: r.Close, Volume: r.Volume, IsComplete: true, TradeCount: r.Trades,
				QuoteVolume: r.QuoteVol, TakerBuyBaseVolume: r.TakerBase, TakerBuyQuoteVolume: r.TakerQuote})
		}
		start = end.Add(time.Millisecond)
	}
	return out, nil
}

func (c *Client) data(ctx context.Context, path string, q url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.dataURL+path+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("data API %s: %w", path, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode != http.StatusOK {
		var e struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(body, &e) == nil && e.Error != "" {
			return fmt.Errorf("data API %s: %s (status %d)", path, e.Error, resp.StatusCode)
		}
		return fmt.Errorf("data API %s: status %d", path, resp.StatusCode)
	}
	return json.Unmarshal(body, out)
}
