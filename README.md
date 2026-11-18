# DevSDK: Unified Trading Strategy SDK

`dev_sdk` is a Go SDK for building algorithmic trading bots. The same strategy code runs as a backtest on the kdraigo engine or live on an exchange; the difference is one configuration value.

Full documentation: **<https://kdraigo.com/docs>**

## Core Features

- **Unified interface**: write your logic once with candle and order-update callbacks.
- **Environment agnostic**: switch between `backtest`, `real_binance`, `real_binance_futures`, `real_bybit`, `real_bybit_futures` and their testnets without changing strategy code.
- **Perpetuals on Binance and Bybit** (`real_binance_futures`, `real_bybit_futures`): isolated margin, one-way positions, USDT-margined, as the backtest engine models them. Both need `LiveOptions.MaxOrderNotional` and `MaxLeverage`, and on mainnet `Armed` plus `KDRAIGO_LIVE_ARMED=1`. On Bybit the account must be a Unified Trading Account set to Isolated Margin (account-wide on Bybit, so the SDK refuses rather than switching it; a sub-account kept for the strategy is the clean way) and the API key an HMAC (system-generated) one with Contract trading permission.
- **Timeframe aggregation**: the stream is aggregated into each timeframe you request (e.g. 15m and 1h at once).
- **Indicators**: ~95 TA-Lib indicators through a timeframe-scoped indicator manager.
- **Multi-exchange backtesting**: synchronized backtests across several exchanges and pairs in one session.
- **Strategy plots**: draw your own values (moving averages, oscillators, signal markers) on the session chart at kdraigo.com.

## Getting Started

### Installation

```bash
go get github.com/kdraigo/dev_sdk@latest
```

**v1.3.0 or later is required.** The platform accepts only request signing version 2 since 2026-10-11; older versions get `401`.

### Basic Bot Example

```go
package main

import (
	"context"
	"log"
	"time"

	sdk "github.com/kdraigo/dev_sdk"
	"github.com/kdraigo/dev_sdk/types"
)

func main() {
	// 1. Configure the SDK
	cfg := &types.Config{
		Environment: types.EnvBacktest, // types.EnvRealBinance to trade live
		Timeframes:  []types.Timeframe{types.Timeframe15m},
		Credentials: types.Credentials{
			KeyID:      "your-key-id",      // from kdraigo.com/account/api-keys
			PrivateKey: "your-private-key", // hex-encoded Ed25519
		},
		Backtest: &types.BacktestOptions{
			Endpoint:           "https://api.kdraigo.com",
			SessionName:        "readme-example",
			RequestedExchanges: []string{"binance"},
			Assets:             []string{"BTC/USDT"}, // BASE/QUOTE
			Wallets:            map[string]float64{"USDT": 10000},
			StartTime:          time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
			EndTime:            time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	// 2. Initialize
	bot, err := sdk.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// 3. Define strategy logic
	bot.SetOnCandleFor(types.Timeframe15m, func(ctx *types.Context, c *types.Candle) {
		rsi, err := bot.IndicatorManagerFor(types.Timeframe15m).RSI(c.Exchange, c.Symbol, "close", 14)
		if err != nil || len(rsi) == 0 {
			return // not enough history yet
		}
		if rsi[len(rsi)-1] < 30 {
			ctx.PlaceOrder(&types.OrderRequest{
				Symbol:   c.Symbol,
				Exchange: c.Exchange,
				Side:     types.OrderSideBuy,
				Type:     types.OrderTypeMarket,
				Quantity: 0.1,
			})
		}
	})

	// 4. Run. Start returns an error if the run is cut short.
	if err := bot.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

### Live telemetry

To see a live strategy on kdraigo.com and get Telegram alerts, set:

```go
Live: &types.LiveOptions{
	RequestedExchanges: []string{"binance"},
	Assets:             []string{"BTC/USDT"},
	TelemetryURL:       "https://api.kdraigo.com/telemetry",
	StrategyName:       "my-strategy",
},
```

### Paper trading

kdraigo's paper exchange fills orders against Binance's real order books and trades, with no exchange account and no real funds. A paper run is a live run: the wall clock, closed 1m bars as they happen, and the same `Live` options. A config moves from paper to testnet or mainnet by changing `Environment` alone (plus arming, for mainnet).

```go
cfg := &types.Config{
	Environment: types.EnvPaperBinanceFutures, // or types.EnvPaperBinance (spot)
	Timeframes:  []types.Timeframe{types.Timeframe15m},
	Credentials: types.Credentials{KeyID: keyID, PrivateKey: privateKeyHex}, // a kdraigo API key
	Live: &types.LiveOptions{
		Assets:           []string{"BTCUSDT"},
		StrategyName:     "my-strategy",
		Leverage:         map[string]float64{"BTCUSDT": 3},
		MaxLeverage:      5,     // the futures caps apply on paper too
		MaxOrderNotional: 2_000, // the arming gates do not
	},
	Paper: &types.PaperOptions{
		AccountID: os.Getenv("KDRAIGO_PAPER_ACCOUNT"), // empty: open a new account
		Balances:  map[string]float64{"USDT": 10_000},
	},
}
```

- **The account lives on the platform.** The first run opens it and logs its id; set `Paper.AccountID` to carry on with it after a restart. Its markets, balances and fill model are fixed when it opens. Resting orders, and the take-profit and stop of a bracket, keep working while the bot is away. Stopping the account on kdraigo.com ends the run: `Start` returns the reason.
- **Fills.** A market order walks the real book. A resting limit fills by default in turn behind the quantity ahead of it at its price (`FillModel: "queue"`); `"trade_through"` waits for the market to trade past the price, `"touch"` fills when it reaches it. `LatencyMs` delays each order on its way to the book.
- **The venue's rules.** Orders are checked as Binance checks them, with its error codes and words (`*paper.Rejected`). Futures are isolated margin, one-way, liquidated on the mark price, and pay funding. Stops trigger on the mark price, as the exchange adapter sends them.
- **Order updates are pushed.** After a reconnect, the bars and order changes missed while away are delivered once each, and `SetOnResume` fires (`ColdStart` is true when the run attached to an existing account at start).
- **History** comes from the platform's data API (`Live.DataURL`), the venue's own bars.
- `Reason` and `Logs` go with each order to the paper exchange. `Live.TelemetryURL` is not used: the paper exchange records the run itself.
- `Paper.Endpoint` points at a local engine (`http://localhost:4004`). The paper adapter also implements `orderexec.Venue`, so code that places one order can place it on paper.

See `examples/paper_basic`.

### Plotting your indicators

A strategy can draw its own values on its chart, in backtests (v1.4.0+) and live (v1.5.0+, with `TelemetryURL` set). Declare each plot once, then add a value per bar:

```go
import "github.com/kdraigo/dev_sdk/plots"

ema, _ := bot.Plot(plots.Def{Key: "ema_fast", Title: "EMA 50"})                  // over the candles
rsi, _ := bot.Plot(plots.Def{Key: "rsi", Title: "RSI 14", Pane: "RSI",           // in a panel below them
	Levels: []plots.Level{{Value: 70}, {Value: 30}}})

bot.SetOnCandleFor(types.Timeframe1h, func(ctx *types.Context, c *types.Candle) {
	ema.Add(c, emaValue)
	rsi.Add(c, rsiValue)
})
```

`Kind` picks lines, step lines, histograms, areas, bands (`AddBand`) or markers (`Mark`). The strategy's style is the default; viewers can hide or restyle each plot on the chart. Plotting never fails a run: NaN values are skipped, and a plot that could not be declared is a nil series that ignores every call. Live, values are sent every 2 s in one request and kept through an outage. See `get_sdk_docs` section `plots` in [kdraigo_mcp](https://github.com/kdraigo/kdraigo_mcp) for every option and limit.

### Market data: funding, positioning and trade flow

Beside its prices, a strategy can read three more series (v1.11.0+). Data with a value on every bar rides on the candle; ask for it in `MarketData`:

```go
cfg.MarketData = []types.MarketData{types.MarketDataPositioning, types.MarketDataFlow}

bot.SetOnCandleFor(types.Timeframe1h, func(ctx *types.Context, c *types.Candle) {
	if p := c.Positioning; p != nil { // perpetuals only
		// p.OpenInterest, p.LongShortRatio, p.TopPositionRatio, p.TakerBuySellRatio;
		// p.Time = the end of the 5-minute period the values are from
	}
	if f := c.Flow; f != nil { // what executed in this bar, by taker side
		// f.BuyVolume, f.SellVolume, f.Delta, f.VWAP, f.LargePrints100k, f.Complete
	}
})
```

Lookbacks, and funding (three settlements a day), are calls:

```go
week := bot.Now().Add(-7 * 24 * time.Hour)
rates, _ := bot.GetFundingRates(ctx.Ctx, "binance_futures", "BTC/USDT", week, bot.Now())
oi, _ := bot.GetPositioning(ctx.Ctx, "binance_futures", "BTC/USDT", types.Timeframe1h, week, bot.Now())
flow, _ := bot.GetFlow(ctx.Ctx, "binance", "BTC/USDT", types.Timeframe1h, week, bot.Now())
```

The rule is the one `GetCandles` follows: nothing a live bot could not have known. In a backtest a `to` after `bot.Now()` is an error. A 5-minute positioning period is published 90 s after it ends, so a candle closing at 12:00 carries the period that ended at 11:55, and a bar ending at `to` is not returned yet. A candle's flow is its own bar.

- `nil` means none for that bar: spot has no positioning, and trade flow starts 2025-03-01. Positioning goes back to 2020-09, funding to 2024-01.
- A value a venue does not publish is `0`. Bybit has no top-trader or taker ratios and no open interest in quote; a real ratio is never 0.
- Live, the SDK reads the same series from the platform's data API (`LiveOptions.DataURL`, default `https://api.kdraigo.com/data`) as each candle closes, so a strategy sees the same numbers it saw in its backtest. A candle waits a few seconds for its flow bar.

See `examples/market_data` for a run that prints each coin's 7-day funding and its open interest every day.

### Checking and placing one order (orderplan, orderexec)

For code that places a single order outside a strategy, such as kdraigo-mcp's executor for orders from the user's AI:

- `orderplan.Build(plan, market, account, limits)` sizes a plan (entry, stop, target, at market or with a limit) from the user's own limits and checks it: shape, entry gap, the stop before the estimated isolated liquidation, margin, open positions, daily loss. `Hash` binds a confirmation to the exact order; `Recheck` runs again at placing.
- `orderexec.Snapshot(ctx, venue, exchange, symbol, now)` reads what the checker takes: the contract's rules and mark price (`Contract` on both futures adapters, for any symbol), the free margin, open positions and today's realized P&L.
- `orderexec.Open(ctx, venue, ticket, opts)` places a passed ticket. At market: the leverage, the entry, a wait for the fill, then the stop and target for what filled. If they cannot be placed, the position is closed at market; if that fails too, the error carries `ErrUnprotected`. With a limit: Bybit only, with the stop and target attached to the order (`PlaceProtectedLimit`), so they exist at the fill even if the process is gone. The venue must be prepared (`PrepareSession`) with the symbol among its assets and caps that cover the order.
- Actions on what is open: `orderplan.BuildClose`, `BuildCancel`, `BuildMoveStop` (with `ActionHash`, `VerifyAction`, `RecheckAction`), read with `orderexec.Holdings` (the position with its stop and the venue's liquidation price, resting entries, resting exits) and carried out by `orderexec.Act`: a close is a reduce-only market order, then the exits left once flat are taken back; a cancel takes back a resting entry; a move places the new stop before the old one goes (on Bybit, a stop held on the position is moved in place with `SetPositionStop`).

## Architecture

The SDK is a pipeline, which keeps state deterministic:

1.  **Exchange adapter**: standardizes the engine's or the exchange's WebSocket/REST data.
2.  **Timeframe aggregator**: builds completed candles of each requested timeframe.
3.  **Indicator manager**: keeps per-pair history and computes indicators on demand.
4.  **Context**: what callbacks receive: the clock, order placement and cancellation.

## Testing

Run the full test suite across the SDK:

```bash
go test ./...
```

## Related Modules

- [kdraigo_mcp](https://github.com/kdraigo/kdraigo_mcp): an MCP server that gives Claude the full strategy development loop on top of this SDK.
- [example_strategy](https://github.com/kdraigo/example_strategy): starter templates.
