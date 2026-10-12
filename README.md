# DevSDK: Unified Trading Strategy SDK

`dev_sdk` is a Go SDK for building algorithmic trading bots. The same strategy code runs as a backtest on the kdraigo engine or live on an exchange; the difference is one configuration value.

Full documentation: **<https://kdraigo.com/docs>**

## Core Features

- **Unified interface**: write your logic once with candle and order-update callbacks.
- **Environment agnostic**: switch between `backtest`, `real_binance`, `real_binance_futures`, `real_bybit` and their testnets without changing strategy code.
- **Timeframe aggregation**: the stream is aggregated into each timeframe you request (e.g. 15m and 1h at once).
- **Indicators**: ~95 TA-Lib indicators through a timeframe-scoped indicator manager.
- **Multi-exchange backtesting**: synchronized backtests across several exchanges and pairs in one session.

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
			KeyID:      "your-key-id",      // from kdraigo.com/api-keys
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
