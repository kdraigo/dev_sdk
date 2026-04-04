# DevSDK: Unified Trading Strategy SDK

`dev_sdk` is a production-grade Go SDK designed for building algorithmic trading bots. It provides a seamless developer experience where the same strategy code can run in multiple environments (backtesting, paper trading, or live) by changing a single configuration string.

## Core Features

- **Unified Interface**: Write your logic once using `OnCandle` and `OnOrderUpdate` callbacks.
- **Environment Agnostic**: Switch between `backtest`, `real_binance`, `real_bybit`, etc., without changing strategy code.
- **Timeframe Aggregation**: Automatically aggregate high-frequency 1m candles into your target timeframe (e.g., 5m, 15m, 1h).
- **Indicator Injection**: Automatically calculate and inject technical indicators (EMA, RSI, etc.) into your callback context.
- **Multi-Exchange Backtesting**: Supports synchronized backtesting across multiple exchanges in a single session.

## Getting Started

### Installation

```bash
go get github.com/kdraigo/flow_v1/dev_sdk
```

### Basic Bot Example

```go
package main

import (
	"context"
	"log"
	sdk "github.com/kdraigo/flow_v1/dev_sdk"
	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

func main() {
	// 1. Configure the SDK
	config := &types.Config{
		Environment: types.EnvBacktest, // Simply change to types.EnvRealBinance for live!
		Timeframe:   types.Timeframe15m,
		Indicators:  []string{"EMA50", "RSI14"},
		Backtest: &types.BacktestOptions{
			Endpoint:           "http://localhost:8080",
			RequestedExchanges: []string{"binance"},
			Assets:             []string{"BTC/USDT"},
			Wallets:            map[string]float64{"USDT": 10000.0},
		},
	}

	// 2. Initialize
	bot, _ := sdk.New(config)

	// 3. Define Strategy Logic
	bot.SetOnCandle(func(ctx *types.Context, candle *types.Candle) {
		ema50 := ctx.GetIndicator("EMA50")
		rsi14 := ctx.GetIndicator("RSI14")

		log.Printf("Price: %f, EMA50: %f, RSI14: %f", candle.Close, ema50, rsi14)

		if rsi14 < 30 {
			ctx.PlaceOrder(&types.OrderRequest{
				Symbol:   candle.Symbol,
				Exchange: candle.Exchange,
				Side:     types.OrderSideBuy,
				Type:     types.OrderTypeMarket,
				Quantity: 0.1,
			})
		}
	})

	// 4. Start the Engine
	bot.Start(context.Background())
}
```

## Architecture

The SDK is built using a pipeline-based architecture to ensure deterministic state management:

1.  **Exchange Adapter**: Standardizes raw WebSocket/REST data from different backends.
2.  **Timeframe Aggregator**: Buffers 1m ticks to build completed candles of the target timeframe.
3.  **Indicator Manager**: Computes requested math indicators using the `lib/core` library.
4.  **Context Wrapper**: Provides a thread-safe context to strategy callbacks with pre-calculated indicators and order execution methods.

## Testing

Run the full test suite across the SDK:

```bash
go test ./...
```

## Related Modules

- `backtester_engine`: The backend for historical data replay and order matching.
- `lib/core`: Internal math and trading primitives used for indicator calculations.
