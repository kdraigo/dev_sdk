package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	sdk "github.com/kdraigo/flow_v1/dev_sdk"
	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

type bybitCreds struct {
	APIKey    string `json:"api_key"`
	APISecret string `json:"api_secret"`
}

func main() {
	fmt.Println("--- BYBIT CANDLE & ORDER SYNC SIMULATION ---")
	raw, err := os.ReadFile("credentials/bybit.json")
	if err != nil {
		log.Fatal("Credentials not found")
	}
	var creds bybitCreds
	_ = json.Unmarshal(raw, &creds)

	cfg := &types.Config{
		Environment: types.EnvTestBybit,
		Timeframes:  []types.Timeframe{types.Timeframe1m},
		Credentials: types.Credentials{APIKey: creds.APIKey, APISecret: creds.APISecret},
		Live: &types.LiveOptions{
			RequestedExchanges: []string{"bybit"},
			Assets:             []string{"BTCUSDT"},
		},
	}

	s, err := sdk.New(cfg)
	if err != nil {
		log.Fatalf("SDK init failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // Give enough time for multiple candles
	defer cancel()

	var lastCandleTime time.Time
	candlesVerified := 0
	const maxCandles = 2

	// Callback: Candle Update
	s.SetOnCandle(func(sdkCtx *types.Context, candle *types.Candle) {
		// 1. Detect Candle Open (First tick of a new timestamp)
		if candle.OpenTime.After(lastCandleTime) {
			fmt.Printf("\n[CANDLE OPEN] Time: %s Symbol: %s Open: %.2f\n",
				candle.OpenTime.Format("15:04:05"), candle.Symbol, candle.Open)
			lastCandleTime = candle.OpenTime

			fmt.Println(">>> Action: Placing MARKET SELL on Candle OPEN...")
			order, err := s.PlaceOrder(ctx, &types.OrderRequest{
				Symbol: "BTCUSDT", Exchange: "bybit", Side: types.OrderSideSell, Type: types.OrderTypeMarket, Quantity: 0.001,
			})
			if err != nil {
				fmt.Printf(" [ERROR] Order on open failed: %v\n", err)
			} else {
				fmt.Printf(" [SUCCESS] Order on open submitted: %s\n", order.ID)
			}
		}

		// 2. Detect Candle Close
		if candle.IsComplete {
			fmt.Printf("[CANDLE CLOSE] Time: %s Close: %.2f Volume: %.4f\n",
				candle.OpenTime.Format("15:04:05"), candle.Close, candle.Volume)

			fmt.Println(">>> Action: Placing MARKET SELL on Candle CLOSE...")
			order, err := s.PlaceOrder(ctx, &types.OrderRequest{
				Symbol: "BTCUSDT", Exchange: "bybit", Side: types.OrderSideSell, Type: types.OrderTypeMarket, Quantity: 0.001,
			})
			if err != nil {
				fmt.Printf(" [ERROR] Order on close failed: %v\n", err)
			} else {
				fmt.Printf(" [SUCCESS] Order on close submitted: %s\n", order.ID)
				candlesVerified++
				if candlesVerified >= maxCandles {
					fmt.Printf("\nVerification of %d candles complete. Shutting down in 5s...\n", maxCandles)
					time.AfterFunc(5*time.Second, cancel)
				}
			}
		} else {
			// Periodic tick log to show activity
			if time.Now().Second()%20 == 0 {
				fmt.Printf("  [TICK] Price: %.2f (Waiting for close...)\n", candle.Close)
			}
		}
	})

	// Callback: Order Update (Private stream)
	s.SetOnOrderUpdate(func(sdkCtx *types.Context, order *types.Order) {
		fmt.Printf("[ORDER UPDATE] ID: %s Status: %s Filled: %.4f\n",
			order.ID, order.Status, order.FilledQty)
	})

	fmt.Println("Starting SDK Loop (1m candles)...")
	if err := s.Start(ctx); err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		log.Fatalf("SDK Start error: %v", err)
	}

	fmt.Println("Simulation finished.")
}
