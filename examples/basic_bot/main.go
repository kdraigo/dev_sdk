package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	sdk "github.com/kdraigo/flow_v1/dev_sdk"
	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

func main() {
	// 1. Unified configuration: simply switch the string here to move from Backtesting to Production!
	config := &types.Config{
		Environment: types.EnvBacktest,
		Timeframe:   types.Timeframe15m,
		// Example: requesting EMA and RSI calculation injects over our candles automatically
		// Indicators: []string{"EMA50", "RSI14"},
		Credentials: types.Credentials{
			APIKey:    "USER_BACKTEST_OR_REAL_KEY",
			APISecret: "SECRET",
		},
		Backtest: &types.BacktestOptions{
			Endpoint:           "http://localhost:4000",
			SessionName:        "Strategy-EMA-Cross",
			RequestedExchanges: []string{"binance"},
			Assets:             []string{"BTC/USDT"},
			Wallets:            map[string]float64{"USDT": 100000.0},
			StartTime:          time.Now().Add(-720 * time.Hour), // Last 30 days
			EndTime:            time.Now(),
		},
	}

	// 2. Initialize the architecture wrapper
	chatbotSDK, err := sdk.New(config)
	if err != nil {
		log.Fatalf("Failed to initialize dev_sdk: %v", err)
	}

	// 3. Bind core logic callbacks
	chatbotSDK.SetOnCandle(func(ctx *types.Context, candle *types.Candle) {
		log.Printf("Received %s candle close at %f", candle.Timeframe, candle.Close)

		// Access integrated indicators mathematically evaluated by the inner framework
		rsi, err := chatbotSDK.IndicatorManager().RSI("binance", "BTC/USDT", 14, "")
		if err != nil {
			log.Printf("Failed to calculate RSI: %v", err)
			return
		}

		log.Printf("Current RSI14: %f\n", rsi[len(rsi)-1])

		if rsi[len(rsi)-1] > 70 {
			req := &types.OrderRequest{
				Symbol:   candle.Symbol,
				Exchange: candle.Exchange,
				Side:     types.OrderSideSell,
				Type:     types.OrderTypeMarket,
				Quantity: 0.1,
			}
			placed, err := ctx.PlaceOrder(req)
			if err != nil {
				log.Printf("Failed to place order: %v", err)
			} else {
				log.Printf("Order placed! ID: %s", placed.ID)
			}
		}
	})

	var openedOrders = make(map[string]*types.Order)
	var mu sync.Mutex

	chatbotSDK.SetOnOrderUpdate(func(ctx *types.Context, order *types.Order) {
		log.Printf("Lifecycle Event: Order %s is now %s", order.ID, order.Status)

		// Typically you'd check for OrderStatusNew or OrderStatusFilled
		if order.Status == types.OrderStatusNew || order.Status == types.OrderStatusFilled {
			mu.Lock()
			openedOrders[order.ID] = order
			mu.Unlock()
		}
	})

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("\nReceived CTRL+C. Shutting down...")

		mu.Lock()
		log.Printf("Successfully opened orders: %d\n", len(openedOrders))
		for _, o := range openedOrders {
			log.Printf(" - ID: %s, Symbol: %s, Side: %s, Status: %s, Qty: %f, Price: %f\n", o.ID, o.Symbol, o.Side, o.Status, o.Quantity, o.Price)
		}
		mu.Unlock()

		os.Exit(0)
	}()

	// 4. Start execution (Blocks and processes events)
	ctx := context.Background()
	log.Println("Starting Bot...")
	if err := chatbotSDK.Start(ctx); err != nil {
		log.Fatalf("SDK Run Error: %v", err)
	}
}
