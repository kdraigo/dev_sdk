package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	sdk "github.com/kdraigo/flow_v1/dev_sdk"
	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

const (
	totalRuns  = 10
	rsiPeriod  = 14
	btcQty     = 0.1
	exchange   = "binance"
	symbol     = "BTC/USDT"
	quoteAsset = "USDT"
	baseAsset  = "BTC"
)

// Fixed time window — computed once at startup so all 10 runs use identical candle data.
var (
	backtestEnd   = time.Now().UTC().Truncate(time.Hour)
	backtestStart = backtestEnd.Add(-720 * time.Hour)
)

func makeConfig() *types.Config {
	return &types.Config{
		Environment: types.EnvBacktest,
		Timeframe:   types.Timeframe15m,
		Credentials: types.Credentials{
			KeyID:      "7b89ece9-97ae-4b76-b938-ce9e5345bfce",
			PrivateKey: "385d5c080a1b4140a5ed9ee76d0ef3fcd291cabab4ec6759bc178ad3a8ed837148309e3cb2a3a014c93d68b4f20a0ba5978ab300531c844dcec672925eb8d63a",
		},
		Backtest: &types.BacktestOptions{
			Endpoint:           "http://localhost:4000",
			SessionName:        "Determinism-Test",
			RequestedExchanges: []string{exchange},
			Assets:             []string{symbol},
			Wallets:            map[string]float64{quoteAsset: 100000.0},
			StartTime:          backtestStart,
			EndTime:            backtestEnd,
		},
	}
}

// runOnce executes the RSI strategy once and returns all FILLED orders in placement order.
// Orders are collected synchronously inside OnCandle from PlaceOrder's return value,
// eliminating any race between the async order-update pipeline and shutdown.
func runOnce(ctx context.Context, run int) ([]*types.Order, error) {
	botSDK, err := sdk.New(makeConfig())
	if err != nil {
		return nil, fmt.Errorf("run %d: failed to init SDK: %w", run, err)
	}

	var (
		mu     sync.Mutex
		orders []*types.Order
		done   = make(chan struct{})
	)

	placeAndCollect := func(sdkCtx *types.Context, req *types.OrderRequest) {
		order, err := sdkCtx.PlaceOrder(req)
		if err != nil || order == nil {
			return
		}
		if order.Status == types.OrderStatusFilled {
			mu.Lock()
			orders = append(orders, order)
			mu.Unlock()
		}
	}

	botSDK.SetOnCandle(func(sdkCtx *types.Context, candle *types.Candle) {
		rsi, err := botSDK.IndicatorManager().RSI(exchange, symbol, rsiPeriod, "")
		if err != nil {
			return
		}
		currentRSI := rsi[len(rsi)-1]

		if currentRSI < 30 {
			acc, err := sdkCtx.Trader.GetAccount(sdkCtx.Ctx, candle.Exchange, quoteAsset)
			if err != nil {
				return
			}
			var usdtFree float64
			for _, b := range acc.Balances {
				if b.Asset == quoteAsset {
					usdtFree = b.Free
					break
				}
			}
			if usdtFree >= btcQty*candle.Close {
				placeAndCollect(sdkCtx, &types.OrderRequest{
					Symbol:   symbol,
					Exchange: exchange,
					Side:     types.OrderSideBuy,
					Type:     types.OrderTypeMarket,
					Quantity: btcQty,
				})
			}
		}

		if currentRSI > 70 {
			acc, err := sdkCtx.Trader.GetAccount(sdkCtx.Ctx, candle.Exchange, baseAsset)
			if err != nil {
				return
			}
			var btcFree float64
			for _, b := range acc.Balances {
				if b.Asset == baseAsset {
					btcFree = b.Free
					break
				}
			}
			if btcFree >= btcQty {
				placeAndCollect(sdkCtx, &types.OrderRequest{
					Symbol:   symbol,
					Exchange: exchange,
					Side:     types.OrderSideSell,
					Type:     types.OrderTypeMarket,
					Quantity: btcQty,
				})
			}
		}
	})

	botSDK.SetOnComplete(func() {
		close(done)
	})

	if err := botSDK.Start(ctx); err != nil {
		return nil, fmt.Errorf("run %d: SDK error: %w", run, err)
	}

	<-done
	return orders, nil
}

// ordersEqual compares two orders on every field that must be deterministic across runs.
// Excludes ID (engine-assigned per session, differs between sessions by design).
func ordersEqual(a, b *types.Order) bool {
	return a.Side == b.Side &&
		a.Type == b.Type &&
		a.Status == b.Status &&
		a.Quantity == b.Quantity &&
		a.Price == b.Price &&
		a.FilledQty == b.FilledQty &&
		a.AveragePrice == b.AveragePrice &&
		a.CreatedAt.Equal(b.CreatedAt) &&
		a.UpdatedAt.Equal(b.UpdatedAt)
}

// checkDeterminism compares all runs and reports any divergence.
// Returns true if all runs produced identical order sequences.
func checkDeterminism(allRuns [][]*types.Order) bool {
	reference := allRuns[0]
	passed := true

	for i := 1; i < len(allRuns); i++ {
		run := allRuns[i]
		if len(run) != len(reference) {
			log.Printf("FAIL: run %d has %d orders, run 1 has %d orders", i+1, len(run), len(reference))
			passed = false
			continue
		}
		for pos, refOrder := range reference {
			if !ordersEqual(refOrder, run[pos]) {
				log.Printf("FAIL: run %d differs at position %d", i+1, pos)
				log.Printf("  ref  : side=%s type=%s price=%f qty=%f status=%s createdAt=%s updatedAt=%s",
					refOrder.Side, refOrder.Type, refOrder.Price, refOrder.Quantity, refOrder.Status,
					refOrder.CreatedAt.Format(time.RFC3339), refOrder.UpdatedAt.Format(time.RFC3339))
				log.Printf("  run%d : side=%s type=%s price=%f qty=%f status=%s createdAt=%s updatedAt=%s",
					i+1, run[pos].Side, run[pos].Type, run[pos].Price, run[pos].Quantity, run[pos].Status,
					run[pos].CreatedAt.Format(time.RFC3339), run[pos].UpdatedAt.Format(time.RFC3339))
				passed = false
			}
		}
	}
	return passed
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful interrupt
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("Interrupted. Shutting down...")
		cancel()
		os.Exit(0)
	}()

	log.Printf("=== Determinism Test: running strategy %d times ===", totalRuns)

	allRuns := make([][]*types.Order, 0, totalRuns)

	for i := 1; i <= totalRuns; i++ {
		log.Printf("--- Run %d/%d ---", i, totalRuns)
		orders, err := runOnce(ctx, i)
		if err != nil {
			log.Fatalf("Run %d failed: %v", i, err)
		}
		log.Printf("Run %d finished: %d FILLED orders", i, len(orders))
		allRuns = append(allRuns, orders)
	}

	log.Println("\n=== Results ===")
	log.Printf("Reference run produced %d orders:", len(allRuns[0]))
	for pos, o := range allRuns[0] {
		log.Printf("  [%d] %s %s | qty=%f price=%f status=%s createdAt=%s",
			pos, o.Side, o.Type, o.Quantity, o.Price, o.Status, o.CreatedAt.Format(time.RFC3339))
	}

	log.Println("\nComparing all runs...")
	if checkDeterminism(allRuns) {
		log.Printf("PASS: all %d runs produced identical order sequences (%d orders each)", totalRuns, len(allRuns[0]))
	} else {
		log.Printf("FAIL: strategy is not deterministic across %d runs", totalRuns)
		os.Exit(1)
	}
}
