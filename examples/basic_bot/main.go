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
	totalRuns  = 1
	rsiPeriod  = 14
	btcQty     = 0.1
	exchange   = "binance"
	symbol     = "BTC/USDT"
	quoteAsset = "USDT"
	baseAsset  = "BTC"

	// Place limit orders 5 % away from market so they are very unlikely to fill
	// within a single 15m candle. This lets us exercise the cancel path cleanly.
	limitOffsetBuy  = 0.95 // limit BUY  at price * 0.95
	limitOffsetSell = 1.05 // limit SELL at price * 1.05
)

// Fixed time window — computed once at startup so all runs use identical candle data.
var (
	backtestEnd   = time.Now().UTC().Truncate(time.Hour)
	backtestStart = time.Date(2025, 6, 6, 0, 0, 0, 0, time.UTC)
)

// -----------------------------------------------------------------------------
// Error collector
// -----------------------------------------------------------------------------

type sessionError struct {
	At      string // candle timestamp
	Action  string // e.g. "PlaceOrder-LimitBuy"
	OrderID string // empty when not order-related
	Err     error
}

type errorCollector struct {
	mu   sync.Mutex
	errs []sessionError
}

func (ec *errorCollector) Add(at, action, orderID string, err error) {
	if err == nil {
		return
	}
	// "not enough data" is expected during the RSI warm-up period at the start of
	// the backtest (RSI needs N closed candles before producing a value). Suppress
	// it so the error log only shows genuine operational failures.
	if err.Error() == "not enough data for indicator calculation" {
		return
	}
	ec.mu.Lock()
	ec.errs = append(ec.errs, sessionError{At: at, Action: action, OrderID: orderID, Err: err})
	ec.mu.Unlock()
	log.Printf("[ERR] %s | %s order=%s → %v", at, action, orderID, err)
}

func (ec *errorCollector) Count() int {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	return len(ec.errs)
}

func (ec *errorCollector) Summary() {
	ec.mu.Lock()
	defer ec.mu.Unlock()
	if len(ec.errs) == 0 {
		log.Println("[ERRORS] None — session completed without errors")
		return
	}
	log.Printf("[ERRORS] %d error(s) during session:", len(ec.errs))
	for i, e := range ec.errs {
		log.Printf("  [%d] %s | %s order=%q → %v", i+1, e.At, e.Action, e.OrderID, e.Err)
	}
}

// -----------------------------------------------------------------------------
// Config
// -----------------------------------------------------------------------------

func makeConfig() *types.Config {
	return &types.Config{
		Environment: types.EnvBacktest,
		Timeframes:  []types.Timeframe{types.Timeframe15m, types.Timeframe1h},
		Credentials: types.Credentials{
			KeyID:      "7b89ece9-97ae-4b76-b938-ce9e5345bfce",
			PrivateKey: "385d5c080a1b4140a5ed9ee76d0ef3fcd291cabab4ec6759bc178ad3a8ed837148309e3cb2a3a014c93d68b4f20a0ba5978ab300531c844dcec672925eb8d63a",
		},
		Backtest: &types.BacktestOptions{
			Endpoint:           "http://localhost:4000",
			SessionName:        "Multi-TF-Test",
			RequestedExchanges: []string{exchange},
			Assets:             []string{symbol},
			Wallets:            map[string]float64{quoteAsset: 100000.0},
			StartTime:          backtestStart,
			EndTime:            backtestEnd,
		},
	}
}

// -----------------------------------------------------------------------------
// Run result
// -----------------------------------------------------------------------------

type runResult struct {
	limitBuysPlaced    int
	limitBuysCanceled  int
	limitBuysFilled    int // filled before we could cancel (unexpected)
	limitSellsPlaced   int
	limitSellsCanceled int
	limitSellsFilled   int
	errors             int
}

// -----------------------------------------------------------------------------
// pendingOrder tracks an open limit order
// -----------------------------------------------------------------------------

type pendingOrder struct {
	id    string
	side  types.OrderSide
	price float64
}

// -----------------------------------------------------------------------------
// runOnce
//
// Strategy:
//   1h  — trend bias via RSI (> 50 → bullish)
//   15m — one open limit order at a time:
//           • if a pending limit order exists → cancel it this candle
//           • else RSI < 30 + bullish  → LIMIT BUY  at price * 0.95
//                RSI > 70 + bearish → LIMIT SELL at price * 1.05
//
//   OnOrderUpdate — clears pendingOrder when a limit fills unexpectedly,
//                   so the cancel path is not attempted on a filled order.
// -----------------------------------------------------------------------------

func runOnce(ctx context.Context, run int) (runResult, error) {
	botSDK, err := sdk.New(makeConfig())
	if err != nil {
		return runResult{}, fmt.Errorf("run %d: failed to init SDK: %w", run, err)
	}

	var (
		mu      sync.Mutex
		result  runResult
		ec      errorCollector
		bullish bool
		pending *pendingOrder // non-nil while a limit order is open
		done    = make(chan struct{})
	)

	// OnOrderUpdate fires when the engine fills (or cancels) any order.
	// We use it to detect unexpected fills so we never try to cancel a filled order.
	botSDK.SetOnOrderUpdate(func(_ *types.Context, order *types.Order) {
		ts := order.UpdatedAt.Format("2006-01-02 15:04")
		switch order.Status {
		case types.OrderStatusFilled:
			mu.Lock()
			if pending != nil && pending.id == order.ID {
				// Order filled before we could cancel — clear the pending slot.
				log.Printf("[OUP] %s | FILLED (unexpected) id=%s side=%s price=%.2f",
					ts, order.ID, order.Side, order.AveragePrice)
				if pending.side == types.OrderSideBuy {
					result.limitBuysFilled++
				} else {
					result.limitSellsFilled++
				}
				pending = nil
			}
			mu.Unlock()
		case types.OrderStatusCanceled:
			mu.Lock()
			log.Printf("[OUP] %s | CANCELED confirmed id=%s", ts, order.ID)
			mu.Unlock()
		}
	})

	// 1h handler: update trend bias.
	botSDK.SetOnCandleFor(types.Timeframe1h, func(sdkCtx *types.Context, candle *types.Candle) {
		ts := candle.OpenTime.Format("2006-01-02 15:04")
		rsiValues, err := botSDK.IndicatorManagerFor(types.Timeframe1h).RSI(exchange, symbol, "close", rsiPeriod)
		if err != nil {
			ec.Add(ts, "RSI-1h", "", err)
			return
		}
		if len(rsiValues) == 0 {
			return
		}
		rsi1h := rsiValues[len(rsiValues)-1]
		mu.Lock()
		bullish = rsi1h > 50
		mu.Unlock()
		log.Printf("[1h] %s RSI=%.2f trend=%s", ts, rsi1h,
			map[bool]string{true: "BULLISH", false: "BEARISH"}[bullish])
	})

	// 15m handler: cancel pending limit order, then fire new signal.
	botSDK.SetOnCandleFor(types.Timeframe15m, func(sdkCtx *types.Context, candle *types.Candle) {
		ts := candle.OpenTime.Format("2006-01-02 15:04")

		rsiValues, err := botSDK.IndicatorManagerFor(types.Timeframe15m).RSI(exchange, symbol, "close", rsiPeriod)
		if err != nil {
			ec.Add(ts, "RSI-15m", "", err)
			return
		}
		if len(rsiValues) == 0 {
			return
		}
		rsi15m := rsiValues[len(rsiValues)-1]

		mu.Lock()
		isBullish := bullish
		pend := pending
		mu.Unlock()

		// --- Cancel the open limit order from the previous candle ---
		if pend != nil {
			cancelErr := sdkCtx.CancelOrder(exchange, symbol, pend.id)
			ec.Add(ts, "CancelOrder", pend.id, cancelErr)
			mu.Lock()
			if cancelErr == nil {
				if pend.side == types.OrderSideBuy {
					result.limitBuysCanceled++
				} else {
					result.limitSellsCanceled++
				}
				log.Printf("[15m] %s CANCEL OK  id=%s side=%s", ts, pend.id, pend.side)
				pending = nil
			} else {
				// Could be already filled (race) or an engine error — either way log it.
				log.Printf("[15m] %s CANCEL FAIL id=%s side=%s err=%v", ts, pend.id, pend.side, cancelErr)
				// If the order no longer exists in the engine, clear pending to avoid retrying.
				pending = nil
			}
			mu.Unlock()
			return
		}

		// --- BUY signal: LIMIT BUY 5 % below market ---
		if rsi15m < 30 && isBullish {
			acc, err := sdkCtx.Trader.GetAccount(sdkCtx.Ctx, candle.Exchange, quoteAsset)
			if err != nil {
				ec.Add(ts, "GetAccount", "", err)
				return
			}
			var usdtFree float64
			for _, b := range acc.Balances {
				if b.Asset == quoteAsset {
					usdtFree = b.Free
					break
				}
			}
			limitPrice := candle.Close * limitOffsetBuy
			if usdtFree >= btcQty*limitPrice {
				order, err := sdkCtx.PlaceOrder(&types.OrderRequest{
					Symbol:   symbol,
					Exchange: exchange,
					Side:     types.OrderSideBuy,
					Type:     types.OrderTypeLimit,
					Price:    limitPrice,
					Quantity: btcQty,
				})
				ec.Add(ts, "PlaceOrder-LimitBuy", "", err)
				if err != nil || order == nil {
					return
				}
				mu.Lock()
				result.limitBuysPlaced++
				pending = &pendingOrder{id: order.ID, side: types.OrderSideBuy, price: limitPrice}
				mu.Unlock()
				log.Printf("[15m] %s LIMIT BUY placed id=%s price=%.2f (market=%.2f -5%%) RSI=%.2f",
					ts, order.ID, limitPrice, candle.Close, rsi15m)
			}
			return
		}

		// --- SELL signal: LIMIT SELL 5 % above market ---
		if rsi15m > 70 && !isBullish {
			acc, err := sdkCtx.Trader.GetAccount(sdkCtx.Ctx, candle.Exchange, baseAsset)
			if err != nil {
				ec.Add(ts, "GetAccount", "", err)
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
				limitPrice := candle.Close * limitOffsetSell
				order, err := sdkCtx.PlaceOrder(&types.OrderRequest{
					Symbol:   symbol,
					Exchange: exchange,
					Side:     types.OrderSideSell,
					Type:     types.OrderTypeLimit,
					Price:    limitPrice,
					Quantity: btcQty,
				})
				ec.Add(ts, "PlaceOrder-LimitSell", "", err)
				if err != nil || order == nil {
					return
				}
				mu.Lock()
				result.limitSellsPlaced++
				pending = &pendingOrder{id: order.ID, side: types.OrderSideSell, price: limitPrice}
				mu.Unlock()
				log.Printf("[15m] %s LIMIT SELL placed id=%s price=%.2f (market=%.2f +5%%) RSI=%.2f",
					ts, order.ID, limitPrice, candle.Close, rsi15m)
			}
		}
	})

	botSDK.SetOnComplete(func() {
		close(done)
	})

	if err := botSDK.Start(ctx); err != nil {
		return runResult{}, fmt.Errorf("run %d: SDK error: %w", run, err)
	}

	<-done

	log.Printf("--- Run %d error summary ---", run)
	ec.Summary()

	mu.Lock()
	result.errors = ec.Count()
	mu.Unlock()

	return result, nil
}

// -----------------------------------------------------------------------------
// Determinism check
// -----------------------------------------------------------------------------

func checkDeterminism(allRuns []runResult) bool {
	ref := allRuns[0]
	passed := true
	for i := 1; i < len(allRuns); i++ {
		r := allRuns[i]
		check := func(name string, a, b int) {
			if a != b {
				log.Printf("FAIL run %d: %s=%d vs ref=%d", i+1, name, a, b)
				passed = false
			}
		}
		check("limitBuysPlaced", r.limitBuysPlaced, ref.limitBuysPlaced)
		check("limitBuysCanceled", r.limitBuysCanceled, ref.limitBuysCanceled)
		check("limitBuysFilled", r.limitBuysFilled, ref.limitBuysFilled)
		check("limitSellsPlaced", r.limitSellsPlaced, ref.limitSellsPlaced)
		check("limitSellsCanceled", r.limitSellsCanceled, ref.limitSellsCanceled)
		check("limitSellsFilled", r.limitSellsFilled, ref.limitSellsFilled)
		check("errors", r.errors, ref.errors)
	}
	return passed
}

// -----------------------------------------------------------------------------
// main
// -----------------------------------------------------------------------------

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("Interrupted. Shutting down...")
		cancel()
		os.Exit(0)
	}()

	log.Printf("=== Multi-TF Limit Order + CancelOrder Test: %d run(s) ===", totalRuns)
	log.Printf("Strategy: limit BUY/SELL at ±5%% market, canceled on the next 15m candle")
	log.Printf("Window: %s → %s", backtestStart.Format(time.RFC3339), backtestEnd.Format(time.RFC3339))

	allRuns := make([]runResult, 0, totalRuns)

	for i := 1; i <= totalRuns; i++ {
		log.Printf("\n=== Run %d/%d ===", i, totalRuns)
		result, err := runOnce(ctx, i)
		if err != nil {
			log.Fatalf("Run %d failed: %v", i, err)
		}
		log.Printf("Run %d finished:", i)
		log.Printf("  Limit BUYs  — placed=%d canceled=%d filled(early)=%d",
			result.limitBuysPlaced, result.limitBuysCanceled, result.limitBuysFilled)
		log.Printf("  Limit SELLs — placed=%d canceled=%d filled(early)=%d",
			result.limitSellsPlaced, result.limitSellsCanceled, result.limitSellsFilled)
		log.Printf("  Errors: %d", result.errors)
		allRuns = append(allRuns, result)
	}

	log.Println("\n=== Final Results ===")
	ref := allRuns[0]
	log.Printf("  Limit BUYs  — placed=%d canceled=%d filled(early)=%d",
		ref.limitBuysPlaced, ref.limitBuysCanceled, ref.limitBuysFilled)
	log.Printf("  Limit SELLs — placed=%d canceled=%d filled(early)=%d",
		ref.limitSellsPlaced, ref.limitSellsCanceled, ref.limitSellsFilled)
	log.Printf("  Errors: %d", ref.errors)

	cancelRate := 0.0
	total := ref.limitBuysPlaced + ref.limitSellsPlaced
	canceled := ref.limitBuysCanceled + ref.limitSellsCanceled
	if total > 0 {
		cancelRate = float64(canceled) / float64(total) * 100
	}
	log.Printf("  Cancel success rate: %.1f%% (%d/%d)", cancelRate, canceled, total)

	if totalRuns > 1 {
		log.Println("\nComparing all runs for determinism...")
		if checkDeterminism(allRuns) {
			log.Printf("PASS: all %d runs produced identical results", totalRuns)
		} else {
			log.Printf("FAIL: strategy is not deterministic across %d runs", totalRuns)
			os.Exit(1)
		}
	}
}
