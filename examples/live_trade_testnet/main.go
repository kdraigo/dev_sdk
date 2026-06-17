// live_trade_testnet verifies the Bybit Spot integration on Testnet.
//
// It reads credentials from credentials/bybit.json, prints the USDT spot
// balance, places a small market BUY, and logs the resulting order updates
// received via the private WebSocket stream.
//
// Run from repo root:
//
//	go run ./dev_sdk/examples/live_trade_testnet
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	sdk "github.com/kdraigo/dev_sdk"
	"github.com/kdraigo/dev_sdk/types"
)

type bybitCreds struct {
	APIKey    string `json:"api_key"`
	APISecret string `json:"api_secret"`
}

func main() {
	// ── Load credentials ─────────────────────────────────────────────────────
	raw, err := os.ReadFile("credentials/bybit.json")
	if err != nil {
		log.Fatalf("credentials/bybit.json not found.\nCopy the example file and fill in your Bybit Testnet keys.")
	}
	var creds bybitCreds
	if err := json.Unmarshal(raw, &creds); err != nil {
		log.Fatalf("Failed to parse credentials: %v", err)
	}
	if creds.APIKey == "YOUR_BYBIT_TESTNET_API_KEY" {
		log.Fatal("Fill in your Bybit Testnet API key in credentials/bybit.json")
	}

	// ── Build config ─────────────────────────────────────────────────────────
	cfg := &types.Config{
		Environment: types.EnvTestBybit,
		Timeframes:  []types.Timeframe{types.Timeframe1m},
		Credentials: types.Credentials{
			APIKey:    creds.APIKey,
			APISecret: creds.APISecret,
		},
		Live: &types.LiveOptions{
			RequestedExchanges: []string{"bybit"},
			Assets:             []string{"BTCUSDT"},
		},
	}

	// ── Create SDK ───────────────────────────────────────────────────────────
	s, err := sdk.New(cfg)
	if err != nil {
		log.Fatalf("sdk.New: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── After WS connects, print balance + place one market BUY ─────────────
	// We wait 3 s for auth handshake to complete before trading.
	go func() {
		time.Sleep(3 * time.Second)

		acc, err := s.GetAccount(ctx, "bybit", "")
		if err != nil {
			log.Printf("[WARN] GetAccount failed: %v", err)
		} else {
			log.Printf("=== Spot wallet balances (%d coins) ===", len(acc.Balances))
			for _, b := range acc.Balances {
				if b.Free > 0 || b.Lock > 0 {
					log.Printf("  %-8s  free=%.6f  locked=%.6f", b.Asset, b.Free, b.Lock)
				}
			}
		}

		log.Println("Placing Spot Market SELL 0.01 BTCUSDT on Testnet...")
		order, err := s.PlaceOrder(ctx, &types.OrderRequest{
			Symbol:   "BTCUSDT",
			Exchange: "bybit",
			Side:     types.OrderSideSell,
			Type:     types.OrderTypeMarket,
			Quantity: 0.01,
		})
		if err != nil {
			log.Printf("[ERROR] PlaceOrder failed: %v", err)
			return
		}
		log.Printf("Order submitted: id=%s status=%s", order.ID, order.Status)
	}()

	// ── Order update callback (private WS) ───────────────────────────────────
	s.SetOnOrderUpdate(func(sdkCtx *types.Context, order *types.Order) {
		log.Printf("[ORDER WS] id=%s symbol=%s side=%s status=%s filled=%.6f",
			order.ID, order.Symbol, order.Side, order.Status, order.FilledQty)

		if order.Status == types.OrderStatusFilled {
			log.Println("✓ Order FILLED (via WS) — Bybit Spot integration verified.")
			time.AfterFunc(2*time.Second, stop)
		}
	})

	// ── Recovery: Poll REST if WS auth failed ────────────────────────────────
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// No easy way to get "last order" from SDK trader currently,
				// but we can check account or just wait.
				// For this test, we'll just wait and see if it fills.
			}
		}
	}()

	// ── Candle callback (public kline WS) ────────────────────────────────────
	s.SetOnCandle(func(sdkCtx *types.Context, candle *types.Candle) {
		if candle.IsComplete {
			log.Printf("[CANDLE] %s %s  O=%.2f H=%.2f L=%.2f C=%.2f V=%.4f",
				candle.Symbol, candle.Timeframe,
				candle.Open, candle.High, candle.Low, candle.Close, candle.Volume)
		}
	})

	log.Println("Starting Bybit Testnet Spot verification — Ctrl+C to stop.")
	if err := s.Start(ctx); err != nil {
		log.Printf("SDK exited: %v", err)
	}
}
