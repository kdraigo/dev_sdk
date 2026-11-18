// paper_basic trades BTCUSDT perpetuals on kdraigo's paper exchange: real
// Binance books and trades, simulated fills, no exchange account.
//
// On each closed 1m bar it compares the close with a 20-bar average read from
// history at start. Flat and above the average, it buys at market; when the
// entry fills, it places a take-profit and a stop as one OCO, which the paper
// exchange keeps working after the bot exits.
//
//	KDRAIGO_KEY_ID=... KDRAIGO_PRIVATE_KEY=... go run ./examples/paper_basic
//
// The key is a kdraigo API key (kdraigo.com, API keys). The first run opens a
// paper account and logs its id; KDRAIGO_PAPER_ACCOUNT=<id> carries on with
// it. KDRAIGO_PAPER_ENDPOINT=http://localhost:4004 and
// KDRAIGO_DATA_URL=http://localhost:8080/api/v1 point it at a local stack.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	sdk "github.com/kdraigo/dev_sdk"
	"github.com/kdraigo/dev_sdk/types"
)

const (
	exchange = types.ExchangeBinanceFutures
	symbol   = "BTCUSDT"
	qty      = 0.002
	period   = 20
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	maxBars, _ := strconv.Atoi(envOr("MAX_CANDLES", "10"))
	cfg := &types.Config{
		Environment: types.EnvPaperBinanceFutures,
		Timeframes:  []types.Timeframe{types.Timeframe1m},
		Credentials: types.Credentials{
			KeyID:      os.Getenv("KDRAIGO_KEY_ID"),
			PrivateKey: os.Getenv("KDRAIGO_PRIVATE_KEY"),
		},
		// A paper run reads Live like an exchange run: the same config trades
		// testnet or mainnet by changing Environment (and arming, for mainnet).
		Live: &types.LiveOptions{
			Assets:           []string{symbol},
			DataURL:          os.Getenv("KDRAIGO_DATA_URL"),
			StrategyName:     "paper_basic",
			Leverage:         map[string]float64{symbol: 2},
			MaxLeverage:      5,
			MaxOrderNotional: 1_000,
		},
		Paper: &types.PaperOptions{
			Endpoint:  os.Getenv("KDRAIGO_PAPER_ENDPOINT"),
			AccountID: os.Getenv("KDRAIGO_PAPER_ACCOUNT"),
			Name:      "paper_basic",
			Balances:  map[string]float64{"USDT": 1_000},
		},
	}

	bot, err := sdk.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var (
		mu     sync.Mutex
		closes []float64
		bars   int
		entry  string // the market entry waiting for its fill
	)
	bot.SetOnResume(func(c *types.Context, st *types.SessionState) {
		log.Printf("attached to account %s (cold start %v): %d positions, %d open orders",
			st.SessionID, st.ColdStart, len(st.Positions), len(st.OpenOrders))
	})

	bot.SetOnCandle(func(c *types.Context, k *types.Candle) {
		mu.Lock()
		defer mu.Unlock()
		if closes == nil {
			// Warm the average from history up to now.
			hist, err := bot.GetCandles(c.Ctx, exchange, symbol, period, types.Timeframe1m)
			if err != nil {
				log.Printf("history: %v", err)
			}
			for _, h := range hist {
				if h.OpenTime.Before(k.OpenTime) {
					closes = append(closes, h.Close)
				}
			}
		}
		closes = append(closes, k.Close)
		if len(closes) > period {
			closes = closes[len(closes)-period:]
		}
		sum := 0.0
		for _, v := range closes {
			sum += v
		}
		avg := sum / float64(len(closes))
		bars++
		log.Printf("%s close %.1f, %d-bar average %.1f", k.OpenTime.UTC().Format("15:04"), k.Close, len(closes), avg)

		positions, err := bot.GetPositions(c.Ctx, exchange)
		if err != nil {
			log.Printf("positions: %v", err)
			return
		}
		for _, p := range positions {
			log.Printf("holding %s %v @ %.1f, unrealized %.2f, liquidation %.1f", p.Side, p.Size, p.EntryPrice, p.UnrealizedPnL, p.LiquidationPrice)
		}
		if len(positions) == 0 && entry == "" && len(closes) == period && k.Close > avg {
			o, err := bot.PlaceOrder(c.Ctx, &types.OrderRequest{Symbol: symbol, Exchange: exchange, Side: types.OrderSideBuy,
				Type: types.OrderTypeMarket, Quantity: qty, Reason: map[string]any{"close": k.Close, "average": avg}})
			if err != nil {
				log.Printf("entry refused: %v", err)
			} else {
				entry = o.ID
				log.Printf("entry %s %s", o.ID, o.Status)
			}
		}
		if bars >= maxBars {
			log.Printf("done after %d bars; the account and its orders stay on the paper exchange", bars)
			cancel()
		}
	})

	bot.SetOnOrderUpdate(func(c *types.Context, o *types.Order) {
		log.Printf("order %s %s %s %s: %v of %v at %.1f, fee %.4f %s", o.ID, o.Type, o.Side, o.Status, o.FilledQty, o.Quantity,
			o.AveragePrice, o.CumulativeFee, o.FeeAsset)
		mu.Lock()
		defer mu.Unlock()
		if o.ID != entry || o.Status != types.OrderStatusFilled {
			return
		}
		entry = ""
		legs, err := bot.PlaceBracket(c.Ctx, &types.BracketRequest{Symbol: symbol, Exchange: exchange, Side: types.OrderSideSell,
			Quantity: o.FilledQty, TakeProfitPrice: o.AveragePrice * 1.004, StopPrice: o.AveragePrice * 0.996})
		if err != nil {
			log.Printf("bracket refused: %v", err)
			return
		}
		log.Printf("bracket: take-profit %s at %.1f, stop %s at %.1f", legs[0].ID, legs[0].Price, legs[1].ID, legs[1].StopPrice)
	})

	start := time.Now()
	if err := bot.Start(ctx); err != nil {
		log.Fatal(err)
	}
	log.Printf("stopped after %s", time.Since(start).Round(time.Second))
}
