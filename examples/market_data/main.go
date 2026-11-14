// market_data — funding, positioning and trade flow in a backtest.
//
// At every daily close it prints, per coin, the 7-day funding sum (the funding
// carry's signal), the open interest and long/short ratio the daily candle
// carries, and the day's taker delta. Run it twice over the same window: the
// output is identical, and nothing in it is later than the candle it is
// printed on.
//
// Credentials: KDRAIGO_KEY_ID and KDRAIGO_PRIVATE_KEY (hex Ed25519), from
// kdraigo.com/account/api-keys. KDRAIGO_ENDPOINT overrides the engine URL.
//
// Run:  go run ./examples/market_data
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	sdk "github.com/kdraigo/dev_sdk"
	"github.com/kdraigo/dev_sdk/types"
)

const exchange = "binance_futures"

var coins = []string{"BTC/USDT", "ETH/USDT", "TRX/USDT"}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	cfg := &types.Config{
		Environment: types.EnvBacktest,
		Timeframes:  []types.Timeframe{types.Timeframe1d},
		MarketData:  []types.MarketData{types.MarketDataPositioning, types.MarketDataFlow},
		Credentials: types.Credentials{
			KeyID:      envOr("KDRAIGO_KEY_ID", "your-key-id"),
			PrivateKey: envOr("KDRAIGO_PRIVATE_KEY", "YOUR_PRIVATE_KEY_HEX"),
		},
		Backtest: &types.BacktestOptions{
			Endpoint:             envOr("KDRAIGO_ENDPOINT", "https://api.kdraigo.com"),
			SessionName:          "market_data example",
			RequestedExchanges:   []string{exchange},
			Assets:               coins,
			Wallets:              map[string]float64{"USDT": 1000},
			MarketTypeByExchange: map[string]string{exchange: "linear_perp"},
			StartTime:            time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC),
			EndTime:              time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC),
		},
	}

	bot, err := sdk.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	bot.SetOnCandleFor(types.Timeframe1d, func(ctx *types.Context, c *types.Candle) {
		now := bot.Now()
		rates, err := bot.GetFundingRates(ctx.Ctx, c.Exchange, c.Symbol, now.Add(-7*24*time.Hour), now)
		if err != nil {
			log.Printf("%s funding: %v", c.Symbol, err)
			return
		}
		sum := 0.0
		for _, r := range rates {
			sum += r.Rate
		}

		line := fmt.Sprintf("%s %-9s funding 7d %+7.2f bp (%d settlements)", c.CloseTime.Add(time.Millisecond).Format("2006-01-02"), c.Symbol, sum*1e4, len(rates))
		if p := c.Positioning; p != nil {
			line += fmt.Sprintf(" | OI %.0f, long/short %.2f (as of %s)", p.OpenInterest, p.LongShortRatio, p.Time.Format("15:04"))
		}
		if f := c.Flow; f != nil {
			line += fmt.Sprintf(" | delta %+.1f, %d prints >= 100k", f.Delta, f.LargePrints100k)
		}
		fmt.Println(line)
	})

	if err := bot.Start(context.Background()); err != nil {
		log.Fatal(err)
	}
}
