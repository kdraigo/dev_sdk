package live

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/hirokisan/bybit/v2"
	"github.com/kdraigo/dev_sdk/aggregator"
	"github.com/kdraigo/dev_sdk/types"
)

type BybitClient struct {
	config *types.Config
	client *bybit.Client

	// filters is keyed by uppercase venue symbol, e.g. "BTCUSDT".
	filters map[string]InstrumentFilter
}

func NewBybitClient(cfg *types.Config) *BybitClient {
	return &BybitClient{config: cfg, filters: make(map[string]InstrumentFilter)}
}

func (b *BybitClient) PrepareSession(ctx context.Context, cfg *types.Config) error {
	log.Println("Bybit: Validating API Keys...")

	if cfg.Environment == types.EnvTestBybit {
		b.client = bybit.NewTestClient().WithAuth(cfg.Credentials.APIKey, cfg.Credentials.APISecret)
	} else {
		b.client = bybit.NewClient().WithAuth(cfg.Credentials.APIKey, cfg.Credentials.APISecret)
	}

	if err := b.fetchSymbolInfo(cfg); err != nil {
		// Non-fatal — log and continue; orders will use raw values.
		log.Printf("Bybit: failed to fetch instrument info: %v", err)
	}

	return nil
}

func (b *BybitClient) fetchSymbolInfo(cfg *types.Config) error {
	assets := []string{}
	if cfg.Live != nil {
		assets = cfg.Live.Assets
	}

	for _, asset := range assets {
		sym := strings.ToUpper(strings.ReplaceAll(asset, "/", ""))
		symV5 := bybit.SymbolV5(sym)

		resp, err := b.client.V5().Market().GetInstrumentsInfo(bybit.V5GetInstrumentsInfoParam{
			Category: bybit.CategoryV5Spot,
			Symbol:   &symV5,
		})
		if err != nil {
			log.Printf("Bybit: instrument info fetch failed for %s: %v", sym, err)
			continue
		}
		if resp.Result.Spot == nil || len(resp.Result.Spot.List) == 0 {
			log.Printf("Bybit: no spot instrument info for %s", sym)
			continue
		}

		item := resp.Result.Spot.List[0]
		tickSize, _ := strconv.ParseFloat(item.PriceFilter.TickSize, 64)
		qtyStep, _ := strconv.ParseFloat(item.LotSizeFilter.BasePrecision, 64)
		minQty, _ := strconv.ParseFloat(item.LotSizeFilter.MinOrderQty, 64)
		minNotional, _ := strconv.ParseFloat(item.LotSizeFilter.MinOrderAmt, 64)

		b.filters[sym] = InstrumentFilter{
			TickSize:    tickSize,
			StepSize:    qtyStep,
			MinQty:      minQty,
			MinNotional: minNotional,
		}
		log.Printf("Bybit: %s tick=%v step=%v minQty=%v minNotional=%v", sym, tickSize, qtyStep, minQty, minNotional)
	}

	return nil
}

func (b *BybitClient) ConnectStream(ctx context.Context, candleChan chan<- *types.Candle, orderChan chan<- *types.Order) error {
	log.Println("Bybit: Connecting to WebSocket streams...")

	// ── Public kline stream (Spot) ───────────────────────────────────────────
	pubWsClient := bybit.NewWebsocketClient()
	if b.config.Environment == types.EnvTestBybit {
		pubWsClient = pubWsClient.WithBaseURL(bybit.TestWebsocketBaseURL)
	}

	pubSrv, err := pubWsClient.V5().Public(bybit.CategoryV5Spot)
	if err != nil {
		return fmt.Errorf("failed to initialize bybit public ws: %w", err)
	}

	interval := bybit.Interval("1")

	assets := []string{}
	if b.config.Live != nil {
		assets = b.config.Live.Assets
	}

	for _, asset := range assets {
		sym := bybit.SymbolV5(strings.ToUpper(strings.ReplaceAll(asset, "/", "")))
		origSym := asset

		if _, err := pubSrv.SubscribeKline(bybit.V5WebsocketPublicKlineParamKey{
			Interval: interval,
			Symbol:   sym,
		}, func(response bybit.V5WebsocketPublicKlineResponse) error {
			for _, k := range response.Data {
				// Drop in-progress klines. Bybit pushes ~one update per second
				// while a candle is forming and sets Confirm=true only on the
				// final tick. Strategies expect one OnCandle per close.
				if !k.Confirm {
					continue
				}
				open, _ := strconv.ParseFloat(k.Open, 64)
				high, _ := strconv.ParseFloat(k.High, 64)
				low, _ := strconv.ParseFloat(k.Low, 64)
				closeVal, _ := strconv.ParseFloat(k.Close, 64)
				volume, _ := strconv.ParseFloat(k.Volume, 64)
				// Bybit kline exposes turnover (quote volume) but not trade count
				// or taker-buy splits, so those stay 0 (documented gap).
				quoteVolume, _ := strconv.ParseFloat(k.Turnover, 64)

				candleChan <- &types.Candle{
					Symbol:      origSym,
					Exchange:    "bybit",
					Timeframe:   types.Timeframe1m,
					OpenTime:    time.UnixMilli(k.Start),
					CloseTime:   time.UnixMilli(k.End),
					Open:        open,
					High:        high,
					Low:         low,
					Close:       closeVal,
					Volume:      volume,
					IsComplete:  k.Confirm,
					QuoteVolume: quoteVolume,
				}
			}
			return nil
		}); err != nil {
			log.Printf("Bybit: failed to subscribe kline for %s: %v", sym, err)
		}
	}

	go func() {
		if err := pubSrv.Start(ctx, func(isClosed bool, err error) {
			log.Printf("Bybit public WS (closed=%v): %v", isClosed, err)
		}); err != nil {
			log.Printf("Bybit public WS exited: %v", err)
		}
	}()

	// ── Private order stream ─────────────────────────────────────────────────
	privWsClient := bybit.NewWebsocketClient().
		WithAuth(b.config.Credentials.APIKey, b.config.Credentials.APISecret)
	if b.config.Environment == types.EnvTestBybit {
		privWsClient = privWsClient.WithBaseURL(bybit.TestWebsocketBaseURL)
	}

	privSrv, err := privWsClient.V5().Private()
	if err != nil {
		log.Printf("Bybit: failed to initialize private ws: %v", err)
	} else {
		if _, err := privSrv.SubscribeOrder(func(response bybit.V5WebsocketPrivateOrderResponse) error {
			for _, d := range response.Data {
				orderChan <- mapBybitOrder(d)
			}
			return nil
		}); err != nil {
			log.Printf("Bybit: failed to subscribe order updates: %v", err)
		}

		go func() {
			if err := privSrv.Start(ctx, func(isClosed bool, err error) {
				log.Printf("Bybit private WS (closed=%v): %v", isClosed, err)
			}); err != nil {
				log.Printf("Bybit private WS exited: %v", err)
			}
		}()
	}

	<-ctx.Done()
	return nil
}

// mapBybitOrder converts a private WS order event to the SDK Order type.
func mapBybitOrder(d bybit.V5WebsocketPrivateOrderData) *types.Order {
	side := types.OrderSideBuy
	if d.Side == bybit.SideSell {
		side = types.OrderSideSell
	}

	orderType := types.OrderTypeMarket
	if d.OrderType == bybit.OrderTypeLimit {
		orderType = types.OrderTypeLimit
	}

	status := mapBybitStatus(d.OrderStatus)

	price, _ := strconv.ParseFloat(d.Price, 64)
	qty, _ := strconv.ParseFloat(d.Qty, 64)
	filledQty, _ := strconv.ParseFloat(d.CumExecQty, 64)
	avgPrice, _ := strconv.ParseFloat(d.AvgPrice, 64)
	fee, _ := strconv.ParseFloat(d.CumExecFee, 64)

	return &types.Order{
		ID:           d.OrderID,
		Symbol:       string(d.Symbol),
		Exchange:     "bybit",
		Side:         side,
		Type:         orderType,
		Status:       status,
		Price:        price,
		Quantity:     qty,
		FilledQty:    filledQty,
		AveragePrice: avgPrice,
		Fee:          fee,
		FeeAsset:     string("USDT"),
		CreatedAt:    parseMillis(d.CreatedTime),
		UpdatedAt:    parseMillis(d.UpdatedTime),
	}
}

func parseMillis(s string) time.Time {
	ms, _ := strconv.ParseInt(s, 10, 64)
	return time.UnixMilli(ms)
}

func mapBybitStatus(s bybit.OrderStatus) types.OrderStatus {
	switch s {
	case bybit.OrderStatusFilled:
		return types.OrderStatusFilled
	case bybit.OrderStatusPartiallyFilled:
		return types.OrderStatusPartiallyFilled
	case bybit.OrderStatusCancelled:
		return types.OrderStatusCanceled
	case bybit.OrderStatusRejected:
		return types.OrderStatusRejected
	default:
		return types.OrderStatusNew
	}
}

// bybitSpotOrderType translates the venue-neutral intent into Bybit's own
// vocabulary. Bybit has only Market and Limit as order *types*; a trigger is
// expressed by setting orderFilter=StopOrder and a triggerPrice alongside,
// which is why the trigger is handled by the caller rather than folded in here.
//
// IntentTakeProfitLimit becomes a plain Limit for the same reason as on
// Binance: the SDK's TAKE_PROFIT_LIMIT is a resting exit at Price, not a
// trigger-based order.
func bybitSpotOrderType(t IntentType) (bybit.OrderType, error) {
	switch t {
	case IntentMarket, IntentStopMarket:
		return bybit.OrderTypeMarket, nil
	case IntentLimit, IntentStopLimit, IntentTakeProfitLimit:
		return bybit.OrderTypeLimit, nil
	default:
		return "", fmt.Errorf("bybit spot: no order type for intent %s", t)
	}
}

func (b *BybitClient) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	intent, err := MapOrder(req, MarketSpot)
	if err != nil {
		return nil, fmt.Errorf("bybit spot: %w", err)
	}
	sym := intent.Symbol

	filter := b.filters[sym]
	if err := filter.Apply(intent, 0); err != nil {
		return nil, fmt.Errorf("bybit spot %s: %w", sym, err)
	}

	orderType, err := bybitSpotOrderType(intent.Type)
	if err != nil {
		return nil, err
	}

	side := bybit.SideBuy
	if intent.Side == types.OrderSideSell {
		side = bybit.SideSell
	}

	param := bybit.V5CreateOrderParam{
		Category:  bybit.CategoryV5Spot,
		Symbol:    bybit.SymbolV5(sym),
		Side:      side,
		OrderType: orderType,
		Qty:       filter.FormatQty(intent.Quantity),
	}

	if intent.Price > 0 {
		priceStr := filter.FormatPrice(intent.Price)
		param.Price = &priceStr
	}

	if intent.Type.Triggered() {
		// A conditional order on spot is a StopOrder with a trigger price.
		// Without orderFilter Bybit books it immediately, which is precisely
		// the behaviour being fixed.
		trigger := filter.FormatPrice(intent.StopPrice)
		orderFilter := bybit.OrderFilterStopOrder
		triggerBy := bybit.TriggerByLastPrice
		// A SELL stop guards a long and triggers on the way down; a BUY stop
		// guards a short and triggers on the way up. Bybit will not infer this.
		direction := bybit.TriggerDirectionFall
		if intent.Side == types.OrderSideBuy {
			direction = bybit.TriggerDirectionRise
		}
		param.TriggerPrice = &trigger
		param.OrderFilter = &orderFilter
		param.TriggerBy = &triggerBy
		param.TriggerDirection = &direction
	}

	res, err := b.client.V5().Order().CreateOrder(param)
	if err != nil {
		return nil, err
	}

	return &types.Order{
		ID:        res.Result.OrderID,
		Symbol:    req.Symbol,
		Exchange:  "bybit",
		Side:      req.Side,
		Type:      req.Type,
		Status:    types.OrderStatusNew,
		Price:     req.Price,
		Quantity:  req.Quantity,
		StopPrice: intent.StopPrice,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}, nil
}

func (b *BybitClient) CancelOrder(ctx context.Context, exchange, symbol, id string) error {
	sym := strings.ReplaceAll(symbol, "/", "")
	_, err := b.client.V5().Order().CancelOrder(bybit.V5CancelOrderParam{
		Category: bybit.CategoryV5Spot,
		Symbol:   bybit.SymbolV5(sym),
		OrderID:  &id,
	})
	return err
}

func (b *BybitClient) GetAccount(ctx context.Context, exchange string, asset string) (*types.Account, error) {
	res, err := b.client.V5().Account().GetWalletBalance(bybit.AccountTypeV5UNIFIED, nil)
	if err != nil {
		return nil, err
	}

	acc := &types.Account{Exchange: "bybit"}
	for _, rawBal := range res.Result.List {
		for _, coin := range rawBal.Coin {
			if asset == "" || string(coin.Coin) == asset {
				valStr := coin.Equity
				if valStr == "" || valStr == "0" {
					valStr = coin.WalletBalance
				}
				if valStr == "" || valStr == "0" {
					valStr = coin.AvailableToWithdraw
				}
				free, _ := strconv.ParseFloat(valStr, 64)
				locked, _ := strconv.ParseFloat(coin.Locked, 64)
				acc.Balances = append(acc.Balances, types.Balance{
					Asset: string(coin.Coin),
					Free:  free,
					Lock:  locked,
				})
			}
		}
	}

	return acc, nil
}

func (b *BybitClient) Next(ctx context.Context) error {
	return nil
}

// bybitInterval maps SDK timeframes to the Bybit V5 kline interval enum.
func bybitInterval(tf types.Timeframe) (bybit.Interval, error) {
	switch tf {
	case types.Timeframe1m:
		return bybit.Interval1, nil
	case types.Timeframe3m:
		return bybit.Interval3, nil
	case types.Timeframe5m:
		return bybit.Interval5, nil
	case types.Timeframe15m:
		return bybit.Interval15, nil
	case types.Timeframe30m:
		return bybit.Interval30, nil
	case types.Timeframe1h:
		return bybit.Interval60, nil
	case types.Timeframe2h:
		return bybit.Interval120, nil
	case types.Timeframe4h:
		return bybit.Interval240, nil
	case types.Timeframe1d:
		return bybit.IntervalD, nil
	default:
		return "", fmt.Errorf("bybit: unsupported timeframe %q", tf)
	}
}

// GetHistoricalCandles pages backwards through Bybit's V5 spot kline REST,
// returning closed candles in [from, to] sorted oldest-first. The client
// requires no authentication for this endpoint, but b.client is reused
// for connection pooling.
func (b *BybitClient) GetHistoricalCandles(ctx context.Context, exchange, symbol string, from, to time.Time, tf types.Timeframe) ([]*types.Candle, error) {
	if b.client == nil {
		// Allow callers to fetch history before PrepareSession (e.g. for
		// quick scripts). Build a default unauthenticated client here.
		if b.config != nil && b.config.Environment == types.EnvTestBybit {
			b.client = bybit.NewTestClient().Client
		} else {
			b.client = bybit.NewClient()
		}
	}
	if from.After(to) {
		return nil, fmt.Errorf("bybit: from %s is after to %s", from, to)
	}
	interval, err := bybitInterval(tf)
	if err != nil {
		return nil, err
	}

	sym := bybit.SymbolV5(strings.ToUpper(strings.ReplaceAll(symbol, "/", "")))
	tfDur := aggregator.ExtractDuration(tf)
	durationMs := tfDur.Milliseconds()
	fromMs := from.UnixMilli()
	toMs := to.UnixMilli()

	const pageLimit = 200
	var all []*types.Candle
	end := toMs

	for end >= fromMs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		startParam := fromMs
		endParam := end
		limit := pageLimit
		resp, err := b.client.V5().Market().GetKline(bybit.V5GetKlineParam{
			Category: bybit.CategoryV5Spot,
			Symbol:   sym,
			Interval: interval,
			Start:    &startParam,
			End:      &endParam,
			Limit:    &limit,
		})
		if err != nil {
			return nil, fmt.Errorf("bybit kline fetch: %w", err)
		}
		if len(resp.Result.List) == 0 {
			break
		}

		// Bybit returns klines newest-first. Convert and reverse so the page
		// is oldest-first, then prepend the page to the accumulator.
		page := make([]*types.Candle, 0, len(resp.Result.List))
		oldestStartMs := end
		for _, k := range resp.Result.List {
			startMs, err := strconv.ParseInt(k.StartTime, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("bybit kline parse start time %q: %w", k.StartTime, err)
			}
			open, _ := strconv.ParseFloat(k.Open, 64)
			high, _ := strconv.ParseFloat(k.High, 64)
			low, _ := strconv.ParseFloat(k.Low, 64)
			closeVal, _ := strconv.ParseFloat(k.Close, 64)
			volume, _ := strconv.ParseFloat(k.Volume, 64)
			quoteVolume, _ := strconv.ParseFloat(k.Turnover, 64)

			page = append(page, &types.Candle{
				Symbol:      symbol,
				Exchange:    "bybit",
				Timeframe:   tf,
				OpenTime:    time.UnixMilli(startMs),
				CloseTime:   time.UnixMilli(startMs + durationMs),
				Open:        open,
				High:        high,
				Low:         low,
				Close:       closeVal,
				Volume:      volume,
				IsComplete:  true,
				QuoteVolume: quoteVolume,
			})
			if startMs < oldestStartMs {
				oldestStartMs = startMs
			}
		}
		// Reverse page (newest-first → oldest-first) and prepend to result.
		for i, j := 0, len(page)-1; i < j; i, j = i+1, j-1 {
			page[i], page[j] = page[j], page[i]
		}
		all = append(page, all...)

		if len(resp.Result.List) < pageLimit {
			break
		}
		// Page back: next end is one millisecond before the oldest candle.
		if oldestStartMs <= fromMs {
			break
		}
		end = oldestStartMs - 1
	}

	return all, nil
}

// OrderFeed: Bybit pushes order updates over its private v5 stream.
func (b *BybitClient) OrderFeed() types.OrderFeed {
	return types.OrderFeed{Push: true, Latency: time.Second}
}
