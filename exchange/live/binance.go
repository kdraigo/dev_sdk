package live

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/adshao/go-binance/v2"
	"github.com/adshao/go-binance/v2/common"
	"github.com/kdraigo/dev_sdk/aggregator"
	"github.com/kdraigo/dev_sdk/types"
)

// isEd25519Secret reports whether a credential secret is an Ed25519 PKCS8 PEM
// rather than an HMAC shared secret.
//
// Shared by the spot and futures clients: both go-binance clients carry the
// same KeyType field, and duplicating the check would let the two drift so that
// a key working on one venue mysteriously failed on the other.
func isEd25519Secret(secret string) bool {
	return strings.Contains(secret, "BEGIN PRIVATE KEY")
}

// newBinanceClient builds a go-binance spot client, selecting the signing scheme
// from the credential shape. The API key string in APIKey is always used as-is
// for the X-MBX-APIKEY header.
func newBinanceClient(creds types.Credentials) *binance.Client {
	c := binance.NewClient(creds.APIKey, creds.APISecret)
	if isEd25519Secret(creds.APISecret) {
		c.KeyType = common.KeyTypeEd25519
	}
	return c
}

type BinanceClient struct {
	config *types.Config
	client *binance.Client

	// filters holds each configured instrument's own trading rules, read from
	// exchangeInfo at PrepareSession. Orders used to go out as
	// fmt.Sprintf("%f", …) — six decimals whatever the symbol's precision —
	// which the venue rejects for anything that does not happen to step in
	// millionths.
	filters map[string]InstrumentFilter
}

func NewBinanceClient(cfg *types.Config) *BinanceClient {
	return &BinanceClient{config: cfg, filters: make(map[string]InstrumentFilter)}
}

// filterFor returns the instrument's rules, or a zero filter that passes
// everything through. A missing filter is a degraded state, not a fatal one:
// the venue still validates, and refusing to trade because exchangeInfo was
// unreachable would be a worse failure than a possible rejection.
func (b *BinanceClient) filterFor(symbol string) InstrumentFilter {
	return b.filters[symbol]
}

func (b *BinanceClient) PrepareSession(ctx context.Context, cfg *types.Config) error {
	log.Println("Real Binance: Validating API Keys...")

	if cfg.Environment == types.EnvTestBinance {
		binance.UseTestnet = true
	}

	b.client = newBinanceClient(cfg.Credentials)

	// Sync time to prevent -1022 Signature Invalid errors
	_, err := b.client.NewSetServerTimeService().Do(ctx)
	if err != nil {
		log.Printf("Warning: failed to sync Binance server time: %v", err)
	}

	err = b.client.NewPingService().Do(ctx)
	if err != nil {
		return fmt.Errorf("binance connection failed: %w", err)
	}

	if err := b.loadFilters(ctx, cfg); err != nil {
		// Non-fatal: see filterFor. Log loudly so a run that is rounding
		// nothing is visible in the output rather than only in a rejection.
		log.Printf("Binance: instrument filters unavailable, orders will be sent unrounded: %v", err)
	}

	return nil
}

// loadFilters reads tick size, step size and the notional minimum for every
// configured symbol.
func (b *BinanceClient) loadFilters(ctx context.Context, cfg *types.Config) error {
	if cfg.Live == nil || len(cfg.Live.Assets) == 0 {
		return nil
	}

	info, err := b.client.NewExchangeInfoService().Do(ctx)
	if err != nil {
		return fmt.Errorf("exchangeInfo: %w", err)
	}

	want := make(map[string]struct{}, len(cfg.Live.Assets))
	for _, asset := range cfg.Live.Assets {
		want[VenueSymbol(asset)] = struct{}{}
	}

	for i := range info.Symbols {
		sym := info.Symbols[i]
		if _, ok := want[sym.Symbol]; !ok {
			continue
		}
		f := InstrumentFilter{}
		if lot := sym.LotSizeFilter(); lot != nil {
			f.StepSize = parseFloat(lot.StepSize)
			f.MinQty = parseFloat(lot.MinQuantity)
		}
		if price := sym.PriceFilter(); price != nil {
			f.TickSize = parseFloat(price.TickSize)
		}
		if notional := sym.NotionalFilter(); notional != nil {
			f.MinNotional = parseFloat(notional.MinNotional)
		}
		b.filters[sym.Symbol] = f
		log.Printf("Binance: %s tick=%v step=%v minQty=%v minNotional=%v",
			sym.Symbol, f.TickSize, f.StepSize, f.MinQty, f.MinNotional)
	}
	return nil
}

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func (b *BinanceClient) ConnectStream(ctx context.Context, candleChan chan<- *types.Candle, orderChan chan<- *types.Order) error {
	log.Println("Real Binance: Connecting to Binance WebSocket...")

	// Always subscribe to 1m as the base feed; the SDK aggregates up to requested timeframes.
	interval := "1m"

	assets := []string{}
	if b.config.Live != nil {
		assets = b.config.Live.Assets
	}

	for _, asset := range assets {
		// e.g. "BTC/USDT" -> "BTCUSDT"
		formatSym := strings.ToUpper(strings.ReplaceAll(asset, "/", ""))
		originalSym := asset

		go func(sym, origSym string) {
			doneC, stopC, err := binance.WsKlineServe(sym, interval, func(event *binance.WsKlineEvent) {
				// Drop in-progress klines. Binance fires every ~2s while a
				// candle is forming and sets IsFinal=true only on close.
				// Strategies expect one OnCandle per close.
				if !event.Kline.IsFinal {
					return
				}
				open, _ := strconv.ParseFloat(event.Kline.Open, 64)
				high, _ := strconv.ParseFloat(event.Kline.High, 64)
				low, _ := strconv.ParseFloat(event.Kline.Low, 64)
				closeVal, _ := strconv.ParseFloat(event.Kline.Close, 64)
				volume, _ := strconv.ParseFloat(event.Kline.Volume, 64)
				quoteVolume, _ := strconv.ParseFloat(event.Kline.QuoteVolume, 64)
				takerBuyBase, _ := strconv.ParseFloat(event.Kline.ActiveBuyVolume, 64)
				takerBuyQuote, _ := strconv.ParseFloat(event.Kline.ActiveBuyQuoteVolume, 64)

				candle := &types.Candle{
					Symbol:              origSym,
					Exchange:            "binance",
					Timeframe:           types.Timeframe1m,
					OpenTime:            time.UnixMilli(event.Kline.StartTime),
					CloseTime:           time.UnixMilli(event.Kline.EndTime),
					Open:                open,
					High:                high,
					Low:                 low,
					Close:               closeVal,
					Volume:              volume,
					IsComplete:          event.Kline.IsFinal,
					TradeCount:          event.Kline.TradeNum,
					QuoteVolume:         quoteVolume,
					TakerBuyBaseVolume:  takerBuyBase,
					TakerBuyQuoteVolume: takerBuyQuote,
				}
				candleChan <- candle
			}, func(err error) {
				log.Printf("Binance WS %s error: %v", sym, err)
			})

			if err != nil {
				log.Printf("Binance WS init error %s: %v", sym, err)
				return
			}

			<-ctx.Done()
			stopC <- struct{}{}
			<-doneC
		}(formatSym, originalSym)
	}

	return nil
}

// binanceSpotOrderType translates a venue-neutral intent into Binance spot's
// own vocabulary.
//
// IntentTakeProfitLimit maps to a plain LIMIT rather than Binance's
// TAKE_PROFIT_LIMIT: the SDK's TAKE_PROFIT_LIMIT is a resting exit at Price
// (see types.OrderTypeTakeProfitLimit), while Binance's is trigger-based and
// requires a stopPrice the strategy never supplied. The returned Order keeps
// the strategy's stated type, so intent is still recoverable from the log.
func binanceSpotOrderType(t IntentType) (binance.OrderType, error) {
	switch t {
	case IntentMarket:
		return binance.OrderTypeMarket, nil
	case IntentLimit, IntentTakeProfitLimit:
		return binance.OrderTypeLimit, nil
	case IntentStopMarket:
		return binance.OrderTypeStopLoss, nil
	case IntentStopLimit:
		return binance.OrderTypeStopLossLimit, nil
	default:
		return "", fmt.Errorf("binance spot: no order type for intent %s", t)
	}
}

func (b *BinanceClient) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	intent, err := MapOrder(req, MarketSpot)
	if err != nil {
		return nil, fmt.Errorf("binance spot: %w", err)
	}
	sym := intent.Symbol

	filter := b.filterFor(sym)
	if err := filter.Apply(intent, 0); err != nil {
		return nil, fmt.Errorf("binance spot %s: %w", sym, err)
	}

	orderType, err := binanceSpotOrderType(intent.Type)
	if err != nil {
		return nil, err
	}

	side := binance.SideTypeBuy
	if intent.Side == types.OrderSideSell {
		side = binance.SideTypeSell
	}

	srv := b.client.NewCreateOrderService().
		Symbol(sym).
		Side(side).
		Type(orderType).
		Quantity(filter.FormatQty(intent.Quantity))

	if intent.Price > 0 {
		srv = srv.Price(filter.FormatPrice(intent.Price))
	}
	if intent.StopPrice > 0 {
		srv = srv.StopPrice(filter.FormatPrice(intent.StopPrice))
	}
	// STOP_LOSS is market-on-trigger and rejects a time-in-force; every other
	// resting type requires one.
	if intent.TimeInForce != "" && orderType != binance.OrderTypeStopLoss {
		srv = srv.TimeInForce(binance.TimeInForceType(intent.TimeInForce))
	}

	res, err := srv.Do(ctx)
	if err != nil {
		return nil, err
	}

	status := types.OrderStatusNew
	if res.Status == binance.OrderStatusTypeFilled {
		status = types.OrderStatusFilled
	} else if res.Status == binance.OrderStatusTypePartiallyFilled {
		status = types.OrderStatusPartiallyFilled
	}

	price, _ := strconv.ParseFloat(res.Price, 64)
	qty, _ := strconv.ParseFloat(res.OrigQuantity, 64)
	execQty, _ := strconv.ParseFloat(res.ExecutedQuantity, 64)

	return &types.Order{
		ID:       strconv.FormatInt(res.OrderID, 10),
		Symbol:   req.Symbol,
		Exchange: "binance",
		Side:     req.Side,
		// The strategy's stated type, not the wire type: a
		// TAKE_PROFIT_LIMIT sent as a LIMIT is still a take-profit as far as
		// the strategy and the telemetry log are concerned.
		Type:         req.Type,
		Status:       status,
		Price:        price,
		Quantity:     qty,
		FilledQty:    execQty,
		AveragePrice: price,
		StopPrice:    intent.StopPrice,
		CreatedAt:    time.UnixMilli(res.TransactTime),
		UpdatedAt:    time.UnixMilli(res.TransactTime),
	}, nil
}

func (b *BinanceClient) CancelOrder(ctx context.Context, exchange, symbol, id string) error {
	orderID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return fmt.Errorf("binance CancelOrder: invalid orderID %q: %w", id, err)
	}
	// Binance uses no slash in symbol (e.g. BTCUSDT not BTC/USDT)
	sym := strings.ReplaceAll(symbol, "/", "")
	_, err = b.client.NewCancelOrderService().Symbol(sym).OrderID(orderID).Do(ctx)
	return err
}

func (b *BinanceClient) GetAccount(ctx context.Context, exchange string, asset string) (*types.Account, error) {
	res, err := b.client.NewGetAccountService().Do(ctx)
	if err != nil {
		return nil, err
	}

	acc := &types.Account{Exchange: "binance"}
	for _, bal := range res.Balances {
		if bal.Asset == asset || asset == "" {
			free, _ := strconv.ParseFloat(bal.Free, 64)
			locked, _ := strconv.ParseFloat(bal.Locked, 64)
			acc.Balances = append(acc.Balances, types.Balance{
				Asset: bal.Asset,
				Free:  free,
				Lock:  locked,
			})
		}
	}

	return acc, nil
}

func (b *BinanceClient) Next(ctx context.Context) error {
	return nil
}

// binanceInterval maps SDK timeframes to Binance kline interval strings.
func binanceInterval(tf types.Timeframe) (string, error) {
	switch tf {
	case types.Timeframe1m:
		return "1m", nil
	case types.Timeframe3m:
		return "3m", nil
	case types.Timeframe5m:
		return "5m", nil
	case types.Timeframe15m:
		return "15m", nil
	case types.Timeframe30m:
		return "30m", nil
	case types.Timeframe1h:
		return "1h", nil
	case types.Timeframe2h:
		return "2h", nil
	case types.Timeframe4h:
		return "4h", nil
	case types.Timeframe1d:
		return "1d", nil
	default:
		return "", fmt.Errorf("binance: unsupported timeframe %q", tf)
	}
}

// GetHistoricalCandles pages forward through Binance's kline REST API,
// returning closed candles in [from, to] sorted oldest-first.
func (b *BinanceClient) GetHistoricalCandles(ctx context.Context, exchange, symbol string, from, to time.Time, tf types.Timeframe) ([]*types.Candle, error) {
	if b.client == nil {
		// Allow callers to fetch history before PrepareSession.
		if b.config != nil && b.config.Environment == types.EnvTestBinance {
			binance.UseTestnet = true
		}
		b.client = newBinanceClient(b.config.Credentials)
	}
	if from.After(to) {
		return nil, fmt.Errorf("binance: from %s is after to %s", from, to)
	}
	interval, err := binanceInterval(tf)
	if err != nil {
		return nil, err
	}

	sym := strings.ToUpper(strings.ReplaceAll(symbol, "/", ""))
	tfDur := aggregator.ExtractDuration(tf)
	durationMs := tfDur.Milliseconds()
	fromMs := from.UnixMilli()
	toMs := to.UnixMilli()

	const pageLimit = 1000
	var all []*types.Candle
	cursor := fromMs

	for cursor <= toMs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		page, err := b.client.NewKlinesService().
			Symbol(sym).
			Interval(interval).
			StartTime(cursor).
			EndTime(toMs).
			Limit(pageLimit).
			Do(ctx)
		if err != nil {
			return nil, fmt.Errorf("binance kline fetch: %w", err)
		}
		if len(page) == 0 {
			break
		}

		var lastOpenMs int64
		for _, k := range page {
			open, _ := strconv.ParseFloat(k.Open, 64)
			high, _ := strconv.ParseFloat(k.High, 64)
			low, _ := strconv.ParseFloat(k.Low, 64)
			closeVal, _ := strconv.ParseFloat(k.Close, 64)
			volume, _ := strconv.ParseFloat(k.Volume, 64)
			quoteVolume, _ := strconv.ParseFloat(k.QuoteAssetVolume, 64)
			takerBuyBase, _ := strconv.ParseFloat(k.TakerBuyBaseAssetVolume, 64)
			takerBuyQuote, _ := strconv.ParseFloat(k.TakerBuyQuoteAssetVolume, 64)

			all = append(all, &types.Candle{
				Symbol:              symbol,
				Exchange:            "binance",
				Timeframe:           tf,
				OpenTime:            time.UnixMilli(k.OpenTime),
				CloseTime:           time.UnixMilli(k.OpenTime + durationMs),
				Open:                open,
				High:                high,
				Low:                 low,
				Close:               closeVal,
				Volume:              volume,
				IsComplete:          true,
				TradeCount:          k.TradeNum,
				QuoteVolume:         quoteVolume,
				TakerBuyBaseVolume:  takerBuyBase,
				TakerBuyQuoteVolume: takerBuyQuote,
			})
			if k.OpenTime > lastOpenMs {
				lastOpenMs = k.OpenTime
			}
		}

		if len(page) < pageLimit {
			break
		}
		// Advance to one millisecond past the most recent candle's open.
		cursor = lastOpenMs + 1
	}

	return all, nil
}

// OrderFeed: Binance *spot* has no user data stream in this adapter — it
// subscribes klines only. Until one exists, order updates are reconstructed by
// the SDK's polling reconciler from ListOpenOrders and GetOrder below.
//
// This is the adapter the OrderFeed type was introduced for: SetOnOrderUpdate
// worked on the backtest engine, on Bybit and on Binance futures, and silently
// never fired here. Declaring the feed makes that a startup line instead of a
// discovery.
func (b *BinanceClient) OrderFeed() types.OrderFeed {
	const interval = 3 * time.Second
	return types.OrderFeed{Push: false, PollEvery: interval, Latency: interval}
}

// ListOpenOrders implements the read half of the SDK's polling order feed.
func (b *BinanceClient) ListOpenOrders(ctx context.Context, exchange, symbol string) ([]*types.Order, error) {
	res, err := b.client.NewListOpenOrdersService().Symbol(VenueSymbol(symbol)).Do(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*types.Order, 0, len(res))
	for _, o := range res {
		out = append(out, mapBinanceSpotOrder(o.OrderID, string(o.Side), string(o.Type), string(o.Status),
			o.Price, o.OrigQuantity, o.ExecutedQuantity, o.StopPrice, symbol, o.UpdateTime))
	}
	return out, nil
}

// GetOrder resolves what became of one order.
//
// The open-orders list cannot distinguish a fill from a cancellation — both
// simply leave it — so the reconciler asks here rather than assuming. Treating
// a disappearance as a fill would report positions the account never held.
func (b *BinanceClient) GetOrder(ctx context.Context, exchange, symbol, id string) (*types.Order, error) {
	orderID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("binance GetOrder: invalid orderID %q: %w", id, err)
	}
	o, err := b.client.NewGetOrderService().Symbol(VenueSymbol(symbol)).OrderID(orderID).Do(ctx)
	if err != nil {
		return nil, err
	}
	return mapBinanceSpotOrder(o.OrderID, string(o.Side), string(o.Type), string(o.Status),
		o.Price, o.OrigQuantity, o.ExecutedQuantity, o.StopPrice, symbol, o.UpdateTime), nil
}

// mapBinanceSpotOrder converts a venue order record into the SDK shape, shared
// by both reads above so they cannot drift apart.
func mapBinanceSpotOrder(id int64, side, orderType, status, price, origQty, execQty, stopPrice, symbol string, updateMs int64) *types.Order {
	s := types.OrderSideBuy
	if strings.EqualFold(side, "SELL") {
		s = types.OrderSideSell
	}
	return &types.Order{
		ID:           strconv.FormatInt(id, 10),
		Symbol:       symbol,
		Exchange:     "binance",
		Side:         s,
		Type:         spotOrderTypeFromVenue(orderType),
		Status:       spotStatusFromVenue(status),
		Price:        parseFloat(price),
		Quantity:     parseFloat(origQty),
		FilledQty:    parseFloat(execQty),
		AveragePrice: parseFloat(price),
		StopPrice:    parseFloat(stopPrice),
		UpdatedAt:    time.UnixMilli(updateMs),
		CreatedAt:    time.UnixMilli(updateMs),
	}
}

func spotOrderTypeFromVenue(t string) types.OrderType {
	switch strings.ToUpper(t) {
	case "LIMIT", "LIMIT_MAKER":
		return types.OrderTypeLimit
	case "STOP_LOSS":
		return types.OrderTypeStopLoss
	case "STOP_LOSS_LIMIT":
		return types.OrderTypeStopLossLimit
	case "TAKE_PROFIT_LIMIT":
		return types.OrderTypeTakeProfitLimit
	default:
		return types.OrderTypeMarket
	}
}

func spotStatusFromVenue(s string) types.OrderStatus {
	switch strings.ToUpper(s) {
	case "FILLED":
		return types.OrderStatusFilled
	case "PARTIALLY_FILLED":
		return types.OrderStatusPartiallyFilled
	case "CANCELED", "EXPIRED":
		return types.OrderStatusCanceled
	case "REJECTED":
		return types.OrderStatusRejected
	default:
		return types.OrderStatusNew
	}
}
