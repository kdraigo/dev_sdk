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
	"github.com/kdraigo/flow_v1/dev_sdk/aggregator"
	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

// newBinanceClient builds a go-binance client, selecting the signing scheme from
// the credential shape: an Ed25519 PKCS8 PEM in APISecret switches the client to
// asymmetric (Ed25519) signing; otherwise it stays HMAC. The API key string in
// APIKey is always used as-is for the X-MBX-APIKEY header.
func newBinanceClient(creds types.Credentials) *binance.Client {
	c := binance.NewClient(creds.APIKey, creds.APISecret)
	if strings.Contains(creds.APISecret, "BEGIN PRIVATE KEY") {
		c.KeyType = common.KeyTypeEd25519
	}
	return c
}

type BinanceClient struct {
	config *types.Config
	client *binance.Client
}

func NewBinanceClient(cfg *types.Config) *BinanceClient {
	return &BinanceClient{config: cfg}
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

	return nil
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

				candle := &types.Candle{
					Symbol:     origSym,
					Exchange:   "binance",
					Timeframe:  types.Timeframe1m,
					OpenTime:   time.UnixMilli(event.Kline.StartTime),
					CloseTime:  time.UnixMilli(event.Kline.EndTime),
					Open:       open,
					High:       high,
					Low:        low,
					Close:      closeVal,
					Volume:     volume,
					IsComplete: event.Kline.IsFinal,
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

func (b *BinanceClient) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	sym := strings.ToUpper(strings.ReplaceAll(req.Symbol, "/", ""))

	side := binance.SideTypeBuy
	if req.Side == types.OrderSideSell {
		side = binance.SideTypeSell
	}

	orderType := binance.OrderTypeMarket
	if req.Type == types.OrderTypeLimit {
		orderType = binance.OrderTypeLimit
	}

	srv := b.client.NewCreateOrderService().
		Symbol(sym).
		Side(side).
		Type(orderType).
		Quantity(fmt.Sprintf("%f", req.Quantity))

	if req.Type == types.OrderTypeLimit {
		srv = srv.Price(fmt.Sprintf("%f", req.Price)).TimeInForce(binance.TimeInForceTypeGTC)
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
		ID:           strconv.FormatInt(res.OrderID, 10),
		Symbol:       req.Symbol,
		Exchange:     "binance",
		Side:         req.Side,
		Type:         req.Type,
		Status:       status,
		Price:        price,
		Quantity:     qty,
		FilledQty:    execQty,
		AveragePrice: price,
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

			all = append(all, &types.Candle{
				Symbol:     symbol,
				Exchange:   "binance",
				Timeframe:  tf,
				OpenTime:   time.UnixMilli(k.OpenTime),
				CloseTime:  time.UnixMilli(k.OpenTime + durationMs),
				Open:       open,
				High:       high,
				Low:        low,
				Close:      closeVal,
				Volume:     volume,
				IsComplete: true,
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
