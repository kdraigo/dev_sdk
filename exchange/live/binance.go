package live

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/adshao/go-binance/v2"
	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

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

	b.client = binance.NewClient(cfg.Credentials.APIKey, cfg.Credentials.APISecret)

	err := b.client.NewPingService().Do(ctx)
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

func (b *BinanceClient) CancelOrder(ctx context.Context, id string) error {
	// Require symbol! For Binance, Cancel Order requires Symbol and OrderID.
	// Our strict signature doesn't have Symbol but we'll try parsing or logging error.
	return fmt.Errorf("in binance, cancellation requires symbol which is not in interface, implement symbol cache locally")
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
