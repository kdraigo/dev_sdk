package live

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/hirokisan/bybit/v2"
	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

type BybitClient struct {
	config *types.Config
	client *bybit.Client
}

func NewBybitClient(cfg *types.Config) *BybitClient {
	return &BybitClient{config: cfg}
}

func (b *BybitClient) PrepareSession(ctx context.Context, cfg *types.Config) error {
	log.Println("Real Bybit: Validating API Keys...")

	if cfg.Environment == types.EnvTestBybit {
		b.client = bybit.NewTestClient().WithAuth(cfg.Credentials.APIKey, cfg.Credentials.APISecret)
	} else {
		b.client = bybit.NewClient().WithAuth(cfg.Credentials.APIKey, cfg.Credentials.APISecret)
	}

	return nil
}

func (b *BybitClient) ConnectStream(ctx context.Context, candleChan chan<- *types.Candle, orderChan chan<- *types.Order) error {
	log.Println("Real Bybit: Connecting to Bybit WebSocket...")

	wsClient := bybit.NewWebsocketClient()
	if b.config.Environment == types.EnvTestBybit {
		wsClient = wsClient.WithBaseURL(bybit.TestNetBaseURL)
	}

	wsSrv, err := wsClient.V5().Public(bybit.CategoryV5Linear)
	if err != nil {
		return fmt.Errorf("failed to initialize bybit ws: %w", err)
	}

	interval := bybit.Interval("1")
	switch b.config.Timeframe {
	case types.Timeframe1m:
		interval = bybit.Interval("1")
	case types.Timeframe3m:
		interval = bybit.Interval("3")
	case types.Timeframe5m:
		interval = bybit.Interval("5")
	case types.Timeframe15m:
		interval = bybit.Interval("15")
	case types.Timeframe30m:
		interval = bybit.Interval("30")
	case types.Timeframe1h:
		interval = bybit.Interval("60")
	case types.Timeframe2h:
		interval = bybit.Interval("120")
	case types.Timeframe4h:
		interval = bybit.Interval("240")
	case types.Timeframe1d:
		interval = bybit.Interval("D")
	}

	assets := []string{}
	if b.config.Live != nil {
		assets = b.config.Live.Assets
	}

	for _, asset := range assets {
		formatSym := strings.ToUpper(strings.ReplaceAll(asset, "/", ""))
		origSym := asset

		_, err := wsSrv.SubscribeKline(bybit.V5WebsocketPublicKlineParamKey{
			Interval: interval,
			Symbol:   bybit.SymbolV5(formatSym),
		}, func(response bybit.V5WebsocketPublicKlineResponse) error {
			for _, k := range response.Data {
				open, _ := strconv.ParseFloat(k.Open, 64)
				high, _ := strconv.ParseFloat(k.High, 64)
				low, _ := strconv.ParseFloat(k.Low, 64)
				closeVal, _ := strconv.ParseFloat(k.Close, 64)
				volume, _ := strconv.ParseFloat(k.Volume, 64)

				candleChan <- &types.Candle{
					Symbol:     origSym,
					Exchange:   "bybit",
					Timeframe:  b.config.Timeframe,
					OpenTime:   time.UnixMilli(k.Start),
					CloseTime:  time.UnixMilli(k.End),
					Open:       open,
					High:       high,
					Low:        low,
					Close:      closeVal,
					Volume:     volume,
					IsComplete: k.Confirm,
				}
			}
			return nil
		})

		if err != nil {
			log.Printf("failed to subscribe to %s: %v", formatSym, err)
		}
	}

	go func() {
		err := wsSrv.Start(ctx, func(isClosed bool, err error) {
			log.Printf("Bybit WS Error (closed=%v): %v", isClosed, err)
		})
		if err != nil {
			log.Printf("Bybit WS Start exited: %v", err)
		}
	}()

	<-ctx.Done()
	return nil
}

func (b *BybitClient) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	sym := strings.ToUpper(strings.ReplaceAll(req.Symbol, "/", ""))

	side := bybit.SideBuy
	if req.Side == types.OrderSideSell {
		side = bybit.SideSell
	}

	orderType := bybit.OrderTypeMarket
	if req.Type == types.OrderTypeLimit {
		orderType = bybit.OrderTypeLimit
	}

	qtyStr := fmt.Sprintf("%f", req.Quantity)

	param := bybit.V5CreateOrderParam{
		Category:  bybit.CategoryV5Linear,
		Symbol:    bybit.SymbolV5(sym),
		Side:      side,
		OrderType: orderType,
		Qty:       qtyStr,
	}

	if req.Type == types.OrderTypeLimit {
		priceStr := fmt.Sprintf("%f", req.Price)
		param.Price = &priceStr
	}

	res, err := b.client.V5().Order().CreateOrder(param)
	if err != nil {
		return nil, err
	}

	status := types.OrderStatusNew

	return &types.Order{
		ID:           res.Result.OrderID,
		Symbol:       req.Symbol,
		Exchange:     "bybit",
		Side:         req.Side,
		Type:         req.Type,
		Status:       status,
		Price:        req.Price,
		Quantity:     req.Quantity,
		FilledQty:    0,
		AveragePrice: 0,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}, nil
}

func (b *BybitClient) CancelOrder(ctx context.Context, id string) error {
	// Usually requires Symbol too like Binance. We'll pass error if symbol is hard required.
	return fmt.Errorf("in bybit, cancellation requires symbol which is not in interface, implement symbol cache locally")
}

func (b *BybitClient) GetAccount(ctx context.Context, exchange string, asset string) (*types.Account, error) {
	accType := bybit.AccountTypeV5UNIFIED
	res, err := b.client.V5().Account().GetWalletBalance(accType, nil)
	if err != nil {
		return nil, err
	}

	acc := &types.Account{Exchange: "bybit"}
	for _, rawBal := range res.Result.List {
		for _, coin := range rawBal.Coin {
			if string(coin.Coin) == asset || asset == "" {
				free, _ := strconv.ParseFloat(coin.AvailableToWithdraw, 64)
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
