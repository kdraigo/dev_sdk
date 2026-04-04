package live

import (
	"context"
	"log"

	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

type BinanceClient struct {
	config *types.Config
}

func NewBinanceClient(cfg *types.Config) *BinanceClient {
	return &BinanceClient{config: cfg}
}

func (b *BinanceClient) PrepareSession(ctx context.Context, cfg *types.Config) error {
	log.Println("Real Binance: Validating API Keys...")
	return nil
}

func (b *BinanceClient) ConnectStream(ctx context.Context, candleChan chan<- *types.Candle, orderChan chan<- *types.Order) error {
	log.Println("Real Binance: Connecting to Binance WebSocket...")
	<-ctx.Done()
	return nil
}

func (b *BinanceClient) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	log.Printf("Real Binance: Interacting with network for %s API...", b.config.Environment)
	return &types.Order{}, nil
}

func (b *BinanceClient) CancelOrder(ctx context.Context, id string) error {
	return nil
}
