package dev_sdk

import (
	"context"
	"testing"
	"time"

	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

type mockAdapter struct {
	prepareCalled chan bool
	connectCalled chan bool
}

func (m *mockAdapter) PrepareSession(ctx context.Context, config *types.Config) error {
	m.prepareCalled <- true
	return nil
}

func (m *mockAdapter) ConnectStream(ctx context.Context, candleChan chan<- *types.Candle, orderChan chan<- *types.Order) error {
	m.connectCalled <- true
	return nil
}

func (m *mockAdapter) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	return nil, nil
}

func (m *mockAdapter) CancelOrder(ctx context.Context, orderID string) error {
	return nil
}

func (m *mockAdapter) GetAccount(ctx context.Context, exchange string, asset string) (*types.Account, error) {
	return &types.Account{}, nil
}

func (m *mockAdapter) Next(ctx context.Context) error {
	return nil
}

func TestNewSDK(t *testing.T) {
	cfg := &types.Config{
		Environment: types.EnvBacktest,
	}

	sdk, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}

	if sdk.config != cfg {
		t.Error("SDK config not set correctly")
	}

	if sdk.adapter == nil {
		t.Error("SDK adapter should not be nil for backtest env")
	}
}

func TestSDK_Start(t *testing.T) {
	mock := &mockAdapter{
		prepareCalled: make(chan bool, 1),
		connectCalled: make(chan bool, 1),
	}
	sdk := &SDK{
		config:        &types.Config{Timeframes: []types.Timeframe{types.Timeframe1m}, Backtest: &types.BacktestOptions{}},
		adapter:       mock,
		rawCandleChan: make(chan *types.Candle, 10),
		orderChan:     make(chan *types.Order, 10),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = sdk.Start(ctx)
	}()

	select {
	case <-mock.prepareCalled:
		// Success
	case <-time.After(time.Second):
		t.Error("PrepareSession was not called")
	}

	select {
	case <-mock.connectCalled:
		// Success
	case <-time.After(time.Second):
		t.Error("ConnectStream was not called")
	}
}
