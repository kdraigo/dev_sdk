package dev_sdk

import (
	"context"
	"fmt"
	"log"

	"github.com/kdraigo/flow_v1/dev_sdk/aggregator"
	"github.com/kdraigo/flow_v1/dev_sdk/exchange/backtest"
	"github.com/kdraigo/flow_v1/dev_sdk/exchange/live"
	"github.com/kdraigo/flow_v1/dev_sdk/indicators"
	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

// SDK represents the central structure users instantiate to run their bots.
type SDK struct {
	config  *types.Config
	adapter Adapter

	// Callbacks
	onCandle      types.OnCandleFunc
	onOrderUpdate types.OnOrderUpdateFunc

	// Channels for internal piping
	rawCandleChan chan *types.Candle
	orderChan     chan *types.Order
}

var _ types.Trader = (*SDK)(nil)

// New initializes the SDK dynamically using the environment configuration.
func New(cfg *types.Config) (*SDK, error) {
	// Architecture requires dynamically choosing the adapter:
	var adapter Adapter
	switch cfg.Environment {
	case types.EnvBacktest:
		adapter = backtest.NewEngineClient(cfg)
	case types.EnvRealBinance, types.EnvTestBinance:
		adapter = live.NewBinanceClient(cfg)
	case types.EnvRealBybit, types.EnvTestBybit:
		adapter = live.NewBybitClient(cfg)
	default:
		return nil, fmt.Errorf("unsupported environment: %s", cfg.Environment)
	}

	return &SDK{
		config:        cfg,
		adapter:       adapter,
		rawCandleChan: make(chan *types.Candle),
		orderChan:     make(chan *types.Order, 100),
	}, nil
}

// SetOnCandle binds the strategy candle iteration callback.
func (s *SDK) SetOnCandle(fn types.OnCandleFunc) {
	s.onCandle = fn
}

// SetOnOrderUpdate binds the strategy callback to watch execution.
func (s *SDK) SetOnOrderUpdate(fn types.OnOrderUpdateFunc) {
	s.onOrderUpdate = fn
}

// Start launches the architecture pipeline and begins processing stream data.
func (s *SDK) Start(ctx context.Context) error {
	if s.adapter == nil {
		log.Println("DevSDK: No adapter attached (stub initialized). Cannot connect.")
		return nil
	}

	// 1. Prepare session configuration against target Backend (crucial for Backtesting)
	if err := s.adapter.PrepareSession(ctx, s.config); err != nil {
		return err
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sdkCtx := &types.Context{
		Ctx:    cctx,
		Cancel: cancel,
		Config: s.config,
		Trader: s,
	}

	// 2. Start internal processing pipelines
	aggChan := make(chan *types.Candle)

	// Pipeline A: Aggregator converts 1m -> target (e.g. 15m)
	timeframeAgg := aggregator.NewTimeframeAggregator(s.config.Timeframe, aggChan)
	go timeframeAgg.Run(s.rawCandleChan)

	// Pipeline B: Indicator Manager applies math state and fires the OnCandle core loop
	indicatorMgr := indicators.NewIndicatorManager(s.config.Indicators)
	go indicatorMgr.Run(sdkCtx, aggChan, s.onCandle)

	// Pipeline C: Order Updates dispatch loop
	go func() {
		for order := range s.orderChan {
			if s.onOrderUpdate != nil {
				s.onOrderUpdate(sdkCtx, order)
			}
		}
	}()

	// 3. Command the exchange Adapter to begin pumping data into `rawCandleChan` & `orderChan` natively
	return s.adapter.ConnectStream(ctx, s.rawCandleChan, s.orderChan)
}

// Exposed methods passing through to adapter

func (s *SDK) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	return s.adapter.PlaceOrder(ctx, req)
}

func (s *SDK) CancelOrder(ctx context.Context, id string) error {
	return s.adapter.CancelOrder(ctx, id)
}
