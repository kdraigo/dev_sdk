package dev_sdk

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

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
	onComplete    func() // Called when a backtest run finishes naturally

	// Channels for internal piping
	rawCandleChan chan *types.Candle
	orderChan     chan *types.Order

	indicatorsManager *indicators.IndicatorManager
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

// SetOnComplete registers a callback invoked when a backtest run finishes naturally.
// The callback fires before Start() returns, allowing cleanup or result reporting.
func (s *SDK) SetOnComplete(fn func()) {
	s.onComplete = fn
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
	// syncChan coordinates the ticking loop: one signal per raw candle consumed,
	// sent only AFTER OnCandle (and any PlaceOrder calls inside it) have returned.
	// This keeps the pipeline fully sequential and deterministic — the engine never
	// receives a "next" request while a concurrent "order" request is in flight.
	syncChan := make(chan bool, 1)

	var imExchanges, imAssets []string
	if s.config.Backtest != nil {
		imExchanges = s.config.Backtest.RequestedExchanges
		imAssets = s.config.Backtest.Assets
	}
	s.indicatorsManager = indicators.NewIndicatorManager(imExchanges, imAssets)
	timeframeAgg := aggregator.NewTimeframeAggregator(s.config.Timeframe)

	// Single goroutine: aggregate → update indicators → fire OnCandle → signal ticking loop.
	// All three steps complete before "next" is sent to the engine, eliminating the race
	// between concurrent "next" and "order" WS messages.
	go func() {
		for rawCandle := range s.rawCandleChan {
			if aggregated := timeframeAgg.Process(rawCandle); aggregated != nil {
				s.indicatorsManager.Update(aggregated)
				if s.onCandle != nil {
					s.onCandle(sdkCtx, aggregated)
				}
			}
			syncChan <- true
		}
		close(syncChan)
	}()

	// Pipeline C: Order Updates dispatch loop
	go func() {
		for {
			select {
			case order := <-s.orderChan:
				if s.onOrderUpdate != nil {
					s.onOrderUpdate(sdkCtx, order)
				}
			case <-sdkCtx.Ctx.Done():
				return
			}
		}
	}()

	// 4. Perform Handshake: Fetch initial account info for all requested assets
	// This ensures "Paper Wallet First" synchronization.
	// But wait, we need the connection to be up.
	// The adapter should handle this by ensuring its internal state is ready.

	// 5. Command the exchange Adapter to begin pumping data into `rawCandleChan` & `orderChan` natives.
	// We run this in a goroutine because it blocks, but we need it to start first.
	go func() {
		if err := s.adapter.ConnectStream(ctx, s.rawCandleChan, s.orderChan); err != nil {
			log.Printf("ConnectStream error: %v", err)
		}
	}()

	// Small delay to allow WS connection to establish (Handshake requires it)
	time.Sleep(500 * time.Millisecond)

	for _, exchange := range s.config.Backtest.RequestedExchanges {
		for _, asset := range s.config.Backtest.Assets {
			quoteAsset := strings.Split(asset, "/")[1]
			_, err := s.adapter.GetAccount(ctx, exchange, quoteAsset)
			if err != nil {
				log.Printf("Handshake: Failed to fetch initial account for %s-%s: %v", exchange, quoteAsset, err)
			} else {
				log.Printf("Handshake: Successfully synchronized wallet for %s-%s", exchange, quoteAsset)
			}
		}
	}

	// 6. Start the deterministic ticking loop for backtesting
	if s.config.Environment == types.EnvBacktest {
		go func() {
			// Trigger initial tick
			if err := s.adapter.Next(ctx); err != nil {
				log.Printf("Initial tick error: %v", err)
				cancel()
				return
			}

			for {
				select {
				case <-sdkCtx.Ctx.Done():
					return
				case _, ok := <-syncChan:
					if !ok {
						// Aggregator closed syncChan — all candles processed, backtest done.
						log.Println("DevSDK: Backtest complete.")
						if s.onComplete != nil {
							s.onComplete()
						}
						cancel()
						return
					}
					// Signal received: Previous candle fully processed by pipelines.
					if err := s.adapter.Next(ctx); err != nil {
						log.Printf("Backtest finished: %v", err)
						if s.onComplete != nil {
							s.onComplete()
						}
						cancel()
						return
					}
				}
			}
		}()
	}

	// Stay alive until the SDK's own context is canceled (backtest done or user interrupt).
	<-sdkCtx.Ctx.Done()
	return nil
}

// Exposed methods passing through to adapter

func (s *SDK) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	return s.adapter.PlaceOrder(ctx, req)
}

func (s *SDK) CancelOrder(ctx context.Context, id string) error {
	return s.adapter.CancelOrder(ctx, id)
}

func (s *SDK) GetAccount(ctx context.Context, exchange string, asset string) (*types.Account, error) {
	return s.adapter.GetAccount(ctx, exchange, asset)
}

func (s *SDK) IndicatorManager() indicators.IndicatorsCalculator {
	return s.indicatorsManager
}
