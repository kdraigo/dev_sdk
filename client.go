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
	onCandleAll      types.OnCandleFunc                     // Fires on every closed candle regardless of timeframe.
	onCandleHandlers map[types.Timeframe]types.OnCandleFunc // Per-timeframe callbacks.
	onOrderUpdate    types.OnOrderUpdateFunc
	onComplete       func() // Called when a backtest run finishes naturally

	// Channels for internal piping
	rawCandleChan chan *types.Candle
	orderChan     chan *types.Order

	// Per-timeframe indicator managers and aggregators.
	indManagers map[types.Timeframe]indicators.IndicatorManager
	aggregators map[types.Timeframe]*aggregator.TimeframeAggregator
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
		rawCandleChan: make(chan *types.Candle, 100),
		orderChan:     make(chan *types.Order, 100),
	}, nil
}

// SetOnCandle registers a catch-all callback that fires whenever any subscribed timeframe closes.
// candle.Timeframe identifies which timeframe triggered the call.
func (s *SDK) SetOnCandle(fn types.OnCandleFunc) {
	s.onCandleAll = fn
}

// SetOnCandleFor registers a callback that fires only when the given timeframe closes.
// Multiple calls with different timeframes register independent handlers.
func (s *SDK) SetOnCandleFor(tf types.Timeframe, fn types.OnCandleFunc) {
	if s.onCandleHandlers == nil {
		s.onCandleHandlers = make(map[types.Timeframe]types.OnCandleFunc)
	}
	s.onCandleHandlers[tf] = fn
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

	if len(s.config.Timeframes) == 0 {
		return fmt.Errorf("config.Timeframes must contain at least one timeframe")
	}

	var imExchanges, imAssets []string
	if s.config.Backtest != nil {
		imExchanges = s.config.Backtest.RequestedExchanges
		imAssets = s.config.Backtest.Assets

		// Round start time up to the next boundary of the largest requested timeframe
		// to avoid receiving a partial first candle.
		largest := largestTimeframe(s.config.Timeframes)
		s.config.Backtest.StartTime = roundUpToTimeframe(s.config.Backtest.StartTime, largest)
	} else if s.config.Live != nil {
		imExchanges = s.config.Live.RequestedExchanges
		imAssets = s.config.Live.Assets
	}

	// Build one indicator manager and one aggregator per requested timeframe.
	s.indManagers = make(map[types.Timeframe]indicators.IndicatorManager, len(s.config.Timeframes))
	s.aggregators = make(map[types.Timeframe]*aggregator.TimeframeAggregator, len(s.config.Timeframes))
	for _, tf := range s.config.Timeframes {
		s.indManagers[tf] = indicators.NewIndicatorManager(imExchanges, imAssets)
		s.aggregators[tf] = aggregator.NewTimeframeAggregator(tf)
	}

	// Single goroutine: fan out each raw tick to all aggregators, fire callbacks for any
	// that cross a boundary, then signal the ticking loop exactly once per raw tick.
	// This keeps the pipeline fully sequential and deterministic.
	go func() {
		for rawCandle := range s.rawCandleChan {
			for _, tf := range s.config.Timeframes {
				if closed := s.aggregators[tf].Process(rawCandle); closed != nil {
					s.indManagers[tf].Update(closed)
					s.dispatchCandle(sdkCtx, tf, closed)
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
	if err := s.adapter.ConnectStream(ctx, s.rawCandleChan, s.orderChan); err != nil {
		return fmt.Errorf("failed to connect stream: %w", err)
	}

	// Allow WS connection to fully establish before the handshake.
	time.Sleep(500 * time.Millisecond)

	if s.config.Backtest != nil {
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
	}

	// 6. Start the deterministic ticking loop for backtesting
	if s.config.Environment == types.EnvBacktest {
		go func() {
			// Trigger initial tick
			if err := s.adapter.Next(ctx); err != nil {
				log.Printf("DevSDK: initial tick error: %v", err)
				cancel()
				return
			}

			// nextTimeout is how long the tick loop waits for syncChan before
			// assuming the engine went silent (e.g. after an order fill) and
			// re-issuing a "next" command to unblock it.
			const nextTimeout = 15 * time.Second

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
					if err := s.adapter.Next(ctx); err != nil {
						log.Printf("DevSDK: Next() error: %v", err)
						if s.onComplete != nil {
							s.onComplete()
						}
						cancel()
						return
					}
				case <-time.After(nextTimeout):
					// Engine went silent (no candle arrived within nextTimeout).
					// This happens when the engine sends an order-fill event but waits
					// for another "next" command before streaming the following candle.
					log.Printf("DevSDK: engine silent for %s — re-issuing next", nextTimeout)
					if err := s.adapter.Next(ctx); err != nil {
						log.Printf("DevSDK: recovery Next() error: %v", err)
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

func (s *SDK) CancelOrder(ctx context.Context, exchange, symbol, id string) error {
	return s.adapter.CancelOrder(ctx, exchange, symbol, id)
}

func (s *SDK) GetAccount(ctx context.Context, exchange string, asset string) (*types.Account, error) {
	return s.adapter.GetAccount(ctx, exchange, asset)
}

// IndicatorManagerFor returns the indicator manager scoped to the given timeframe.
// Call this inside SetOnCandleFor or SetOnCandle (use candle.Timeframe) to get
// RSI/EMA/etc. values calculated from candles of that specific timeframe.
func (s *SDK) IndicatorManagerFor(tf types.Timeframe) indicators.IndicatorsCalculator {
	return s.indManagers[tf]
}

// IndicatorManager returns the indicator manager for the first configured timeframe.
// Kept for single-timeframe backward compatibility.
func (s *SDK) IndicatorManager() indicators.IndicatorsCalculator {
	if len(s.config.Timeframes) == 0 {
		return nil
	}
	return s.indManagers[s.config.Timeframes[0]]
}

// dispatchCandle fires the per-timeframe handler (if any) then the catch-all handler (if any).
func (s *SDK) dispatchCandle(ctx *types.Context, tf types.Timeframe, candle *types.Candle) {
	if fn, ok := s.onCandleHandlers[tf]; ok {
		fn(ctx, candle)
	}
	if s.onCandleAll != nil {
		s.onCandleAll(ctx, candle)
	}
}

// largestTimeframe returns the timeframe with the longest duration from the slice.
func largestTimeframe(tfs []types.Timeframe) types.Timeframe {
	largest := tfs[0]
	largestD := aggregator.ExtractDuration(largest)
	for _, tf := range tfs[1:] {
		if d := aggregator.ExtractDuration(tf); d > largestD {
			largest = tf
			largestD = d
		}
	}
	return largest
}

// roundUpToTimeframe rounds t up to the next boundary of the given timeframe duration.
// If t is already on a boundary it is returned unchanged.
func roundUpToTimeframe(t time.Time, tf types.Timeframe) time.Time {
	d := aggregator.ExtractDuration(tf)
	truncated := t.UTC().Truncate(d)
	if truncated.Equal(t.UTC()) {
		return t
	}
	rounded := truncated.Add(d)
	log.Printf("DevSDK: start time rounded up from %s to %s (largest timeframe: %s)", t.UTC().Format(time.RFC3339), rounded.Format(time.RFC3339), tf)
	return rounded
}
