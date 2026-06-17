package dev_sdk

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/kdraigo/dev_sdk/aggregator"
	"github.com/kdraigo/dev_sdk/exchange/backtest"
	"github.com/kdraigo/dev_sdk/exchange/live"
	"github.com/kdraigo/dev_sdk/indicators"
	"github.com/kdraigo/dev_sdk/telemetry"
	"github.com/kdraigo/dev_sdk/types"
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

	publisher telemetry.Publisher

	// clock is the strategy-facing time source. wallClock in live mode,
	// backtestClock in backtest mode. Backtest clock advances on every
	// dispatched closed candle (only path that mutates it).
	clock Clock

	// Telemetry counters. Read by the heartbeat goroutine via atomics so the
	// dispatch goroutine never blocks. Bumped from candle and order paths.
	candlesProcessed atomic.Int64
	ordersPlaced     atomic.Int32
	lastOrderID      atomic.Value // string
}

var _ types.Trader = (*SDK)(nil)

// New initializes the SDK dynamically using the environment configuration.
func New(cfg *types.Config) (*SDK, error) {
	// Architecture requires dynamically choosing the adapter:
	var adapter Adapter
	var clock Clock
	switch cfg.Environment {
	case types.EnvBacktest:
		adapter = backtest.NewEngineClient(cfg)
		// Backtest clock starts at session.From so sdk.Now() is meaningful
		// before the first candle is dispatched (e.g. for warmup history calls).
		var start time.Time
		if cfg.Backtest != nil {
			start = cfg.Backtest.StartTime
		}
		clock = newBacktestClock(start)
	case types.EnvRealBinance, types.EnvTestBinance:
		adapter = live.NewBinanceClient(cfg)
		clock = wallClock{}
	case types.EnvRealBybit, types.EnvTestBybit:
		adapter = live.NewBybitClient(cfg)
		clock = wallClock{}
	default:
		return nil, fmt.Errorf("unsupported environment: %s", cfg.Environment)
	}

	var pub telemetry.Publisher
	if cfg.Live != nil && cfg.Live.TelemetryURL != "" {
		// Reuse a caller-supplied session id when set (resume/extend an existing
		// session); otherwise mint a fresh one.
		sessionID := cfg.Live.SessionID
		if sessionID == "" {
			sessionID = uuid.New().String()
		}
		// First configured exchange + symbol become the publisher's defaults.
		// Heartbeat / initial_balance / balance / session_stopped events use
		// these so the first ingest creates a properly-populated session row
		// (otherwise the frontend's chart sits on an empty symbol).
		defaultExchange := ""
		defaultSymbol := ""
		if len(cfg.Live.RequestedExchanges) > 0 {
			defaultExchange = cfg.Live.RequestedExchanges[0]
		}
		if len(cfg.Live.Assets) > 0 {
			defaultSymbol = cfg.Live.Assets[0]
		}
		pub = telemetry.NewPublisher(sessionID, cfg.Live.TelemetryURL, cfg.Credentials.KeyID, cfg.Credentials.PrivateKey, defaultExchange, defaultSymbol)
		log.Printf("SDK: telemetry session_id=%s → %s (default %s/%s)", sessionID, cfg.Live.TelemetryURL, defaultExchange, defaultSymbol)
	} else {
		pub = telemetry.NoOpPublisher{}
	}

	return &SDK{
		config:        cfg,
		adapter:       adapter,
		rawCandleChan: make(chan *types.Candle, 100),
		orderChan:     make(chan *types.Order, 100),
		publisher:     pub,
		clock:         clock,
	}, nil
}

// Now returns the current time in the strategy's frame of reference.
// In live mode this is wall time; in backtest mode it is the simulated clock
// (close time of the last dispatched candle, or the configured session start
// before any candle has been dispatched).
func (s *SDK) Now() time.Time {
	if s.clock == nil {
		return time.Now()
	}
	return s.clock.Now()
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
		Clock:  s.clock,
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
		count := 0
		for rawCandle := range s.rawCandleChan {
			count++
			s.candlesProcessed.Store(int64(count))
			if count%1000 == 0 {
				log.Printf("DevSDK: processed %d candles... (%s)", count, rawCandle.OpenTime.Format("2006-01-02 15:04"))
			}
			// Advance the simulated clock for backtest. In live the clock is a
			// wallClock and Advance is a no-op (interface check below).
			if bc, ok := s.clock.(*backtestClock); ok && rawCandle != nil {
				bc.Advance(rawCandle.CloseTime)
			}
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
				// Order updates from the exchange/engine stream carry no
				// strategy-supplied reason — that lives on the original
				// PlaceOrder call.
				if s.publisher != nil {
					s.publisher.PublishOrder(order, nil, nil)
				}
				if s.onOrderUpdate != nil {
					s.onOrderUpdate(sdkCtx, order)
				}
				// After a fill, refresh the current-balance snapshot so the
				// frontend's wallet delta picks up the change without
				// reconstructing from order history.
				if order.Status == types.OrderStatusFilled && s.publisher != nil && s.publisher.Enabled() {
					go s.publishCurrentBalanceFor(sdkCtx.Ctx, order.Exchange, order.Symbol)
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

	// Telemetry side-channel: live mode only. Heartbeat at 5s, initial
	// wallet snapshot on connect, current wallet snapshot on a 60s fallback
	// timer (plus the per-fill trigger in the order dispatch loop above),
	// explicit stopped event on graceful shutdown.
	if s.publisher != nil && s.publisher.Enabled() && s.config.Environment != types.EnvBacktest && s.config.Live != nil {
		startedAt := time.Now()
		s.publishInitialBalances(ctx)
		go s.heartbeatLoop(sdkCtx.Ctx, startedAt)
		go s.balanceFallbackLoop(sdkCtx.Ctx)
		defer s.publisher.PublishStopped("normal_shutdown")
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
	reason, logs := req.Reason, req.Logs

	// Strip telemetry fields for live adapters — exchanges never see Reason/Logs.
	// In backtest mode the fields are forwarded to the engine so it can persist them.
	if s.config.Environment != types.EnvBacktest {
		req.Reason, req.Logs = nil, nil
	}

	order, err := s.adapter.PlaceOrder(ctx, req)
	if err == nil && order != nil {
		s.ordersPlaced.Add(1)
		s.lastOrderID.Store(order.ID)
		if s.publisher != nil {
			s.publisher.PublishOrder(order, reason, logs)
		}
	}
	return order, err
}

func (s *SDK) CancelOrder(ctx context.Context, exchange, symbol, id string) error {
	err := s.adapter.CancelOrder(ctx, exchange, symbol, id)
	if err == nil && s.publisher != nil {
		// Optimization: Publish a synthetic CANCELED event immediately to telemetry.
		// This ensures live_trades is updated even if the private WS stream is lagging or auth-failed.
		s.publisher.PublishOrder(&types.Order{
			ID:       id,
			Symbol:   symbol,
			Exchange: exchange,
			Status:   types.OrderStatusCanceled,
		}, nil, nil)
	}
	return err
}

func (s *SDK) GetAccount(ctx context.Context, exchange string, asset string) (*types.Account, error) {
	return s.adapter.GetAccount(ctx, exchange, asset)
}

// GetCandles returns the last `count` closed candles for the given timeframe,
// ending at sdk.Now(). In live mode "now" is wall time; in backtest "now" is
// the close time of the most recent dispatched candle (or the configured
// session start before any candle has been dispatched).
//
// Equivalent to GetCandlesFromTo(s.Now() - count*tfDuration, s.Now(), tf).
func (s *SDK) GetCandles(ctx context.Context, exchange, symbol string, count int, tf types.Timeframe) ([]*types.Candle, error) {
	if count <= 0 {
		return nil, fmt.Errorf("count must be positive, got %d", count)
	}
	tfDur := aggregator.ExtractDuration(tf)
	to := s.Now()
	from := to.Add(-time.Duration(count) * tfDur)
	return s.GetCandlesFromTo(ctx, exchange, symbol, from, to, tf)
}

// GetCandlesFromTo returns closed candles in [from, to] for the given
// timeframe. In backtest mode the engine rejects requests with `to` past the
// simulated playhead to prevent lookahead leak.
func (s *SDK) GetCandlesFromTo(ctx context.Context, exchange, symbol string, from, to time.Time, tf types.Timeframe) ([]*types.Candle, error) {
	if s.adapter == nil {
		return nil, fmt.Errorf("sdk has no adapter")
	}
	return s.adapter.GetHistoricalCandles(ctx, exchange, symbol, from, to, tf)
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

// ── telemetry side-channel helpers (live mode only) ─────────────────────────

// publishInitialBalances fetches the wallet for each configured (exchange,
// asset) pair once at Start and forwards to telemetry. Server treats the
// first ever write per session as immutable.
func (s *SDK) publishInitialBalances(ctx context.Context) {
	if s.config.Live == nil {
		return
	}
	relevant := extractRelevantAssets(s.config.Live.Assets)
	for _, exch := range s.config.Live.RequestedExchanges {
		account, err := s.adapter.GetAccount(ctx, exch, "")
		if err != nil {
			log.Printf("[telemetry] initial balance fetch %s failed: %v", exch, err)
			continue
		}
		if account == nil {
			continue
		}
		filtered := filterAccountBalances(account, relevant)
		s.publisher.PublishInitialBalance(filtered)
	}
}

// publishCurrentBalanceFor refreshes the wallet snapshot after a fill. The
// symbol's quote asset is the most likely thing to have changed; we fetch
// the whole account anyway so the snapshot is consistent (then filter to the
// strategy's configured pair so the live_trades JSONB doesn't carry every
// dust balance on the exchange).
func (s *SDK) publishCurrentBalanceFor(ctx context.Context, exchange, symbol string) {
	if exchange == "" {
		return
	}
	account, err := s.adapter.GetAccount(ctx, exchange, "")
	if err != nil {
		log.Printf("[telemetry] current balance fetch %s failed: %v", exchange, err)
		return
	}
	if account == nil {
		return
	}
	var relevant map[string]struct{}
	if s.config.Live != nil {
		relevant = extractRelevantAssets(s.config.Live.Assets)
	}
	s.publisher.PublishBalance(filterAccountBalances(account, relevant))
}

// extractRelevantAssets returns the union of base+quote assets across the
// configured trading symbols. Handles both slash ("BTC/USDT") and concat
// ("BTCUSDT") forms. For concat, suffix-matches a fixed quote-asset list.
func extractRelevantAssets(symbols []string) map[string]struct{} {
	out := make(map[string]struct{}, len(symbols)*2)
	commonQuotes := []string{"USDT", "USDC", "BUSD", "FDUSD", "DAI", "BTC", "ETH", "BNB"}
	for _, sym := range symbols {
		if sym == "" {
			continue
		}
		if i := strings.Index(sym, "/"); i >= 0 {
			out[strings.ToUpper(sym[:i])] = struct{}{}
			out[strings.ToUpper(sym[i+1:])] = struct{}{}
			continue
		}
		up := strings.ToUpper(sym)
		matched := false
		for _, q := range commonQuotes {
			if strings.HasSuffix(up, q) && len(up) > len(q) {
				out[up[:len(up)-len(q)]] = struct{}{}
				out[q] = struct{}{}
				matched = true
				break
			}
		}
		if !matched {
			// Unknown format — keep whole symbol so the operator at least
			// sees something; better than dropping the snapshot entirely.
			out[up] = struct{}{}
		}
	}
	return out
}

// filterAccountBalances returns a shallow copy of the account with only the
// balances whose asset is in `keep`. If keep is nil/empty, returns the
// original (no filtering).
func filterAccountBalances(account *types.Account, keep map[string]struct{}) *types.Account {
	if account == nil || len(keep) == 0 {
		return account
	}
	out := &types.Account{
		Exchange: account.Exchange,
		Balances: make([]types.Balance, 0, len(keep)),
	}
	for _, b := range account.Balances {
		if _, ok := keep[strings.ToUpper(b.Asset)]; ok {
			out.Balances = append(out.Balances, b)
		}
	}
	return out
}

// heartbeatLoop fires a heartbeat every 5s with the latest counters. Exits
// on ctx.Done — Start's defer-stopped runs after this.
func (s *SDK) heartbeatLoop(ctx context.Context, startedAt time.Time) {
	const heartbeatInterval = 5 * time.Second
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lastID := ""
			if v := s.lastOrderID.Load(); v != nil {
				lastID, _ = v.(string)
			}
			s.publisher.PublishHeartbeat(telemetry.HeartbeatMeta{
				UptimeSeconds:    int64(time.Since(startedAt).Seconds()),
				CandlesProcessed: s.candlesProcessed.Load(),
				OrdersPlaced:     int(s.ordersPlaced.Load()),
				LastOrderID:      lastID,
			})
		}
	}
}

// balanceFallbackLoop refreshes the current-balance snapshot on a slow timer
// for sessions that don't trade often. The per-fill trigger covers the
// active case.
func (s *SDK) balanceFallbackLoop(ctx context.Context) {
	if s.config.Live == nil {
		return
	}
	const interval = 60 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, exch := range s.config.Live.RequestedExchanges {
				s.publishCurrentBalanceFor(ctx, exch, "")
			}
		}
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
