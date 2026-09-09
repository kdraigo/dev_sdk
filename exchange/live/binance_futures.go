package live

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/common"
	"github.com/adshao/go-binance/v2/futures"
	"github.com/kdraigo/dev_sdk/aggregator"
	"github.com/kdraigo/dev_sdk/types"
)

// BinanceFuturesClient trades Binance USDⓈ-M perpetuals.
//
// It is a separate adapter from BinanceClient rather than a mode of it, and it
// imports only go-binance's futures package. The user's Binance account holds
// real funds and open orders on *spot*; the futures wallet is a separate
// balance, and keeping the two adapters in separate files with disjoint imports
// is what makes "the futures path cannot reach spot" checkable rather than a
// claim. TestNoSpotClientInFuturesAdapter enforces it.
//
// Scope matches the backtest engine exactly: isolated margin, one-way
// positions, linear USDT-margined. Anything else is refused at PrepareSession
// rather than half-supported, because a strategy validated against the
// simulator would otherwise run under semantics the simulator never modelled.
type BinanceFuturesClient struct {
	config *types.Config
	client *futures.Client
	guard  Guard

	filters map[string]InstrumentFilter

	// marks caches the latest mark price per symbol from the market stream. It
	// values market orders for the notional cap, which otherwise could not be
	// enforced on the one order type that has no price of its own.
	mu    sync.RWMutex
	marks map[string]float64

	// brackets maps an order id to the id of the leg that must be cancelled
	// when it fills. Binance does not link the legs of a bracket on futures the
	// way the backtest engine does, so the adapter does it.
	brackets map[string]string

	stopOnce sync.Once
	stopped  chan struct{}
}

var (
	_ interface {
		SetLeverage(ctx context.Context, exchange, pair string, leverage float64) error
	} = (*BinanceFuturesClient)(nil)
	_ interface {
		GetPositions(ctx context.Context, exchange string) ([]*types.Position, error)
	} = (*BinanceFuturesClient)(nil)
)

func NewBinanceFuturesClient(cfg *types.Config) *BinanceFuturesClient {
	return &BinanceFuturesClient{
		config:   cfg,
		guard:    Guard{Env: cfg.Environment, Opts: cfg.Live},
		filters:  make(map[string]InstrumentFilter),
		marks:    make(map[string]float64),
		brackets: make(map[string]string),
		stopped:  make(chan struct{}),
	}
}

// newFuturesClient builds the futures client, reusing the spot adapter's
// credential-shape check so an Ed25519 key that works on one venue works on the
// other.
func newFuturesClient(creds types.Credentials) *futures.Client {
	c := futures.NewClient(creds.APIKey, creds.APISecret)
	if isEd25519Secret(creds.APISecret) {
		c.KeyType = common.KeyTypeEd25519
	}
	return c
}

func (b *BinanceFuturesClient) assets() []string {
	if b.config == nil || b.config.Live == nil {
		return nil
	}
	return b.config.Live.Assets
}

// ── session ──────────────────────────────────────────────────────────────────

func (b *BinanceFuturesClient) PrepareSession(ctx context.Context, cfg *types.Config) error {
	b.config = cfg
	b.guard = Guard{Env: cfg.Environment, Opts: cfg.Live}

	// Refuse an environment this adapter is not for, rather than trading
	// futures under a config written for something else. client.go only routes
	// futures environments here, but the adapter is exported and a direct
	// caller must not be able to slip past the gates by naming a value the
	// guard treats as out of scope.
	if !IsFutures(cfg.Environment) {
		return fmt.Errorf("binance futures adapter cannot run environment %q; use %s or %s",
			cfg.Environment, types.EnvTestBinanceFutures, types.EnvRealBinanceFutures)
	}

	// The arming configuration is validated before anything else. A run that
	// cannot legally place an order must fail now, not hours later at the first
	// signal — by which point the strategy has already sized a position it
	// turns out it cannot open.
	if err := b.guard.CheckSession(); err != nil {
		return err
	}

	// futures.UseTestnet is a package-level global, so it is set exactly once,
	// here, and never toggled again. Flipping it mid-run would silently
	// redirect subsequent requests to the other network.
	if cfg.Environment == types.EnvTestBinanceFutures {
		futures.UseTestnet = true
		log.Println("Binance futures: TESTNET")
	} else {
		log.Printf("Binance futures: MAINNET, armed=%v dry_run=%v max_notional=%v max_leverage=%v",
			cfg.Live.Armed, cfg.Live.DryRun, cfg.Live.MaxOrderNotional, cfg.Live.MaxLeverage)
	}

	b.client = newFuturesClient(cfg.Credentials)

	if _, err := b.client.NewSetServerTimeService().Do(ctx); err != nil {
		log.Printf("Binance futures: server time sync failed: %v", err)
	}
	if err := b.client.NewPingService().Do(ctx); err != nil {
		return fmt.Errorf("binance futures connection failed: %w", err)
	}

	if err := b.loadFilters(ctx); err != nil {
		return fmt.Errorf("binance futures: %w", err)
	}
	if err := b.assertAccountShape(ctx); err != nil {
		return err
	}
	if err := b.applyConfiguredLeverage(ctx); err != nil {
		return err
	}
	return nil
}

// loadFilters reads each instrument's own trading rules. Unlike on spot this is
// fatal: a futures order rejected for precision after a signal has fired leaves
// the strategy believing it has exposure it does not have.
func (b *BinanceFuturesClient) loadFilters(ctx context.Context) error {
	assets := b.assets()
	if len(assets) == 0 {
		return nil
	}

	info, err := b.client.NewExchangeInfoService().Do(ctx)
	if err != nil {
		return fmt.Errorf("exchangeInfo: %w", err)
	}

	want := make(map[string]struct{}, len(assets))
	for _, a := range assets {
		want[VenueSymbol(a)] = struct{}{}
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
		if pf := sym.PriceFilter(); pf != nil {
			f.TickSize = parseFloat(pf.TickSize)
		}
		if mn := sym.MinNotionalFilter(); mn != nil {
			f.MinNotional = parseFloat(mn.Notional)
		}
		b.filters[sym.Symbol] = f
		delete(want, sym.Symbol)
		log.Printf("Binance futures: %s tick=%v step=%v minQty=%v minNotional=%v",
			sym.Symbol, f.TickSize, f.StepSize, f.MinQty, f.MinNotional)
	}

	if len(want) > 0 {
		missing := make([]string, 0, len(want))
		for s := range want {
			missing = append(missing, s)
		}
		return fmt.Errorf("no perpetual contract on Binance futures for %s", strings.Join(missing, ", "))
	}
	return nil
}

// assertAccountShape refuses an account configured differently from what the
// backtest engine models.
//
// This is not pedantry. Under cross margin a losing position draws on the whole
// wallet, so the isolated-margin liquidation price the simulator computed —
// and every position size derived from it — is simply wrong. Under hedge mode
// a reduce-only sell does not reduce a long. Both would run, produce numbers,
// and mean something other than the backtest that justified the strategy.
func (b *BinanceFuturesClient) assertAccountShape(ctx context.Context) error {
	mode, err := b.client.NewGetPositionModeService().Do(ctx)
	if err != nil {
		return fmt.Errorf("binance futures: reading position mode: %w", err)
	}
	if mode.DualSidePosition {
		return fmt.Errorf("binance futures account is in hedge mode; this SDK models one-way positions only — switch the account to one-way, or run a backtest configured the same way first")
	}

	risks, err := b.client.NewGetPositionRiskService().Do(ctx)
	if err != nil {
		return fmt.Errorf("binance futures: reading position risk: %w", err)
	}
	want := make(map[string]struct{}, len(b.assets()))
	for _, a := range b.assets() {
		want[VenueSymbol(a)] = struct{}{}
	}
	for _, r := range risks {
		if _, ok := want[r.Symbol]; !ok {
			continue
		}
		if !strings.EqualFold(r.MarginType, "isolated") {
			// A dry run must not change the account. Skipping the write keeps
			// that promise, and saying so keeps the run honest about what a
			// live one would additionally do.
			if b.guard.DryRun() {
				log.Printf("Binance futures DRY RUN: would switch %s from %s to isolated margin", r.Symbol, r.MarginType)
				continue
			}
			// Switching margin type is only possible with no position and no
			// open order on the symbol, so try it and report honestly if not.
			if err := b.client.NewChangeMarginTypeService().
				Symbol(r.Symbol).MarginType(futures.MarginTypeIsolated).Do(ctx); err != nil {
				return fmt.Errorf("binance futures %s: switching %s margin to isolated: %w",
					r.Symbol, r.MarginType, explainPermissionError(err, "changeMarginType"))
			}
			log.Printf("Binance futures: %s switched from %s to isolated margin", r.Symbol, r.MarginType)
		}
	}
	return nil
}

// applyConfiguredLeverage sets the starting leverage per pair.
//
// A pair absent from LiveOptions.Leverage is left alone rather than reset to
// 1x: the account's existing setting is a deliberate choice by whoever made it,
// and silently overriding it would resize every position the strategy opens.
func (b *BinanceFuturesClient) applyConfiguredLeverage(ctx context.Context) error {
	if b.config.Live == nil {
		return nil
	}
	for pair, lev := range b.config.Live.Leverage {
		sym := VenueSymbol(pair)
		if b.guard.DryRun() {
			log.Printf("Binance futures DRY RUN: would set %s leverage to %vx", sym, lev)
			continue
		}
		if _, err := b.client.NewChangeLeverageService().
			Symbol(sym).Leverage(int(lev)).Do(ctx); err != nil {
			return fmt.Errorf("binance futures: setting %vx leverage on %s: %w",
				lev, sym, explainPermissionError(err, "changeLeverage"))
		}
		log.Printf("Binance futures: %s leverage set to %vx", sym, lev)
	}
	return nil
}

// ── orders ───────────────────────────────────────────────────────────────────

// binanceFuturesOrderType translates a venue-neutral intent into Binance
// futures' vocabulary.
//
// go-binance declares only LIMIT, MARKET and LIQUIDATION as OrderType
// constants, which does not mean the venue lacks the rest: CreateOrderService
// passes the type through unvalidated, so the string literals below are the
// correct and only way to reach STOP_MARKET and friends. Concluding otherwise
// and falling back to MARKET is exactly the defect this file was written to
// avoid repeating.
func binanceFuturesOrderType(t IntentType) (futures.OrderType, error) {
	switch t {
	case IntentMarket:
		return futures.OrderTypeMarket, nil
	case IntentLimit, IntentTakeProfitLimit:
		return futures.OrderTypeLimit, nil
	case IntentStopMarket:
		return futures.OrderType("STOP_MARKET"), nil
	case IntentStopLimit:
		return futures.OrderType("STOP"), nil
	default:
		return "", fmt.Errorf("binance futures: no order type for intent %s", t)
	}
}

func (b *BinanceFuturesClient) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	intent, err := MapOrder(req, MarketLinearPerp)
	if err != nil {
		return nil, fmt.Errorf("binance futures: %w", err)
	}
	return b.place(ctx, req, intent, "")
}

// place is the single path every order takes, bracket legs included, so the
// gates and the rounding cannot be bypassed by adding another entry point.
func (b *BinanceFuturesClient) place(ctx context.Context, req *types.OrderRequest, intent *OrderIntent, groupID string) (*types.Order, error) {
	sym := intent.Symbol
	filter := b.filters[sym]
	if err := filter.Apply(intent, b.markFor(sym)); err != nil {
		return nil, fmt.Errorf("binance futures %s: %w", sym, err)
	}
	if err := b.guard.CheckOrder(intent, b.markFor(sym)); err != nil {
		return nil, err
	}

	orderType, err := binanceFuturesOrderType(intent.Type)
	if err != nil {
		return nil, err
	}

	side := futures.SideTypeBuy
	if intent.Side == types.OrderSideSell {
		side = futures.SideTypeSell
	}

	srv := b.client.NewCreateOrderService().
		Symbol(sym).
		Side(side).
		Type(orderType).
		Quantity(filter.FormatQty(intent.Quantity)).
		// RESULT, not Binance's default ACK.
		//
		// ACK returns status=NEW with avgPrice=0 even for a market order that
		// filled instantly, while the backtest engine returns the order
		// FILLED with its price. A strategy branching on
		// `order.Status == FILLED` would therefore work in backtest and
		// silently never fire live — the same code reading two different
		// truths. Measured on a mainnet round trip: ACK said NEW/0, Binance
		// had recorded FILLED at 104.32.
		NewOrderResponseType(futures.NewOrderRespTypeRESULT).
		// One-way mode: BOTH is the only valid position side, and stating it
		// makes a hedge-mode account fail loudly instead of opening a leg.
		PositionSide(futures.PositionSideTypeBoth)

	if intent.Price > 0 {
		srv = srv.Price(filter.FormatPrice(intent.Price))
	}
	if intent.StopPrice > 0 {
		srv = srv.StopPrice(filter.FormatPrice(intent.StopPrice)).
			// Trigger on the mark, which is what the backtest engine
			// liquidates and stops on by default (MarkPriceSource
			// "mark_series"). Measured on production data the mark and the
			// trade price diverge by up to 1.3% at the bar low, so triggering
			// on the contract price here would mean live and backtest
			// genuinely disagree about when a stop fires.
			WorkingType(futures.WorkingTypeMarkPrice)
	}
	if intent.ReduceOnly {
		srv = srv.ReduceOnly(true)
	}
	if intent.TimeInForce != "" && orderType == futures.OrderTypeLimit {
		srv = srv.TimeInForce(futures.TimeInForceType(intent.TimeInForce))
	}

	if b.guard.DryRun() {
		return b.dryRunAck(req, intent, groupID), nil
	}

	res, err := srv.Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("binance futures %s %s %s: %w", sym, intent.Side, intent.Type, explainOrderError(err, intent))
	}

	order := &types.Order{
		ID:       strconv.FormatInt(res.OrderID, 10),
		Symbol:   req.Symbol,
		Exchange: types.ExchangeBinanceFutures,
		Side:     req.Side,
		// The strategy's stated type, not the wire type, so intent survives
		// into the order log.
		Type:         req.Type,
		Status:       mapFuturesStatus(res.Status),
		Price:        parseFloat(res.Price),
		Quantity:     parseFloat(res.OrigQuantity),
		FilledQty:    parseFloat(res.ExecutedQuantity),
		AveragePrice: parseFloat(res.AvgPrice),
		StopPrice:    intent.StopPrice,
		GroupID:      groupID,
		CreatedAt:    time.UnixMilli(res.UpdateTime),
		UpdatedAt:    time.UnixMilli(res.UpdateTime),
	}
	return order, nil
}

// dryRunAck logs what would have been sent and returns a synthetic
// acknowledgement, so a strategy can be exercised against real market data
// without the account seeing anything.
func (b *BinanceFuturesClient) dryRunAck(req *types.OrderRequest, intent *OrderIntent, groupID string) *types.Order {
	log.Printf("Binance futures DRY RUN: %s %s %v %s type=%s price=%v stop=%v reduceOnly=%v",
		intent.Side, intent.Symbol, intent.Quantity, intent.TimeInForce,
		intent.Type, intent.Price, intent.StopPrice, intent.ReduceOnly)
	now := time.Now()
	return &types.Order{
		ID:        fmt.Sprintf("dryrun-%d", now.UnixNano()),
		Symbol:    req.Symbol,
		Exchange:  types.ExchangeBinanceFutures,
		Side:      req.Side,
		Type:      req.Type,
		Status:    types.OrderStatusNew,
		Price:     intent.Price,
		Quantity:  intent.Quantity,
		StopPrice: intent.StopPrice,
		GroupID:   groupID,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func (b *BinanceFuturesClient) CancelOrder(ctx context.Context, exchange, symbol, id string) error {
	if b.guard.DryRun() {
		log.Printf("Binance futures DRY RUN: cancel %s on %s", id, symbol)
		return nil
	}
	orderID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return fmt.Errorf("binance futures CancelOrder: invalid orderID %q: %w", id, err)
	}
	_, err = b.client.NewCancelOrderService().
		Symbol(VenueSymbol(symbol)).OrderID(orderID).Do(ctx)
	if err != nil {
		return err
	}
	b.forgetBracket(id)
	return nil
}

// PlaceBracket places a take-profit and a protective stop as a pair.
//
// Binance does not link them the way the backtest engine's OCO does, so the
// adapter cancels the survivor itself when the first leg fills (see
// handleOrderUpdate). Both legs are reduce-only, and on futures they reserve
// nothing — the margin is already posted against the position — so both can
// rest at once. That is a real difference from the spot wallet, where the
// second leg's reservation is what prevented two independent take-profits.
func (b *BinanceFuturesClient) PlaceBracket(ctx context.Context, req *types.BracketRequest) ([]*types.Order, error) {
	if req == nil {
		return nil, fmt.Errorf("bracket request must not be nil")
	}
	if req.TakeProfitPrice <= 0 || req.StopPrice <= 0 {
		return nil, fmt.Errorf("bracket requires a positive TakeProfitPrice and StopPrice")
	}

	groupID := fmt.Sprintf("bracket-%d", time.Now().UnixNano())

	tpReq := &types.OrderRequest{
		Symbol: req.Symbol, Exchange: req.Exchange, Side: req.Side, Quantity: req.Quantity,
		Type: types.OrderTypeTakeProfitLimit, Price: req.TakeProfitPrice, ReduceOnly: true,
	}
	slPrice := req.StopLimitPrice
	slType := types.OrderTypeStopLossLimit
	if slPrice <= 0 {
		// No limit price given: a stop that must fill beats a stop that might,
		// so it becomes market-on-trigger rather than resting at the trigger.
		slType = types.OrderTypeStopLoss
	}
	slReq := &types.OrderRequest{
		Symbol: req.Symbol, Exchange: req.Exchange, Side: req.Side, Quantity: req.Quantity,
		Type: slType, Price: slPrice, StopPrice: req.StopPrice, ReduceOnly: true,
	}

	tpIntent, err := MapOrder(tpReq, MarketLinearPerp)
	if err != nil {
		return nil, fmt.Errorf("binance futures bracket take-profit: %w", err)
	}
	slIntent, err := MapOrder(slReq, MarketLinearPerp)
	if err != nil {
		return nil, fmt.Errorf("binance futures bracket stop: %w", err)
	}

	tp, err := b.place(ctx, tpReq, tpIntent, groupID)
	if err != nil {
		return nil, err
	}
	sl, err := b.place(ctx, slReq, slIntent, groupID)
	if err != nil {
		// Leaving a naked take-profit resting would be worse than failing:
		// the position would have an exit but no protection, which is the
		// opposite of what a bracket is for.
		if cancelErr := b.CancelOrder(ctx, req.Exchange, req.Symbol, tp.ID); cancelErr != nil {
			log.Printf("Binance futures: stop leg failed AND its take-profit %s could not be cancelled: %v", tp.ID, cancelErr)
		}
		return nil, fmt.Errorf("binance futures bracket stop leg: %w", err)
	}

	b.mu.Lock()
	b.brackets[tp.ID] = sl.ID
	b.brackets[sl.ID] = tp.ID
	b.mu.Unlock()

	return []*types.Order{tp, sl}, nil
}

// ── account and positions ────────────────────────────────────────────────────

func (b *BinanceFuturesClient) GetAccount(ctx context.Context, exchange string, asset string) (*types.Account, error) {
	res, err := b.client.NewGetAccountV3Service().Do(ctx)
	if err != nil {
		return nil, err
	}
	acc := &types.Account{Exchange: types.ExchangeBinanceFutures}
	for _, a := range res.Assets {
		if asset != "" && a.Asset != asset {
			continue
		}
		wallet := parseFloat(a.WalletBalance)
		available := parseFloat(a.AvailableBalance)
		if wallet == 0 && available == 0 {
			continue
		}
		acc.Balances = append(acc.Balances, types.Balance{
			Asset: a.Asset,
			Free:  available,
			// What is not available is posted as margin. Reporting it as
			// locked keeps the futures account readable through the same
			// Balance shape as a spot one.
			Lock: wallet - available,
		})
	}
	return acc, nil
}

// GetPositions reads open positions from the exchange rather than from local
// state. Local state can be stale in ways the backtest never is: a liquidation,
// an ADL, or a fill delivered while the user data stream was disconnected.
func (b *BinanceFuturesClient) GetPositions(ctx context.Context, exchange string) ([]*types.Position, error) {
	if exchange != "" && exchange != types.ExchangeBinanceFutures {
		return nil, nil
	}
	// v2 rather than v3 deliberately: v3 dropped marginType and leverage, and
	// types.Position carries leverage.
	risks, err := b.client.NewGetPositionRiskService().Do(ctx)
	if err != nil {
		return nil, err
	}

	var out []*types.Position
	for _, r := range risks {
		amt := parseFloat(r.PositionAmt)
		if amt == 0 {
			// Flat is the absence of a position, not a zero-size one.
			continue
		}
		side := "LONG"
		if amt < 0 {
			side = "SHORT"
			amt = -amt
		}
		out = append(out, &types.Position{
			Symbol:         r.Symbol,
			Exchange:       types.ExchangeBinanceFutures,
			Side:           side,
			Size:           amt,
			EntryPrice:     parseFloat(r.EntryPrice),
			Leverage:       parseFloat(r.Leverage),
			IsolatedMargin: parseFloat(r.IsolatedMargin),
			MarkPrice:      parseFloat(r.MarkPrice),
			UnrealizedPnL:  parseFloat(r.UnRealizedProfit),
			// RealizedPnL and FundingPaid are not carried by positionRisk.
			// They are per-position accumulations the venue does not track,
			// and inventing them from income history would attribute payments
			// to the wrong position lifetime after a flip. Left at zero rather
			// than filled with a plausible wrong number.
		})
	}
	return out, nil
}

// SetLeverage changes the leverage used for new positions on a pair.
//
// Refused while a position is open, matching the backtest engine: re-levering
// an open position rewrites its liquidation price, and a strategy that believes
// it did something the venue silently ignored sizes everything after it wrong.
func (b *BinanceFuturesClient) SetLeverage(ctx context.Context, exchange, pair string, leverage float64) error {
	if leverage <= 0 {
		return fmt.Errorf("leverage must be positive, got %v", leverage)
	}
	if b.config.Live != nil && b.config.Live.MaxLeverage > 0 && leverage > b.config.Live.MaxLeverage {
		return &ErrNotArmed{
			Gate:   "cap",
			Detail: fmt.Sprintf("requested %vx on %s exceeds MaxLeverage %vx", leverage, pair, b.config.Live.MaxLeverage),
		}
	}

	sym := VenueSymbol(pair)
	positions, err := b.GetPositions(ctx, types.ExchangeBinanceFutures)
	if err != nil {
		return fmt.Errorf("binance futures: checking for an open position before re-levering: %w", err)
	}
	for _, p := range positions {
		if p.Symbol == sym {
			return fmt.Errorf("binance futures: refusing to change leverage on %s while a %s position of %v is open", sym, p.Side, p.Size)
		}
	}

	if b.guard.DryRun() {
		log.Printf("Binance futures DRY RUN: set %s leverage to %vx", sym, leverage)
		return nil
	}
	_, err = b.client.NewChangeLeverageService().Symbol(sym).Leverage(int(leverage)).Do(ctx)
	return err
}

func (b *BinanceFuturesClient) Next(ctx context.Context) error { return nil }

// ── history ──────────────────────────────────────────────────────────────────

// GetHistoricalCandles pages forward through the futures kline REST API,
// returning closed candles in [from, to] oldest-first.
func (b *BinanceFuturesClient) GetHistoricalCandles(ctx context.Context, exchange, symbol string, from, to time.Time, tf types.Timeframe) ([]*types.Candle, error) {
	if b.client == nil {
		if b.config != nil && b.config.Environment == types.EnvTestBinanceFutures {
			futures.UseTestnet = true
		}
		b.client = newFuturesClient(b.config.Credentials)
	}
	if from.After(to) {
		return nil, fmt.Errorf("binance futures: from %s is after to %s", from, to)
	}
	interval, err := binanceInterval(tf)
	if err != nil {
		return nil, err
	}

	sym := VenueSymbol(symbol)
	durationMs := aggregator.ExtractDuration(tf).Milliseconds()
	toMs := to.UnixMilli()
	cursor := from.UnixMilli()

	const pageLimit = 1000
	var all []*types.Candle

	for cursor <= toMs {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		page, err := b.client.NewKlinesService().
			Symbol(sym).Interval(interval).
			StartTime(cursor).EndTime(toMs).Limit(pageLimit).Do(ctx)
		if err != nil {
			return nil, fmt.Errorf("binance futures kline fetch: %w", err)
		}
		if len(page) == 0 {
			break
		}

		var lastOpenMs int64
		for _, k := range page {
			all = append(all, &types.Candle{
				Symbol:              symbol,
				Exchange:            types.ExchangeBinanceFutures,
				Timeframe:           tf,
				OpenTime:            time.UnixMilli(k.OpenTime),
				CloseTime:           time.UnixMilli(k.OpenTime + durationMs),
				Open:                parseFloat(k.Open),
				High:                parseFloat(k.High),
				Low:                 parseFloat(k.Low),
				Close:               parseFloat(k.Close),
				Volume:              parseFloat(k.Volume),
				IsComplete:          true,
				TradeCount:          k.TradeNum,
				QuoteVolume:         parseFloat(k.QuoteAssetVolume),
				TakerBuyBaseVolume:  parseFloat(k.TakerBuyBaseAssetVolume),
				TakerBuyQuoteVolume: parseFloat(k.TakerBuyQuoteAssetVolume),
			})
			if k.OpenTime > lastOpenMs {
				lastOpenMs = k.OpenTime
			}
		}

		if len(page) < pageLimit {
			break
		}
		cursor = lastOpenMs + 1
	}
	return all, nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

func (b *BinanceFuturesClient) markFor(symbol string) float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.marks[symbol]
}

func (b *BinanceFuturesClient) setMark(symbol string, price float64) {
	b.mu.Lock()
	b.marks[symbol] = price
	b.mu.Unlock()
}

func (b *BinanceFuturesClient) forgetBracket(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if sibling, ok := b.brackets[id]; ok {
		delete(b.brackets, sibling)
		delete(b.brackets, id)
	}
}

// ErrConditionalOrdersUnavailable reports that the venue refused a triggered
// order type outright. Callers can test for it and fall back deliberately —
// polling the mark and closing at market, say — rather than discovering the
// gap the first time a stop is needed.
//
// Measured on Binance futures TESTNET (2026-09-01): every conditional type —
// STOP, STOP_MARKET, TAKE_PROFIT, TAKE_PROFIT_MARKET — is refused with -4120
// on /fapi/v1/order, while LIMIT and MARKET on the same endpoint and the same
// credentials succeed. The instrument's own exchangeInfo advertises all of
// them, so exchangeInfo cannot be used to detect this ahead of time; the only
// honest signal is the rejection itself.
var ErrConditionalOrdersUnavailable = errors.New("this Binance futures environment does not accept conditional (stop / take-profit) orders on /fapi/v1/order")

// ErrFuturesPermissionDenied reports that the key reaches Binance but is not
// allowed to act. It is separated from every other failure because the fix is
// entirely different: nothing about the code, the account or the order is
// wrong, and a message about margin modes or order types would send the reader
// looking in the wrong place.
var ErrFuturesPermissionDenied = errors.New("the API key can read Binance futures but not trade it")

// binanceCodePermissionDenied is -2015. Binance returns it for a key without
// the Futures permission, for an IP outside the key's allow-list, and for a
// key that is simply wrong, so the text below names all three rather than
// guessing which one applies.
const binanceCodePermissionDenied = -2015

// explainPermissionError turns -2015 into something actionable. Read endpoints
// on /fapi need only "Enable Reading", so a key can list balances and positions
// perfectly and still be refused on every write — which reads as a bug in the
// caller until you know it is a checkbox.
func explainPermissionError(err error, action string) error {
	var apiErr *common.APIError
	if errors.As(err, &apiErr) && apiErr.Code == binanceCodePermissionDenied {
		return fmt.Errorf("%w: %s was refused with -2015. Read endpoints succeed because they need only \"Enable Reading\"; writes need \"Enable Futures\" on the API key. Check, in order: the key has Enable Futures ticked, this host's IP is in the key's allow-list, and the key belongs to the account holding the futures wallet", ErrFuturesPermissionDenied, action)
	}
	return err
}

// binanceCodeConditionalUnsupported is Binance's -4120, whose own message
// ("Please use the Algo Order API endpoints instead") describes a TWAP/VP
// service that does not place protective stops.
const binanceCodeConditionalUnsupported = -4120

// explainOrderError replaces a venue error that would send the reader in the
// wrong direction with one that says what actually happened.
func explainOrderError(err error, intent *OrderIntent) error {
	var apiErr *common.APIError
	if errors.As(err, &apiErr) && apiErr.Code == binanceCodePermissionDenied {
		return explainPermissionError(err, "placing an order")
	}
	if errors.As(err, &apiErr) && apiErr.Code == binanceCodeConditionalUnsupported && intent.Type.Triggered() {
		return fmt.Errorf("%w (venue said: %s). The order was NOT placed and was NOT converted to a market order: a protective stop that executes immediately is worse than one that fails loudly", ErrConditionalOrdersUnavailable, apiErr.Message)
	}
	return err
}

func mapFuturesStatus(s futures.OrderStatusType) types.OrderStatus {
	switch s {
	case futures.OrderStatusTypeFilled:
		return types.OrderStatusFilled
	case futures.OrderStatusTypePartiallyFilled:
		return types.OrderStatusPartiallyFilled
	case futures.OrderStatusTypeCanceled, futures.OrderStatusTypeExpired:
		return types.OrderStatusCanceled
	case futures.OrderStatusTypeRejected:
		return types.OrderStatusRejected
	default:
		return types.OrderStatusNew
	}
}

// sdkOrderType recovers the SDK's order type from what the venue reports, so a
// fill arriving on the user data stream is described the same way the
// PlaceOrder that created it was.
func sdkOrderType(t string, hasStop bool) types.OrderType {
	switch strings.ToUpper(t) {
	case "MARKET":
		return types.OrderTypeMarket
	case "LIMIT":
		return types.OrderTypeLimit
	case "STOP_MARKET", "TAKE_PROFIT_MARKET":
		return types.OrderTypeStopLoss
	case "STOP":
		return types.OrderTypeStopLossLimit
	case "TAKE_PROFIT":
		return types.OrderTypeTakeProfitLimit
	case "LIQUIDATION":
		// A forced close is a market order the venue placed on the strategy's
		// behalf. The backtest engine models it the same way.
		return types.OrderTypeMarket
	default:
		if hasStop {
			return types.OrderTypeStopLoss
		}
		return types.OrderTypeMarket
	}
}

// OrderFeed: Binance futures pushes order updates over the user data stream,
// so fills, stop triggers and liquidations arrive unprompted in about a
// second.
//
// PollEvery is nevertheless set, as a slow safety net rather than the primary
// feed. A websocket that drops takes its undelivered events with it: an order
// that filled during the gap is simply never mentioned again, and the strategy
// waits forever for a fill that already happened. The push path cannot detect
// its own silence, so something has to ask. Dedup in the SDK's reconciler
// means the two sources cost nothing when they agree, which is almost always.
func (b *BinanceFuturesClient) OrderFeed() types.OrderFeed {
	return types.OrderFeed{Push: true, PollEvery: 30 * time.Second, Latency: time.Second}
}

// ListOpenOrders implements the read half of the SDK's order reconciliation.
func (b *BinanceFuturesClient) ListOpenOrders(ctx context.Context, exchange, symbol string) ([]*types.Order, error) {
	res, err := b.client.NewListOpenOrdersService().Symbol(VenueSymbol(symbol)).Do(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*types.Order, 0, len(res))
	for _, o := range res {
		out = append(out, mapFuturesOrder(o, symbol))
	}
	return out, nil
}

// GetOrder resolves what became of one order.
//
// An order missing from the open list is FILLED or CANCELED, and the list
// cannot say which. Assuming a fill would report a position the account does
// not hold; assuming a cancel would hide one it does.
func (b *BinanceFuturesClient) GetOrder(ctx context.Context, exchange, symbol, id string) (*types.Order, error) {
	orderID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("binance futures GetOrder: invalid orderID %q: %w", id, err)
	}
	o, err := b.client.NewGetOrderService().Symbol(VenueSymbol(symbol)).OrderID(orderID).Do(ctx)
	if err != nil {
		return nil, err
	}
	return mapFuturesOrder(o, symbol), nil
}

// mapFuturesOrder converts a venue order record into the SDK shape. Shared by
// both reads so they cannot drift apart.
func mapFuturesOrder(o *futures.Order, symbol string) *types.Order {
	side := types.OrderSideBuy
	if o.Side == futures.SideTypeSell {
		side = types.OrderSideSell
	}
	declared := string(o.OrigType)
	if declared == "" {
		declared = string(o.Type)
	}
	stop := parseFloat(o.StopPrice)
	return &types.Order{
		ID:           strconv.FormatInt(o.OrderID, 10),
		Symbol:       symbol,
		Exchange:     types.ExchangeBinanceFutures,
		Side:         side,
		Type:         sdkOrderType(declared, stop > 0),
		Status:       mapFuturesStatus(o.Status),
		Price:        parseFloat(o.Price),
		Quantity:     parseFloat(o.OrigQuantity),
		FilledQty:    parseFloat(o.ExecutedQuantity),
		AveragePrice: parseFloat(o.AvgPrice),
		StopPrice:    stop,
		CreatedAt:    time.UnixMilli(o.Time),
		UpdatedAt:    time.UnixMilli(o.UpdateTime),
	}
}
