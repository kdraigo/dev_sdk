package live

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/adshao/go-binance/v2/futures"
	"github.com/kdraigo/dev_sdk/types"
)

// Binance futures needs two websocket connections, and the spot adapter only
// ever opened the first of them:
//
//  1. the market stream, which carries klines, and
//  2. the user data stream, which carries the strategy's own fills.
//
// Without the second, orderChan never fires and a strategy never learns that
// its order filled, that a stop triggered, or that the position was liquidated.
// On spot that is merely bad. On perpetuals it is the difference between a
// strategy that knows it has been liquidated and one that keeps trading against
// a position the exchange already closed.
//
// The user data stream is also the one with a lifecycle: it is addressed by a
// listenKey that expires 60 minutes after it is issued unless it is renewed.
// A missed renewal does not error — the socket simply stops delivering — which
// is the same silent failure shape as every defect this platform has hit.

const (
	// listenKeyRenewInterval is deliberately half of Binance's 60-minute
	// expiry. One missed renewal then still leaves a full period to recover in,
	// rather than ending the stream.
	listenKeyRenewInterval = 30 * time.Minute

	// reconnectBase and reconnectMax bound the backoff between reconnects.
	reconnectBase = 1 * time.Second
	reconnectMax  = 60 * time.Second
)

// ConnectStream opens both connections and keeps them open for the life of ctx.
func (b *BinanceFuturesClient) ConnectStream(ctx context.Context, candleChan chan<- *types.Candle, orderChan chan<- *types.Order) error {
	symbols := make([]string, 0, len(b.assets()))
	original := make(map[string]string, len(b.assets()))
	for _, a := range b.assets() {
		sym := VenueSymbol(a)
		symbols = append(symbols, sym)
		original[sym] = a
	}
	if len(symbols) == 0 {
		log.Println("Binance futures: no assets configured, no streams opened")
		return nil
	}

	go b.superviseMarketStream(ctx, symbols, original, candleChan)
	go b.superviseMarkStream(ctx, symbols)
	go b.superviseUserDataStream(ctx, original, orderChan)

	return nil
}

// Stop releases the supervisors. Idempotent; ConnectStream's context ending
// does the same thing, and this exists for callers that hold no context.
func (b *BinanceFuturesClient) Stop() {
	b.stopOnce.Do(func() { close(b.stopped) })
}

// runWithBackoff restarts connect whenever it returns, until ctx ends. Every
// websocket here is long-lived and every one of them will drop eventually;
// treating a drop as terminal is how a strategy ends up silently blind.
func (b *BinanceFuturesClient) runWithBackoff(ctx context.Context, name string, connect func() (doneC chan struct{}, stopC chan struct{}, err error), onReconnect func()) {
	backoff := reconnectBase
	first := true

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopped:
			return
		default:
		}

		doneC, stopC, err := connect()
		if err != nil {
			log.Printf("Binance futures: %s connect failed, retrying in %s: %v", name, backoff, err)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}

		if !first && onReconnect != nil {
			// Fills that landed while the socket was down were never
			// delivered, so local memory is not authoritative any more. Read
			// the truth back from the exchange rather than carrying on.
			onReconnect()
		}
		first = false
		backoff = reconnectBase
		log.Printf("Binance futures: %s stream connected", name)

		select {
		case <-doneC:
			log.Printf("Binance futures: %s stream closed, reconnecting", name)
		case <-ctx.Done():
			close(stopC)
			return
		case <-b.stopped:
			close(stopC)
			return
		}

		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > reconnectMax {
		return reconnectMax
	}
	return d
}

// sleepCtx waits for d, reporting false if the context ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// ── market stream ────────────────────────────────────────────────────────────

// superviseMarketStream carries klines. One combined connection for every
// symbol, rather than the per-symbol goroutine the spot adapter opens: Binance
// counts connections, and a strategy on ten symbols should not need ten of them.
func (b *BinanceFuturesClient) superviseMarketStream(ctx context.Context, symbols []string, original map[string]string, candleChan chan<- *types.Candle) {
	pairs := make(map[string]string, len(symbols))
	for _, s := range symbols {
		// Always subscribe at 1m; the SDK aggregates up to the requested
		// timeframes from that base feed.
		pairs[s] = "1m"
	}

	b.runWithBackoff(ctx, "kline", func() (chan struct{}, chan struct{}, error) {
		return futures.WsCombinedKlineServe(pairs, func(event *futures.WsKlineEvent) {
			// Drop in-progress klines: Binance fires every ~2s while a candle
			// is forming and sets IsFinal only on close. Strategies expect one
			// OnCandle per close.
			if event == nil || !event.Kline.IsFinal {
				return
			}
			symbol := original[event.Symbol]
			if symbol == "" {
				symbol = event.Symbol
			}
			candle := &types.Candle{
				Symbol:              symbol,
				Exchange:            types.ExchangeBinanceFutures,
				Timeframe:           types.Timeframe1m,
				OpenTime:            time.UnixMilli(event.Kline.StartTime),
				CloseTime:           time.UnixMilli(event.Kline.EndTime),
				Open:                parseFloat(event.Kline.Open),
				High:                parseFloat(event.Kline.High),
				Low:                 parseFloat(event.Kline.Low),
				Close:               parseFloat(event.Kline.Close),
				Volume:              parseFloat(event.Kline.Volume),
				IsComplete:          true,
				TradeCount:          event.Kline.TradeNum,
				QuoteVolume:         parseFloat(event.Kline.QuoteVolume),
				TakerBuyBaseVolume:  parseFloat(event.Kline.ActiveBuyVolume),
				TakerBuyQuoteVolume: parseFloat(event.Kline.ActiveBuyQuoteVolume),
			}
			select {
			case candleChan <- candle:
			case <-ctx.Done():
			}
		}, func(err error) {
			log.Printf("Binance futures kline stream error: %v", err)
		})
	}, nil)
}

// superviseMarkStream keeps the mark price current.
//
// It is not decoration. The notional cap has to value a market order, which has
// no price of its own, and without a mark the guard refuses the order rather
// than guessing. The mark is also the series stops trigger on, so having it
// locally is what lets the adapter report a stop's distance honestly.
func (b *BinanceFuturesClient) superviseMarkStream(ctx context.Context, symbols []string) {
	want := make(map[string]struct{}, len(symbols))
	for _, s := range symbols {
		want[s] = struct{}{}
	}

	b.runWithBackoff(ctx, "mark price", func() (chan struct{}, chan struct{}, error) {
		return futures.WsAllMarkPriceServe(func(events futures.WsAllMarkPriceEvent) {
			for _, e := range events {
				if e == nil {
					continue
				}
				if _, ok := want[e.Symbol]; !ok {
					continue
				}
				if p := parseFloat(e.MarkPrice); p > 0 {
					b.setMark(e.Symbol, p)
				}
			}
		}, func(err error) {
			log.Printf("Binance futures mark price stream error: %v", err)
		})
	}, nil)
}

// ── user data stream ─────────────────────────────────────────────────────────

// superviseUserDataStream owns the listenKey lifecycle and the fill feed.
func (b *BinanceFuturesClient) superviseUserDataStream(ctx context.Context, original map[string]string, orderChan chan<- *types.Order) {
	var (
		keyMu     sync.Mutex
		listenKey string
		expired   = make(chan struct{}, 1)
	)

	// One keepalive loop for the life of the stream, renewing whichever key is
	// current. Binance expires a listenKey 60 minutes after issue and says
	// nothing when it does: the socket stays open and simply stops delivering.
	go func() {
		ticker := time.NewTicker(listenKeyRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-b.stopped:
				return
			case <-ticker.C:
				keyMu.Lock()
				key := listenKey
				keyMu.Unlock()
				if key == "" {
					continue
				}
				if err := b.client.NewKeepaliveUserStreamService().ListenKey(key).Do(ctx); err != nil {
					// Force a reconnect, which mints a fresh key. Logging and
					// hoping is what makes this failure silent.
					log.Printf("Binance futures: listenKey keepalive failed, forcing reconnect: %v", err)
					select {
					case expired <- struct{}{}:
					default:
					}
					continue
				}
				log.Printf("Binance futures: listenKey renewed")
			}
		}
	}()

	b.runWithBackoff(ctx, "user data", func() (chan struct{}, chan struct{}, error) {
		key, err := b.client.NewStartUserStreamService().Do(ctx)
		if err != nil {
			return nil, nil, err
		}
		keyMu.Lock()
		listenKey = key
		keyMu.Unlock()

		doneC, stopC, err := futures.WsUserDataServe(key, func(event *futures.WsUserDataEvent) {
			if event == nil {
				return
			}
			switch event.Event {
			case futures.UserDataEventTypeOrderTradeUpdate:
				b.handleOrderUpdate(ctx, &event.OrderTradeUpdate, original, orderChan)
			case futures.UserDataEventTypeListenKeyExpired:
				log.Println("Binance futures: listenKey expired, reconnecting")
				select {
				case expired <- struct{}{}:
				default:
				}
			case futures.UserDataEventTypeMarginCall:
				// The venue is warning that margin is short. It is not a fill,
				// so it does not belong on orderChan, but it is the last
				// notice before a liquidation and must not be swallowed.
				log.Printf("Binance futures: MARGIN CALL — positions are close to liquidation")
			case futures.UserDataEventTypeAccountConfigUpdate:
				log.Printf("Binance futures: leverage on %s changed to %dx",
					event.WsUserDataAccountConfigUpdate.AccountConfigUpdate.Symbol,
					event.WsUserDataAccountConfigUpdate.AccountConfigUpdate.Leverage)
			}
		}, func(err error) {
			log.Printf("Binance futures user data stream error: %v", err)
		})
		if err != nil {
			return nil, nil, err
		}

		// Fold an expiry notice into the connection's own done channel so the
		// supervisor treats it as a drop and reconnects with a fresh key.
		merged := make(chan struct{})
		go func() {
			select {
			case <-doneC:
			case <-expired:
			case <-ctx.Done():
			}
			close(merged)
		}()
		return merged, stopC, nil
	}, func() { b.reconcile(ctx, orderChan, original) })
}

// handleOrderUpdate turns one ORDER_TRADE_UPDATE into an SDK order.
func (b *BinanceFuturesClient) handleOrderUpdate(ctx context.Context, u *futures.WsOrderTradeUpdate, original map[string]string, orderChan chan<- *types.Order) {
	id := strconv.FormatInt(u.ID, 10)
	symbol := original[u.Symbol]
	if symbol == "" {
		symbol = u.Symbol
	}

	side := types.OrderSideBuy
	if u.Side == futures.SideTypeSell {
		side = types.OrderSideSell
	}

	stop := parseFloat(u.StopPrice)
	// OriginalType is what the order was placed as; Type is what it became
	// after a trigger fired. The strategy asked for the former.
	declared := string(u.OriginalType)
	if declared == "" {
		declared = string(u.Type)
	}

	status := mapFuturesStatus(u.Status)
	liquidated := strings.EqualFold(string(u.Type), "LIQUIDATION") ||
		strings.EqualFold(string(u.OriginalType), "LIQUIDATION")
	if liquidated {
		// The single most important event the live path can deliver, and the
		// one the backtest already models. It must not arrive looking like an
		// ordinary market fill the strategy placed itself.
		log.Printf("Binance futures: LIQUIDATION on %s — %s %s at %s", u.Symbol, u.Side, u.AccumulatedFilledQty, u.AveragePrice)
	}

	order := &types.Order{
		ID:           id,
		Symbol:       symbol,
		Exchange:     types.ExchangeBinanceFutures,
		Side:         side,
		Type:         sdkOrderType(declared, stop > 0),
		Status:       status,
		Price:        parseFloat(u.OriginalPrice),
		Quantity:     parseFloat(u.OriginalQty),
		FilledQty:    parseFloat(u.AccumulatedFilledQty),
		AveragePrice: parseFloat(u.AveragePrice),
		Fee:          parseFloat(u.Commission),
		FeeAsset:     u.CommissionAsset,
		StopPrice:    stop,
		CreatedAt:    time.UnixMilli(u.TradeTime),
		UpdatedAt:    time.UnixMilli(u.TradeTime),
	}

	b.mu.RLock()
	sibling := b.brackets[id]
	b.mu.RUnlock()
	order.GroupID = ""
	if sibling != "" {
		order.GroupID = bracketGroupID(id, sibling)
	}

	select {
	case orderChan <- order:
	case <-ctx.Done():
		return
	}

	// Binance does not link bracket legs on futures, so when one fills the
	// other is still resting and would, on its own, open a position in the
	// opposite direction the next time price reached it.
	if status == types.OrderStatusFilled && sibling != "" {
		go func() {
			if err := b.CancelOrder(context.WithoutCancel(ctx), types.ExchangeBinanceFutures, symbol, sibling); err != nil {
				log.Printf("Binance futures: %s filled but its bracket sibling %s could not be cancelled — an orphan order is resting: %v", id, sibling, err)
				return
			}
			log.Printf("Binance futures: %s filled, cancelled bracket sibling %s", id, sibling)
		}()
	}
	if status == types.OrderStatusFilled || status == types.OrderStatusCanceled || status == types.OrderStatusRejected {
		b.forgetBracket(id)
	}
}

// bracketGroupID gives the two legs of a bracket one stable shared id,
// whichever of them is being reported.
func bracketGroupID(a, b string) string {
	if a < b {
		return "bracket-" + a + "-" + b
	}
	return "bracket-" + b + "-" + a
}

// reconcile re-reads the exchange after a reconnect.
//
// Anything that happened while the socket was down was never delivered, so the
// adapter's own memory is not evidence. Positions come straight from the venue,
// and a bracket whose sibling is no longer open is forgotten rather than
// cancelled a second time.
func (b *BinanceFuturesClient) reconcile(ctx context.Context, orderChan chan<- *types.Order, original map[string]string) {
	positions, err := b.GetPositions(ctx, types.ExchangeBinanceFutures)
	if err != nil {
		log.Printf("Binance futures: reconnect reconciliation failed to read positions: %v", err)
	} else if len(positions) == 0 {
		log.Println("Binance futures: reconnected, no open positions")
	} else {
		for _, p := range positions {
			log.Printf("Binance futures: reconnected, %s %s %v @ %v (margin %v, unrealized %v)",
				p.Symbol, p.Side, p.Size, p.EntryPrice, p.IsolatedMargin, p.UnrealizedPnL)
		}
	}

	open := map[string]struct{}{}
	for sym := range original {
		orders, err := b.client.NewListOpenOrdersService().Symbol(sym).Do(ctx)
		if err != nil {
			log.Printf("Binance futures: reconnect reconciliation failed to read open orders on %s: %v", sym, err)
			return
		}
		for _, o := range orders {
			open[strconv.FormatInt(o.OrderID, 10)] = struct{}{}
		}
	}

	b.mu.Lock()
	for id := range b.brackets {
		if _, still := open[id]; !still {
			delete(b.brackets, id)
		}
	}
	b.mu.Unlock()
}
