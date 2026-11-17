package live

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kdraigo/dev_sdk/types"
)

// Bybit's two websockets (https://bybit-exchange.github.io/docs/v5/ws/connect):
//
//  1. public linear: closed 1m klines, and tickers for the mark price;
//  2. private: the account's order updates, after an auth message.
//
// Both drop eventually. A drop must never be terminal — a strategy that stops
// hearing about its fills keeps trading against a position the venue already
// closed — so each runs under a supervisor that redials with backoff, and the
// private one reconciles over REST after every reconnect.

const (
	// bybitPingEvery is Bybit's recommended heartbeat.
	bybitPingEvery = 20 * time.Second
	// bybitReadTimeout ends a connection that has gone silent: with tickers
	// arriving every 100 ms and pongs every 20 s, a minute of nothing is a dead
	// socket, not a quiet market.
	bybitReadTimeout = 60 * time.Second
	// bybitAckTimeout bounds the wait for an auth or subscribe answer.
	bybitAckTimeout = 10 * time.Second
)

// ConnectStream opens both connections and keeps them open for the life of ctx.
func (b *BybitFuturesClient) ConnectStream(ctx context.Context, candleChan chan<- *types.Candle, orderChan chan<- *types.Order) error {
	symbols := make([]string, 0, len(b.assets()))
	original := make(map[string]string, len(b.assets()))
	for _, a := range b.assets() {
		sym := VenueSymbol(a)
		symbols = append(symbols, sym)
		original[sym] = a
	}

	b.mu.Lock()
	b.orderOut = orderChan
	b.mu.Unlock()

	if len(symbols) > 0 {
		go b.supervise(ctx, "public", b.wsURL()+"/v5/public/linear",
			func(conn *websocket.Conn) error { return b.subscribe(conn, bybitPublicTopics(symbols)) },
			func(msg []byte) { b.handlePublic(ctx, msg, original, candleChan) }, nil)
	} else {
		log.Println("Bybit futures: no assets configured, no market stream opened")
	}
	go b.supervise(ctx, "private", b.wsURL()+"/v5/private",
		func(conn *websocket.Conn) error {
			if err := b.authenticate(conn); err != nil {
				return err
			}
			return b.subscribe(conn, []string{"order"})
		},
		func(msg []byte) { b.handlePrivate(ctx, msg, original, orderChan) },
		func() { b.reconcile(ctx, original) })
	return nil
}

// Stop releases the supervisors. Idempotent.
func (b *BybitFuturesClient) Stop() {
	b.stopOnce.Do(func() { close(b.stopped) })
}

func bybitPublicTopics(symbols []string) []string {
	topics := make([]string, 0, 2*len(symbols))
	for _, s := range symbols {
		// Always 1m: the SDK aggregates up to the strategy's timeframes.
		topics = append(topics, "kline.1."+s, "tickers."+s)
	}
	return topics
}

// supervise keeps one websocket open until ctx ends: dial, set up (auth,
// subscribe), read until the connection fails, wait, and dial again.
func (b *BybitFuturesClient) supervise(ctx context.Context, name, wsURL string, setup func(*websocket.Conn) error, handle func([]byte), onReconnect func()) {
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

		conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
		if err == nil {
			err = setup(conn)
			if err != nil {
				conn.Close()
			}
		}
		if err != nil {
			log.Printf("Bybit futures: %s stream connect failed, retrying in %s: %v", name, backoff, err)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}

		if !first && onReconnect != nil {
			// Whatever happened while the socket was down was never delivered.
			onReconnect()
		}
		first = false
		backoff = reconnectBase
		log.Printf("Bybit futures: %s stream connected", name)

		err = b.readLoop(ctx, conn, handle)
		conn.Close()
		select {
		case <-ctx.Done():
			return
		case <-b.stopped:
			return
		default:
		}
		log.Printf("Bybit futures: %s stream closed (%v), reconnecting", name, err)
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

// readLoop reads until the connection fails, pinging as Bybit asks.
func (b *BybitFuturesClient) readLoop(ctx context.Context, conn *websocket.Conn, handle func([]byte)) error {
	var writeMu sync.Mutex
	done := make(chan struct{})
	defer close(done)
	go func() {
		t := time.NewTicker(bybitPingEvery)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				conn.Close()
				return
			case <-b.stopped:
				conn.Close()
				return
			case <-t.C:
				writeMu.Lock()
				err := conn.WriteJSON(map[string]string{"op": "ping"})
				writeMu.Unlock()
				if err != nil {
					conn.Close()
					return
				}
			}
		}
	}()
	for {
		conn.SetReadDeadline(time.Now().Add(bybitReadTimeout))
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		handle(msg)
	}
}

// bybitWSReply is an answer to an op (auth, subscribe, ping).
type bybitWSReply struct {
	Op      string `json:"op"`
	Success *bool  `json:"success"`
	RetMsg  string `json:"ret_msg"`
}

// waitFor reads until the answer to op arrives, or fails.
func waitFor(conn *websocket.Conn, op string) error {
	conn.SetReadDeadline(time.Now().Add(bybitAckTimeout))
	defer conn.SetReadDeadline(time.Time{})
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("waiting for the %s answer: %w", op, err)
		}
		var r bybitWSReply
		if json.Unmarshal(msg, &r) != nil || r.Op != op {
			continue
		}
		if r.Success != nil && !*r.Success {
			return fmt.Errorf("%s refused: %s", op, r.RetMsg)
		}
		return nil
	}
}

// authenticate signs in on the private stream: signature =
// hex(HMAC_SHA256(secret, "GET/realtime" + expires)).
func (b *BybitFuturesClient) authenticate(conn *websocket.Conn) error {
	key, secret := b.config.Credentials.APIKey, b.config.Credentials.APISecret
	offset := int64(0)
	if b.api != nil {
		offset = b.api.offsetMs.Load()
	}
	expires := time.Now().UnixMilli() + offset + 10_000
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("GET/realtime" + strconv.FormatInt(expires, 10)))
	if err := conn.WriteJSON(map[string]any{
		"op":   "auth",
		"args": []any{key, expires, hex.EncodeToString(mac.Sum(nil))},
	}); err != nil {
		return err
	}
	if err := waitFor(conn, "auth"); err != nil {
		return fmt.Errorf("%w (the key may lack permission, or belong to the other network)", err)
	}
	return nil
}

func (b *BybitFuturesClient) subscribe(conn *websocket.Conn, topics []string) error {
	if err := conn.WriteJSON(map[string]any{"op": "subscribe", "args": topics}); err != nil {
		return err
	}
	return waitFor(conn, "subscribe")
}

// ── public ───────────────────────────────────────────────────────────────────

type bybitWSPush struct {
	Topic string          `json:"topic"`
	Type  string          `json:"type"`
	Data  json.RawMessage `json:"data"`
}

func (b *BybitFuturesClient) handlePublic(ctx context.Context, msg []byte, original map[string]string, candleChan chan<- *types.Candle) {
	var p bybitWSPush
	if json.Unmarshal(msg, &p) != nil || p.Topic == "" {
		return // op answers (pong, subscribe)
	}
	switch {
	case strings.HasPrefix(p.Topic, "kline."):
		sym := p.Topic[strings.LastIndex(p.Topic, ".")+1:]
		var ks []struct {
			Start    int64  `json:"start"`
			End      int64  `json:"end"`
			Open     string `json:"open"`
			Close    string `json:"close"`
			High     string `json:"high"`
			Low      string `json:"low"`
			Volume   string `json:"volume"`
			Turnover string `json:"turnover"`
			Confirm  bool   `json:"confirm"`
		}
		if json.Unmarshal(p.Data, &ks) != nil {
			return
		}
		for _, k := range ks {
			// Bybit pushes a forming candle every second or so and sets confirm
			// on its last tick: one OnCandle per close, as strategies expect.
			if !k.Confirm {
				continue
			}
			symbol := original[sym]
			if symbol == "" {
				symbol = sym
			}
			candle := &types.Candle{
				Symbol: symbol, Exchange: types.ExchangeBybitFutures, Timeframe: types.Timeframe1m,
				OpenTime: time.UnixMilli(k.Start), CloseTime: time.UnixMilli(k.End),
				Open: parseFloat(k.Open), High: parseFloat(k.High), Low: parseFloat(k.Low), Close: parseFloat(k.Close),
				Volume: parseFloat(k.Volume), QuoteVolume: parseFloat(k.Turnover), IsComplete: true,
			}
			select {
			case candleChan <- candle:
			case <-ctx.Done():
				return
			}
		}
	case strings.HasPrefix(p.Topic, "tickers."):
		var t struct {
			Symbol    string `json:"symbol"`
			MarkPrice string `json:"markPrice"`
		}
		if json.Unmarshal(p.Data, &t) != nil {
			return
		}
		// A delta leaves out what did not change: no markPrice means the last
		// one still holds, never zero.
		if m := parseFloat(t.MarkPrice); m > 0 && t.Symbol != "" {
			b.setMark(t.Symbol, m)
		}
	}
}

// ── private ──────────────────────────────────────────────────────────────────

func (b *BybitFuturesClient) handlePrivate(ctx context.Context, msg []byte, original map[string]string, orderChan chan<- *types.Order) {
	var p bybitWSPush
	if json.Unmarshal(msg, &p) != nil || p.Topic != "order" {
		return
	}
	var recs []bybitOrderRecord
	if err := json.Unmarshal(p.Data, &recs); err != nil {
		log.Printf("Bybit futures: unreadable order update: %v", err)
		return
	}
	for _, rec := range recs {
		b.handleOrderUpdate(ctx, rec, original, orderChan)
	}
}

// handleOrderUpdate turns one pushed order record into an SDK order, and
// cancels a bracket's surviving leg when the other one fills.
func (b *BybitFuturesClient) handleOrderUpdate(ctx context.Context, rec bybitOrderRecord, original map[string]string, orderChan chan<- *types.Order) {
	symbol := original[rec.Symbol]
	if symbol == "" {
		symbol = rec.Symbol
	}
	order := b.mapRecord(rec, symbol)

	if bybitLiquidation(rec.CreateType) {
		log.Printf("Bybit futures: LIQUIDATION on %s — %s %s at %s (%s)", rec.Symbol, rec.Side, rec.CumExecQty, rec.AvgPrice, rec.CreateType)
	}

	// Fee is this update's new fees, CumulativeFee the order's total.
	b.mu.Lock()
	order.Fee = order.CumulativeFee - b.feeSeen[order.ID]
	if order.Fee < 0 {
		order.Fee = 0
	}
	b.feeSeen[order.ID] = order.CumulativeFee
	leg := b.brackets[order.ID]
	b.mu.Unlock()
	order.GroupID = leg.groupID

	select {
	case orderChan <- order:
	case <-ctx.Done():
		return
	}

	if order.Status == types.OrderStatusFilled && leg.sibling != "" {
		go func() {
			err := b.CancelOrder(context.WithoutCancel(ctx), types.ExchangeBybitFutures, symbol, leg.sibling)
			switch {
			case err == nil:
				log.Printf("Bybit futures: %s filled, cancelled bracket sibling %s", order.ID, leg.sibling)
			case errors.Is(err, types.ErrOrderClosed):
				// Bybit cancels reduce-only orders itself once the position is
				// flat; nothing is left resting.
				log.Printf("Bybit futures: %s filled, its bracket sibling %s had already closed", order.ID, leg.sibling)
			default:
				log.Printf("Bybit futures: %s filled but its bracket sibling %s could not be cancelled — an orphan order is resting: %v", order.ID, leg.sibling, err)
			}
		}()
	}
	switch order.Status {
	case types.OrderStatusFilled, types.OrderStatusCanceled, types.OrderStatusRejected:
		if order.Status != types.OrderStatusFilled || leg.sibling == "" {
			b.forgetOrder(order.ID)
		} else {
			// Keep the bracket until the sibling's cancel has been sent.
			b.mu.Lock()
			delete(b.declared, order.ID)
			delete(b.feeSeen, order.ID)
			b.mu.Unlock()
		}
	}
}

// bybitLiquidation reports whether an order was created by a liquidation or an
// auto-deleverage rather than by the account (V5 createType).
func bybitLiquidation(createType string) bool {
	t := strings.ToLower(createType)
	return strings.Contains(t, "liq") || strings.Contains(t, "adl") || strings.Contains(t, "takeover")
}

// reconcile re-reads the venue after a reconnect: positions straight from the
// exchange, and brackets whose legs are no longer open are forgotten rather
// than cancelled a second time.
func (b *BybitFuturesClient) reconcile(ctx context.Context, original map[string]string) {
	positions, err := b.GetPositions(ctx, types.ExchangeBybitFutures)
	if err != nil {
		log.Printf("Bybit futures: reconnect reconciliation failed to read positions: %v", err)
	} else if len(positions) == 0 {
		log.Println("Bybit futures: reconnected, no open positions")
	} else {
		for _, p := range positions {
			log.Printf("Bybit futures: reconnected, %s %s %v @ %v (margin %v, unrealized %v)",
				p.Symbol, p.Side, p.Size, p.EntryPrice, p.IsolatedMargin, p.UnrealizedPnL)
		}
	}
	open := map[string]struct{}{}
	for sym, orig := range original {
		orders, err := b.ListOpenOrders(ctx, types.ExchangeBybitFutures, orig)
		if err != nil {
			log.Printf("Bybit futures: reconnect reconciliation failed to read open orders on %s: %v", sym, err)
			return
		}
		for _, o := range orders {
			if o.Status == types.OrderStatusNew || o.Status == types.OrderStatusPartiallyFilled {
				open[o.ID] = struct{}{}
			}
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
