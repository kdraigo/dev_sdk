package paper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/kdraigo/dev_sdk/signing"
	"github.com/kdraigo/dev_sdk/types"
)

// readTimeout is how long the client waits for any frame; the engine pings
// every 20 s.
const readTimeout = 70 * time.Second

// doneMemory is how many finished orders are remembered, so a resync after a
// reconnect does not report one twice.
const doneMemory = 4096

// ErrNotConnected is a call made while the WebSocket is down.
var ErrNotConnected = errors.New("paper: not connected to the paper exchange")

// ConnectStream connects to the account and keeps the connection for the
// life of ctx, redialing across drops. It returns once the first connection
// is up.
func (c *Client) ConnectStream(ctx context.Context, candleChan chan<- *types.Candle, orderChan chan<- *types.Order) error {
	if c.accountID == "" {
		return errors.New("paper: PrepareSession first")
	}
	ready := make(chan error, 1)
	c.queue = make(chan queued, 1024)
	go c.dispatch(ctx, candleChan)
	go c.supervise(ctx, candleChan, orderChan, ready)
	select {
	case err := <-ready:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Ended is closed when the account can no longer be traded: it was stopped
// (from the website, or by its plan), or the engine refuses this key. Err
// says why. A dropped connection is not an end: it is redialed.
func (c *Client) Ended() <-chan struct{} { return c.ended }

// StreamErr is why the stream ended, once Ended is closed.
func (c *Client) StreamErr() error {
	select {
	case <-c.ended:
		return c.endErr
	default:
		return nil
	}
}

func (c *Client) end(err error) {
	c.endMu.Do(func() {
		c.endErr = err
		close(c.ended)
	})
}

func (c *Client) supervise(ctx context.Context, candleChan chan<- *types.Candle, orderChan chan<- *types.Order, ready chan<- error) {
	first := true
	failures := 0
	for ctx.Err() == nil {
		conn, err := c.dial(ctx)
		if err != nil {
			var fatal *fatalError
			if errors.As(err, &fatal) {
				c.end(fatal.err)
				if first {
					ready <- fatal.err
				}
				return
			}
			if first {
				// The first dial answers the caller: a bot that cannot reach
				// the paper exchange at start says so rather than waiting.
				ready <- err
				return
			}
			failures++
			wait := backoff(c.reconnectBase, c.reconnectMax, failures)
			log.Printf("Paper: reconnect failed (%v); retrying in %s", err, wait)
			if !sleep(ctx, wait) {
				return
			}
			continue
		}
		failures = 0
		reason, stopped := c.run(ctx, conn, candleChan, orderChan, first, ready)
		first = false
		if stopped != nil {
			c.end(stopped)
			return
		}
		if ctx.Err() != nil {
			return
		}
		log.Printf("Paper: connection to the paper exchange lost (%v); reconnecting", reason)
		if !sleep(ctx, c.reconnectBase) {
			return
		}
	}
}

// fatalError is a dial the engine refused for good.
type fatalError struct{ err error }

func (f *fatalError) Error() string { return f.err.Error() }

func (c *Client) dial(ctx context.Context) (*websocket.Conn, error) {
	path := apiPrefix + "/accounts/" + c.accountID + "/ws"
	headers, err := signing.Headers(signing.StyleStandard, c.cfg.Credentials.KeyID, c.cfg.Credentials.PrivateKey, http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, &fatalError{err}
	}
	wsURL := "ws" + strings.TrimPrefix(c.endpoint, "http") + path
	dctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	conn, resp, err := websocket.DefaultDialer.DialContext(dctx, wsURL, headers)
	if err != nil {
		if resp != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			var e struct {
				Error string `json:"error"`
			}
			_ = json.Unmarshal(body, &e)
			msg := fmt.Errorf("paper: the paper exchange refused the connection: %s (status %d)", e.Error, resp.StatusCode)
			switch resp.StatusCode {
			case http.StatusNotFound, http.StatusForbidden, http.StatusConflict:
				// Gone, someone else's, or stopped: redialing cannot help.
				return nil, &fatalError{msg}
			}
			return nil, msg
		}
		return nil, err
	}
	conn.SetReadLimit(16 << 20)
	conn.SetPingHandler(func(data string) error {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		err := conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(5*time.Second))
		var ne net.Error
		if err == websocket.ErrCloseSent || (errors.As(err, &ne) && ne.Timeout()) {
			return nil
		}
		return err
	})
	return conn, nil
}

// run serves one connection until it drops. It reports why, and a non-nil
// stopped when the account itself has ended.
func (c *Client) run(ctx context.Context, conn *websocket.Conn, candleChan chan<- *types.Candle, orderChan chan<- *types.Order,
	first bool, ready chan<- error) (reason error, stopped error) {
	// The engine's first frame is the account as it stands.
	_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
	var f frame
	if err := conn.ReadJSON(&f); err != nil {
		conn.Close()
		if first {
			ready <- fmt.Errorf("paper: reading the account: %w", err)
			return nil, fmt.Errorf("paper: reading the account: %w", err)
		}
		return err, nil
	}
	var state view
	if f.Action == "session_state" {
		_ = json.Unmarshal(f.Data, &state)
	}

	c.mu.Lock()
	c.conn = conn
	up := c.connUp
	c.mu.Unlock()
	close(up)

	// Goodbye on shutdown: the engine marks the bot away; the account keeps
	// running.
	connDone := make(chan struct{})
	defer close(connDone)
	go func() {
		select {
		case <-ctx.Done():
			c.writeMu.Lock()
			_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			_ = conn.WriteJSON(map[string]any{"action": "close", "request_id": uuid.NewString()})
			c.writeMu.Unlock()
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
			conn.Close()
		case <-connDone:
		}
	}()

	// The strategy hears of an attach before any candle of it: on a resumed
	// account at start (a cold start, this process holds nothing), and after
	// every reconnect. Its callback may call the account, so it runs beside
	// the read loop while candles wait in the queue.
	c.mu.Lock()
	onResume := c.onResume
	c.mu.Unlock()
	if onResume != nil && (!first || c.resumed) {
		hold := make(chan struct{})
		c.queue <- queued{gate: hold}
		st := c.sessionState(state, first)
		go func() {
			defer close(hold)
			onResume(st)
		}()
	}
	if first {
		ready <- nil
	} else {
		go c.resync(ctx, state, orderChan)
	}

	defer func() {
		c.mu.Lock()
		c.conn = nil
		c.connUp = make(chan struct{})
		pending := c.pending
		c.pending = map[string]chan frame{}
		c.mu.Unlock()
		for _, ch := range pending {
			close(ch) // the call sees the drop
		}
		conn.Close()
	}()

	for {
		_ = conn.SetReadDeadline(time.Now().Add(readTimeout))
		var f frame
		if err := conn.ReadJSON(&f); err != nil {
			return err, nil
		}
		if f.RequestID != "" {
			c.mu.Lock()
			ch := c.pending[f.RequestID]
			delete(c.pending, f.RequestID)
			c.mu.Unlock()
			if ch != nil {
				ch <- f
			}
			continue
		}
		var e event
		if err := json.Unmarshal(f.Data, &e); err != nil {
			continue
		}
		switch e.Type {
		case "candle":
			select {
			case c.queue <- queued{e: e}:
			case <-ctx.Done():
				return ctx.Err(), nil
			}
		case "order_update":
			var u orderUpdate
			if json.Unmarshal(e.Data, &u) == nil {
				c.forward(ctx, u.Order, u.Fills, orderChan)
			}
		case "account_state":
			var st struct {
				Status     string `json:"status"`
				StopReason string `json:"stop_reason"`
			}
			if json.Unmarshal(e.Data, &st) == nil && st.Status != "" && st.Status != "RUNNING" {
				msg := "paper: the account was " + strings.ToLower(st.Status)
				if st.StopReason != "" {
					msg += " (" + st.StopReason + ")"
				}
				return nil, errors.New(msg)
			}
		case "feed_status":
			var st struct {
				State string `json:"state"`
			}
			if json.Unmarshal(e.Data, &st) == nil && st.State != "" && st.State != "LIVE" {
				log.Printf("Paper: %s market feed is %s", e.Market, st.State)
			}
		}
	}
}

// queued is a candle on its way to the SDK, or a gate the queue waits at
// until the strategy's resume callback has returned.
type queued struct {
	e    event
	gate chan struct{}
}

// dispatch hands the queued candles to the SDK in order, for the life of the
// stream (one queue across reconnects, so bars never overtake each other).
func (c *Client) dispatch(ctx context.Context, candleChan chan<- *types.Candle) {
	for {
		select {
		case <-ctx.Done():
			return
		case q := <-c.queue:
			if q.gate != nil {
				select {
				case <-q.gate:
				case <-ctx.Done():
					return
				}
				continue
			}
			c.onCandle(ctx, q.e, candleChan)
		}
	}
}

// onCandle hands a closed 1m bar to the SDK, once. After a reconnect the bars
// missed while away are read from the data API first.
func (c *Client) onCandle(ctx context.Context, e event, candleChan chan<- *types.Candle) {
	var w candleWire
	if json.Unmarshal(e.Data, &w) != nil {
		return
	}
	_, vs, _ := strings.Cut(e.Market, ":")
	configured, ok := c.symbols[vs]
	if !ok {
		return
	}
	c.mu.Lock()
	prev := c.lastOpen[vs]
	c.mu.Unlock()
	if w.OpenTime <= prev {
		return // already delivered
	}
	if prev > 0 && w.OpenTime > prev+60_000 {
		missed, err := c.GetHistoricalCandles(ctx, c.venue, configured, time.UnixMilli(prev+60_000), time.UnixMilli(w.OpenTime-1), types.Timeframe1m)
		if err != nil {
			log.Printf("Paper: %d bars of %s missed while away could not be read: %v", (w.OpenTime-prev)/60_000-1, vs, err)
		}
		for _, m := range missed {
			if o := m.OpenTime.UnixMilli(); o > prev && o < w.OpenTime {
				m.CloseTime = time.UnixMilli(o + 59_999)
				c.push(ctx, candleChan, vs, m)
				prev = o
			}
		}
	}
	c.push(ctx, candleChan, vs, &types.Candle{Symbol: configured, Exchange: c.venue, Timeframe: types.Timeframe1m,
		OpenTime: time.UnixMilli(w.OpenTime), CloseTime: time.UnixMilli(w.CloseTime), Open: w.Open, High: w.High, Low: w.Low,
		Close: w.Close, Volume: w.Volume, IsComplete: true, TradeCount: w.Trades, QuoteVolume: w.QuoteVol,
		TakerBuyBaseVolume: w.TakerBase, TakerBuyQuoteVolume: w.TakerQuote})
}

func (c *Client) push(ctx context.Context, candleChan chan<- *types.Candle, vs string, k *types.Candle) {
	c.mu.Lock()
	c.lastOpen[vs] = k.OpenTime.UnixMilli()
	c.last[vs] = k.Close
	c.mu.Unlock()
	select {
	case candleChan <- k:
	case <-ctx.Done():
	}
}

// forward hands an order's change to the SDK unless the strategy was told of
// it already.
func (c *Client) forward(ctx context.Context, w orderWire, fills []fillWire, orderChan chan<- *types.Order) {
	o := c.order(w, fills)
	c.mu.Lock()
	if c.done[w.ID] || (w.Status == "REJECTED" && w.ClientOrderID != nil && c.placing[*w.ClientOrderID]) {
		c.mu.Unlock()
		return
	}
	b := belief{status: o.Status, filled: o.FilledQty}
	if prev, ok := c.seen[w.ID]; ok && (prev == b || b.filled < prev.filled) {
		// Told already, or older than what it was told (a resync's snapshot
		// arriving after a newer push).
		c.mu.Unlock()
		return
	}
	if isTerminal(o.Status) {
		delete(c.seen, w.ID)
		delete(c.open, w.ID)
		delete(c.intent, w.ID)
		c.done[w.ID] = true
		c.doneFIFO = append(c.doneFIFO, w.ID)
		if len(c.doneFIFO) > doneMemory {
			delete(c.done, c.doneFIFO[0])
			c.doneFIFO = c.doneFIFO[1:]
		}
	} else {
		c.seen[w.ID] = b
		c.open[w.ID] = true
	}
	c.mu.Unlock()
	select {
	case orderChan <- o:
	case <-ctx.Done():
	}
}

// resync reports what changed while the connection was down: every open
// order as it now stands, and how each order that was open and no longer is
// ended.
func (c *Client) resync(ctx context.Context, state view, orderChan chan<- *types.Order) {
	still := map[uint64]bool{}
	for _, w := range state.Orders {
		still[w.ID] = true
		c.forward(ctx, w, nil, orderChan)
	}
	c.mu.Lock()
	var gone []uint64
	for id := range c.open {
		if !still[id] {
			gone = append(gone, id)
		}
	}
	c.mu.Unlock()
	for _, id := range gone {
		raw, err := c.call(ctx, "get_order", map[string]any{"order_id": id})
		if err != nil {
			log.Printf("Paper: order %d changed while away and could not be read: %v", id, err)
			continue
		}
		var w orderWire
		if json.Unmarshal(raw, &w) == nil {
			c.forward(ctx, w, nil, orderChan)
		}
	}
}

// call sends an action and waits for its answer.
func (c *Client) call(ctx context.Context, action string, data any) (json.RawMessage, error) {
	c.mu.Lock()
	conn := c.conn
	if conn == nil {
		c.mu.Unlock()
		return nil, ErrNotConnected
	}
	id := uuid.NewString()
	ch := make(chan frame, 1)
	c.pending[id] = ch
	c.mu.Unlock()
	forget := func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}
	c.writeMu.Lock()
	_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	err := conn.WriteJSON(map[string]any{"action": action, "request_id": id, "data": data})
	c.writeMu.Unlock()
	if err != nil {
		forget()
		return nil, fmt.Errorf("%w: %v", ErrNotConnected, err)
	}
	timer := time.NewTimer(c.callTimeout)
	defer timer.Stop()
	select {
	case f, ok := <-ch:
		if !ok {
			return nil, fmt.Errorf("%w: the connection dropped before %s was answered", ErrNotConnected, action)
		}
		if f.Status != "ok" {
			var res orderResult
			if json.Unmarshal(f.Data, &res) == nil && res.Reject != nil {
				return f.Data, &Rejected{Code: res.Reject.Code, Reason: res.Reject.Reason, Msg: res.Reject.Msg}
			}
			return f.Data, &EngineError{Code: f.Code, Msg: f.Error}
		}
		return f.Data, nil
	case <-timer.C:
		forget()
		return nil, fmt.Errorf("%w: no answer to %s in %s", ErrNotConnected, action, c.callTimeout)
	case <-ctx.Done():
		forget()
		return nil, ctx.Err()
	}
}

// EngineError is an action the engine could not carry out (busy, the
// account stopped, an unknown market).
type EngineError struct {
	Code string
	Msg  string
}

func (e *EngineError) Error() string { return "paper exchange: " + e.Msg }

// waitConnected waits up to d for a connection.
func (c *Client) waitConnected(ctx context.Context, d time.Duration) error {
	c.mu.Lock()
	up := c.connUp
	c.mu.Unlock()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-up:
		return nil
	case <-c.ended:
		return c.endErr
	case <-t.C:
		return ErrNotConnected
	case <-ctx.Done():
		return ctx.Err()
	}
}

func isTerminal(s types.OrderStatus) bool {
	return s == types.OrderStatusFilled || s == types.OrderStatusCanceled || s == types.OrderStatusRejected
}

func backoff(base, max time.Duration, n int) time.Duration {
	d := base
	for i := 1; i < n && d < max; i++ {
		d *= 2
	}
	return min(d, max)
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
