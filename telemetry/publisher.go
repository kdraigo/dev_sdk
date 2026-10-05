package telemetry

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// Wire-format size caps. Kept in sync with live_trades' server-side
// validation; the SDK truncates locally to avoid 413s.
const (
	MaxReasonBytes    = 4096
	MaxLogsTotalBytes = 16384
	MaxLogLineBytes   = 1024
	MaxLogLineCount   = 32
)

// HeartbeatMeta is the per-tick payload appended to the 5s heartbeat. Cheap
// to capture, gives the operator a live view of strategy progress beyond
// "process alive".
type HeartbeatMeta struct {
	UptimeSeconds    int64
	CandlesProcessed int64
	OrdersPlaced     int
	LastOrderID      string
}

// Publisher broadcasts SDK events to live_trades. All methods must be
// non-blocking — implementations fire goroutines internally so the strategy
// callback is never delayed by network I/O.
type Publisher interface {
	PublishOrder(order *types.Order, reason map[string]any, logs []string)
	PublishBalance(account *types.Account)
	PublishInitialBalance(account *types.Account)
	PublishHeartbeat(meta HeartbeatMeta)
	PublishStopped(reason string)
	// PublishSessionMeta declares the strategy's identity. Sent once at Start;
	// the server upserts, so it is safe to re-send on reconnect.
	PublishSessionMeta(name string, config map[string]any)
	// Enabled reports whether telemetry is actually being sent. Heartbeat
	// goroutines should not start when this is false.
	Enabled() bool
}

// Option configures a publisher.
type Option func(*httpPublisher)

// WithEnvironment names the SDK environment (types.Environment, e.g.
// "real_binance_futures") in session_meta, so the platform can tell real
// money from testnet without guessing from the strategy config.
func WithEnvironment(env string) Option {
	return func(p *httpPublisher) { p.environment = env }
}

// NewPublisher returns an httpPublisher when url is set, otherwise a NoOpPublisher.
// defaultExchange / defaultSymbol are stamped onto every payload that doesn't
// carry its own (heartbeat, initial_balance, balance, session_stopped) so the
// first ingest creates a live_sessions row with the right exchange/symbol —
// otherwise the frontend's chart sits on an empty symbol forever.
func NewPublisher(sessionID, url, keyID, privateKey, defaultExchange, defaultSymbol string, opts ...Option) Publisher {
	if url == "" {
		return NoOpPublisher{}
	}
	p := &httpPublisher{
		sessionID:       sessionID,
		baseURL:         url,
		keyID:           keyID,
		privateKey:      privateKey,
		defaultExchange: defaultExchange,
		defaultSymbol:   defaultSymbol,
		client:          &http.Client{Timeout: 5 * time.Second},
		open:            make(map[string]orderPayload),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// ── NoOp ─────────────────────────────────────────────────────────────────────

type NoOpPublisher struct{}

func (NoOpPublisher) PublishOrder(*types.Order, map[string]any, []string) {}
func (NoOpPublisher) PublishBalance(*types.Account)                       {}
func (NoOpPublisher) PublishInitialBalance(*types.Account)                {}
func (NoOpPublisher) PublishHeartbeat(HeartbeatMeta)                      {}
func (NoOpPublisher) PublishStopped(string)                               {}
func (NoOpPublisher) PublishSessionMeta(string, map[string]any)           {}
func (NoOpPublisher) Enabled() bool                                       { return false }

// ── HTTP ──────────────────────────────────────────────────────────────────────

type httpPublisher struct {
	sessionID       string
	baseURL         string
	keyID           string
	privateKey      string
	defaultExchange string
	defaultSymbol   string
	environment     string
	client          *http.Client

	// open is the last payload sent for each order not yet finished, used to
	// fill in sparse updates. See complete.
	mu   sync.Mutex
	open map[string]orderPayload
}

// maxTrackedOrders bounds the open-order memory. Past it an arbitrary entry
// is dropped: the memory only improves sparse updates, which live_trades also
// guards against, so losing one is harmless where growing without bound on a
// long-running bot is not.
const maxTrackedOrders = 4096

func (p *httpPublisher) Enabled() bool { return true }

type telemetryPayload struct {
	SessionID string              `json:"session_id"`
	Exchange  string              `json:"exchange,omitempty"`
	Symbol    string              `json:"symbol,omitempty"`
	EventType string              `json:"event_type"`
	Order     *orderPayload       `json:"order,omitempty"`
	Balance   *balancePayload     `json:"balance,omitempty"`
	Balances  []balancePayload    `json:"balances,omitempty"`
	Heartbeat *heartbeatPayload   `json:"heartbeat,omitempty"`
	Stopped   *stoppedPayload     `json:"stopped,omitempty"`
	Meta      *sessionMetaPayload `json:"meta,omitempty"`
}

type sessionMetaPayload struct {
	StrategyName string         `json:"strategy_name"`
	Config       map[string]any `json:"config,omitempty"`
	Environment  string         `json:"environment,omitempty"`
}

type orderPayload struct {
	OrderID       string         `json:"order_id"`
	ClientOrderID string         `json:"client_order_id"`
	Side          string         `json:"side"`
	Type          string         `json:"type"`
	Status        string         `json:"status"`
	Price         float64        `json:"price"`
	Qty           float64        `json:"qty"`
	FilledQty     float64        `json:"filled_qty"`
	AvgPrice      float64        `json:"avg_price"`
	Fee           float64        `json:"fee"`
	FeeAsset      string         `json:"fee_asset"`
	RealizedPnL   float64        `json:"realized_pnl"`
	StopPrice     float64        `json:"stop_price,omitempty"`
	ReduceOnly    bool           `json:"reduce_only,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
	UpdatedAt     time.Time      `json:"updated_at"`
	Reason        map[string]any `json:"reason,omitempty"`
	Logs          []string       `json:"logs,omitempty"`
}

type balancePayload struct {
	Asset      string    `json:"asset"`
	Free       float64   `json:"free"`
	Locked     float64   `json:"locked"`
	RecordedAt time.Time `json:"recorded_at"`
}

type heartbeatPayload struct {
	RecordedAt       time.Time `json:"recorded_at"`
	UptimeSeconds    int64     `json:"uptime_seconds"`
	CandlesProcessed int64     `json:"candles_processed"`
	OrdersPlaced     int       `json:"orders_placed"`
	LastOrderID      string    `json:"last_order_id"`
}

type stoppedPayload struct {
	Reason     string    `json:"reason"`
	RecordedAt time.Time `json:"recorded_at"`
}

func (p *httpPublisher) PublishOrder(order *types.Order, reason map[string]any, logs []string) {
	reasonOut, logsOut := truncateReasonAndLogs(reason, logs)
	// The order's total commission, not what this update charged: Binance
	// futures reports each fill's commission alone, and live_trades keeps one
	// row per order.
	fee := order.Fee
	if order.CumulativeFee > 0 {
		fee = order.CumulativeFee
	}
	op := orderPayload{
		OrderID:     order.ID,
		Side:        string(order.Side),
		Type:        string(order.Type),
		Status:      string(order.Status),
		Price:       order.Price,
		Qty:         order.Quantity,
		FilledQty:   order.FilledQty,
		AvgPrice:    order.AveragePrice,
		Fee:         fee,
		FeeAsset:    order.FeeAsset,
		RealizedPnL: order.RealizedPnL,
		StopPrice:   order.StopPrice,
		ReduceOnly:  order.ReduceOnly,
		CreatedAt:   order.CreatedAt,
		UpdatedAt:   order.UpdatedAt,
		Reason:      reasonOut,
		Logs:        logsOut,
	}
	p.complete(&op)
	payload := &telemetryPayload{
		SessionID: p.sessionID,
		Exchange:  order.Exchange,
		Symbol:    order.Symbol,
		EventType: "order",
		Order:     &op,
	}
	go p.send(payload)
}

// complete fills what a sparse update left out from the last payload sent for
// the same order, then remembers this one.
//
// The SDK's synthetic CANCELED carries only an id and a status, and a poll
// finds no commission; sent as they are, live_trades would hear of an order
// with no side, type or size, or one whose fee went back to zero. Payloads
// are completed here rather than in CancelOrder because only the publisher
// sees every update an order has had.
func (p *httpPublisher) complete(o *orderPayload) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if last, ok := p.open[o.OrderID]; ok {
		if o.Side == "" {
			o.Side = last.Side
		}
		if o.Type == "" {
			o.Type = last.Type
		}
		if o.Price == 0 {
			o.Price = last.Price
		}
		if o.Qty == 0 {
			o.Qty = last.Qty
		}
		if o.FilledQty < last.FilledQty {
			o.FilledQty = last.FilledQty
		}
		if o.AvgPrice == 0 {
			o.AvgPrice = last.AvgPrice
		}
		if o.Fee == 0 {
			o.Fee, o.FeeAsset = last.Fee, last.FeeAsset
		}
		if o.RealizedPnL == 0 {
			o.RealizedPnL = last.RealizedPnL
		}
		if o.StopPrice == 0 {
			o.StopPrice = last.StopPrice
		}
		// Reduce-only is fixed at placement; a sparse update just omits it.
		o.ReduceOnly = o.ReduceOnly || last.ReduceOnly
		if o.CreatedAt.IsZero() {
			o.CreatedAt = last.CreatedAt
		}
	}
	if o.UpdatedAt.IsZero() {
		o.UpdatedAt = time.Now().UTC()
	}
	if o.CreatedAt.IsZero() {
		o.CreatedAt = o.UpdatedAt
	}

	switch types.OrderStatus(o.Status) {
	case types.OrderStatusFilled, types.OrderStatusCanceled, types.OrderStatusRejected:
		delete(p.open, o.OrderID)
		return
	}
	if _, ok := p.open[o.OrderID]; !ok && len(p.open) >= maxTrackedOrders {
		for k := range p.open {
			delete(p.open, k)
			break
		}
	}
	kept := *o
	kept.Reason, kept.Logs = nil, nil
	p.open[o.OrderID] = kept
}

func (p *httpPublisher) PublishBalance(account *types.Account) {
	go p.send(p.buildBalancesPayload(account, "balance"))
}

func (p *httpPublisher) PublishInitialBalance(account *types.Account) {
	go p.send(p.buildBalancesPayload(account, "initial_balance"))
}

// buildBalancesPayload uses the account's exchange when present, falling back
// to the publisher's configured default. Symbol always comes from the default
// (balances don't have a symbol of their own; we attach it so the first event
// can create the session row with the right pair).
func (p *httpPublisher) buildBalancesPayload(account *types.Account, eventType string) *telemetryPayload {
	now := time.Now().UTC()
	out := make([]balancePayload, 0, len(account.Balances))
	for _, b := range account.Balances {
		out = append(out, balancePayload{
			Asset:      b.Asset,
			Free:       b.Free,
			Locked:     b.Lock,
			RecordedAt: now,
		})
	}
	exch := account.Exchange
	if exch == "" {
		exch = p.defaultExchange
	}
	return &telemetryPayload{
		SessionID: p.sessionID,
		Exchange:  exch,
		Symbol:    p.defaultSymbol,
		EventType: eventType,
		Balances:  out,
	}
}

func (p *httpPublisher) PublishHeartbeat(meta HeartbeatMeta) {
	go p.send(&telemetryPayload{
		SessionID: p.sessionID,
		Exchange:  p.defaultExchange,
		Symbol:    p.defaultSymbol,
		EventType: "heartbeat",
		Heartbeat: &heartbeatPayload{
			RecordedAt:       time.Now().UTC(),
			UptimeSeconds:    meta.UptimeSeconds,
			CandlesProcessed: meta.CandlesProcessed,
			OrdersPlaced:     meta.OrdersPlaced,
			LastOrderID:      meta.LastOrderID,
		},
	})
}

// PublishSessionMeta declares the strategy identity for this session.
//
// Sent synchronously at Start: it is one small request, and getting it in before
// the first order means the console never briefly shows the session as a bare
// UUID. The server upserts, so re-sending on a reconnect is harmless.
func (p *httpPublisher) PublishSessionMeta(name string, config map[string]any) {
	if name == "" && len(config) == 0 && p.environment == "" {
		return
	}
	p.send(&telemetryPayload{
		SessionID: p.sessionID,
		Exchange:  p.defaultExchange,
		Symbol:    p.defaultSymbol,
		EventType: "session_meta",
		Meta: &sessionMetaPayload{
			StrategyName: name,
			Config:       config,
			Environment:  p.environment,
		},
	})
}

func (p *httpPublisher) PublishStopped(reason string) {
	// Send synchronously so the goroutine doesn't get killed mid-flight by
	// the process exiting after Start() returns. Bounded by client timeout.
	p.send(&telemetryPayload{
		SessionID: p.sessionID,
		Exchange:  p.defaultExchange,
		Symbol:    p.defaultSymbol,
		EventType: "session_stopped",
		Stopped: &stoppedPayload{
			Reason:     reason,
			RecordedAt: time.Now().UTC(),
		},
	})
}

// truncateReasonAndLogs enforces the same caps as the server but in a
// best-effort way: instead of failing, it trims and adds a marker. The user's
// strategy should never see a telemetry failure.
func truncateReasonAndLogs(reason map[string]any, logs []string) (map[string]any, []string) {
	var reasonOut map[string]any
	if reason != nil {
		// Deep copy so we can mutate without disturbing the caller.
		reasonOut = make(map[string]any, len(reason)+1)
		for k, v := range reason {
			reasonOut[k] = v
		}
		if b, err := json.Marshal(reasonOut); err == nil && len(b) > MaxReasonBytes {
			reasonOut = map[string]any{
				"_truncated":     true,
				"_original_size": len(b),
			}
		}
	}

	var logsOut []string
	if len(logs) > 0 {
		logsOut = make([]string, 0, len(logs))
		total := 0
		truncatedExtra := 0
		for _, ln := range logs {
			if len(ln) > MaxLogLineBytes {
				ln = ln[:MaxLogLineBytes-3] + "..."
			}
			if total+len(ln) > MaxLogsTotalBytes || len(logsOut) >= MaxLogLineCount-1 {
				truncatedExtra = len(logs) - len(logsOut)
				break
			}
			logsOut = append(logsOut, ln)
			total += len(ln)
		}
		if truncatedExtra > 0 {
			logsOut = append(logsOut, fmt.Sprintf("[truncated %d more lines]", truncatedExtra))
		}
	}
	return reasonOut, logsOut
}

func (p *httpPublisher) send(payload *telemetryPayload) {
	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[telemetry] marshal error: %v", err)
		return
	}

	method := http.MethodPost
	sigPath := "/api/v1/telemetry"
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)

	reqURL := p.baseURL
	if reqURL == "https://api.kdraigo.com" || reqURL == "http://localhost:5001" {
		reqURL = reqURL + sigPath
	}

	req, err := http.NewRequest(method, reqURL, bytes.NewReader(body))
	if err != nil {
		log.Printf("[telemetry] request error: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	// Kdraigo Signature
	if p.keyID != "" && p.privateKey != "" {
		privKeyBytes, err := hex.DecodeString(p.privateKey)
		if err == nil && len(privKeyBytes) == ed25519.PrivateKeySize {
			canonical := fmt.Sprintf("%s\n%s\n%s\n%s", method, sigPath, timestamp, string(body))
			sig := ed25519.Sign(privKeyBytes, []byte(canonical))
			req.Header.Set("X-Key-ID", p.keyID)
			req.Header.Set("X-Signature", hex.EncodeToString(sig))
			req.Header.Set("X-Timestamp", timestamp)
		}
	}

	resp, err := p.client.Do(req)
	if err != nil {
		log.Printf("[telemetry] POST error: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		log.Printf("[telemetry] POST %s returned %d: %s", req.URL, resp.StatusCode, string(b))
	}
}
