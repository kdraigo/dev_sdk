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
	"time"

	"github.com/kdraigo/flow_v1/dev_sdk/types"
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
	// Enabled reports whether telemetry is actually being sent. Heartbeat
	// goroutines should not start when this is false.
	Enabled() bool
}

// NewPublisher returns an httpPublisher when url is set, otherwise a NoOpPublisher.
// defaultExchange / defaultSymbol are stamped onto every payload that doesn't
// carry its own (heartbeat, initial_balance, balance, session_stopped) so the
// first ingest creates a live_sessions row with the right exchange/symbol —
// otherwise the frontend's chart sits on an empty symbol forever.
func NewPublisher(sessionID, url, keyID, privateKey, defaultExchange, defaultSymbol string) Publisher {
	if url == "" {
		return NoOpPublisher{}
	}
	return &httpPublisher{
		sessionID:       sessionID,
		baseURL:         url,
		keyID:           keyID,
		privateKey:      privateKey,
		defaultExchange: defaultExchange,
		defaultSymbol:   defaultSymbol,
		client:          &http.Client{Timeout: 5 * time.Second},
	}
}

// ── NoOp ─────────────────────────────────────────────────────────────────────

type NoOpPublisher struct{}

func (NoOpPublisher) PublishOrder(*types.Order, map[string]any, []string) {}
func (NoOpPublisher) PublishBalance(*types.Account)                       {}
func (NoOpPublisher) PublishInitialBalance(*types.Account)                {}
func (NoOpPublisher) PublishHeartbeat(HeartbeatMeta)                      {}
func (NoOpPublisher) PublishStopped(string)                               {}
func (NoOpPublisher) Enabled() bool                                       { return false }

// ── HTTP ──────────────────────────────────────────────────────────────────────

type httpPublisher struct {
	sessionID       string
	baseURL         string
	keyID           string
	privateKey      string
	defaultExchange string
	defaultSymbol   string
	client          *http.Client
}

func (p *httpPublisher) Enabled() bool { return true }

type telemetryPayload struct {
	SessionID string            `json:"session_id"`
	Exchange  string            `json:"exchange,omitempty"`
	Symbol    string            `json:"symbol,omitempty"`
	EventType string            `json:"event_type"`
	Order     *orderPayload     `json:"order,omitempty"`
	Balance   *balancePayload   `json:"balance,omitempty"`
	Balances  []balancePayload  `json:"balances,omitempty"`
	Heartbeat *heartbeatPayload `json:"heartbeat,omitempty"`
	Stopped   *stoppedPayload   `json:"stopped,omitempty"`
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
	payload := &telemetryPayload{
		SessionID: p.sessionID,
		Exchange:  order.Exchange,
		Symbol:    order.Symbol,
		EventType: "order",
		Order: &orderPayload{
			OrderID:   order.ID,
			Side:      string(order.Side),
			Type:      string(order.Type),
			Status:    string(order.Status),
			Price:     order.Price,
			Qty:       order.Quantity,
			FilledQty: order.FilledQty,
			AvgPrice:  order.AveragePrice,
			Fee:       order.Fee,
			FeeAsset:  order.FeeAsset,
			CreatedAt: order.CreatedAt,
			UpdatedAt: order.UpdatedAt,
			Reason:    reasonOut,
			Logs:      logsOut,
		},
	}
	go p.send(payload)
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
