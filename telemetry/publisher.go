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

// Publisher broadcasts SDK events to an external telemetry service.
// All methods must be non-blocking — implementations fire goroutines internally.
type Publisher interface {
	PublishOrder(order *types.Order)
	PublishBalance(account *types.Account)
}

// NewPublisher returns an HTTPPublisher when url is set, otherwise a NoOpPublisher.
func NewPublisher(sessionID, url, keyID, privateKey string) Publisher {
	if url == "" {
		return NoOpPublisher{}
	}
	return &httpPublisher{
		sessionID:  sessionID,
		baseURL:    url,
		keyID:      keyID,
		privateKey: privateKey,
		client:     &http.Client{Timeout: 5 * time.Second},
	}
}

// ── NoOp ─────────────────────────────────────────────────────────────────────

type NoOpPublisher struct{}

func (NoOpPublisher) PublishOrder(*types.Order)     {}
func (NoOpPublisher) PublishBalance(*types.Account) {}

// ── HTTP ──────────────────────────────────────────────────────────────────────

type httpPublisher struct {
	sessionID  string
	baseURL    string
	apiKey     string
	keyID      string
	privateKey string
	client     *http.Client
}

type telemetryPayload struct {
	SessionID string          `json:"session_id"`
	Exchange  string          `json:"exchange"`
	Symbol    string          `json:"symbol"`
	EventType string          `json:"event_type"`
	Order     *orderPayload   `json:"order,omitempty"`
	Balance   *balancePayload `json:"balance,omitempty"`
}

type orderPayload struct {
	OrderID       string    `json:"order_id"`
	ClientOrderID string    `json:"client_order_id"`
	Side          string    `json:"side"`
	Type          string    `json:"type"`
	Status        string    `json:"status"`
	Price         float64   `json:"price"`
	Qty           float64   `json:"qty"`
	FilledQty     float64   `json:"filled_qty"`
	AvgPrice      float64   `json:"avg_price"`
	Fee           float64   `json:"fee"`
	FeeAsset      string    `json:"fee_asset"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type balancePayload struct {
	Asset      string    `json:"asset"`
	Free       float64   `json:"free"`
	Locked     float64   `json:"locked"`
	RecordedAt time.Time `json:"recorded_at"`
}

func (p *httpPublisher) PublishOrder(order *types.Order) {
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
		},
	}
	go p.send(payload)
}

func (p *httpPublisher) PublishBalance(account *types.Account) {
	for _, b := range account.Balances {
		b := b
		payload := &telemetryPayload{
			SessionID: p.sessionID,
			Exchange:  account.Exchange,
			EventType: "balance",
			Balance: &balancePayload{
				Asset:      b.Asset,
				Free:       b.Free,
				Locked:     b.Lock,
				RecordedAt: time.Now().UTC(),
			},
		}
		go p.send(payload)
	}
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
