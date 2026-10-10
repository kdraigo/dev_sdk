package telemetry

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/signing"
	"github.com/kdraigo/dev_sdk/types"
)

func TestTruncate_UnderCap_NoChange(t *testing.T) {
	reason := map[string]any{"rsi": 32, "signal": "oversold"}
	logs := []string{"line one", "line two"}
	gotR, gotL := truncateReasonAndLogs(reason, logs)
	if gotR["signal"] != "oversold" {
		t.Fatalf("reason unexpectedly mutated: %v", gotR)
	}
	if len(gotL) != 2 || gotL[0] != "line one" {
		t.Fatalf("logs unexpectedly mutated: %v", gotL)
	}
}

func TestTruncate_OverCapReason_ReplacedWithMarker(t *testing.T) {
	big := strings.Repeat("x", MaxReasonBytes+1)
	reason := map[string]any{"blob": big}
	gotR, _ := truncateReasonAndLogs(reason, nil)
	if gotR["_truncated"] != true {
		t.Fatalf("expected _truncated marker, got %v", gotR)
	}
	if _, ok := gotR["blob"]; ok {
		t.Fatalf("over-cap content should be dropped, got %v", gotR)
	}
}

func TestTruncate_OverCapLogTotal_AppendsTruncatedMarker(t *testing.T) {
	// 32 lines × ~600 bytes each = ~19.2 KB > 16 KB cap.
	logs := make([]string, MaxLogLineCount)
	for i := range logs {
		logs[i] = strings.Repeat("y", 600)
	}
	_, gotL := truncateReasonAndLogs(nil, logs)

	if len(gotL) >= len(logs) {
		t.Fatalf("expected truncation, got %d lines (input %d)", len(gotL), len(logs))
	}
	last := gotL[len(gotL)-1]
	if !strings.Contains(last, "[truncated") {
		t.Fatalf("expected truncation marker as last line, got %q", last)
	}

	// Total size must respect the cap.
	total := 0
	for _, ln := range gotL {
		total += len(ln)
	}
	if total > MaxLogsTotalBytes+len(last) {
		t.Fatalf("truncated logs still exceed cap: %d bytes", total)
	}
}

func TestTruncate_OverCapPerLine_LineIsTrimmed(t *testing.T) {
	long := strings.Repeat("z", MaxLogLineBytes*2)
	_, gotL := truncateReasonAndLogs(nil, []string{long})
	if len(gotL[0]) > MaxLogLineBytes {
		t.Fatalf("per-line trim failed: got %d bytes", len(gotL[0]))
	}
	if !strings.HasSuffix(gotL[0], "...") {
		t.Fatalf("expected '...' suffix on trimmed line, got %q", gotL[0][len(gotL[0])-3:])
	}
}

func TestTruncate_NilInputs_NilOutputs(t *testing.T) {
	r, l := truncateReasonAndLogs(nil, nil)
	if r != nil || l != nil {
		t.Fatalf("nil inputs should yield nil outputs, got %v / %v", r, l)
	}
}

func TestNewPublisher_NoURL_ReturnsNoOp(t *testing.T) {
	p := NewPublisher("sid", "", "k", "", "binance", "BTCUSDT")
	if _, ok := p.(NoOpPublisher); !ok {
		t.Fatalf("expected NoOpPublisher when URL is empty, got %T", p)
	}
	if p.Enabled() {
		t.Fatal("NoOpPublisher should not be enabled")
	}
}

func TestNewPublisher_WithURL_ReturnsHTTP(t *testing.T) {
	p := NewPublisher("sid", "http://localhost:5001", "k", "", "binance", "BTCUSDT")
	if !p.Enabled() {
		t.Fatal("httpPublisher should be enabled when URL is set")
	}
}

func TestBuildBalancesPayload_PreservesAssetsAndDefaults(t *testing.T) {
	hp := &httpPublisher{sessionID: "sid", defaultExchange: "binance", defaultSymbol: "ETHUSDT"}
	account := &types.Account{
		Exchange: "binance",
		Balances: []types.Balance{
			{Asset: "USDT", Free: 100, Lock: 0},
			{Asset: "BTC", Free: 0.5, Lock: 0},
		},
	}
	got := hp.buildBalancesPayload(account, "initial_balance")
	if got.EventType != "initial_balance" {
		t.Fatalf("event_type: %s", got.EventType)
	}
	if got.Symbol != "ETHUSDT" {
		t.Fatalf("symbol must come from publisher default, got %q", got.Symbol)
	}
	if got.Exchange != "binance" {
		t.Fatalf("exchange: %q", got.Exchange)
	}
	if len(got.Balances) != 2 {
		t.Fatalf("expected 2 balances, got %d", len(got.Balances))
	}
	b, _ := json.Marshal(got)
	if !strings.Contains(string(b), `"asset":"USDT"`) || !strings.Contains(string(b), `"asset":"BTC"`) {
		t.Fatalf("missing asset keys in payload: %s", string(b))
	}
}

func TestBuildBalancesPayload_AccountExchangeWins(t *testing.T) {
	hp := &httpPublisher{sessionID: "sid", defaultExchange: "binance", defaultSymbol: "BTCUSDT"}
	got := hp.buildBalancesPayload(&types.Account{Exchange: "bybit"}, "balance")
	if got.Exchange != "bybit" {
		t.Fatalf("account exchange should override default, got %q", got.Exchange)
	}
}

// capture starts a server that hands each telemetry body to the test.
func capture(t *testing.T) (*httpPublisher, <-chan telemetryPayload) {
	t.Helper()
	got := make(chan telemetryPayload, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p telemetryPayload
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			t.Errorf("decode: %v", err)
		}
		got <- p
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	return NewPublisher("sess-1", srv.URL, "", "", "binance_futures", "BTC/USDT").(*httpPublisher), got
}

func receive(t *testing.T, got <-chan telemetryPayload) *orderPayload {
	t.Helper()
	select {
	case p := <-got:
		return p.Order
	case <-time.After(5 * time.Second):
		t.Fatal("no telemetry received")
		return nil
	}
}

// Binance futures reports each fill's commission alone; live_trades keeps one
// row per order, so the publisher sends the order's total.
func TestPublishOrder_SendsOrderTotals(t *testing.T) {
	p, got := capture(t)
	p.PublishOrder(&types.Order{
		ID: "42", Symbol: "BTC/USDT", Exchange: "binance_futures", Side: types.OrderSideSell,
		Type: types.OrderTypeTakeProfitLimit, Status: types.OrderStatusFilled,
		Fee: 0.017, CumulativeFee: 0.033, FeeAsset: "USDT", RealizedPnL: 5.1,
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}, nil, nil)

	o := receive(t, got)
	if o.Fee != 0.033 {
		t.Fatalf("fee = %v, want the order's total 0.033", o.Fee)
	}
	if o.RealizedPnL != 5.1 {
		t.Fatalf("realized_pnl = %v, want 5.1", o.RealizedPnL)
	}
}

// The SDK's synthetic CANCELED carries only an id and a status. It must reach
// live_trades with the order's side, type, size and placement time.
func TestComplete_FillsSparseCancelFromLastPayload(t *testing.T) {
	p := NewPublisher("s", "http://example.invalid", "", "", "", "").(*httpPublisher)
	placed := time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)

	first := orderPayload{OrderID: "7", Side: "SELL", Type: "TAKE_PROFIT_LIMIT", Status: "NEW",
		Price: 80000, Qty: 0.002, CreatedAt: placed, UpdatedAt: placed, Reason: map[string]any{"x": 1}}
	p.complete(&first)
	partial := orderPayload{OrderID: "7", Side: "SELL", Type: "TAKE_PROFIT_LIMIT", Status: "PARTIALLY_FILLED",
		Price: 80000, Qty: 0.002, FilledQty: 0.001, AvgPrice: 80000, Fee: 0.016, FeeAsset: "USDT", RealizedPnL: 2.5,
		CreatedAt: placed, UpdatedAt: placed.Add(time.Minute)}
	p.complete(&partial)

	cancel := orderPayload{OrderID: "7", Status: "CANCELED"}
	p.complete(&cancel)

	if cancel.Side != "SELL" || cancel.Type != "TAKE_PROFIT_LIMIT" || cancel.Price != 80000 || cancel.Qty != 0.002 {
		t.Fatalf("identity not filled in: %+v", cancel)
	}
	if cancel.FilledQty != 0.001 || cancel.Fee != 0.016 || cancel.RealizedPnL != 2.5 {
		t.Fatalf("a cancel after a partial fill must keep the fill: %+v", cancel)
	}
	if !cancel.CreatedAt.Equal(placed) || cancel.UpdatedAt.IsZero() {
		t.Fatalf("times: created %v updated %v", cancel.CreatedAt, cancel.UpdatedAt)
	}
	if len(p.open) != 0 {
		t.Fatalf("a finished order must be forgotten, %d tracked", len(p.open))
	}
}

func TestComplete_KeepsNoReasonAndStaysBounded(t *testing.T) {
	p := NewPublisher("s", "http://example.invalid", "", "", "", "").(*httpPublisher)
	for i := 0; i < maxTrackedOrders+10; i++ {
		o := orderPayload{OrderID: strconv.Itoa(i), Status: "NEW", Reason: map[string]any{"big": i}, Logs: []string{"l"}}
		p.complete(&o)
	}
	if len(p.open) > maxTrackedOrders {
		t.Fatalf("tracked %d orders, cap is %d", len(p.open), maxTrackedOrders)
	}
	for _, o := range p.open {
		if o.Reason != nil || o.Logs != nil {
			t.Fatal("the open-order memory must not hold reasoning or logs")
		}
	}
}

// The bot needs the stop price to show a stop loss, and reduce-only to tell an
// exit from an entry; a sparse update must not lose either.
func TestPublishOrder_SendsStopPriceAndReduceOnly(t *testing.T) {
	p, got := capture(t)
	p.PublishOrder(&types.Order{
		ID: "algo:9", Symbol: "BTC/USDT", Exchange: "binance_futures", Side: types.OrderSideSell,
		Type: types.OrderTypeStopLoss, Status: types.OrderStatusNew, Quantity: 0.002,
		StopPrice: 85000, ReduceOnly: true, CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}, nil, nil)
	o := receive(t, got)
	if o.StopPrice != 85000 || !o.ReduceOnly {
		t.Fatalf("stop_price %v reduce_only %v, want 85000 true", o.StopPrice, o.ReduceOnly)
	}

	cancel := orderPayload{OrderID: "algo:9", Status: "CANCELED"}
	p.complete(&cancel)
	if cancel.StopPrice != 85000 || !cancel.ReduceOnly {
		t.Fatalf("sparse cancel lost stop_price/reduce_only: %v %v", cancel.StopPrice, cancel.ReduceOnly)
	}
}

func TestPublishSessionMeta_SendsEnvironment(t *testing.T) {
	got := make(chan telemetryPayload, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p telemetryPayload
		_ = json.NewDecoder(r.Body).Decode(&p)
		got <- p
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	p := NewPublisher("s", srv.URL, "", "", "binance_futures", "BTC/USDT", WithEnvironment("real_binance_futures"))
	// Even with no name or config, the environment alone is worth sending.
	p.PublishSessionMeta("", nil)

	select {
	case m := <-got:
		if m.EventType != "session_meta" || m.Meta == nil || m.Meta.Environment != "real_binance_futures" {
			t.Fatalf("got %+v", m)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no session_meta sent")
	}
}

// Telemetry is signed with version 2: a fresh nonce per event, the body by
// hash, over the path live_trades receives.
func TestPublish_SignsV2(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	type seen struct {
		nonce string
		ok    bool
	}
	got := make(chan seen, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		sig, _ := hex.DecodeString(r.Header.Get("X-Signature"))
		canonical := signing.Canonical(http.MethodPost, "/api/v1/telemetry", r.URL.Query(), r.Header.Get("X-Timestamp"), r.Header.Get("X-Nonce"), body)
		got <- seen{nonce: r.Header.Get("X-Nonce"), ok: r.Header.Get("X-Key-ID") == "key-1" && ed25519.Verify(pub, []byte(canonical), sig)}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p := NewPublisher("sess-1", srv.URL, "key-1", hex.EncodeToString(priv), "binance", "BTC/USDT").(*httpPublisher)
	order := &types.Order{ID: "1", Symbol: "BTC/USDT", Status: types.OrderStatusNew, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	p.PublishOrder(order, nil, nil)
	p.PublishOrder(order, nil, nil)

	var nonces []string
	for range 2 {
		select {
		case s := <-got:
			if !s.ok {
				t.Fatal("not a valid version 2 signature")
			}
			nonces = append(nonces, s.nonce)
		case <-time.After(5 * time.Second):
			t.Fatal("no telemetry received")
		}
	}
	if len(nonces[0]) != 32 || nonces[0] == nonces[1] {
		t.Fatalf("each event needs its own nonce: %q", nonces)
	}
}
