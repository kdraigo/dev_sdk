package telemetry

import (
	"encoding/json"
	"strings"
	"testing"

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
