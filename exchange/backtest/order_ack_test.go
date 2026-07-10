package backtest

import (
	"encoding/json"
	"testing"

	"github.com/kdraigo/dev_sdk/types"
)

// TestParseOrderAck_FilledMarket pins the wire→types.Order mapping for a
// synchronous market fill: ID, Symbol, AveragePrice and FilledQty must be
// populated even though the engine omits average_price/filled_qty (D2).
func TestParseOrderAck_FilledMarket(t *testing.T) {
	// Wire shape from lib/core.Order (snake_case, numeric id, `pair`).
	raw := json.RawMessage(`{
		"id": 42,
		"exchange_id": 42,
		"pair": "BTC/USDT",
		"side": "buy",
		"type": "MARKET",
		"status": "FILLED",
		"price": 50000.0,
		"quantity": 0.1,
		"commission": 5.0
	}`)

	got := parseOrderAck(raw)
	if got == nil {
		t.Fatal("parseOrderAck returned nil")
	}
	if got.ID != "42" {
		t.Errorf("ID = %q, want \"42\"", got.ID)
	}
	if got.Symbol != "BTC/USDT" {
		t.Errorf("Symbol = %q, want \"BTC/USDT\"", got.Symbol)
	}
	if got.Side != types.OrderSideBuy {
		t.Errorf("Side = %q, want BUY", got.Side)
	}
	if got.Type != types.OrderTypeMarket {
		t.Errorf("Type = %q, want MARKET", got.Type)
	}
	if got.Status != types.OrderStatusFilled {
		t.Errorf("Status = %q, want FILLED", got.Status)
	}
	if got.Price != 50000.0 {
		t.Errorf("Price = %v, want 50000", got.Price)
	}
	if got.AveragePrice != got.Price {
		t.Errorf("AveragePrice = %v, want == Price (%v)", got.AveragePrice, got.Price)
	}
	if got.FilledQty != got.Quantity {
		t.Errorf("FilledQty = %v, want == Quantity (%v)", got.FilledQty, got.Quantity)
	}
	if got.Fee != 5.0 {
		t.Errorf("Fee = %v, want 5.0 (from commission)", got.Fee)
	}
}

// TestParseOrderAck_IDFallback verifies the ID falls back to `id` when
// `exchange_id` is absent.
func TestParseOrderAck_IDFallback(t *testing.T) {
	raw := json.RawMessage(`{"id": 7, "pair": "ETH/USDT", "status": "NEW", "price": 3000, "quantity": 1}`)
	got := parseOrderAck(raw)
	if got == nil {
		t.Fatal("parseOrderAck returned nil")
	}
	if got.ID != "7" {
		t.Errorf("ID = %q, want \"7\"", got.ID)
	}
	// Non-filled order: AveragePrice/FilledQty stay zero.
	if got.AveragePrice != 0 || got.FilledQty != 0 {
		t.Errorf("non-filled order should not derive fill fields; got avg=%v filled=%v", got.AveragePrice, got.FilledQty)
	}
}
