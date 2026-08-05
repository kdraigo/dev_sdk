package backtest

import (
	"encoding/json"
	"testing"
)

// TestQuoteAssetOf covers the guard around what used to be an unchecked index
// into strings.Split. A symbol without a slash took the whole strategy process
// down with a panic, which is a harsh response to a typo in a config file.
func TestQuoteAssetOf(t *testing.T) {
	valid := map[string]string{
		"BTC/USDT": "USDT",
		"ETH/USDT": "USDT",
		"SOL/BTC":  "BTC",
	}
	for symbol, want := range valid {
		got, err := quoteAssetOf(symbol)
		if err != nil {
			t.Errorf("quoteAssetOf(%q) returned an error: %v", symbol, err)
			continue
		}
		if got != want {
			t.Errorf("quoteAssetOf(%q) = %q, want %q", symbol, got, want)
		}
	}

	for _, symbol := range []string{"BTCUSDT", "", "/", "BTC/", "/USDT", "BTC/USDT/EXTRA"} {
		if _, err := quoteAssetOf(symbol); err == nil {
			t.Errorf("quoteAssetOf(%q) must return an error rather than panicking", symbol)
		}
	}
}

// TestParseOrderAck_CarriesBracketFields: without these a strategy receiving an
// order update cannot tell a stop fill from a limit fill at the same price, nor
// see which orders formed a mutually cancelling pair.
func TestParseOrderAck_CarriesBracketFields(t *testing.T) {
	raw := json.RawMessage(`{
		"exchange_id": 7,
		"pair": "BTC/USDT",
		"side": "SELL",
		"type": "STOP_LOSS_LIMIT",
		"status": "NEW",
		"price": 49000,
		"quantity": 0.5,
		"stop": 49500,
		"group_id": 42
	}`)

	order := parseOrderAck(raw)
	if order == nil {
		t.Fatal("expected an order")
	}

	if order.StopPrice != 49500 {
		t.Errorf("StopPrice = %v, want 49500", order.StopPrice)
	}
	if order.GroupID != "42" {
		t.Errorf("GroupID = %q, want \"42\"", order.GroupID)
	}
	if order.ID != "7" {
		t.Errorf("ID = %q, want \"7\"", order.ID)
	}
}

// TestParseOrderAck_PlainOrderHasNoBracketFields: absent stop/group must stay
// zero rather than being invented.
func TestParseOrderAck_PlainOrderHasNoBracketFields(t *testing.T) {
	raw := json.RawMessage(`{
		"exchange_id": 1,
		"pair": "BTC/USDT",
		"side": "BUY",
		"type": "LIMIT",
		"status": "FILLED",
		"price": 50000,
		"quantity": 1
	}`)

	order := parseOrderAck(raw)
	if order == nil {
		t.Fatal("expected an order")
	}

	if order.StopPrice != 0 {
		t.Errorf("StopPrice = %v, want 0", order.StopPrice)
	}
	if order.GroupID != "" {
		t.Errorf("GroupID = %q, want empty", order.GroupID)
	}
	// A filled ack carries no average_price/filled_qty, so they are derived.
	if order.FilledQty != 1 || order.AveragePrice != 50000 {
		t.Errorf("derived fill = (%v, %v), want (1, 50000)", order.FilledQty, order.AveragePrice)
	}
}
