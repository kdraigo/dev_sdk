package backtest

import "testing"

// A position is valued at its pair's last streamed close, the price the
// engine itself values it at. Before any candle of the pair (a cold resume)
// it has no mark.
func TestPosition_ValuedAtTheLastClose(t *testing.T) {
	e := &EngineClient{}
	e.lastClose.Store("binance_futures|BTC/USDT", 51000.0)

	long := e.position("binance_futures", positionWire{Pair: "BTC/USDT", Side: "LONG", Size: 2, EntryPrice: 50000, FundingPaid: 3})
	if long.MarkPrice != 51000 || long.UnrealizedPnL != 2000 {
		t.Errorf("long: mark %v unrealized %v, want 51000 and 2000", long.MarkPrice, long.UnrealizedPnL)
	}
	if long.Exchange != "binance_futures" || long.FundingPaid != 3 {
		t.Errorf("long: %+v", long)
	}

	short := e.position("binance_futures", positionWire{Pair: "BTC/USDT", Side: "SHORT", Size: 2, EntryPrice: 50000})
	if short.UnrealizedPnL != -2000 {
		t.Errorf("short: unrealized %v, want -2000", short.UnrealizedPnL)
	}

	other := e.position("bybit_futures", positionWire{Pair: "BTC/USDT", Side: "LONG", Size: 1, EntryPrice: 50000})
	if other.MarkPrice != 0 || other.UnrealizedPnL != 0 {
		t.Errorf("no close seen on that exchange: %+v", other)
	}
}
