package live

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/adshao/go-binance/v2/futures"

	"github.com/kdraigo/dev_sdk/types"
)

// Binance reports each fill's commission ("n") and realized profit ("rp")
// alone. An exit filled in two trades must report the order's totals in
// CumulativeFee and RealizedPnL, count a fill delivered twice once, and keep
// Fee as the update's own commission for strategies that book it per update.
func TestHandleOrderUpdate_SumsFillsPerOrder(t *testing.T) {
	b := NewBinanceFuturesClient(&types.Config{Environment: types.EnvTestBinanceFutures})
	out := make(chan *types.Order, 8)
	original := map[string]string{"BTCUSDT": "BTC/USDT"}
	now := time.Now().UnixMilli()

	update := func(status futures.OrderStatusType, tradeID int64, last, total, fee, rp string) *futures.WsOrderTradeUpdate {
		return &futures.WsOrderTradeUpdate{
			Symbol: "BTCUSDT", ID: 42, Side: futures.SideTypeSell,
			Type: futures.OrderTypeLimit, OriginalType: futures.OrderTypeLimit,
			ExecutionType: futures.OrderExecutionTypeTrade, Status: status,
			OriginalQty: "0.002", OriginalPrice: "80000", AveragePrice: "80000",
			LastFilledQty: last, AccumulatedFilledQty: total,
			Commission: fee, CommissionAsset: "USDT", RealizedPnL: rp,
			TradeID: tradeID, TradeTime: now,
		}
	}
	near := func(t *testing.T, name string, got, want float64) {
		t.Helper()
		if math.Abs(got-want) > 1e-12 {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
	ctx := context.Background()

	first := update(futures.OrderStatusTypePartiallyFilled, 1001, "0.001", "0.001", "0.016", "2.5")
	b.handleOrderUpdate(ctx, first, original, out)
	o := <-out
	near(t, "first Fee", o.Fee, 0.016)
	near(t, "first CumulativeFee", o.CumulativeFee, 0.016)
	near(t, "first RealizedPnL", o.RealizedPnL, 2.5)

	b.handleOrderUpdate(ctx, first, original, out) // the same fill, delivered again
	o = <-out
	near(t, "duplicate CumulativeFee", o.CumulativeFee, 0.016)
	near(t, "duplicate RealizedPnL", o.RealizedPnL, 2.5)

	b.handleOrderUpdate(ctx, update(futures.OrderStatusTypeFilled, 1002, "0.001", "0.002", "0.017", "2.6"), original, out)
	o = <-out
	near(t, "second Fee (this update's own)", o.Fee, 0.017)
	near(t, "second CumulativeFee", o.CumulativeFee, 0.033)
	near(t, "second RealizedPnL", o.RealizedPnL, 5.1)

	b.mu.RLock()
	left := len(b.fills)
	b.mu.RUnlock()
	if left != 0 {
		t.Fatalf("a filled order's totals must be forgotten, %d left", left)
	}
}

// An update that is not a fill carries no trade id and adds nothing.
func TestHandleOrderUpdate_NonFillAddsNothing(t *testing.T) {
	b := NewBinanceFuturesClient(&types.Config{Environment: types.EnvTestBinanceFutures})
	out := make(chan *types.Order, 1)
	b.handleOrderUpdate(context.Background(), &futures.WsOrderTradeUpdate{
		Symbol: "BTCUSDT", ID: 7, Side: futures.SideTypeBuy,
		Type: futures.OrderTypeLimit, OriginalType: futures.OrderTypeLimit,
		ExecutionType: futures.OrderExecutionTypeNew, Status: futures.OrderStatusTypeNew,
		OriginalQty: "0.001", OriginalPrice: "70000", TradeTime: time.Now().UnixMilli(),
	}, map[string]string{"BTCUSDT": "BTC/USDT"}, out)
	o := <-out
	if o.CumulativeFee != 0 || o.RealizedPnL != 0 {
		t.Fatalf("a NEW update must carry no totals, got fee %v rp %v", o.CumulativeFee, o.RealizedPnL)
	}
	if len(b.fills) != 0 {
		t.Fatalf("a NEW update must not start tracking, got %d entries", len(b.fills))
	}
}
