package paper

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/exchange/live"
	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const minute = int64(60_000)

// started is a client attached to the mock, with its channels.
type started struct {
	c       *Client
	candles chan *types.Candle
	orders  chan *types.Order
	cancel  context.CancelFunc
}

func start(t *testing.T, m *mockEngine, cfg *types.Config) *started {
	t.Helper()
	c := NewClient(cfg)
	c.reconnectBase, c.callTimeout = 20*time.Millisecond, 2*time.Second
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	require.NoError(t, c.PrepareSession(ctx, cfg))
	s := &started{c: c, candles: make(chan *types.Candle, 100), orders: make(chan *types.Order, 100), cancel: cancel}
	require.NoError(t, c.ConnectStream(ctx, s.candles, s.orders))
	m.waitConnected(t)
	return s
}

func recvCandle(t *testing.T, ch chan *types.Candle) *types.Candle {
	t.Helper()
	select {
	case k := <-ch:
		return k
	case <-time.After(3 * time.Second):
		t.Fatal("no candle")
		return nil
	}
}

func recvOrder(t *testing.T, ch chan *types.Order) *types.Order {
	t.Helper()
	select {
	case o := <-ch:
		return o
	case <-time.After(3 * time.Second):
		t.Fatal("no order update")
		return nil
	}
}

func quiet[T any](t *testing.T, ch chan T, what string) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected %s: %+v", what, v)
	case <-time.After(150 * time.Millisecond):
	}
}

// A new run opens a signed account with the configured markets, balances
// and leverage, and the engine's candles reach the SDK as closed 1m bars.
func TestAttach(t *testing.T) {
	m := newMockEngine(t, types.ExchangeBinanceFutures)
	cfg := m.config(types.EnvPaperBinanceFutures)
	s := start(t, m, cfg)

	assert.Equal(t, "acc-1", s.c.AccountID())
	assert.Equal(t, "binance_futures", m.created["venue"])
	assert.Equal(t, []any{"BTCUSDT"}, m.created["symbols"])
	assert.Equal(t, map[string]any{"USDT": "20000"}, m.created["balances"])
	assert.Equal(t, 3, m.leverage["BTCUSDT"])
	assert.Equal(t, []string{"binance_futures"}, cfg.Live.RequestedExchanges, "the venue is the default exchange")
	assert.Equal(t, live.InstrumentFilter{TickSize: 0.1, StepSize: 0.001, MinQty: 0.001, MinNotional: 100}, s.c.filters["BTCUSDT"])

	open := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixMilli()
	m.pushCandle(open, 60_000)
	k := recvCandle(t, s.candles)
	assert.Equal(t, "BTCUSDT", k.Symbol)
	assert.Equal(t, "binance_futures", k.Exchange)
	assert.Equal(t, types.Timeframe1m, k.Timeframe)
	assert.True(t, k.IsComplete)
	assert.Equal(t, open, k.OpenTime.UnixMilli())
	assert.Equal(t, 60_000.0, k.Close)
	assert.Equal(t, 0.7, k.TakerBuyBaseVolume)
	assert.Equal(t, int64(42), k.TradeCount)
}

// A dropped connection is redialed. A bar sent twice reaches the SDK once,
// and the bars missed while away are read from the data API, in order. An
// order that filled while away is reported once.
func TestReattachWithoutDuplicates(t *testing.T) {
	m := newMockEngine(t, types.ExchangeBinanceFutures)
	s := start(t, m, m.config(types.EnvPaperBinanceFutures))
	ctx := context.Background()

	o, err := s.c.PlaceOrder(ctx, &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Type: types.OrderTypeLimit, Quantity: 0.01, Price: 59_000})
	require.NoError(t, err)
	assert.Equal(t, types.OrderStatusNew, o.Status)

	t0 := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixMilli()
	m.pushCandle(t0, 100)
	m.pushCandle(t0+minute, 101)
	recvCandle(t, s.candles)
	recvCandle(t, s.candles)

	m.mu.Lock()
	m.candles = []map[string]any{dataRow(t0+2*minute, 102), dataRow(t0+3*minute, 103)}
	ord := m.orders[101]
	ord["status"], ord["filled_quantity"], ord["avg_price"], ord["fee"] = "FILLED", "0.010", "59000", "0.12"
	m.mu.Unlock()
	m.dropConn()
	m.waitConnected(t)

	filled := recvOrder(t, s.orders)
	assert.Equal(t, "101", filled.ID)
	assert.Equal(t, types.OrderStatusFilled, filled.Status)
	assert.Equal(t, 0.12, filled.CumulativeFee)

	m.pushCandle(t0+minute, 101) // sent again after the reconnect
	m.pushCandle(t0+4*minute, 104)
	for _, want := range []int64{t0 + 2*minute, t0 + 3*minute, t0 + 4*minute} {
		k := recvCandle(t, s.candles)
		assert.Equal(t, want, k.OpenTime.UnixMilli())
		assert.Equal(t, want+59_999, k.CloseTime.UnixMilli(), "a read bar closes as a pushed one")
	}
	quiet(t, s.candles, "candle")

	// The engine's own push of the same fill, late, is not news.
	m.push("order_update", map[string]any{"order": ord})
	quiet(t, s.orders, "order update")
}

// An order whose answer was lost with the connection is sent again with the
// same client id, and the engine answers with the order the first made.
func TestIdempotentRetry(t *testing.T) {
	m := newMockEngine(t, types.ExchangeBinanceFutures)
	s := start(t, m, m.config(types.EnvPaperBinanceFutures))
	m.mu.Lock()
	m.dropOn = "order"
	m.mu.Unlock()

	o, err := s.c.PlaceOrder(context.Background(), &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy,
		Type: types.OrderTypeMarket, Quantity: 0.0104})
	require.NoError(t, err)
	assert.Equal(t, 2, m.count("order"), "sent twice")
	m.mu.Lock()
	var cids []any
	for _, r := range m.requests {
		if r["action"] == "order" {
			cids = append(cids, r["data"].(map[string]any)["client_order_id"])
		}
	}
	n := len(m.orders)
	m.mu.Unlock()
	assert.Equal(t, cids[0], cids[1], "with one client id")
	assert.NotEmpty(t, cids[0])
	assert.Equal(t, 1, n, "one order")
	assert.Equal(t, types.OrderStatusFilled, o.Status)
	assert.Equal(t, "0.010", m.lastRequest("order")["quantity"], "the size floors to the step")
}

// Order changes the engine pushes reach the SDK once each.
func TestPushedUpdates(t *testing.T) {
	m := newMockEngine(t, types.ExchangeBinanceFutures)
	s := start(t, m, m.config(types.EnvPaperBinanceFutures))
	o, err := s.c.PlaceOrder(context.Background(), &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideSell,
		Type: types.OrderTypeTakeProfitLimit, Quantity: 0.02, Price: 61_000.04, ReduceOnly: true})
	require.NoError(t, err)
	assert.Equal(t, types.OrderTypeTakeProfitLimit, o.Type, "the type the strategy asked for")
	req := m.lastRequest("order")
	assert.Equal(t, "LIMIT", req["type"], "a resting limit on the engine")
	assert.Equal(t, "61000.0", req["price"], "on the tick")
	assert.Equal(t, true, req["reduce_only"])

	m.mu.Lock()
	row := m.orders[101]
	part := copyRow(row)
	part["status"], part["filled_quantity"], part["fee"] = "PARTIALLY_FILLED", "0.010", "0.10"
	full := copyRow(row)
	full["status"], full["filled_quantity"], full["fee"], full["avg_price"] = "FILLED", "0.020", "0.20", "61000"
	m.mu.Unlock()
	fill := func(fee string) []any {
		return []any{map[string]any{"order_id": 101, "price": "61000", "quantity": "0.010", "fee": fee}}
	}
	m.push("order_update", map[string]any{"order": part, "fills": fill("0.10")})
	m.push("order_update", map[string]any{"order": part, "fills": fill("0.10")}) // again
	m.push("order_update", map[string]any{"order": full, "fills": fill("0.10")})
	m.push("order_update", map[string]any{"order": full})

	u1 := recvOrder(t, s.orders)
	assert.Equal(t, types.OrderStatusPartiallyFilled, u1.Status)
	assert.Equal(t, 0.01, u1.FilledQty)
	assert.Equal(t, 0.10, u1.Fee)
	assert.Equal(t, types.OrderTypeTakeProfitLimit, u1.Type)
	u2 := recvOrder(t, s.orders)
	assert.Equal(t, types.OrderStatusFilled, u2.Status)
	assert.Equal(t, 0.10, u2.Fee, "this update's fill")
	assert.Equal(t, 0.20, u2.CumulativeFee, "the order's total")
	assert.Equal(t, 61_000.0, u2.AveragePrice)
	quiet(t, s.orders, "order update")
}

// An order refused at placement is the error PlaceOrder returns, never an
// update too, as on an exchange.
func TestRefusalIsNotAnUpdate(t *testing.T) {
	m := newMockEngine(t, types.ExchangeBinanceFutures)
	s := start(t, m, m.config(types.EnvPaperBinanceFutures))
	_, err := s.c.PlaceOrder(context.Background(), &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy,
		Type: types.OrderTypeLimit, Quantity: 0.002, Price: 99_999})
	var rj *Rejected
	require.ErrorAs(t, err, &rj)
	assert.Equal(t, -4016, rj.Code)
	assert.Contains(t, rj.Msg, "Limit price can't be higher")
	quiet(t, s.orders, "order update")
}

// Orders map onto the engine's types as an exchange adapter maps them; the
// futures caps apply on paper, the arming gates do not.
func TestOrderMapping(t *testing.T) {
	m := newMockEngine(t, types.ExchangeBinanceFutures)
	s := start(t, m, m.config(types.EnvPaperBinanceFutures))
	ctx := context.Background()

	_, err := s.c.PlaceOrder(ctx, &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideSell, Type: types.OrderTypeStopLoss,
		Quantity: 0.01, StopPrice: 58_000.07, ReduceOnly: true, Reason: map[string]any{"why": "stop"}, Logs: []string{"armed"}})
	require.NoError(t, err)
	req := m.lastRequest("order")
	assert.Equal(t, "STOP_MARKET", req["type"])
	assert.Equal(t, "58000.1", req["stop_price"])
	assert.Equal(t, "MARK_PRICE", req["working_type"], "futures stops trigger on the mark, as the exchange adapter sends")
	assert.Equal(t, map[string]any{"why": "stop"}, req["reason"])
	assert.Equal(t, []any{"armed"}, req["logs"])

	_, err = s.c.PlaceOrder(ctx, &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Type: types.OrderTypeStopLossLimit,
		Quantity: 0.01, StopPrice: 61_000, Price: 61_050})
	require.NoError(t, err)
	assert.Equal(t, "STOP", m.lastRequest("order")["type"])
	assert.Equal(t, "GTC", m.lastRequest("order")["time_in_force"])

	_, err = s.c.PlaceOrder(ctx, &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Type: types.OrderTypeLimit, Quantity: 1, Price: 60_000})
	var na *live.ErrNotArmed
	require.ErrorAs(t, err, &na, "MaxOrderNotional applies on paper")
	assert.Equal(t, "cap", na.Gate)

	_, err = s.c.PlaceOrder(ctx, &types.OrderRequest{Symbol: "ETHUSDT", Side: types.OrderSideBuy, Type: types.OrderTypeMarket, Quantity: 1})
	assert.ErrorContains(t, err, "not ETHUSDT")

	err = s.c.SetLeverage(ctx, "binance_futures", "BTCUSDT", 20)
	require.ErrorAs(t, err, &na, "MaxLeverage applies on paper")
	require.NoError(t, s.c.SetLeverage(ctx, "binance_futures", "BTCUSDT", 5))
	assert.Equal(t, 5, m.leverage["BTCUSDT"])

	legs, err := s.c.PlaceBracket(ctx, &types.BracketRequest{Symbol: "BTCUSDT", Side: types.OrderSideSell, Quantity: 0.01,
		TakeProfitPrice: 62_000, StopPrice: 58_000})
	require.NoError(t, err)
	require.Len(t, legs, 2)
	assert.Equal(t, types.OrderTypeTakeProfitLimit, legs[0].Type)
	assert.Equal(t, types.OrderTypeStopLoss, legs[1].Type)
	assert.Equal(t, legs[0].GroupID, legs[1].GroupID)
	assert.NotEmpty(t, legs[0].GroupID)
	b := m.lastRequest("order_bracket")["legs"].([]any)
	assert.Equal(t, "LIMIT", b[0].(map[string]any)["type"])
	assert.Equal(t, true, b[0].(map[string]any)["reduce_only"])
	assert.Equal(t, "STOP_MARKET", b[1].(map[string]any)["type"])

	require.NoError(t, s.c.CancelOrder(ctx, "binance_futures", "BTCUSDT", legs[0].ID))
	var rj *Rejected
	err = s.c.CancelOrder(ctx, "binance_futures", "BTCUSDT", legs[0].ID)
	require.ErrorAs(t, err, &rj)
	assert.Equal(t, -2011, rj.Code)
	assert.ErrorIs(t, err, types.ErrOrderClosed, "as every adapter says it")
}

// Spot brackets are Binance's OCO: a LIMIT_MAKER and a stop, never
// reduce-only.
func TestSpotBracket(t *testing.T) {
	m := newMockEngine(t, "binance")
	cfg := m.config(types.EnvPaperBinance)
	cfg.Live.Leverage = nil
	s := start(t, m, cfg)
	assert.Empty(t, m.leverage, "no leverage on spot")
	_, err := s.c.PlaceBracket(context.Background(), &types.BracketRequest{Symbol: "BTCUSDT", Side: types.OrderSideSell, Quantity: 0.01,
		TakeProfitPrice: 62_000, StopPrice: 58_000, StopLimitPrice: 57_900})
	require.NoError(t, err)
	b := m.lastRequest("order_bracket")["legs"].([]any)
	tp, sl := b[0].(map[string]any), b[1].(map[string]any)
	assert.Equal(t, "LIMIT_MAKER", tp["type"])
	assert.Nil(t, tp["time_in_force"])
	assert.Nil(t, tp["reduce_only"])
	assert.Equal(t, "STOP_LOSS_LIMIT", sl["type"])
	assert.Nil(t, sl["working_type"], "spot stops trigger on the last trade")
}

// The reads: funds by bucket, positions with the engine's marks, the
// contract, realized P&L and open orders.
func TestReads(t *testing.T) {
	m := newMockEngine(t, types.ExchangeBinanceFutures)
	s := start(t, m, m.config(types.EnvPaperBinanceFutures))
	ctx := context.Background()

	acc, err := s.c.GetAccount(ctx, "binance_futures", "usdt")
	require.NoError(t, err)
	assert.Equal(t, []types.Balance{{Asset: "USDT", Free: 19_000, Lock: 1_000}}, acc.Balances)

	ps, err := s.c.GetPositions(ctx, "binance_futures")
	require.NoError(t, err)
	require.Len(t, ps, 1)
	assert.Equal(t, types.Position{Symbol: "BTCUSDT", Exchange: "binance_futures", Side: "LONG", Size: 0.01, EntryPrice: 60_000, Leverage: 3,
		IsolatedMargin: 400, RealizedPnL: 2, FundingPaid: 0.1, MarkPrice: 60_100, UnrealizedPnL: 1, LiquidationPrice: 40_000.5}, *ps[0])

	k, err := s.c.Contract(ctx, "binance_futures", "BTCUSDT")
	require.NoError(t, err)
	assert.Equal(t, types.Contract{Exchange: "binance_futures", Symbol: "BTCUSDT", TickSize: 0.1, StepSize: 0.001, MinQty: 0.001,
		MinNotional: 100, MaxLeverage: 125, MaintenanceMargin: 0.004, Mark: 60_010.5}, *k)

	pnl, err := s.c.RealizedPnL(ctx, "binance_futures", time.Now().Add(-time.Hour))
	require.NoError(t, err)
	assert.Equal(t, -12.5, pnl)

	_, err = s.c.PlaceOrder(ctx, &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Type: types.OrderTypeLimit, Quantity: 0.01, Price: 59_000})
	require.NoError(t, err)
	open, err := s.c.ListOpenOrders(ctx, "binance_futures", "BTCUSDT")
	require.NoError(t, err)
	require.Len(t, open, 1)
	got, err := s.c.GetOrder(ctx, "binance_futures", "BTCUSDT", open[0].ID)
	require.NoError(t, err)
	assert.Equal(t, 59_000.0, got.Price)

	t0 := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC).UnixMilli()
	m.mu.Lock()
	m.candles = []map[string]any{dataRow(t0, 1), dataRow(t0+minute, 2)}
	m.mu.Unlock()
	hist, err := s.c.GetHistoricalCandles(ctx, "binance_futures", "BTCUSDT", time.UnixMilli(t0), time.UnixMilli(t0+minute), types.Timeframe1m)
	require.NoError(t, err)
	require.Len(t, hist, 2)
	assert.Equal(t, 2.0, hist[1].Close)
	assert.Equal(t, t0+2*minute, hist[1].CloseTime.UnixMilli())
}

// The account stopped from the website ends the run, with the reason.
func TestAccountStopped(t *testing.T) {
	m := newMockEngine(t, types.ExchangeBinanceFutures)
	s := start(t, m, m.config(types.EnvPaperBinanceFutures))
	m.mu.Lock()
	m.status = "STOPPED"
	m.mu.Unlock()
	m.push("account_state", map[string]any{"status": "STOPPED", "stop_reason": "user"})
	select {
	case <-s.c.Ended():
	case <-time.After(3 * time.Second):
		t.Fatal("the run did not end")
	}
	assert.ErrorContains(t, s.c.StreamErr(), "stopped (user)")
}

// Resuming checks the account against the run, and the resume callback hears
// of the attach before any candle: as a cold start, then after a reconnect.
func TestResume(t *testing.T) {
	m := newMockEngine(t, types.ExchangeBinanceFutures)
	m.accountID, m.symbols = "acc-9", []string{"BTCUSDT", "ETHUSDT"}
	cfg := m.config(types.EnvPaperBinanceFutures)
	cfg.Paper.AccountID = "acc-9"

	c := NewClient(cfg)
	c.reconnectBase = 20 * time.Millisecond
	states := make(chan *types.SessionState, 4)
	c.SetOnResume(func(st *types.SessionState) {
		// A callback may call the account while candles wait.
		_, err := c.GetPositions(context.Background(), "")
		assert.NoError(t, err)
		time.Sleep(100 * time.Millisecond)
		states <- st
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, c.PrepareSession(ctx, cfg))
	assert.Nil(t, m.created, "no account opened")
	assert.Empty(t, m.leverage, "the position already has the configured leverage")
	candles := make(chan *types.Candle, 10)
	require.NoError(t, c.ConnectStream(ctx, candles, make(chan *types.Order, 10)))
	m.waitConnected(t)
	m.pushCandle(time.Now().Truncate(time.Minute).UnixMilli(), 1)

	select {
	case st := <-states:
		assert.True(t, st.ColdStart)
		assert.Equal(t, "acc-9", st.SessionID)
		require.Len(t, st.Positions, 1)
		assert.Equal(t, 0.01, st.Positions[0].Size)
		require.Len(t, st.Wallets, 1)
		assert.Equal(t, 19_000.0, st.Wallets[0].Balances[0].Free)
	case <-candles:
		t.Fatal("a candle before the resume callback returned")
	case <-time.After(3 * time.Second):
		t.Fatal("no resume callback")
	}
	recvCandle(t, candles)

	m.dropConn()
	m.waitConnected(t)
	st := <-states
	assert.False(t, st.ColdStart, "a reconnect")
	assert.True(t, st.Resumed)
}

func TestResumeRefusals(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(m *mockEngine, cfg *types.Config)
		want  string
	}{
		{"stopped", func(m *mockEngine, cfg *types.Config) { m.status = "STOPPED" }, "is stopped"},
		{"other markets", func(m *mockEngine, cfg *types.Config) { m.symbols = []string{"ETHUSDT"} }, "does not trade BTCUSDT"},
		{"other venue", func(m *mockEngine, cfg *types.Config) { m.venue = "binance" }, "trades binance, not binance_futures"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newMockEngine(t, types.ExchangeBinanceFutures)
			m.accountID, m.symbols = "acc-9", []string{"BTCUSDT"}
			cfg := m.config(types.EnvPaperBinanceFutures)
			cfg.Paper.AccountID = "acc-9"
			tc.setup(m, cfg)
			err := NewClient(cfg).PrepareSession(context.Background(), cfg)
			assert.ErrorContains(t, err, tc.want)
		})
	}
}

// The futures caps are required on paper as on an exchange; the arming
// gates are not.
func TestSessionGates(t *testing.T) {
	m := newMockEngine(t, types.ExchangeBinanceFutures)
	cfg := m.config(types.EnvPaperBinanceFutures)
	cfg.Live.MaxOrderNotional = 0
	var na *live.ErrNotArmed
	require.ErrorAs(t, NewClient(cfg).PrepareSession(context.Background(), cfg), &na)
	assert.Equal(t, "cap", na.Gate)

	cfg = m.config(types.EnvPaperBinanceFutures)
	cfg.Live.Armed = false
	require.NoError(t, NewClient(cfg).PrepareSession(context.Background(), cfg), "no arming on paper")

	cfg = m.config(types.EnvPaperBinanceFutures)
	cfg.Live.RequestedExchanges = []string{"bybit_futures"}
	assert.ErrorContains(t, NewClient(cfg).PrepareSession(context.Background(), cfg), "trades binance_futures only")

	cfg = m.config(types.EnvPaperBinanceFutures)
	cfg.Credentials.PrivateKey = ""
	assert.ErrorContains(t, NewClient(cfg).PrepareSession(context.Background(), cfg), "kdraigo API key")
}

// A connection refused for good (gone, someone else's key, stopped) ends the
// first connect at once.
func TestDialRefused(t *testing.T) {
	m := newMockEngine(t, types.ExchangeBinanceFutures)
	cfg := m.config(types.EnvPaperBinanceFutures)
	c := NewClient(cfg)
	require.NoError(t, c.PrepareSession(context.Background(), cfg))
	m.mu.Lock()
	m.status = "STOPPED"
	m.mu.Unlock()
	err := c.ConnectStream(context.Background(), make(chan *types.Candle), make(chan *types.Order))
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "status 409"), err.Error())
	assert.False(t, errors.Is(err, ErrNotConnected))
}

func copyRow(r map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range r {
		out[k] = v
	}
	return out
}
