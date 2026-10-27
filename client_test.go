package dev_sdk

import (
	"context"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

type mockAdapter struct {
	prepareCalled chan bool
	connectCalled chan bool
}

func (m *mockAdapter) PrepareSession(ctx context.Context, config *types.Config) error {
	m.prepareCalled <- true
	return nil
}

func (m *mockAdapter) ConnectStream(ctx context.Context, candleChan chan<- *types.Candle, orderChan chan<- *types.Order) error {
	m.connectCalled <- true
	return nil
}

func (m *mockAdapter) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	return nil, nil
}

func (m *mockAdapter) CancelOrder(ctx context.Context, exchange, symbol, orderID string) error {
	return nil
}

func (m *mockAdapter) GetAccount(ctx context.Context, exchange string, asset string) (*types.Account, error) {
	return &types.Account{}, nil
}

func (m *mockAdapter) Next(ctx context.Context) error {
	return nil
}

func (m *mockAdapter) GetHistoricalCandles(ctx context.Context, exchange, symbol string, from, to time.Time, tf types.Timeframe) ([]*types.Candle, error) {
	return nil, nil
}

func TestNewSDK(t *testing.T) {
	cfg := &types.Config{
		Environment: types.EnvBacktest,
	}

	sdk, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create SDK: %v", err)
	}

	if sdk.config != cfg {
		t.Error("SDK config not set correctly")
	}

	if sdk.adapter == nil {
		t.Error("SDK adapter should not be nil for backtest env")
	}
}

func TestSDK_Start(t *testing.T) {
	mock := &mockAdapter{
		prepareCalled: make(chan bool, 1),
		connectCalled: make(chan bool, 1),
	}
	sdk := &SDK{
		config:        &types.Config{Timeframes: []types.Timeframe{types.Timeframe1m}, Backtest: &types.BacktestOptions{}},
		adapter:       mock,
		rawCandleChan: make(chan *types.Candle, 10),
		orderChan:     make(chan *types.Order, 10),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = sdk.Start(ctx)
	}()

	select {
	case <-mock.prepareCalled:
		// Success
	case <-time.After(time.Second):
		t.Error("PrepareSession was not called")
	}

	select {
	case <-mock.connectCalled:
		// Success
	case <-time.After(time.Second):
		t.Error("ConnectStream was not called")
	}
}

func TestRoundUpToTimeframe_LandsOnBucketStarts(t *testing.T) {
	at := func(s string) time.Time {
		t.Helper()
		ts, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}
		return ts
	}
	cases := []struct {
		tf       types.Timeframe
		in, want string
	}{
		{types.Timeframe1h, "2026-09-02T10:30:00Z", "2026-09-02T11:00:00Z"},
		{types.Timeframe1d, "2026-09-02T00:00:00Z", "2026-09-02T00:00:00Z"},
		// Weeks start on Monday 00:00 UTC, as data_provider's 1w bars do.
		{types.Timeframe1w, "2026-09-02T00:00:00Z", "2026-09-07T00:00:00Z"},
		{types.Timeframe1w, "2026-08-31T00:00:00Z", "2026-08-31T00:00:00Z"},
		// Months start on the 1st, whatever their length. Truncating by 31
		// days gave 2026-09-19 here.
		{types.Timeframe1M, "2026-09-02T00:00:00Z", "2026-10-01T00:00:00Z"},
		{types.Timeframe1M, "2026-01-31T12:00:00Z", "2026-02-01T00:00:00Z"},
		{types.Timeframe1M, "2026-02-01T00:00:01Z", "2026-03-01T00:00:00Z"},
		{types.Timeframe1M, "2026-10-01T00:00:00Z", "2026-10-01T00:00:00Z"},
	}
	for _, c := range cases {
		if got := roundUpToTimeframe(at(c.in), c.tf); !got.Equal(at(c.want)) {
			t.Errorf("%s %s: got %s, want %s", c.tf, c.in, got.Format(time.RFC3339), c.want)
		}
	}
}

// startAdapter records the start the session was prepared with.
type startAdapter struct {
	mockAdapter
	prepared chan time.Time
}

func (a *startAdapter) PrepareSession(ctx context.Context, config *types.Config) error {
	a.prepared <- config.Backtest.StartTime
	return context.Canceled // the test needs nothing past it
}

// The session is prepared with the rounded start, so the engine streams from
// the boundary: rounded after, a run starting on a Wednesday got a partial
// first weekly bar.
func TestSDK_StartRoundsBeforeThePreparedSession(t *testing.T) {
	a := &startAdapter{prepared: make(chan time.Time, 1)}
	s := &SDK{
		config: &types.Config{Environment: types.EnvBacktest, Timeframes: []types.Timeframe{types.Timeframe1d, types.Timeframe1w},
			Backtest: &types.BacktestOptions{StartTime: time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)}},
		adapter:       a,
		rawCandleChan: make(chan *types.Candle, 1),
		orderChan:     make(chan *types.Order, 1),
	}
	_ = s.Start(context.Background())
	select {
	case got := <-a.prepared:
		if want := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
			t.Errorf("prepared with %s, want Monday %s", got, want)
		}
	default:
		t.Fatal("PrepareSession was not called")
	}
}
