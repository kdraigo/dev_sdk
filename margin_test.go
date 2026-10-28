package dev_sdk

import (
	"context"
	"errors"
	"testing"

	"github.com/kdraigo/dev_sdk/types"
)

// marginAdapter answers an account and positions the way the backtester does:
// the collateral's Free is available, its Lock is posted margin, and a
// synthetic base balance per position.
type marginAdapter struct {
	mockAdapter
	positions []*types.Position
}

func (m *marginAdapter) GetAccount(context.Context, string, string) (*types.Account, error) {
	return &types.Account{Exchange: "binance_futures", Balances: []types.Balance{
		{Asset: "USDT", Free: 7000, Lock: 3000},
		{Asset: "BTC", Free: 0.1, Lock: 2000},
	}}, nil
}

func (m *marginAdapter) GetPositions(context.Context, string) ([]*types.Position, error) {
	return m.positions, nil
}

func TestGetMargin_SumsTheAccountAndItsPositions(t *testing.T) {
	s := &SDK{adapter: &marginAdapter{positions: []*types.Position{
		{Symbol: "BTC/USDT", Side: "LONG", Size: 0.1, UnrealizedPnL: -250, FundingPaid: 12},
		{Symbol: "ETHUSDT", Side: "SHORT", Size: 1, UnrealizedPnL: 40, FundingPaid: -2},
		{Symbol: "BTC/USDC", Side: "LONG", Size: 1, UnrealizedPnL: 999},
	}}}
	m, err := s.GetMargin(context.Background(), "binance_futures", "USDT")
	if err != nil {
		t.Fatal(err)
	}
	want := types.Margin{Exchange: "binance_futures", Asset: "USDT", Available: 7000, InUse: 3000,
		Unrealized: -210, Equity: 9790, FundingPaid: 10, Positions: 2}
	if *m != want {
		t.Errorf("got %+v\nwant %+v", *m, want)
	}
}

// Without a position reader there is no margin to read: the same answer as
// GetPositions, which a strategy can test for.
func TestGetMargin_NeedsAPositionReader(t *testing.T) {
	s := &SDK{adapter: &mockAdapter{}}
	if _, err := s.GetMargin(context.Background(), "bybit", "USDT"); !errors.Is(err, ErrUnsupportedByAdapter) {
		t.Errorf("err = %v, want ErrUnsupportedByAdapter", err)
	}
}
