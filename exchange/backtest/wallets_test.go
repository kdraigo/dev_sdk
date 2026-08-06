package backtest

import (
	"testing"

	"github.com/google/uuid"
	"github.com/kdraigo/dev_sdk/types"
)

func totalBalance(wallets []startSessionRequestWallet) float64 {
	var total float64
	for _, w := range wallets {
		total += w.Balance
	}
	return total
}

// TestBuildWallets_SingleExchangeShorthand: with one exchange the flat form is
// unambiguous and stays supported, because it is what nearly every strategy uses.
func TestBuildWallets_SingleExchangeShorthand(t *testing.T) {
	wallets, err := buildWallets(uuid.New(), &types.BacktestOptions{
		RequestedExchanges: []string{"binance"},
		Wallets:            map[string]float64{"USDT": 10000},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(wallets) != 1 {
		t.Fatalf("expected 1 wallet, got %d", len(wallets))
	}
	if wallets[0].Exchange != "binance" || wallets[0].Balance != 10000 {
		t.Errorf("got %+v", wallets[0])
	}
}

// TestBuildWallets_PerExchangeSumsToTotal is the semantic that matters: funds
// sit at a venue, and overall capital is the sum of the per-venue balances.
func TestBuildWallets_PerExchangeSumsToTotal(t *testing.T) {
	wallets, err := buildWallets(uuid.New(), &types.BacktestOptions{
		RequestedExchanges: []string{"binance", "bybit"},
		WalletsByExchange: map[string]map[string]float64{
			"binance": {"USDT": 10000},
			"bybit":   {"USDT": 5000},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := totalBalance(wallets); got != 15000 {
		t.Errorf("total capital = %v, want 15000", got)
	}

	perExchange := map[string]float64{}
	for _, w := range wallets {
		perExchange[w.Exchange] += w.Balance
	}
	if perExchange["binance"] != 10000 {
		t.Errorf("binance = %v, want 10000 (only this is spendable there)", perExchange["binance"])
	}
	if perExchange["bybit"] != 5000 {
		t.Errorf("bybit = %v, want 5000", perExchange["bybit"])
	}
}

// TestBuildWallets_FlatFormRejectedAcrossExchanges is the regression guard. The
// old code applied each Wallets entry to every exchange, so this configuration
// silently produced 20000 of buying power instead of the 10000 that was written.
func TestBuildWallets_FlatFormRejectedAcrossExchanges(t *testing.T) {
	_, err := buildWallets(uuid.New(), &types.BacktestOptions{
		RequestedExchanges: []string{"binance", "bybit"},
		Wallets:            map[string]float64{"USDT": 10000},
	})
	if err == nil {
		t.Fatal("a flat wallet across several exchanges must be rejected as ambiguous, not duplicated")
	}
}

func TestBuildWallets_RejectsContradictoryConfig(t *testing.T) {
	_, err := buildWallets(uuid.New(), &types.BacktestOptions{
		RequestedExchanges: []string{"binance"},
		Wallets:            map[string]float64{"USDT": 10000},
		WalletsByExchange:  map[string]map[string]float64{"binance": {"USDT": 5000}},
	})
	if err == nil {
		t.Fatal("setting both wallet forms must be rejected")
	}
}

func TestBuildWallets_RejectsMissingBalances(t *testing.T) {
	if _, err := buildWallets(uuid.New(), &types.BacktestOptions{
		RequestedExchanges: []string{"binance"},
	}); err == nil {
		t.Fatal("a session with no starting balance must be rejected")
	}
}

// TestBuildWallets_RejectsUnrequestedExchange catches a typo that would
// otherwise leave the strategy with no funds where it actually trades.
func TestBuildWallets_RejectsUnrequestedExchange(t *testing.T) {
	_, err := buildWallets(uuid.New(), &types.BacktestOptions{
		RequestedExchanges: []string{"binance"},
		WalletsByExchange:  map[string]map[string]float64{"bybit": {"USDT": 5000}},
	})
	if err == nil {
		t.Fatal("funding an exchange that is not being streamed must be rejected")
	}
}

// TestBuildWallets_IsDeterministic: the session request is signed, so an
// unstable wallet order would change the payload between identical runs.
func TestBuildWallets_IsDeterministic(t *testing.T) {
	opts := &types.BacktestOptions{
		RequestedExchanges: []string{"binance", "bybit"},
		WalletsByExchange: map[string]map[string]float64{
			"binance": {"USDT": 10000, "BTC": 1},
			"bybit":   {"USDT": 5000, "ETH": 2},
		},
	}

	id := uuid.New()
	first, err := buildWallets(id, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for i := 0; i < 20; i++ {
		next, err := buildWallets(id, opts)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		for j := range first {
			if next[j] != first[j] {
				t.Fatalf("wallet order is not stable at index %d: %+v vs %+v", j, next[j], first[j])
			}
		}
	}
}
