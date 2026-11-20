package live

import (
	"fmt"
	"os"
	"strings"

	"github.com/kdraigo/dev_sdk/types"
)

// ArmedEnvVar must be set to "1" in the process environment before a mainnet
// futures order is allowed out. It is deliberately separate from the config: a
// config file can be committed, copied between machines, or inherited from an
// example, and none of those should be able to arm a machine on their own.
const ArmedEnvVar = "KDRAIGO_LIVE_ARMED"

// Guard decides whether an order may leave the process.
//
// The gates are independent on purpose. Any one of them alone would be a single
// point of failure, and the failure mode being guarded against — an unintended
// mainnet order on an account that also holds spot funds and open orders — is
// not one worth being clever about.
type Guard struct {
	Env  types.Environment
	Opts *types.LiveOptions

	// Getenv is injected so the environment gate is testable without mutating
	// the real process environment, which leaks between parallel tests.
	// Nil means os.Getenv.
	Getenv func(string) string
}

func (g Guard) getenv(key string) string {
	if g.Getenv != nil {
		return g.Getenv(key)
	}
	return os.Getenv(key)
}

// MainnetEnvironments are the environment values that reach real funds.
func MainnetEnvironments() []types.Environment {
	return []types.Environment{
		types.EnvRealBinance,
		types.EnvRealBybit,
		types.EnvRealBinanceFutures,
		types.EnvRealBybitFutures,
	}
}

// IsMainnet reports whether the environment places orders against real money.
func IsMainnet(env types.Environment) bool {
	for _, e := range MainnetEnvironments() {
		if env == e {
			return true
		}
	}
	return false
}

// IsFutures reports whether the environment is a perpetual-futures one, on any
// venue. The caps and the arming gates apply to all of them; an adapter checks
// its own venue with IsBinanceFutures or IsBybitFutures.
//
// Paper futures count: the caps apply there too, so a config that runs on
// paper runs on mainnet unchanged. The arming gates do not, since paper holds
// no real funds (IsMainnet is false).
func IsFutures(env types.Environment) bool {
	return IsBinanceFutures(env) || IsBybitFutures(env) || env == types.EnvPaperBinanceFutures
}

// IsPaper reports whether the environment is kdraigo's paper exchange.
func IsPaper(env types.Environment) bool {
	return env == types.EnvPaperBinance || env == types.EnvPaperBinanceFutures
}

// IsBinanceFutures reports whether the environment is Binance USDⓈ-M perpetuals.
func IsBinanceFutures(env types.Environment) bool {
	return env == types.EnvRealBinanceFutures || env == types.EnvTestBinanceFutures
}

// IsBybitFutures reports whether the environment is Bybit USDT perpetuals.
func IsBybitFutures(env types.Environment) bool {
	return env == types.EnvRealBybitFutures || env == types.EnvTestBybitFutures
}

// ErrNotArmed is returned when a gate refuses an order. Callers can test for it
// rather than string-matching, and a strategy can distinguish "I am not allowed
// to trade" from "the exchange rejected this".
type ErrNotArmed struct {
	Gate   string
	Detail string
}

func (e *ErrNotArmed) Error() string {
	return fmt.Sprintf("live trading refused at the %s gate: %s", e.Gate, e.Detail)
}

// CheckSession validates the arming configuration once, at PrepareSession, so a
// misconfigured run fails before it has streamed a single candle rather than at
// the first signal — possibly hours later, and possibly in a state where the
// strategy has already sized a position it cannot place.
func (g Guard) CheckSession() error {
	if !IsFutures(g.Env) {
		return nil
	}
	if g.Opts == nil {
		return &ErrNotArmed{Gate: "config", Detail: "Config.Live is nil; futures trading needs LiveOptions with explicit caps"}
	}

	// Gate 4 applies on testnet too. Caps that are only exercised on mainnet
	// are caps nobody has tested.
	if g.Opts.MaxOrderNotional <= 0 {
		return &ErrNotArmed{
			Gate:   "cap",
			Detail: "LiveOptions.MaxOrderNotional must be set to a positive quote-currency limit; there is deliberately no default, so an unset cap cannot pass for an unlimited one",
		}
	}
	if g.Opts.MaxLeverage <= 0 {
		return &ErrNotArmed{
			Gate:   "cap",
			Detail: "LiveOptions.MaxLeverage must be set to a positive limit; there is deliberately no default",
		}
	}
	for pair, lev := range g.Opts.Leverage {
		if lev > g.Opts.MaxLeverage {
			return &ErrNotArmed{
				Gate:   "cap",
				Detail: fmt.Sprintf("configured leverage %vx on %s exceeds MaxLeverage %vx", lev, pair, g.Opts.MaxLeverage),
			}
		}
	}

	if !IsMainnet(g.Env) {
		return nil
	}
	if !g.Opts.Armed {
		return &ErrNotArmed{
			Gate:   "armed",
			Detail: "LiveOptions.Armed is false; set it explicitly to trade real funds on " + string(g.Env),
		}
	}
	if strings.TrimSpace(g.getenv(ArmedEnvVar)) != "1" {
		return &ErrNotArmed{
			Gate:   "environment",
			Detail: ArmedEnvVar + "=1 must be set in the process environment; config alone cannot arm a machine",
		}
	}
	return nil
}

// CheckOrder re-runs the gates for one order and adds the notional cap, which
// can only be evaluated against a concrete request.
//
// refPrice values a market order. A market order with no reference price is
// refused rather than waved through: an order whose size cannot be checked is
// exactly the one the cap exists for.
//
// A reduce-only order is not capped: it can only shrink a position (the venue
// cuts it to the position's size), never add exposure, which is what the cap
// limits. Capped, it refused a position's own exits: a long's take-profit or
// a short's stop is valued above the entry, past a cap sized to the position
// (wiki bug #69: the executor opened, could not place the take-profit, and
// had to close at market). A close is never blocked for want of a price.
func (g Guard) CheckOrder(intent *OrderIntent, refPrice float64) error {
	if err := g.CheckSession(); err != nil {
		return err
	}
	if !IsFutures(g.Env) {
		return nil
	}
	if intent == nil {
		return fmt.Errorf("intent must not be nil")
	}
	if intent.ReduceOnly {
		return nil
	}

	price := intent.Price
	if price <= 0 {
		price = intent.StopPrice
	}
	if price <= 0 {
		price = refPrice
	}
	if price <= 0 {
		return &ErrNotArmed{
			Gate:   "cap",
			Detail: fmt.Sprintf("cannot value a %s order on %s: no limit price, no trigger and no reference price, so MaxOrderNotional cannot be enforced", intent.Type, intent.Symbol),
		}
	}

	notional := price * intent.Quantity
	if notional > g.Opts.MaxOrderNotional {
		return &ErrNotArmed{
			Gate:   "cap",
			Detail: fmt.Sprintf("order notional %.2f exceeds MaxOrderNotional %.2f (%v %s at %v)", notional, g.Opts.MaxOrderNotional, intent.Quantity, intent.Symbol, price),
		}
	}
	return nil
}

// DryRun reports whether orders should be logged rather than sent.
func (g Guard) DryRun() bool {
	return g.Opts != nil && g.Opts.DryRun
}
