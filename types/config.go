package types

import "time"

// Environment dictates the context in which the SDK operates.
type Environment string

const (
	EnvBacktest    Environment = "backtest"
	EnvRealBinance Environment = "real_binance"
	EnvRealBybit   Environment = "real_bybit"
	EnvTestBinance Environment = "test_binance"
	EnvTestBybit   Environment = "test_bybit"

	// Perpetual futures are their own environments rather than a flag on the
	// spot ones. The venue is a different API, a different wallet and a
	// different set of things that can go wrong, and the exchange identifier
	// "binance_futures" already names it everywhere else on the platform —
	// ClickHouse candle tables, contract specs, funding and mark price.
	//
	// A flag would also mean one misread boolean is the distance between a
	// spot account holding real funds and a futures one.
	EnvRealBinanceFutures Environment = "real_binance_futures"
	EnvTestBinanceFutures Environment = "test_binance_futures"
)

// ExchangeBinanceFutures is the platform-wide identifier for Binance
// USDⓈ-M perpetuals. It is the same string data_provider collects under and the
// backtester keys contract specs by, so a backtest and a live run name the
// venue identically.
const ExchangeBinanceFutures = "binance_futures"

// Timeframe dictates the period of time each candle covers.
type Timeframe string

const (
	Timeframe1m  Timeframe = "1m"
	Timeframe3m  Timeframe = "3m"
	Timeframe5m  Timeframe = "5m"
	Timeframe15m Timeframe = "15m"
	Timeframe30m Timeframe = "30m"
	Timeframe1h  Timeframe = "1h"
	Timeframe2h  Timeframe = "2h"
	Timeframe4h  Timeframe = "4h"
	Timeframe1d  Timeframe = "1d"

	// Note the casing: lowercase m is minute, uppercase M is month.
	Timeframe2m Timeframe = "2m"
	Timeframe1w Timeframe = "1w"
	Timeframe1M Timeframe = "1M"
)

// Config is the main configuration object provided by user to initialize the strategy bot SDK.
type Config struct {
	Environment Environment // The environment to connect to.
	Timeframes  []Timeframe // One or more aggregate candle sizes the strategy subscribes to (e.g. [15m, 1h]).
	Indicators  []string    // Formatted indicator names (e.g. "EMA10", "RSI14") to pre-calculate.
	// IndicatorHistory caps the rolling candle history retained per series by the
	// indicator manager. Bounds memory and per-call compute on long-running
	// sessions. Default (when 0) is 1500. Raise it only if you request indicator
	// periods approaching that window.
	IndicatorHistory int
	Credentials      Credentials      // API Keys for specific real/testnet exchanges
	Backtest         *BacktestOptions // Specific settings required when configuring a new backtest engine session.
	Live             *LiveOptions     // Specific settings required when configuring a real exchange stream.
}

// Credentials holds sensitive authentication data for exchange APIs and platform access.
type Credentials struct {
	APIKey    string // Exchange API Key
	APISecret string // Exchange API Secret

	// Kdraigo Platform API Key
	KeyID      string // API Key ID (UUID)
	PrivateKey string // Ed25519 Private Key (Hex)
}

// BacktestOptions contains configuration necessary to prepare the engine session for backtesting.
type BacktestOptions struct {
	Endpoint           string   // Engine API URL e.g. "http://localhost:8080"
	SessionName        string   // Human readable name for the backtesting run.
	RequestedExchanges []string // List of exchanges to pull historical data against (e.g., "binance").
	Assets             []string // Trading pairs requested (e.g., "BTC/USDT", "ETH/USDT").
	// Wallets is the starting balance per asset, for a single-exchange session.
	// Key is the asset symbol (e.g. "USDT"), value is the amount.
	//
	// Funds belong to an exchange: money at binance cannot be spent on bybit.
	// With exactly one requested exchange that is unambiguous, so this shorthand
	// is fine. With several, use WalletsByExchange — this field previously
	// applied the same balance to *every* exchange, so a two-exchange session
	// silently ran with double the intended capital.
	Wallets map[string]float64

	// WalletsByExchange allocates starting balances per exchange, keyed by
	// exchange then asset. Total session capital is the sum:
	//
	//	{"binance": {"USDT": 10000}, "bybit": {"USDT": 5000}}
	//
	// is 15000 overall, of which only 10000 is spendable on binance. Set this
	// or Wallets, not both.
	WalletsByExchange map[string]map[string]float64

	// MarketTypeByExchange selects the execution model per exchange:
	// "spot" (the default) or "linear_perp" for USDT-margined perpetuals.
	//
	// Per exchange rather than per session, so one run can hold spot and
	// futures side by side — cash-and-carry, or hedging spot with a perp.
	// An exchange absent from this map is spot.
	MarketTypeByExchange map[string]string

	// LeverageByExchange sets the starting leverage for a futures wallet.
	// Absent means the session default (1x, i.e. unleveraged unless asked).
	// Leverage can also be changed per pair mid-run via Session.SetLeverage.
	LeverageByExchange map[string]float64
	StartTime          time.Time // Historic start time for data stream.
	EndTime            time.Time // Historic end time for data stream.

	// Simulation overrides the engine's execution assumptions. Leave nil for
	// the defaults, which are the conservative ones.
	Simulation *SimulationOptions
}

// SimulationOptions are the execution assumptions a backtest runs under. They
// are recorded on the session, so a stored result stays interpretable after the
// defaults change.
//
// Every field is optional: a nil pointer means "use the engine default". They
// are pointers rather than plain values so that asking for zero fees — which is
// how results produced before fees were charged can be reproduced — is
// distinguishable from not asking at all.
type SimulationOptions struct {
	// FillPolicy decides which order fills first when one candle triggers
	// several. "pessimistic" (the default) fills the adverse order first;
	// "optimistic" the favourable one; "creation_order" reproduces the
	// behaviour from before this option existed.
	FillPolicy string `json:"fill_policy,omitempty"`

	// GapFills books a stop that gapped through its trigger at the bar's open
	// rather than at the stop price. Defaults to true.
	GapFills *bool `json:"gap_fills,omitempty"`

	// Fees as a fraction, e.g. 0.001 for 0.1%. Both default to Binance spot.
	MakerFee *float64 `json:"maker_fee,omitempty"`
	TakerFee *float64 `json:"taker_fee,omitempty"`

	// Futures fees, separate from the spot pair above rather than overriding
	// it: a mixed session runs both schedules at once. Default to Binance
	// USDⓈ-M, where maker and taker differ by 2.5x — charging one for the
	// other is a material error, unlike on spot where they usually match.
	FuturesMakerFee *float64 `json:"futures_maker_fee,omitempty"`
	FuturesTakerFee *float64 `json:"futures_taker_fee,omitempty"`

	// DefaultLeverage applies to futures wallets that do not set their own.
	// Defaults to 1: unleveraged unless explicitly asked for.
	DefaultLeverage *float64 `json:"default_leverage,omitempty"`

	// MarginMode and PositionMode exist to be validated. v1 supports
	// "isolated" margin and "one_way" positions only; anything else is
	// rejected at session creation rather than half-supported.
	MarginMode   string `json:"margin_mode,omitempty"`
	PositionMode string `json:"position_mode,omitempty"`

	// ContractSpecsVersion pins the maintenance-margin ladder, so a stored
	// result stays interpretable after the exchange revises its tiers. Empty
	// means the newest capture the engine has embedded.
	ContractSpecsVersion string `json:"contract_specs_version,omitempty"`

	// FundingEnabled defaults to true. Turning it off is allowed for isolating
	// its effect, but it must be a deliberate choice: omitting funding is a
	// directional bias, not a simplification — it flatters whichever side was
	// being paid over the window.
	FundingEnabled *bool `json:"funding_enabled,omitempty"`

	// MarkPriceSource selects what liquidation triggers on: "mark_series"
	// (the default, and what a real exchange uses) or "trade_close".
	//
	// The difference is not academic. Measured on production data the two
	// series diverge by up to 1.3% at the bar low, and on ~3% of bars the mark
	// wicks below the trade low — liquidations that trade prices miss
	// entirely. Runs that fall back are counted and recorded on the session.
	MarkPriceSource string `json:"mark_price_source,omitempty"`
}

// LiveOptions contains configuration necessary to hook onto live order books and websocket endpoints.
type LiveOptions struct {
	RequestedExchanges []string // List of real exchanges to connect to (e.g., "binance", "bybit").
	Assets             []string // Trading pairs requested (e.g., "BTCUSDT", "ETHUSDT"). Note: Binance requires no slashes usually.
	TelemetryURL       string   // Base URL of the live_trades service (e.g. "http://localhost:5001"). Empty = disabled.
	TelemetryAPIKey    string   // X-API-Key for live_trades service authentication.
	SessionID          string   // Optional fixed telemetry session id. Empty = a new UUID is generated; set to resume/extend an existing session.
	// StrategyName is a human-readable label for the run, the live counterpart
	// of BacktestOptions.SessionName. Published once at Start; without it the
	// console can only identify a session by its UUID.
	StrategyName string
	// StrategyConfig is an optional snapshot of the parameters the strategy was
	// started with (timeframes, thresholds, risk settings...). Stored verbatim
	// for display — the platform never interprets it. Capped at 8 KB encoded.
	// Do not put secrets here; it is readable from the console.
	StrategyConfig map[string]any

	// ── Perpetual futures ────────────────────────────────────────────────
	// The fields below apply to the *_binance_futures environments and are
	// ignored on spot.

	// Leverage is the starting leverage per pair, applied at PrepareSession.
	// A pair absent from the map keeps whatever the exchange account already
	// has — the SDK does not guess, because guessing 1x on an account set to
	// 20x would silently resize every position the strategy opens.
	Leverage map[string]float64

	// Armed must be set explicitly before any order reaches a mainnet futures
	// venue. It defaults to false so an unmodified config cannot trade, and it
	// is only one of the gates: KDRAIGO_LIVE_ARMED=1 must also be present in
	// the process environment.
	Armed bool

	// DryRun logs the exact request that would be sent and returns a synthetic
	// acknowledgement instead of placing anything. This is how a strategy is
	// exercised against real market data without touching the account.
	DryRun bool

	// MaxOrderNotional caps a single order's quote-currency value. Required
	// for futures — there is no default, so forgetting to set one is a refusal
	// rather than an unlimited cap.
	MaxOrderNotional float64

	// MaxLeverage caps leverage on any pair, checked both against the Leverage
	// map at session start and against SetLeverage calls at runtime. Required
	// for futures, for the same reason as MaxOrderNotional.
	MaxLeverage float64
}

// Market types for MarketTypeByExchange.
const (
	// MarketTypeSpot is the default: cash balances, no leverage, nothing that
	// liquidates.
	MarketTypeSpot = "spot"

	// MarketTypeLinearPerp is a USDT-margined perpetual under isolated margin
	// and one-way positions.
	MarketTypeLinearPerp = "linear_perp"
)
