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
)

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
)

// Config is the main configuration object provided by user to initialize the strategy bot SDK.
type Config struct {
	Environment Environment      // The environment to connect to.
	Timeframes  []Timeframe      // One or more aggregate candle sizes the strategy subscribes to (e.g. [15m, 1h]).
	Indicators  []string         // Formatted indicator names (e.g. "EMA10", "RSI14") to pre-calculate.
	Credentials Credentials      // API Keys for specific real/testnet exchanges
	Backtest    *BacktestOptions // Specific settings required when configuring a new backtest engine session.
	Live        *LiveOptions     // Specific settings required when configuring a real exchange stream.
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
	Endpoint           string             // Engine API URL e.g. "http://localhost:8080"
	SessionName        string             // Human readable name for the backtesting run.
	RequestedExchanges []string           // List of exchanges to pull historical data against (e.g., "binance").
	Assets             []string           // Trading pairs requested (e.g., "BTC/USDT", "ETH/USDT").
	Wallets            map[string]float64 // Initial starting balances. Key is asset symbol (e.g. "USDT"), Value is amount.
	StartTime          time.Time          // Historic start time for data stream.
	EndTime            time.Time          // Historic end time for data stream.
}

// LiveOptions contains configuration necessary to hook onto live order books and websocket endpoints.
type LiveOptions struct {
	RequestedExchanges []string // List of real exchanges to connect to (e.g., "binance", "bybit").
	Assets             []string // Trading pairs requested (e.g., "BTCUSDT", "ETHUSDT"). Note: Binance requires no slashes usually.
	TelemetryURL       string   // Base URL of the live_trades service (e.g. "http://localhost:5001"). Empty = disabled.
	TelemetryAPIKey    string   // X-API-Key for live_trades service authentication.
	SessionID          string   // Optional fixed telemetry session id. Empty = a new UUID is generated; set to resume/extend an existing session.
}
