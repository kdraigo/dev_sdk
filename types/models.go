package types

import (
	"context"
	"time"
)

// Candle standardizes OHLCV data passed back to user strategies.
type Candle struct {
	Symbol     string
	Exchange   string
	Timeframe  Timeframe
	OpenTime   time.Time
	CloseTime  time.Time
	Open       float64
	High       float64
	Low        float64
	Close      float64
	Volume     float64
	IsComplete bool // True when the candle has fully closed on its timeframe length.
}

// OrderType dictates whether an order is Market, Limit, etc.
type OrderType string

const (
	OrderTypeMarket OrderType = "MARKET"
	OrderTypeLimit  OrderType = "LIMIT"
)

// OrderSide specifies buying or selling.
type OrderSide string

const (
	OrderSideBuy  OrderSide = "BUY"
	OrderSideSell OrderSide = "SELL"
)

// OrderStatus gives the lifecycle state of an order.
type OrderStatus string

const (
	OrderStatusNew             OrderStatus = "NEW"
	OrderStatusPartiallyFilled OrderStatus = "PARTIALLY_FILLED"
	OrderStatusFilled          OrderStatus = "FILLED"
	OrderStatusCanceled        OrderStatus = "CANCELED"
	OrderStatusRejected        OrderStatus = "REJECTED"
)

// OrderRequest is what the strategy sends to the SDK to create a new position.
type OrderRequest struct {
	Symbol   string
	Exchange string
	Side     OrderSide
	Type     OrderType
	Quantity float64
	Price    float64 // Zero if Market order

	// Reason and Logs are telemetry-only annotations. The SDK strips both
	// before forwarding to the adapter; only live_trades sees them. Use them
	// to capture the strategy's decision context ({rsi: 32, signal: "..."})
	// and short log lines for post-hoc review. Size caps are enforced
	// server-side (4 KB reason, 16 KB logs); the SDK truncates locally to
	// avoid 413s.
	Reason map[string]any `json:"-"`
	Logs   []string       `json:"-"`
}

// Order is the state representation of an order returned by the exchange flow.
type Order struct {
	ID           string
	Symbol       string
	Exchange     string
	Side         OrderSide
	Type         OrderType
	Status       OrderStatus
	Price        float64
	Quantity     float64
	FilledQty    float64
	AveragePrice float64
	Fee          float64
	FeeAsset     string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Balance represents a single asset's available and locked funds.
type Balance struct {
	Asset string  `json:"asset"`
	Free  float64 `json:"free"`
	Lock  float64 `json:"lock"`
}

// Account represents the total state of a user's wallet on an exchange.
type Account struct {
	Exchange string    `json:"exchange"`
	Balances []Balance `json:"balances"`
}

// Position standardizes an ongoing open position in a trading pair.
type Position struct {
	Symbol        string
	Exchange      string
	Size          float64
	EntryPrice    float64
	MarkPrice     float64
	UnrealizedPnL float64
}

// Trader is the internal dependency decoupled interface to execute logic.
type Trader interface {
	PlaceOrder(ctx context.Context, req *OrderRequest) (*Order, error)
	CancelOrder(ctx context.Context, exchange, symbol, id string) error
	GetAccount(ctx context.Context, exchange string, asset string) (*Account, error)
}

// ClockProvider returns the current time in the strategy's frame of reference.
// In live mode it returns wall time; in backtest mode it returns the simulated
// clock (close time of the last dispatched candle, or the session start time
// before any candle has been dispatched).
type ClockProvider interface {
	Now() time.Time
}

// Context wraps runtime specifics accessible in callback functions.
// Allows users to query the indicators pre-calculated and manage connection lifecycle.
type Context struct {
	Ctx           context.Context
	Cancel        context.CancelFunc
	Config        *Config
	IndicatorsMap map[string]float64
	Trader        Trader
	Clock         ClockProvider
}

func (c *Context) SetIndicators(in map[string]float64) {
	c.IndicatorsMap = in
}

func (c *Context) GetIndicator(name string) float64 {
	return c.IndicatorsMap[name]
}

func (c *Context) PlaceOrder(req *OrderRequest) (*Order, error) {
	return c.Trader.PlaceOrder(c.Ctx, req)
}

func (c *Context) CancelOrder(exchange, symbol, orderID string) error {
	return c.Trader.CancelOrder(c.Ctx, exchange, symbol, orderID)
}

// Now returns the current time in the strategy's frame of reference.
// Use this instead of time.Now() so strategy code is portable across live and
// backtest modes.
func (c *Context) Now() time.Time {
	if c.Clock == nil {
		return time.Now()
	}
	return c.Clock.Now()
}

// Callbacks

// OnCandleFunc is invoked by the SDK whenever a new populated Candle is ready.
// The context carries active indicators requested during initialization.
type OnCandleFunc func(ctx *Context, candle *Candle)

// OnOrderUpdateFunc is invoked whenever a placed order changes its processing state.
type OnOrderUpdateFunc func(ctx *Context, order *Order)
