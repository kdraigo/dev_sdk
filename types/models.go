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
	Volume     float64 // Base-asset volume. (There is no separate BaseVolume field: Volume IS the base volume.)
	IsComplete bool     // True when the candle has fully closed on its timeframe length.

	// Advanced order-flow metrics (Wyckoff / Composite Man analysis).
	// These are 0 when the source exchange does not provide them
	// (e.g. Bybit klines expose QuoteVolume but not TradeCount or taker-buy splits).
	TradeCount          int64   // Number of trades in the candle.
	QuoteVolume         float64 // Quote-asset volume (turnover).
	TakerBuyBaseVolume  float64 // Taker buy base-asset volume (aggressive-buy pressure).
	TakerBuyQuoteVolume float64 // Taker buy quote-asset volume.
}

// OrderType dictates whether an order is Market, Limit, etc.
type OrderType string

const (
	OrderTypeMarket OrderType = "MARKET"
	OrderTypeLimit  OrderType = "LIMIT"

	// OrderTypeStopLoss is market-on-trigger: it books at StopPrice once the
	// bar trades through it.
	OrderTypeStopLoss OrderType = "STOP_LOSS"

	// OrderTypeStopLossLimit triggers at StopPrice and books at Price.
	OrderTypeStopLossLimit OrderType = "STOP_LOSS_LIMIT"

	// OrderTypeTakeProfitLimit is a resting exit at Price. It behaves as a
	// limit order; the distinct type exists so a run can be read back and the
	// strategy's intent recovered.
	OrderTypeTakeProfitLimit OrderType = "TAKE_PROFIT_LIMIT"
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

	// StopPrice is the trigger for STOP_LOSS and STOP_LOSS_LIMIT orders.
	// A SELL stop triggers when the bar trades at or below it; a BUY stop when
	// it trades at or above. It is ignored by other order types.
	StopPrice float64

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

	// StopPrice is the trigger price for stop orders, and zero otherwise.
	// Without it a stop fill is indistinguishable from a limit fill at the
	// same price when handling OnOrderUpdate.
	StopPrice float64

	// GroupID links the legs of a bracket. Two orders sharing a non-empty
	// GroupID are mutually cancelling: when one fills, the other is cancelled.
	GroupID string
}

// BracketRequest places a take-profit and a protective stop as a mutually
// cancelling pair sharing one fund reservation.
//
// It is a pair rather than a ladder on purpose. The backtest wallet reserves
// funds per order, so several independent take-profits against one position
// cannot be placed at once; a strategy wanting a ladder should re-bracket the
// remaining quantity after each fill.
type BracketRequest struct {
	Symbol   string
	Exchange string
	Side     OrderSide
	Quantity float64

	// TakeProfitPrice is the resting exit; StopPrice is the trigger for the
	// protective leg. StopLimitPrice is the price that leg books at, and
	// defaults to StopPrice when zero.
	TakeProfitPrice float64
	StopPrice       float64
	StopLimitPrice  float64

	Reason map[string]any `json:"-"`
	Logs   []string       `json:"-"`
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
