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
	IsComplete bool    // True when the candle has fully closed on its timeframe length.

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

	// ReduceOnly restricts the order to shrinking an open position: it can
	// never open one, nor flip an existing one to the opposite side.
	//
	// This matters most for protective stops. In one-way mode a stop sized
	// larger than the position — a stale bracket, or sizing computed off
	// intended rather than filled exposure — does not stop at flat. The
	// surplus opens a position on the other side, so the strategy ends up with
	// the same exposure inverted on exactly the bar it wanted none.
	//
	// Futures only; a spot wallet rejects it rather than silently ignoring it.
	ReduceOnly bool

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
	Symbol   string
	Exchange string

	// Side is "LONG" or "SHORT". Size is always positive, so a flat pair is
	// the absence of a position rather than a zero-size one.
	Side string
	Size float64

	// EntryPrice is the size-weighted average entry.
	EntryPrice float64
	Leverage   float64

	// IsolatedMargin is the collateral committed to this position, and under
	// isolated margin it is also the maximum loss.
	IsolatedMargin float64

	// RealizedPnL accumulates over the position's life, excluding fees.
	// FundingPaid accumulates signed funding; positive means paid out.
	RealizedPnL float64
	FundingPaid float64

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

// OrderFeed describes how an adapter delivers order state transitions to
// orderChan, and therefore to SetOnOrderUpdate.
//
// It exists because the Adapter contract hands every adapter an orderChan
// without saying whether anything will ever come out of it. Three of the four
// adapters push order updates; Binance spot only ever streamed klines, so
// SetOnOrderUpdate was silently dead there while working everywhere else. The
// same strategy, no error, no warning.
//
// Declaring the feed turns that from a discovery into a startup log line.
type OrderFeed struct {
	// Push is true when the venue itself notifies. Fills arrive in
	// milliseconds and nothing needs to ask.
	Push bool

	// PollEvery is how often the SDK should ask the venue for order state
	// when it will not push. Zero means no polling fallback is available, and
	// an adapter with Push false and PollEvery zero delivers nothing at all —
	// which the SDK warns about rather than leaving to be found in production.
	PollEvery time.Duration

	// Latency is the worst-case delay between something happening at the
	// venue and the strategy hearing about it.
	//
	// Deliberately part of the public description rather than hidden. A
	// three-second polled feed really is different from a fifty-millisecond
	// pushed one, and papering over that would create exactly the kind of
	// silent backtest-to-live divergence this type was introduced to end.
	Latency time.Duration
}

// Delivers reports whether the feed produces order updates by any means.
func (f OrderFeed) Delivers() bool { return f.Push || f.PollEvery > 0 }

// Describe renders the feed for a startup log line.
func (f OrderFeed) Describe() string {
	switch {
	case f.Push && f.PollEvery > 0:
		return "pushed by the venue, reconciled every " + f.PollEvery.String()
	case f.Push:
		return "pushed by the venue (latency ~" + f.Latency.String() + ")"
	case f.PollEvery > 0:
		return "polled every " + f.PollEvery.String() + " (latency ~" + f.Latency.String() + ")"
	default:
		return "NONE — order updates will not be delivered"
	}
}
