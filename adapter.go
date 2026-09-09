package dev_sdk

import (
	"context"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// BracketPlacer is an optional capability. An adapter that can place a
// take-profit and a protective stop as one mutually cancelling pair implements
// it; the rest do not, and callers get ErrUnsupportedByAdapter.
//
// It is deliberately separate from Adapter: widening that interface would break
// every live adapter for a capability only the backtest engine currently has.
type BracketPlacer interface {
	PlaceBracket(ctx context.Context, req *types.BracketRequest) ([]*types.Order, error)
}

// Adapter interface standardizes how the SDK interacts with any underlying exchange (Real or Backtest).
type Adapter interface {
	// PrepareSession is called before starting the stream.
	PrepareSession(ctx context.Context, config *types.Config) error

	// ConnectStream initiates the WebSocket (or polling mechanism) to receive live data.
	ConnectStream(ctx context.Context, candleChan chan<- *types.Candle, orderChan chan<- *types.Order) error

	// PlaceOrder translates the generic SDK request into exchange-specific API calls.
	PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error)

	// CancelOrder aborts an open order. exchange and symbol are required by most exchanges.
	CancelOrder(ctx context.Context, exchange, symbol, orderID string) error

	// GetAccount fetches the current balance for an asset.
	GetAccount(ctx context.Context, exchange string, asset string) (*types.Account, error)

	// Next requests the next data point (tick/candle) from the exchange (primarily for Backtesting).
	Next(ctx context.Context) error

	// GetHistoricalCandles returns closed candles in [from, to] for the given
	// timeframe. In live mode the adapter fetches from the exchange REST API.
	// In backtest mode it round-trips through the engine, which validates that
	// `to` does not exceed the simulated playhead and serves from the
	// data_provider. Always read-only — no playhead, wallet, or coordinator
	// state is mutated.
	GetHistoricalCandles(ctx context.Context, exchange, symbol string, from, to time.Time, tf types.Timeframe) ([]*types.Candle, error)
}

// LeverageSetter is an optional capability for adapters that model leverage.
//
// Separate from Adapter for the same reason as BracketPlacer: widening that
// interface would break every live adapter for something only the backtest
// engine currently does.
type LeverageSetter interface {
	// SetLeverage sets the leverage used for new positions on a pair.
	//
	// Refused while a position is open — re-levering a live position would
	// silently rewrite its liquidation price, which is not a thing an exchange
	// lets you do either.
	SetLeverage(ctx context.Context, exchange, pair string, leverage float64) error
}

// PositionReader is an optional capability for adapters that hold positions.
type PositionReader interface {
	// GetPositions returns the open positions on an exchange, or across every
	// futures wallet when exchange is empty.
	GetPositions(ctx context.Context, exchange string) ([]*types.Position, error)
}

// OrderFeedDescriber is an optional capability: an adapter that states how it
// delivers order updates.
//
// Separate from Adapter for the same reason as BracketPlacer — widening that
// interface would break every adapter at once — and optional so an adapter
// that has not been audited yet is reported as unknown rather than assumed
// working. Silence is the failure being designed out, so an adapter that says
// nothing must not read as an adapter that says "fine".
type OrderFeedDescriber interface {
	OrderFeed() types.OrderFeed
}

// OrderStateReader is what a polling fallback needs from an adapter that
// cannot push. An adapter providing these two reads earns an order feed for
// free: the SDK supplies the reconciler.
//
// GetOrder is not redundant with ListOpenOrders. An order that has vanished
// from the open list is either FILLED or CANCELED and the open list cannot say
// which — resolving that ambiguity requires asking about the order itself.
// Treating disappearance as a fill would invent positions the account does not
// hold.
type OrderStateReader interface {
	ListOpenOrders(ctx context.Context, exchange, symbol string) ([]*types.Order, error)
	GetOrder(ctx context.Context, exchange, symbol, id string) (*types.Order, error)
}

// resolveOrderFeed reports how the given adapter delivers order updates,
// falling back to the polling reconciler when the adapter cannot push but can
// be asked.
func resolveOrderFeed(adapter Adapter) types.OrderFeed {
	var feed types.OrderFeed
	if d, ok := adapter.(OrderFeedDescriber); ok {
		feed = d.OrderFeed()
	}
	if !feed.Push && feed.PollEvery == 0 {
		if _, ok := adapter.(OrderStateReader); ok {
			// The adapter never declared a feed but can answer questions, so
			// the SDK can build one. Conservative default: slow enough not to
			// spend the venue's rate limit, fast enough to be useful.
			feed.PollEvery = defaultOrderPollInterval
			feed.Latency = defaultOrderPollInterval
		}
	}
	return feed
}

// defaultOrderPollInterval is the fallback cadence for an adapter that can be
// polled but did not say how often. Order polling competes with order
// placement for the venue's rate limit, so this errs slow.
const defaultOrderPollInterval = 3 * time.Second
