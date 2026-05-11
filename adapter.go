package dev_sdk

import (
	"context"
	"time"

	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

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
