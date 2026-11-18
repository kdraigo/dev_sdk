package orderexec

import (
	"github.com/kdraigo/dev_sdk/exchange/live"
	"github.com/kdraigo/dev_sdk/exchange/paper"
)

// Both futures adapters can place a ticket, and so can paper.
var (
	_ Venue = (*live.BinanceFuturesClient)(nil)
	_ Venue = (*live.BybitFuturesClient)(nil)
	_ Venue = (*paper.Client)(nil)
)

// Bybit holds a limit entry's stop and target before the fill; Binance does
// not yet (it needs a testnet answer first).
var _ ProtectedLimitPlacer = (*live.BybitFuturesClient)(nil)

// Bybit holds a stop on the position itself; both have every read an action needs.
var _ PositionStopper = (*live.BybitFuturesClient)(nil)
