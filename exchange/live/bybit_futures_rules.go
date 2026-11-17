package live

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// The Bybit perpetuals adapter's pure rules: no network, no library types, so
// each one is tested exactly (bybit_futures_rules_test.go). They are the
// places where the SDK's meaning becomes Bybit's, and where a mistake sends a
// real order that means something else.

// Bybit V5 order statuses, as the REST and websocket APIs spell them.
const (
	bybitStatusCreated                 = "Created"
	bybitStatusNew                     = "New"
	bybitStatusRejected                = "Rejected"
	bybitStatusPartiallyFilled         = "PartiallyFilled"
	bybitStatusPartiallyFilledCanceled = "PartiallyFilledCanceled"
	bybitStatusFilled                  = "Filled"
	bybitStatusCancelled               = "Cancelled"
	bybitStatusUntriggered             = "Untriggered"
	bybitStatusTriggered               = "Triggered"
	bybitStatusDeactivated             = "Deactivated"
)

// mapBybitFuturesStatus converts a V5 order status.
//
// A conditional order rests as Untriggered and passes through Triggered on its
// way to New or Filled: both are "still live" to the SDK. Deactivated is a
// conditional order cancelled before it fired (by the user, or by the venue
// when the position it guarded closed), so it is a cancel, never a fill.
// PartiallyFilledCanceled is a cancel too: the filled part is already in
// FilledQty. An unknown status reads as still live rather than as a fill, the
// side on which a mistake costs a poll instead of a phantom position.
func mapBybitFuturesStatus(s string) types.OrderStatus {
	switch s {
	case bybitStatusFilled:
		return types.OrderStatusFilled
	case bybitStatusPartiallyFilled:
		return types.OrderStatusPartiallyFilled
	case bybitStatusCancelled, bybitStatusDeactivated, bybitStatusPartiallyFilledCanceled:
		return types.OrderStatusCanceled
	case bybitStatusRejected:
		return types.OrderStatusRejected
	default:
		return types.OrderStatusNew
	}
}

// bybitTriggerDirection says which way the price must cross a stop's trigger:
// 1 when it rises to it, 2 when it falls to it.
//
// It follows the SDK's own definition of a stop (types.OrderRequest.StopPrice):
// a SELL stop fires when the price trades at or below the trigger, a BUY stop at
// or above. Deriving it from the current price instead would flip a stop the
// moment the price had already passed its trigger: a protective stop that the
// market has gone through must fire at once, not wait for the price to come
// back up through it.
func bybitTriggerDirection(side types.OrderSide) (int, error) {
	switch side {
	case types.OrderSideSell:
		return 2, nil
	case types.OrderSideBuy:
		return 1, nil
	default:
		return 0, fmt.Errorf("bybit futures: no trigger direction for side %q", side)
	}
}

// bybitFuturesOrderType is the wire order type for an intent. Bybit has only
// Market and Limit; a trigger is a triggerPrice set alongside. A take-profit is
// a plain resting limit, as on Binance: the SDK's TAKE_PROFIT_LIMIT is an exit
// at a price, not a trigger.
func bybitFuturesOrderType(t IntentType) (string, error) {
	switch t {
	case IntentMarket, IntentStopMarket:
		return "Market", nil
	case IntentLimit, IntentStopLimit, IntentTakeProfitLimit:
		return "Limit", nil
	default:
		return "", fmt.Errorf("bybit futures: no order type for intent %s", t)
	}
}

// sdkTypeFromBybit recovers the SDK order type from a venue record for an
// order this process did not place (one left from an earlier run, say). An
// order with a trigger is a stop: market-on-trigger unless it rests at a price.
func sdkTypeFromBybit(orderType string, triggerPrice float64, stopOrderType string) types.OrderType {
	if triggerPrice > 0 || stopOrderType != "" {
		if strings.EqualFold(orderType, "Limit") {
			return types.OrderTypeStopLossLimit
		}
		return types.OrderTypeStopLoss
	}
	if strings.EqualFold(orderType, "Limit") {
		return types.OrderTypeLimit
	}
	return types.OrderTypeMarket
}

// bybitLinkIDMax is the longest orderLinkId Bybit accepts.
const bybitLinkIDMax = 36

var linkIDSeq atomic.Uint64

// newBybitLinkID mints a client order id: unique within the process even for
// two orders in the same nanosecond, and short enough for Bybit's limit. It is
// how a retried placement is recognised as the same order rather than a second
// one.
func newBybitLinkID(now time.Time) string {
	id := fmt.Sprintf("kd%d-%d", now.UnixNano(), linkIDSeq.Add(1))
	if len(id) > bybitLinkIDMax {
		id = id[len(id)-bybitLinkIDMax:]
	}
	return id
}

// bybitPositionSide converts a V5 position's side. An empty side ("" or
// "None") is a flat position, reported as no side at all.
func bybitPositionSide(side string) string {
	switch side {
	case "Buy":
		return "LONG"
	case "Sell":
		return "SHORT"
	default:
		return ""
	}
}
