package live

import (
	"context"
	"fmt"

	"github.com/kdraigo/dev_sdk/types"
)

// PlaceProtectedLimit rests a limit entry whose stop and target Bybit holds
// with the order and applies to the position at the fill (tpslMode Full,
// both triggered on the mark and closed at market). They exist the moment the
// limit fills, whether or not this process is running then: an entry that
// fills while the user's computer sleeps is protected.
//
// For one position per contract (one-way mode): Full mode's stop and target
// are the position's, all of it.
func (b *BybitFuturesClient) PlaceProtectedLimit(ctx context.Context, req *types.OrderRequest, stop, target float64) (*types.Order, error) {
	if req == nil {
		return nil, fmt.Errorf("protected limit: request must not be nil")
	}
	if req.Type != types.OrderTypeLimit || req.Price <= 0 || req.ReduceOnly {
		return nil, fmt.Errorf("protected limit: an opening LIMIT order with a price is required")
	}
	if stop <= 0 || target <= 0 {
		return nil, fmt.Errorf("protected limit: a stop and a target are required")
	}
	long := req.Side == types.OrderSideBuy
	if long && !(stop < req.Price && req.Price < target) || !long && !(target < req.Price && req.Price < stop) {
		return nil, fmt.Errorf("protected limit: a %s at %g needs its stop (%g) and target (%g) on either side of it", req.Side, req.Price, stop, target)
	}
	intent, err := MapOrder(req, MarketLinearPerp)
	if err != nil {
		return nil, fmt.Errorf("bybit futures protected limit: %w", err)
	}
	return b.placeWith(ctx, req, intent, "", func(body map[string]any, f InstrumentFilter) {
		body["takeProfit"] = f.FormatPrice(target)
		body["stopLoss"] = f.FormatPrice(stop)
		body["tpslMode"] = "Full"
		body["tpTriggerBy"] = "MarkPrice"
		body["slTriggerBy"] = "MarkPrice"
		body["tpOrderType"] = "Market"
		body["slOrderType"] = "Market"
	})
}
