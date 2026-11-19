package live

import (
	"context"
	"fmt"
	"net/url"

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

// PositionStop is the stop Bybit holds on the position itself (tpslMode
// Full), as a limit entry's attached stop becomes at the fill; 0 when there
// is none or no position.
func (b *BybitFuturesClient) PositionStop(ctx context.Context, symbol string) (float64, error) {
	b.ensureAPI()
	sym := VenueSymbol(symbol)
	var r bybitList[bybitPositionRecord]
	if err := b.api.get(ctx, "/v5/position/list", url.Values{"category": {"linear"}, "symbol": {sym}}, true, &r); err != nil {
		return 0, err
	}
	for _, p := range r.List {
		if p.Symbol == sym && parseFloat(p.Size) > 0 {
			return parseFloat(p.StopLoss), nil
		}
	}
	return 0, nil
}

// SetPositionStop moves the position's own stop (tpslMode Full, on the mark,
// closed at market). The stop is on the contract's grid and checked by the
// caller; Bybit refuses one on the wrong side of the price.
func (b *BybitFuturesClient) SetPositionStop(ctx context.Context, symbol string, stop float64) error {
	b.ensureAPI()
	sym := VenueSymbol(symbol)
	f, ok := b.filters[sym]
	if !ok {
		return fmt.Errorf("bybit futures: %s was not prepared — add it to LiveOptions.Assets", sym)
	}
	if stop <= 0 {
		return fmt.Errorf("bybit futures: a stop above 0 is needed")
	}
	if b.guard.DryRun() {
		return nil
	}
	body := map[string]any{
		"category": "linear", "symbol": sym, "positionIdx": 0, "tpslMode": "Full",
		"stopLoss": f.FormatPrice(stop), "slTriggerBy": "MarkPrice", "slOrderType": "Market",
	}
	var res struct{}
	if err := b.api.post(ctx, "/v5/position/trading-stop", body, &res); err != nil {
		return fmt.Errorf("bybit futures %s trading-stop: %w", sym, explainBybitKeyError(err))
	}
	return nil
}

// PositionTarget is the target Bybit holds on the position itself (tpslMode
// Full), as a limit entry's attached target becomes at the fill; 0 when there
// is none or no position.
func (b *BybitFuturesClient) PositionTarget(ctx context.Context, symbol string) (float64, error) {
	b.ensureAPI()
	sym := VenueSymbol(symbol)
	var r bybitList[bybitPositionRecord]
	if err := b.api.get(ctx, "/v5/position/list", url.Values{"category": {"linear"}, "symbol": {sym}}, true, &r); err != nil {
		return 0, err
	}
	for _, p := range r.List {
		if p.Symbol == sym && parseFloat(p.Size) > 0 {
			return parseFloat(p.TakeProfit), nil
		}
	}
	return 0, nil
}

// SetPositionTarget moves the position's own target (tpslMode Full, on the
// mark, closed at market); its stop is left as it is. The target is on the
// contract's grid and checked by the caller; Bybit refuses one on the wrong
// side of the price.
func (b *BybitFuturesClient) SetPositionTarget(ctx context.Context, symbol string, target float64) error {
	b.ensureAPI()
	sym := VenueSymbol(symbol)
	f, ok := b.filters[sym]
	if !ok {
		return fmt.Errorf("bybit futures: %s was not prepared — add it to LiveOptions.Assets", sym)
	}
	if target <= 0 {
		return fmt.Errorf("bybit futures: a target above 0 is needed")
	}
	if b.guard.DryRun() {
		return nil
	}
	body := map[string]any{
		"category": "linear", "symbol": sym, "positionIdx": 0, "tpslMode": "Full",
		"takeProfit": f.FormatPrice(target), "tpTriggerBy": "MarkPrice", "tpOrderType": "Market",
	}
	var res struct{}
	if err := b.api.post(ctx, "/v5/position/trading-stop", body, &res); err != nil {
		return fmt.Errorf("bybit futures %s trading-stop: %w", sym, explainBybitKeyError(err))
	}
	return nil
}
