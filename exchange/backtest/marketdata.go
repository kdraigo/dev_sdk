package backtest

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kdraigo/dev_sdk/internal/marketdata"
	"github.com/kdraigo/dev_sdk/types"
)

// simulationPayload is the session's simulation options on the wire, with the
// market data the session asks the engine to attach to every candle. The
// options are embedded so their fields stay where the engine reads them.
type simulationPayload struct {
	*types.SimulationOptions
	MarketData []string `json:"market_data,omitempty"`
}

// simulationFor builds the wire options: nil when there is nothing to send, as
// before market data existed.
func simulationFor(sim *types.SimulationOptions, series []types.MarketData) *simulationPayload {
	if sim == nil && len(series) == 0 {
		return nil
	}
	out := &simulationPayload{SimulationOptions: sim}
	for _, s := range series {
		out.MarketData = append(out.MarketData, string(s))
	}
	return out
}

// marketDataTimeout bounds one read. A multi-year funding window or 50,000
// flow bars is one data_provider query, but it is a large one.
const marketDataTimeout = 30 * time.Second

func (e *EngineClient) marketDataRead(ctx context.Context, action, exchange, symbol string, tf types.Timeframe, from, to time.Time, out any) error {
	data := map[string]interface{}{
		"exchange": exchange,
		"pair":     symbol,
		"from":     from,
		"to":       to,
	}
	if tf != "" {
		data["timeframe"] = string(tf)
	}
	raw, err := e.callWithin(ctx, action, data, marketDataTimeout)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("%s decode: %w", action, err)
	}
	return nil
}

// GetFundingRates implements dev_sdk.MarketDataReader: the engine serves the
// settlements in [from, to] and refuses a to past the simulated playhead.
func (e *EngineClient) GetFundingRates(ctx context.Context, exchange, symbol string, from, to time.Time) ([]types.FundingRate, error) {
	var resp struct {
		Rates []marketdata.FundingWire `json:"rates"`
	}
	if err := e.marketDataRead(ctx, "funding_history", exchange, symbol, "", from, to, &resp); err != nil {
		return nil, err
	}
	out := make([]types.FundingRate, 0, len(resp.Rates))
	for _, r := range resp.Rates {
		out = append(out, r.FundingRate())
	}
	return out, nil
}

// GetPositioning implements dev_sdk.MarketDataReader: bars known by to (the
// engine applies the publish lag) and nothing past the playhead.
func (e *EngineClient) GetPositioning(ctx context.Context, exchange, symbol string, tf types.Timeframe, from, to time.Time) ([]types.Positioning, error) {
	var resp struct {
		Bars []marketdata.PositioningWire `json:"bars"`
	}
	if err := e.marketDataRead(ctx, "positioning_history", exchange, symbol, tf, from, to, &resp); err != nil {
		return nil, err
	}
	out := make([]types.Positioning, 0, len(resp.Bars))
	for _, b := range resp.Bars {
		out = append(out, *b.Positioning(time.Time{}))
	}
	return out, nil
}

// GetFlow implements dev_sdk.MarketDataReader: bars closed by to.
func (e *EngineClient) GetFlow(ctx context.Context, exchange, symbol string, tf types.Timeframe, from, to time.Time) ([]types.Flow, error) {
	var resp struct {
		Bars []marketdata.FlowWire `json:"bars"`
	}
	if err := e.marketDataRead(ctx, "flow_history", exchange, symbol, tf, from, to, &resp); err != nil {
		return nil, err
	}
	out := make([]types.Flow, 0, len(resp.Bars))
	for _, b := range resp.Bars {
		out = append(out, *b.Flow())
	}
	return out, nil
}
