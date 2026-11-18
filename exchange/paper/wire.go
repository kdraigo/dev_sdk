package paper

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// The paper exchange's JSON: snake_case, decimals as strings, null for what
// is not set.

// num reads a decimal the engine writes as a string, a number or null.
type num float64

func (n *num) UnmarshalJSON(b []byte) error {
	b = bytes.Trim(b, `"`)
	if len(b) == 0 || string(b) == "null" {
		*n = 0
		return nil
	}
	v, err := strconv.ParseFloat(string(b), 64)
	if err != nil {
		return err
	}
	*n = num(v)
	return nil
}

// frame is every message on the WebSocket, answers and pushes alike.
type frame struct {
	Action    string          `json:"action"`
	RequestID string          `json:"request_id"`
	Status    string          `json:"status"`
	Data      json.RawMessage `json:"data"`
	Error     string          `json:"error"`
	Code      string          `json:"code"`
}

// event is a push's data.
type event struct {
	Type   string          `json:"type"`
	Market string          `json:"market"`
	Data   json.RawMessage `json:"data"`
}

type orderWire struct {
	ID            uint64    `json:"id"`
	ClientOrderID *string   `json:"client_order_id"`
	Symbol        string    `json:"symbol"`
	Side          string    `json:"side"`
	Type          string    `json:"type"`
	TIF           string    `json:"time_in_force"`
	Price         num       `json:"price"`
	StopPrice     num       `json:"stop_price"`
	Qty           num       `json:"quantity"`
	FilledQty     num       `json:"filled_quantity"`
	AvgPrice      num       `json:"avg_price"`
	Fee           num       `json:"fee"`
	FeeAsset      *string   `json:"fee_asset"`
	ReduceOnly    bool      `json:"reduce_only"`
	GroupID       *int64    `json:"group_id"`
	ParentID      *int64    `json:"parent_id"`
	Status        string    `json:"status"`
	Reason        *string   `json:"reason"`
	PlacedAt      time.Time `json:"placed_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type fillWire struct {
	OrderID uint64 `json:"order_id"`
	Price   num    `json:"price"`
	Qty     num    `json:"quantity"`
	Fee     num    `json:"fee"`
}

// orderResult is an order request's answer: the order rows (one per leg) and
// the fills it made at once, or the refusal.
type orderResult struct {
	Orders []orderWire `json:"orders"`
	Fills  []fillWire  `json:"fills"`
	Reject *rejectWire `json:"reject"`
}

type rejectWire struct {
	Code   int    `json:"code"`
	Reason string `json:"reason"`
	Msg    string `json:"msg"`
}

// orderUpdate is an order_update push.
type orderUpdate struct {
	Order orderWire  `json:"order"`
	Fills []fillWire `json:"fills"`
}

type balanceWire struct {
	Asset  string `json:"asset"`
	Bucket string `json:"bucket"`
	Amount num    `json:"amount"`
}

type positionWire struct {
	Symbol           string `json:"symbol"`
	Side             string `json:"side"`
	Size             num    `json:"size"`
	EntryPrice       num    `json:"entry_price"`
	Leverage         num    `json:"leverage"`
	IsolatedMargin   num    `json:"isolated_margin"`
	Mark             num    `json:"mark"`
	UnrealizedPnL    num    `json:"unrealized_pnl"`
	LiquidationPrice num    `json:"liquidation_price"`
	RealizedPnL      num    `json:"realized_pnl"`
	Fees             num    `json:"fees"`
	Funding          num    `json:"funding"`
}

type accountWire struct {
	ID         string  `json:"id"`
	Kind       string  `json:"kind"`
	Venue      string  `json:"venue"`
	Status     string  `json:"status"`
	StopReason *string `json:"stop_reason"`
	APIKeyID   *string `json:"api_key_id"`
}

// view is a running account as the engine holds it (the WebSocket's first
// frame, and GET /accounts/:id while it runs).
type view struct {
	Account   accountWire    `json:"account"`
	Symbols   []string       `json:"symbols"`
	Balances  []balanceWire  `json:"balances"`
	Positions []positionWire `json:"positions"`
	Orders    []orderWire    `json:"open_orders"`
}

type candleWire struct {
	OpenTime   int64   `json:"open_time"`
	CloseTime  int64   `json:"close_time"`
	Open       float64 `json:"open"`
	High       float64 `json:"high"`
	Low        float64 `json:"low"`
	Close      float64 `json:"close"`
	Volume     float64 `json:"volume"`
	QuoteVol   float64 `json:"quote_volume"`
	Trades     int64   `json:"trade_count"`
	TakerBase  float64 `json:"taker_buy_base"`
	TakerQuote float64 `json:"taker_buy_quote"`
}

type contractWire struct {
	Symbol      string `json:"symbol"`
	TickSize    num    `json:"tick_size"`
	StepSize    num    `json:"step_size"`
	MinQty      num    `json:"min_qty"`
	MinNotional num    `json:"min_notional"`
	Mark        num    `json:"mark"`
	Last        num    `json:"last"`
	MaxLeverage num    `json:"max_leverage"`
	MMR         num    `json:"maintenance_margin_rate"`
}

// instrumentWire is a market in GET /instruments: data_provider's rules, the
// ones the engine builds the market from.
type instrumentWire struct {
	TickSize string `json:"tick_size"`
	Rules    *struct {
		StepSize    string `json:"step_size"`
		MinQty      string `json:"min_qty"`
		MinNotional string `json:"min_notional"`
	} `json:"rules"`
}

// sdkType is the SDK's name for an engine order type. A LIMIT leg of a
// bracket, or one attached to an entry, is the take-profit the strategy
// asked for; the engine has no resting take-profit type of its own (its
// TAKE_PROFIT_LIMIT waits for a trigger).
func sdkType(o orderWire, intent types.OrderType) types.OrderType {
	switch strings.ToUpper(o.Type) {
	case "MARKET", "LIQUIDATION":
		return types.OrderTypeMarket
	case "LIMIT", "LIMIT_MAKER":
		if intent == types.OrderTypeTakeProfitLimit || o.GroupID != nil || o.ParentID != nil {
			return types.OrderTypeTakeProfitLimit
		}
		return types.OrderTypeLimit
	case "STOP_MARKET", "STOP_LOSS":
		return types.OrderTypeStopLoss
	case "STOP_LIMIT", "STOP", "STOP_LOSS_LIMIT":
		return types.OrderTypeStopLossLimit
	case "TAKE_PROFIT_MARKET", "TAKE_PROFIT_LIMIT", "TAKE_PROFIT":
		return types.OrderTypeTakeProfitLimit
	}
	return types.OrderType(o.Type)
}

// sdkStatus is the SDK's name for an engine status. An expired order (an IOC
// or post-only remainder) ended without filling the rest, which the SDK
// calls cancelled.
func sdkStatus(s string) types.OrderStatus {
	switch s {
	case "PENDING", "NEW":
		return types.OrderStatusNew
	case "PARTIALLY_FILLED":
		return types.OrderStatusPartiallyFilled
	case "FILLED":
		return types.OrderStatusFilled
	case "CANCELED", "EXPIRED":
		return types.OrderStatusCanceled
	case "REJECTED":
		return types.OrderStatusRejected
	}
	return types.OrderStatus(s)
}

func orderID(id uint64) string { return strconv.FormatUint(id, 10) }

func optString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
