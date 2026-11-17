package live

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// bybitV5 is a minimal Bybit V5 REST client for the perpetuals adapter.
//
// It exists instead of github.com/hirokisan/bybit for the paths that move
// money, because that library (v2.39.0, checked 2026-11-17) gets them wrong in
// ways nothing reports:
//
//   - V5CreateOrderParam.ReduceOnly is tagged `json:"reduce_only"`; V5 reads
//     `reduceOnly`, so a closing order goes out without it and can open or flip
//     a position instead of reducing one;
//   - its TimeInForce constants are the V2 names ("GoodTillCancel");
//   - it drops retExtInfo and the rate-limit headers, and wraps HTTP 400 and
//     429 as plain strings;
//   - its websockets never reconnect.
//
// Every field name here is the V5 docs' own spelling
// (https://bybit-exchange.github.io/docs/v5/), and the tests check the wire
// bodies against them.
type bybitV5 struct {
	baseURL string
	key     string
	secret  string
	http    *http.Client

	// offsetMs is the venue's clock minus ours. V5 refuses a request whose
	// timestamp is not within recv_window of its own clock (retCode 10002).
	offsetMs atomic.Int64
	now      func() time.Time
}

const (
	bybitMainnetREST = "https://api.bybit.com"
	bybitTestnetREST = "https://api-testnet.bybit.com"
	bybitMainnetWS   = "wss://stream.bybit.com"
	bybitTestnetWS   = "wss://stream-testnet.bybit.com"

	// bybitRecvWindow is how far a request's timestamp may trail the venue's
	// clock, in ms. Five seconds is V5's default, sent explicitly so the
	// signature and the header agree.
	bybitRecvWindow = "5000"
)

func newBybitV5(baseURL, key, secret string) *bybitV5 {
	return &bybitV5{
		baseURL: strings.TrimRight(baseURL, "/"),
		key:     key,
		secret:  secret,
		// A request that hangs is worse than one that fails: a placement
		// blocked forever keeps a strategy waiting on an order it cannot see.
		http: &http.Client{Timeout: 15 * time.Second},
		now:  time.Now,
	}
}

// bybitAPIError is a V5 refusal: retCode is not 0. The order, cancel or change
// did not happen, so it is safe to report as a plain failure.
type bybitAPIError struct {
	Code int
	Msg  string
	Path string
}

func (e *bybitAPIError) Error() string {
	return fmt.Sprintf("bybit %s: retCode %d: %s", e.Path, e.Code, e.Msg)
}

// errBybitUnsure marks a request whose outcome is unknown: the connection
// broke, timed out, or the venue answered with an HTTP error before it could
// say. For a placement this is the dangerous case — the order may exist — so
// the caller looks it up instead of assuming either way.
var errBybitUnsure = errors.New("bybit: the request's outcome is unknown")

// bybitCode returns the V5 retCode inside err, or 0.
func bybitCode(err error) int {
	var apiErr *bybitAPIError
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	return 0
}

// V5 retCodes the adapter acts on (https://bybit-exchange.github.io/docs/v5/error).
const (
	bybitCodeRateLimit         = 10006
	bybitCodeIPRateLimit       = 10018
	bybitCodeTimestamp         = 10002
	bybitCodeInvalidKey        = 10003
	bybitCodeBadSign           = 10004
	bybitCodePermission        = 10005
	bybitCodeIPNotAllowed      = 10010
	bybitCodeOrderNotExist     = 110001
	bybitCodeOrderCompleted    = 110008
	bybitCodeOrderCancelled    = 110010
	bybitCodeLeverageUnchanged = 110043
	bybitCodeDuplicateLinkID   = 110072
)

type bybitEnvelope struct {
	RetCode int             `json:"retCode"`
	RetMsg  string          `json:"retMsg"`
	Result  json.RawMessage `json:"result"`
	Time    int64           `json:"time"`
}

// sign is V5's HMAC: hex(HMAC_SHA256(secret, timestamp + key + recvWindow + payload)),
// where payload is the query string for a GET and the raw JSON body for a POST.
func (c *bybitV5) sign(ts, payload string) string {
	mac := hmac.New(sha256.New, []byte(c.secret))
	mac.Write([]byte(ts + c.key + bybitRecvWindow + payload))
	return hex.EncodeToString(mac.Sum(nil))
}

func (c *bybitV5) timestamp() string {
	return strconv.FormatInt(c.now().UnixMilli()+c.offsetMs.Load(), 10)
}

// get calls a GET endpoint. private signs it.
func (c *bybitV5) get(ctx context.Context, path string, q url.Values, private bool, out any) error {
	query := q.Encode()
	u := c.baseURL + path
	if query != "" {
		u += "?" + query
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	if private {
		c.authorize(req, query)
	}
	return c.do(req, path, out)
}

// post calls a signed POST endpoint with a JSON body.
func (c *bybitV5) post(ctx context.Context, path string, body any, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	c.authorize(req, string(raw))
	return c.do(req, path, out)
}

func (c *bybitV5) authorize(req *http.Request, payload string) {
	ts := c.timestamp()
	req.Header.Set("X-BAPI-API-KEY", c.key)
	req.Header.Set("X-BAPI-TIMESTAMP", ts)
	req.Header.Set("X-BAPI-RECV-WINDOW", bybitRecvWindow)
	req.Header.Set("X-BAPI-SIGN", c.sign(ts, payload))
}

func (c *bybitV5) do(req *http.Request, path string, out any) error {
	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %s %s: %v", errBybitUnsure, req.Method, path, err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("%w: %s %s: reading the answer: %v", errBybitUnsure, req.Method, path, err)
	}
	if res.StatusCode != http.StatusOK {
		snippet := strings.TrimSpace(string(body))
		if len(snippet) > 300 {
			snippet = snippet[:300]
		}
		// 403 from V5 is the IP rate limit ("access too frequent"): the docs
		// say to stop for at least ten minutes. Named, so nobody retries into a
		// longer ban.
		if res.StatusCode == http.StatusForbidden {
			return fmt.Errorf("%w: %s %s: HTTP 403, Bybit's IP rate limit (wait 10 minutes before retrying): %s", errBybitUnsure, req.Method, path, snippet)
		}
		return fmt.Errorf("%w: %s %s: HTTP %d: %s", errBybitUnsure, req.Method, path, res.StatusCode, snippet)
	}
	var env bybitEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return fmt.Errorf("%w: %s %s: unreadable answer: %v", errBybitUnsure, req.Method, path, err)
	}
	if env.RetCode != 0 {
		return &bybitAPIError{Code: env.RetCode, Msg: env.RetMsg, Path: path}
	}
	if out == nil || len(env.Result) == 0 {
		return nil
	}
	if err := json.Unmarshal(env.Result, out); err != nil {
		return fmt.Errorf("bybit %s: decoding the result: %w", path, err)
	}
	return nil
}

// syncTime measures the venue's clock (GET /v5/market/time, public).
func (c *bybitV5) syncTime(ctx context.Context) error {
	var r struct {
		TimeNano string `json:"timeNano"`
	}
	before := c.now()
	if err := c.get(ctx, "/v5/market/time", nil, false, &r); err != nil {
		return err
	}
	nano, err := strconv.ParseInt(r.TimeNano, 10, 64)
	if err != nil {
		return fmt.Errorf("bybit: reading the server time %q: %w", r.TimeNano, err)
	}
	// Assume the venue stamped its clock halfway through the round trip.
	mid := before.Add(c.now().Sub(before) / 2)
	c.offsetMs.Store(nano/1e6 - mid.UnixMilli())
	return nil
}

// ── V5 shapes: only the fields the adapter reads ─────────────────────────────

type bybitInstrument struct {
	Symbol         string `json:"symbol"`
	Status         string `json:"status"`
	ContractType   string `json:"contractType"`
	SettleCoin     string `json:"settleCoin"`
	LeverageFilter struct {
		MaxLeverage  string `json:"maxLeverage"`
		LeverageStep string `json:"leverageStep"`
	} `json:"leverageFilter"`
	PriceFilter struct {
		TickSize string `json:"tickSize"`
	} `json:"priceFilter"`
	LotSizeFilter struct {
		QtyStep          string `json:"qtyStep"`
		MinOrderQty      string `json:"minOrderQty"`
		MaxOrderQty      string `json:"maxOrderQty"`
		MaxMktOrderQty   string `json:"maxMktOrderQty"`
		MinNotionalValue string `json:"minNotionalValue"`
	} `json:"lotSizeFilter"`
}

type bybitOrderRecord struct {
	OrderID          string `json:"orderId"`
	OrderLinkID      string `json:"orderLinkId"`
	Symbol           string `json:"symbol"`
	Side             string `json:"side"`
	OrderType        string `json:"orderType"`
	OrderStatus      string `json:"orderStatus"`
	Price            string `json:"price"`
	Qty              string `json:"qty"`
	AvgPrice         string `json:"avgPrice"`
	CumExecQty       string `json:"cumExecQty"`
	CumExecFee       string `json:"cumExecFee"`
	CumFeeDetail     any    `json:"cumFeeDetail"`
	ClosedPnl        string `json:"closedPnl"`
	TriggerPrice     string `json:"triggerPrice"`
	StopOrderType    string `json:"stopOrderType"`
	ReduceOnly       bool   `json:"reduceOnly"`
	CloseOnTrigger   bool   `json:"closeOnTrigger"`
	CreateType       string `json:"createType"`
	CancelType       string `json:"cancelType"`
	RejectReason     string `json:"rejectReason"`
	CreatedTime      string `json:"createdTime"`
	UpdatedTime      string `json:"updatedTime"`
	PositionIdx      int    `json:"positionIdx"`
	TimeInForce      string `json:"timeInForce"`
	TriggerDirection int    `json:"triggerDirection"`
	TriggerBy        string `json:"triggerBy"`
}

type bybitPositionRecord struct {
	Symbol         string `json:"symbol"`
	Side           string `json:"side"`
	Size           string `json:"size"`
	AvgPrice       string `json:"avgPrice"`
	EntryPrice     string `json:"entryPrice"` // the websocket's name for avgPrice
	Leverage       string `json:"leverage"`
	PositionIM     string `json:"positionIM"`
	MarkPrice      string `json:"markPrice"`
	LiqPrice       string `json:"liqPrice"`
	UnrealisedPnl  string `json:"unrealisedPnl"`
	CumRealisedPnl string `json:"cumRealisedPnl"`
	PositionIdx    int    `json:"positionIdx"`
	PositionStatus string `json:"positionStatus"`
}

type bybitList[T any] struct {
	List           []T    `json:"list"`
	NextPageCursor string `json:"nextPageCursor"`
}
