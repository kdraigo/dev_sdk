// Package paper is the adapter for kdraigo's paper exchange (the
// paper_binance and paper_binance_futures environments): orders go to the
// platform's livetester engine, which fills them against Binance's real order
// books and trades, and the strategy sees the engine's closed 1m candles and
// its order updates over one signed WebSocket.
//
// A paper run is a live run: wall clock, candles as they close, the futures
// caps of LiveOptions (but no arming gates, since no real funds are at
// stake). The account lives on the platform, so its resting orders and the
// stops attached to them keep working while the bot is away, and the bot
// carries on where it left off with PaperOptions.AccountID.
package paper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kdraigo/dev_sdk/exchange/live"
	"github.com/kdraigo/dev_sdk/signing"
	"github.com/kdraigo/dev_sdk/types"
)

// DefaultEndpoint is the hosted paper exchange.
const DefaultEndpoint = "https://api.kdraigo.com"

// DefaultDataURL is the hosted data API, where history is read.
const DefaultDataURL = "https://api.kdraigo.com/data"

const apiPrefix = "/api/v1/paper"

// Client is the paper adapter.
type Client struct {
	cfg      *types.Config
	venue    string // binance | binance_futures
	kind     live.MarketKind
	endpoint string
	dataURL  string
	http     *http.Client

	accountID string
	// symbols maps the venue's symbol (BTCUSDT) to the one the strategy
	// configured (BTCUSDT or BTC/USDT): candles and orders carry the latter.
	symbols map[string]string
	filters map[string]live.InstrumentFilter

	mu       sync.Mutex
	conn     *websocket.Conn
	connUp   chan struct{} // closed while a connection is up; replaced on a drop
	pending  map[string]chan frame
	last     map[string]float64 // last close per venue symbol: values a market order
	lastOpen map[string]int64   // last candle pushed per venue symbol (open time, ms)
	seen     map[uint64]belief  // what the strategy was told of each open order
	done     map[uint64]bool    // orders already reported finished
	doneFIFO []uint64
	open     map[uint64]bool // orders this process knows to be open
	// placing holds the client ids of orders on their way, and of those the
	// engine refused: such an order's REJECTED row is the refusal PlaceOrder
	// returned, not an update (an exchange sends none).
	placing     map[string]bool
	refusedFIFO []string
	intent      map[uint64]types.OrderType // the type each order was placed as
	writeMu     sync.Mutex

	ended  chan struct{}
	endErr error
	endMu  sync.Once

	queue    chan queued
	resumed  bool // PrepareSession attached to an existing account
	onResume func(*types.SessionState)

	// reconnect pacing; variables so tests can shorten them.
	reconnectBase time.Duration
	reconnectMax  time.Duration
	callTimeout   time.Duration
}

type belief struct {
	status types.OrderStatus
	filled float64
}

// NewClient makes the adapter for a paper environment. cfg.Live's exchanges
// default to the paper venue.
func NewClient(cfg *types.Config) *Client {
	c := &Client{
		cfg: cfg, venue: "binance", kind: live.MarketSpot, http: &http.Client{Timeout: 20 * time.Second},
		symbols: map[string]string{}, filters: map[string]live.InstrumentFilter{},
		connUp: make(chan struct{}), pending: map[string]chan frame{}, last: map[string]float64{},
		lastOpen: map[string]int64{}, seen: map[uint64]belief{}, done: map[uint64]bool{}, open: map[uint64]bool{},
		intent: map[uint64]types.OrderType{}, placing: map[string]bool{}, ended: make(chan struct{}),
		reconnectBase: time.Second, reconnectMax: 30 * time.Second, callTimeout: 20 * time.Second,
	}
	if cfg.Environment == types.EnvPaperBinanceFutures {
		c.venue, c.kind = types.ExchangeBinanceFutures, live.MarketLinearPerp
	}
	c.endpoint = DefaultEndpoint
	if cfg.Paper != nil && cfg.Paper.Endpoint != "" {
		c.endpoint = strings.TrimRight(cfg.Paper.Endpoint, "/")
	}
	c.dataURL = DefaultDataURL
	if cfg.Live != nil {
		if cfg.Live.DataURL != "" {
			c.dataURL = strings.TrimRight(cfg.Live.DataURL, "/")
		}
		if len(cfg.Live.RequestedExchanges) == 0 {
			cfg.Live.RequestedExchanges = []string{c.venue}
		}
	}
	return c
}

// AccountID is the paper account the run trades, once PrepareSession ran.
func (c *Client) AccountID() string { return c.accountID }

func (c *Client) guard() live.Guard {
	return live.Guard{Env: c.cfg.Environment, Opts: c.cfg.Live}
}

// PrepareSession opens or resumes the paper account, applies the configured
// leverage and reads each market's order rules.
func (c *Client) PrepareSession(ctx context.Context, cfg *types.Config) error {
	c.cfg = cfg
	if cfg.Credentials.KeyID == "" || cfg.Credentials.PrivateKey == "" {
		return errors.New("paper: a paper run signs with a kdraigo API key; set Credentials.KeyID and Credentials.PrivateKey")
	}
	if cfg.Live == nil || len(cfg.Live.Assets) == 0 {
		return errors.New("paper: set Config.Live.Assets to the markets to trade (a paper run reads Live like an exchange run)")
	}
	for _, ex := range cfg.Live.RequestedExchanges {
		if ex != c.venue {
			return fmt.Errorf("paper: %s trades %s only, not %q", cfg.Environment, c.venue, ex)
		}
	}
	if err := c.guard().CheckSession(); err != nil {
		return err
	}
	var symbols []string
	for _, a := range cfg.Live.Assets {
		vs := live.VenueSymbol(a)
		if _, dup := c.symbols[vs]; !dup {
			symbols = append(symbols, vs)
		}
		c.symbols[vs] = a
	}
	opts := cfg.Paper
	if opts == nil {
		opts = &types.PaperOptions{}
	}

	var positions []positionWire
	if opts.AccountID != "" {
		v, err := c.resume(ctx, opts.AccountID, symbols)
		if err != nil {
			return err
		}
		positions = v.Positions
		c.resumed = true
	} else {
		if err := c.create(ctx, opts, symbols); err != nil {
			return err
		}
	}

	if err := c.loadFilters(ctx, symbols); err != nil {
		return err
	}
	if c.kind == live.MarketLinearPerp {
		for pair, lev := range cfg.Live.Leverage {
			if err := c.applyLeverage(ctx, live.VenueSymbol(pair), lev, positions); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client) create(ctx context.Context, opts *types.PaperOptions, symbols []string) error {
	body := map[string]any{"venue": c.venue, "symbols": symbols, "name": opts.Name,
		"fill_model": opts.FillModel, "latency_ms": opts.LatencyMs, "strategy_name": c.cfg.Live.StrategyName}
	if len(opts.Balances) > 0 {
		bal := map[string]string{}
		for asset, v := range opts.Balances {
			bal[strings.ToUpper(asset)] = fmt.Sprint(v)
		}
		body["balances"] = bal
	}
	var acc accountWire
	if err := c.rest(ctx, http.MethodPost, apiPrefix+"/accounts", body, &acc); err != nil {
		return fmt.Errorf("paper: opening an account: %w", err)
	}
	c.accountID = acc.ID
	log.Printf("Paper: opened account %s (%s: %s). Set Paper.AccountID = %q to carry on with it after a restart.",
		acc.ID, c.venue, strings.Join(symbols, ", "), acc.ID)
	return nil
}

func (c *Client) resume(ctx context.Context, id string, symbols []string) (*view, error) {
	// A running account answers with its live view, a stopped one with its
	// stored row.
	var raw json.RawMessage
	if err := c.rest(ctx, http.MethodGet, apiPrefix+"/accounts/"+url.PathEscape(id), nil, &raw); err != nil {
		return nil, fmt.Errorf("paper: resuming account %s: %w", id, err)
	}
	var v view
	if err := json.Unmarshal(raw, &v); err != nil || v.Account.ID == "" {
		var acc accountWire
		if err := json.Unmarshal(raw, &acc); err != nil {
			return nil, fmt.Errorf("paper: resuming account %s: %w", id, err)
		}
		v = view{Account: acc}
	}
	acc := v.Account
	if acc.Status != "RUNNING" {
		reason := optString(acc.StopReason)
		if reason != "" {
			reason = " (" + reason + ")"
		}
		return nil, fmt.Errorf("paper: account %s is %s%s; leave Paper.AccountID empty to open a new one", id, strings.ToLower(acc.Status), reason)
	}
	if acc.Venue != c.venue {
		return nil, fmt.Errorf("paper: account %s trades %s, not %s", id, acc.Venue, c.venue)
	}
	if acc.Kind != "bot" || acc.APIKeyID == nil || *acc.APIKeyID != c.cfg.Credentials.KeyID {
		return nil, fmt.Errorf("paper: account %s was not opened with this API key; a bot attaches only to an account its own key opened", id)
	}
	has := map[string]bool{}
	for _, s := range v.Symbols {
		has[s] = true
	}
	for _, s := range symbols {
		if !has[s] {
			return nil, fmt.Errorf("paper: account %s does not trade %s (it trades %s); its markets are fixed when it opens", id, s, strings.Join(v.Symbols, ", "))
		}
	}
	c.accountID = acc.ID
	log.Printf("Paper: resumed account %s (%s: %s, %d open orders, %d positions)", acc.ID, c.venue,
		strings.Join(v.Symbols, ", "), len(v.Orders), len(v.Positions))
	return &v, nil
}

// loadFilters reads each market's tick, step and minimums from the rules the
// engine builds its markets from, so rounding here matches the engine.
func (c *Client) loadFilters(ctx context.Context, symbols []string) error {
	var all map[string]map[string]instrumentWire
	if err := c.rest(ctx, http.MethodGet, apiPrefix+"/instruments", nil, &all); err != nil {
		return fmt.Errorf("paper: reading the markets: %w", err)
	}
	for _, s := range symbols {
		in, ok := all[c.venue][s]
		if !ok || in.Rules == nil {
			return fmt.Errorf("paper: %s is not traded on paper %s", s, c.venue)
		}
		c.filters[s] = live.InstrumentFilter{TickSize: parse(in.TickSize), StepSize: parse(in.Rules.StepSize),
			MinQty: parse(in.Rules.MinQty), MinNotional: parse(in.Rules.MinNotional)}
	}
	return nil
}

func (c *Client) applyLeverage(ctx context.Context, symbol string, lev float64, positions []positionWire) error {
	if lev != math.Trunc(lev) || lev < 1 {
		return fmt.Errorf("paper: leverage on %s must be a whole number of at least 1, got %v", symbol, lev)
	}
	for _, p := range positions {
		if p.Symbol == symbol && float64(p.Leverage) == lev {
			return nil // already so, with a position open
		}
	}
	var res orderResult
	err := c.rest(ctx, http.MethodPost, apiPrefix+"/accounts/"+c.accountID+"/leverage", map[string]any{"symbol": symbol, "leverage": int(lev)}, &res)
	if err != nil {
		return fmt.Errorf("paper: leverage %vx on %s: %w", lev, symbol, err)
	}
	return nil
}

// rest sends a signed request to the engine. A refusal comes back as an
// error carrying the engine's words (a *Rejected for an order rule).
func (c *Client) rest(ctx context.Context, method, path string, body any, out any) error {
	var b []byte
	if body != nil {
		var err error
		if b, err = json.Marshal(body); err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if path != apiPrefix+"/instruments" {
		h, err := signing.Headers(signing.StyleStandard, c.cfg.Credentials.KeyID, c.cfg.Credentials.PrivateKey, method, path, nil, b)
		if err != nil {
			return err
		}
		for k, v := range h {
			req.Header[k] = v
		}
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if resp.StatusCode/100 != 2 {
		var e struct {
			Error  string      `json:"error"`
			Code   string      `json:"code"`
			Reject *rejectWire `json:"reject"`
		}
		_ = json.Unmarshal(data, &e)
		if e.Reject != nil {
			return &Rejected{Code: e.Reject.Code, Reason: e.Reject.Reason, Msg: e.Reject.Msg}
		}
		if e.Error == "" {
			e.Error = strings.TrimSpace(string(data))
		}
		return &HTTPError{Status: resp.StatusCode, Code: e.Code, Msg: e.Error}
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

// Rejected is an order, leverage or cancel the paper exchange refused under
// the venue's rules, with Binance's code and words.
type Rejected struct {
	Code   int
	Reason string
	Msg    string
}

func (r *Rejected) Error() string {
	return fmt.Sprintf("paper exchange refused (%d %s): %s", r.Code, r.Reason, r.Msg)
}

// HTTPError is a refusal of a REST call.
type HTTPError struct {
	Status int
	Code   string
	Msg    string
}

func (e *HTTPError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("paper exchange: %s (%s, status %d)", e.Msg, e.Code, e.Status)
	}
	return fmt.Sprintf("paper exchange: %s (status %d)", e.Msg, e.Status)
}

func parse(s string) float64 {
	var n num
	_ = n.UnmarshalJSON([]byte(s))
	return float64(n)
}
