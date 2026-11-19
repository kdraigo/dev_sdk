package paper

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kdraigo/dev_sdk/signing"
	"github.com/kdraigo/dev_sdk/types"
)

// mockEngine is the paper exchange's API as the adapter uses it: REST for
// the account, the signed WebSocket, and the data API's candles.
type mockEngine struct {
	t     *testing.T
	srv   *httptest.Server
	keyID string
	priv  string
	pub   ed25519.PublicKey

	mu        sync.Mutex
	venue     string
	accountID string
	status    string
	symbols   []string
	created   map[string]any
	leverage  map[string]int
	orders    map[uint64]map[string]any
	byCID     map[string][]uint64
	nextID    uint64
	requests  []map[string]any // every WS request, in order
	conn      *websocket.Conn
	writeMu   sync.Mutex
	dials     int
	dropOn    string // drop the connection, unanswered, on the first request of this action
	candles   []map[string]any
	connected chan struct{}
	plots     map[string]any // the answer to a plots action (default: stored)
}

func newMockEngine(t *testing.T, venue string) *mockEngine {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	m := &mockEngine{t: t, keyID: "key-1", priv: hex.EncodeToString(priv), pub: pub, venue: venue, status: "RUNNING",
		leverage: map[string]int{}, orders: map[uint64]map[string]any{}, byCID: map[string][]uint64{}, nextID: 100,
		connected: make(chan struct{}, 16)}
	m.srv = httptest.NewServer(http.HandlerFunc(m.serve))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *mockEngine) config(env types.Environment) *types.Config {
	return &types.Config{Environment: env, Timeframes: []types.Timeframe{types.Timeframe1m},
		Credentials: types.Credentials{KeyID: m.keyID, PrivateKey: m.priv},
		Live:        &types.LiveOptions{Assets: []string{"BTCUSDT"}, DataURL: m.srv.URL + "/data", MaxOrderNotional: 50_000, MaxLeverage: 10, Leverage: map[string]float64{"BTCUSDT": 3}},
		Paper:       &types.PaperOptions{Endpoint: m.srv.URL, Name: "test", Balances: map[string]float64{"USDT": 20_000}}}
}

// verify checks a request's version 2 signature.
func (m *mockEngine) verify(r *http.Request, body []byte) bool {
	if r.Header.Get("X-Key-ID") != m.keyID {
		return false
	}
	sig, err := hex.DecodeString(r.Header.Get("X-Signature"))
	if err != nil {
		return false
	}
	c := signing.Canonical(r.Method, r.URL.Path, r.URL.Query(), r.Header.Get("X-Timestamp"), r.Header.Get("X-Nonce"), body)
	return ed25519.Verify(m.pub, []byte(c), sig)
}

func (m *mockEngine) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	reply := func(status int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(v)
	}
	p := r.URL.Path
	switch {
	case p == "/data/candles":
		from, _ := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
		to, _ := strconv.ParseInt(r.URL.Query().Get("to"), 10, 64)
		m.mu.Lock()
		var out []map[string]any
		for _, c := range m.candles {
			if o := c["open_time"].(int64); o >= from && o <= to {
				out = append(out, c)
			}
		}
		m.mu.Unlock()
		reply(200, out)
		return
	case p == apiPrefix+"/instruments":
		reply(200, map[string]any{m.venue: map[string]any{"BTCUSDT": map[string]any{"tick_size": "0.10",
			"rules": map[string]any{"step_size": "0.001", "min_qty": "0.001", "min_notional": "100"}}}})
		return
	}
	if !m.verify(r, body) {
		reply(401, map[string]any{"error": "unauthorized signature"})
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case p == apiPrefix+"/accounts" && r.Method == http.MethodPost:
		_ = json.Unmarshal(body, &m.created)
		m.accountID = "acc-1"
		for _, s := range m.created["symbols"].([]any) {
			m.symbols = append(m.symbols, s.(string))
		}
		reply(201, m.accountJSON())
	case p == apiPrefix+"/accounts/"+m.accountID && r.Method == http.MethodGet:
		if m.status != "RUNNING" {
			reply(200, m.accountJSON())
			return
		}
		reply(200, m.viewLocked())
	case p == apiPrefix+"/accounts/"+m.accountID+"/leverage":
		var b struct {
			Symbol   string `json:"symbol"`
			Leverage int    `json:"leverage"`
		}
		_ = json.Unmarshal(body, &b)
		m.leverage[b.Symbol] = b.Leverage
		reply(200, map[string]any{})
	case p == apiPrefix+"/accounts/"+m.accountID+"/ws":
		if m.status != "RUNNING" {
			reply(409, map[string]any{"error": "the account is stopped"})
			return
		}
		m.mu.Unlock()
		m.ws(w, r)
		m.mu.Lock()
	default:
		reply(404, map[string]any{"error": "account not found"})
	}
}

func (m *mockEngine) accountJSON() map[string]any {
	return map[string]any{"id": m.accountID, "kind": "bot", "venue": m.venue, "status": m.status, "api_key_id": m.keyID,
		"stop_reason": nil}
}

func (m *mockEngine) viewLocked() map[string]any {
	var open []map[string]any
	for id := uint64(0); id <= m.nextID; id++ {
		if o, ok := m.orders[id]; ok && (o["status"] == "NEW" || o["status"] == "PARTIALLY_FILLED") {
			open = append(open, o)
		}
	}
	return map[string]any{"account": m.accountJSON(), "symbols": m.symbols,
		"balances": []map[string]any{{"asset": "USDT", "bucket": "free", "amount": "19000"}, {"asset": "USDT", "bucket": "locked", "amount": "600"},
			{"asset": "USDT", "bucket": "position_margin:BTCUSDT", "amount": "400"}, {"asset": "USDT", "bucket": "deposit", "amount": "-20000"},
			{"asset": "USDT", "bucket": "fees", "amount": "0.3"}, {"asset": "BTC", "bucket": "exchange", "amount": "1"}},
		"positions":   []map[string]any{{"symbol": "BTCUSDT", "side": "LONG", "size": "0.010", "entry_price": "60000", "leverage": 3, "isolated_margin": "400", "mark": "60100", "unrealized_pnl": "1", "liquidation_price": "40000.5", "realized_pnl": "2", "fees": "0.3", "funding": "0.1"}},
		"open_orders": open}
}

var upgrader = websocket.Upgrader{}

func (m *mockEngine) ws(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	m.mu.Lock()
	m.conn = conn
	m.dials++
	state := m.viewLocked()
	m.mu.Unlock()
	m.write(conn, map[string]any{"action": "session_state", "status": "ok", "data": state})
	m.connected <- struct{}{}
	for {
		var req struct {
			Action    string          `json:"action"`
			RequestID string          `json:"request_id"`
			Data      json.RawMessage `json:"data"`
		}
		if err := conn.ReadJSON(&req); err != nil {
			return
		}
		var data map[string]any
		_ = json.Unmarshal(req.Data, &data)
		m.mu.Lock()
		m.requests = append(m.requests, map[string]any{"action": req.Action, "data": data})
		drop := m.dropOn == req.Action
		if drop {
			m.dropOn = ""
		}
		resp := m.answerLocked(req.Action, data)
		m.mu.Unlock()
		if drop {
			conn.Close() // the request landed; its answer is lost
			return
		}
		resp["action"], resp["request_id"] = req.Action, req.RequestID
		m.write(conn, resp)
	}
}

func (m *mockEngine) write(conn *websocket.Conn, v any) {
	m.writeMu.Lock()
	defer m.writeMu.Unlock()
	_ = conn.WriteJSON(v)
}

// newOrderLocked makes an order row from a request, once per client id.
func (m *mockEngine) newOrderLocked(d map[string]any, group *int64) map[string]any {
	m.nextID++
	now := time.Now().UTC()
	o := map[string]any{"id": m.nextID, "client_order_id": d["client_order_id"], "symbol": d["symbol"], "side": d["side"],
		"type": d["type"], "time_in_force": d["time_in_force"], "price": d["price"], "stop_price": d["stop_price"],
		"quantity": d["quantity"], "filled_quantity": "0", "avg_price": nil, "fee": "0", "fee_asset": nil,
		"reduce_only": d["reduce_only"] == true, "group_id": group, "parent_id": nil, "status": "NEW", "placed_at": now, "updated_at": now}
	if d["type"] == "MARKET" {
		o["status"], o["filled_quantity"], o["avg_price"], o["fee"], o["fee_asset"] = "FILLED", d["quantity"], "60000", "0.24", "USDT"
	}
	m.orders[m.nextID] = o
	return o
}

func (m *mockEngine) answerLocked(action string, d map[string]any) map[string]any {
	ok := func(v any) map[string]any { return map[string]any{"status": "ok", "data": v} }
	switch action {
	case "order":
		cid, _ := d["client_order_id"].(string)
		if ids, seen := m.byCID[cid]; seen {
			return ok(map[string]any{"orders": []any{m.orders[ids[0]]}})
		}
		if d["price"] == "99999.0" {
			// Refused under the price band: the engine stores the row and
			// pushes it, here before its answer.
			o := m.newOrderLocked(d, nil)
			o["status"], o["reject_code"] = "REJECTED", -4016
			go m.push("order_update", map[string]any{"order": o})
			time.Sleep(50 * time.Millisecond)
			rj := map[string]any{"code": -4016, "reason": "percent_price", "msg": "Limit price can't be higher than 61800.00."}
			return map[string]any{"status": "error", "code": "percent_price", "error": rj["msg"], "data": map[string]any{"reject": rj, "orders": []any{o}}}
		}
		o := m.newOrderLocked(d, nil)
		m.byCID[cid] = []uint64{o["id"].(uint64)}
		res := map[string]any{"orders": []any{o}}
		if o["status"] == "FILLED" {
			res["fills"] = []any{map[string]any{"order_id": o["id"], "price": "60000", "quantity": o["quantity"], "fee": "0.24"}}
		}
		return ok(res)
	case "order_bracket":
		legs := d["legs"].([]any)
		cid := legs[0].(map[string]any)["client_order_id"].(string)
		if ids, seen := m.byCID[cid]; seen {
			return ok(map[string]any{"orders": []any{m.orders[ids[0]], m.orders[ids[1]]}})
		}
		g := int64(m.nextID + 1)
		var out []any
		var ids []uint64
		for _, l := range legs {
			o := m.newOrderLocked(l.(map[string]any), &g)
			out, ids = append(out, o), append(ids, o["id"].(uint64))
		}
		m.byCID[cid] = ids
		return ok(map[string]any{"orders": out})
	case "cancel":
		id := uint64(d["order_id"].(float64))
		o, found := m.orders[id]
		if !found || o["status"] != "NEW" {
			return map[string]any{"status": "error", "code": "unknown_order", "error": "Unknown order sent.",
				"data": map[string]any{"reject": map[string]any{"code": -2011, "reason": "unknown_order", "msg": "Unknown order sent."}}}
		}
		o["status"] = "CANCELED"
		return ok(map[string]any{"orders": []any{o}})
	case "get_order":
		o, found := m.orders[uint64(d["order_id"].(float64))]
		if !found {
			return map[string]any{"status": "error", "code": "unknown_order", "error": "not found"}
		}
		return ok(o)
	case "open_orders":
		return ok(m.viewLocked()["open_orders"])
	case "positions":
		return ok(m.viewLocked()["positions"])
	case "account":
		return ok(m.viewLocked())
	case "contract":
		return ok(map[string]any{"symbol": "BTCUSDT", "tick_size": "0.1", "step_size": "0.001", "min_qty": "0.001", "min_notional": 100,
			"mark": "60010.5", "last": "60000", "max_leverage": 125, "maintenance_margin_rate": "0.004"})
	case "realized_pnl":
		return ok(map[string]any{"realized_pnl": "-12.5"})
	case "leverage":
		m.leverage[d["symbol"].(string)] = int(d["leverage"].(float64))
		return ok(map[string]any{})
	case "close":
		return ok(map[string]any{"detached": true})
	case "plots":
		if m.plots != nil {
			return m.plots
		}
		return ok(map[string]any{"warning": ""})
	}
	return map[string]any{"status": "error", "code": "unknown_action", "error": "unknown action " + action}
}

// push sends an event to the connected client.
func (m *mockEngine) push(typ string, data any) {
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()
	m.write(conn, map[string]any{"action": typ, "status": "ok",
		"data": map[string]any{"type": typ, "market": m.venue + ":BTCUSDT", "data": data}})
}

func (m *mockEngine) pushCandle(open int64, close float64) {
	m.push("candle", candleRow(open, close))
}

func candleRow(open int64, close float64) map[string]any {
	return map[string]any{"open_time": open, "close_time": open + 59_999, "open": close - 1, "high": close + 2, "low": close - 3,
		"close": close, "volume": 1.5, "quote_volume": 90_000.0, "trade_count": int64(42), "taker_buy_base": 0.7, "taker_buy_quote": 42_000.0}
}

// dataRow is a candle as the data API returns it.
func dataRow(open int64, close float64) map[string]any {
	return map[string]any{"open_time": open, "close_time": open + 60_000, "open": close - 1, "high": close + 2, "low": close - 3,
		"close": close, "volume": 1.5, "quote_asset_volume": 90_000.0, "trade_num": 42}
}

// dropConn closes the client's connection from the server side.
func (m *mockEngine) dropConn() {
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()
	conn.Close()
}

func (m *mockEngine) waitConnected(t *testing.T) {
	t.Helper()
	select {
	case <-m.connected:
	case <-time.After(5 * time.Second):
		t.Fatal("no connection")
	}
}

func (m *mockEngine) actions() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, r := range m.requests {
		out = append(out, fmt.Sprint(r["action"]))
	}
	return out
}

func (m *mockEngine) lastRequest(action string) map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := len(m.requests) - 1; i >= 0; i-- {
		if m.requests[i]["action"] == action {
			d, _ := m.requests[i]["data"].(map[string]any)
			return d
		}
	}
	return nil
}

func (m *mockEngine) count(action string) int {
	n := 0
	for _, a := range m.actions() {
		if a == action {
			n++
		}
	}
	return n
}
