package backtest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kdraigo/dev_sdk/types"
)

func TestSimulationFor(t *testing.T) {
	assert.Nil(t, simulationFor(nil, nil), "nothing asked: no simulation object, as before")

	fee := 0.0004
	raw, err := json.Marshal(simulationFor(&types.SimulationOptions{TakerFee: &fee}, []types.MarketData{types.MarketDataFlow, types.MarketDataPositioning}))
	require.NoError(t, err)
	assert.JSONEq(t, `{"taker_fee":0.0004,"market_data":["flow","positioning"]}`, string(raw), "the options stay where the engine reads them")

	raw, err = json.Marshal(simulationFor(nil, []types.MarketData{types.MarketDataFlow}))
	require.NoError(t, err)
	assert.JSONEq(t, `{"market_data":["flow"]}`, string(raw))

	raw, err = json.Marshal(simulationFor(&types.SimulationOptions{TakerFee: &fee}, nil))
	require.NoError(t, err)
	assert.JSONEq(t, `{"taker_fee":0.0004}`, string(raw))
}

// fakeEngine upgrades one connection and runs serve on it.
func fakeEngine(t *testing.T, serve func(c *websocket.Conn)) *websocket.Conn {
	t.Helper()
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		serve(c)
	}))
	t.Cleanup(srv.Close)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestTick_MarketDataReachesTheCandle(t *testing.T) {
	frame := `{"action":"next","status":"ok","data":{"seq":0,"done":false,"orders":[],
		"tick":{"Exchange":"binance_futures","Pair":"BTC/USDT","Candle":{"time":"2026-01-05T12:00:00Z","updatedAt":"2026-01-05T12:59:59.999Z","open":1,"high":2,"low":1,"close":2,"volume":10,"complete":true}},
		"positioning":{"time":1767617700000,"open_time":1767617400000,"open_interest":95000.5,"open_interest_value":9.1e9,"long_short_ratio":2.4,"long_account_share":0.7,"top_account_ratio":2.6,"top_position_ratio":2.1,"taker_buy_sell_ratio":0.95,"periods":1},
		"flow":{"open_time":1767614400000,"buy_volume":6,"sell_volume":4,"delta":2,"buy_quote":12,"sell_quote":8,"vwap":2,"buy_trades":30,"sell_trades":20,"max_print_quote":150000,"large_prints_10k":3,"large_quote_10k":60000,"large_prints_100k":1,"large_quote_100k":150000,"minutes":60,"complete":true}}}`
	conn := fakeEngine(t, func(c *websocket.Conn) {
		_ = c.WriteMessage(websocket.TextMessage, []byte(frame))
		time.Sleep(200 * time.Millisecond)
	})

	candles := make(chan *types.Candle, 1)
	e := NewEngineClient(&types.Config{})
	e.smallestTF = types.Timeframe1h
	go e.readLoop(conn, candles, make(chan *types.Order, 1))

	select {
	case c := <-candles:
		require.NotNil(t, c.Positioning)
		require.NotNil(t, c.Flow)
		assert.Equal(t, time.Date(2026, 1, 5, 12, 55, 0, 0, time.UTC), c.Positioning.Time)
		assert.Equal(t, time.Date(2026, 1, 5, 12, 50, 0, 0, time.UTC), c.Positioning.OpenTime)
		assert.Equal(t, 95000.5, c.Positioning.OpenInterest)
		assert.Equal(t, 0.95, c.Positioning.TakerBuySellRatio)
		assert.Equal(t, time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC), c.Flow.OpenTime)
		assert.Equal(t, 2.0, c.Flow.Delta)
		assert.Equal(t, int64(1), c.Flow.LargePrints100k)
		assert.True(t, c.Flow.Complete)
	case <-time.After(5 * time.Second):
		t.Fatal("no candle")
	}
}

func TestTick_WithoutMarketDataTheFieldsStayNil(t *testing.T) {
	frame := `{"action":"next","status":"ok","data":{"done":false,"orders":[],
		"tick":{"Exchange":"binance","Pair":"BTC/USDT","Candle":{"time":"2026-01-05T12:00:00Z","updatedAt":"2026-01-05T12:59:59.999Z","open":1,"high":1,"low":1,"close":1,"complete":true}}}}`
	conn := fakeEngine(t, func(c *websocket.Conn) {
		_ = c.WriteMessage(websocket.TextMessage, []byte(frame))
		time.Sleep(200 * time.Millisecond)
	})
	candles := make(chan *types.Candle, 1)
	e := NewEngineClient(&types.Config{})
	go e.readLoop(conn, candles, make(chan *types.Order, 1))
	select {
	case c := <-candles:
		assert.Nil(t, c.Positioning)
		assert.Nil(t, c.Flow)
	case <-time.After(5 * time.Second):
		t.Fatal("no candle")
	}
}

// echoEngine answers each read request with reply(action, data) or an error.
func echoEngine(t *testing.T, reply func(action string, data map[string]any) (any, string)) *EngineClient {
	t.Helper()
	conn := fakeEngine(t, func(c *websocket.Conn) {
		for {
			var req struct {
				Action    string         `json:"action"`
				RequestID string         `json:"request_id"`
				Data      map[string]any `json:"data"`
			}
			if err := c.ReadJSON(&req); err != nil {
				return
			}
			out, errText := reply(req.Action, req.Data)
			msg := map[string]any{"action": req.Action, "request_id": req.RequestID, "status": "ok", "data": out}
			if errText != "" {
				msg = map[string]any{"action": req.Action, "request_id": req.RequestID, "status": "error", "error": errText}
			}
			if err := c.WriteJSON(msg); err != nil {
				return
			}
		}
	})
	e := NewEngineClient(&types.Config{})
	e.wsConn.Store(conn)
	go e.readLoop(conn, make(chan *types.Candle, 1), make(chan *types.Order, 1))
	return e
}

func TestMarketDataReads(t *testing.T) {
	var asked []map[string]any
	var actions []string
	e := echoEngine(t, func(action string, data map[string]any) (any, string) {
		actions = append(actions, action)
		asked = append(asked, data)
		switch action {
		case "funding_history":
			return map[string]any{"rates": []map[string]any{
				{"symbol": "BTCUSDT", "funding_time": int64(1712102400001), "funding_rate": 0.0001, "mark_price": 65488.9, "interval_hours": 8},
			}}, ""
		case "positioning_history":
			return map[string]any{"bars": []map[string]any{
				{"time": int64(1712106000000), "open_time": int64(1712102400000), "open_interest": 77798.371, "periods": 12},
			}}, ""
		case "flow_history":
			if data["pair"] == "NOPE/USDT" {
				return nil, "data-provider /api/v1/flow status=400"
			}
			return map[string]any{"bars": []map[string]any{
				{"open_time": int64(1762560000000), "buy_volume": 370.6, "sell_volume": 300, "delta": 70.6, "minutes": 60, "complete": true},
			}}, ""
		}
		return nil, "Unknown action"
	})
	ctx := context.Background()
	from, to := time.UnixMilli(1712102400000).UTC(), time.UnixMilli(1712361599999).UTC()

	rates, err := e.GetFundingRates(ctx, "binance_futures", "BTC/USDT", from, to)
	require.NoError(t, err)
	require.Len(t, rates, 1)
	assert.Equal(t, time.UnixMilli(1712102400001).UTC(), rates[0].Time)
	assert.Equal(t, 0.0001, rates[0].Rate)
	assert.Equal(t, 8, rates[0].IntervalHours)

	pos, err := e.GetPositioning(ctx, "binance_futures", "BTC/USDT", types.Timeframe1h, from, to)
	require.NoError(t, err)
	require.Len(t, pos, 1)
	assert.Equal(t, time.UnixMilli(1712106000000).UTC(), pos[0].Time)
	assert.Equal(t, 12, pos[0].Periods)

	flow, err := e.GetFlow(ctx, "binance", "BTC/USDT", types.Timeframe1h, from, to)
	require.NoError(t, err)
	require.Len(t, flow, 1)
	assert.Equal(t, 70.6, flow[0].Delta)

	_, err = e.GetFlow(ctx, "binance", "NOPE/USDT", types.Timeframe1h, from, to)
	assert.ErrorContains(t, err, "status=400", "the engine's error text reaches the strategy")

	assert.Equal(t, []string{"funding_history", "positioning_history", "flow_history", "flow_history"}, actions)
	assert.NotContains(t, asked[0], "timeframe", "funding has no timeframe")
	assert.Equal(t, "1h", asked[1]["timeframe"])
	assert.Equal(t, "BTC/USDT", asked[1]["pair"])
}
