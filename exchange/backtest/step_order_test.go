package backtest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/kdraigo/dev_sdk/types"
)

// A step's fills are queued before its candle: they filled inside the bar,
// and the SDK hands a step's fills to the strategy before the bar only if
// they are already queued when the candle arrives (bug #16).
func TestReadLoop_QueuesAStepsFillsBeforeItsCandle(t *testing.T) {
	step := `{"action":"next","status":"ok","data":{"seq":1,"done":false,
		"tick":{"Exchange":"binance","Pair":"BTC/USDT","Candle":{"time":"2025-01-13T13:00:00Z","updatedAt":"2025-01-13T13:59:59.999Z",
			"open":176,"high":177,"low":170,"close":175.78,"volume":10,"complete":true}},
		"orders":[{"id":7,"exchange":"binance","pair":"BTC/USDT","side":"SELL","type":"STOP_LOSS_LIMIT","status":"FILLED",
			"price":170.5066,"quantity":10,"stop":170.5066,"created_at":"2025-01-13T09:59:00Z","updated_at":"2025-01-13T13:59:00Z"}]}}`
	done := `{"action":"next","status":"ok","data":{"seq":2,"done":true}}`
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		_ = c.WriteMessage(websocket.TextMessage, []byte(step))
		_ = c.WriteMessage(websocket.TextMessage, []byte(done))
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Orders unbuffered: the loop can queue the candle only once the fill
	// has been taken, if the fill goes first.
	e := &EngineClient{}
	candles := make(chan *types.Candle, 1)
	orders := make(chan *types.Order)
	go e.readLoop(conn, candles, orders)

	select {
	case c := <-candles:
		t.Fatalf("the candle (%v) was queued before its step's fill", c.OpenTime)
	case <-time.After(300 * time.Millisecond):
	}
	select {
	case o := <-orders:
		if o.ID != "7" || o.Status != types.OrderStatusFilled {
			t.Fatalf("fill %+v", o)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no fill")
	}
	select {
	case c := <-candles:
		if c.Close != 175.78 {
			t.Fatalf("candle %+v", c)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no candle after the fill")
	}
}
