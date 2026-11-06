package backtest

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/kdraigo/dev_sdk/types"
)

// A cancel of an order that already closed (a bracket's other leg) comes back
// as types.ErrOrderClosed; any other refused cancel stays a plain error.
func TestCancelOfAClosedOrder(t *testing.T) {
	replies := `{"action":"cancel","request_id":"a","status":"error","error":"order 9 is already canceled","code":"order_closed"}` + "\n" +
		`{"action":"cancel","request_id":"b","status":"error","error":"order 10 not found in any wallet"}`
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for _, m := range strings.Split(replies, "\n") {
			_ = c.WriteMessage(websocket.TextMessage, []byte(m))
		}
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	a, b := make(chan error, 1), make(chan error, 1)
	e := &EngineClient{pendingCancels: map[string]chan error{"a": a, "b": b}}
	go e.readLoop(conn, make(chan *types.Candle, 1), make(chan *types.Order, 1))

	for name, ch := range map[string]chan error{"a": a, "b": b} {
		select {
		case err := <-ch:
			closed := errors.Is(err, types.ErrOrderClosed)
			if err == nil || closed != (name == "a") {
				t.Fatalf("cancel %s: %v (closed %v)", name, err, closed)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("cancel %s: no answer", name)
		}
	}
}
