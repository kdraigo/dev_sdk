package live

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeBybit answers the V5 endpoints the adapter uses, checks every private
// request's signature the way Bybit does, and records what it was sent.
type fakeBybit struct {
	t           *testing.T
	key, secret string
	srv         *httptest.Server

	mu     sync.Mutex
	calls  []fakeCall
	routes map[string]func(c fakeCall) fakeAnswer

	// websocket side
	wsConns  int
	onPublic func(conn *websocket.Conn)
	onPriv   func(conn *websocket.Conn, n int)
}

type fakeCall struct {
	Method string
	Path   string
	Query  url.Values
	Body   map[string]any
}

type fakeAnswer struct {
	HTTP    int
	RetCode int
	RetMsg  string
	Result  any
}

func ok(result any) fakeAnswer { return fakeAnswer{Result: result} }

func newFakeBybit(t *testing.T) *fakeBybit {
	f := &fakeBybit{t: t, key: "test-key", secret: "test-secret", routes: map[string]func(fakeCall) fakeAnswer{}}
	f.routes["/v5/market/time"] = func(fakeCall) fakeAnswer {
		return ok(map[string]string{"timeNano": strconv.FormatInt(time.Now().UnixNano(), 10)})
	}
	f.routes["/v5/market/instruments-info"] = func(c fakeCall) fakeAnswer {
		if c.Query.Get("category") != "linear" {
			return fakeAnswer{RetCode: 10001, RetMsg: "category"}
		}
		return ok(map[string]any{"list": []any{map[string]any{
			"symbol": c.Query.Get("symbol"), "status": "Trading", "contractType": "LinearPerpetual", "settleCoin": "USDT",
			"leverageFilter": map[string]string{"maxLeverage": "100", "leverageStep": "0.01"},
			"priceFilter":    map[string]string{"tickSize": "0.10"},
			"lotSizeFilter":  map[string]string{"qtyStep": "0.001", "minOrderQty": "0.001", "maxOrderQty": "100", "minNotionalValue": "5"},
		}}})
	}
	f.routes["/v5/account/info"] = func(fakeCall) fakeAnswer {
		return ok(map[string]any{"marginMode": "ISOLATED_MARGIN", "unifiedMarginStatus": 6})
	}
	f.routes["/v5/position/list"] = func(c fakeCall) fakeAnswer {
		return ok(map[string]any{"list": []any{map[string]any{"symbol": "BTCUSDT", "side": "", "size": "0", "positionIdx": 0}}})
	}
	f.routes["/v5/position/set-leverage"] = func(fakeCall) fakeAnswer {
		return fakeAnswer{RetCode: bybitCodeLeverageUnchanged, RetMsg: "Set leverage has not been modified."}
	}
	f.routes["/v5/market/tickers"] = func(c fakeCall) fakeAnswer {
		return ok(map[string]any{"list": []any{map[string]string{"symbol": c.Query.Get("symbol"), "markPrice": "100000"}}})
	}
	f.routes["/v5/order/create"] = func(c fakeCall) fakeAnswer {
		return ok(map[string]string{"orderId": "o-" + c.Body["orderLinkId"].(string), "orderLinkId": c.Body["orderLinkId"].(string)})
	}
	f.routes["/v5/order/realtime"] = func(c fakeCall) fakeAnswer { return ok(map[string]any{"list": []any{}}) }
	f.routes["/v5/order/history"] = func(c fakeCall) fakeAnswer { return ok(map[string]any{"list": []any{}}) }
	f.routes["/v5/order/cancel"] = func(c fakeCall) fakeAnswer { return ok(map[string]string{"orderId": c.Body["orderId"].(string)}) }

	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

var upgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func (f *fakeBybit) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v5/public/linear" || r.URL.Path == "/v5/private" {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		f.mu.Lock()
		f.wsConns++
		n := f.wsConns
		pub, priv := f.onPublic, f.onPriv
		f.mu.Unlock()
		if r.URL.Path == "/v5/public/linear" && pub != nil {
			pub(conn)
		}
		if r.URL.Path == "/v5/private" && priv != nil {
			priv(conn, n)
		}
		return
	}

	raw, _ := io.ReadAll(r.Body)
	c := fakeCall{Method: r.Method, Path: r.URL.Path, Query: r.URL.Query()}
	if len(raw) > 0 {
		require.NoError(f.t, json.Unmarshal(raw, &c.Body))
	}
	f.mu.Lock()
	f.calls = append(f.calls, c)
	route := f.routes[r.URL.Path]
	f.mu.Unlock()

	var ans fakeAnswer
	if !strings.HasPrefix(r.URL.Path, "/v5/market/") && !f.signed(r, raw) {
		ans = fakeAnswer{RetCode: bybitCodeBadSign, RetMsg: "error sign!"}
	} else if route == nil {
		ans = fakeAnswer{HTTP: http.StatusNotFound}
	} else {
		ans = route(c)
	}
	if ans.HTTP != 0 && ans.HTTP != http.StatusOK {
		w.WriteHeader(ans.HTTP)
		return
	}
	res, _ := json.Marshal(ans.Result)
	if ans.Result == nil {
		res = []byte("{}")
	}
	fmt.Fprintf(w, `{"retCode":%d,"retMsg":%q,"result":%s,"time":%d}`, ans.RetCode, ans.RetMsg, res, time.Now().UnixMilli())
}

// signed checks the request the way V5 does.
func (f *fakeBybit) signed(r *http.Request, body []byte) bool {
	ts, recv := r.Header.Get("X-BAPI-TIMESTAMP"), r.Header.Get("X-BAPI-RECV-WINDOW")
	payload := r.URL.RawQuery
	if r.Method == http.MethodPost {
		payload = string(body)
	}
	mac := hmac.New(sha256.New, []byte(f.secret))
	mac.Write([]byte(ts + f.key + recv + payload))
	return r.Header.Get("X-BAPI-API-KEY") == f.key && r.Header.Get("X-BAPI-SIGN") == hex.EncodeToString(mac.Sum(nil))
}

func (f *fakeBybit) route(path string, fn func(fakeCall) fakeAnswer) {
	f.mu.Lock()
	f.routes[path] = fn
	f.mu.Unlock()
}

func (f *fakeBybit) callsTo(path string) []fakeCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeCall
	for _, c := range f.calls {
		if c.Path == path {
			out = append(out, c)
		}
	}
	return out
}

func bybitTestConfig(f *fakeBybit) *types.Config {
	return &types.Config{
		Environment: types.EnvTestBybitFutures,
		Credentials: types.Credentials{APIKey: f.key, APISecret: f.secret},
		Live: &types.LiveOptions{
			Assets:           []string{"BTC/USDT"},
			MaxOrderNotional: 10_000,
			MaxLeverage:      5,
			Leverage:         map[string]float64{"BTCUSDT": 3},
		},
	}
}

func preparedBybit(t *testing.T, f *fakeBybit, cfg *types.Config) *BybitFuturesClient {
	t.Helper()
	b := NewBybitFuturesClient(cfg)
	b.restBase = f.srv.URL
	b.wsBase = "ws" + strings.TrimPrefix(f.srv.URL, "http")
	require.NoError(t, b.PrepareSession(context.Background(), cfg))
	return b
}

// ── session ──────────────────────────────────────────────────────────────────

func TestBybitFuturesPrepareSession(t *testing.T) {
	f := newFakeBybit(t)
	b := preparedBybit(t, f, bybitTestConfig(f))

	assert.Equal(t, InstrumentFilter{TickSize: 0.1, StepSize: 0.001, MinQty: 0.001, MinNotional: 5}, b.filters["BTCUSDT"])
	lev := f.callsTo("/v5/position/set-leverage")
	require.Len(t, lev, 1)
	assert.Equal(t, map[string]any{"category": "linear", "symbol": "BTCUSDT", "buyLeverage": "3", "sellLeverage": "3"}, lev[0].Body)
	assert.Equal(t, 100000.0, b.markFor("BTCUSDT"))
}

func TestBybitFuturesRefusesAnAccountItCannotModel(t *testing.T) {
	cases := map[string]struct {
		path   string
		answer fakeAnswer
		want   string
	}{
		"cross margin": {"/v5/account/info", ok(map[string]any{"marginMode": "REGULAR_MARGIN", "unifiedMarginStatus": 6}), "isolated margin only"},
		"portfolio":    {"/v5/account/info", ok(map[string]any{"marginMode": "PORTFOLIO_MARGIN", "unifiedMarginStatus": 6}), "isolated margin only"},
		"classic":      {"/v5/account/info", ok(map[string]any{"marginMode": "ISOLATED_MARGIN", "unifiedMarginStatus": 1}), "classic"},
		"hedge mode": {"/v5/position/list", ok(map[string]any{"list": []any{
			map[string]any{"symbol": "BTCUSDT", "side": "", "size": "0", "positionIdx": 1},
			map[string]any{"symbol": "BTCUSDT", "side": "", "size": "0", "positionIdx": 2},
		}}), "hedge mode"},
		"not trading": {"/v5/market/instruments-info", ok(map[string]any{"list": []any{map[string]any{
			"symbol": "BTCUSDT", "status": "Closed", "contractType": "LinearPerpetual", "settleCoin": "USDT"}}}), "not trading"},
		"inverse": {"/v5/market/instruments-info", ok(map[string]any{"list": []any{map[string]any{
			"symbol": "BTCUSDT", "status": "Trading", "contractType": "InverseFutures", "settleCoin": "BTC"}}}), "USDT perpetuals only"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFakeBybit(t)
			ans := c.answer
			f.route(c.path, func(fakeCall) fakeAnswer { return ans })
			cfg := bybitTestConfig(f)
			b := NewBybitFuturesClient(cfg)
			b.restBase = f.srv.URL
			err := b.PrepareSession(context.Background(), cfg)
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
			assert.Empty(t, f.callsTo("/v5/position/set-leverage"), "nothing is changed on an account that is refused")
		})
	}
}

func TestBybitFuturesRefusesOtherEnvironmentsAndMissingCaps(t *testing.T) {
	f := newFakeBybit(t)
	cfg := bybitTestConfig(f)
	cfg.Environment = types.EnvTestBinanceFutures
	b := NewBybitFuturesClient(cfg)
	b.restBase = f.srv.URL
	assert.ErrorContains(t, b.PrepareSession(context.Background(), cfg), "cannot run environment")

	cfg = bybitTestConfig(f)
	cfg.Live.MaxOrderNotional = 0
	b = NewBybitFuturesClient(cfg)
	b.restBase = f.srv.URL
	var notArmed *ErrNotArmed
	assert.ErrorAs(t, b.PrepareSession(context.Background(), cfg), &notArmed)

	// Mainnet needs Armed and the environment variable as well.
	cfg = bybitTestConfig(f)
	cfg.Environment = types.EnvRealBybitFutures
	b = NewBybitFuturesClient(cfg)
	b.restBase = f.srv.URL
	assert.ErrorAs(t, b.PrepareSession(context.Background(), cfg), &notArmed)
	assert.Empty(t, f.calls, "a refused session never reaches the venue")

	// And the Binance adapter refuses Bybit's environments.
	bcfg := bybitTestConfig(f)
	assert.ErrorContains(t, NewBinanceFuturesClient(bcfg).PrepareSession(context.Background(), bcfg), "cannot run environment")
}

func TestBybitFuturesExplainsAWrongKey(t *testing.T) {
	f := newFakeBybit(t)
	cfg := bybitTestConfig(f)
	cfg.Credentials.APISecret = "not-the-secret"
	b := NewBybitFuturesClient(cfg)
	b.restBase = f.srv.URL
	err := b.PrepareSession(context.Background(), cfg)
	require.ErrorIs(t, err, ErrBybitPermissionDenied)
	assert.Contains(t, err.Error(), "other network")

	cfg.Credentials.APISecret = "-----BEGIN PRIVATE KEY-----\nabc"
	err = NewBybitFuturesClient(cfg).PrepareSession(context.Background(), cfg)
	assert.ErrorContains(t, err, "RSA")
}

// ── orders ───────────────────────────────────────────────────────────────────

func TestBybitFuturesWireBodies(t *testing.T) {
	f := newFakeBybit(t)
	b := preparedBybit(t, f, bybitTestConfig(f))
	ctx := context.Background()

	_, err := b.PlaceOrder(ctx, &types.OrderRequest{Symbol: "BTC/USDT", Exchange: types.ExchangeBybitFutures, Side: types.OrderSideBuy, Type: types.OrderTypeMarket, Quantity: 0.0129})
	require.NoError(t, err)
	_, err = b.PlaceOrder(ctx, &types.OrderRequest{Symbol: "BTC/USDT", Side: types.OrderSideSell, Type: types.OrderTypeStopLoss, Quantity: 0.012, StopPrice: 95000.04, ReduceOnly: true})
	require.NoError(t, err)
	_, err = b.PlaceOrder(ctx, &types.OrderRequest{Symbol: "BTC/USDT", Side: types.OrderSideSell, Type: types.OrderTypeTakeProfitLimit, Quantity: 0.012, Price: 105000.06, ReduceOnly: true})
	require.NoError(t, err)
	_, err = b.PlaceOrder(ctx, &types.OrderRequest{Symbol: "BTC/USDT", Side: types.OrderSideBuy, Type: types.OrderTypeStopLossLimit, Quantity: 0.012, StopPrice: 101000, Price: 101100, ReduceOnly: true})
	require.NoError(t, err)

	creates := f.callsTo("/v5/order/create")
	require.Len(t, creates, 4)
	for _, c := range creates {
		assert.Equal(t, "linear", c.Body["category"])
		assert.Equal(t, "BTCUSDT", c.Body["symbol"])
		assert.Equal(t, 0.0, c.Body["positionIdx"])
		assert.NotEmpty(t, c.Body["orderLinkId"])
		assert.NotContains(t, c.Body, "reduce_only", "the library's misspelling must never reach the wire")
	}

	market := creates[0].Body
	assert.Equal(t, "Buy", market["side"])
	assert.Equal(t, "Market", market["orderType"])
	assert.Equal(t, "0.012", market["qty"], "quantity floors to the step")
	assert.NotContains(t, market, "price")
	assert.NotContains(t, market, "reduceOnly")
	assert.NotContains(t, market, "triggerPrice")

	stop := creates[1].Body
	assert.Equal(t, "Sell", stop["side"])
	assert.Equal(t, "Market", stop["orderType"])
	assert.Equal(t, "95000.0", stop["triggerPrice"])
	assert.Equal(t, 2.0, stop["triggerDirection"], "a sell stop fires as the price falls")
	assert.Equal(t, "MarkPrice", stop["triggerBy"])
	assert.Equal(t, true, stop["reduceOnly"])
	assert.Equal(t, true, stop["closeOnTrigger"])

	tp := creates[2].Body
	assert.Equal(t, "Limit", tp["orderType"])
	assert.Equal(t, "105000.1", tp["price"])
	assert.Equal(t, "GTC", tp["timeInForce"])
	assert.Equal(t, true, tp["reduceOnly"])
	assert.NotContains(t, tp, "triggerPrice")
	assert.NotContains(t, tp, "closeOnTrigger")

	stopLimit := creates[3].Body
	assert.Equal(t, "Limit", stopLimit["orderType"])
	assert.Equal(t, "101100.0", stopLimit["price"])
	assert.Equal(t, "101000.0", stopLimit["triggerPrice"])
	assert.Equal(t, 1.0, stopLimit["triggerDirection"], "a buy stop fires as the price rises")
}

func TestBybitFuturesGatesComeBeforeTheVenue(t *testing.T) {
	f := newFakeBybit(t)
	b := preparedBybit(t, f, bybitTestConfig(f))
	ctx := context.Background()

	// 0.2 BTC at the 100,000 mark is 20,000: over the 10,000 cap.
	_, err := b.PlaceOrder(ctx, &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Type: types.OrderTypeMarket, Quantity: 0.2})
	var notArmed *ErrNotArmed
	require.ErrorAs(t, err, &notArmed)
	assert.Equal(t, "cap", notArmed.Gate)

	// A contract that was not prepared has no rules to check against.
	_, err = b.PlaceOrder(ctx, &types.OrderRequest{Symbol: "ETHUSDT", Side: types.OrderSideBuy, Type: types.OrderTypeMarket, Quantity: 0.01})
	assert.ErrorContains(t, err, "not prepared")

	// Below the contract's minimum notional (0.001 × 100,000 = 100 is fine; 0.0001 rounds to zero).
	_, err = b.PlaceOrder(ctx, &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Type: types.OrderTypeMarket, Quantity: 0.0001})
	assert.ErrorContains(t, err, "rounds to zero")

	assert.Empty(t, f.callsTo("/v5/order/create"), "no refused order reaches the venue")
}

func TestBybitFuturesReadsTheFillBack(t *testing.T) {
	f := newFakeBybit(t)
	f.route("/v5/order/realtime", func(c fakeCall) fakeAnswer {
		return ok(map[string]any{"list": []any{map[string]any{
			"orderId": c.Query.Get("orderId"), "symbol": "BTCUSDT", "side": "Buy", "orderType": "Market",
			"orderStatus": "Filled", "qty": "0.012", "cumExecQty": "0.012", "avgPrice": "100010.5",
			"cumFeeDetail": map[string]string{"USDT": "0.66"}, "createdTime": "1800000000000", "updatedTime": "1800000000100",
		}}})
	})
	b := preparedBybit(t, f, bybitTestConfig(f))
	o, err := b.PlaceOrder(context.Background(), &types.OrderRequest{Symbol: "BTC/USDT", Side: types.OrderSideBuy, Type: types.OrderTypeMarket, Quantity: 0.012})
	require.NoError(t, err)
	assert.Equal(t, types.OrderStatusFilled, o.Status)
	assert.Equal(t, 100010.5, o.AveragePrice)
	assert.Equal(t, 0.012, o.FilledQty)
	assert.InDelta(t, 0.66, o.CumulativeFee, 1e-12)
	assert.Equal(t, types.ExchangeBybitFutures, o.Exchange)
	assert.True(t, strings.HasPrefix(o.ID, "o-kd"))
}

func TestBybitFuturesAnUnsurePlacementIsLookedUp(t *testing.T) {
	f := newFakeBybit(t)
	f.route("/v5/order/create", func(fakeCall) fakeAnswer { return fakeAnswer{HTTP: http.StatusBadGateway} })
	var linkSeen string
	f.route("/v5/order/realtime", func(c fakeCall) fakeAnswer {
		if id := c.Query.Get("orderLinkId"); id != "" {
			linkSeen = id
			return ok(map[string]any{"list": []any{map[string]any{"orderId": "venue-1", "orderLinkId": id, "symbol": "BTCUSDT", "side": "Sell", "orderType": "Market", "orderStatus": "Untriggered", "qty": "0.01", "triggerPrice": "95000"}}})
		}
		return ok(map[string]any{"list": []any{}})
	})
	b := preparedBybit(t, f, bybitTestConfig(f))
	o, err := b.PlaceOrder(context.Background(), &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideSell, Type: types.OrderTypeStopLoss, Quantity: 0.01, StopPrice: 95000, ReduceOnly: true})
	require.NoError(t, err, "the order exists: reporting a failure would invite a second one")
	assert.Equal(t, "venue-1", o.ID)
	assert.NotEmpty(t, linkSeen)

	// Not found by its link id either: not placed, and said so.
	f.route("/v5/order/realtime", func(fakeCall) fakeAnswer { return ok(map[string]any{"list": []any{}}) })
	_, err = b.PlaceOrder(context.Background(), &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Type: types.OrderTypeMarket, Quantity: 0.01})
	assert.ErrorContains(t, err, "not placed")

	// The check fails too: the order MAY exist, and the error says so.
	f.route("/v5/order/realtime", func(fakeCall) fakeAnswer { return fakeAnswer{HTTP: http.StatusBadGateway} })
	_, err = b.PlaceOrder(context.Background(), &types.OrderRequest{Symbol: "BTCUSDT", Side: types.OrderSideBuy, Type: types.OrderTypeMarket, Quantity: 0.01})
	assert.ErrorContains(t, err, "MAY exist")
}

func TestBybitFuturesBracketWithdrawsTheTakeProfitWhenTheStopFails(t *testing.T) {
	f := newFakeBybit(t)
	f.route("/v5/order/create", func(c fakeCall) fakeAnswer {
		if _, stop := c.Body["triggerPrice"]; stop {
			return fakeAnswer{RetCode: 110092, RetMsg: "expect Falling, but trigger_price[95000] >= current[90000]"}
		}
		return ok(map[string]string{"orderId": "tp-1", "orderLinkId": c.Body["orderLinkId"].(string)})
	})
	b := preparedBybit(t, f, bybitTestConfig(f))
	_, err := b.PlaceBracket(context.Background(), &types.BracketRequest{Symbol: "BTCUSDT", Side: types.OrderSideSell, Quantity: 0.01, TakeProfitPrice: 105000, StopPrice: 95000})
	require.Error(t, err)
	cancels := f.callsTo("/v5/order/cancel")
	require.Len(t, cancels, 1)
	assert.Equal(t, "tp-1", cancels[0].Body["orderId"])
}

func TestBybitFuturesCancelOfAClosedOrder(t *testing.T) {
	f := newFakeBybit(t)
	f.route("/v5/order/cancel", func(fakeCall) fakeAnswer {
		return fakeAnswer{RetCode: bybitCodeOrderNotExist, RetMsg: "Order does not exist."}
	})
	b := preparedBybit(t, f, bybitTestConfig(f))
	err := b.CancelOrder(context.Background(), types.ExchangeBybitFutures, "BTCUSDT", "gone")
	assert.ErrorIs(t, err, types.ErrOrderClosed)
}

func TestBybitFuturesPositionsAndAccount(t *testing.T) {
	f := newFakeBybit(t)
	b := preparedBybit(t, f, bybitTestConfig(f))
	f.route("/v5/position/list", func(c fakeCall) fakeAnswer {
		assert.Equal(t, "USDT", c.Query.Get("settleCoin"))
		return ok(map[string]any{"list": []any{
			map[string]any{"symbol": "BTCUSDT", "side": "Buy", "size": "0.012", "avgPrice": "100000", "leverage": "3", "positionIM": "400.5", "markPrice": "101000", "unrealisedPnl": "12", "positionIdx": 0},
			map[string]any{"symbol": "ETHUSDT", "side": "", "size": "0", "positionIdx": 0},
			map[string]any{"symbol": "SOLUSDT", "side": "Sell", "size": "3", "avgPrice": "150", "leverage": "2", "positionIdx": 0},
		}})
	})
	ps, err := b.GetPositions(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, ps, 2, "a flat row is no position")
	assert.Equal(t, types.Position{Symbol: "BTCUSDT", Exchange: types.ExchangeBybitFutures, Side: "LONG", Size: 0.012, EntryPrice: 100000, Leverage: 3, IsolatedMargin: 400.5, MarkPrice: 101000, UnrealizedPnL: 12}, *ps[0])
	assert.Equal(t, "SHORT", ps[1].Side)

	other, err := b.GetPositions(context.Background(), types.ExchangeBinanceFutures)
	require.NoError(t, err)
	assert.Empty(t, other)

	f.route("/v5/account/wallet-balance", func(c fakeCall) fakeAnswer {
		assert.Equal(t, "UNIFIED", c.Query.Get("accountType"))
		return ok(map[string]any{"list": []any{map[string]any{"coin": []any{map[string]string{
			"coin": "USDT", "walletBalance": "1000", "totalPositionIM": "400", "totalOrderIM": "50", "locked": "0", "bonus": "0",
		}}}}})
	})
	acc, err := b.GetAccount(context.Background(), types.ExchangeBybitFutures, "")
	require.NoError(t, err)
	require.Len(t, acc.Balances, 1)
	assert.Equal(t, types.Balance{Asset: "USDT", Free: 550, Lock: 450}, acc.Balances[0])
}

func TestBybitFuturesLeverageChangeRefusedWithAPosition(t *testing.T) {
	f := newFakeBybit(t)
	b := preparedBybit(t, f, bybitTestConfig(f))
	f.route("/v5/position/list", func(fakeCall) fakeAnswer {
		return ok(map[string]any{"list": []any{map[string]any{"symbol": "BTCUSDT", "side": "Buy", "size": "0.01", "positionIdx": 0}}})
	})
	before := len(f.callsTo("/v5/position/set-leverage"))
	assert.ErrorContains(t, b.SetLeverage(context.Background(), types.ExchangeBybitFutures, "BTCUSDT", 2), "while a LONG position")
	var notArmed *ErrNotArmed
	assert.ErrorAs(t, b.SetLeverage(context.Background(), types.ExchangeBybitFutures, "BTCUSDT", 10), &notArmed)
	assert.Len(t, f.callsTo("/v5/position/set-leverage"), before)
}

func TestBybitFuturesFilledLegCancelsItsSibling(t *testing.T) {
	f := newFakeBybit(t)
	b := preparedBybit(t, f, bybitTestConfig(f))
	b.brackets["tp-1"] = bracketLeg{sibling: "sl-1", groupID: "bracket-7"}
	b.brackets["sl-1"] = bracketLeg{sibling: "tp-1", groupID: "bracket-7"}
	b.declared["tp-1"] = types.OrderTypeTakeProfitLimit

	orders := make(chan *types.Order, 4)
	original := map[string]string{"BTCUSDT": "BTC/USDT"}
	rec := bybitOrderRecord{OrderID: "tp-1", Symbol: "BTCUSDT", Side: "Sell", OrderType: "Limit", OrderStatus: "PartiallyFilled",
		Qty: "0.01", CumExecQty: "0.004", AvgPrice: "105000", CumFeeDetail: map[string]any{"USDT": "0.1"}, ReduceOnly: true}
	b.handleOrderUpdate(context.Background(), rec, original, orders)
	rec.OrderStatus, rec.CumExecQty, rec.CumFeeDetail, rec.ClosedPnl = "Filled", "0.01", map[string]any{"USDT": "0.25"}, "50"
	b.handleOrderUpdate(context.Background(), rec, original, orders)

	first, second := <-orders, <-orders
	assert.Equal(t, types.OrderTypeTakeProfitLimit, first.Type, "reported under the name it was placed with")
	assert.Equal(t, "BTC/USDT", first.Symbol)
	assert.Equal(t, "bracket-7", first.GroupID)
	assert.InDelta(t, 0.1, first.Fee, 1e-12)
	assert.InDelta(t, 0.15, second.Fee, 1e-12, "each update carries only its new fees")
	assert.InDelta(t, 0.25, second.CumulativeFee, 1e-12)
	assert.Equal(t, 50.0, second.RealizedPnL)
	assert.Equal(t, types.OrderStatusFilled, second.Status)

	require.Eventually(t, func() bool { return len(f.callsTo("/v5/order/cancel")) == 1 }, 2*time.Second, 10*time.Millisecond)
	assert.Equal(t, "sl-1", f.callsTo("/v5/order/cancel")[0].Body["orderId"])
	require.Eventually(t, func() bool {
		b.mu.RLock()
		defer b.mu.RUnlock()
		return len(b.brackets) == 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestBybitFuturesKlinePagesBackwards(t *testing.T) {
	f := newFakeBybit(t)
	b := preparedBybit(t, f, bybitTestConfig(f))
	base := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	row := func(i int) []string {
		ms := base.Add(time.Duration(i) * time.Minute).UnixMilli()
		return []string{strconv.FormatInt(ms, 10), "1", "2", "0.5", "1.5", "10", "15"}
	}
	f.route("/v5/market/kline", func(c fakeCall) fakeAnswer {
		end, _ := strconv.ParseInt(c.Query.Get("end"), 10, 64)
		var list [][]string
		// Newest first, at most 1000 per page, from the end backwards.
		for i := 2499; i >= 0 && len(list) < 1000; i-- {
			if base.Add(time.Duration(i)*time.Minute).UnixMilli() <= end {
				list = append(list, row(i))
			}
		}
		return ok(map[string]any{"list": list})
	})
	cs, err := b.GetHistoricalCandles(context.Background(), types.ExchangeBybitFutures, "BTC/USDT", base, base.Add(2499*time.Minute), types.Timeframe1m)
	require.NoError(t, err)
	require.Len(t, cs, 2500)
	assert.True(t, base.Equal(cs[0].OpenTime), "first %s", cs[0].OpenTime)
	assert.True(t, base.Add(2499*time.Minute).Equal(cs[2499].OpenTime), "last %s", cs[2499].OpenTime)
	for i := 1; i < len(cs); i++ {
		require.True(t, cs[i].OpenTime.After(cs[i-1].OpenTime))
	}
	assert.Len(t, f.callsTo("/v5/market/kline"), 3)
}

// ── streams ──────────────────────────────────────────────────────────────────

func TestBybitFuturesStreams(t *testing.T) {
	f := newFakeBybit(t)
	b := preparedBybit(t, f, bybitTestConfig(f))
	f.route("/v5/order/realtime", func(fakeCall) fakeAnswer { return ok(map[string]any{"list": []any{}}) })

	f.mu.Lock()
	f.onPublic = func(conn *websocket.Conn) {
		var sub map[string]any
		require.NoError(t, conn.ReadJSON(&sub))
		assert.Equal(t, "subscribe", sub["op"])
		assert.ElementsMatch(t, []any{"kline.1.BTCUSDT", "tickers.BTCUSDT"}, sub["args"])
		conn.WriteJSON(map[string]any{"op": "subscribe", "success": true})
		conn.WriteJSON(map[string]any{"topic": "kline.1.BTCUSDT", "data": []any{map[string]any{"start": 1, "end": 60000, "open": "1", "close": "2", "high": "3", "low": "0.5", "volume": "9", "confirm": false}}})
		conn.WriteJSON(map[string]any{"topic": "kline.1.BTCUSDT", "data": []any{map[string]any{"start": 1, "end": 60000, "open": "1", "close": "2", "high": "3", "low": "0.5", "volume": "9", "turnover": "18", "confirm": true}}})
		conn.WriteJSON(map[string]any{"topic": "tickers.BTCUSDT", "type": "snapshot", "data": map[string]any{"symbol": "BTCUSDT", "markPrice": "101234.5"}})
		conn.WriteJSON(map[string]any{"topic": "tickers.BTCUSDT", "type": "delta", "data": map[string]any{"symbol": "BTCUSDT", "lastPrice": "101300"}})
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}
	f.onPriv = func(conn *websocket.Conn, n int) {
		var auth map[string]any
		require.NoError(t, conn.ReadJSON(&auth))
		args := auth["args"].([]any)
		expires := strconv.FormatFloat(args[1].(float64), 'f', 0, 64)
		mac := hmac.New(sha256.New, []byte(f.secret))
		mac.Write([]byte("GET/realtime" + expires))
		good := auth["op"] == "auth" && args[0] == f.key && args[2] == hex.EncodeToString(mac.Sum(nil))
		conn.WriteJSON(map[string]any{"op": "auth", "success": good, "ret_msg": ""})
		if !good {
			return
		}
		var sub map[string]any
		require.NoError(t, conn.ReadJSON(&sub))
		assert.Equal(t, []any{"order"}, sub["args"])
		conn.WriteJSON(map[string]any{"op": "subscribe", "success": true})
		conn.WriteJSON(map[string]any{"topic": "order", "data": []any{map[string]any{
			"orderId": fmt.Sprintf("ws-%d", n), "symbol": "BTCUSDT", "side": "Buy", "orderType": "Market", "orderStatus": "Filled",
			"qty": "0.01", "cumExecQty": "0.01", "avgPrice": "100000", "createType": "CreateByUser"}}})
		// The first private connection drops: the adapter must come back.
		if n <= 3 {
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}
	f.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	candles := make(chan *types.Candle, 8)
	orders := make(chan *types.Order, 8)
	require.NoError(t, b.ConnectStream(ctx, candles, orders))

	select {
	case c := <-candles:
		assert.Equal(t, "BTC/USDT", c.Symbol)
		assert.Equal(t, 2.0, c.Close)
		assert.Equal(t, 18.0, c.QuoteVolume)
		assert.True(t, c.IsComplete)
	case <-time.After(5 * time.Second):
		t.Fatal("no closed candle")
	}
	select {
	case c := <-candles:
		t.Fatalf("a forming candle was delivered: %+v", c)
	case <-time.After(200 * time.Millisecond):
	}
	require.Eventually(t, func() bool { return b.markFor("BTCUSDT") == 101234.5 }, 2*time.Second, 10*time.Millisecond, "a delta without markPrice keeps the last one")

	got := map[string]bool{}
	deadline := time.After(10 * time.Second)
	for len(got) < 2 {
		select {
		case o := <-orders:
			got[o.ID] = true
			assert.Equal(t, types.OrderStatusFilled, o.Status)
		case <-deadline:
			t.Fatalf("the private stream did not come back after a drop; orders seen: %v", got)
		}
	}
	assert.NotEmpty(t, f.callsTo("/v5/order/realtime"), "a reconnect reconciles over REST")
}

// ── isolation ────────────────────────────────────────────────────────────────

// bybitFuturesFiles are every file the Bybit perpetuals adapter is made of.
var bybitFuturesFiles = []string{"bybit_futures.go", "bybit_futures_stream.go", "bybit_futures_rules.go", "bybit_v5.go"}

// TestBybitFuturesAdapterUsesItsOwnClient keeps the library whose order
// parameters lose reduceOnly away from the perpetuals path, and keeps that path
// away from the spot category (on a Unified Trading Account the USDT is shared).
func TestBybitFuturesAdapterUsesItsOwnClient(t *testing.T) {
	for _, name := range bybitFuturesFiles {
		raw, err := os.ReadFile(name)
		require.NoError(t, err, "%s must exist for this check to mean anything", name)
		src := string(raw)
		for _, forbidden := range []string{`"github.com/hirokisan/bybit`, `"spot"`, "CategoryV5Spot", `"reduce_only":`} {
			assert.NotContains(t, src, forbidden, "%s must not contain %s", name, forbidden)
		}
	}
	core, err := os.ReadFile("bybit_futures.go")
	require.NoError(t, err)
	assert.Contains(t, string(core), `"category": "linear"`)
}
