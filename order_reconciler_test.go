package dev_sdk

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/exchange/backtest"
	"github.com/kdraigo/dev_sdk/exchange/live"
	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ord(id string, status types.OrderStatus, filled float64) *types.Order {
	return &types.Order{ID: id, Symbol: "BTCUSDT", Status: status, FilledQty: filled}
}

// TestReconciler_SameTransitionDeliveredOnce is the reason the reconciler
// exists. During a reconnect the push stream and the poller are both live, and
// without a single dedup point the strategy is told about one fill twice —
// which, for a strategy sizing the next order off the last fill, doubles it.
func TestReconciler_SameTransitionDeliveredOnce(t *testing.T) {
	r := newOrderReconciler()

	assert.True(t, r.observe(ord("1", types.OrderStatusNew, 0)), "first sighting is news")
	assert.False(t, r.observe(ord("1", types.OrderStatusNew, 0)), "the same state is not")
	assert.False(t, r.observe(ord("1", types.OrderStatusNew, 0)), "however often it is repeated")
}

// TestReconciler_PartialFillsProgress: a partial fill that has grown is news;
// one that has not is the poller seeing the same thing again.
func TestReconciler_PartialFillsProgress(t *testing.T) {
	r := newOrderReconciler()
	require.True(t, r.observe(ord("1", types.OrderStatusPartiallyFilled, 0.3)))
	assert.False(t, r.observe(ord("1", types.OrderStatusPartiallyFilled, 0.3)), "same fill quantity")
	assert.True(t, r.observe(ord("1", types.OrderStatusPartiallyFilled, 0.7)), "the fill grew")
	assert.True(t, r.observe(ord("1", types.OrderStatusFilled, 1.0)), "and then completed")
}

// TestReconciler_TerminalOrdersAreForgotten keeps a long-running strategy from
// growing the belief map for the life of the process.
func TestReconciler_TerminalOrdersAreForgotten(t *testing.T) {
	r := newOrderReconciler()
	r.observe(ord("1", types.OrderStatusNew, 0))
	assert.Len(t, r.openIDs(), 1)

	r.observe(ord("1", types.OrderStatusFilled, 1))
	assert.Empty(t, r.openIDs(), "a filled order can never change again")

	r.observe(ord("2", types.OrderStatusNew, 0))
	r.observe(ord("2", types.OrderStatusCanceled, 0))
	assert.Empty(t, r.openIDs())
}

// TestReconciler_TrackSuppressesTheEcho: PlaceOrder already returned the
// acknowledgement to the caller, so the first poll must not hand it back as a
// new event.
func TestReconciler_TrackSuppressesTheEcho(t *testing.T) {
	r := newOrderReconciler()
	placed := ord("42", types.OrderStatusNew, 0)
	r.track(placed)

	assert.False(t, r.observe(ord("42", types.OrderStatusNew, 0)), "the echo is not news")
	assert.True(t, r.observe(ord("42", types.OrderStatusFilled, 1)), "but the fill is")
}

// TestReconciler_IgnoresUnidentifiableOrders: an order with no id cannot be
// tracked, and emitting it would produce an event nothing can be correlated to.
func TestReconciler_IgnoresUnidentifiableOrders(t *testing.T) {
	r := newOrderReconciler()
	assert.False(t, r.observe(nil))
	assert.False(t, r.observe(&types.Order{ID: ""}))
}

// TestOrderFeed_EveryAdapterDeclaresOne is the check that would have caught the
// original defect: Binance spot handed the SDK an orderChan and never wrote to
// it, so SetOnOrderUpdate was dead there and alive everywhere else.
func TestOrderFeed_EveryAdapterDeclaresOne(t *testing.T) {
	cfg := &types.Config{}
	adapters := map[string]Adapter{
		"backtest engine": backtest.NewEngineClient(cfg),
		"binance futures": live.NewBinanceFuturesClient(cfg),
		"binance spot":    live.NewBinanceClient(cfg),
		"bybit spot":      live.NewBybitClient(cfg),
	}

	for name, a := range adapters {
		t.Run(name, func(t *testing.T) {
			_, declares := a.(OrderFeedDescriber)
			assert.True(t, declares,
				"%s must declare an OrderFeed: an adapter that says nothing about order delivery is how a dead SetOnOrderUpdate goes unnoticed", name)

			feed := resolveOrderFeed(a)
			assert.True(t, feed.Delivers(),
				"%s delivers no order updates at all — SetOnOrderUpdate would never fire", name)
		})
	}
}

// TestOrderFeed_SpotIsPolledFuturesIsPushed pins the actual difference, so a
// regression that quietly drops the Binance spot poller fails here.
func TestOrderFeed_SpotIsPolledFuturesIsPushed(t *testing.T) {
	spot := resolveOrderFeed(live.NewBinanceClient(&types.Config{}))
	assert.False(t, spot.Push, "binance spot has no user data stream in this adapter")
	assert.Positive(t, spot.PollEvery, "so it must be polled")
	assert.Positive(t, spot.Latency, "and must admit to the latency that costs")

	fut := resolveOrderFeed(live.NewBinanceFuturesClient(&types.Config{}))
	assert.True(t, fut.Push)
}

// TestOrderFeed_PollingFallbackForUndeclaredReaders: an adapter that never
// declared a feed but can be asked still gets one, rather than silently
// delivering nothing.
func TestOrderFeed_PollingFallbackForUndeclaredReaders(t *testing.T) {
	feed := resolveOrderFeed(readerOnlyAdapter{})
	assert.False(t, feed.Push)
	assert.Equal(t, defaultOrderPollInterval, feed.PollEvery)
}

// TestOrderFeed_SilentAdapterStaysSilent: an adapter that neither pushes nor
// can be read must report exactly that. Defaulting it to "fine" would restore
// the bug this whole mechanism removes.
func TestOrderFeed_SilentAdapterStaysSilent(t *testing.T) {
	feed := resolveOrderFeed(spotOnlyAdapter{})
	assert.False(t, feed.Delivers())
	assert.Contains(t, feed.Describe(), "NONE")
}

func TestOrderFeed_Describe(t *testing.T) {
	assert.Contains(t, types.OrderFeed{Push: true, Latency: time.Second}.Describe(), "pushed")
	assert.Contains(t, types.OrderFeed{PollEvery: 3 * time.Second, Latency: 3 * time.Second}.Describe(), "polled every 3s")
	assert.Contains(t, types.OrderFeed{}.Describe(), "NONE")
}

// readerOnlyAdapter can answer questions but never declared a feed.
type readerOnlyAdapter struct{ spotOnlyAdapter }

func (readerOnlyAdapter) ListOpenOrders(ctx context.Context, exchange, symbol string) ([]*types.Order, error) {
	return nil, nil
}
func (readerOnlyAdapter) GetOrder(ctx context.Context, exchange, symbol, id string) (*types.Order, error) {
	return nil, nil
}

// TestOrderFeed_PushedAdaptersAlsoGetASafetyNet
//
// A dropped websocket takes its undelivered events with it: an order that
// filled during the gap is never mentioned again, and the push path cannot
// detect its own silence. So the adapters that push must also be askable.
func TestOrderFeed_PushedAdaptersAlsoGetASafetyNet(t *testing.T) {
	fut := live.NewBinanceFuturesClient(&types.Config{})

	feed := resolveOrderFeed(fut)
	require.True(t, feed.Push, "the stream is still the primary feed")
	assert.Positive(t, feed.PollEvery, "and a slow poll must back it up")
	assert.Greater(t, feed.PollEvery, feed.Latency,
		"the safety net must be slower than the push path, or it becomes the feed")

	_, askable := interface{}(fut).(OrderStateReader)
	assert.True(t, askable, "and the adapter must be answerable for the poll to work")

	assert.Contains(t, feed.Describe(), "reconciled every")
}

// TestIsRateLimited recognises a venue telling us to stop asking.
func TestIsRateLimited(t *testing.T) {
	for _, msg := range []string{
		"<APIError> code=-1003, msg=Way too many requests; IP(1.2.3.4) banned until 1788959337018",
		"rate limit exceeded",
		"HTTP 429 Too Many Requests",
	} {
		assert.True(t, isRateLimited(errors.New(msg)), msg)
	}
	assert.False(t, isRateLimited(errors.New("connection refused")))
	assert.False(t, isRateLimited(nil))
}

// TestPollBackoff_RetreatsAndCaps: the loop used to retry on the same cadence
// forever, which extends an IP ban rather than waiting it out.
func TestPollBackoff_RetreatsAndCaps(t *testing.T) {
	d := 2 * time.Second
	seen := []time.Duration{}
	for i := 0; i < 8; i++ {
		d = nextPollBackoff(d)
		seen = append(seen, d)
	}
	assert.Greater(t, seen[1], seen[0], "backoff must actually retreat")
	assert.Equal(t, maxPollBackoff, seen[len(seen)-1], "and settle at the cap")
	for _, d := range seen {
		assert.LessOrEqual(t, d, maxPollBackoff)
	}
}

// TestFuturesPollIsSlowerThanTheBanThreshold pins the lesson from the run that
// got the IP banned: a pushed feed's safety net must be slow.
func TestFuturesPollIsSlowerThanTheBanThreshold(t *testing.T) {
	feed := resolveOrderFeed(live.NewBinanceFuturesClient(&types.Config{}))
	assert.GreaterOrEqual(t, feed.PollEvery, time.Minute,
		"a 30s safety-net poll earned 30 consecutive -1003 IP bans in one session")
}
