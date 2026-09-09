package dev_sdk

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// The reconciler exists so that "did my order fill?" has one answer across
// every backend.
//
// Some venues push order updates; some do not. Rather than exposing that
// difference to strategies — which is how Binance spot ended up with a
// SetOnOrderUpdate that never fired — the SDK keeps a belief about each
// order's state and emits transitions. Three things update that belief, and a
// strategy cannot tell which one did:
//
//	push event    -> update -> emit diff   (milliseconds)
//	poll tick     -> update -> emit diff   (seconds)
//	reconnect gap -> resync -> emit diff   (once)
//
// Only the latency differs, and that is declared in types.OrderFeed rather
// than hidden.

// orderBelief is the last state the strategy was told about an order. A
// transition is emitted only when the venue's answer differs from this, so a
// poll that finds nothing new stays silent and a partial fill that has not
// grown does not re-fire.
type orderBelief struct {
	status    types.OrderStatus
	filledQty float64
}

// orderReconciler tracks beliefs and decides what is worth emitting.
//
// It is safe for concurrent use because push and poll can both be live at once
// — during a reconnect especially — and the same fill must not be delivered
// twice.
type orderReconciler struct {
	mu      sync.Mutex
	beliefs map[string]orderBelief
}

func newOrderReconciler() *orderReconciler {
	return &orderReconciler{beliefs: make(map[string]orderBelief)}
}

// observe records what the venue says about an order and reports whether that
// is news. It is the single dedup point for every source of order state.
func (r *orderReconciler) observe(o *types.Order) bool {
	if o == nil || o.ID == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	prev, seen := r.beliefs[o.ID]
	next := orderBelief{status: o.Status, filledQty: o.FilledQty}
	if seen && prev == next {
		return false
	}
	r.beliefs[o.ID] = next

	// A terminal order will never change again, so its belief is dropped to
	// keep the map from growing for the life of the process. Re-observing it
	// later reports news once more, which is the right trade: a duplicate
	// terminal event is harmless, an unbounded map on a long-running strategy
	// is not.
	if isTerminal(o.Status) {
		delete(r.beliefs, o.ID)
	}
	return true
}

func isTerminal(s types.OrderStatus) bool {
	return s == types.OrderStatusFilled ||
		s == types.OrderStatusCanceled ||
		s == types.OrderStatusRejected
}

// track seeds a belief from an order the strategy just placed, so the first
// poll does not report the acknowledgement it already returned as news.
func (r *orderReconciler) track(o *types.Order) {
	if o == nil || o.ID == "" || isTerminal(o.Status) {
		return
	}
	r.mu.Lock()
	r.beliefs[o.ID] = orderBelief{status: o.Status, filledQty: o.FilledQty}
	r.mu.Unlock()
}

// openIDs returns the orders currently believed to be live.
func (r *orderReconciler) openIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.beliefs))
	for id := range r.beliefs {
		out = append(out, id)
	}
	return out
}

// pollOrderState reconstructs an order feed for an adapter that cannot push.
//
// It runs only when types.OrderFeed says so, and it emits into the same
// orderChan the push path uses, so Pipeline C and SetOnOrderUpdate are
// unchanged.
func (s *SDK) pollOrderState(ctx context.Context, reader OrderStateReader) {
	interval := s.orderFeed.PollEvery
	if interval <= 0 {
		return
	}
	symbols, exchanges := s.liveSymbols()
	if len(symbols) == 0 || len(exchanges) == 0 {
		log.Println("DevSDK: order polling has no configured symbols; not started")
		return
	}
	log.Printf("DevSDK: polling order state every %s (%v)", interval, symbols)

	// The interval adapts. A venue that answers happily is polled at the
	// configured cadence; one that is rate-limiting is backed away from, hard.
	delay := interval
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		switch err := s.pollOnce(ctx, reader, exchanges, symbols); {
		case err == nil:
			delay = interval
		case isRateLimited(err):
			// Binance answers a rate-limit breach with an IP ban and says, in
			// the error itself, to use the websocket instead. Retrying on the
			// same cadence — which this loop used to do, forever — extends the
			// ban and fills the log with nothing useful. Measured: 30
			// consecutive bans in one 32-minute session.
			delay = nextPollBackoff(delay)
			log.Printf("DevSDK: order polling rate-limited, backing off to %s: %v", delay, err)
		default:
			delay = nextPollBackoff(delay)
			log.Printf("DevSDK: order polling failed, retrying in %s: %v", delay, err)
		}
	}
}

// maxPollBackoff caps the retreat. Beyond a few minutes the poll has stopped
// being a safety net and the push stream is the only feed, which is what the
// venue is asking for anyway.
const maxPollBackoff = 5 * time.Minute

func nextPollBackoff(d time.Duration) time.Duration {
	d *= 4
	if d > maxPollBackoff {
		return maxPollBackoff
	}
	return d
}

// isRateLimited recognises a venue telling us to stop asking. Matched on the
// message rather than a typed code so it holds across venues, since being
// banned costs the same whoever issued it.
func isRateLimited(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "too many requests") ||
		strings.Contains(msg, "rate limit") ||
		strings.Contains(msg, "-1003") ||
		strings.Contains(msg, "banned until") ||
		strings.Contains(msg, "429")
}

func (s *SDK) pollOnce(ctx context.Context, reader OrderStateReader, exchanges, symbols []string) error {
	// When the venue pushes and the strategy has nothing outstanding, there is
	// nothing for a poll to discover: any new order will announce itself on the
	// stream. Asking anyway is a request per interval per symbol spent on
	// confirming that nothing is happening, and it is what earns a rate-limit
	// ban on a quiet session.
	//
	// A venue that cannot push is different — there the poll *is* the feed and
	// must run regardless.
	if s.orderFeed.Push && len(s.reconciler.openIDs()) == 0 {
		return nil
	}

	// Everything currently on the book. Anything the strategy believed was
	// live and is missing here has reached a terminal state — but the open
	// list cannot say which one.
	stillOpen := make(map[string]struct{})

	for _, exch := range exchanges {
		for _, sym := range symbols {
			open, err := reader.ListOpenOrders(ctx, exch, sym)
			if err != nil {
				return fmt.Errorf("%s %s: %w", exch, sym, err)
			}
			for _, o := range open {
				if o == nil {
					continue
				}
				stillOpen[o.ID] = struct{}{}
				if s.reconciler.observe(o) {
					s.emitOrder(ctx, o)
				}
			}
		}
	}

	// Resolve the disappearances. An order that has left the open list is
	// FILLED or CANCELED, and guessing "gone means filled" would invent
	// positions the account does not hold — so each one is asked about
	// directly.
	for _, id := range s.reconciler.openIDs() {
		if _, ok := stillOpen[id]; ok {
			continue
		}
		resolved := false
		for _, exch := range exchanges {
			for _, sym := range symbols {
				o, err := reader.GetOrder(ctx, exch, sym, id)
				if err != nil || o == nil {
					continue
				}
				if s.reconciler.observe(o) {
					s.emitOrder(ctx, o)
				}
				resolved = true
				break
			}
			if resolved {
				break
			}
		}
		if !resolved {
			// Better to say nothing than to emit a fabricated terminal state.
			log.Printf("DevSDK: order %s left the open book but its final state could not be read; not guessing", id)
		}
	}
	return nil
}

// emitOrder hands a transition to the same channel the push path feeds.
func (s *SDK) emitOrder(ctx context.Context, o *types.Order) {
	select {
	case s.orderChan <- o:
	case <-ctx.Done():
	}
}

// liveSymbols returns the configured symbols and exchanges for a live session.
func (s *SDK) liveSymbols() (symbols, exchanges []string) {
	if s.config == nil || s.config.Live == nil {
		return nil, nil
	}
	return s.config.Live.Assets, s.config.Live.RequestedExchanges
}

// OrderFeed reports how order updates reach this session, so a strategy can
// adapt rather than assume. It is valid after Start.
//
// A strategy that must react within milliseconds can check Latency and refuse
// to run on a polled feed, instead of silently behaving differently from the
// backtest that justified it.
func (s *SDK) OrderFeed() types.OrderFeed { return s.orderFeed }
