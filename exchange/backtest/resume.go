package backtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// Machine-readable reasons the engine gives for refusing an attach. They decide
// the one question that matters: retry, or stop.
const (
	codeSessionUnknown     = "session_unknown"
	codeSessionKeyMismatch = "session_key_mismatch"
	codeSessionTerminal    = "session_terminal"
	codeSessionClosing     = "session_closing"
	codeUnauthorized       = "unauthorized"
)

// AttachError is a refusal from the engine when opening or reopening a session.
type AttachError struct {
	Code    string
	Status  int
	Message string
}

func (e *AttachError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("backtest engine refused session attach (%s): %s", e.Code, e.Message)
	}
	return fmt.Sprintf("backtest engine refused session attach: %s", e.Message)
}

// retryableAttach reports whether a failed attach is worth redialing.
//
// Only session_closing is: the engine is finalizing a session and will shortly
// be definite about it. Everything else is permanent — and session_unknown in
// particular must never be treated as "so start a fresh session". After a
// successful attach it means candles were lost; on a cold resume it means the
// run the caller asked to continue is gone. Silently starting over would hand
// back results the caller reads as a continuation of something else.
//
// A transport-level failure with no engine answer at all (the engine is
// restarting, the proxy blipped) is retryable: nothing has told us the session
// is gone.
func retryableAttach(err error) bool {
	var aerr *AttachError
	if errors.As(err, &aerr) {
		return aerr.Code == codeSessionClosing
	}
	return true
}

// classifyDialError turns a failed WebSocket upgrade into something the
// supervisor can act on, reading the engine's reason out of the refusal body.
func classifyDialError(dialErr error, resp *http.Response) error {
	if resp == nil {
		return fmt.Errorf("websocket dial failed: %w", dialErr)
	}
	defer func() { _ = resp.Body.Close() }()

	var body struct {
		Error string `json:"error"`
		Code  string `json:"code"`
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	_ = json.Unmarshal(raw, &body)

	if body.Code == "" && resp.StatusCode < 500 {
		// A refusal we cannot read is still a refusal, but an unreadable one
		// should not be mistaken for a permanent verdict.
		return fmt.Errorf("websocket dial failed (HTTP %d): %w", resp.StatusCode, dialErr)
	}
	if body.Code == "" {
		return fmt.Errorf("websocket dial failed (HTTP %d): %w", resp.StatusCode, dialErr)
	}

	msg := body.Error
	if msg == "" {
		msg = dialErr.Error()
	}
	return &AttachError{Code: body.Code, Status: resp.StatusCode, Message: msg}
}

// decodeSessionState decodes the engine's opening frame.
//
// It is handled inside the read loop rather than probed for at dial time. The
// probe seemed natural and was wrong twice over: an engine predating the frame
// sends nothing on attach — this client has not asked for anything yet — so
// the read would block; and a gorilla read deadline that fires leaves the
// connection unusable, so "wait briefly, then give up" poisons the socket it
// was trying to protect.
//
// The engine sends this frame before anything else, so handling it in the loop
// still guarantees it is seen before the first candle — which is the property
// that matters, since a resumed strategy has to warm up before a bar reaches
// it. An engine that sends no such frame simply never triggers this path.
func decodeSessionState(data json.RawMessage) (*types.SessionState, error) {
	var state types.SessionState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decoding session state: %w", err)
	}
	return &state, nil
}

// applyResume hands the engine's view of the session to the caller, and
// resynchronizes the client's tick sequence with it.
func (e *EngineClient) applyResume(state *types.SessionState) {
	// Anything the engine served but we never received is re-requested rather
	// than skipped. Without this the reconnect itself would cost a candle.
	if inflight := e.inflightSeq.Load(); inflight == 0 {
		e.nextSeq.Store(state.LastSeq)
	}

	log.Printf("Backtest Engine: resumed session %s at %s (%d/%d candles, last seq %d)",
		state.SessionID, state.Playhead.Format(time.RFC3339),
		state.Progress.ProcessedCandles, state.Progress.TotalCandles, state.LastSeq)

	e.onResumeMu.Lock()
	fn := e.onResume
	e.onResumeMu.Unlock()
	if fn != nil {
		fn(state)
	}
}

// storeStreamErr records the run's terminal error, if one is not already set.
//
// Sticky by design: the first thing that genuinely ended the run is the
// truthful explanation, and a later cascade should not overwrite it.
func (e *EngineClient) storeStreamErr(err error) {
	if e.streamErr.Load() != nil {
		return
	}
	e.streamErr.Store(&err)
}

// failPendingWaiters releases every in-flight request when a connection drops.
//
// Their responses are never coming: the socket that would have carried them is
// gone. Without this each caller sits out its own timeout — up to thirty
// seconds for a history fetch — for an answer that cannot arrive.
func (e *EngineClient) failPendingWaiters() {
	err := errors.New("backtest engine: connection dropped before a response arrived")

	e.pendingMu.Lock()
	orders, brackets := e.pendingOrders, e.pendingBrackets
	accounts, cancels := e.pendingAccounts, e.pendingCancels
	history, raw := e.pendingHistory, e.pendingRaw
	e.pendingOrders = make(map[string]chan *orderResponse)
	e.pendingBrackets = make(map[string]chan *bracketResponse)
	e.pendingAccounts = make(map[string]chan *accountResponse)
	e.pendingCancels = make(map[string]chan error)
	e.pendingHistory = make(map[string]chan *historyResponse)
	e.pendingRaw = make(map[string]chan *rawResponse)
	e.pendingMu.Unlock()

	for _, ch := range orders {
		select {
		case ch <- &orderResponse{err: err}:
		default:
		}
	}
	for _, ch := range brackets {
		select {
		case ch <- &bracketResponse{err: err}:
		default:
		}
	}
	for _, ch := range accounts {
		select {
		case ch <- &accountResponse{err: err}:
		default:
		}
	}
	for _, ch := range cancels {
		select {
		case ch <- err:
		default:
		}
	}
	for _, ch := range history {
		select {
		case ch <- &historyResponse{err: err}:
		default:
		}
	}
	for _, ch := range raw {
		select {
		case ch <- &rawResponse{err: err}:
		default:
		}
	}
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > reconnectMax {
		return reconnectMax
	}
	return d
}

// sleepCtx waits for d, reporting false if the context ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
