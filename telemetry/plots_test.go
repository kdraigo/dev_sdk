package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kdraigo/dev_sdk/plots"
)

// plotServer answers every POST with status and body, and records the events.
func plotServer(t *testing.T, status int, body string) (*httpPublisher, <-chan telemetryPayload, <-chan http.Header) {
	t.Helper()
	got := make(chan telemetryPayload, 4)
	headers := make(chan http.Header, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var p telemetryPayload
		_ = json.Unmarshal(b, &p)
		got <- p
		headers <- r.Header.Clone()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	pub := NewPublisher("sess-1", srv.URL, "key-id",
		"385d5c080a1b4140a5ed9ee76d0ef3fcd291cabab4ec6759bc178ad3a8ed837148309e3cb2a3a014c93d68b4f20a0ba5978ab300531c844dcec672925eb8d63a",
		"binance_futures", "BTC/USDT").(*httpPublisher)
	return pub, got, headers
}

func batch() *plots.Batch {
	return &plots.Batch{
		Defs:   []plots.Def{{Key: "ema", Kind: plots.Line}},
		Points: []plots.Point{{Key: "ema", Exchange: "binance_futures", Pair: "BTC/USDT", Time: 1, Value: 2}},
	}
}

func TestPublishPlots_PostsASignedPlotsEvent(t *testing.T) {
	pub, got, headers := plotServer(t, http.StatusNoContent, "")
	warning, err := pub.PublishPlots(context.Background(), batch())
	if err != nil || warning != "" {
		t.Fatalf("PublishPlots = %q, %v", warning, err)
	}
	p := <-got
	if p.EventType != "plots" || p.SessionID != "sess-1" || p.Plots == nil || len(p.Plots.Points) != 1 || p.Plots.Defs[0].Key != "ema" {
		t.Fatalf("event %+v", p)
	}
	h := <-headers
	if h.Get("X-Signature") == "" || h.Get("X-Nonce") == "" {
		t.Fatalf("not signed: %v", h)
	}
}

func TestPublishPlots_ReadsTheWarning(t *testing.T) {
	pub, _, _ := plotServer(t, http.StatusOK, `{"warning":"plot storage is full"}`)
	warning, err := pub.PublishPlots(context.Background(), batch())
	if err != nil || warning != "plot storage is full" {
		t.Fatalf("PublishPlots = %q, %v", warning, err)
	}
}

// A refusal cannot be fixed by sending again; an outage can.
func TestPublishPlots_TellsARefusalFromAnOutage(t *testing.T) {
	for status, rejected := range map[int]bool{400: true, 403: true, 413: true, 408: false, 429: false, 500: false, 502: false} {
		pub, _, _ := plotServer(t, status, "nope")
		_, err := pub.PublishPlots(context.Background(), batch())
		if err == nil {
			t.Fatalf("%d: no error", status)
		}
		if errors.Is(err, ErrPlotsRejected) != rejected {
			t.Errorf("%d: rejected=%v, want %v (%v)", status, !rejected, rejected, err)
		}
	}
}

func TestPublishPlots_EmptyBatchSendsNothing(t *testing.T) {
	pub, got, _ := plotServer(t, http.StatusNoContent, "")
	if _, err := pub.PublishPlots(context.Background(), &plots.Batch{}); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-got:
		t.Fatalf("sent %+v", p)
	default:
	}
}
