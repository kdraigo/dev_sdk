package dev_sdk

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// A run stopped mid-way (its context cancelled: Ctrl+C, the cloud workspace's
// stop) tells the engine it is over with the close action, so the engine ends
// the session instead of parking it for a resume nobody will make.
func TestSDK_StoppedRunClosesItsSession(t *testing.T) {
	mock := newMockEngineServer()
	defer mock.server.Close()

	sdk, err := New(newTruncationConfig(mock.server.URL))
	if err != nil {
		t.Fatalf("New SDK err: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var once sync.Once
	sdk.SetOnCandle(func(*types.Context, *types.Candle) {
		once.Do(cancel) // stop after the first candle
	})

	if err := sdk.Start(ctx); err == nil {
		t.Fatal("a stopped run must not look finished")
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mock.mu.Lock()
		closed := mock.closed
		mock.mu.Unlock()
		if closed {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the engine was never told the run is over")
}
