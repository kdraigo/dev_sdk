package dev_sdk

import (
	"testing"
	"time"
)

func TestBacktestClock_StartsAtSessionFrom(t *testing.T) {
	start := time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC)
	c := newBacktestClock(start)
	if got := c.Now(); !got.Equal(start) {
		t.Fatalf("expected Now() == %s, got %s", start, got)
	}
}

func TestBacktestClock_AdvancesForward(t *testing.T) {
	start := time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC)
	c := newBacktestClock(start)

	step1 := start.Add(time.Minute)
	c.Advance(step1)
	if got := c.Now(); !got.Equal(step1) {
		t.Fatalf("after first Advance: expected %s, got %s", step1, got)
	}

	step2 := start.Add(15 * time.Minute)
	c.Advance(step2)
	if got := c.Now(); !got.Equal(step2) {
		t.Fatalf("after second Advance: expected %s, got %s", step2, got)
	}
}

func TestBacktestClock_DoesNotRewind(t *testing.T) {
	start := time.Date(2025, 5, 5, 0, 0, 0, 0, time.UTC)
	c := newBacktestClock(start)

	forward := start.Add(time.Hour)
	c.Advance(forward)

	// Try to rewind — should be ignored.
	c.Advance(start)
	if got := c.Now(); !got.Equal(forward) {
		t.Fatalf("clock should not rewind: expected %s, got %s", forward, got)
	}

	// Equal time — also ignored (no observable change required either way).
	c.Advance(forward)
	if got := c.Now(); !got.Equal(forward) {
		t.Fatalf("clock should not move on equal time: expected %s, got %s", forward, got)
	}
}

func TestWallClock_ReturnsNow(t *testing.T) {
	c := wallClock{}
	before := time.Now()
	got := c.Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("wallClock.Now() = %s outside [%s, %s]", got, before, after)
	}
}
