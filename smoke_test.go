package dev_sdk

import (
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

// A smoke run keeps the window's first days and says so in the session's
// name; a window already shorter, or no variable, is left as it is.
func TestApplySmoke(t *testing.T) {
	start := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	end := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name, days string
		end        time.Time
		wantEnd    time.Time
		wantName   string
	}{
		{"cut", "7", end, start.AddDate(0, 0, 7), "rsi (smoke)"},
		{"shorter window", "7", start.AddDate(0, 0, 3), start.AddDate(0, 0, 3), "rsi (smoke)"},
		{"unset", "", end, end, "rsi"},
		{"unreadable", "a week", end, end, "rsi"},
		{"zero", "0", end, end, "rsi"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := &types.BacktestOptions{SessionName: "rsi", StartTime: start, EndTime: tc.end}
			applySmoke(b, tc.days)
			if !b.EndTime.Equal(tc.wantEnd) || b.SessionName != tc.wantName || !b.StartTime.Equal(start) {
				t.Errorf("got %s → %s %q, want %s → %s %q", b.StartTime, b.EndTime, b.SessionName, start, tc.wantEnd, tc.wantName)
			}
		})
	}
}
