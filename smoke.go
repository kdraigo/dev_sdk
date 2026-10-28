package dev_sdk

import (
	"log"
	"strconv"
	"strings"

	"github.com/kdraigo/dev_sdk/types"
)

// smokeEnv is set by the cloud workspace for a smoke run: the backtest is cut
// to its first days, so a panic or a refused order shows in seconds, before
// the full run starts. Nothing else sets it.
const smokeEnv = "KDRAIGO_SMOKE_DAYS"

// applySmoke cuts a backtest to the first days of its window (days is the
// variable's value; empty, unreadable or not positive leaves it whole) and
// names the session as a smoke run.
func applySmoke(b *types.BacktestOptions, days string) {
	if days == "" {
		return
	}
	n, err := strconv.Atoi(strings.TrimSpace(days))
	if err != nil || n <= 0 {
		log.Printf("DevSDK: ignoring %s=%q: not a positive number of days", smokeEnv, days)
		return
	}
	if end := b.StartTime.AddDate(0, 0, n); b.EndTime.IsZero() || end.Before(b.EndTime) {
		b.EndTime = end
	}
	b.SessionName = strings.TrimSpace(b.SessionName + " (smoke)")
	log.Printf("DevSDK: smoke run: the first %d days only, %s to %s", n,
		b.StartTime.UTC().Format("2006-01-02 15:04"), b.EndTime.UTC().Format("2006-01-02 15:04"))
}
