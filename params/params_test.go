package params

import (
	"fmt"
	"testing"
	"time"
)

// failed records what fail was called with, instead of exiting.
func capture(t *testing.T) *string {
	t.Helper()
	var msg string
	old := fail
	fail = func(format string, args ...any) {
		if msg == "" {
			msg = fmt.Sprintf(format, args...)
		}
	}
	t.Cleanup(func() {
		fail = old
		mu.Lock()
		kinds, values = map[string]string{}, map[string]string{}
		mu.Unlock()
	})
	return &msg
}

func TestDefaultsAndEnvironment(t *testing.T) {
	failed := capture(t)
	t.Setenv("FAST", "21")
	t.Setenv("RISK", "0.02")
	t.Setenv("LONG_ONLY", "1")
	t.Setenv("PAIRS", " BTC/USDT, ,ETH/USDT ")
	if v := Int("FAST", 50, Range(5, 500)); v != 21 {
		t.Errorf("FAST = %d", v)
	}
	if v := Int("SLOW", 200); v != 200 {
		t.Errorf("SLOW = %d, want its default", v)
	}
	if v := Float("RISK", 0.01, Range(0, 0.1)); v != 0.02 {
		t.Errorf("RISK = %v", v)
	}
	if !Bool("LONG_ONLY", false) {
		t.Error("LONG_ONLY")
	}
	if v := List("PAIRS", "BTC/USDT"); len(v) != 2 || v[1] != "ETH/USDT" {
		t.Errorf("PAIRS = %q", v)
	}
	if v := Choice("SIDE", "long", []string{"long", "short", "both"}); v != "long" {
		t.Errorf("SIDE = %q", v)
	}
	if v := Duration("HOLD", "4h"); v != 4*time.Hour {
		t.Errorf("HOLD = %v", v)
	}
	if *failed != "" {
		t.Fatal(*failed)
	}
	got := Values()
	if got["FAST"] != "21" || got["SLOW"] != "200" || got["SIDE"] != "long" {
		t.Errorf("Values() = %v", got)
	}
}

func TestBadValuesStopTheProgram(t *testing.T) {
	for name, declare := range map[string]func(){
		"not an int":         func() { t.Setenv("A", "x"); Int("A", 1) },
		"out of range":       func() { t.Setenv("B", "9"); Int("B", 1, Range(1, 5)) },
		"float out of range": func() { t.Setenv("C", "0.5"); Float("C", 0.01, Range(0, 0.1)) },
		"not a choice":       func() { t.Setenv("D", "up"); Choice("D", "long", []string{"long", "short"}) },
		"bad date":           func() { t.Setenv("E", "yesterday"); Date("E", "today") },
		"bad duration":       func() { t.Setenv("F", "4 hours"); Duration("F", "4h") },
		"lower-case name":    func() { Int("fast", 1) },
		"platform name":      func() { String("KDRAIGO_KEY_ID", "") },
		"declared twice":     func() { Int("G", 1); String("G", "x") },
	} {
		t.Run(name, func(t *testing.T) {
			failed := capture(t)
			declare()
			if *failed == "" {
				t.Fatal("accepted")
			}
		})
	}
}

func TestDates(t *testing.T) {
	today := time.Date(2026, 10, 27, 15, 4, 0, 0, time.UTC)
	for raw, want := range map[string]time.Time{
		"today":      time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC),
		"today-30d":  time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC),
		"2025-03-01": time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
	} {
		got, err := ParseDate(raw, today)
		if err != nil || !got.Equal(want) {
			t.Errorf("ParseDate(%q) = %v, %v; want %v", raw, got, err, want)
		}
	}
	for _, bad := range []string{"today-x", "today-3", "2025/03/01", "today+1d"} {
		if _, err := ParseDate(bad, today); err == nil {
			t.Errorf("ParseDate(%q) accepted", bad)
		}
	}
}
