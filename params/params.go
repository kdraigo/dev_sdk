// Package params declares a strategy's settings. Each is read from the
// environment by its name, with a default, a type and optional bounds and help:
//
//	fast := params.Int("FAST_PERIOD", 50, params.Range(5, 500), params.Help("fast EMA period"))
//	start := params.Date("BT_START", "today-30d", params.Help("first day of the backtest, UTC"))
//
// The cloud workspace reads the declarations from the code without running it,
// so the website shows a form for them and a backtest can be run again with
// other values, with no AI and no code change. That is why a declaration's
// name, default and options must be written as literals in the call itself.
//
// Names are upper-case letters, digits and _ (at most 64), and never start
// with KDRAIGO_, which the platform keeps for its own variables. A value that
// does not parse, or falls outside its range or choices, stops the program at
// once with a message naming the parameter.
package params

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Option refines a declaration.
type Option func(*spec)

// Range bounds an Int or Float, both ends included.
func Range(min, max float64) Option {
	return func(s *spec) { s.min, s.max, s.bounded = min, max, true }
}

// Help is a short description, shown next to the parameter on the website.
func Help(text string) Option {
	return func(s *spec) { s.help = text }
}

type spec struct {
	min, max float64
	bounded  bool
	help     string
}

var (
	mu     sync.Mutex
	kinds  = map[string]string{}
	values = map[string]string{}

	// fail stops the program on a bad value; tests replace it.
	fail = func(format string, args ...any) { log.Fatalf(format, args...) }
	// now is the clock "today" is read from; tests replace it.
	now = time.Now
)

var nameRule = regexp.MustCompile(`^[A-Z_][A-Z0-9_]{0,63}$`)

// read returns the raw value of name (its default when the environment does
// not set it) after checking the name, and records it as kind.
func read(kind, name, def string) string {
	switch {
	case !nameRule.MatchString(name):
		fail("parameter %q: names are upper-case letters, digits and _, at most 64", name)
	case strings.HasPrefix(name, "KDRAIGO_"):
		fail("parameter %s: the KDRAIGO_ names are the platform's", name)
	}
	raw, ok := os.LookupEnv(name)
	if !ok {
		raw = def
	}
	mu.Lock()
	defer mu.Unlock()
	if k, seen := kinds[name]; seen && k != kind {
		fail("parameter %s is declared twice, as %s and as %s", name, k, kind)
	}
	kinds[name] = kind
	values[name] = raw
	return raw
}

func apply(opts []Option) spec {
	var s spec
	for _, o := range opts {
		o(&s)
	}
	return s
}

func checkRange(name string, v float64, s spec) {
	if s.bounded && (v < s.min || v > s.max) {
		fail("parameter %s = %v: must be between %v and %v", name, v, s.min, s.max)
	}
}

// Int is an integer parameter.
func Int(name string, def int, opts ...Option) int {
	raw := read("int", name, strconv.Itoa(def))
	v, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		fail("parameter %s = %q: want an integer", name, raw)
	}
	checkRange(name, float64(v), apply(opts))
	return v
}

// Float is a number parameter.
func Float(name string, def float64, opts ...Option) float64 {
	raw := read("float", name, strconv.FormatFloat(def, 'f', -1, 64))
	v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		fail("parameter %s = %q: want a number", name, raw)
	}
	checkRange(name, v, apply(opts))
	return v
}

// Bool is a yes/no parameter: true, false, 1 or 0.
func Bool(name string, def bool, opts ...Option) bool {
	raw := read("bool", name, strconv.FormatBool(def))
	v, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		fail("parameter %s = %q: want true or false", name, raw)
	}
	return v
}

// String is a free text parameter.
func String(name, def string, opts ...Option) string {
	return read("string", name, def)
}

// Choice is one of a fixed list of values.
func Choice(name, def string, choices []string, opts ...Option) string {
	raw := strings.TrimSpace(read("choice", name, def))
	for _, c := range choices {
		if raw == c {
			return raw
		}
	}
	fail("parameter %s = %q: want one of %s", name, raw, strings.Join(choices, ", "))
	return raw
}

// List is a comma-separated list, e.g. "BTC/USDT,ETH/USDT"; blanks are
// dropped.
func List(name, def string, opts ...Option) []string {
	raw := read("list", name, def)
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Date is a UTC day: "2025-10-01", "today", or "today-30d" (that many days
// before today).
func Date(name, def string, opts ...Option) time.Time {
	raw := strings.TrimSpace(read("date", name, def))
	t, err := ParseDate(raw, now())
	if err != nil {
		fail("parameter %s = %q: %v", name, raw, err)
	}
	return t
}

// ParseDate reads a Date parameter's value against today (UTC).
func ParseDate(raw string, today time.Time) (time.Time, error) {
	day := today.UTC().Truncate(24 * time.Hour)
	switch {
	case raw == "today":
		return day, nil
	case strings.HasPrefix(raw, "today-") && strings.HasSuffix(raw, "d"):
		n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimPrefix(raw, "today-"), "d"))
		if err != nil || n < 0 {
			return time.Time{}, fmt.Errorf("want YYYY-MM-DD, today or today-<days>d")
		}
		return day.AddDate(0, 0, -n), nil
	}
	t, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("want YYYY-MM-DD, today or today-<days>d")
	}
	return t, nil
}

// Duration is a length of time, e.g. "4h" or "90m".
func Duration(name, def string, opts ...Option) time.Duration {
	raw := strings.TrimSpace(read("duration", name, def))
	d, err := time.ParseDuration(raw)
	if err != nil {
		fail("parameter %s = %q: want a duration such as 4h or 90m", name, raw)
	}
	return d
}

// Values are the values of every parameter read so far, by name, as given:
// what a run's log records so its result can be reproduced.
func Values() map[string]string {
	mu.Lock()
	defer mu.Unlock()
	out := make(map[string]string, len(values))
	for k, v := range values {
		out[k] = v
	}
	return out
}

// Log prints every parameter read so far, sorted: "params: FAST=50 SLOW=200".
func Log() {
	v := Values()
	names := make([]string, 0, len(v))
	for n := range v {
		names = append(names, n)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = n + "=" + v[n]
	}
	log.Printf("params: %s", strings.Join(parts, " "))
}
