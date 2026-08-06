package types

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

// fixtureSpec mirrors an entry in timeframes.json, the file checked into both
// this repo and lib. The two maintain independent tables — dev_sdk must not
// depend on lib — so this fixture is what stops them drifting apart.
type fixtureSpec struct {
	Name   string `json:"name"`
	Millis int64  `json:"millis"`
	Kind   string `json:"kind"`
}

func loadFixture(t *testing.T) []fixtureSpec {
	t.Helper()

	raw, err := os.ReadFile("timeframes.json")
	if err != nil {
		t.Fatalf("read shared fixture: %v", err)
	}

	var doc struct {
		Timeframes []fixtureSpec `json:"timeframes"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("decode shared fixture: %v", err)
	}
	return doc.Timeframes
}

var kindNames = map[TimeframeKind]string{
	KindModulo:        "modulo",
	KindWeekMonday:    "week_monday",
	KindCalendarMonth: "calendar_month",
}

// TestTimeframeTableMatchesSharedFixture is the anti-drift guard between this
// SDK and lib/timeframe.
func TestTimeframeTableMatchesSharedFixture(t *testing.T) {
	fixture := loadFixture(t)
	specs := AllTimeframes()

	if len(specs) != len(fixture) {
		t.Fatalf("table has %d timeframes, shared fixture has %d", len(specs), len(fixture))
	}

	for i, want := range fixture {
		got := specs[i]
		if string(got.Name) != want.Name {
			t.Errorf("index %d: name %q, fixture says %q", i, got.Name, want.Name)
		}
		if got.Millis != want.Millis {
			t.Errorf("%s: millis %d, fixture says %d", got.Name, got.Millis, want.Millis)
		}
		if kindNames[got.Kind] != want.Kind {
			t.Errorf("%s: kind %q, fixture says %q", got.Name, kindNames[got.Kind], want.Kind)
		}
	}
}

func TestParseTimeframe(t *testing.T) {
	for _, spec := range AllTimeframes() {
		if _, err := ParseTimeframe(spec.Name); err != nil {
			t.Errorf("ParseTimeframe(%q) must succeed: %v", spec.Name, err)
		}
	}

	for _, bad := range []Timeframe{"", "1y", "7m", "1D", "1W", "1s"} {
		if _, err := ParseTimeframe(bad); err == nil {
			t.Errorf("ParseTimeframe(%q) must fail", bad)
		}
	}
}

// TestMinuteVersusMonth is the casing guard: lowercase m is a minute, uppercase
// M a month, and nothing may fold one into the other.
func TestMinuteVersusMonth(t *testing.T) {
	minute, err := ParseTimeframe(Timeframe1m)
	if err != nil {
		t.Fatal(err)
	}
	month, err := ParseTimeframe(Timeframe1M)
	if err != nil {
		t.Fatal(err)
	}

	if minute.Kind != KindModulo || minute.Millis != 60_000 {
		t.Errorf("1m must be a 60s modulo timeframe, got %+v", minute)
	}
	if month.Kind != KindCalendarMonth {
		t.Errorf("1M must be a calendar month, got %+v", month)
	}
}

func ts(y int, mo time.Month, d, h, mi int) time.Time {
	return time.Date(y, mo, d, h, mi, 0, 0, time.UTC)
}

// TestWeekBucketsOpenOnMonday: bucketing is done over Unix milliseconds, whose
// epoch is a Thursday, so weeks need an explicit offset to open on Monday.
func TestWeekBucketsOpenOnMonday(t *testing.T) {
	spec, err := ParseTimeframe(Timeframe1w)
	if err != nil {
		t.Fatal(err)
	}

	for _, probe := range []time.Time{
		ts(2026, time.January, 5, 0, 0),   // Monday
		ts(2026, time.January, 8, 12, 30), // Thursday
		ts(2026, time.January, 11, 23, 59),
	} {
		start := spec.BucketStart(probe)
		if start.Weekday() != time.Monday {
			t.Errorf("%s bucketed to %s (%s), want a Monday", probe, start, start.Weekday())
		}
		if start.After(probe) {
			t.Errorf("bucket start %s is after its own input %s", start, probe)
		}
	}

	// Bucketing is implemented over Unix milliseconds, where the epoch is a
	// Thursday — so a naive modulo would anchor weeks to Thursdays. Prove the
	// Monday offset is actually being applied.
	//
	// (Note this is specifically about millisecond modulo. Go's Time.Truncate
	// anchors at the zero time, year 1, which happens to be a Monday, so the
	// aggregator's previous use of Truncate was already correct for weeks. The
	// reason it needed replacing is calendar months, not weeks.)
	probe := ts(2026, time.January, 8, 12, 0) // a Thursday
	naive := time.UnixMilli((probe.UnixMilli() / int64(7*24*time.Hour/time.Millisecond)) *
		int64(7*24*time.Hour/time.Millisecond)).UTC()
	if naive.Weekday() != time.Thursday {
		t.Fatalf("fixture assumption broken: naive modulo gave %s", naive.Weekday())
	}
	if spec.BucketStart(probe).Equal(naive) {
		t.Error("week bucketing is epoch-anchored (Thursday) rather than Monday-anchored")
	}
}

// TestCalendarMonthBucketing covers lengths a fixed duration cannot express.
func TestCalendarMonthBucketing(t *testing.T) {
	spec, err := ParseTimeframe(Timeframe1M)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		at   time.Time
		want time.Time
	}{
		{ts(2024, time.February, 29, 23, 59), ts(2024, time.February, 1, 0, 0)}, // leap
		{ts(2025, time.February, 28, 12, 0), ts(2025, time.February, 1, 0, 0)},
		{ts(2025, time.December, 31, 23, 59), ts(2025, time.December, 1, 0, 0)},
	} {
		if got := spec.BucketStart(tc.at); !got.Equal(tc.want) {
			t.Errorf("BucketStart(%s) = %s, want %s", tc.at, got, tc.want)
		}
	}

	// A month boundary is a boundary; a day within one is not.
	if spec.SameBucket(ts(2026, time.January, 31, 23, 59), ts(2026, time.February, 1, 0, 0)) {
		t.Error("31 Jan and 1 Feb must not share a month bucket")
	}
	if !spec.SameBucket(ts(2026, time.January, 1, 0, 0), ts(2026, time.January, 31, 23, 59)) {
		t.Error("1 Jan and 31 Jan must share a month bucket")
	}
}

// TestSmallestTimeframe: ordering is by duration, not name length. "5m" is a
// longer string than "1h" but a shorter period.
func TestSmallestTimeframe(t *testing.T) {
	got, ok := SmallestTimeframe([]Timeframe{Timeframe1h, Timeframe5m, Timeframe1d})
	if !ok || got != Timeframe5m {
		t.Errorf("Smallest = %q (ok=%v), want \"5m\"", got, ok)
	}

	// A calendar month has Millis == 0 but is the longest timeframe, so ordering
	// must use the approximate length rather than the raw field.
	if got, _ := SmallestTimeframe([]Timeframe{Timeframe1M, Timeframe1d}); got != Timeframe1d {
		t.Errorf("Smallest with 1M = %q, want \"1d\"", got)
	}

	if _, ok := SmallestTimeframe(nil); ok {
		t.Error("Smallest(nil) must report false")
	}
	if _, ok := SmallestTimeframe([]Timeframe{"1y"}); ok {
		t.Error("Smallest of only-unknown values must report false")
	}
}

func TestBucketStartRejectsPreEpoch(t *testing.T) {
	zero := time.Time{}
	for _, spec := range AllTimeframes() {
		if got := spec.BucketStart(zero); got.After(time.Unix(0, 0)) {
			t.Errorf("%s: a zero time must not produce a plausible bucket, got %s", spec.Name, got)
		}
	}
}
