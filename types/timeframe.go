package types

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// This mirrors github.com/kdraigo/lib/timeframe, which is the platform's source
// of truth. It is duplicated rather than imported on purpose: dev_sdk is a
// public-facing SDK and must not drag lib's transitive dependencies — sqlx,
// lib/pq, testcontainers, OpenTelemetry, gin — into every strategy binary.
//
// The duplication is contained by timeframes.json, a fixture checked into both
// repositories, and a test on each side asserting its table matches. If the two
// ever drift, both test suites fail.
//
// Casing is significant: lowercase "m" is minute, uppercase "M" is month. Never
// lowercase a timeframe string.

// TimeframeKind describes how a timestamp maps to the start of its bucket.
type TimeframeKind int

const (
	// KindModulo buckets by epoch modulo. Correct for any period that divides a
	// day evenly, because the Unix epoch begins at a UTC midnight.
	KindModulo TimeframeKind = iota

	// KindWeekMonday anchors weeks to Monday 00:00 UTC. Epoch 0 is a Thursday,
	// so plain modulo bucketing would open weekly candles on the wrong day.
	KindWeekMonday

	// KindCalendarMonth buckets into calendar months, which are 28–31 days and
	// so cannot be expressed as a fixed duration.
	KindCalendarMonth
)

const (
	minuteMs       = int64(60_000)
	dayMs          = int64(86_400_000)
	weekMs         = int64(604_800_000)
	mondayOffsetMs = int64(345_600_000) // epoch Thursday → Monday
	approxMonthMs  = int64(31) * dayMs
)

// TimeframeSpec describes one timeframe.
type TimeframeSpec struct {
	Name   Timeframe
	Millis int64 // nominal length; 0 for a calendar month
	Kind   TimeframeKind
}

var timeframeSpecs = []TimeframeSpec{
	{Name: Timeframe1m, Millis: minuteMs, Kind: KindModulo},
	{Name: Timeframe2m, Millis: 2 * minuteMs, Kind: KindModulo},
	{Name: Timeframe3m, Millis: 3 * minuteMs, Kind: KindModulo},
	{Name: Timeframe5m, Millis: 5 * minuteMs, Kind: KindModulo},
	{Name: Timeframe15m, Millis: 15 * minuteMs, Kind: KindModulo},
	{Name: Timeframe30m, Millis: 30 * minuteMs, Kind: KindModulo},
	{Name: Timeframe1h, Millis: 60 * minuteMs, Kind: KindModulo},
	{Name: Timeframe2h, Millis: 120 * minuteMs, Kind: KindModulo},
	{Name: Timeframe4h, Millis: 240 * minuteMs, Kind: KindModulo},
	{Name: Timeframe1d, Millis: dayMs, Kind: KindModulo},
	{Name: Timeframe1w, Millis: weekMs, Kind: KindWeekMonday},
	{Name: Timeframe1M, Millis: 0, Kind: KindCalendarMonth},
}

var timeframesByName = func() map[Timeframe]TimeframeSpec {
	m := make(map[Timeframe]TimeframeSpec, len(timeframeSpecs))
	for _, s := range timeframeSpecs {
		m[s.Name] = s
	}
	return m
}()

// ParseTimeframe resolves a canonical timeframe name. It is strict and
// case-sensitive: an unknown value is an error rather than a silent downgrade
// to one minute.
func ParseTimeframe(tf Timeframe) (TimeframeSpec, error) {
	spec, ok := timeframesByName[tf]
	if !ok {
		return TimeframeSpec{}, fmt.Errorf("unsupported timeframe %q (supported: %s)", tf, strings.Join(TimeframeNames(), ", "))
	}
	return spec, nil
}

// AllTimeframes returns every supported timeframe, ascending by duration.
func AllTimeframes() []TimeframeSpec {
	out := make([]TimeframeSpec, len(timeframeSpecs))
	copy(out, timeframeSpecs)
	return out
}

// TimeframeNames returns every supported name, ascending by duration.
func TimeframeNames() []string {
	out := make([]string, len(timeframeSpecs))
	for i, s := range timeframeSpecs {
		out[i] = string(s.Name)
	}
	return out
}

// SmallestTimeframe returns the shortest of the given timeframes, by true
// duration rather than by name length — "5m" is shorter than "1h" despite the
// longer string. Unknown values are ignored.
func SmallestTimeframe(in []Timeframe) (Timeframe, bool) {
	specs := make([]TimeframeSpec, 0, len(in))
	for _, tf := range in {
		if spec, err := ParseTimeframe(tf); err == nil {
			specs = append(specs, spec)
		}
	}
	if len(specs) == 0 {
		return "", false
	}

	sort.SliceStable(specs, func(i, j int) bool {
		return specs[i].ApproxMillis() < specs[j].ApproxMillis()
	})
	return specs[0].Name, true
}

// ApproxMillis returns a non-zero nominal length; a calendar month reports 31
// days. Use it for sizing, never for bucketing.
func (s TimeframeSpec) ApproxMillis() int64 {
	if s.Kind == KindCalendarMonth {
		return approxMonthMs
	}
	return s.Millis
}

// Duration is the nominal length as a time.Duration. A calendar month has no
// fixed duration, so this is an approximation for it.
func (s TimeframeSpec) Duration() time.Duration {
	return time.Duration(s.ApproxMillis()) * time.Millisecond
}

// BucketStart returns the inclusive open time of the bucket containing t.
//
// Pre-epoch timestamps are unsupported and clamp to math.MinInt64: Go truncates
// integer division toward zero rather than flooring, so modulo bucketing would
// otherwise return a start later than its own input. A zero-valued time.Time
// marshalled through an API lands here, and must not produce a plausible answer.
func (s TimeframeSpec) BucketStart(t time.Time) time.Time {
	ms := t.UTC().UnixMilli()
	if ms < 0 {
		return time.UnixMilli(math.MinInt64).UTC()
	}

	switch s.Kind {
	case KindWeekMonday:
		start := ((ms-mondayOffsetMs)/weekMs)*weekMs + mondayOffsetMs
		return time.UnixMilli(start).UTC()

	case KindCalendarMonth:
		u := t.UTC()
		return time.Date(u.Year(), u.Month(), 1, 0, 0, 0, 0, time.UTC)

	default:
		return time.UnixMilli((ms / s.Millis) * s.Millis).UTC()
	}
}

// SameBucket reports whether two instants fall in the same bucket.
//
// This is the correct way to detect a boundary crossing: subtracting timestamps
// and comparing against a duration cannot express a calendar month.
func (s TimeframeSpec) SameBucket(a, b time.Time) bool {
	return s.BucketStart(a).Equal(s.BucketStart(b))
}
