// Package plots lets a strategy show its own numbers on the session chart: an
// EMA over the candles, ADX in a panel of its own, a marker where a signal
// fired.
//
// Declare each plot once, before Start, then add a value per bar:
//
//	fast, _ := sdk.Plot(plots.Def{Key: "ema_fast", Title: "EMA 50"})
//	adx, _ := sdk.Plot(plots.Def{Key: "adx", Title: "ADX 14", Pane: "ADX",
//		Levels: []plots.Level{{Value: 25}}})
//
//	sdk.SetOnCandleFor(types.Timeframe1h, func(ctx *types.Context, c *types.Candle) {
//		fast.Add(c, emaFast)
//		adx.Add(c, adxValue)
//	})
//
// A value belongs to an exchange, a pair and a bar's open time, so one plot
// serves every pair a strategy trades and the chart shows the selected pair's
// values. The strategy sets how a plot looks; the chart lets the viewer hide it
// or restyle it.
//
// Plotting never fails a run. A plot that cannot be declared returns an error
// and a nil *Series, and every method of a nil *Series does nothing, so
// trading code does not need to guard each call.
package plots

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"reflect"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/kdraigo/dev_sdk/types"
)

// Kind is how a plot is drawn.
type Kind string

const (
	Line      Kind = "line"      // a line through the values
	Step      Kind = "step"      // a line that holds each value until the next one
	Histogram Kind = "histogram" // a bar per value from zero
	Area      Kind = "area"      // a line with the area under it filled
	Band      Kind = "band"      // two lines (AddBand) with the space between them filled
	Markers   Kind = "markers"   // a marker on each bar a value was added for
)

// LineStyle is the dash pattern of a line.
type LineStyle string

const (
	Solid  LineStyle = "solid"
	Dashed LineStyle = "dashed"
	Dotted LineStyle = "dotted"
)

// Shape is a marker's shape.
type Shape string

const (
	ArrowUp   Shape = "arrow_up"
	ArrowDown Shape = "arrow_down"
	Circle    Shape = "circle"
	Square    Shape = "square"
)

// Position is where a marker sits relative to its bar.
type Position string

const (
	Above   Position = "above" // above the bar's high
	Below   Position = "below" // below the bar's low
	AtValue Position = "at"    // at the value added with it
)

// Limits, which the platform enforces too.
const (
	MaxPlots    = 32 // plots per session
	MaxPanes    = 8  // panels besides the candles
	MaxLevels   = 8  // levels per plot
	MaxKeyLen   = 64
	MaxTitleLen = 64
	MaxPaneLen  = 32
	MaxTextLen  = 32 // characters of marker text; longer text is cut

	// maxBuffered bounds the points waiting to be sent. The backtest sends
	// them with every tick, so reaching it means nothing is draining them.
	maxBuffered = 200_000
)

// Level is a horizontal line drawn across a plot's panel, such as RSI's 70
// and 30.
type Level struct {
	Value float64 `json:"value"`
	Title string  `json:"title,omitempty"`
}

// Marker styles a Markers plot.
type Marker struct {
	Shape    Shape    `json:"shape,omitempty"`    // default Circle
	Position Position `json:"position,omitempty"` // default Above
}

// Def declares a plot. Only Key is required.
type Def struct {
	// Key names the plot for the platform: lowercase letters, digits, '_',
	// '.' and '-', at most 64 characters. Values are added under it, and the
	// chart remembers a viewer's choices by it.
	Key string `json:"key"`
	// Title is what the chart's legend shows. Defaults to Key.
	Title string `json:"title,omitempty"`
	// Pane is the panel the plot is drawn in. Empty draws it over the candles
	// on their price scale, which suits values in price units (moving
	// averages, bands). Any other name gets a panel of its own below the
	// candles, shared by every plot with the same Pane.
	Pane string `json:"pane,omitempty"`
	// Kind defaults to Line.
	Kind Kind `json:"kind,omitempty"`
	// Timeframe is the timeframe of the values. Add fills it in from the
	// candle; set it when adding with AddAt. The chart uses it to never draw a
	// value before its bar closed.
	Timeframe types.Timeframe `json:"timeframe,omitempty"`
	// Color is "#rrggbb". Empty takes the chart's next plot colour.
	Color string `json:"color,omitempty"`
	// Color2 colours a band's fill and a histogram's negative bars.
	Color2 string `json:"color2,omitempty"`
	// LineWidth is 1 to 4 pixels; 0 is the chart's default.
	LineWidth int       `json:"line_width,omitempty"`
	LineStyle LineStyle `json:"line_style,omitempty"`
	// SignColors draws a histogram's bars in the chart's up colour at or above
	// zero and its down colour below.
	SignColors bool `json:"sign_colors,omitempty"`
	// Precision is the decimals shown; 0 lets the chart choose.
	Precision int     `json:"precision,omitempty"`
	Levels    []Level `json:"levels,omitempty"`
	// Marker styles a Markers plot and is ignored on the rest.
	Marker *Marker `json:"marker,omitempty"`
	// Hidden plots start switched off on the chart; the viewer can show them.
	Hidden bool `json:"hidden,omitempty"`
}

// Point is one value as it travels to the platform. Field names are short
// because a long run sends millions of them.
type Point struct {
	Key      string   `json:"k"`
	Exchange string   `json:"x"`
	Pair     string   `json:"p"`
	Time     int64    `json:"t"` // the bar's open time, Unix milliseconds
	Value    float64  `json:"v"`
	Value2   *float64 `json:"v2,omitempty"` // a band's lower value
	Text     string   `json:"s,omitempty"`  // a marker's text
}

// Batch is what a carrier sends: declarations not yet sent, then points.
type Batch struct {
	Defs   []Def   `json:"defs,omitempty"`
	Points []Point `json:"points,omitempty"`
}

// Empty reports whether the batch carries nothing.
func (b *Batch) Empty() bool { return b == nil || (len(b.Defs) == 0 && len(b.Points) == 0) }

var (
	keyPattern   = regexp.MustCompile(`^[a-z0-9_.-]{1,64}$`)
	colorPattern = regexp.MustCompile(`^#[0-9a-fA-F]{6}([0-9a-fA-F]{2})?$`)
)

// Normalize fills in defaults and checks a declaration against the limits.
func (d Def) Normalize() (Def, error) {
	if !keyPattern.MatchString(d.Key) {
		return d, fmt.Errorf("plot key %q: use 1-%d of a-z, 0-9, '_', '.', '-'", d.Key, MaxKeyLen)
	}
	if d.Title == "" {
		d.Title = d.Key
	}
	if utf8.RuneCountInString(d.Title) > MaxTitleLen {
		return d, fmt.Errorf("plot %s: title longer than %d characters", d.Key, MaxTitleLen)
	}
	if utf8.RuneCountInString(d.Pane) > MaxPaneLen {
		return d, fmt.Errorf("plot %s: pane name longer than %d characters", d.Key, MaxPaneLen)
	}
	if d.Kind == "" {
		d.Kind = Line
	}
	switch d.Kind {
	case Line, Step, Histogram, Area, Band, Markers:
	default:
		return d, fmt.Errorf("plot %s: unknown kind %q", d.Key, d.Kind)
	}
	for _, c := range []string{d.Color, d.Color2} {
		if c != "" && !colorPattern.MatchString(c) {
			return d, fmt.Errorf("plot %s: colour %q is not #rrggbb", d.Key, c)
		}
	}
	if d.LineWidth < 0 || d.LineWidth > 4 {
		return d, fmt.Errorf("plot %s: line width %d is not 1-4", d.Key, d.LineWidth)
	}
	switch d.LineStyle {
	case "", Solid, Dashed, Dotted:
	default:
		return d, fmt.Errorf("plot %s: unknown line style %q", d.Key, d.LineStyle)
	}
	if d.Precision < 0 || d.Precision > 10 {
		return d, fmt.Errorf("plot %s: precision %d is not 0-10", d.Key, d.Precision)
	}
	if len(d.Levels) > MaxLevels {
		return d, fmt.Errorf("plot %s: more than %d levels", d.Key, MaxLevels)
	}
	for _, l := range d.Levels {
		if !finite(l.Value) {
			return d, fmt.Errorf("plot %s: level %v is not a number", d.Key, l.Value)
		}
		if utf8.RuneCountInString(l.Title) > MaxTextLen {
			return d, fmt.Errorf("plot %s: level title longer than %d characters", d.Key, MaxTextLen)
		}
	}
	if d.Kind == Markers {
		m := Marker{Shape: Circle, Position: Above}
		if d.Marker != nil {
			m = *d.Marker
		}
		if m.Shape == "" {
			m.Shape = Circle
		}
		if m.Position == "" {
			m.Position = Above
		}
		switch m.Shape {
		case ArrowUp, ArrowDown, Circle, Square:
		default:
			return d, fmt.Errorf("plot %s: unknown marker shape %q", d.Key, m.Shape)
		}
		switch m.Position {
		case Above, Below, AtValue:
		default:
			return d, fmt.Errorf("plot %s: unknown marker position %q", d.Key, m.Position)
		}
		d.Marker = &m
	} else {
		d.Marker = nil
	}
	return d, nil
}

// Recorder holds a strategy's plots and the points waiting to be sent. The SDK
// owns one; the adapter that carries plots to the platform drains it.
//
// Until a carrier activates it, adding a value does nothing, so plotting costs
// nothing where plots go nowhere.
type Recorder struct {
	active atomic.Bool

	mu       sync.Mutex
	series   map[string]*Series
	declared map[string]Def // each plot as first declared, for re-declaration
	defs     map[string]Def // as sent: Timeframe filled in by the first Add
	panes    map[string]bool
	pending  []string // keys whose declaration is still to be sent, in order
	points   []Point
	dropped  bool // the buffer overflowed and this was reported
	warned   map[string]bool
}

// NewRecorder returns an inactive recorder.
func NewRecorder() *Recorder {
	return &Recorder{
		series:   make(map[string]*Series),
		declared: make(map[string]Def),
		defs:     make(map[string]Def),
		panes:    make(map[string]bool),
		warned:   make(map[string]bool),
	}
}

// Activate starts recording values. Called by the carrier that will send them.
func (r *Recorder) Activate() { r.active.Store(true) }

// Active reports whether values are being recorded.
func (r *Recorder) Active() bool { return r.active.Load() }

// Declare registers a plot. Declaring the same key again with the same Def
// returns the same series, so a strategy that builds one bot per pair can let
// each bot declare its plots; a different Def under a used key is an error.
func (r *Recorder) Declare(d Def) (*Series, error) {
	if r == nil {
		return nil, fmt.Errorf("plots: no recorder")
	}
	n, err := d.Normalize()
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.series[n.Key]; ok {
		if reflect.DeepEqual(r.declared[n.Key], n) {
			return s, nil
		}
		return nil, fmt.Errorf("plot %s is already declared differently", n.Key)
	}
	if len(r.series) >= MaxPlots {
		return nil, fmt.Errorf("plot %s: a session has at most %d plots", n.Key, MaxPlots)
	}
	if n.Pane != "" && !r.panes[n.Pane] && len(r.panes) >= MaxPanes {
		return nil, fmt.Errorf("plot %s: a session has at most %d panels", n.Key, MaxPanes)
	}
	if n.Pane != "" {
		r.panes[n.Pane] = true
	}
	s := &Series{r: r, key: n.Key, kind: n.Kind}
	r.series[n.Key] = s
	r.declared[n.Key] = n
	r.defs[n.Key] = n
	r.pending = append(r.pending, n.Key)
	return s, nil
}

// Pending reports whether anything is waiting to be sent.
func (r *Recorder) Pending() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending) > 0 || len(r.points) > 0
}

// Take removes and returns what is waiting to be sent, declarations first,
// keeping the batch's encoded size under about maxBytes (0 means no limit).
// What does not fit stays for the next call. Nil means nothing is waiting.
func (r *Recorder) Take(maxBytes int) *Batch {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) == 0 && len(r.points) == 0 {
		return nil
	}

	const envelope = 32 // {"defs":[],"points":[]}
	size := envelope
	b := &Batch{}
	nd := 0
	for _, key := range r.pending {
		d := r.defs[key]
		enc, _ := json.Marshal(d)
		if maxBytes > 0 && size+len(enc)+1 > maxBytes && (nd > 0 || len(b.Points) > 0) {
			break
		}
		size += len(enc) + 1
		b.Defs = append(b.Defs, d)
		nd++
	}
	r.pending = r.pending[nd:]

	np := 0
	for i := range r.points {
		ps := pointSize(&r.points[i])
		if maxBytes > 0 && size+ps > maxBytes && (nd > 0 || np > 0) {
			break
		}
		size += ps
		np++
	}
	if np > 0 {
		b.Points = append([]Point(nil), r.points[:np]...)
		r.points = r.points[np:]
		if len(r.points) == 0 {
			r.points = nil // let a large backlog's array go
		}
	}
	return b
}

// Return puts a batch that could not be sent back in front of what is
// waiting, so it goes out first next time.
func (r *Recorder) Return(b *Batch) {
	if r == nil || b.Empty() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(b.Defs) > 0 {
		keys := make([]string, 0, len(b.Defs)+len(r.pending))
		seen := make(map[string]bool, len(b.Defs))
		for _, d := range b.Defs {
			keys = append(keys, d.Key)
			seen[d.Key] = true
		}
		for _, k := range r.pending {
			if !seen[k] {
				keys = append(keys, k)
			}
		}
		r.pending = keys
	}
	if len(b.Points) > 0 {
		r.points = append(append(make([]Point, 0, len(b.Points)+len(r.points)), b.Points...), r.points...)
	}
}

// pointSize is a ceiling on a point's encoded size. Cheap on purpose: it runs
// for every point, and a ceiling is all a size limit needs.
func pointSize(p *Point) int {
	n := 100 + len(p.Key) + len(p.Exchange) + len(p.Pair)
	if p.Value2 != nil {
		n += 32
	}
	if p.Text != "" {
		n += 8 + 6*len(p.Text) // \u00XX, the worst escaping
	}
	return n
}

func (r *Recorder) add(s *Series, tf types.Timeframe, p Point) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if tf != "" {
		if d := r.defs[s.key]; d.Timeframe == "" {
			d.Timeframe = tf
			r.defs[s.key] = d
			if !contains(r.pending, s.key) {
				r.pending = append(r.pending, s.key)
			}
		}
	}
	if len(r.points) >= maxBuffered {
		if !r.dropped {
			r.dropped = true
			log.Printf("plots: %d points are waiting to be sent; dropping new ones", maxBuffered)
		}
		return
	}
	r.points = append(r.points, p)
}

// warnOnce logs a misuse of one plot the first time it happens.
func (r *Recorder) warnOnce(key, format string, args ...any) {
	r.mu.Lock()
	seen := r.warned[key]
	r.warned[key] = true
	r.mu.Unlock()
	if !seen {
		log.Printf("plots: "+format, args...)
	}
}

func contains(keys []string, k string) bool {
	for _, x := range keys {
		if x == k {
			return true
		}
	}
	return false
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// Series is one declared plot. A nil *Series ignores every call.
type Series struct {
	r    *Recorder
	key  string
	kind Kind
}

// Key is the plot's key.
func (s *Series) Key() string {
	if s == nil {
		return ""
	}
	return s.key
}

// Add records v for the candle's bar. On a Markers plot it places a marker at
// v (see Mark for one with text).
//
// NaN and infinite values are skipped, so an indicator that is still warming
// up can be added unchecked.
func (s *Series) Add(c *types.Candle, v float64) {
	if s == nil || c == nil || !s.r.Active() {
		return
	}
	s.addAt(c.Exchange, c.Symbol, c.OpenTime, c.Timeframe, v, nil, "")
}

// AddAt records v for the bar opening at t. Use it when the strategy keeps its
// own bars rather than the SDK's candles, and set Def.Timeframe.
func (s *Series) AddAt(exchange, pair string, t time.Time, v float64) {
	if s == nil || !s.r.Active() {
		return
	}
	s.addAt(exchange, pair, t, "", v, nil, "")
}

// AddBand records a Band plot's upper and lower values for the candle's bar.
func (s *Series) AddBand(c *types.Candle, upper, lower float64) {
	if s == nil || c == nil || !s.r.Active() {
		return
	}
	s.addBand(c.Exchange, c.Symbol, c.OpenTime, c.Timeframe, upper, lower)
}

// AddBandAt records a Band plot's values for the bar opening at t.
func (s *Series) AddBandAt(exchange, pair string, t time.Time, upper, lower float64) {
	if s == nil || !s.r.Active() {
		return
	}
	s.addBand(exchange, pair, t, "", upper, lower)
}

// Mark places a marker on the candle's bar, with optional text (at most 32
// characters; longer is cut). A marker positioned AtValue sits at the close.
func (s *Series) Mark(c *types.Candle, text string) {
	if s == nil || c == nil || !s.r.Active() {
		return
	}
	s.mark(c.Exchange, c.Symbol, c.OpenTime, c.Timeframe, c.Close, text)
}

// MarkAt places a marker on the bar opening at t. price is where a marker
// positioned AtValue sits; the others ignore it.
func (s *Series) MarkAt(exchange, pair string, t time.Time, price float64, text string) {
	if s == nil || !s.r.Active() {
		return
	}
	s.mark(exchange, pair, t, "", price, text)
}

func (s *Series) addBand(exchange, pair string, t time.Time, tf types.Timeframe, upper, lower float64) {
	if s.kind != Band {
		s.r.warnOnce(s.key, "plot %s is a %s plot; AddBand needs a band", s.key, s.kind)
		return
	}
	if !finite(lower) {
		return
	}
	s.addAt(exchange, pair, t, tf, upper, &lower, "")
}

func (s *Series) mark(exchange, pair string, t time.Time, tf types.Timeframe, price float64, text string) {
	if s.kind != Markers {
		s.r.warnOnce(s.key, "plot %s is a %s plot; Mark needs a markers plot", s.key, s.kind)
		return
	}
	if !finite(price) {
		price = 0
	}
	if utf8.RuneCountInString(text) > MaxTextLen {
		text = string([]rune(text)[:MaxTextLen])
	}
	s.addAt(exchange, pair, t, tf, price, nil, text)
}

func (s *Series) addAt(exchange, pair string, t time.Time, tf types.Timeframe, v float64, v2 *float64, text string) {
	if s.kind == Band && v2 == nil {
		s.r.warnOnce(s.key, "plot %s is a band; add its values with AddBand", s.key)
		return
	}
	if !finite(v) {
		return
	}
	if exchange == "" || pair == "" || t.IsZero() {
		s.r.warnOnce(s.key, "plot %s: a value needs an exchange, a pair and a bar time; skipped", s.key)
		return
	}
	s.r.add(s, tf, Point{
		Key:      s.key,
		Exchange: exchange,
		Pair:     pair,
		Time:     t.UnixMilli(),
		Value:    v,
		Value2:   v2,
		Text:     text,
	})
}
