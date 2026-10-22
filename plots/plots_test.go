package plots

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/kdraigo/dev_sdk/types"
)

var bar = &types.Candle{
	Exchange:  "binance_futures",
	Symbol:    "BTC/USDT",
	Timeframe: types.Timeframe1h,
	OpenTime:  time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC),
	Close:     65000,
}

func active() *Recorder {
	r := NewRecorder()
	r.Activate()
	return r
}

func TestNilSeriesIgnoresEverything(t *testing.T) {
	var s *Series
	s.Add(bar, 1)
	s.AddAt("x", "p", time.Now(), 1)
	s.AddBand(bar, 2, 1)
	s.AddBandAt("x", "p", time.Now(), 2, 1)
	s.Mark(bar, "hi")
	s.MarkAt("x", "p", time.Now(), 1, "hi")
	if s.Key() != "" {
		t.Fatal("nil series has a key")
	}
}

func TestInactiveRecorderRecordsNothing(t *testing.T) {
	r := NewRecorder()
	s, err := r.Declare(Def{Key: "ema"})
	if err != nil {
		t.Fatal(err)
	}
	s.Add(bar, 1)
	b := r.Take(0)
	if len(b.Points) != 0 {
		t.Fatalf("inactive recorder kept %d points", len(b.Points))
	}
	if len(b.Defs) != 1 {
		t.Fatalf("declaration not kept: %+v", b)
	}
}

func TestDeclareDefaultsAndValidation(t *testing.T) {
	r := active()
	s, err := r.Declare(Def{Key: "cross", Kind: Markers})
	if err != nil {
		t.Fatal(err)
	}
	d := r.Take(0).Defs[0]
	if d.Title != "cross" || d.Marker == nil || d.Marker.Shape != Circle || d.Marker.Position != Above {
		t.Fatalf("defaults not filled: %+v", d)
	}
	if s.Key() != "cross" {
		t.Fatal("key")
	}

	bad := []Def{
		{Key: ""},
		{Key: "Upper"},
		{Key: strings.Repeat("a", MaxKeyLen+1)},
		{Key: "a", Kind: "candles"},
		{Key: "a", Color: "red"},
		{Key: "a", LineWidth: 5},
		{Key: "a", LineStyle: "wavy"},
		{Key: "a", Precision: 11},
		{Key: "a", Levels: []Level{{Value: math.NaN()}}},
		{Key: "a", Levels: make([]Level, MaxLevels+1)},
		{Key: "a", Kind: Markers, Marker: &Marker{Shape: "star"}},
		{Key: "a", Pane: strings.Repeat("p", MaxPaneLen+1)},
	}
	for _, d := range bad {
		if _, err := r.Declare(d); err == nil {
			t.Errorf("Declare(%+v) accepted", d)
		}
	}
}

func TestRedeclare(t *testing.T) {
	r := active()
	a, err := r.Declare(Def{Key: "ema", Title: "EMA 50"})
	if err != nil {
		t.Fatal(err)
	}
	a.Add(bar, 1) // fills Timeframe in; a second bot's declaration must still match
	b, err := r.Declare(Def{Key: "ema", Title: "EMA 50"})
	if err != nil || a != b {
		t.Fatalf("identical re-declaration: %v, same=%v", err, a == b)
	}
	if _, err := r.Declare(Def{Key: "ema", Title: "EMA 20"}); err == nil {
		t.Fatal("a different declaration under a used key was accepted")
	}
}

func TestPlotAndPaneLimits(t *testing.T) {
	r := active()
	for i := range MaxPanes {
		if _, err := r.Declare(Def{Key: fmt.Sprintf("p%d", i), Pane: fmt.Sprintf("P%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Declare(Def{Key: "one_more_pane", Pane: "extra"}); err == nil {
		t.Fatal("a ninth panel was accepted")
	}
	if _, err := r.Declare(Def{Key: "same_pane", Pane: "P0"}); err != nil {
		t.Fatalf("an existing panel was refused: %v", err)
	}
	for i := len(r.series); i < MaxPlots; i++ {
		if _, err := r.Declare(Def{Key: fmt.Sprintf("o%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Declare(Def{Key: "too_many"}); err == nil {
		t.Fatal("plot 33 was accepted")
	}
}

func TestAddSkipsNonFiniteAndFillsTimeframe(t *testing.T) {
	r := active()
	s, _ := r.Declare(Def{Key: "ema"})
	s.Add(bar, math.NaN())
	s.Add(bar, math.Inf(1))
	s.Add(bar, 64000.5)
	b := r.Take(0)
	if len(b.Points) != 1 {
		t.Fatalf("got %d points, want 1", len(b.Points))
	}
	p := b.Points[0]
	if p.Key != "ema" || p.Exchange != "binance_futures" || p.Pair != "BTC/USDT" ||
		p.Time != bar.OpenTime.UnixMilli() || p.Value != 64000.5 {
		t.Fatalf("point %+v", p)
	}
	if len(b.Defs) != 1 || b.Defs[0].Timeframe != types.Timeframe1h {
		t.Fatalf("timeframe not filled in: %+v", b.Defs)
	}
	// The whole batch must encode: a NaN would make json.Marshal fail.
	if _, err := json.Marshal(b); err != nil {
		t.Fatal(err)
	}
}

func TestTimeframeFilledLaterIsResent(t *testing.T) {
	r := active()
	s, _ := r.Declare(Def{Key: "ema"})
	if b := r.Take(0); len(b.Defs) != 1 || b.Defs[0].Timeframe != "" {
		t.Fatalf("first take %+v", b)
	}
	s.Add(bar, 1)
	b := r.Take(0)
	if len(b.Defs) != 1 || b.Defs[0].Timeframe != types.Timeframe1h {
		t.Fatalf("filled declaration not re-sent: %+v", b.Defs)
	}
	s.Add(bar, 2)
	if b := r.Take(0); len(b.Defs) != 0 {
		t.Fatalf("declaration re-sent without a change: %+v", b.Defs)
	}
}

func TestBandAndMarkers(t *testing.T) {
	r := active()
	band, _ := r.Declare(Def{Key: "bb", Kind: Band})
	marks, _ := r.Declare(Def{Key: "cross", Kind: Markers, Marker: &Marker{Shape: ArrowUp, Position: Below}})
	band.Add(bar, 5) // wrong method for a band: ignored
	band.AddBand(bar, 6, 4)
	band.AddBand(bar, 6, math.NaN())
	marks.Mark(bar, strings.Repeat("x", 40))
	marks.AddBand(bar, 1, 0) // wrong method: ignored
	b := r.Take(0)
	if len(b.Points) != 2 {
		t.Fatalf("got %d points: %+v", len(b.Points), b.Points)
	}
	if p := b.Points[0]; p.Value != 6 || p.Value2 == nil || *p.Value2 != 4 {
		t.Fatalf("band point %+v", p)
	}
	if p := b.Points[1]; len(p.Text) != MaxTextLen || p.Value != bar.Close {
		t.Fatalf("marker point %+v", p)
	}
}

func TestAddAtNeedsIdentity(t *testing.T) {
	r := active()
	s, _ := r.Declare(Def{Key: "x", Timeframe: types.Timeframe4h})
	s.AddAt("", "BTC/USDT", bar.OpenTime, 1)
	s.AddAt("binance", "", bar.OpenTime, 1)
	s.AddAt("binance", "BTC/USDT", time.Time{}, 1)
	s.AddAt("binance", "BTC/USDT", bar.OpenTime, 1)
	if b := r.Take(0); len(b.Points) != 1 {
		t.Fatalf("got %d points", len(b.Points))
	}
}

func TestTakeRespectsSizeAndKeepsOrder(t *testing.T) {
	r := active()
	s, _ := r.Declare(Def{Key: "ema"})
	const n = 5000
	for i := range n {
		s.AddAt("binance", "BTC/USDT", bar.OpenTime.Add(time.Duration(i)*time.Hour), float64(i)+0.123456789)
	}
	const limit = 48 << 10
	var got []Point
	batches := 0
	for b := r.Take(limit); b != nil; b = r.Take(limit) {
		enc, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		if len(enc) > limit {
			t.Fatalf("batch of %d bytes is over %d", len(enc), limit)
		}
		got = append(got, b.Points...)
		batches++
	}
	if len(got) != n {
		t.Fatalf("got %d points, want %d", len(got), n)
	}
	for i, p := range got {
		if p.Value != float64(i)+0.123456789 {
			t.Fatalf("point %d out of order: %v", i, p.Value)
		}
	}
	if batches < 2 {
		t.Fatalf("%d batches; the size limit never applied", batches)
	}
}

func TestReturnGoesFirst(t *testing.T) {
	r := active()
	s, _ := r.Declare(Def{Key: "ema"})
	s.Add(bar, 1)
	b := r.Take(0)
	s.Add(bar, 2)
	r.Return(b)
	again := r.Take(0)
	if len(again.Defs) != 1 || len(again.Points) != 2 || again.Points[0].Value != 1 || again.Points[1].Value != 2 {
		t.Fatalf("returned batch not first: %+v", again)
	}
	if r.Take(0) != nil {
		t.Fatal("something left over")
	}
}

func TestBufferIsBounded(t *testing.T) {
	r := active()
	s, _ := r.Declare(Def{Key: "ema"})
	for i := range maxBuffered + 10 {
		s.AddAt("binance", "BTC/USDT", bar.OpenTime.Add(time.Duration(i)*time.Minute), 1)
	}
	if got := len(r.Take(0).Points); got != maxBuffered {
		t.Fatalf("buffer holds %d points, want %d", got, maxBuffered)
	}
}

func TestRedeclareQueuesEveryDeclarationAgain(t *testing.T) {
	r := active()
	r.Declare(Def{Key: "b"})
	r.Declare(Def{Key: "a"})
	r.Take(0)
	if r.Take(0) != nil {
		t.Fatal("nothing should wait")
	}
	r.Redeclare()
	b := r.Take(0)
	if b == nil || len(b.Defs) != 2 || b.Defs[0].Key != "b" || b.Defs[1].Key != "a" {
		t.Fatalf("redeclared %+v, want b then a", b)
	}
	r.Redeclare()
	r.Redeclare()
	if b := r.Take(0); len(b.Defs) != 2 {
		t.Fatalf("redeclaring twice queued %d", len(b.Defs))
	}
}
