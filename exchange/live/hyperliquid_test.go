package live

import (
	"testing"

	hl "github.com/sonirico/go-hyperliquid"

	"github.com/kdraigo/dev_sdk/types"
)

func TestBaseTicker(t *testing.T) {
	cases := map[string]string{
		"BTCUSDT":   "BTC",
		"ETHUSDT":   "ETH",
		"HYPEUSDC":  "HYPE",
		"SOLUSD":    "SOL",
		"BTC":       "BTC",
		"@142":      "@142",      // spot-id passthrough
		"UBTC/USDC": "UBTC/USDC", // pair-name passthrough
	}
	for in, want := range cases {
		if got := baseTicker(in); got != want {
			t.Errorf("baseTicker(%q) = %q, want %q", in, got, want)
		}
	}
}

// sampleSpotMeta mirrors the shape of Hyperliquid's spotMeta: USDC is token 0,
// the wrapped assets carry "U" prefixes, and universe names are "@<index>"
// (except the special PURR/USDC pair).
func sampleSpotMeta() *hl.SpotMeta {
	return &hl.SpotMeta{
		Tokens: []hl.SpotTokenInfo{
			{Name: "USDC", Index: 0, SzDecimals: 2},
			{Name: "PURR", Index: 1, SzDecimals: 0},
			{Name: "HYPE", Index: 107, SzDecimals: 2},
			{Name: "UBTC", Index: 142, SzDecimals: 5},
			{Name: "UETH", Index: 151, SzDecimals: 4},
		},
		Universe: []hl.SpotAssetInfo{
			{Name: "PURR/USDC", Tokens: []int{1, 0}, Index: 0},
			{Name: "@107", Tokens: []int{107, 0}, Index: 107},
			{Name: "@142", Tokens: []int{142, 0}, Index: 142},
			{Name: "@151", Tokens: []int{151, 0}, Index: 151},
		},
	}
}

func TestBuildSpotResolverAndResolveCoin(t *testing.T) {
	h := NewHyperliquidClient(&types.Config{})
	h.buildSpotResolver(sampleSpotMeta())

	// Wrapped names and their de-wrapped aliases both resolve to the spot id.
	coinCases := map[string]string{
		"BTCUSDT":   "@142",
		"ETHUSDT":   "@151",
		"HYPEUSDC":  "@107",
		"UBTC":      "@142",
		"@142":      "@142",      // passthrough
		"PURR/USDC": "PURR/USDC", // passthrough
	}
	for in, want := range coinCases {
		got, err := h.resolveCoin(in)
		if err != nil {
			t.Errorf("resolveCoin(%q) error: %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("resolveCoin(%q) = %q, want %q", in, got, want)
		}
	}

	// szDecimals captured from the base token.
	if h.szByCoin["@142"] != 5 {
		t.Errorf("szByCoin[@142] = %d, want 5", h.szByCoin["@142"])
	}
	if h.szByCoin["@151"] != 4 {
		t.Errorf("szByCoin[@151] = %d, want 4", h.szByCoin["@151"])
	}

	// Unknown market errors instead of silently passing.
	if _, err := h.resolveCoin("DOGEUSDT"); err == nil {
		t.Error("resolveCoin(DOGEUSDT) expected error, got nil")
	}
}

func TestHlToCandle(t *testing.T) {
	c := &hl.Candle{
		TimeOpen: 1690000000000, TimeClose: 1690000059999, Interval: "1m",
		Open: "62341.0", High: "62370.0", Low: "62334.0", Close: "62370.0", Volume: "34.7",
	}
	got := hlToCandle(c, "BTCUSDT")
	if got.Symbol != "BTCUSDT" || got.Exchange != "hyperliquid" || !got.IsComplete {
		t.Errorf("unexpected candle identity: %+v", got)
	}
	if got.Open != 62341.0 || got.High != 62370.0 || got.Low != 62334.0 || got.Close != 62370.0 || got.Volume != 34.7 {
		t.Errorf("bad OHLCV mapping: %+v", got)
	}
	if got.OpenTime.UnixMilli() != 1690000000000 || got.CloseTime.UnixMilli() != 1690000059999 {
		t.Errorf("bad time mapping: %+v", got)
	}
}

func TestMapHyperliquidStatus(t *testing.T) {
	cases := map[hl.OrderStatusValue]types.OrderStatus{
		"open":           types.OrderStatusNew,
		"filled":         types.OrderStatusFilled,
		"canceled":       types.OrderStatusCanceled,
		"marginCanceled": types.OrderStatusCanceled,
		"rejected":       types.OrderStatusRejected,
	}
	for in, want := range cases {
		if got := mapHyperliquidStatus(in); got != want {
			t.Errorf("mapHyperliquidStatus(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestMapHyperliquidOrder(t *testing.T) {
	o := hl.WsOrder{
		Order: hl.WsBasicOrder{
			Coin: "@142", Side: "A", LimitPx: "62000.0", Sz: "0.2", OrigSz: "0.5", Oid: 12345, Timestamp: 1690000000000,
		},
		Status:          "open",
		StatusTimestamp: 1690000001000,
	}
	got := mapHyperliquidOrder(o)
	if got.ID != "12345" || got.Side != types.OrderSideSell || got.Exchange != "hyperliquid" {
		t.Errorf("unexpected order identity: %+v", got)
	}
	if got.Quantity != 0.5 || got.FilledQty != 0.3 { // origSz 0.5 - remaining 0.2
		t.Errorf("bad qty mapping: quantity=%v filled=%v", got.Quantity, got.FilledQty)
	}
	if got.Status != types.OrderStatusNew {
		t.Errorf("bad status: %v", got.Status)
	}
}
