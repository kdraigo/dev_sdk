package live

import (
	"context"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	hl "github.com/sonirico/go-hyperliquid"

	"github.com/kdraigo/dev_sdk/types"
)

// marketSlippage is the default slippage tolerance applied to Hyperliquid
// market orders (5%). Hyperliquid has no true "market" order — MarketOpen
// submits an aggressive IOC limit priced off the mid with this allowance.
const marketSlippage = 0.05

// HyperliquidClient is the live SDK adapter for the Hyperliquid DEX (spot).
// Unlike Binance/Bybit, Hyperliquid authenticates with an EVM wallet and signs
// L1 actions (EIP-712); all signing is delegated to github.com/sonirico/go-hyperliquid.
type HyperliquidClient struct {
	config *types.Config

	ex   *hl.Exchange
	info *hl.Info

	accountAddr string // master EVM account whose funds are traded

	// spotCoins maps a base ticker (e.g. "BTC", "UBTC") to the Hyperliquid spot
	// coin id (e.g. "@142"). szByCoin maps that coin id to the base token's
	// size-decimals for order-size rounding. Built once in PrepareSession.
	spotCoins map[string]string
	szByCoin  map[string]int
}

func NewHyperliquidClient(cfg *types.Config) *HyperliquidClient {
	return &HyperliquidClient{
		config:    cfg,
		spotCoins: make(map[string]string),
		szByCoin:  make(map[string]int),
	}
}

func (h *HyperliquidClient) baseURL() string {
	if h.config.Environment == types.EnvTestHyperliquid {
		return hl.TestnetAPIURL
	}
	return hl.MainnetAPIURL
}

func (h *HyperliquidClient) PrepareSession(ctx context.Context, cfg *types.Config) error {
	log.Println("Hyperliquid: preparing session...")

	pk, err := crypto.HexToECDSA(strings.TrimPrefix(cfg.Credentials.WalletSecretKey, "0x"))
	if err != nil {
		return fmt.Errorf("hyperliquid: invalid WalletSecretKey: %w", err)
	}

	// The account address is the master wallet that owns the funds. When an
	// agent wallet signs, this must be set explicitly; otherwise default to the
	// signer's own address.
	h.accountAddr = cfg.Credentials.WalletAddress
	if h.accountAddr == "" {
		h.accountAddr = crypto.PubkeyToAddress(pk.PublicKey).Hex()
	}

	baseURL := h.baseURL()
	h.info = hl.NewInfo(ctx, baseURL, true, nil, nil, nil)

	meta, err := h.info.Meta(ctx)
	if err != nil {
		return fmt.Errorf("hyperliquid: fetch meta: %w", err)
	}
	spotMeta, err := h.info.SpotMeta(ctx)
	if err != nil {
		return fmt.Errorf("hyperliquid: fetch spotMeta: %w", err)
	}
	h.buildSpotResolver(spotMeta)

	h.ex = hl.NewExchange(ctx, pk, baseURL, meta, "", h.accountAddr, spotMeta, nil)

	// Validate connectivity + credentials by reading spot balances.
	state, err := h.info.SpotUserState(ctx, h.accountAddr)
	if err != nil {
		return fmt.Errorf("hyperliquid: validate account %s: %w", h.accountAddr, err)
	}
	log.Printf("Hyperliquid: account %s validated (%d spot balances)", h.accountAddr, len(state.Balances))
	return nil
}

// buildSpotResolver maps config tickers to Hyperliquid spot coin ids, restricted
// to USDC-quoted markets. Hyperliquid wraps some assets with a "U" prefix
// (UBTC, UETH, USOL); both the wrapped name and its de-wrapped alias resolve.
func (h *HyperliquidClient) buildSpotResolver(spotMeta *hl.SpotMeta) {
	tokensByIdx := make(map[int]hl.SpotTokenInfo, len(spotMeta.Tokens))
	for _, t := range spotMeta.Tokens {
		tokensByIdx[t.Index] = t
	}

	for _, u := range spotMeta.Universe {
		if len(u.Tokens) < 2 {
			continue
		}
		base := tokensByIdx[u.Tokens[0]]
		quote := tokensByIdx[u.Tokens[1]]
		if !strings.EqualFold(quote.Name, "USDC") || base.Name == "" {
			continue
		}
		baseUp := strings.ToUpper(base.Name)
		if _, ok := h.spotCoins[baseUp]; !ok {
			h.spotCoins[baseUp] = u.Name
		}
		if len(baseUp) > 1 && baseUp[0] == 'U' {
			if dw := baseUp[1:]; h.spotCoins[dw] == "" {
				h.spotCoins[dw] = u.Name
			}
		}
		h.szByCoin[u.Name] = base.SzDecimals
	}
}

// resolveCoin maps a configured symbol (e.g. "BTCUSDT", "BTC/USDC", "@142") to
// the Hyperliquid spot coin id used for orders and candle subscriptions.
func (h *HyperliquidClient) resolveCoin(symbol string) (string, error) {
	base := baseTicker(symbol)
	if strings.HasPrefix(base, "@") || strings.Contains(base, "/") {
		return base, nil // already a spot coin id / pair name
	}
	if coin, ok := h.spotCoins[base]; ok {
		return coin, nil
	}
	return "", fmt.Errorf("hyperliquid: no USDC spot market for symbol %q (base %q)", symbol, base)
}

func (h *HyperliquidClient) ConnectStream(ctx context.Context, candleChan chan<- *types.Candle, orderChan chan<- *types.Order) error {
	log.Println("Hyperliquid: connecting WebSocket streams...")

	ws := hl.NewWebsocketClient(h.baseURL())
	if err := ws.Connect(ctx); err != nil {
		return fmt.Errorf("hyperliquid: ws connect: %w", err)
	}

	assets := []string{}
	if h.config.Live != nil {
		assets = h.config.Live.Assets
	}

	// ── Candle streams (one subscription per asset) ──────────────────────────
	for _, asset := range assets {
		coin, err := h.resolveCoin(asset)
		if err != nil {
			log.Printf("Hyperliquid: skip candle stream: %v", err)
			continue
		}
		origSym := asset

		// Hyperliquid pushes the in-progress candle repeatedly with no explicit
		// "final" flag. Emit a candle as complete only once the next candle
		// opens (rollover), guaranteeing one closed candle per minute.
		var mu sync.Mutex
		var prev *hl.Candle

		if _, err := ws.Candles(hl.CandlesSubscriptionParams{Coin: coin, Interval: "1m"},
			func(c hl.Candle, err error) {
				if err != nil {
					log.Printf("Hyperliquid: candle stream error for %s: %v", origSym, err)
					return
				}
				mu.Lock()
				if prev != nil && c.TimeOpen > prev.TimeOpen {
					candleChan <- hlToCandle(prev, origSym)
				}
				cc := c
				prev = &cc
				mu.Unlock()
			}); err != nil {
			log.Printf("Hyperliquid: subscribe candles for %s: %v", origSym, err)
		}
	}

	// ── Order updates ────────────────────────────────────────────────────────
	if _, err := ws.OrderUpdates(hl.OrderUpdatesSubscriptionParams{User: h.accountAddr},
		func(orders []hl.WsOrder, err error) {
			if err != nil {
				log.Printf("Hyperliquid: order stream error: %v", err)
				return
			}
			for _, o := range orders {
				orderChan <- mapHyperliquidOrder(o)
			}
		}); err != nil {
		log.Printf("Hyperliquid: subscribe order updates: %v", err)
	}

	<-ctx.Done()
	_ = ws.Close()
	return nil
}

// hlToCandle converts a Hyperliquid WS/REST candle to the SDK Candle type.
func hlToCandle(c *hl.Candle, origSym string) *types.Candle {
	open, _ := strconv.ParseFloat(c.Open, 64)
	high, _ := strconv.ParseFloat(c.High, 64)
	low, _ := strconv.ParseFloat(c.Low, 64)
	closeVal, _ := strconv.ParseFloat(c.Close, 64)
	volume, _ := strconv.ParseFloat(c.Volume, 64)

	return &types.Candle{
		Symbol:     origSym,
		Exchange:   "hyperliquid",
		Timeframe:  types.Timeframe(c.Interval),
		OpenTime:   time.UnixMilli(c.TimeOpen),
		CloseTime:  time.UnixMilli(c.TimeClose),
		Open:       open,
		High:       high,
		Low:        low,
		Close:      closeVal,
		Volume:     volume,
		IsComplete: true,
	}
}

// mapHyperliquidOrder converts a WS order update to the SDK Order type.
func mapHyperliquidOrder(o hl.WsOrder) *types.Order {
	b := o.Order
	side := types.OrderSideBuy
	if strings.EqualFold(b.Side, "A") || strings.EqualFold(b.Side, "sell") {
		side = types.OrderSideSell
	}

	price, _ := strconv.ParseFloat(b.LimitPx, 64)
	origSz, _ := strconv.ParseFloat(b.OrigSz, 64)
	remaining, _ := strconv.ParseFloat(b.Sz, 64)

	return &types.Order{
		ID:        strconv.FormatInt(b.Oid, 10),
		Symbol:    b.Coin,
		Exchange:  "hyperliquid",
		Side:      side,
		Type:      types.OrderTypeLimit,
		Status:    mapHyperliquidStatus(o.Status),
		Price:     price,
		Quantity:  origSz,
		FilledQty: math.Max(0, origSz-remaining),
		FeeAsset:  "USDC",
		CreatedAt: time.UnixMilli(b.Timestamp),
		UpdatedAt: time.UnixMilli(o.StatusTimestamp),
	}
}

func mapHyperliquidStatus(s hl.OrderStatusValue) types.OrderStatus {
	v := strings.ToLower(string(s))
	switch {
	case strings.Contains(v, "filled"):
		return types.OrderStatusFilled
	case strings.Contains(v, "cancel"):
		return types.OrderStatusCanceled
	case strings.Contains(v, "reject"):
		return types.OrderStatusRejected
	case v == "open":
		return types.OrderStatusNew
	default:
		return types.OrderStatusNew
	}
}

func (h *HyperliquidClient) PlaceOrder(ctx context.Context, req *types.OrderRequest) (*types.Order, error) {
	coin, err := h.resolveCoin(req.Symbol)
	if err != nil {
		return nil, err
	}
	isBuy := req.Side != types.OrderSideSell

	size := req.Quantity
	if dec, ok := h.szByCoin[coin]; ok {
		size = roundToStep(size, math.Pow10(-dec))
	}

	var status hl.OrderStatus
	if req.Type == types.OrderTypeMarket {
		status, err = h.ex.MarketOpen(ctx, coin, isBuy, size, nil, marketSlippage, nil, nil)
	} else {
		status, err = h.ex.Order(ctx, hl.CreateOrderRequest{
			Coin:      coin,
			IsBuy:     isBuy,
			Price:     req.Price,
			Size:      size,
			OrderType: hl.OrderType{Limit: &hl.LimitOrderType{Tif: hl.TifGtc}},
		}, nil)
	}
	if err != nil {
		return nil, err
	}
	if status.Error != nil {
		return nil, fmt.Errorf("hyperliquid: order rejected: %s", *status.Error)
	}

	order := &types.Order{
		Symbol:    req.Symbol,
		Exchange:  "hyperliquid",
		Side:      req.Side,
		Type:      req.Type,
		Status:    types.OrderStatusNew,
		Price:     req.Price,
		Quantity:  req.Quantity,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	switch {
	case status.Filled != nil:
		order.ID = strconv.Itoa(status.Filled.Oid)
		order.Status = types.OrderStatusFilled
		order.FilledQty, _ = strconv.ParseFloat(status.Filled.TotalSz, 64)
		order.AveragePrice, _ = strconv.ParseFloat(status.Filled.AvgPx, 64)
	case status.Resting != nil:
		order.ID = strconv.FormatInt(status.Resting.Oid, 10)
	}
	return order, nil
}

func (h *HyperliquidClient) CancelOrder(ctx context.Context, exchange, symbol, id string) error {
	coin, err := h.resolveCoin(symbol)
	if err != nil {
		return err
	}
	oid, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return fmt.Errorf("hyperliquid: invalid order id %q: %w", id, err)
	}
	_, err = h.ex.Cancel(ctx, coin, oid)
	return err
}

func (h *HyperliquidClient) GetAccount(ctx context.Context, exchange string, asset string) (*types.Account, error) {
	state, err := h.info.SpotUserState(ctx, h.accountAddr)
	if err != nil {
		return nil, err
	}
	acc := &types.Account{Exchange: "hyperliquid"}
	for _, b := range state.Balances {
		if asset != "" && !strings.EqualFold(b.Coin, asset) {
			continue
		}
		total, _ := strconv.ParseFloat(b.Total, 64)
		hold, _ := strconv.ParseFloat(b.Hold, 64)
		acc.Balances = append(acc.Balances, types.Balance{
			Asset: b.Coin,
			Free:  math.Max(0, total-hold),
			Lock:  hold,
		})
	}
	return acc, nil
}

func (h *HyperliquidClient) Next(ctx context.Context) error { return nil }

// GetHistoricalCandles fetches closed candles via Hyperliquid's candleSnapshot.
// NOTE: Hyperliquid only retains a rolling recent window (~3.5 days for 1m), so
// deep indicator warmup history is unavailable — older ranges return empty.
func (h *HyperliquidClient) GetHistoricalCandles(ctx context.Context, exchange, symbol string, from, to time.Time, tf types.Timeframe) ([]*types.Candle, error) {
	if h.info == nil {
		h.info = hl.NewInfo(ctx, h.baseURL(), true, nil, nil, nil)
		if len(h.spotCoins) == 0 {
			if spotMeta, err := h.info.SpotMeta(ctx); err == nil {
				h.buildSpotResolver(spotMeta)
			}
		}
	}
	if from.After(to) {
		return nil, fmt.Errorf("hyperliquid: from %s is after to %s", from, to)
	}
	coin, err := h.resolveCoin(symbol)
	if err != nil {
		return nil, err
	}

	candles, err := h.info.CandlesSnapshot(ctx, coin, string(tf), from.UnixMilli(), to.UnixMilli())
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: candleSnapshot: %w", err)
	}

	out := make([]*types.Candle, 0, len(candles))
	for i := range candles {
		c := candles[i]
		sdkC := hlToCandle(&c, symbol)
		sdkC.Timeframe = tf
		out = append(out, sdkC)
	}
	return out, nil
}

// baseTicker extracts the base asset from a configured symbol, e.g.
// "BTCUSDT" -> "BTC", "ETHUSDT" -> "ETH". Spot-id ("@142") and pair-name
// ("UBTC/USDC") symbols are returned unchanged for passthrough.
func baseTicker(symbol string) string {
	if strings.HasPrefix(symbol, "@") || strings.Contains(symbol, "/") {
		return symbol
	}
	s := strings.ToUpper(strings.TrimSpace(symbol))
	for _, quote := range []string{"USDT0", "USDT", "USDC", "USDH", "USD"} {
		if strings.HasSuffix(s, quote) && len(s) > len(quote) {
			return strings.TrimSuffix(s, quote)
		}
	}
	return s
}
