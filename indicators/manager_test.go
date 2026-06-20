package indicators

import (
	"testing"

	"github.com/kdraigo/dev_sdk/types"
)

func TestIndicatorManager_Prunes(t *testing.T) {
	const (
		maxPoints = 100
		exchange  = "binance"
		symbol    = "BTC/USDT"
		fed       = 250 // > 2*maxPoints so at least one prune happens
	)

	im := NewIndicatorManager([]string{exchange}, []string{symbol}, WithMaxPoints(maxPoints)).(*indicatorManager)

	for i := 0; i < fed; i++ {
		im.Update(&types.Candle{
			Exchange: exchange,
			Symbol:   symbol,
			Open:     float64(i),
			High:     float64(i),
			Low:      float64(i),
			Close:    float64(i),
			Volume:   float64(i),
		})
	}

	p := im.pairCandlePoints[exchange][symbol]

	// History stays bounded: never below the retention window, never above the
	// 2x prune trigger.
	if len(p.Close) < maxPoints || len(p.Close) >= 2*maxPoints {
		t.Fatalf("Close length %d out of bounds [%d, %d)", len(p.Close), maxPoints, 2*maxPoints)
	}

	// All series stay index-aligned after pruning.
	if !(len(p.Open) == len(p.Close) && len(p.High) == len(p.Close) &&
		len(p.Low) == len(p.Close) && len(p.Volume) == len(p.Close)) {
		t.Fatalf("series lengths diverged: O=%d H=%d L=%d C=%d V=%d",
			len(p.Open), len(p.High), len(p.Low), len(p.Close), len(p.Volume))
	}

	// The most recent candle is preserved (we feed Close=i, so last must be fed-1).
	if got := p.Close[len(p.Close)-1]; got != float64(fed-1) {
		t.Fatalf("latest Close = %v, want %v", got, float64(fed-1))
	}

	// Pruning drops the oldest: the retained head must be newer than candle 0.
	if got := p.Close[0]; got == 0 {
		t.Fatalf("oldest point not pruned: Close[0] = %v", got)
	}
}

func TestIndicatorManager_Run(t *testing.T) {
	// requested := []string{"EMA50", "RSI14"}
	// im := NewIndicatorManager(requested)

	// aggChan := make(chan *types.Candle, 10)

	// // Create context
	// ctx, cancel := context.WithCancel(context.Background())
	// defer cancel()

	// sdkCtx := &types.Context{
	// 	Ctx:           ctx,
	// 	Cancel:        cancel,
	// 	IndicatorsMap: make(map[string]float64),
	// }

	// receivedCandle := make(chan bool)
	// onCandle := func(c *types.Context, candle *types.Candle) {
	// 	// Verify indicators are set (currently stubs return 0.0)
	// 	for _, name := range requested {
	// 		val := c.GetIndicator(name)
	// 		if val != 0.0 {
	// 			t.Errorf("Expected stub indicator %s to be 0.0, got %f", name, val)
	// 		}
	// 	}
	// 	receivedCandle <- true
	// }

	// go im.Run(sdkCtx, aggChan, onCandle)

	// candle := &types.Candle{
	// 	Symbol:    "BTC/USDT",
	// 	Close:     50000,
	// 	CloseTime: time.Now(),
	// }
	// aggChan <- candle

	// select {
	// case <-receivedCandle:
	// 	// Success
	// case <-time.After(1 * time.Second):
	// 	t.Fatal("Timeout waiting for OnCandle callback")
	// }
}
