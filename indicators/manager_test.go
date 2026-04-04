package indicators

import (
	"context"
	"testing"
	"time"

	"github.com/kdraigo/flow_v1/dev_sdk/types"
)

func TestIndicatorManager_Run(t *testing.T) {
	requested := []string{"EMA50", "RSI14"}
	im := NewIndicatorManager(requested)

	aggChan := make(chan *types.Candle, 10)

	// Create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sdkCtx := &types.Context{
		Ctx:           ctx,
		Cancel:        cancel,
		IndicatorsMap: make(map[string]float64),
	}

	receivedCandle := make(chan bool)
	onCandle := func(c *types.Context, candle *types.Candle) {
		// Verify indicators are set (currently stubs return 0.0)
		for _, name := range requested {
			val := c.GetIndicator(name)
			if val != 0.0 {
				t.Errorf("Expected stub indicator %s to be 0.0, got %f", name, val)
			}
		}
		receivedCandle <- true
	}

	go im.Run(sdkCtx, aggChan, onCandle)

	candle := &types.Candle{
		Symbol:    "BTC/USDT",
		Close:     50000,
		CloseTime: time.Now(),
	}
	aggChan <- candle

	select {
	case <-receivedCandle:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for OnCandle callback")
	}
}
