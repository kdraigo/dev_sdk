package types

// Contract is a perpetual's trading rules and its mark price now: what an
// order checker (dev_sdk/orderplan) needs before it sizes an order. The
// futures adapters read it on demand for any symbol, not only the configured
// pairs.
type Contract struct {
	Exchange    string
	Symbol      string // the venue's, BTCUSDT
	TickSize    float64
	StepSize    float64
	MinQty      float64
	MinNotional float64
	// MaxLeverage is the exchange's highest leverage at its lowest risk tier;
	// 0 when it could not be read.
	MaxLeverage float64
	// MaintenanceMargin is the lowest risk tier's maintenance margin rate
	// (0.005 = 0.5%); 0 when it could not be read, and a checker then assumes
	// a higher one so its liquidation estimate errs near.
	MaintenanceMargin float64
	Mark              float64
}
