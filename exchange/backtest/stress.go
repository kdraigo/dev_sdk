package backtest

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/kdraigo/dev_sdk/types"
)

// Stress settings the platform sets on a run (a cloud workspace's stress
// variants), over whatever the strategy's own config asks for: the same code,
// charged worse execution. Unset, nothing changes.
const (
	// EnvSlippageBps worsens every taker fill by this many basis points.
	EnvSlippageBps = "KDRAIGO_SIM_SLIPPAGE_BPS"
	// EnvFeeMult scales every fee, e.g. 2 for double.
	EnvFeeMult = "KDRAIGO_SIM_FEE_MULT"
	// EnvFunding set to "off" leaves funding out.
	EnvFunding = "KDRAIGO_SIM_FUNDING"
)

// withStress is the strategy's simulation options with the platform's stress
// settings over them, logged so the run's output says what it was charged.
func withStress(sim *types.SimulationOptions) *types.SimulationOptions {
	slip, fee, funding := strings.TrimSpace(os.Getenv(EnvSlippageBps)), strings.TrimSpace(os.Getenv(EnvFeeMult)), strings.TrimSpace(os.Getenv(EnvFunding))
	if slip == "" && fee == "" && funding == "" {
		return sim
	}
	out := types.SimulationOptions{}
	if sim != nil {
		out = *sim
	}
	var said []string
	if v, err := strconv.ParseFloat(slip, 64); slip != "" && err == nil {
		out.SlippageBps = &v
		said = append(said, fmt.Sprintf("slippage %gbp", v))
	}
	if v, err := strconv.ParseFloat(fee, 64); fee != "" && err == nil {
		out.FeeMult = &v
		said = append(said, fmt.Sprintf("fees x%g", v))
	}
	if strings.EqualFold(funding, "off") {
		off := false
		out.FundingEnabled = &off
		said = append(said, "funding off")
	}
	if len(said) > 0 {
		log.Printf("Backtest Engine: stress %s (platform settings over the strategy's)", strings.Join(said, ", "))
	}
	return &out
}
