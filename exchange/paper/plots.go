package paper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/kdraigo/dev_sdk/plots"
	"github.com/kdraigo/dev_sdk/telemetry"
)

// PublishPlots implements telemetry.PlotPublisher: a strategy's plots go over
// the account's connection, and the paper exchange records them in the
// account's live session, so a paper bot needs no telemetry setting. The
// warning is the platform's note about values it did not store.
func (c *Client) PublishPlots(ctx context.Context, b *plots.Batch) (string, error) {
	if b.Empty() {
		return "", nil
	}
	data, err := c.call(ctx, "plots", b)
	var ee *EngineError
	switch {
	case errors.As(err, &ee) && ee.Code == "plots_rejected":
		return "", fmt.Errorf("%w: %s", telemetry.ErrPlotsRejected, ee.Msg)
	case err != nil:
		return "", err
	}
	var reply struct {
		Warning string `json:"warning"`
	}
	_ = json.Unmarshal(data, &reply)
	return reply.Warning, nil
}

var _ telemetry.PlotPublisher = (*Client)(nil)
