package live

import (
	"errors"
	"testing"

	"github.com/adshao/go-binance/v2/common"
	"github.com/kdraigo/dev_sdk/types"
	"github.com/stretchr/testify/assert"
)

// A cancel Binance refuses with -2011 is an order no longer open; other errors stay as they are.
func TestClosedOnCancel(t *testing.T) {
	err := closedOnCancel(&common.APIError{Code: -2011, Message: "Unknown order sent."})
	assert.ErrorIs(t, err, types.ErrOrderClosed)
	other := &common.APIError{Code: -1021, Message: "Timestamp outside the window"}
	assert.Same(t, error(other), closedOnCancel(other))
	assert.False(t, errors.Is(closedOnCancel(errors.New("boom")), types.ErrOrderClosed))
}
