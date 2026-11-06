package types

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanReason(t *testing.T) {
	clean := map[string]any{"rsi": 31.2, "note": "dip"}
	got, bad := CleanReason(clean)
	assert.Nil(t, bad)
	assert.Equal(t, clean, got)

	in := map[string]any{
		"rsi":   math.NaN(),
		"ok":    1.5,
		"bands": map[string]any{"upper": math.Inf(1), "lower": 2.0},
		"last":  []float64{1, math.NaN()},
		"f32":   float32(math.Inf(-1)),
	}
	got, bad = CleanReason(in)
	assert.Equal(t, []string{"bands.upper", "f32", "last[1]", "rsi"}, bad)
	_, err := json.Marshal(got)
	require.NoError(t, err, "a cleaned reason encodes")
	assert.Nil(t, got["rsi"])
	assert.Equal(t, 1.5, got["ok"])
	assert.Equal(t, map[string]any{"upper": nil, "lower": 2.0}, got["bands"])
	assert.Equal(t, []any{1.0, nil}, got["last"])
	assert.True(t, math.IsNaN(in["rsi"].(float64)), "the strategy's own map is left as it was")

	got, bad = CleanReason(nil)
	assert.Nil(t, got)
	assert.Nil(t, bad)
}
