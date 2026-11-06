package types

import (
	"errors"
	"math"
	"sort"
	"strconv"
)

// CleanReason returns reason with every NaN or infinite number replaced by
// nil, nested maps and slices included, and the paths it replaced, sorted.
// Encoding a reason to JSON fails on such numbers, which used to refuse the
// whole order; an indicator still warming up is a normal source of NaN.
// reason itself is not changed; it is returned as is when it is clean.
func CleanReason(reason map[string]any) (map[string]any, []string) {
	if reason == nil {
		return nil, nil
	}
	var bad []string
	out, changed := cleanValue(reason, "", &bad)
	if !changed {
		return reason, nil
	}
	sort.Strings(bad)
	return out.(map[string]any), bad
}

func finite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

// cleanValue returns v cleaned and whether anything in it was replaced.
func cleanValue(v any, path string, bad *[]string) (any, bool) {
	switch x := v.(type) {
	case float64:
		if !finite(x) {
			*bad = append(*bad, path)
			return nil, true
		}
	case float32:
		if !finite(float64(x)) {
			*bad = append(*bad, path)
			return nil, true
		}
	case map[string]any:
		var out map[string]any
		for k, e := range x {
			p := k
			if path != "" {
				p = path + "." + k
			}
			if c, changed := cleanValue(e, p, bad); changed {
				if out == nil {
					out = make(map[string]any, len(x))
					for k2, e2 := range x {
						out[k2] = e2
					}
				}
				out[k] = c
			}
		}
		if out != nil {
			return out, true
		}
	case map[string]float64:
		m := make(map[string]any, len(x))
		for k, e := range x {
			m[k] = e
		}
		return cleanValue(m, path, bad)
	case []any:
		var out []any
		for i, e := range x {
			if c, changed := cleanValue(e, path+"["+strconv.Itoa(i)+"]", bad); changed {
				if out == nil {
					out = append([]any(nil), x...)
				}
				out[i] = c
			}
		}
		if out != nil {
			return out, true
		}
	case []float64:
		s := make([]any, len(x))
		for i, e := range x {
			s[i] = e
		}
		return cleanValue(s, path, bad)
	case []float32:
		s := make([]any, len(x))
		for i, e := range x {
			s[i] = e
		}
		return cleanValue(s, path, bad)
	}
	return v, false
}

// ErrOrderClosed is what CancelOrder returns, wrapped, for an order that is no
// longer open: it filled, or it was cancelled already (a bracket's other leg,
// which the exchange or the engine cancels when one leg fills). Nothing is
// left to cancel; check it with errors.Is and carry on.
var ErrOrderClosed = errors.New("order is no longer open")
