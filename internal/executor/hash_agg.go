package executor

import (
	"context"
	"fmt"

	"github.com/space-node/space-node/internal/types"
)

// SumAggSpec configures the first hash aggregation supported by space-node.
type SumAggSpec struct {
	GroupKey string
	SumField string
	SumAlias string
}

// HashAggExecutor maintains grouped SUM state and emits updated aggregate rows.
type HashAggExecutor struct {
	spec  SumAggSpec
	sums  map[string]float64
	group map[string]types.Value
}

// NewHashAggExecutor creates a grouped SUM aggregation executor.
func NewHashAggExecutor(spec SumAggSpec) (*HashAggExecutor, error) {
	if spec.GroupKey == "" {
		return nil, fmt.Errorf("aggregate group key is required")
	}
	if spec.SumField == "" {
		return nil, fmt.Errorf("aggregate sum field is required")
	}
	if spec.SumAlias == "" {
		spec.SumAlias = "sum_" + spec.SumField
	}
	return &HashAggExecutor{spec: spec, sums: map[string]float64{}, group: map[string]types.Value{}}, nil
}

// Process applies insert changes to aggregate state and emits the current total.
func (e *HashAggExecutor) Process(_ context.Context, change types.Change) ([]types.Change, error) {
	if change.Op != types.Insert {
		return nil, fmt.Errorf("hash aggregate only supports insert changes in PR 2")
	}
	groupValue, ok := change.Row[e.spec.GroupKey]
	if !ok {
		return nil, fmt.Errorf("row missing group key %q", e.spec.GroupKey)
	}
	amount, err := numericValue(change.Row[e.spec.SumField])
	if err != nil {
		return nil, fmt.Errorf("row field %q: %w", e.spec.SumField, err)
	}
	key := fmt.Sprint(groupValue)
	e.group[key] = groupValue
	e.sums[key] += amount
	change.Row = types.Row{e.spec.GroupKey: e.group[key], e.spec.SumAlias: e.sums[key]}
	return []types.Change{change}, nil
}

func numericValue(v types.Value) (float64, error) {
	switch n := v.(type) {
	case int:
		return float64(n), nil
	case int64:
		return float64(n), nil
	case float32:
		return float64(n), nil
	case float64:
		return n, nil
	default:
		return 0, fmt.Errorf("expected numeric value, got %T", v)
	}
}
