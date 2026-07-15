package engine

import (
	"fmt"

	"github.com/space-node/space-node/internal/storage"
	"github.com/space-node/space-node/internal/types"
)

// SumViewSpec defines the first supported materialized view shape:
// SELECT group_key, SUM(sum_field) FROM source GROUP BY group_key.
type SumViewSpec struct {
	Name     string
	Source   string
	GroupKey string
	SumField string
	SumAlias string
}

type materializedSumView struct {
	spec  SumViewSpec
	store *storage.MemoryViewStore
}

func newMaterializedSumView(spec SumViewSpec) (*materializedSumView, error) {
	if spec.Name == "" {
		return nil, fmt.Errorf("view name is required")
	}
	if spec.Source == "" {
		return nil, fmt.Errorf("view source is required")
	}
	if spec.GroupKey == "" {
		return nil, fmt.Errorf("view group key is required")
	}
	if spec.SumField == "" {
		return nil, fmt.Errorf("view sum field is required")
	}
	if spec.SumAlias == "" {
		spec.SumAlias = "sum_" + spec.SumField
	}
	return &materializedSumView{spec: spec, store: storage.NewMemoryViewStore()}, nil
}

func (v *materializedSumView) apply(change types.Change) error {
	if change.Source != v.spec.Source {
		return nil
	}
	if change.Op != types.Insert {
		return fmt.Errorf("view %s only supports insert changes in PR 1", v.spec.Name)
	}
	group, ok := change.Row[v.spec.GroupKey]
	if !ok {
		return fmt.Errorf("row missing group key %q", v.spec.GroupKey)
	}
	amount, err := numericValue(change.Row[v.spec.SumField])
	if err != nil {
		return fmt.Errorf("row field %q: %w", v.spec.SumField, err)
	}

	key := fmt.Sprint(group)
	current, ok := v.store.Get(key)
	if !ok {
		current = types.Row{v.spec.GroupKey: group, v.spec.SumAlias: 0.0}
	}
	currentSum, err := numericValue(current[v.spec.SumAlias])
	if err != nil {
		return fmt.Errorf("stored sum field %q: %w", v.spec.SumAlias, err)
	}
	current[v.spec.SumAlias] = currentSum + amount
	v.store.Put(key, current)
	return nil
}

func (v *materializedSumView) get(key string) (types.Row, bool) {
	return v.store.Get(key)
}

func (v *materializedSumView) all() []types.Row {
	return v.store.All()
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
