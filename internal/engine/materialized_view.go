package engine

import (
	"github.com/space-node/space-node/internal/executor"
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
	Filter   executor.Predicate
}

type materializedSumView struct {
	spec     SumViewSpec
	store    *storage.MemoryViewStore
	pipeline executor.Pipeline
}

func newMaterializedSumView(spec SumViewSpec) (*materializedSumView, error) {
	store := storage.NewMemoryViewStore()
	agg, err := executor.NewHashAggExecutor(executor.SumAggSpec{GroupKey: spec.GroupKey, SumField: spec.SumField, SumAlias: spec.SumAlias})
	if err != nil {
		return nil, err
	}
	materialize, err := executor.NewMaterializeExecutor(spec.Name, spec.GroupKey, store)
	if err != nil {
		return nil, err
	}
	pipeline := executor.NewPipeline(
		executor.NewSourceExecutor(spec.Source),
		executor.NewFilterExecutor(spec.Filter),
		executor.NewProjectExecutor(spec.GroupKey, spec.SumField),
		agg,
		materialize,
	)
	return &materializedSumView{spec: spec, store: store, pipeline: pipeline}, nil
}

func (v *materializedSumView) get(key string) (types.Row, bool) {
	return v.store.Get(key)
}

func (v *materializedSumView) all() []types.Row {
	return v.store.All()
}
