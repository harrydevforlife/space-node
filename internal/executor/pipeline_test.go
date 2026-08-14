package executor_test

import (
	"context"
	"testing"

	"github.com/space-node/space-node/internal/executor"
	"github.com/space-node/space-node/internal/storage"
	"github.com/space-node/space-node/internal/types"
)

func TestPipelineFiltersAggregatesAndMaterializes(t *testing.T) {
	store := storage.NewMemoryViewStore()
	agg, err := executor.NewHashAggExecutor(executor.SumAggSpec{GroupKey: "user_id", SumField: "amount", SumAlias: "total_amount"})
	if err != nil {
		t.Fatal(err)
	}
	materialize, err := executor.NewMaterializeExecutor("user_spend", "user_id", store)
	if err != nil {
		t.Fatal(err)
	}
	pipeline := executor.NewPipeline(
		executor.NewSourceExecutor("orders"),
		executor.NewFilterExecutor(func(row types.Row) bool { return row["amount"].(float64) > 0 }),
		executor.NewProjectExecutor("user_id", "amount"),
		agg,
		materialize,
	)

	ctx := context.Background()
	if err := pipeline.Process(ctx, types.Change{Epoch: 1, Op: types.Insert, Source: "orders", Row: types.Row{"user_id": 7, "amount": 20.0, "ignored": "x"}}); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.Process(ctx, types.Change{Epoch: 2, Op: types.Insert, Source: "orders", Row: types.Row{"user_id": 7, "amount": -100.0}}); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.Process(ctx, types.Change{Epoch: 3, Op: types.Insert, Source: "other", Row: types.Row{"user_id": 7, "amount": 100.0}}); err != nil {
		t.Fatal(err)
	}
	if err := pipeline.Process(ctx, types.Change{Epoch: 4, Op: types.Insert, Source: "orders", Row: types.Row{"user_id": 7, "amount": 5.5}}); err != nil {
		t.Fatal(err)
	}

	row, ok := store.Get("7")
	if !ok {
		t.Fatal("expected materialized row")
	}
	if got := row["total_amount"]; got != 25.5 {
		t.Fatalf("total_amount = %v, want 25.5", got)
	}
	if _, ok := row["ignored"]; ok {
		t.Fatal("projected output should not include ignored column")
	}
}
