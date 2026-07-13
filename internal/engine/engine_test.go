package engine_test

import (
	"testing"

	"github.com/space-node/space-node/internal/engine"
	"github.com/space-node/space-node/internal/types"
)

func TestInMemorySumViewUpdatesIncrementally(t *testing.T) {
	db := engine.New()
	err := db.CreateStream("orders", types.Schema{Columns: []types.Column{
		{Name: "user_id", Type: types.IntType},
		{Name: "amount", Type: types.FloatType},
	}})
	if err != nil {
		t.Fatal(err)
	}
	err = db.CreateSumView(engine.SumViewSpec{
		Name:     "user_spend",
		Source:   "orders",
		GroupKey: "user_id",
		SumField: "amount",
		SumAlias: "total_amount",
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := db.Insert(1, "orders", types.Row{"user_id": 7, "amount": 20.0}); err != nil {
		t.Fatal(err)
	}
	row, ok := db.GetViewRow("user_spend", "7")
	if !ok {
		t.Fatal("expected user_spend row for user 7")
	}
	if got := row["total_amount"]; got != 20.0 {
		t.Fatalf("after first insert total_amount = %v, want 20", got)
	}

	if err := db.Insert(2, "orders", types.Row{"user_id": 7, "amount": 5.5}); err != nil {
		t.Fatal(err)
	}
	row, ok = db.GetViewRow("user_spend", "7")
	if !ok {
		t.Fatal("expected user_spend row for user 7")
	}
	if got := row["total_amount"]; got != 25.5 {
		t.Fatalf("after second insert total_amount = %v, want 25.5", got)
	}
}

func TestViewRowsAreReturnedAsCopies(t *testing.T) {
	db := engine.New()
	if err := db.CreateStream("orders", types.Schema{Columns: []types.Column{{Name: "user_id", Type: types.IntType}, {Name: "amount", Type: types.FloatType}}}); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateSumView(engine.SumViewSpec{Name: "user_spend", Source: "orders", GroupKey: "user_id", SumField: "amount"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Insert(1, "orders", types.Row{"user_id": 1, "amount": 10.0}); err != nil {
		t.Fatal(err)
	}

	row, ok := db.GetViewRow("user_spend", "1")
	if !ok {
		t.Fatal("expected row")
	}
	row["sum_amount"] = 999.0

	row, ok = db.GetViewRow("user_spend", "1")
	if !ok {
		t.Fatal("expected row")
	}
	if got := row["sum_amount"]; got != 10.0 {
		t.Fatalf("stored row was mutated through returned copy: got %v", got)
	}
}
