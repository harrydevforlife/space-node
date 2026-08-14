package executor

import (
	"context"
	"fmt"

	"github.com/space-node/space-node/internal/storage"
	"github.com/space-node/space-node/internal/types"
)

// MaterializeExecutor writes final pipeline rows into a materialized view store.
type MaterializeExecutor struct {
	viewName string
	keyField string
	store    *storage.MemoryViewStore
}

// NewMaterializeExecutor creates a materializer for a view.
func NewMaterializeExecutor(viewName, keyField string, store *storage.MemoryViewStore) (*MaterializeExecutor, error) {
	if viewName == "" {
		return nil, fmt.Errorf("view name is required")
	}
	if keyField == "" {
		return nil, fmt.Errorf("materialize key field is required")
	}
	if store == nil {
		return nil, fmt.Errorf("materialize store is required")
	}
	return &MaterializeExecutor{viewName: viewName, keyField: keyField, store: store}, nil
}

// Process stores each row by the configured key field.
func (e *MaterializeExecutor) Process(_ context.Context, change types.Change) ([]types.Change, error) {
	key, ok := change.Row[e.keyField]
	if !ok {
		return nil, fmt.Errorf("materialized view %q row missing key field %q", e.viewName, e.keyField)
	}
	e.store.Put(fmt.Sprint(key), change.Row)
	return []types.Change{change}, nil
}
