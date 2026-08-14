package executor

import (
	"context"

	"github.com/space-node/space-node/internal/types"
)

// Predicate decides whether a row should continue through the pipeline.
type Predicate func(types.Row) bool

// FilterExecutor drops changes whose rows do not match a predicate.
type FilterExecutor struct {
	predicate Predicate
}

// NewFilterExecutor creates a filter. A nil predicate keeps all rows.
func NewFilterExecutor(predicate Predicate) FilterExecutor {
	return FilterExecutor{predicate: predicate}
}

// Process forwards matching changes.
func (e FilterExecutor) Process(_ context.Context, change types.Change) ([]types.Change, error) {
	if e.predicate == nil || e.predicate(change.Row) {
		return []types.Change{change}, nil
	}
	return nil, nil
}
