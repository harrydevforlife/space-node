package executor

import (
	"context"

	"github.com/space-node/space-node/internal/types"
)

// ProjectExecutor keeps a subset of columns.
type ProjectExecutor struct {
	columns []string
}

// NewProjectExecutor creates a projection. With no columns it forwards rows unchanged.
func NewProjectExecutor(columns ...string) ProjectExecutor {
	return ProjectExecutor{columns: append([]string(nil), columns...)}
}

// Process rewrites each row to only projected columns.
func (e ProjectExecutor) Process(_ context.Context, change types.Change) ([]types.Change, error) {
	if len(e.columns) == 0 {
		return []types.Change{change}, nil
	}
	projected := make(types.Row, len(e.columns))
	for _, column := range e.columns {
		if value, ok := change.Row[column]; ok {
			projected[column] = value
		}
	}
	change.Row = projected
	return []types.Change{change}, nil
}
