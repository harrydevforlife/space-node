package executor

import (
	"context"
	"fmt"

	"github.com/space-node/space-node/internal/types"
)

// SourceExecutor accepts records from a single source stream.
type SourceExecutor struct {
	stream string
}

// NewSourceExecutor creates a source gate for stream.
func NewSourceExecutor(stream string) SourceExecutor {
	return SourceExecutor{stream: stream}
}

// Process forwards changes for the configured stream and drops unrelated streams.
func (e SourceExecutor) Process(_ context.Context, change types.Change) ([]types.Change, error) {
	if e.stream == "" {
		return nil, fmt.Errorf("source stream is required")
	}
	if change.Source != e.stream {
		return nil, nil
	}
	return []types.Change{change}, nil
}
