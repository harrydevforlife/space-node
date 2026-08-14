package executor

import (
	"context"

	"github.com/space-node/space-node/internal/types"
)

// Executor processes one changelog record and emits zero or more records.
// This mirrors RisingWave's message-driven executor model at PR 2 scale.
type Executor interface {
	Process(ctx context.Context, change types.Change) ([]types.Change, error)
}

// Pipeline applies executors in order.
type Pipeline struct {
	operators []Executor
}

// NewPipeline creates an ordered executor pipeline.
func NewPipeline(operators ...Executor) Pipeline {
	return Pipeline{operators: append([]Executor(nil), operators...)}
}

// Process pushes a change through every executor.
func (p Pipeline) Process(ctx context.Context, change types.Change) error {
	changes := []types.Change{change}
	for _, operator := range p.operators {
		next := make([]types.Change, 0, len(changes))
		for _, current := range changes {
			out, err := operator.Process(ctx, current)
			if err != nil {
				return err
			}
			next = append(next, out...)
		}
		changes = next
		if len(changes) == 0 {
			return nil
		}
	}
	return nil
}
