package types

// Barrier marks an epoch boundary. Later PRs will propagate barriers through
// executor graphs and use them to trigger checkpoints.
type Barrier struct {
	Epoch uint64
}
