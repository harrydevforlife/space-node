package types

// Op describes the kind of changelog record flowing through the engine.
type Op int

const (
	Insert Op = iota
	Delete
	Update
)

// Change is the unit of stream data processed by the first engine slice.
type Change struct {
	Epoch  uint64
	Op     Op
	Source string
	Row    Row
}
