package types

// Row is a named collection of values flowing through streams and views.
type Row map[string]Value

// Clone returns a shallow copy so callers cannot mutate stored rows by holding
// on to a map reference returned from the engine.
func (r Row) Clone() Row {
	out := make(Row, len(r))
	for k, v := range r {
		out[k] = v
	}
	return out
}
