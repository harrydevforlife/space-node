package types

// Value is a scalar value carried in rows. The first educational version keeps
// this intentionally dynamic; later SQL/type-checking work can narrow it.
type Value any
