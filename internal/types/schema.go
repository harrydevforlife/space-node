package types

// DataType is the small set of scalar types needed for the first examples.
type DataType string

const (
	IntType       DataType = "int"
	FloatType     DataType = "float"
	StringType    DataType = "string"
	BoolType      DataType = "bool"
	TimestampType DataType = "timestamp"
)

// Column describes one field in a stream or materialized view schema.
type Column struct {
	Name string
	Type DataType
}

// Schema describes the row shape for a stream or materialized view.
type Schema struct {
	Columns []Column
}

// HasColumn reports whether the schema contains a named column.
func (s Schema) HasColumn(name string) bool {
	for _, col := range s.Columns {
		if col.Name == name {
			return true
		}
	}
	return false
}
