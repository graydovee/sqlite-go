package sqlite

import (
	"io"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// Row represents a single row of query results.
type Row struct {
	values []*vdbe.Mem
	cols   []ResultColumnInfo
}

// ColumnInt returns the integer value of the given column index.
func (r *Row) ColumnInt(idx int) int64 {
	if idx < 0 || idx >= len(r.values) {
		return 0
	}
	return r.values[idx].IntValue()
}

// ColumnFloat returns the float value of the given column index.
func (r *Row) ColumnFloat(idx int) float64 {
	if idx < 0 || idx >= len(r.values) {
		return 0
	}
	return r.values[idx].FloatValue()
}

// ColumnText returns the text value of the given column index.
func (r *Row) ColumnText(idx int) string {
	if idx < 0 || idx >= len(r.values) {
		return ""
	}
	return r.values[idx].TextValue()
}

// ColumnBlob returns the blob value of the given column index.
func (r *Row) ColumnBlob(idx int) []byte {
	if idx < 0 || idx >= len(r.values) {
		return nil
	}
	return r.values[idx].BlobValue()
}

// ColumnType returns the type of the given column index.
func (r *Row) ColumnType(idx int) ColumnType {
	if idx < 0 || idx >= len(r.values) {
		return ColNull
	}
	return memTypeToColumnType(r.values[idx].Type)
}

// ColumnIsNull returns true if the column value is NULL.
func (r *Row) ColumnIsNull(idx int) bool {
	if idx < 0 || idx >= len(r.values) {
		return true
	}
	return r.values[idx].Type == vdbe.MemNull
}

// ColumnValue returns the raw value as an interface{}.
// Returns nil for NULL, int64 for integers, float64 for floats,
// string for text, and []byte for blobs.
func (r *Row) ColumnValue(idx int) interface{} {
	if idx < 0 || idx >= len(r.values) {
		return nil
	}
	m := r.values[idx]
	switch m.Type {
	case vdbe.MemNull:
		return nil
	case vdbe.MemInt:
		return m.IntVal
	case vdbe.MemFloat:
		return m.FloatVal
	case vdbe.MemStr:
		return string(m.Bytes)
	case vdbe.MemBlob:
		return m.Bytes
	}
	return nil
}

// ColumnCount returns the number of columns in this row.
func (r *Row) ColumnCount() int {
	return len(r.values)
}

// ColumnName returns the name of the column at the given index.
func (r *Row) ColumnName(idx int) string {
	if idx < 0 || idx >= len(r.cols) {
		return ""
	}
	return r.cols[idx].Name
}

// ColumnNames returns all column names.
func (r *Row) ColumnNames() []string {
	names := make([]string, len(r.cols))
	for i, c := range r.cols {
		names[i] = c.Name
	}
	return names
}

// Scan copies column values into the provided destinations.
// Each dest must be a pointer to int64, float64, string, []byte, or interface{}.
func (r *Row) Scan(dest ...interface{}) error {
	for i, d := range dest {
		if i >= len(r.values) {
			return io.ErrUnexpectedEOF
		}
		if err := scanValue(r.values[i], d); err != nil {
			return err
		}
	}
	return nil
}

// ResultSet holds the results of a query. It provides an iterator
// over the rows returned by a SELECT statement.
type ResultSet struct {
	rows   []Row
	index  int
	cols   []ResultColumnInfo
}

// newResultSet creates a ResultSet from collected rows.
func newResultSet(rows []Row, cols []ResultColumnInfo) *ResultSet {
	return &ResultSet{
		rows:  rows,
		cols:  cols,
		index: -1,
	}
}

// Next advances to the next row. Returns false if there are no more rows.
func (rs *ResultSet) Next() bool {
	rs.index++
	return rs.index < len(rs.rows)
}

// Row returns the current row.
func (rs *ResultSet) Row() *Row {
	if rs.index < 0 || rs.index >= len(rs.rows) {
		return nil
	}
	return &rs.rows[rs.index]
}

// ColumnCount returns the number of columns.
func (rs *ResultSet) ColumnCount() int {
	return len(rs.cols)
}

// ColumnNames returns the names of all result columns.
func (rs *ResultSet) ColumnNames() []string {
	names := make([]string, len(rs.cols))
	for i, c := range rs.cols {
		names[i] = c.Name
	}
	return names
}

// ColumnInfo returns metadata about the given column.
func (rs *ResultSet) ColumnInfo(idx int) *ResultColumnInfo {
	if idx < 0 || idx >= len(rs.cols) {
		return nil
	}
	return &rs.cols[idx]
}

// Rows returns all remaining rows as a slice.
func (rs *ResultSet) Rows() []Row {
	if rs.index < 0 {
		return rs.rows
	}
	return rs.rows[rs.index:]
}

// Close releases the result set. No-op for now but provided for
// forward compatibility.
func (rs *ResultSet) Close() error {
	return nil
}

// ResultColumnInfo describes a column in a result set.
type ResultColumnInfo struct {
	Name     string
	Type     ColumnType
	Nullable bool
}

// scanValue assigns a Mem value to a destination pointer.
func scanValue(m *vdbe.Mem, dest interface{}) error {
	switch d := dest.(type) {
	case *int64:
		*d = m.IntValue()
	case *float64:
		*d = m.FloatValue()
	case *string:
		*d = m.TextValue()
	case *[]byte:
		b := m.BlobValue()
		if b != nil {
			cp := make([]byte, len(b))
			copy(cp, b)
			*d = cp
		} else {
			*d = nil
		}
	case *interface{}:
		switch m.Type {
		case vdbe.MemNull:
			*d = nil
		case vdbe.MemInt:
			*d = m.IntVal
		case vdbe.MemFloat:
			*d = m.FloatVal
		case vdbe.MemStr:
			*d = string(m.Bytes)
		case vdbe.MemBlob:
			*d = m.Bytes
		}
	}
	return nil
}

// memTypeToColumnType converts a VDBE MemType to a public ColumnType.
func memTypeToColumnType(mt vdbe.MemType) ColumnType {
	switch mt {
	case vdbe.MemInt:
		return ColInteger
	case vdbe.MemFloat:
		return ColFloat
	case vdbe.MemStr:
		return ColText
	case vdbe.MemBlob:
		return ColBlob
	default:
		return ColNull
	}
}
