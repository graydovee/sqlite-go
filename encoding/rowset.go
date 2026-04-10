package encoding

import "sort"

// RowSet tracks a set of row IDs for WHERE clause optimization.
// Rows are stored in sorted order for efficient iteration.
type RowSet struct {
	rows  []int64
	index map[int64]bool
}

// NewRowSet creates a new empty RowSet.
func NewRowSet() *RowSet {
	return &RowSet{
		rows:  make([]int64, 0),
		index: make(map[int64]bool),
	}
}

// Insert adds a row ID to the set.
func (rs *RowSet) Insert(rowid int64) {
	if rs.index[rowid] {
		return // Already present
	}
	rs.index[rowid] = true
	// Binary search for insertion point
	idx := sort.Search(len(rs.rows), func(i int) bool {
		return rs.rows[i] >= rowid
	})
	rs.rows = append(rs.rows, 0)
	copy(rs.rows[idx+1:], rs.rows[idx:])
	rs.rows[idx] = rowid
}

// Test returns whether the row ID is in the set.
func (rs *RowSet) Test(rowid int64) bool {
	return rs.index[rowid]
}

// Next returns the smallest row ID in the set, removing it.
// Returns (rowid, true) if found, or (0, false) if empty.
func (rs *RowSet) Next() (int64, bool) {
	if len(rs.rows) == 0 {
		return 0, false
	}
	rowid := rs.rows[0]
	rs.rows = rs.rows[1:]
	delete(rs.index, rowid)
	return rowid, true
}

// Clear removes all entries.
func (rs *RowSet) Clear() {
	rs.rows = rs.rows[:0]
	for k := range rs.index {
		delete(rs.index, k)
	}
}

// Count returns the number of entries.
func (rs *RowSet) Count() int {
	return len(rs.rows)
}

// IsEmpty returns whether the set is empty.
func (rs *RowSet) IsEmpty() bool {
	return len(rs.rows) == 0
}
