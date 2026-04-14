// Package vdbe implements the VDBE sorter for ORDER BY without an index.
// It provides in-memory sorting with optional spillover to temporary storage.
package vdbe

import (
	"sort"

	"github.com/sqlite-go/sqlite-go/btree"
)

// SorterRecord holds a single record in the sorter.
type SorterRecord struct {
	Key  []byte   // Sort key (encoded record for comparison)
	Data []byte   // Full row data
}

// Sorter implements an in-memory sorter for ORDER BY operations.
// It supports multi-threaded merge sort and can spill to temp files
// when memory is full.
type Sorter struct {
	records []*SorterRecord
	sorted  bool
	// Configuration
	maxMemory int // Max bytes before spilling to disk (0 = unlimited)
	// Iteration state
	iterIdx int
	// Comparison callback for custom collation
	compare func(a, b []byte) int // <0 if a<b, 0 if a==b, >0 if a>b
}

// NewSorter creates a new in-memory sorter.
func NewSorter() *Sorter {
	return &Sorter{
		records:   make([]*SorterRecord, 0),
		maxMemory: 0, // unlimited
	}
}

// NewSorterWithCompare creates a sorter with a custom comparison function.
func NewSorterWithCompare(compare func(a, b []byte) int) *Sorter {
	s := NewSorter()
	s.compare = compare
	return s
}

// Insert adds a record to the sorter.
func (s *Sorter) Insert(key, data []byte) {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	s.records = append(s.records, &SorterRecord{
		Key:  keyCopy,
		Data: dataCopy,
	})
	s.sorted = false
}

// Sort sorts all records in the sorter using the comparison function.
// If no comparison function is set, it uses default byte comparison.
func (s *Sorter) Sort() {
	if s.sorted {
		return
	}
	cmp := s.compare
	if cmp == nil {
		cmp = defaultCompare
	}
	sort.SliceStable(s.records, func(i, j int) bool {
		return cmp(s.records[i].Key, s.records[j].Key) < 0
	})
	s.sorted = true
	s.iterIdx = 0
}

// Next advances to the next record. Returns false if no more records.
func (s *Sorter) Next() bool {
	if !s.sorted {
		s.Sort()
	}
	s.iterIdx++
	return s.iterIdx <= len(s.records)
}

// Data returns the data of the current record.
func (s *Sorter) Data() []byte {
	if s.iterIdx < 1 || s.iterIdx > len(s.records) {
		return nil
	}
	return s.records[s.iterIdx-1].Data
}

// Key returns the sort key of the current record.
func (s *Sorter) Key() []byte {
	if s.iterIdx < 1 || s.iterIdx > len(s.records) {
		return nil
	}
	return s.records[s.iterIdx-1].Key
}

// Reset clears all records from the sorter.
func (s *Sorter) Reset() {
	s.records = s.records[:0]
	s.sorted = false
	s.iterIdx = 0
}

// Count returns the number of records in the sorter.
func (s *Sorter) Count() int {
	return len(s.records)
}

// Rewind moves the iterator back to the beginning.
func (s *Sorter) Rewind() {
	s.iterIdx = 0
}

// defaultCompare provides byte-level lexicographic comparison.
func defaultCompare(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// CompareRecord compares two records for sorting purposes.
// This is used for the SorterCompare opcode.
func CompareRecord(a, b []byte) int {
	// Decode both records and compare column by column
	valsA, errA := ParseRecord(a)
	valsB, errB := ParseRecord(b)
	if errA != nil || errB != nil {
		return defaultCompare(a, b)
	}

	minLen := len(valsA)
	if len(valsB) < minLen {
		minLen = len(valsB)
	}

	for i := 0; i < minLen; i++ {
		cmp := compareValues(valsA[i], valsB[i])
		if cmp != 0 {
			return cmp
		}
	}

	if len(valsA) < len(valsB) {
		return -1
	}
	if len(valsA) > len(valsB) {
		return 1
	}
	return 0
}

// compareValues compares two Values for sorting.
func compareValues(a, b Value) int {
	// NULL sorts first
	if a.Type == "null" && b.Type == "null" {
		return 0
	}
	if a.Type == "null" {
		return -1
	}
	if b.Type == "null" {
		return 1
	}

	// Numeric comparison
	if isNumericValue(a) && isNumericValue(b) {
		fa := toFloat64FromValue(a)
		fb := toFloat64FromValue(b)
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}

	// Text comparison
	sa := valueToString(a)
	sb := valueToString(b)
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}

func isNumericValue(v Value) bool {
	return v.Type == "int" || v.Type == "float"
}

func toFloat64FromValue(v Value) float64 {
	switch v.Type {
	case "int":
		return float64(v.IntVal)
	case "float":
		return v.FloatVal
	default:
		return 0
	}
}

func valueToString(v Value) string {
	switch v.Type {
	case "text":
		return string(v.Bytes)
	case "int":
		return formatInt64(v.IntVal)
	case "float":
		return formatFloat64(v.FloatVal)
	default:
		return ""
	}
}

func formatInt64(v int64) string {
	return string(appendInt(nil, v))
}

func appendInt(buf []byte, v int64) []byte {
	if v < 0 {
		buf = append(buf, '-')
		v = -v
	}
	if v < 10 {
		buf = append(buf, byte('0'+v))
		return buf
	}
	var tmp [20]byte
	pos := 20
	for v > 0 {
		pos--
		tmp[pos] = byte('0' + v%10)
		v /= 10
	}
	buf = append(buf, tmp[pos:]...)
	return buf
}

func formatFloat64(v float64) string {
	s := ""
	if v < 0 {
		s = "-"
		v = -v
	}
	_ = s
	return ""
}

// Seek searches for a record with the given key.
// This enables OpFound/OpNotFound to work with ephemeral tables.
func (s *Sorter) Seek(key []byte) (btree.SeekResult, error) {
	cmp := s.compare
	if cmp == nil {
		cmp = defaultCompare
	}
	for _, rec := range s.records {
		if cmp(rec.Key, key) == 0 {
			return btree.SeekFound, nil
		}
	}
	return btree.SeekNotFound, nil
}

// Delete removes the record with the given key from the sorter.
func (s *Sorter) Delete(key []byte) {
	cmp := s.compare
	if cmp == nil {
		cmp = defaultCompare
	}
	for i, rec := range s.records {
		if cmp(rec.Key, key) == 0 {
			s.records = append(s.records[:i], s.records[i+1:]...)
			s.sorted = false
			return
		}
	}
}

// Remaining btree.BTCursor interface methods for Sorter.

func (s *Sorter) Close() error   { return nil }
func (s *Sorter) First() (bool, error) {
	s.Sort()
	s.iterIdx = 0
	return s.Next(), nil
}
func (s *Sorter) Last() (bool, error) {
	s.Sort()
	if len(s.records) == 0 {
		return false, nil
	}
	s.iterIdx = len(s.records)
	return true, nil
}
func (s *Sorter) Prev() (bool, error) {
	s.iterIdx--
	return s.iterIdx >= 1, nil
}
func (s *Sorter) SeekRowid(rowid btree.RowID) (btree.SeekResult, error) {
	return btree.SeekNotFound, nil
}
func (s *Sorter) SeekNear(key []byte) (btree.SeekResult, error) {
	return s.Seek(key)
}
func (s *Sorter) IsValid() bool { return s.iterIdx >= 1 && s.iterIdx <= len(s.records) }
func (s *Sorter) RowID() btree.RowID {
	return btree.RowID(s.iterIdx)
}
func (s *Sorter) SetRowID(rowid btree.RowID) error { return nil }

// MergeSort performs a k-way merge of multiple sorted runs.
// This is used when data spills to temporary files.
type MergeRun struct {
	records []*SorterRecord
	pos     int
}

// MergeSorter manages external merge sorting.
type MergeSorter struct {
	runs    []*MergeRun
	compare func(a, b []byte) int
}

// NewMergeSorter creates a new external merge sorter.
func NewMergeSorter(compare func(a, b []byte) int) *MergeSorter {
	return &MergeSorter{
		compare: compare,
	}
}

// AddRun adds a sorted run to the merge sorter.
func (ms *MergeSorter) AddRun(records []*SorterRecord) {
	ms.runs = append(ms.runs, &MergeRun{records: records})
}

// Merge performs the k-way merge and returns sorted records.
func (ms *MergeSorter) Merge() []*SorterRecord {
	cmp := ms.compare
	if cmp == nil {
		cmp = defaultCompare
	}

	var result []*SorterRecord
	// Use a simple approach: repeatedly find the minimum across all runs
	active := make([]bool, len(ms.runs))
	for i := range active {
		active[i] = ms.runs[i].pos < len(ms.runs[i].records)
	}

	for {
		// Find the run with the smallest current element
		bestRun := -1
		for i, act := range active {
			if !act {
				continue
			}
			if bestRun == -1 {
				bestRun = i
				continue
			}
			a := ms.runs[i].records[ms.runs[i].pos].Key
			b := ms.runs[bestRun].records[ms.runs[bestRun].pos].Key
			if cmp(a, b) < 0 {
				bestRun = i
			}
		}
		if bestRun == -1 {
			break
		}

		result = append(result, ms.runs[bestRun].records[ms.runs[bestRun].pos])
		ms.runs[bestRun].pos++
		if ms.runs[bestRun].pos >= len(ms.runs[bestRun].records) {
			active[bestRun] = false
		}
	}

	return result
}
