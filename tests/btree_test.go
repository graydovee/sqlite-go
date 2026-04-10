package tests

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"github.com/sqlite-go/sqlite-go/btree"
	"github.com/sqlite-go/sqlite-go/pager"
	"github.com/sqlite-go/sqlite-go/vfs"
)

// ============================================================================
// B-Tree comprehensive tests - ported from C test_btree.c / test3.c
// ============================================================================

// openTestBTree opens a file-backed B-Tree for testing.
func openTestBTreeFile(t *testing.T) (*btree.BTreeImpl, *pager.PagerImpl) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "btree_test.db")

	cfg := pager.PagerConfig{
		VFS:         vfs.Find("unix"),
		Path:        path,
		PageSize:    4096,
		CacheSize:   10,
		JournalMode: pager.JournalMemory,
	}
	p, err := pager.OpenPager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Open(); err != nil {
		t.Fatal(err)
	}

	bt, err := btree.OpenBTreeConn(p).Open(p)
	if err != nil {
		t.Fatal(err)
	}
	return bt.(*btree.BTreeImpl), p
}

// openTestBTreeMem opens an in-memory B-Tree for testing.
func openTestBTreeMem(t *testing.T) (*btree.BTreeImpl, *pager.PagerImpl) {
	t.Helper()
	cfg := pager.PagerConfig{
		Path:        ":memory:",
		PageSize:    4096,
		CacheSize:   10,
		VFS:         vfs.Find("memory"),
		JournalMode: pager.JournalMemory,
	}
	p, err := pager.OpenPager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Open(); err != nil {
		t.Fatal(err)
	}
	bt, err := btree.OpenBTreeConn(p).Open(p)
	if err != nil {
		t.Fatal(err)
	}
	return bt.(*btree.BTreeImpl), p
}

// --- Helpers ---

// insertRow inserts a single row into the B-Tree.
func insertRow(t *testing.T, bt *btree.BTreeImpl, rootPage pager.PageNumber, rowid btree.RowID, values ...btree.Value) {
	t.Helper()
	record := btree.MakeRecord(values)
	cur, err := bt.Cursor(rootPage, true)
	if err != nil {
		t.Fatalf("Cursor for insert rowid=%d: %v", rowid, err)
	}
	defer cur.Close()
	if err := bt.Insert(cur, nil, record, rowid, btree.SeekNotFound); err != nil {
		t.Fatalf("Insert rowid=%d: %v", rowid, err)
	}
}

// countRows counts all rows via forward scan.
func countRows(t *testing.T, bt *btree.BTreeImpl, rootPage pager.PageNumber) int {
	t.Helper()
	cur, err := bt.Cursor(rootPage, false)
	if err != nil {
		t.Fatalf("Cursor for count: %v", err)
	}
	defer cur.Close()
	count := 0
	hasRow, err := cur.First()
	for hasRow && err == nil {
		count++
		hasRow, err = cur.Next()
	}
	return count
}

// collectAllRowIDs collects all row IDs via forward scan.
func collectAllRowIDs(t *testing.T, bt *btree.BTreeImpl, rootPage pager.PageNumber) []int64 {
	t.Helper()
	cur, err := bt.Cursor(rootPage, false)
	if err != nil {
		t.Fatalf("Cursor: %v", err)
	}
	defer cur.Close()
	var ids []int64
	hasRow, err := cur.First()
	for hasRow && err == nil {
		ids = append(ids, int64(cur.RowID()))
		hasRow, err = cur.Next()
	}
	return ids
}

// --- Test: Insert 10000 rows ---

func TestBTreeInsert10000Rows(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	rootPage, err := bt.CreateBTree(btree.CreateTable)
	if err != nil {
		t.Fatalf("CreateBTree: %v", err)
	}
	bt.Begin(true)
	defer bt.Commit()

	const numRows = 10000
	for i := int64(1); i <= numRows; i++ {
		insertRow(t, bt, rootPage, btree.RowID(i),
			btree.IntValue(i),
			btree.TextValue(fmt.Sprintf("name_%d", i)),
		)
	}

	// Verify count
	count, err := bt.Count(rootPage)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != numRows {
		t.Errorf("Count = %d, want %d", count, numRows)
	}

	// Verify via forward scan
	scanned := 0
	lastRowid := int64(0)
	cur, _ := bt.Cursor(rootPage, false)
	hasRow, _ := cur.First()
	for hasRow {
		scanned++
		rid := int64(cur.RowID())
		if rid <= lastRowid {
			t.Errorf("out of order: rowid %d after %d", rid, lastRowid)
		}
		lastRowid = rid

		data, err := cur.Data()
		if err != nil {
			t.Errorf("Data() error at rowid %d: %v", rid, err)
		}
		values, err := btree.ParseRecord(data)
		if err != nil {
			t.Errorf("ParseRecord error at rowid %d: %v", rid, err)
		}
		if len(values) < 1 || values[0].IntVal != rid {
			t.Errorf("rowid %d: value mismatch", rid)
		}
		hasRow, _ = cur.Next()
	}
	cur.Close()
	if scanned != numRows {
		t.Errorf("forward scan: got %d rows, want %d", scanned, numRows)
	}

	// Verify via backward scan
	scanned = 0
	cur, _ = bt.Cursor(rootPage, false)
	hasRow, _ = cur.Last()
	for hasRow {
		scanned++
		hasRow, _ = cur.Prev()
	}
	cur.Close()
	if scanned != numRows {
		t.Errorf("backward scan: got %d rows, want %d", scanned, numRows)
	}

	// Integrity check
	var errs []string
	bt.IntegrityCheck(rootPage, 0, &errs)
	for _, e := range errs {
		t.Errorf("integrity: %s", e)
	}
}

// --- Test: Delete every other row ---

func TestBTreeDeleteEveryOtherRow(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(btree.CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	const numRows = 200
	for i := int64(1); i <= numRows; i++ {
		insertRow(t, bt, rootPage, btree.RowID(i), btree.IntValue(i))
	}

	// Delete even rows
	for i := int64(2); i <= numRows; i += 2 {
		cur, err := bt.Cursor(rootPage, true)
		if err != nil {
			t.Fatalf("Cursor for delete rowid=%d: %v", i, err)
		}
		result, err := cur.SeekRowid(btree.RowID(i))
		if err != nil {
			cur.Close()
			t.Fatalf("SeekRowid(%d): %v", i, err)
		}
		if result != btree.SeekFound {
			cur.Close()
			t.Fatalf("rowid %d not found for delete", i)
		}
		if err := bt.Delete(cur); err != nil {
			cur.Close()
			t.Fatalf("Delete rowid %d: %v", i, err)
		}
		cur.Close()
	}

	// Verify count
	count, _ := bt.Count(rootPage)
	expected := numRows / 2
	if count != int64(expected) {
		t.Errorf("after delete evens: count=%d, want %d", count, expected)
	}

	// Verify odd rows remain
	for i := int64(1); i <= numRows; i += 2 {
		cur, _ := bt.Cursor(rootPage, false)
		result, _ := cur.SeekRowid(btree.RowID(i))
		cur.Close()
		if result != btree.SeekFound {
			t.Errorf("odd rowid %d not found after deleting evens", i)
		}
	}

	// Verify even rows gone
	for i := int64(2); i <= numRows; i += 2 {
		cur, _ := bt.Cursor(rootPage, false)
		result, _ := cur.SeekRowid(btree.RowID(i))
		cur.Close()
		if result == btree.SeekFound {
			t.Errorf("even rowid %d still present after delete", i)
		}
	}

	// Verify scan order
	ids := collectAllRowIDs(t, bt, rootPage)
	if len(ids) != expected {
		t.Fatalf("scan count: got %d, want %d", len(ids), expected)
	}
	for _, id := range ids {
		if id%2 == 0 {
			t.Errorf("even rowid %d found in scan", id)
		}
	}

	// Integrity check
	var errs []string
	bt.IntegrityCheck(rootPage, 0, &errs)
	for _, e := range errs {
		t.Errorf("integrity: %s", e)
	}
}

// --- Test: Random insert/delete patterns ---

func TestBTreeRandomInsertDelete(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(btree.CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	rng := rand.New(rand.NewSource(42))
	const totalOps = 2000
	present := make(map[int64]bool)

	for op := 0; op < totalOps; op++ {
		rowid := rng.Int63n(500) + 1
		if present[rowid] {
			// Delete it
			cur, _ := bt.Cursor(rootPage, true)
			result, err := cur.SeekRowid(btree.RowID(rowid))
			if err != nil {
				cur.Close()
				t.Fatalf("op %d: SeekRowid(%d): %v", op, rowid, err)
			}
			if result == btree.SeekFound {
				if err := bt.Delete(cur); err != nil {
					cur.Close()
					t.Fatalf("op %d: Delete(%d): %v", op, rowid, err)
				}
				present[rowid] = false
			}
			cur.Close()
		} else {
			// Insert it
			insertRow(t, bt, rootPage, btree.RowID(rowid),
				btree.IntValue(rowid),
				btree.TextValue(fmt.Sprintf("v%d_op%d", rowid, op)),
			)
			present[rowid] = true
		}
	}

	// Count expected rows
	expected := 0
	for _, v := range present {
		if v {
			expected++
		}
	}

	count, _ := bt.Count(rootPage)
	if count != int64(expected) {
		t.Errorf("Count = %d, want %d", count, expected)
	}

	// Verify all present rows can be found and absent ones cannot
	for rowid, isPresent := range present {
		cur, _ := bt.Cursor(rootPage, false)
		result, _ := cur.SeekRowid(btree.RowID(rowid))
		cur.Close()
		if isPresent && result != btree.SeekFound {
			t.Errorf("rowid %d should be present but not found", rowid)
		}
		if !isPresent && result == btree.SeekFound {
			t.Errorf("rowid %d should be absent but found", rowid)
		}
	}

	// Integrity check
	var errs []string
	bt.IntegrityCheck(rootPage, 0, &errs)
	for _, e := range errs {
		t.Errorf("integrity: %s", e)
	}
}

// --- Test: Large payloads (overflow page chains) ---

func TestBTreeLargePayloadOverflow(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(btree.CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Create payloads of varying sizes to test overflow page handling
	sizes := []int{100, 500, 1000, 2000, 4000, 8000, 12000}

	for i, size := range sizes {
		bigData := make([]byte, size)
		for j := range bigData {
			bigData[j] = byte((j + i) % 256)
		}

		record := btree.MakeRecord([]btree.Value{
			btree.IntValue(int64(i + 1)),
			btree.BlobValue(bigData),
		})
		cur, _ := bt.Cursor(rootPage, true)
		err := bt.Insert(cur, nil, record, btree.RowID(i+1), btree.SeekNotFound)
		cur.Close()
		if err != nil {
			t.Fatalf("Insert size=%d: %v", size, err)
		}
	}

	// Read back and verify each row
	for i, size := range sizes {
		expectedData := make([]byte, size)
		for j := range expectedData {
			expectedData[j] = byte((j + i) % 256)
		}

		cur, _ := bt.Cursor(rootPage, false)
		result, _ := cur.SeekRowid(btree.RowID(i + 1))
		if result != btree.SeekFound {
			cur.Close()
			t.Fatalf("row %d (size=%d) not found", i+1, size)
		}
		data, err := cur.Data()
		cur.Close()
		if err != nil {
			t.Fatalf("Data row %d: %v", i+1, err)
		}

		values, err := btree.ParseRecord(data)
		if err != nil {
			t.Fatalf("ParseRecord row %d: %v", i+1, err)
		}
		if len(values) != 2 {
			t.Fatalf("row %d: expected 2 values, got %d", i+1, len(values))
		}
		if len(values[1].Bytes) != size {
			t.Fatalf("row %d: blob len=%d, want %d", i+1, len(values[1].Bytes), size)
		}
		for j := range expectedData {
			if values[1].Bytes[j] != expectedData[j] {
				t.Fatalf("row %d: blob mismatch at byte %d", i+1, j)
			}
		}
	}

	// Integrity check
	var errs []string
	bt.IntegrityCheck(rootPage, 0, &errs)
	for _, e := range errs {
		t.Errorf("integrity: %s", e)
	}
}

// --- Test: Multiple B-Tree instances ---

func TestBTreeMultipleInstances(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	bt.Begin(true)
	defer bt.Commit()

	// Create multiple tables
	const numTables = 5
	rootPages := make([]pager.PageNumber, numTables)
	for i := 0; i < numTables; i++ {
		root, err := bt.CreateBTree(btree.CreateTable)
		if err != nil {
			t.Fatalf("CreateBTree %d: %v", i, err)
		}
		rootPages[i] = root
	}

	// Insert different data into each table
	for i, root := range rootPages {
		for j := int64(1); j <= 50; j++ {
			insertRow(t, bt, root, btree.RowID(j),
				btree.IntValue(j),
				btree.TextValue(fmt.Sprintf("table%d_row%d", i, j)),
			)
		}
	}

	// Verify each table independently
	for i, root := range rootPages {
		count, err := bt.Count(root)
		if err != nil {
			t.Errorf("Count table %d: %v", i, err)
		}
		if count != 50 {
			t.Errorf("table %d: count=%d, want 50", i, count)
		}

		// Verify first and last row
		cur, _ := bt.Cursor(root, false)
		hasRow, _ := cur.First()
		if !hasRow {
			cur.Close()
			t.Errorf("table %d: empty", i)
			continue
		}
		if cur.RowID() != 1 {
			t.Errorf("table %d: first rowid=%d, want 1", i, cur.RowID())
		}
		hasRow, _ = cur.Last()
		if !hasRow {
			cur.Close()
			t.Errorf("table %d: empty (last)", i)
			continue
		}
		if cur.RowID() != 50 {
			t.Errorf("table %d: last rowid=%d, want 50", i, cur.RowID())
		}
		cur.Close()
	}

	// Integrity check all tables
	for i, root := range rootPages {
		var errs []string
		bt.IntegrityCheck(root, 0, &errs)
		for _, e := range errs {
			t.Errorf("table %d integrity: %s", i, e)
		}
	}
}

// --- Test: Stress create/drop tables ---

func TestBTreeStressCreateDrop(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	bt.Begin(true)
	defer bt.Commit()

	const numIterations = 50

	for i := 0; i < numIterations; i++ {
		// Create table
		root, err := bt.CreateBTree(btree.CreateTable)
		if err != nil {
			t.Fatalf("iter %d: CreateBTree: %v", i, err)
		}

		// Insert some rows
		for j := int64(1); j <= 10; j++ {
			insertRow(t, bt, root, btree.RowID(j), btree.IntValue(j))
		}

		// Verify count
		count, err := bt.Count(root)
		if err != nil {
			t.Fatalf("iter %d: Count: %v", i, err)
		}
		if count != 10 {
			t.Errorf("iter %d: count=%d, want 10", i, count)
		}

		// Clear table
		if err := bt.Clear(root); err != nil {
			t.Fatalf("iter %d: Clear: %v", i, err)
		}

		count, _ = bt.Count(root)
		if count != 0 {
			t.Errorf("iter %d: after clear count=%d, want 0", i, count)
		}
	}
}

// --- Test: Forward and backward scan consistency ---

func TestBTreeForwardBackwardConsistency(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(btree.CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert rows in random order
	rng := rand.New(rand.NewSource(123))
	rowids := make([]int64, 500)
	for i := range rowids {
		rowids[i] = int64(i + 1)
	}
	rng.Shuffle(len(rowids), func(i, j int) {
		rowids[i], rowids[j] = rowids[j], rowids[i]
	})

	for _, rid := range rowids {
		insertRow(t, bt, rootPage, btree.RowID(rid), btree.IntValue(rid))
	}

	// Forward scan
	forwardIDs := collectAllRowIDs(t, bt, rootPage)

	// Backward scan
	cur, _ := bt.Cursor(rootPage, false)
	var backwardIDs []int64
	hasRow, _ := cur.Last()
	for hasRow {
		backwardIDs = append(backwardIDs, int64(cur.RowID()))
		hasRow, _ = cur.Prev()
	}
	cur.Close()

	if len(forwardIDs) != len(backwardIDs) {
		t.Fatalf("forward=%d rows, backward=%d rows", len(forwardIDs), len(backwardIDs))
	}

	// Forward should be ascending, backward should be descending
	for i := range forwardIDs {
		if forwardIDs[i] != backwardIDs[len(backwardIDs)-1-i] {
			t.Errorf("mismatch at position %d: forward=%d, backward_rev=%d",
				i, forwardIDs[i], backwardIDs[len(backwardIDs)-1-i])
		}
	}

	// Both should be sorted
	if !sort.SliceIsSorted(forwardIDs, func(i, j int) bool {
		return forwardIDs[i] < forwardIDs[j]
	}) {
		t.Error("forward scan not sorted")
	}
}

// --- Test: Seek in multi-level tree ---

func TestBTreeSeekMultiLevel(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(btree.CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert enough to cause splits (more than fits in a single leaf)
	const numRows = 5000
	for i := int64(1); i <= numRows; i++ {
		insertRow(t, bt, rootPage, btree.RowID(i), btree.IntValue(i))
	}

	// Test seeking to specific positions
	tests := []struct {
		rowid     int64
		wantFound bool
	}{
		{1, true},
		{50, true},
		{500, true},
		{2500, true},
		{4999, true},
		{5000, true},
		{0, false},
		{5001, false},
		{10000, false},
		{-1, false},
	}

	for _, tt := range tests {
		cur, _ := bt.Cursor(rootPage, false)
		result, err := cur.SeekRowid(btree.RowID(tt.rowid))
		cur.Close()
		if err != nil {
			t.Errorf("SeekRowid(%d): %v", tt.rowid, err)
			continue
		}
		if tt.wantFound && result != btree.SeekFound {
			t.Errorf("SeekRowid(%d) = %d, want SeekFound", tt.rowid, result)
		}
		if !tt.wantFound && result == btree.SeekFound {
			t.Errorf("SeekRowid(%d) should not find", tt.rowid)
		}
	}
}

// --- Test: Delete all rows and reinsert ---

func TestBTreeDeleteAllAndReinsert(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(btree.CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert 100 rows
	for i := int64(1); i <= 100; i++ {
		insertRow(t, bt, rootPage, btree.RowID(i), btree.IntValue(i))
	}

	// Delete all rows
	for i := int64(1); i <= 100; i++ {
		cur, _ := bt.Cursor(rootPage, true)
		cur.SeekRowid(btree.RowID(i))
		bt.Delete(cur)
		cur.Close()
	}

	count, _ := bt.Count(rootPage)
	if count != 0 {
		t.Errorf("after delete all: count=%d, want 0", count)
	}

	// Reinsert with different data
	for i := int64(101); i <= 200; i++ {
		insertRow(t, bt, rootPage, btree.RowID(i),
			btree.IntValue(i),
			btree.TextValue("reinserted"),
		)
	}

	count, _ = bt.Count(rootPage)
	if count != 100 {
		t.Errorf("after reinsert: count=%d, want 100", count)
	}

	// Verify the reinserted data
	cur, _ := bt.Cursor(rootPage, false)
	hasRow, _ := cur.First()
	first := true
	for hasRow {
		if first {
			if cur.RowID() != 101 {
				t.Errorf("first rowid=%d, want 101", cur.RowID())
			}
			first = false
		}
		hasRow, _ = cur.Next()
	}
	cur.Close()
}

// --- Test: Update existing rows ---

func TestBTreeUpdateRows(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(btree.CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert rows
	for i := int64(1); i <= 50; i++ {
		insertRow(t, bt, rootPage, btree.RowID(i),
			btree.IntValue(i),
			btree.TextValue(fmt.Sprintf("original_%d", i)),
		)
	}

	// Update each row with new data
	for i := int64(1); i <= 50; i++ {
		cur, _ := bt.Cursor(rootPage, true)
		result, _ := cur.SeekRowid(btree.RowID(i))
		if result != btree.SeekFound {
			cur.Close()
			t.Fatalf("rowid %d not found for update", i)
		}
		newRecord := btree.MakeRecord([]btree.Value{
			btree.IntValue(i * 10),
			btree.TextValue(fmt.Sprintf("updated_%d", i)),
		})
		bt.Insert(cur, nil, newRecord, btree.RowID(i), btree.SeekFound)
		cur.Close()
	}

	// Verify updates
	for i := int64(1); i <= 50; i++ {
		cur, _ := bt.Cursor(rootPage, false)
		cur.SeekRowid(btree.RowID(i))
		data, _ := cur.Data()
		cur.Close()

		values, err := btree.ParseRecord(data)
		if err != nil {
			t.Fatalf("ParseRecord rowid %d: %v", i, err)
		}
		if values[0].IntVal != i*10 {
			t.Errorf("rowid %d: value=%d, want %d", i, values[0].IntVal, i*10)
		}
		expectedText := fmt.Sprintf("updated_%d", i)
		if string(values[1].Bytes) != expectedText {
			t.Errorf("rowid %d: text=%q, want %q", i, string(values[1].Bytes), expectedText)
		}
	}
}

// --- Test: Varint encoding edge cases ---

func TestBTreeVarintEdgeCases(t *testing.T) {
	tests := []struct {
		v int64
	}{
		{0},
		{1},
		{127},
		{128},
		{255},
		{256},
		{16383},
		{16384},
		{2097151},
		{2097152},
		{268435455},
		{268435456},
		{1<<31 - 1},
		{1 << 31},
		{1<<63 - 1}, // max int64
		{-1},
		{-128},
		{-32768},
		{-2147483648},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("varint_%d", tt.v), func(t *testing.T) {
			buf := make([]byte, 10)
			n := btree.PutVarint(buf, tt.v)
			if n == 0 {
				t.Fatalf("PutVarint returned 0")
			}

			got, rn := btree.ReadVarint(buf[:n])
			if got != tt.v {
				t.Errorf("ReadVarint = %d, want %d", got, tt.v)
			}
			if rn != n {
				t.Errorf("ReadVarint consumed %d bytes, wrote %d", rn, n)
			}

			// Also test VarintLen
			if vl := btree.VarintLen(tt.v); vl != n {
				t.Errorf("VarintLen = %d, wrote %d", vl, n)
			}
		})
	}
}

// --- Test: Record encoding with many columns ---

func TestBTreeRecordManyColumns(t *testing.T) {
	// Test with a large number of columns
	const numCols = 50
	values := make([]btree.Value, numCols)
	for i := range values {
		switch i % 4 {
		case 0:
			values[i] = btree.IntValue(int64(i))
		case 1:
			values[i] = btree.TextValue(fmt.Sprintf("col_%d", i))
		case 2:
			values[i] = btree.FloatValue(float64(i) + 0.5)
		case 3:
			values[i] = btree.NullValue()
		}
	}

	data := btree.MakeRecord(values)
	got, err := btree.ParseRecord(data)
	if err != nil {
		t.Fatalf("ParseRecord: %v", err)
	}
	if len(got) != numCols {
		t.Fatalf("expected %d values, got %d", numCols, len(got))
	}
	for i, w := range values {
		if w.Type != got[i].Type {
			t.Errorf("col %d: type=%q, want %q", i, got[i].Type, w.Type)
		}
	}
}

// --- Test: BTree on disk (file-backed) ---

func TestBTreeFilePersistence(t *testing.T) {
	bt, p := openTestBTreeFile(t)
	dir := t.TempDir()

	rootPage, _ := bt.CreateBTree(btree.CreateTable)
	bt.Begin(true)

	for i := int64(1); i <= 100; i++ {
		insertRow(t, bt, rootPage, btree.RowID(i),
			btree.IntValue(i),
			btree.TextValue(fmt.Sprintf("persist_%d", i)),
		)
	}
	bt.Commit()
	p.Close()

	// Re-open and verify
	cfg := pager.PagerConfig{
		VFS:         vfs.Find("unix"),
		Path:        filepath.Join(dir, "btree_test.db"),
		PageSize:    4096,
		CacheSize:   10,
		JournalMode: pager.JournalMemory,
	}
	p2, err := pager.OpenPager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p2.Open()
	defer p2.Close()

	bt2, err := btree.OpenBTreeConn(p2).Open(p2)
	if err != nil {
		t.Fatal(err)
	}
	bti := bt2.(*btree.BTreeImpl)

	count, _ := bti.Count(rootPage)
	if count != 100 {
		t.Errorf("after reopen: count=%d, want 100", count)
	}
}

// --- Test: Interleaved insert and delete ---

func TestBTreeInterleavedInsertDelete(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(btree.CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert 1-100
	for i := int64(1); i <= 100; i++ {
		insertRow(t, bt, rootPage, btree.RowID(i), btree.IntValue(i))
	}

	// Delete 1-50
	for i := int64(1); i <= 50; i++ {
		cur, _ := bt.Cursor(rootPage, true)
		cur.SeekRowid(btree.RowID(i))
		bt.Delete(cur)
		cur.Close()
	}

	// Insert 101-200
	for i := int64(101); i <= 200; i++ {
		insertRow(t, bt, rootPage, btree.RowID(i), btree.IntValue(i))
	}

	// Delete 51-100
	for i := int64(51); i <= 100; i++ {
		cur, _ := bt.Cursor(rootPage, true)
		cur.SeekRowid(btree.RowID(i))
		bt.Delete(cur)
		cur.Close()
	}

	// Should have 101-200
	count, _ := bt.Count(rootPage)
	if count != 100 {
		t.Errorf("count=%d, want 100", count)
	}

	ids := collectAllRowIDs(t, bt, rootPage)
	if len(ids) != 100 {
		t.Fatalf("scan: got %d rows, want 100", len(ids))
	}
	for _, id := range ids {
		if id < 101 || id > 200 {
			t.Errorf("unexpected rowid %d", id)
		}
	}

	// Integrity check
	var errs []string
	bt.IntegrityCheck(rootPage, 0, &errs)
	for _, e := range errs {
		t.Errorf("integrity: %s", e)
	}
}

// --- Test: Index B-Tree (key-value) ---

func TestBTreeIndexTree(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	rootPage, err := bt.CreateBTree(btree.CreateIndex)
	if err != nil {
		t.Fatalf("CreateBTree (index): %v", err)
	}

	bt.Begin(true)
	defer bt.Commit()

	// In an index B-Tree, the key contains the indexed value + rowid
	// Insert index entries
	for i := int64(1); i <= 100; i++ {
		key := btree.MakeRecord([]btree.Value{
			btree.TextValue(fmt.Sprintf("key_%03d", i)),
			btree.IntValue(i),
		})
		cur, _ := bt.Cursor(rootPage, true)
		if err := bt.Insert(cur, key, nil, 0, btree.SeekNotFound); err != nil {
			cur.Close()
			t.Fatalf("Insert index entry %d: %v", i, err)
		}
		cur.Close()
	}

	count, _ := bt.Count(rootPage)
	if count != 100 {
		t.Errorf("index count=%d, want 100", count)
	}

	// Verify ordered scan
	cur, _ := bt.Cursor(rootPage, false)
	defer cur.Close()
	lastKey := ""
	scanned := 0
	hasRow, _ := cur.First()
	for hasRow {
		scanned++
		key := cur.Key()
		if string(key) <= lastKey {
			t.Errorf("index out of order: %q after %q", string(key), lastKey)
		}
		lastKey = string(key)
		hasRow, _ = cur.Next()
	}
	if scanned != 100 {
		t.Errorf("index scan: %d rows, want 100", scanned)
	}
}

// --- Test: Empty B-Tree operations ---

func TestBTreeEmptyOperations(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(btree.CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Count on empty tree
	count, _ := bt.Count(rootPage)
	if count != 0 {
		t.Errorf("empty tree count=%d, want 0", count)
	}

	// Scan empty tree
	cur, _ := bt.Cursor(rootPage, false)
	hasRow, err := cur.First()
	if hasRow {
		t.Error("expected no rows in empty tree (First)")
	}
	if err != nil {
		t.Errorf("First on empty: %v", err)
	}
	hasRow, err = cur.Last()
	if hasRow {
		t.Error("expected no rows in empty tree (Last)")
	}
	cur.Close()

	// Seek on empty tree
	cur, _ = bt.Cursor(rootPage, false)
	result, _ := cur.SeekRowid(1)
	if result == btree.SeekFound {
		t.Error("expected SeekRowid to not find in empty tree")
	}
	cur.Close()
}

// --- Test: Sequential insert with sequential scan ---

func TestBTreeSequentialInsert(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(btree.CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert in order 1..1000
	const n = 1000
	for i := int64(1); i <= n; i++ {
		insertRow(t, bt, rootPage, btree.RowID(i), btree.IntValue(i))
	}

	// Verify all rows in order
	cur, _ := bt.Cursor(rootPage, false)
	defer cur.Close()
	expected := int64(1)
	hasRow, _ := cur.First()
	for hasRow {
		if cur.RowID() != btree.RowID(expected) {
			t.Errorf("rowid=%d, want %d", cur.RowID(), expected)
		}
		expected++
		hasRow, _ = cur.Next()
	}
	if expected != n+1 {
		t.Errorf("scanned %d rows, want %d", expected-1, n)
	}
}

// --- Test: Reverse insert with ordered scan ---

func TestBTreeReverseInsert(t *testing.T) {
	bt, p := openTestBTreeMem(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(btree.CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert in reverse order 1000..1
	const n = 1000
	for i := int64(n); i >= 1; i-- {
		insertRow(t, bt, rootPage, btree.RowID(i), btree.IntValue(i))
	}

	// Should still scan in order 1..1000
	ids := collectAllRowIDs(t, bt, rootPage)
	if len(ids) != n {
		t.Fatalf("scan: %d rows, want %d", len(ids), n)
	}
	for i, id := range ids {
		if id != int64(i+1) {
			t.Errorf("position %d: rowid=%d, want %d", i, id, i+1)
		}
	}
}
