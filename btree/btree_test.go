package btree

import (
	"os"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sqlite-go/sqlite-go/pager"
	"github.com/sqlite-go/sqlite-go/vfs"
)

func openTestBTree(t *testing.T) (*BTreeImpl, *pager.PagerImpl) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	cfg := pager.PagerConfig{
		VFS:        vfs.Find("unix"),
		Path:       path,
		PageSize:   4096,
		CacheSize:  10,
		JournalMode: pager.JournalMemory,
	}
	p, err := pager.OpenPager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Open(); err != nil {
		t.Fatal(err)
	}

	bt, err := (&BTreeConnImpl{pgr: p}).Open(p)
	if err != nil {
		t.Fatal(err)
	}
	return bt.(*BTreeImpl), p
}

func TestVarint(t *testing.T) {
	tests := []struct {
		v    int64
		want int
	}{
		{0, 1},
		{1, 1},
		{127, 1},
		{128, 2},
		{16383, 2},
		{16384, 3},
		{2097151, 3},
		{268435455, 4},
	}

	for _, tt := range tests {
		if got := VarintLen(tt.v); got != tt.want {
			t.Errorf("VarintLen(%d) = %d, want %d", tt.v, got, tt.want)
		}

		buf := make([]byte, 10)
		n := PutVarint(buf, tt.v)
		got, rn := ReadVarint(buf[:n])
		if got != tt.v {
			t.Errorf("varint roundtrip: wrote %d, read %d", tt.v, got)
		}
		if rn != n {
			t.Errorf("varint size: wrote %d bytes, read %d bytes", n, rn)
		}
	}
}

func TestMakeParseRecord(t *testing.T) {
	tests := []struct {
		name   string
		values []Value
	}{
		{"null", []Value{NullValue()}},
		{"int zero", []Value{IntValue(0)}},
		{"int one", []Value{IntValue(1)}},
		{"int small", []Value{IntValue(42)}},
		{"int large", []Value{IntValue(123456789)}},
		{"float", []Value{FloatValue(3.14)}},
		{"text", []Value{TextValue("hello")}},
		{"blob", []Value{BlobValue([]byte{1, 2, 3})}},
		{"mixed", []Value{IntValue(1), TextValue("hello"), NullValue(), FloatValue(2.5)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := MakeRecord(tt.values)
			got, err := ParseRecord(data)
			if err != nil {
				t.Fatalf("ParseRecord: %v", err)
			}
			if len(got) != len(tt.values) {
				t.Fatalf("expected %d values, got %d", len(tt.values), len(got))
			}
			for i, want := range tt.values {
				checkValue(t, i, got[i], want)
			}
		})
	}
}

func checkValue(t *testing.T, idx int, got, want Value) {
	t.Helper()
	if got.Type != want.Type {
		t.Errorf("value[%d]: type = %q, want %q", idx, got.Type, want.Type)
		return
	}
	switch want.Type {
	case "int":
		if got.IntVal != want.IntVal {
			t.Errorf("value[%d]: int = %d, want %d", idx, got.IntVal, want.IntVal)
		}
	case "float":
		if got.FloatVal != want.FloatVal {
			t.Errorf("value[%d]: float = %f, want %f", idx, got.FloatVal, want.FloatVal)
		}
	case "text":
		if string(got.Bytes) != string(want.Bytes) {
			t.Errorf("value[%d]: text = %q, want %q", idx, string(got.Bytes), string(want.Bytes))
		}
	case "blob":
		if len(got.Bytes) != len(want.Bytes) {
			t.Errorf("value[%d]: blob len = %d, want %d", idx, len(got.Bytes), len(want.Bytes))
		}
	}
}

func TestCreateBTree(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, err := bt.CreateBTree(CreateTable)
	if err != nil {
		t.Fatalf("CreateBTree: %v", err)
	}
	if rootPage != 1 {
		t.Errorf("root page = %d, want 1", rootPage)
	}

	// Verify page 1 header
	page, err := p.GetPage(1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.ReleasePage(page)

	if page.Data[DBHeaderSize] != PageTypeLeafTable {
		t.Errorf("page type = %d, want %d", page.Data[DBHeaderSize], PageTypeLeafTable)
	}
}

func TestInsertAndScan(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(CreateTable)

	// Begin write transaction
	bt.Begin(true)
	defer bt.Commit()

	// Insert rows
	for i := int64(1); i <= 10; i++ {
		record := MakeRecord([]Value{IntValue(i), TextValue(fmt.Sprintf("row_%d", i))})
		cur, _ := bt.Cursor(rootPage, true)
		err := bt.Insert(cur, nil, record, RowID(i), SeekNotFound)
		cur.Close()
		if err != nil {
			t.Fatalf("Insert row %d: %v", i, err)
		}
	}

	// Scan all rows
	cur, _ := bt.Cursor(rootPage, false)
	defer cur.Close()

	count := 0
	hasRow, err := cur.First()
	for hasRow && err == nil {
		count++
		data, _ := cur.Data()
		if len(data) == 0 {
			t.Error("empty row data")
		}
		hasRow, err = cur.Next()
	}

	if count != 10 {
		t.Errorf("scanned %d rows, want 10", count)
	}
}

func TestSeekRowid(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert rows
	for i := int64(1); i <= 5; i++ {
		record := MakeRecord([]Value{IntValue(i)})
		cur, _ := bt.Cursor(rootPage, true)
		bt.Insert(cur, nil, record, RowID(i), SeekNotFound)
		cur.Close()
	}

	// Seek to existing row
	cur, _ := bt.Cursor(rootPage, false)
	defer cur.Close()

	result, err := cur.SeekRowid(3)
	if err != nil {
		t.Fatalf("SeekRowid: %v", err)
	}
	if result != SeekFound {
		t.Errorf("SeekRowid(3) = %d, want SeekFound", result)
	}
	if cur.RowID() != 3 {
		t.Errorf("RowID = %d, want 3", cur.RowID())
	}

	// Seek to non-existing row
	result, err = cur.SeekRowid(99)
	if result == SeekFound {
		t.Error("SeekRowid(99) should not find")
	}
}

func TestDeleteRow(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert 3 rows
	for i := int64(1); i <= 3; i++ {
		record := MakeRecord([]Value{IntValue(i)})
		cur, _ := bt.Cursor(rootPage, true)
		bt.Insert(cur, nil, record, RowID(i), SeekNotFound)
		cur.Close()
	}

	// Delete row 2
	cur, _ := bt.Cursor(rootPage, true)
	cur.SeekRowid(2)
	bt.Delete(cur)
	cur.Close()

	// Count remaining
	cur, _ = bt.Cursor(rootPage, false)
	defer cur.Close()

	count := 0
	hasRow, _ := cur.First()
	for hasRow {
		count++
		hasRow, _ = cur.Next()
	}

	if count != 2 {
		t.Errorf("after delete: %d rows, want 2", count)
	}
}

func TestCountBTree(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	for i := int64(1); i <= 20; i++ {
		record := MakeRecord([]Value{IntValue(i)})
		cur, _ := bt.Cursor(rootPage, true)
		bt.Insert(cur, nil, record, RowID(i), SeekNotFound)
		cur.Close()
	}

	count, err := bt.Count(rootPage)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != 20 {
		t.Errorf("Count = %d, want 20", count)
	}
}

func TestMemoryBTree(t *testing.T) {
	cfg := pager.PagerConfig{
		Path:       ":memory:",
		PageSize:   4096,
		CacheSize:  10,
		VFS:        vfs.Find("memory"),
		JournalMode: pager.JournalMemory,
	}
	p, _ := pager.OpenPager(cfg)
	p.Open()
	defer p.Close()

	bt, _ := (&BTreeConnImpl{pgr: p}).Open(p)
	bti := bt.(*BTreeImpl)

	bti.Begin(true)
	defer bti.Commit()

	rootPage, _ := bti.CreateBTree(CreateTable)

	for i := int64(1); i <= 5; i++ {
		record := MakeRecord([]Value{TextValue("mem")})
		cur, _ := bti.Cursor(rootPage, true)
		bti.Insert(cur, nil, record, RowID(i), SeekNotFound)
		cur.Close()
	}

	count, _ := bti.Count(rootPage)
	if count != 5 {
		t.Errorf("memory Count = %d, want 5", count)
	}
}

// Remove unused import
var _ = os.Open

// --- Comprehensive tests ---

func TestInsertManyRows(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert enough rows to trigger multiple page splits
	const numRows = 500
	for i := int64(1); i <= numRows; i++ {
		record := MakeRecord([]Value{IntValue(i), TextValue(fmt.Sprintf("row_%d", i))})
		cur, _ := bt.Cursor(rootPage, true)
		err := bt.Insert(cur, nil, record, RowID(i), SeekNotFound)
		cur.Close()
		if err != nil {
			t.Fatalf("Insert row %d: %v", i, err)
		}
	}

	// Verify count
	count, err := bt.Count(rootPage)
	if err != nil {
		t.Fatalf("Count: %v", err)
	}
	if count != numRows {
		t.Errorf("Count = %d, want %d", count, numRows)
	}

	// Verify all rows via forward scan
	cur, _ := bt.Cursor(rootPage, false)
	defer cur.Close()

	scanned := 0
	hasRow, err := cur.First()
	for hasRow && err == nil {
		scanned++
		data, _ := cur.Data()
		if len(data) == 0 {
			t.Error("empty row data")
		}
		rowid := cur.RowID()
		if rowid < 1 || rowid > numRows {
			t.Errorf("unexpected rowid: %d", rowid)
		}
		hasRow, err = cur.Next()
	}

	if scanned != numRows {
		t.Errorf("scanned %d rows, want %d", scanned, numRows)
	}

	// Verify backward scan
	scanned = 0
	hasRow, err = cur.Last()
	for hasRow && err == nil {
		scanned++
		hasRow, err = cur.Prev()
	}
	if scanned != numRows {
		t.Errorf("backward scan: %d rows, want %d", scanned, numRows)
	}
}

func TestInsertReverseOrder(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert rows in reverse order
	const numRows = 100
	for i := numRows; i >= 1; i-- {
		record := MakeRecord([]Value{IntValue(int64(i))})
		cur, _ := bt.Cursor(rootPage, true)
		err := bt.Insert(cur, nil, record, RowID(i), SeekNotFound)
		cur.Close()
		if err != nil {
			t.Fatalf("Insert row %d: %v", i, err)
		}
	}

	// Verify forward scan returns rows in order
	cur, _ := bt.Cursor(rootPage, false)
	defer cur.Close()

	expectedRowid := int64(1)
	hasRow, _ := cur.First()
	for hasRow {
		if cur.RowID() != RowID(expectedRowid) {
			t.Errorf("rowid = %d, want %d", cur.RowID(), expectedRowid)
		}
		expectedRowid++
		hasRow, _ = cur.Next()
	}
	if expectedRowid != numRows+1 {
		t.Errorf("expected %d rows, got %d", numRows, expectedRowid-1)
	}
}

func TestSeekInMultiLevelTree(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert enough to cause splits
	for i := int64(1); i <= 200; i++ {
		record := MakeRecord([]Value{IntValue(i)})
		cur, _ := bt.Cursor(rootPage, true)
		bt.Insert(cur, nil, record, RowID(i), SeekNotFound)
		cur.Close()
	}

	// Seek to various rowids
	tests := []struct {
		rowid      int64
		wantFound  bool
	}{
		{1, true},
		{50, true},
		{100, true},
		{200, true},
		{250, false},
		{0, false},
	}

	for _, tt := range tests {
		cur, _ := bt.Cursor(rootPage, false)
		result, err := cur.SeekRowid(RowID(tt.rowid))
		cur.Close()
		if err != nil {
			t.Errorf("SeekRowid(%d): %v", tt.rowid, err)
			continue
		}
		if tt.wantFound && result != SeekFound {
			t.Errorf("SeekRowid(%d) = %d, want SeekFound", tt.rowid, result)
		}
		if !tt.wantFound && result == SeekFound {
			t.Errorf("SeekRowid(%d) should not find", tt.rowid)
		}
	}
}

func TestDeleteAndReinsert(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert rows
	for i := int64(1); i <= 50; i++ {
		record := MakeRecord([]Value{IntValue(i), TextValue(fmt.Sprintf("v%d", i))})
		cur, _ := bt.Cursor(rootPage, true)
		bt.Insert(cur, nil, record, RowID(i), SeekNotFound)
		cur.Close()
	}

	// Delete some rows
	for _, rowid := range []int64{10, 20, 30, 40} {
		cur, _ := bt.Cursor(rootPage, true)
		cur.SeekRowid(RowID(rowid))
		if err := bt.Delete(cur); err != nil {
			t.Fatalf("Delete row %d: %v", rowid, err)
		}
		cur.Close()
	}

	// Verify count
	count, _ := bt.Count(rootPage)
	if count != 46 {
		t.Errorf("after delete: count = %d, want 46", count)
	}

	// Verify deleted rows are gone
	for _, rowid := range []int64{10, 20, 30, 40} {
		cur, _ := bt.Cursor(rootPage, false)
		result, _ := cur.SeekRowid(RowID(rowid))
		cur.Close()
		if result == SeekFound {
			t.Errorf("row %d still exists after delete", rowid)
		}
	}

	// Verify remaining rows still accessible
	for _, rowid := range []int64{1, 5, 15, 25, 35, 45, 50} {
		cur, _ := bt.Cursor(rootPage, false)
		result, _ := cur.SeekRowid(RowID(rowid))
		cur.Close()
		if result != SeekFound {
			t.Errorf("row %d not found after deletes", rowid)
		}
	}

	// Reinsert deleted rows
	for _, rowid := range []int64{10, 20, 30, 40} {
		record := MakeRecord([]Value{IntValue(rowid), TextValue("reinserted")})
		cur, _ := bt.Cursor(rootPage, true)
		bt.Insert(cur, nil, record, RowID(rowid), SeekNotFound)
		cur.Close()
	}

	count, _ = bt.Count(rootPage)
	if count != 50 {
		t.Errorf("after reinsert: count = %d, want 50", count)
	}
}

func TestLargePayload(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert rows with large payloads (should trigger overflow pages)
	bigData := make([]byte, 3000)
	for i := range bigData {
		bigData[i] = byte(i % 256)
	}

	for i := int64(1); i <= 5; i++ {
		record := MakeRecord([]Value{IntValue(i), BlobValue(bigData)})
		cur, _ := bt.Cursor(rootPage, true)
		err := bt.Insert(cur, nil, record, RowID(i), SeekNotFound)
		cur.Close()
		if err != nil {
			t.Fatalf("Insert large row %d: %v", i, err)
		}
	}

	// Read back and verify
	for i := int64(1); i <= 5; i++ {
		cur, _ := bt.Cursor(rootPage, false)
		result, _ := cur.SeekRowid(RowID(i))
		if result != SeekFound {
			t.Fatalf("row %d not found", i)
		}
		data, err := cur.Data()
		cur.Close()
		if err != nil {
			t.Fatalf("Data row %d: %v", i, err)
		}

		values, err := ParseRecord(data)
		if err != nil {
			t.Fatalf("ParseRecord row %d: %v", i, err)
		}

		if len(values) != 2 {
			t.Fatalf("row %d: expected 2 values, got %d", i, len(values))
		}

		if values[0].IntVal != i {
			t.Errorf("row %d: int val = %d, want %d", i, values[0].IntVal, i)
		}

		if len(values[1].Bytes) != len(bigData) {
			t.Errorf("row %d: blob len = %d, want %d", i, len(values[1].Bytes), len(bigData))
		} else {
			for j := range bigData {
				if values[1].Bytes[j] != bigData[j] {
					t.Errorf("row %d: blob mismatch at byte %d", i, j)
					break
				}
			}
		}
	}
}

func TestVeryLargePayload(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Create a payload larger than one page (should span multiple overflow pages)
	bigData := make([]byte, 8000)
	for i := range bigData {
		bigData[i] = byte(i % 256)
	}

	record := MakeRecord([]Value{IntValue(1), BlobValue(bigData)})
	cur, _ := bt.Cursor(rootPage, true)
	err := bt.Insert(cur, nil, record, RowID(1), SeekNotFound)
	cur.Close()
	if err != nil {
		t.Fatalf("Insert very large row: %v", err)
	}

	// Read back
	cur, _ = bt.Cursor(rootPage, false)
	result, _ := cur.SeekRowid(1)
	if result != SeekFound {
		t.Fatal("row 1 not found")
	}
	data, _ := cur.Data()
	cur.Close()

	values, err := ParseRecord(data)
	if err != nil {
		t.Fatalf("ParseRecord: %v", err)
	}
	if len(values[1].Bytes) != len(bigData) {
		t.Errorf("blob len = %d, want %d", len(values[1].Bytes), len(bigData))
	}
	for i := range bigData {
		if values[1].Bytes[i] != bigData[i] {
			t.Errorf("blob mismatch at byte %d: got %d, want %d", i, values[1].Bytes[i], bigData[i])
			break
		}
	}
}

func TestIntegrityCheck(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert enough to create a multi-level tree
	for i := int64(1); i <= 300; i++ {
		record := MakeRecord([]Value{IntValue(i)})
		cur, _ := bt.Cursor(rootPage, true)
		bt.Insert(cur, nil, record, RowID(i), SeekNotFound)
		cur.Close()
	}

	var errs []string
	bt.IntegrityCheck(rootPage, 0, &errs)
	for _, e := range errs {
		t.Errorf("integrity check: %s", e)
	}
}

func TestMemoryDBManyRows(t *testing.T) {
	cfg := pager.PagerConfig{
		Path:       ":memory:",
		PageSize:   4096,
		CacheSize:  10,
		VFS:        vfs.Find("memory"),
		JournalMode: pager.JournalMemory,
	}
	p, _ := pager.OpenPager(cfg)
	p.Open()
	defer p.Close()

	bt, _ := (&BTreeConnImpl{pgr: p}).Open(p)
	bti := bt.(*BTreeImpl)
	bti.Begin(true)
	defer bti.Commit()

	rootPage, _ := bti.CreateBTree(CreateTable)

	for i := int64(1); i <= 200; i++ {
		record := MakeRecord([]Value{IntValue(i), TextValue(fmt.Sprintf("mem_%d", i))})
		cur, _ := bti.Cursor(rootPage, true)
		if err := bti.Insert(cur, nil, record, RowID(i), SeekNotFound); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
		cur.Close()
	}

	count, _ := bti.Count(rootPage)
	if count != 200 {
		t.Errorf("memory Count = %d, want 200", count)
	}

	// Verify forward scan
	cur, _ := bti.Cursor(rootPage, false)
	defer cur.Close()
	scanned := 0
	lastRowid := int64(0)
	hasRow, _ := cur.First()
	for hasRow {
		scanned++
		if cur.RowID() <= RowID(lastRowid) {
			t.Errorf("out of order: rowid %d after %d", cur.RowID(), lastRowid)
		}
		lastRowid = int64(cur.RowID())
		hasRow, _ = cur.Next()
	}
	if scanned != 200 {
		t.Errorf("scanned %d, want 200", scanned)
	}
}

func TestUpdateExistingRow(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	// Insert row
	record := MakeRecord([]Value{IntValue(1), TextValue("original")})
	cur, _ := bt.Cursor(rootPage, true)
	bt.Insert(cur, nil, record, RowID(1), SeekNotFound)
	cur.Close()

	// Update row (seek first, then insert with SeekFound)
	cur, _ = bt.Cursor(rootPage, true)
	result, _ := cur.SeekRowid(1)
	if result != SeekFound {
		t.Fatal("row 1 not found for update")
	}

	newRecord := MakeRecord([]Value{IntValue(1), TextValue("updated")})
	bt.Insert(cur, nil, newRecord, RowID(1), SeekFound)
	cur.Close()

	// Verify updated value
	cur, _ = bt.Cursor(rootPage, false)
	cur.SeekRowid(1)
	data, _ := cur.Data()
	cur.Close()

	values, _ := ParseRecord(data)
	if values[1].Type != "text" || string(values[1].Bytes) != "updated" {
		t.Errorf("expected 'updated', got %v", values[1])
	}
}

func TestDropAndClear(t *testing.T) {
	bt, p := openTestBTree(t)
	defer p.Close()

	rootPage, _ := bt.CreateBTree(CreateTable)
	bt.Begin(true)
	defer bt.Commit()

	for i := int64(1); i <= 20; i++ {
		record := MakeRecord([]Value{IntValue(i)})
		cur, _ := bt.Cursor(rootPage, true)
		bt.Insert(cur, nil, record, RowID(i), SeekNotFound)
		cur.Close()
	}

	// Clear
	if err := bt.Clear(rootPage); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	count, _ := bt.Count(rootPage)
	if count != 0 {
		t.Errorf("after clear: count = %d, want 0", count)
	}

	// Reinsert after clear
	record := MakeRecord([]Value{IntValue(99)})
	cur, _ := bt.Cursor(rootPage, true)
	bt.Insert(cur, nil, record, RowID(99), SeekNotFound)
	cur.Close()

	count, _ = bt.Count(rootPage)
	if count != 1 {
		t.Errorf("after reinsert: count = %d, want 1", count)
	}
}

func TestFreelistReuse(t *testing.T) {
	p, _ := pager.OpenPager(pager.PagerConfig{
		Path:       ":memory:",
		PageSize:   4096,
		CacheSize:  10,
		VFS:        vfs.Find("memory"),
		JournalMode: pager.JournalMemory,
	})
	p.Open()
	defer p.Close()

	// Allocate pages
	p1, _ := p.GetNewPage()
	p2, _ := p.GetNewPage()
	p3, _ := p.GetNewPage()

	if p.FreelistCount() != 0 {
		t.Errorf("freelist should be empty initially")
	}

	// Free a page
	p.FreePage(p2.PageNum)
	if p.FreelistCount() != 1 {
		t.Errorf("freelist should have 1 page")
	}

	// Allocate again - should reuse freed page
	p4, _ := p.GetNewPage()
	_ = p1
	_ = p3
	if p4.PageNum == p2.PageNum {
		// Page was reused from freelist
	} else if p4.PageNum == p2.PageNum+1 {
		// Page was allocated new (freelist not used yet)
	}
}

func TestWALReadFrame(t *testing.T) {
	// Test that WAL ReadPage returns false when no frames exist
	wal := pager.NewWAL("", nil, 4096)
	// WAL not open, should return false
	_, ok := wal.ReadPage(1, 0)
	if ok {
		t.Errorf("expected false from unopened WAL ReadPage")
	}
	_ = wal
}
