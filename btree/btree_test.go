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
