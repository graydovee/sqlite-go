package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sqlite-go/sqlite-go/btree"
	"github.com/sqlite-go/sqlite-go/pager"
	"github.com/sqlite-go/sqlite-go/sqlite"
	"github.com/sqlite-go/sqlite-go/vfs"
)

// ============================================================================
// Performance benchmarks
// ============================================================================

// --- SQL-level benchmarks ---

// BenchmarkInsert measures INSERT performance.
func BenchmarkInsert(b *testing.B) {
	db, err := sqlite.OpenInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INTEGER, name TEXT, score REAL)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec("INSERT INTO bench VALUES (?, ?, ?)", i, fmt.Sprintf("name_%d", i), float64(i)*1.5)
	}
	b.StopTimer()
}

// BenchmarkInsertPrepared measures INSERT performance with prepared statements.
func BenchmarkInsertPrepared(b *testing.B) {
	db, err := sqlite.OpenInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INTEGER, name TEXT, score REAL)")

	stmt, err := db.Prepare("INSERT INTO bench VALUES (?, ?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Finalize()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmt.Bind(1, i)
		stmt.Bind(2, fmt.Sprintf("name_%d", i))
		stmt.Bind(3, float64(i)*1.5)
		stmt.Reset()
	}
}

// BenchmarkSelect measures SELECT performance.
func BenchmarkSelect(b *testing.B) {
	db, err := sqlite.OpenInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INTEGER, name TEXT, score REAL)")

	// Pre-populate
	const numRows = 1000
	for i := 0; i < numRows; i++ {
		db.Exec("INSERT INTO bench VALUES (?, ?, ?)", i, fmt.Sprintf("name_%d", i), float64(i)*1.5)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := db.Query("SELECT id, name, score FROM bench")
		if err != nil {
			b.Fatal(err)
		}
		for rs.Next() {
			_ = rs.Row().ColumnInt(0)
			_ = rs.Row().ColumnText(1)
			_ = rs.Row().ColumnFloat(2)
		}
		rs.Close()
	}
}

// BenchmarkSelectExpression measures SELECT with expression evaluation.
func BenchmarkSelectExpression(b *testing.B) {
	db, err := sqlite.OpenInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := db.Query("SELECT 1 + 2 * 3")
		if err != nil {
			b.Fatal(err)
		}
		rs.Next()
		rs.Close()
	}
}

// BenchmarkUpdate measures UPDATE performance.
func BenchmarkUpdate(b *testing.B) {
	db, err := sqlite.OpenInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INTEGER, val INTEGER)")

	// Pre-populate
	const numRows = 1000
	for i := 0; i < numRows; i++ {
		db.Exec("INSERT INTO bench VALUES (?, ?)", i, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec("UPDATE bench SET val = val + 1")
	}
	b.StopTimer()
}

// BenchmarkDelete measures DELETE performance.
func BenchmarkDelete(b *testing.B) {
	db, err := sqlite.OpenInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INTEGER, val TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Re-populate for each iteration
		for j := 0; j < 100; j++ {
			db.Exec("INSERT INTO bench VALUES (?, ?)", j, fmt.Sprintf("val_%d", j))
		}
		b.StartTimer()

		db.Exec("DELETE FROM bench")
	}
}

// BenchmarkCreateTable measures CREATE TABLE performance.
func BenchmarkCreateTable(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db, err := sqlite.OpenInMemory()
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		db.Exec(fmt.Sprintf("CREATE TABLE t%d (id INTEGER, name TEXT, val REAL)", i))
		db.Close()
	}
}

// BenchmarkCreateTableComplex measures CREATE TABLE with constraints.
func BenchmarkCreateTableComplex(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db, err := sqlite.OpenInMemory()
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		db.Exec(fmt.Sprintf(`CREATE TABLE t%d (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			email TEXT UNIQUE,
			age INTEGER DEFAULT 0,
			score REAL
		)`, i))
		db.Close()
	}
}

// BenchmarkSelectWithWhere measures SELECT with WHERE clause.
func BenchmarkSelectWhere(b *testing.B) {
	db, err := sqlite.OpenInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INTEGER, val TEXT)")

	const numRows = 1000
	for i := 0; i < numRows; i++ {
		db.Exec("INSERT INTO bench VALUES (?, ?)", i, fmt.Sprintf("val_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := db.Query("SELECT id, val FROM bench WHERE id = ?", i%numRows)
		if err != nil {
			b.Skipf("WHERE not supported: %v", err)
		}
		for rs.Next() {
		}
		rs.Close()
	}
}

// BenchmarkTransaction measures transaction performance.
func BenchmarkTransaction(b *testing.B) {
	db, err := sqlite.OpenInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INTEGER, val TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Begin()
		for j := 0; j < 100; j++ {
			db.Exec("INSERT INTO bench VALUES (?, ?)", i*100+j, fmt.Sprintf("val_%d", i*100+j))
		}
		db.Commit()
	}
	b.StopTimer()
}

// BenchmarkMultipleTables measures operations across multiple tables.
func BenchmarkMultipleTables(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db, err := sqlite.OpenInMemory()
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()

		// Create 10 tables
		for j := 0; j < 10; j++ {
			db.Exec(fmt.Sprintf("CREATE TABLE t%d (id INTEGER, val TEXT)", j))
		}

		// Insert into each
		for j := 0; j < 10; j++ {
			for k := 0; k < 10; k++ {
				db.Exec(fmt.Sprintf("INSERT INTO t%d VALUES (%d, 'val')", j, k))
			}
		}

		// Query each
		for j := 0; j < 10; j++ {
			rs, _ := db.Query(fmt.Sprintf("SELECT * FROM t%d", j))
			if rs != nil {
				for rs.Next() {
				}
				rs.Close()
			}
		}

		db.Close()
	}
}

// --- B-Tree level benchmarks ---

// BenchmarkBTreeInsert measures raw B-Tree INSERT performance.
func BenchmarkBTreeInsert(b *testing.B) {
	cfg := pager.PagerConfig{
		Path:        ":memory:",
		PageSize:    4096,
		CacheSize:   100,
		VFS:         vfs.Find("memory"),
		JournalMode: pager.JournalMemory,
	}
	p, _ := pager.OpenPager(cfg)
	p.Open()
	defer p.Close()

	bt, _ := btree.OpenBTreeConn(p).Open(p)
	bti := bt.(*btree.BTreeImpl)
	rootPage, _ := bti.CreateBTree(btree.CreateTable)
	bti.Begin(true)
	defer bti.Commit()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := btree.MakeRecord([]btree.Value{
			btree.IntValue(int64(i)),
			btree.TextValue(fmt.Sprintf("val_%d", i)),
		})
		cur, _ := bti.Cursor(rootPage, true)
		bti.Insert(cur, nil, record, btree.RowID(i+1), btree.SeekNotFound)
		cur.Close()
	}
}

// BenchmarkBTreeScan measures B-Tree forward scan performance.
func BenchmarkBTreeScan(b *testing.B) {
	cfg := pager.PagerConfig{
		Path:        ":memory:",
		PageSize:    4096,
		CacheSize:   100,
		VFS:         vfs.Find("memory"),
		JournalMode: pager.JournalMemory,
	}
	p, _ := pager.OpenPager(cfg)
	p.Open()
	defer p.Close()

	bt, _ := btree.OpenBTreeConn(p).Open(p)
	bti := bt.(*btree.BTreeImpl)
	rootPage, _ := bti.CreateBTree(btree.CreateTable)
	bti.Begin(true)

	const numRows = 10000
	for i := int64(1); i <= numRows; i++ {
		record := btree.MakeRecord([]btree.Value{btree.IntValue(i)})
		cur, _ := bti.Cursor(rootPage, true)
		bti.Insert(cur, nil, record, btree.RowID(i), btree.SeekNotFound)
		cur.Close()
	}
	bti.Commit()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, _ := bti.Cursor(rootPage, false)
		hasRow, _ := cur.First()
		for hasRow {
			_, _ = cur.Data()
			hasRow, _ = cur.Next()
		}
		cur.Close()
	}
}

// BenchmarkBTreeSeek measures B-Tree seek performance.
func BenchmarkBTreeSeek(b *testing.B) {
	cfg := pager.PagerConfig{
		Path:        ":memory:",
		PageSize:    4096,
		CacheSize:   100,
		VFS:         vfs.Find("memory"),
		JournalMode: pager.JournalMemory,
	}
	p, _ := pager.OpenPager(cfg)
	p.Open()
	defer p.Close()

	bt, _ := btree.OpenBTreeConn(p).Open(p)
	bti := bt.(*btree.BTreeImpl)
	rootPage, _ := bti.CreateBTree(btree.CreateTable)
	bti.Begin(true)

	const numRows = 10000
	for i := int64(1); i <= numRows; i++ {
		record := btree.MakeRecord([]btree.Value{btree.IntValue(i)})
		cur, _ := bti.Cursor(rootPage, true)
		bti.Insert(cur, nil, record, btree.RowID(i), btree.SeekNotFound)
		cur.Close()
	}
	bti.Commit()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cur, _ := bti.Cursor(rootPage, false)
		cur.SeekRowid(btree.RowID(i%numRows + 1))
		cur.Close()
	}
}

// --- Record encoding benchmarks ---

// BenchmarkMakeRecord measures record creation performance.
func BenchmarkMakeRecord(b *testing.B) {
	values := []btree.Value{
		btree.IntValue(42),
		btree.TextValue("hello world"),
		btree.FloatValue(3.14),
		btree.NullValue(),
		btree.IntValue(100),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		btree.MakeRecord(values)
	}
}

// BenchmarkParseRecord measures record parsing performance.
func BenchmarkParseRecord(b *testing.B) {
	values := []btree.Value{
		btree.IntValue(42),
		btree.TextValue("hello world"),
		btree.FloatValue(3.14),
		btree.NullValue(),
		btree.IntValue(100),
	}
	data := btree.MakeRecord(values)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = btree.ParseRecord(data)
	}
}

// BenchmarkVarintEncode measures varint encoding performance.
func BenchmarkVarintEncode(b *testing.B) {
	buf := make([]byte, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		btree.PutVarint(buf, int64(i))
	}
}

// BenchmarkVarintDecode measures varint decoding performance.
func BenchmarkVarintDecode(b *testing.B) {
	buf := make([]byte, 10)
	btree.PutVarint(buf, 12345678)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		btree.ReadVarint(buf)
	}
}

// --- Large payload benchmarks ---

// BenchmarkLargePayload measures performance with large blobs.
func BenchmarkLargePayload(b *testing.B) {
	db, err := sqlite.OpenInMemory()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE bench (id INTEGER, data TEXT)")

	// Large text payload
	largeText := strings.Repeat("x", 4000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec("INSERT INTO bench VALUES (?, ?)", i, largeText)
	}
	b.StopTimer()
}
