package translated

import (
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// openTestDB opens an in-memory database for testing.
func openTestDB(t *testing.T) *sqlite.Database {
	t.Helper()
	db, err := sqlite.OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// queryText executes a query and returns the first column as string.
func queryText(t *testing.T, db *sqlite.Database, sql string) string {
	t.Helper()
	rs, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatalf("Query(%q): no rows", sql)
	}
	return rs.Row().ColumnText(0)
}

// queryInt64 executes a query and returns the first column as int64.
func queryInt64(t *testing.T, db *sqlite.Database, sql string) int64 {
	t.Helper()
	rs, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatalf("Query(%q): no rows", sql)
	}
	return rs.Row().ColumnInt(0)
}

// execSQL executes SQL or fatals the test.
func execSQL(t *testing.T, db *sqlite.Database, sql string) {
	t.Helper()
	if err := db.Exec(sql); err != nil {
		t.Fatalf("Exec(%q): %v", sql, err)
	}
}

// catchSQL executes SQL and returns (error occurred, error message).
func catchSQL(t *testing.T, db *sqlite.Database, sql string) (bool, string) {
	t.Helper()
	err := db.Exec(sql)
	if err != nil {
		return true, err.Error()
	}
	return false, ""
}

// ============================================================================
// btree01.test translations
//
// Original tests verify b-tree balance() routine correctness with various
// blob sizes and page sizes. They use WITH RECURSIVE CTEs, zeroblob(),
// PRAGMA page_size, PRAGMA integrity_check, WITHOUT ROWID tables, and
// LEFT/RIGHT JOINs.
// ============================================================================

func TestBTree01(t *testing.T) {
	db := openTestDB(t)

	// btree01-1.1: Basic balance() test with page_size=65536
	// Requires: PRAGMA page_size, WITH RECURSIVE CTE, zeroblob(),
	//           PRAGMA integrity_check
	t.Run("btree01-1.1", func(t *testing.T) {
		t.Skip("PRAGMA page_size, WITH RECURSIVE CTE, zeroblob(), integrity_check not yet supported")

		execSQL(t, db, "PRAGMA page_size=65536")
		execSQL(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB)")
		// Original uses WITH RECURSIVE to insert 30 rows with zeroblob(6500)
		// then updates and checks integrity
	})

	// btree01-1.2.$i: Repeated balance tests with varying update targets
	for i := 1; i <= 30; i++ {
		i := i
		t.Run("btree01-1.2", func(t *testing.T) {
			t.Skip("WITH RECURSIVE CTE, zeroblob(), PRAGMA not yet supported")
			_ = i
		})
	}

	// btree01-1.3.$i
	for i := 1; i <= 30; i++ {
		i := i
		t.Run("btree01-1.3", func(t *testing.T) {
			t.Skip("WITH RECURSIVE CTE, zeroblob(), PRAGMA not yet supported")
			_ = i
		})
	}

	// btree01-1.4.$i
	for i := 1; i <= 30; i++ {
		i := i
		t.Run("btree01-1.4", func(t *testing.T) {
			t.Skip("WITH RECURSIVE CTE, zeroblob(), PRAGMA not yet supported")
			_ = i
		})
	}

	// btree01-1.5.$i
	for i := 1; i <= 30; i++ {
		i := i
		t.Run("btree01-1.5", func(t *testing.T) {
			t.Skip("WITH RECURSIVE CTE, zeroblob(), PRAGMA not yet supported")
			_ = i
		})
	}

	// btree01-1.6.$i
	for i := 1; i <= 30; i++ {
		i := i
		t.Run("btree01-1.6", func(t *testing.T) {
			t.Skip("WITH RECURSIVE CTE, zeroblob(), PRAGMA not yet supported")
			_ = i
		})
	}

	// btree01-1.7.$i
	for i := 1; i <= 30; i++ {
		i := i
		t.Run("btree01-1.7", func(t *testing.T) {
			t.Skip("WITH RECURSIVE CTE, zeroblob(), PRAGMA not yet supported")
			_ = i
		})
	}

	// btree01-1.8.$i (31 iterations)
	for i := 1; i <= 31; i++ {
		i := i
		t.Run("btree01-1.8", func(t *testing.T) {
			t.Skip("WITH RECURSIVE CTE, zeroblob(), PRAGMA not yet supported")
			_ = i
		})
	}
}

func TestBTree01_2(t *testing.T) {
	db := openTestDB(t)

	// btree01-2.1: OSSFuzz issue 45329 - stay-on-last page optimization
	// Requires: PRAGMA page_size=1024, WITHOUT ROWID, WITH RECURSIVE,
	//           zeroblob(), LEFT JOIN, ORDER BY
	t.Run("btree01-2.1", func(t *testing.T) {
		t.Skip("PRAGMA page_size, WITHOUT ROWID tables, WITH RECURSIVE CTE not yet supported")

		execSQL(t, db, "PRAGMA page_size=1024")
		execSQL(t, db, "CREATE TABLE t1(a INT PRIMARY KEY, b BLOB, c INT) WITHOUT ROWID")
		// ... insert data, create t2, LEFT JOIN ...
	})

	// btree01-2.2: RIGHT JOIN test
	t.Run("btree01-2.2", func(t *testing.T) {
		t.Skip("RIGHT JOIN not yet supported")
	})
}
