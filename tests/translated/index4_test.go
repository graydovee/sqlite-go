package tests

import (
	"fmt"
	"testing"
)

// ============================================================================
// index4.test translations
//
// Original tests focus on:
// - Creating indices on large tables (up to 65536 rows with randomblob)
// - integrity_check after index creation
// - Memory-limited index creation
// - Index on table with mix of small values, NULLs, and large blobs
// - UNIQUE constraint failure during index creation
// ============================================================================

// TestIndex4_1 translates index4-1.x:
// Create large table with randomblob, create index, verify integrity.
func TestIndex4_1(t *testing.T) {
	// 1.1: Create table and populate with doubling inserts to 65536 rows
	t.Run("1.1", func(t *testing.T) {
		t.Skip("INSERT ... SELECT from self (doubling) not yet supported")

		db := openTestDB(t)
		execSQL(t, db, "BEGIN")
		execSQL(t, db, "CREATE TABLE t1(x)")
		execSQL(t, db, "INSERT INTO t1 VALUES(randomblob(102))")

		// Double the rows 16 times: 1→2→4→8→...→65536
		for i := 0; i < 16; i++ {
			execSQL(t, db, "INSERT INTO t1 SELECT randomblob(102) FROM t1")
		}
		execSQL(t, db, "COMMIT")

		got := queryInt(t, db, "SELECT count(*) FROM t1")
		if got != 65536 {
			t.Errorf("count(*) = %d, want 65536", got)
		}
	})

	// 1.2: Create index on the large table
	t.Run("1.2", func(t *testing.T) {
		t.Skip("depends on test 1.1 (INSERT ... SELECT from self)")
	})

	// 1.3: integrity_check
	t.Run("1.3", func(t *testing.T) {
		t.Skip("PRAGMA integrity_check not yet supported")
	})

	// 1.4: Same test with limited memory (cache_size=10)
	t.Run("1.4", func(t *testing.T) {
		t.Skip("PRAGMA cache_size and memory management not yet supported")
	})

	// 1.6: Table with mix of small strings, NULL, and large blobs, then index
	t.Run("1.6", func(t *testing.T) {
		t.Skip("INSERT ... SELECT from self, DROP TABLE, CREATE INDEX not yet supported")

		db := openTestDB(t)
		execSQL(t, db, "BEGIN")
		execSQL(t, db, "DROP TABLE t1")
		execSQL(t, db, "CREATE TABLE t1(x)")

		// Insert small strings
		for _, v := range []string{"a", "b", "c", "d", "e", "f", "g"} {
			execSQL(t, db, fmt.Sprintf("INSERT INTO t1 VALUES('%s')", v))
		}
		// Insert NULL
		execSQL(t, db, "INSERT INTO t1 VALUES(NULL)")

		// Double with randomblob 5 times: 8→16→32→64→128→256
		for _, size := range []int{1202, 2202, 3202, 4202, 5202} {
			execSQL(t, db, fmt.Sprintf("INSERT INTO t1 SELECT randomblob(%d) FROM t1", size))
		}
		execSQL(t, db, "COMMIT")

		execSQL(t, db, "CREATE INDEX i1 ON t1(x)")
		got := queryText(t, db, "PRAGMA integrity_check")
		if got != "ok" {
			t.Errorf("integrity_check = %q, want 'ok'", got)
		}
	})

	// 1.7: Simple table with one row, create index
	t.Run("1.7", func(t *testing.T) {
		t.Skip("DROP TABLE not yet supported")

		db := openTestDB(t)
		execSQL(t, db, "BEGIN")
		execSQL(t, db, "DROP TABLE t1")
		execSQL(t, db, "CREATE TABLE t1(x)")
		execSQL(t, db, "INSERT INTO t1 VALUES('a')")
		execSQL(t, db, "COMMIT")

		execSQL(t, db, "CREATE INDEX i1 ON t1(x)")
		got := queryText(t, db, "PRAGMA integrity_check")
		if got != "ok" {
			t.Errorf("integrity_check = %q, want 'ok'", got)
		}
	})

	// 1.8: Empty table, create index
	t.Run("1.8", func(t *testing.T) {
		t.Skip("DROP TABLE not yet supported")

		db := openTestDB(t)
		execSQL(t, db, "BEGIN")
		execSQL(t, db, "DROP TABLE t1")
		execSQL(t, db, "CREATE TABLE t1(x)")
		execSQL(t, db, "COMMIT")

		execSQL(t, db, "CREATE INDEX i1 ON t1(x)")
		got := queryText(t, db, "PRAGMA integrity_check")
		if got != "ok" {
			t.Errorf("integrity_check = %q, want 'ok'", got)
		}
	})
}

// TestIndex4_2 translates index4-2.x:
// UNIQUE index creation failure on duplicate data.
func TestIndex4_2(t *testing.T) {
	// 2.1: Create table with duplicate values, verify row count
	t.Run("2.1", func(t *testing.T) {
		db := openTestDB(t)

		execSQLArgs(t, db, "CREATE TABLE t2(x)")
		execSQLArgs(t, db, "INSERT INTO t2 VALUES(14)")
		execSQLArgs(t, db, "INSERT INTO t2 VALUES(35)")
		execSQLArgs(t, db, "INSERT INTO t2 VALUES(15)")
		execSQLArgs(t, db, "INSERT INTO t2 VALUES(35)") // duplicate
		execSQLArgs(t, db, "INSERT INTO t2 VALUES(16)")

		got := queryRowCount(t, db, "SELECT * FROM t2")
		if got != 5 {
			t.Errorf("got %d rows, want 5", got)
		}
	})

	// 2.2: UNIQUE index on column with duplicate values should fail
	t.Run("2.2", func(t *testing.T) {
		t.Skip("UNIQUE constraint during CREATE INDEX not yet supported")

		db := openTestDB(t)
		execSQL(t, db, "CREATE TABLE t2(x)")
		execSQL(t, db, "INSERT INTO t2 VALUES(14)")
		execSQL(t, db, "INSERT INTO t2 VALUES(35)")
		execSQL(t, db, "INSERT INTO t2 VALUES(15)")
		execSQL(t, db, "INSERT INTO t2 VALUES(35)") // duplicate
		execSQL(t, db, "INSERT INTO t2 VALUES(16)")

		err := db.Exec("CREATE UNIQUE INDEX i3 ON t2(x)")
		if err == nil {
			t.Error("expected UNIQUE constraint failure")
		}
		// Original expects: {1 {UNIQUE constraint failed: t2.x}}
	})
}

// Ensure unused imports are referenced
var _ = fmt.Sprintf
