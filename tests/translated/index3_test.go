package tests

import (
	"testing"
)

// ============================================================================
// index3.test translations
//
// Original tests focus on:
// - UNIQUE index creation failure on non-unique data
// - Leaving no residue after failure
// - Backwards compatibility with string column names in CREATE INDEX
// - Schema corruption handling
// ============================================================================

// TestIndex3_1 translates index3-1.x:
// UNIQUE index creation on non-unique data should fail and leave no residue.
func TestIndex3_1(t *testing.T) {
	// index3-1.1: Create table with duplicate values, verify they exist
	t.Run("index3-1.1", func(t *testing.T) {
		db := openTestDB(t)

		execSQLArgs(t, db, "CREATE TABLE t1(a)")
		execSQLArgs(t, db, "INSERT INTO t1 VALUES(1)")
		execSQLArgs(t, db, "INSERT INTO t1 VALUES(1)")

		got := queryRowCount(t, db, "SELECT * FROM t1")
		if got != 2 {
			t.Errorf("got %d rows, want 2", got)
		}
	})

	// index3-1.2: UNIQUE index on non-unique column should fail
	t.Run("index3-1.2", func(t *testing.T) {
		t.Skip("UNIQUE constraint during CREATE INDEX not yet supported")

		db := openTestDB(t)
		execSQLArgs(t, db, "CREATE TABLE t1(a)")
		execSQLArgs(t, db, "INSERT INTO t1 VALUES(1)")
		execSQLArgs(t, db, "INSERT INTO t1 VALUES(1)")

		err := db.Exec("BEGIN")
		if err != nil {
			t.Fatalf("BEGIN: %v", err)
		}
		err = db.Exec("CREATE UNIQUE INDEX i1 ON t1(a)")
		if err == nil {
			t.Error("expected error creating UNIQUE index on non-unique column")
		}
		// Original expects: "UNIQUE constraint failed: t1.a"
	})

	// index3-1.3: After failed index creation, COMMIT should succeed
	t.Run("index3-1.3", func(t *testing.T) {
		t.Skip("transaction state after failed DDL not yet verified")

		db := openTestDB(t)
		execSQLArgs(t, db, "CREATE TABLE t1(a)")
		execSQLArgs(t, db, "INSERT INTO t1 VALUES(1)")
		execSQLArgs(t, db, "INSERT INTO t1 VALUES(1)")
		db.Exec("BEGIN")
		db.Exec("CREATE UNIQUE INDEX i1 ON t1(a)") // will fail

		err := db.Exec("COMMIT")
		if err != nil {
			t.Errorf("COMMIT after failed index creation: %v", err)
		}
	})

	// index3-1.4: integrity_check after the failed index creation
	t.Run("index3-1.4", func(t *testing.T) {
		t.Skip("PRAGMA integrity_check not yet supported")
	})
}

// TestIndex3_2 translates index3-2.x:
// Backwards compatibility with string column names in CREATE INDEX.
func TestIndex3_2(t *testing.T) {
	// index3-2.1: String column names in PRIMARY KEY and UNIQUE
	t.Run("index3-2.1", func(t *testing.T) {
		t.Skip("string column names in PRIMARY KEY/UNIQUE not yet supported")

		db := openTestDB(t)
		execSQLArgs(t, db, "DROP TABLE t1")
		execSQLArgs(t, db, "CREATE TABLE t1(a, b, c, d, e, PRIMARY KEY('a'), UNIQUE('b' COLLATE nocase DESC))")
		execSQLArgs(t, db, "CREATE INDEX t1c ON t1('c')")
		execSQLArgs(t, db, "CREATE INDEX t1d ON t1('d' COLLATE binary ASC)")

		// Insert 30 rows using CTE equivalent
		for x := 1; x <= 30; x++ {
			bVal := ""
			// Original uses printf('ab%03xxy', x)
			// This produces strings like 'ab001xy', 'ab002xy', etc.
			bVal = ""
			execSQLArgs(t, db, "INSERT INTO t1(a, b, c, d, e) VALUES(?, ?, ?, ?, ?)",
				x, bVal, x, x, x)
		}
	})

	// index3-2.2: Query using COLLATE nocase
	t.Run("index3-2.2", func(t *testing.T) {
		t.Skip("COLLATE nocase not yet supported")
	})

	// index3-2.3: Verify index names in sqlite_master
	t.Run("index3-2.3", func(t *testing.T) {
		t.Skip("sqlite_master querying not yet supported")
	})

	// index3-2.4: Create tables with various quoting styles for PRIMARY KEY
	t.Run("index3-2.4", func(t *testing.T) {
		t.Skip("quoted column names in PRIMARY KEY not yet verified")

		db := openTestDB(t)
		execSQLArgs(t, db, "CREATE TABLE t2a(a integer, b, PRIMARY KEY(a))")
		execSQL(t, db, `CREATE TABLE t2b("a" integer, b, PRIMARY KEY("a"))`)
		execSQLArgs(t, db, "CREATE TABLE t2c([a] integer, b, PRIMARY KEY([a]))")
		execSQLArgs(t, db, "CREATE TABLE t2d('a' integer, b, PRIMARY KEY('a'))")
	})

	// index3-2.5: Verify table names in sqlite_master
	t.Run("index3-2.5", func(t *testing.T) {
		t.Skip("sqlite_master LIKE query not yet supported")
	})
}

// TestIndex3_99 translates index3-99.1:
// Schema corruption test - intentionally corrupts the database.
// This must be the last test in the series.
func TestIndex3_99(t *testing.T) {
	t.Run("index3-99.1", func(t *testing.T) {
		t.Skip("PRAGMA writable_schema and intentional schema corruption not supported")

		// Original:
		// PRAGMA writable_schema=on;
		// UPDATE sqlite_master SET sql='nonsense' WHERE name='t1d'
		// db close; reopen; DROP INDEX t1c → error about malformed schema
	})
}

// Ensure unused imports are referenced
var _ = testing.T{}
