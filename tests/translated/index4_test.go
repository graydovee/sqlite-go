package tests

import (
	"testing"
)

// ============================================================================
// index4.test translations
//
// Original tests focus on:
// - Creating indices on large tables (65536 rows with randomblob)
// - integrity_check after index creation
// - Memory-limited index creation
// - Index on table with mix of small values, NULLs, and large blobs
// - UNIQUE constraint failure during index creation
// ============================================================================

func TestIndex4_1(t *testing.T) {
	// 1.1: Create table and populate with randomblob, doubling inserts
	t.Run("1.1", func(t *testing.T) {
		t.Skip("INSERT ... SELECT from self (doubling), randomblob() not yet supported")

		// Original:
		// BEGIN;
		// CREATE TABLE t1(x);
		// INSERT INTO t1 VALUES(randomblob(102));
		// INSERT INTO t1 SELECT randomblob(102) FROM t1;  -- 2
		// ... up to 65536 rows
		// COMMIT;
	})

	// 1.2: Create index on the large table
	t.Run("1.2", func(t *testing.T) {
		t.Skip("index creation on large table depends on test 1.1")
	})

	// 1.3: integrity_check
	t.Run("1.3", func(t *testing.T) {
		t.Skip("PRAGMA integrity_check not yet supported")
	})

	// 1.4: Same test with limited memory
	t.Run("1.4", func(t *testing.T) {
		t.Skip("memory management and cache_size PRAGMA not yet supported")
	})

	// 1.6: Table with mix of small strings, NULL, and large blobs
	t.Run("1.6", func(t *testing.T) {
		t.Skip("INSERT ... SELECT from self, randomblob() not yet supported")

		// Original:
		// BEGIN;
		// DROP TABLE t1;
		// CREATE TABLE t1(x);
		// INSERT INTO t1 VALUES('a'); ... 'g'
		// INSERT INTO t1 VALUES(NULL);
		// INSERT INTO t1 SELECT randomblob(1202) FROM t1;  -- 16
		// ... up to 256 rows
		// COMMIT;
		// CREATE INDEX i1 ON t1(x);
		// PRAGMA integrity_check → ok
	})

	// 1.7: Simple table with one row
	t.Run("1.7", func(t *testing.T) {
		t.Skip("DROP TABLE not yet supported")

		// Original:
		// BEGIN; DROP TABLE t1; CREATE TABLE t1(x);
		// INSERT INTO t1 VALUES('a'); COMMIT;
		// CREATE INDEX i1 ON t1(x); PRAGMA integrity_check → ok
	})

	// 1.8: Empty table
	t.Run("1.8", func(t *testing.T) {
		t.Skip("DROP TABLE not yet supported")

		// Original:
		// BEGIN; DROP TABLE t1; CREATE TABLE t1(x); COMMIT;
		// CREATE INDEX i1 ON t1(x); PRAGMA integrity_check → ok
	})
}

func TestIndex4_2(t *testing.T) {
	db := openTestDB(t)

	// 2.1: Create table with duplicate values
	t.Run("2.1", func(t *testing.T) {
		execSQL(t, db, "CREATE TABLE t2(x)")
		execSQL(t, db, "INSERT INTO t2 VALUES(14)")
		execSQL(t, db, "INSERT INTO t2 VALUES(35)")
		execSQL(t, db, "INSERT INTO t2 VALUES(15)")
		execSQL(t, db, "INSERT INTO t2 VALUES(35)")
		execSQL(t, db, "INSERT INTO t2 VALUES(16)")

		// Verify 5 rows
		rs, err := db.Query("SELECT * FROM t2")
		if err != nil {
			t.Fatalf("SELECT: %v", err)
		}
		defer rs.Close()
		count := 0
		for rs.Next() {
			count++
		}
		if count != 5 {
			t.Errorf("got %d rows, want 5", count)
		}
	})

	// 2.2: UNIQUE index on non-unique data should fail
	t.Run("2.2", func(t *testing.T) {
		t.Skip("UNIQUE constraint during CREATE INDEX not yet supported")

		execSQL(t, db, "CREATE TABLE t2(x)")
		execSQL(t, db, "INSERT INTO t2 VALUES(14)")
		execSQL(t, db, "INSERT INTO t2 VALUES(35)")
		execSQL(t, db, "INSERT INTO t2 VALUES(15)")
		execSQL(t, db, "INSERT INTO t2 VALUES(35)") // duplicate!
		execSQL(t, db, "INSERT INTO t2 VALUES(16)")

		err := db.Exec("CREATE UNIQUE INDEX i3 ON t2(x)")
		if err == nil {
			t.Error("expected UNIQUE constraint failure")
		}
		// Original expects: {1 {UNIQUE constraint failed: t2.x}}
	})
}
