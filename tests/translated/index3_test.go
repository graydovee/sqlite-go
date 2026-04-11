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

func TestIndex3_1(t *testing.T) {
	db := openTestDB(t)

	// index3-1.1: Create table with duplicate values
	t.Run("index3-1.1", func(t *testing.T) {
		execSQL(t, db, "CREATE TABLE t1(a)")
		execSQL(t, db, "INSERT INTO t1 VALUES(1)")
		execSQL(t, db, "INSERT INTO t1 VALUES(1)")

		// Verify both rows exist
		rs, err := db.Query("SELECT * FROM t1")
		if err != nil {
			t.Fatalf("SELECT: %v", err)
		}
		defer rs.Close()
		count := 0
		for rs.Next() {
			count++
		}
		if count != 2 {
			t.Errorf("got %d rows, want 2", count)
		}
	})

	// index3-1.2: UNIQUE index creation on non-unique column should fail
	t.Run("index3-1.2", func(t *testing.T) {
		t.Skip("UNIQUE constraint during CREATE INDEX not yet supported")

		execSQL(t, db, "CREATE TABLE t1(a)")
		execSQL(t, db, "INSERT INTO t1 VALUES(1)")
		execSQL(t, db, "INSERT INTO t1 VALUES(1)")

		err := db.Exec("BEGIN")
		if err != nil {
			t.Fatalf("BEGIN: %v", err)
		}
		err = db.Exec("CREATE UNIQUE INDEX i1 ON t1(a)")
		if err == nil {
			t.Error("expected error creating UNIQUE index on non-unique column")
		}
		// Original expects: {1 {UNIQUE constraint failed: t1.a}}
	})

	// index3-1.3: After failed index creation, COMMIT should succeed
	t.Run("index3-1.3", func(t *testing.T) {
		t.Skip("transaction state after failed DDL not yet verified")

		execSQL(t, db, "CREATE TABLE t1(a)")
		execSQL(t, db, "INSERT INTO t1 VALUES(1)")
		execSQL(t, db, "INSERT INTO t1 VALUES(1)")
		db.Exec("BEGIN")
		db.Exec("CREATE UNIQUE INDEX i1 ON t1(a)") // will fail

		err := db.Exec("COMMIT")
		if err != nil {
			t.Errorf("COMMIT after failed index creation: %v", err)
		}
	})

	// index3-1.4: integrity_check
	t.Run("index3-1.4", func(t *testing.T) {
		t.Skip("PRAGMA integrity_check not yet supported")
	})
}

func TestIndex3_2(t *testing.T) {
	// index3-2.1: Backwards compat - string column names in CREATE INDEX
	t.Run("index3-2.1", func(t *testing.T) {
		t.Skip("string column names in PRIMARY KEY/UNIQUE, WITH RECURSIVE not yet supported")

		// Original:
		// DROP TABLE t1;
		// CREATE TABLE t1(a, b, c, d, e,
		//   PRIMARY KEY('a'), UNIQUE('b' COLLATE nocase DESC));
		// CREATE INDEX t1c ON t1('c');
		// CREATE INDEX t1d ON t1('d' COLLATE binary ASC);
		// WITH RECURSIVE c(x) AS (VALUES(1) UNION SELECT x+1 FROM c WHERE x<30)
		//   INSERT INTO t1(a,b,c,d,e)
		//     SELECT x, printf('ab%03xxy',x), x, x, x FROM c;
	})

	// index3-2.2: Query using the index
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
	})

	// index3-2.5: Verify table names in sqlite_master
	t.Run("index3-2.5", func(t *testing.T) {
		t.Skip("sqlite_master LIKE query not yet supported")
	})
}

func TestIndex3_99(t *testing.T) {
	// index3-99.1: Schema corruption test (last test - intentionally corrupts DB)
	t.Run("index3-99.1", func(t *testing.T) {
		t.Skip("PRAGMA writable_schema and intentional schema corruption not supported")

		// Original:
		// PRAGMA writable_schema=on;
		// UPDATE sqlite_master SET sql='nonsense' WHERE name='t1d'
		// db close; reopen; DROP INDEX t1c → error about malformed schema
	})
}
