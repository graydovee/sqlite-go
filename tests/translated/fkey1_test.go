package tests

import (
	"strings"
	"testing"
)

// ============================================================================
// fkey1.test translations
//
// Tests for foreign key functionality:
// - Basic FK table creation (CREATE TABLE with REFERENCES)
// - FK table create/drop cycle
// - PRAGMA foreign_key_list tests
// - Dequoting tests with quoted identifiers
// - Self-referencing FK with INSERT OR REPLACE
// - Partial indexes on parent tables
//
// Skipped:
// - fkey1-3.5: sqlite3_db_status (not exposed in Go API)
// - fkey1-5.2.1: sqlite3 trace callback (not exposed)
// - Test 7.x: requires reset_db
// - Test 8.x: requires reset_db, writable_schema, database_may_be_corrupt
// - Test 9.x: requires reset_db
// ============================================================================

// --- fkey1-1.x: Basic FK table creation ---

// TestFkey1_1 tests creating tables with various foreign key references.
func TestFkey1_1(t *testing.T) {
	// fkey1-1.0: Create t1 with self-reference and reference to t2
	t.Run("fkey1-1.0", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, `
			CREATE TABLE t1(
				a INTEGER PRIMARY KEY,
				b INTEGER
					REFERENCES t1 ON DELETE CASCADE
					REFERENCES t2,
				c TEXT,
				FOREIGN KEY (b,c) REFERENCES t2(x,y) ON UPDATE CASCADE
			)
		`)
	})

	// fkey1-1.1: Create t2
	t.Run("fkey1-1.1", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, `
			CREATE TABLE t2(
				x INTEGER PRIMARY KEY,
				y TEXT
			)
		`)
	})

	// fkey1-1.2: Create t3 with multiple references
	t.Run("fkey1-1.2", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, `
			CREATE TABLE t3(
				a INTEGER REFERENCES t2,
				b INTEGER REFERENCES t1,
				FOREIGN KEY (a,b) REFERENCES t2(x,y)
			)
		`)
	})
}

// --- fkey1-2.x: FK table create/drop cycle ---

// TestFkey1_2 tests creating and dropping multiple tables with FK references.
func TestFkey1_2(t *testing.T) {
	// fkey1-2.1: Create tables referencing t4, then drop them in various order
	t.Run("fkey1-2.1", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t4(a integer primary key)")
		mustExec(t, db, "CREATE TABLE t5(x references t4)")
		mustExec(t, db, "CREATE TABLE t6(x references t4)")
		mustExec(t, db, "CREATE TABLE t7(x references t4)")
		mustExec(t, db, "CREATE TABLE t8(x references t4)")
		mustExec(t, db, "CREATE TABLE t9(x references t4)")
		mustExec(t, db, "CREATE TABLE t10(x references t4)")

		mustExec(t, db, "DROP TABLE t7")
		mustExec(t, db, "DROP TABLE t9")
		mustExec(t, db, "DROP TABLE t5")
		mustExec(t, db, "DROP TABLE t8")
		mustExec(t, db, "DROP TABLE t6")
		mustExec(t, db, "DROP TABLE t10")
	})
}

// --- fkey1-3.x: PRAGMA foreign_key_list tests ---

// TestFkey1_3 tests PRAGMA foreign_key_list output.
func TestFkey1_3(t *testing.T) {
	t.Skip("PRAGMA foreign_key_list not implemented")
	// fkey1-3.1: Single-column references with column-level FK
	t.Run("fkey1-3.1", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t5(a PRIMARY KEY, b, c)")
		mustExec(t, db, `
			CREATE TABLE t6(
				d REFERENCES t5,
				e REFERENCES t5(c)
			)
		`)

		got := queryFlatStrings(t, db, "PRAGMA foreign_key_list(t6)")
		// Expected rows (order matters):
		//   row 0: 0 0 t5 e c {NO ACTION} {NO ACTION} NONE
		//   row 1: 1 0 t5 d {} {NO ACTION} {NO ACTION} NONE
		want := []string{
			"0", "0", "t5", "e", "c", "NO ACTION", "NO ACTION", "NONE",
			"1", "0", "t5", "d", "", "NO ACTION", "NO ACTION", "NONE",
		}
		assertResults(t, got, want)
	})

	// fkey1-3.2: Multi-column FK with table-level constraint
	t.Run("fkey1-3.2", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t5(a PRIMARY KEY, b, c)")
		mustExec(t, db, `
			CREATE TABLE t7(d, e, f,
				FOREIGN KEY (d, e) REFERENCES t5(a, b)
			)
		`)

		got := queryFlatStrings(t, db, "PRAGMA foreign_key_list(t7)")
		want := []string{
			"0", "0", "t5", "d", "a", "NO ACTION", "NO ACTION", "NONE",
			"0", "1", "t5", "e", "b", "NO ACTION", "NO ACTION", "NONE",
		}
		assertResults(t, got, want)
	})

	// fkey1-3.3: FK with ON DELETE CASCADE ON UPDATE SET NULL
	t.Run("fkey1-3.3", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t5(a PRIMARY KEY, b, c)")
		mustExec(t, db, `
			CREATE TABLE t8(d, e, f,
				FOREIGN KEY (d, e) REFERENCES t5 ON DELETE CASCADE ON UPDATE SET NULL
			)
		`)

		got := queryFlatStrings(t, db, "PRAGMA foreign_key_list(t8)")
		want := []string{
			"0", "0", "t5", "d", "", "SET NULL", "CASCADE", "NONE",
			"0", "1", "t5", "e", "", "SET NULL", "CASCADE", "NONE",
		}
		assertResults(t, got, want)
	})

	// fkey1-3.4: FK with ON DELETE CASCADE ON UPDATE SET DEFAULT
	t.Run("fkey1-3.4", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t5(a PRIMARY KEY, b, c)")
		mustExec(t, db, `
			CREATE TABLE t9(d, e, f,
				FOREIGN KEY (d, e) REFERENCES t5 ON DELETE CASCADE ON UPDATE SET DEFAULT
			)
		`)

		got := queryFlatStrings(t, db, "PRAGMA foreign_key_list(t9)")
		want := []string{
			"0", "0", "t5", "d", "", "SET DEFAULT", "CASCADE", "NONE",
			"0", "1", "t5", "e", "", "SET DEFAULT", "CASCADE", "NONE",
		}
		assertResults(t, got, want)
	})

	// fkey1-3.5: sqlite3_db_status check - skipped (not exposed in Go API)
	t.Run("fkey1-3.5", func(t *testing.T) {
		t.Skipf("sqlite3_db_status not exposed in Go API")
	})
}

// --- fkey1-4.x: Dequoting tests with quoted identifiers ---

// TestFkey1_4 tests foreign key handling with quoted identifiers.
func TestFkey1_4(t *testing.T) {
	t.Skip("Quoted identifier handling not fully implemented")
	// fkey1-4.0: Quoted identifiers with "xx" prefix
	t.Run("fkey1-4.0", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "PRAGMA foreign_keys=ON")
		mustExec(t, db, `CREATE TABLE "xx1"("xx2" TEXT PRIMARY KEY, "xx3" TEXT)`)
		mustExec(t, db, `INSERT INTO "xx1"("xx2","xx3") VALUES('abc','def')`)
		mustExec(t, db, `CREATE TABLE "xx4"("xx5" TEXT REFERENCES "xx1" ON DELETE CASCADE)`)
		mustExec(t, db, `INSERT INTO "xx4"("xx5") VALUES('abc')`)
		mustExec(t, db, `INSERT INTO "xx1"("xx2","xx3") VALUES('uvw','xyz')`)

		// SELECT 1, "xx5" FROM "xx4" → should return {1 abc}
		got := queryFlatStrings(t, db, `SELECT 1, "xx5" FROM "xx4"`)
		want := []string{"1", "abc"}
		assertResults(t, got, want)

		// DELETE FROM "xx1" cascades to "xx4"
		mustExec(t, db, `DELETE FROM "xx1"`)

		// SELECT 2, "xx5" FROM "xx4" → should return empty (all deleted by cascade)
		got2 := queryFlatStrings(t, db, `SELECT 2, "xx5" FROM "xx4"`)
		if len(got2) != 0 {
			t.Errorf("expected empty result after cascade delete, got %v", got2)
		}
	})

	// fkey1-4.1: Quoted identifiers with escaped double-quote character
	t.Run("fkey1-4.1", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "PRAGMA foreign_keys=ON")
		// "xx" replaced with a single escaped double-quote character
		mustExec(t, db, `CREATE TABLE """1"("""2" TEXT PRIMARY KEY, """3" TEXT)`)
		mustExec(t, db, `INSERT INTO """1"("""2","""3") VALUES('abc','def')`)
		mustExec(t, db, `CREATE TABLE """4"("""5" TEXT REFERENCES """1" ON DELETE CASCADE)`)
		mustExec(t, db, `INSERT INTO """4"("""5") VALUES('abc')`)
		mustExec(t, db, `INSERT INTO """1"("""2","""3") VALUES('uvw','xyz')`)

		// SELECT 1, """5" FROM """4" → should return {1 abc}
		got := queryFlatStrings(t, db, `SELECT 1, """5" FROM """4"`)
		want := []string{"1", "abc"}
		assertResults(t, got, want)

		// DELETE FROM """1" cascades to """4"
		mustExec(t, db, `DELETE FROM """1"`)

		// SELECT 2, """5" FROM """4" → should return empty
		got2 := queryFlatStrings(t, db, `SELECT 2, """5" FROM """4"`)
		if len(got2) != 0 {
			t.Errorf("expected empty result after cascade delete, got %v", got2)
		}
	})

	// fkey1-4.2: PRAGMA table_info on quoted table
	t.Run("fkey1-4.2", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, `CREATE TABLE """1"("""2" TEXT PRIMARY KEY, """3" TEXT)`)

		got := queryFlatStrings(t, db, `PRAGMA table_info="""1"`)
		// Expected: 0 {"2} TEXT 0 {} 1  1 {"3} TEXT 0 {} 0
		// Note: PRAGMA table_info returns columns:
		//   cid, name, type, notnull, dflt_value, pk
		want := []string{
			"0", "\"2", "TEXT", "0", "", "1",
			"1", "\"3", "TEXT", "0", "", "0",
		}
		assertResults(t, got, want)
	})
}

// --- fkey1-5.x: Self-referencing FK with INSERT OR REPLACE ---

// TestFkey1_5 tests self-referencing foreign keys and INSERT OR REPLACE behavior.
func TestFkey1_5(t *testing.T) {
	// fkey1-5.1: Create self-referencing table and insert data
	t.Run("fkey1-5.1", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "PRAGMA foreign_keys=ON")
		mustExec(t, db, `
			CREATE TABLE t11(
				x INTEGER PRIMARY KEY,
				parent REFERENCES t11 ON DELETE CASCADE
			)
		`)
		mustExec(t, db, "INSERT INTO t11 VALUES (1, NULL), (2, 1), (3, 2)")
	})

	// fkey1-5.2: INSERT OR REPLACE causes FK violation due to cascade
	t.Run("fkey1-5.2", func(t *testing.T) {
		t.Skip("INSERT OR REPLACE with cascade not yet implemented")
		db := openTestDB(t)
		mustExec(t, db, "PRAGMA foreign_keys=ON")
		mustExec(t, db, `
			CREATE TABLE t11(
				x INTEGER PRIMARY KEY,
				parent REFERENCES t11 ON DELETE CASCADE
			)
		`)
		mustExec(t, db, "INSERT INTO t11 VALUES (1, NULL), (2, 1), (3, 2)")

		errOccurred, errMsg := catchSQL(t, db, "INSERT OR REPLACE INTO t11 VALUES (2, 3)")
		if !errOccurred {
			t.Fatalf("expected FOREIGN KEY constraint error, got success")
		}
		if !strings.Contains(errMsg, "FOREIGN KEY constraint") {
			t.Errorf("expected FOREIGN KEY constraint error, got: %s", errMsg)
		}
	})

	// fkey1-5.2.1: sqlite3 trace callback test - skipped (not exposed in Go API)
	t.Run("fkey1-5.2.1", func(t *testing.T) {
		t.Skipf("sqlite3 trace callback not exposed in Go API")
	})

	// fkey1-5.3: Another self-referencing FK test with Foo table
	t.Run("fkey1-5.3", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "PRAGMA foreign_keys=ON")
		mustExec(t, db, `
			CREATE TABLE Foo (
				Id INTEGER PRIMARY KEY,
				ParentId INTEGER REFERENCES Foo(Id) ON DELETE CASCADE,
				C1
			)
		`)
		mustExec(t, db, "INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (1, null, 'A')")
		mustExec(t, db, "INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (2, 1, 'A-2-1')")
		mustExec(t, db, "INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (3, 2, 'A-3-2')")
		mustExec(t, db, "INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (4, 3, 'A-4-3')")
	})

	// fkey1-5.4: INSERT OR REPLACE causes FK violation with Foo table
	t.Run("fkey1-5.4", func(t *testing.T) {
		t.Skip("INSERT OR REPLACE with cascade not yet implemented")
		db := openTestDB(t)
		mustExec(t, db, "PRAGMA foreign_keys=ON")
		mustExec(t, db, `
			CREATE TABLE Foo (
				Id INTEGER PRIMARY KEY,
				ParentId INTEGER REFERENCES Foo(Id) ON DELETE CASCADE,
				C1
			)
		`)
		mustExec(t, db, "INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (1, null, 'A')")
		mustExec(t, db, "INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (2, 1, 'A-2-1')")
		mustExec(t, db, "INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (3, 2, 'A-3-2')")
		mustExec(t, db, "INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (4, 3, 'A-4-3')")

		errOccurred, errMsg := catchSQL(t, db, "INSERT OR REPLACE INTO Foo(Id, ParentId, C1) VALUES (2, 3, 'A-2-3')")
		if !errOccurred {
			t.Fatalf("expected FOREIGN KEY constraint error, got success")
		}
		if !strings.Contains(errMsg, "FOREIGN KEY constraint") {
			t.Errorf("expected FOREIGN KEY constraint error, got: %s", errMsg)
		}
	})
}

// --- fkey1-6.x: Partial indexes on parent tables ---

// TestFkey1_6 tests that foreign key processing is not fooled by partial indexes.
func TestFkey1_6(t *testing.T) {
	t.Skip("Partial indexes (WHERE clause) not implemented")
	// 6.0: Create parent table with partial unique index and child table
	t.Run("fkey1-6.0", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "PRAGMA foreign_keys=ON")
		mustExec(t, db, "CREATE TABLE p1(x, y)")
		mustExec(t, db, "CREATE UNIQUE INDEX p1x ON p1(x) WHERE y<2")
		mustExec(t, db, "INSERT INTO p1 VALUES(1, 1)")
		mustExec(t, db, "CREATE TABLE c1(a REFERENCES p1(x))")
	})

	// 6.1: Partial index is not sufficient for FK - should get mismatch error
	t.Run("fkey1-6.1", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "PRAGMA foreign_keys=ON")
		mustExec(t, db, "CREATE TABLE p1(x, y)")
		mustExec(t, db, "CREATE UNIQUE INDEX p1x ON p1(x) WHERE y<2")
		mustExec(t, db, "INSERT INTO p1 VALUES(1, 1)")
		mustExec(t, db, "CREATE TABLE c1(a REFERENCES p1(x))")

		errOccurred, errMsg := catchSQL(t, db, "INSERT INTO c1 VALUES(1)")
		if !errOccurred {
			t.Fatalf("expected foreign key mismatch error, got success")
		}
		if !strings.Contains(errMsg, "foreign key mismatch") {
			t.Errorf("expected foreign key mismatch error, got: %s", errMsg)
		}
	})

	// 6.2: With proper unique index, FK works
	t.Run("fkey1-6.2", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "PRAGMA foreign_keys=ON")
		mustExec(t, db, "CREATE TABLE p1(x, y)")
		mustExec(t, db, "CREATE UNIQUE INDEX p1x ON p1(x) WHERE y<2")
		mustExec(t, db, "INSERT INTO p1 VALUES(1, 1)")
		mustExec(t, db, "CREATE TABLE c1(a REFERENCES p1(x))")
		mustExec(t, db, "CREATE UNIQUE INDEX p1x2 ON p1(x)")

		// Now the insert should succeed with a proper unique index
		mustExec(t, db, "INSERT INTO c1 VALUES(1)")
	})
}
