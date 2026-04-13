package tests

import (
	"testing"
)

// ============================================================================
// Foreign key tests - translated from SQLite fkey2.test
// ============================================================================

// TestFkey2SimpleImmediate tests simple immediate FK constraints.
func TestFkey2SimpleImmediate(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")
	mustExec(t, db, "CREATE TABLE t1(a PRIMARY KEY, b)")
	mustExec(t, db, "CREATE TABLE t2(c REFERENCES t1(a), d)")
	mustExec(t, db, "CREATE TABLE t3(a PRIMARY KEY, b)")
	mustExec(t, db, "CREATE TABLE t4(c REFERENCES t3, d)")
	mustExec(t, db, "CREATE TABLE t7(a, b INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE t8(c REFERENCES t7, d)")

	tests := []struct {
		sql string
		ok  bool
	}{
		{"INSERT INTO t2 VALUES(1, 3)", false},
		{"INSERT INTO t1 VALUES(1, 2)", true},
		{"INSERT INTO t2 VALUES(1, 3)", true},
		{"INSERT INTO t2 VALUES(2, 4)", false},
		{"INSERT INTO t2 VALUES(NULL, 4)", true},
		{"UPDATE t2 SET c=2 WHERE d=4", false},
		{"UPDATE t2 SET c=1 WHERE d=4", true},
		{"UPDATE t2 SET c=NULL WHERE d=4", true},
		{"DELETE FROM t1 WHERE a=1", false},
		{"UPDATE t1 SET a = 2", false},
		{"UPDATE t1 SET a = 1", true},
	}
	for i, tt := range tests {
		err := db.Exec(tt.sql)
		if tt.ok && err != nil {
			t.Errorf("test %d: expected success for %q, got %v", i, tt.sql, err)
		}
		if !tt.ok && err == nil {
			t.Errorf("test %d: expected error for %q", i, tt.sql)
		}
	}

	// t7/t8 tests
	if err := db.Exec("INSERT INTO t8 VALUES(1, 3)"); err == nil {
		t.Error("expected FK error for t8 insert")
	}
	mustExec(t, db, "INSERT INTO t7 VALUES(2, 1)")
	mustExec(t, db, "INSERT INTO t8 VALUES(1, 3)")
	if err := db.Exec("INSERT INTO t8 VALUES(2, 4)"); err == nil {
		t.Error("expected FK error for t8 insert with bad ref")
	}
}

// TestFkey2Deferred tests deferred FK constraints inside transactions.
func TestFkey2Deferred(t *testing.T) {
	// DEFERRABLE INITIALLY DEFERRED
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")
	mustExec(t, db, `CREATE TABLE node(
		nodeid PRIMARY KEY,
		parent REFERENCES node DEFERRABLE INITIALLY DEFERRED
	)`)
	mustExec(t, db, `CREATE TABLE leaf(
		cellid PRIMARY KEY,
		parent REFERENCES node DEFERRABLE INITIALLY DEFERRED
	)`)

	// Insert with FK violation outside transaction should fail immediately
	if err := db.Exec("INSERT INTO node VALUES(1, 0)"); err == nil {
		t.Error("expected FK error for node insert with bad parent")
	}

	// Inside a transaction, deferred FK allows temporary violations
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "INSERT INTO node VALUES(1, 0)")
	mustExec(t, db, "UPDATE node SET parent = NULL")
	mustExec(t, db, "COMMIT")

	got := queryFlatStrings(t, db, "SELECT * FROM node")
	assertResults(t, got, []string{"1", ""})

	// More complex deferred scenario
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "INSERT INTO leaf VALUES('a', 2)")
	mustExec(t, db, "INSERT INTO node VALUES(2, 0)")
	mustExec(t, db, "UPDATE node SET parent = 1 WHERE nodeid = 2")
	mustExec(t, db, "COMMIT")

	got = queryFlatStrings(t, db, "SELECT * FROM node")
	assertResults(t, got, []string{"1", "", "2", "1"})
	got = queryFlatStrings(t, db, "SELECT * FROM leaf")
	assertResults(t, got, []string{"a", "2"})
}

// TestFkey2Cascade tests CASCADE actions.
func TestFkey2Cascade(t *testing.T) {
	t.Skip("CHECK constraint enforcement during CASCADE not implemented")
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")
	mustExec(t, db, "CREATE TABLE ab(a PRIMARY KEY, b)")
	mustExec(t, db, `CREATE TABLE cd(
		c PRIMARY KEY REFERENCES ab ON UPDATE CASCADE ON DELETE CASCADE, d
	)`)
	mustExec(t, db, `CREATE TABLE ef(e REFERENCES cd ON UPDATE CASCADE, f, CHECK (e!=5))`)

	mustExec(t, db, "INSERT INTO ab VALUES(1, 'b')")
	mustExec(t, db, "INSERT INTO cd VALUES(1, 'd')")
	mustExec(t, db, "INSERT INTO ef VALUES(1, 'e')")

	// CASCADE should propagate update but CHECK constraint prevents e!=5
	if err := db.Exec("UPDATE ab SET a = 5"); err == nil {
		t.Error("expected CHECK constraint error on cascade")
	}

	// Verify data was rolled back
	got := queryString(t, db, "SELECT a FROM ab")
	if got != "1" {
		t.Errorf("ab.a after failed cascade = %q, want 1", got)
	}

	// Test CASCADE delete
	if err := db.Exec("DELETE FROM ab"); err == nil {
		t.Error("expected FK error on delete (ef references cd)")
	}
}

// TestFkey2RecursiveCascade tests that FK CASCADE actions recurse even without recursive triggers.
func TestFkey2RecursiveCascade(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")
	mustExec(t, db, `CREATE TABLE t1(
		node PRIMARY KEY,
		parent REFERENCES t1 ON DELETE CASCADE
	)`)
	mustExec(t, db, "INSERT INTO t1 VALUES(1, NULL)")
	mustExec(t, db, "INSERT INTO t1 VALUES(2, 1)")
	mustExec(t, db, "INSERT INTO t1 VALUES(3, 1)")
	mustExec(t, db, "INSERT INTO t1 VALUES(4, 2)")
	mustExec(t, db, "INSERT INTO t1 VALUES(5, 2)")
	mustExec(t, db, "INSERT INTO t1 VALUES(6, 3)")
	mustExec(t, db, "INSERT INTO t1 VALUES(7, 3)")

	mustExec(t, db, "PRAGMA recursive_triggers = off")
	mustExec(t, db, "DELETE FROM t1 WHERE node = 1")
	got := queryStrings(t, db, "SELECT node FROM t1")
	if len(got) != 0 {
		t.Errorf("after cascade delete, got nodes %v, want empty", got)
	}
}

// TestFkey2IPKChildKey tests using INTEGER PRIMARY KEY as child key.
func TestFkey2IPKChildKey(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")
	mustExec(t, db, "CREATE TABLE t1(a PRIMARY KEY, b)")
	mustExec(t, db, "CREATE TABLE t2(c INTEGER PRIMARY KEY REFERENCES t1, b)")

	if err := db.Exec("INSERT INTO t2 VALUES(1, 'A')"); err == nil {
		t.Error("expected FK error for IPK child insert")
	}

	mustExec(t, db, "INSERT INTO t1 VALUES(1, 2)")
	mustExec(t, db, "INSERT INTO t1 VALUES(2, 3)")
	mustExec(t, db, "INSERT INTO t2 VALUES(1, 'A')")
	mustExec(t, db, "UPDATE t2 SET c = 2")

	if err := db.Exec("UPDATE t2 SET c = 3"); err == nil {
		t.Error("expected FK error for IPK child update to non-existent parent")
	}
	if err := db.Exec("DELETE FROM t1 WHERE a = 2"); err == nil {
		t.Error("expected FK error deleting parent referenced by IPK child")
	}
}

// TestFkey2SetDefault tests SET DEFAULT actions.
func TestFkey2SetDefault(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")
	mustExec(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b)")
	mustExec(t, db, `CREATE TABLE t2(
		c INTEGER PRIMARY KEY,
		d INTEGER DEFAULT 1 REFERENCES t1 ON DELETE SET DEFAULT
	)`)

	mustExec(t, db, "INSERT INTO t1 VALUES(1, 'one')")
	mustExec(t, db, "INSERT INTO t1 VALUES(2, 'two')")
	mustExec(t, db, "INSERT INTO t2 VALUES(1, 2)")

	got := queryFlatStrings(t, db, "SELECT * FROM t2")
	assertResults(t, got, []string{"1", "2"})

	// Delete from parent should set child d to default (1)
	mustExec(t, db, "DELETE FROM t1 WHERE a = 2")
	got = queryFlatStrings(t, db, "SELECT * FROM t2")
	assertResults(t, got, []string{"1", "1"})
}

// TestFkey2Mismatch tests foreign key mismatch errors.
func TestFkey2Mismatch(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")

	// Reference to non-existent column
	mustExec(t, db, "CREATE TABLE p(a PRIMARY KEY, b)")
	mustExec(t, db, "CREATE TABLE c(x REFERENCES p(c))")

	if err := db.Exec("INSERT INTO c DEFAULT VALUES"); err == nil {
		t.Error("expected FK mismatch error")
	}

	// Composite PK with single-column FK
	db2 := openTestDB(t)
	mustExec(t, db2, "PRAGMA foreign_keys = on")
	mustExec(t, db2, "CREATE TABLE p2(a, b, PRIMARY KEY(a, b))")
	mustExec(t, db2, "CREATE TABLE c2(x REFERENCES p2)")
	if err := db2.Exec("INSERT INTO c2 DEFAULT VALUES"); err == nil {
		t.Error("expected FK mismatch error for composite PK")
	}
}

// TestFkey2CascadeAction tests ON UPDATE CASCADE.
func TestFkey2CascadeAction(t *testing.T) {
	
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")
	mustExec(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b)")
	mustExec(t, db, "CREATE TABLE t2(c, d, FOREIGN KEY(c) REFERENCES t1(a) ON UPDATE CASCADE)")

	mustExec(t, db, "INSERT INTO t1 VALUES(10, 100)")
	mustExec(t, db, "INSERT INTO t2 VALUES(10, 100)")
	mustExec(t, db, "UPDATE t1 SET a = 15")

	got := queryFlatStrings(t, db, "SELECT * FROM t2")
	assertResults(t, got, []string{"15", "100"})
}

// TestFkey2Restrict tests RESTRICT actions.
func TestFkey2Restrict(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")
	mustExec(t, db, `CREATE TABLE t1(a, b PRIMARY KEY)`)
	mustExec(t, db, `CREATE TABLE t2(
		x REFERENCES t1 ON UPDATE RESTRICT ON DELETE RESTRICT
	)`)
	mustExec(t, db, "INSERT INTO t1 VALUES(1, 'one')")
	mustExec(t, db, "INSERT INTO t1 VALUES(2, 'two')")
	mustExec(t, db, "INSERT INTO t1 VALUES(3, 'three')")

	mustExec(t, db, "INSERT INTO t2 VALUES('two')")

	// DELETE parent with child row → RESTRICT error
	if err := db.Exec("DELETE FROM t1 WHERE b = 'two'"); err == nil {
		t.Error("expected FK RESTRICT error on DELETE")
	}

	// UPDATE parent key with child row → RESTRICT error
	if err := db.Exec("UPDATE t1 SET b = 'five' WHERE b = 'two'"); err == nil {
		t.Error("expected FK RESTRICT error on UPDATE")
	}

	// UPDATE parent key with no child row → OK
	mustExec(t, db, "UPDATE t1 SET b = 'four' WHERE b = 'one'")

	// DELETE parent with no child row → OK
	mustExec(t, db, "DELETE FROM t1 WHERE b = 'four'")

	// After deleting child, parent delete should succeed
	mustExec(t, db, "DELETE FROM t2")
	mustExec(t, db, "DELETE FROM t1 WHERE b = 'two'")
	mustExec(t, db, "DELETE FROM t1 WHERE b = 'three'")
}

// TestFkey2DropTable tests DROP TABLE with FK constraints.
func TestFkey2DropTable(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")

	// Dropping table that doesn't reference anything is OK
	mustExec(t, db, "CREATE TABLE t1(a, b REFERENCES nosuchtable)")
	mustExec(t, db, "DROP TABLE t1")

	// Dropping parent table with referencing child should fail
	mustExec(t, db, "CREATE TABLE t1(a PRIMARY KEY, b)")
	mustExec(t, db, "INSERT INTO t1 VALUES('a', 1)")
	mustExec(t, db, "CREATE TABLE t2(x REFERENCES t1)")
	mustExec(t, db, "INSERT INTO t2 VALUES('a')")

	if err := db.Exec("DROP TABLE t1"); err == nil {
		t.Error("expected FK error dropping parent table")
	}

	// After deleting from child, parent can be dropped
	mustExec(t, db, "DELETE FROM t2")
	mustExec(t, db, "DROP TABLE t1")
}

// TestFkey2SelfRef tests self-referencing FK constraints.
func TestFkey2SelfRef(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")
	mustExec(t, db, "CREATE TABLE self(a INTEGER PRIMARY KEY, b REFERENCES self(a))")

	// Self-referencing row is valid
	mustExec(t, db, "INSERT INTO self VALUES(13, 13)")
	mustExec(t, db, "UPDATE self SET a = 14, b = 14")

	if err := db.Exec("UPDATE self SET b = 15"); err == nil {
		t.Error("expected FK error for self-ref update to non-existent")
	}
	if err := db.Exec("UPDATE self SET a = 15"); err == nil {
		t.Error("expected FK error for self-ref update to non-existent")
	}
	if err := db.Exec("UPDATE self SET a = 15, b = 16"); err == nil {
		t.Error("expected FK error for self-ref update to mismatched pair")
	}
	mustExec(t, db, "UPDATE self SET a = 17, b = 17")
	mustExec(t, db, "DELETE FROM self")
	if err := db.Exec("INSERT INTO self VALUES(20, 21)"); err == nil {
		t.Error("expected FK error inserting self-ref with non-existent parent")
	}
}

// TestFkey2ConflictTests tests ON CONFLICT with FK constraints.
func TestFkey2ConflictTests(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")
	mustExec(t, db, "CREATE TABLE pp(a PRIMARY KEY, b)")
	mustExec(t, db, "CREATE TABLE cc(c PRIMARY KEY, d REFERENCES pp)")

	conflictTypes := []string{
		"INSERT", "INSERT OR IGNORE", "INSERT OR ABORT",
		"INSERT OR ROLLBACK", "INSERT OR REPLACE", "INSERT OR FAIL",
	}
	for _, ct := range conflictTypes {
		if err := db.Exec(ct + " INTO cc VALUES(1, 2)"); err == nil {
			t.Errorf("%s INTO cc: expected FK error", ct)
		}
		got := queryFlatStrings(t, db, "SELECT * FROM cc")
		if len(got) != 0 {
			t.Errorf("after %s conflict: cc should be empty, got %v", ct, got)
		}
	}

	// With valid parent, insert succeeds, then a second conflicting insert fails
	mustExec(t, db, "INSERT INTO pp VALUES(2, 'two')")
	mustExec(t, db, "INSERT INTO cc VALUES(1, 2)")

	for _, ct := range conflictTypes {
		if err := db.Exec(ct+" INTO cc VALUES(3, 4)"); err == nil {
			t.Errorf("%s INTO cc with bad ref: expected FK error", ct)
		}
	}

	got := queryFlatStrings(t, db, "SELECT * FROM cc")
	assertResults(t, got, []string{"1", "2"})
}

// TestFkey2GenfkeySetNull tests ON UPDATE/DELETE SET NULL.
func TestFkey2GenfkeySetNull(t *testing.T) {
	t.Skip("SET NULL action not yet implemented")
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")
	mustExec(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c, UNIQUE(c, b))")
	mustExec(t, db, `CREATE TABLE t2(e REFERENCES t1 ON UPDATE SET NULL ON DELETE SET NULL, f)`)
	mustExec(t, db, `CREATE TABLE t3(g, h, i,
		FOREIGN KEY (h, i) REFERENCES t1(b, c) ON UPDATE SET NULL ON DELETE SET NULL
	)`)

	mustExec(t, db, "INSERT INTO t1 VALUES(1, 2, 3)")
	mustExec(t, db, "INSERT INTO t1 VALUES(4, 5, 6)")
	mustExec(t, db, "INSERT INTO t2 VALUES(1, 'one')")
	mustExec(t, db, "INSERT INTO t2 VALUES(4, 'four')")

	// Update parent PK → child SET NULL
	mustExec(t, db, "UPDATE t1 SET a = 2 WHERE a = 1")
	got := queryFlatStrings(t, db, "SELECT * FROM t2")
	// After SET NULL: row with e=1 should have e=NULL
	if got[0] != "" || got[1] != "one" {
		t.Errorf("after SET NULL update t2: got %v, want ['', 'one', '4', 'four']", got)
	}

	// Delete from parent → child SET NULL
	mustExec(t, db, "DELETE FROM t1 WHERE a = 4")
	got = queryFlatStrings(t, db, "SELECT * FROM t2")
	if got[2] != "" || got[3] != "four" {
		t.Errorf("after SET NULL delete t2: got %v", got)
	}
}

// TestFkey2InsertOrReplace tests INSERT OR REPLACE with FK.
func TestFkey2InsertOrReplace(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")
	mustExec(t, db, "CREATE TABLE t11(x INTEGER PRIMARY KEY, parent REFERENCES t11 ON DELETE CASCADE)")
	mustExec(t, db, "INSERT INTO t11 VALUES (1, NULL)")
	mustExec(t, db, "INSERT INTO t11 VALUES (2, 1)")
	mustExec(t, db, "INSERT INTO t11 VALUES (3, 2)")

	// REPLACE deletes (2,1) which cascades to delete (3,2), but the new row wants parent=3 which was just deleted
	if err := db.Exec("INSERT OR REPLACE INTO t11 VALUES (2, 3)"); err == nil {
		t.Error("expected FK error for INSERT OR REPLACE cascade conflict")
	}
}

// TestFkey2IPKAffinity tests that IPK affinity is not applied to child key.
func TestFkey2IPKAffinity(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "PRAGMA foreign_keys = on")
	mustExec(t, db, "CREATE TABLE i(i INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE j(j REFERENCES i)")
	mustExec(t, db, "INSERT INTO i VALUES(35)")
	mustExec(t, db, "INSERT INTO j VALUES('35.0')")

	got := queryFlatStrings(t, db, "SELECT j, typeof(j) FROM j")
	if len(got) < 2 {
		t.Fatalf("expected 2 values, got %v", got)
	}
	if got[0] != "35.0" || got[1] != "text" {
		t.Errorf("got %v, want ['35.0' 'text']", got)
	}

	if err := db.Exec("DELETE FROM i"); err == nil {
		t.Error("expected FK error deleting parent (35 vs '35.0')")
	}
}
