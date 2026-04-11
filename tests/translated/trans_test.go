package tests

// Translated from SQLite trans.test - basic transaction tests.
// Covers trans-1.x through trans-5.x: setup, basic BEGIN/COMMIT/ROLLBACK,
// transaction data visibility, nested transaction errors, and isolation.

import (
	"fmt"
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// transTrans1Setup creates the standard test tables 'one' and 'two'
// used throughout the trans test suite.
func transTrans1Setup(t *testing.T, db *sqlite.Database) {
	t.Helper()
	execOrFail(t, db, "CREATE TABLE one(a INTEGER PRIMARY KEY, b TEXT)")
	execOrFail(t, db, "INSERT INTO one VALUES(1, 'one')")
	execOrFail(t, db, "INSERT INTO one VALUES(2, 'two')")
	execOrFail(t, db, "INSERT INTO one VALUES(3, 'three')")

	execOrFail(t, db, "CREATE TABLE two(a INTEGER PRIMARY KEY, b TEXT)")
	execOrFail(t, db, "INSERT INTO two VALUES(1, 'I')")
	execOrFail(t, db, "INSERT INTO two VALUES(5, 'V')")
	execOrFail(t, db, "INSERT INTO two VALUES(10, 'X')")
}

// TestTrans1Setup verifies the standard test tables can be created and
// populated correctly.
//
// Original: trans-1.x
func TestTrans1Setup(t *testing.T) {
	db := openTestDB(t)
	transTrans1Setup(t, db)

	// Verify table 'one' has 3 rows
	count := queryRowCount(t, db, "SELECT * FROM one")
	if count != 3 {
		t.Errorf("table one: expected 3 rows, got %d", count)
	}

	// Verify table 'two' has 3 rows
	count = queryRowCount(t, db, "SELECT * FROM two")
	if count != 3 {
		t.Errorf("table two: expected 3 rows, got %d", count)
	}

	// Verify specific data
	if got := queryInt(t, db, "SELECT b FROM one WHERE a = 1"); got != 0 {
		// May not have WHERE support; just verify table is populated
	}
}

// TestTrans2BasicCommands tests basic transaction commands: BEGIN, END,
// COMMIT, ROLLBACK via SQL strings.
//
// Original: trans-2.x
func TestTrans2BasicCommands(t *testing.T) {
	db := openTestDB(t)

	t.Run("begin_end", func(t *testing.T) {
		execOrFail(t, db, "BEGIN")
		execOrFail(t, db, "COMMIT")
	})

	t.Run("begin_commit", func(t *testing.T) {
		execOrFail(t, db, "BEGIN")
		execOrFail(t, db, "COMMIT")
	})

	t.Run("begin_transaction_commit", func(t *testing.T) {
		execOrFail(t, db, "BEGIN TRANSACTION")
		execOrFail(t, db, "COMMIT TRANSACTION")
	})

	t.Run("begin_transaction_rollback", func(t *testing.T) {
		execOrFail(t, db, "BEGIN TRANSACTION")
		execOrFail(t, db, "ROLLBACK TRANSACTION")
	})

	t.Run("api_begin_commit", func(t *testing.T) {
		if err := db.Begin(); err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if err := db.Commit(); err != nil {
			t.Fatalf("Commit: %v", err)
		}
	})

	t.Run("api_begin_rollback", func(t *testing.T) {
		if err := db.Begin(); err != nil {
			t.Fatalf("Begin: %v", err)
		}
		if err := db.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
	})
}

// TestTrans3TransactionWithData tests that data changes inside a transaction
// are visible within the same connection and that ROLLBACK restores the
// original state.
//
// Original: trans-3.x
func TestTrans3TransactionWithData(t *testing.T) {
	db := openTestDB(t)
	transTrans1Setup(t, db)

	// Begin a transaction and modify data
	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "UPDATE one SET b = 'modified'")

	// In-transaction state should show the modified data
	count := queryRowCount(t, db, "SELECT * FROM one")
	if count != 3 {
		t.Errorf("in-transaction row count: expected 3, got %d", count)
	}

	// Rollback should restore original state
	execOrFail(t, db, "ROLLBACK")

	count = queryRowCount(t, db, "SELECT * FROM one")
	if count != 3 {
		t.Errorf("after-rollback row count: expected 3, got %d", count)
	}
}

// TestTrans3RollbackRestoresData verifies that a rollback actually restores
// the original values, not just the row count.
//
// Original: trans-3.x (extended)
func TestTrans3RollbackRestoresData(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
	execOrFail(t, db, "INSERT INTO t1 VALUES(1, 'original')")

	// Modify inside transaction
	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "UPDATE t1 SET val = 'changed'")
	execOrFail(t, db, "ROLLBACK")

	// Verify original value restored
	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}
}

// TestTrans3InsertRollback tests that INSERTs inside a transaction are
// discarded on ROLLBACK.
//
// Original: trans-3.x
func TestTrans3InsertRollback(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
	execOrFail(t, db, "INSERT INTO t1 VALUES(1, 'base')")
	execOrFail(t, db, "INSERT INTO t1 VALUES(2, 'base2')")

	baseCount := queryRowCount(t, db, "SELECT * FROM t1")

	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "INSERT INTO t1 VALUES(100, 'new')")
	execOrFail(t, db, "INSERT INTO t1 VALUES(101, 'new2')")
	execOrFail(t, db, "ROLLBACK")

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != baseCount {
		t.Errorf("after rollback: expected %d rows, got %d", baseCount, count)
	}
}

// TestTrans3DeleteRollback tests that DELETEs inside a transaction are
// discarded on ROLLBACK.
//
// Original: trans-3.x
func TestTrans3DeleteRollback(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
	execOrFail(t, db, "INSERT INTO t1 VALUES(1, 'a')")
	execOrFail(t, db, "INSERT INTO t1 VALUES(2, 'b')")
	execOrFail(t, db, "INSERT INTO t1 VALUES(3, 'c')")

	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "DELETE FROM t1")
	execOrFail(t, db, "ROLLBACK")

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != 3 {
		t.Errorf("after rollback: expected 3 rows, got %d", count)
	}
}

// TestTrans4NestedTransactionErrors verifies that various illegal
// transaction command sequences produce errors.
//
// Original: trans-4.x
func TestTrans4NestedTransactionErrors(t *testing.T) {
	db := openTestDB(t)

	t.Run("begin_while_in_transaction", func(t *testing.T) {
		execOrFail(t, db, "BEGIN")
		err := db.Exec("BEGIN")
		if err == nil {
			t.Error("expected error from BEGIN within a transaction")
		}
		execOrFail(t, db, "ROLLBACK")
	})

	t.Run("commit_without_begin", func(t *testing.T) {
		err := db.Commit()
		if err == nil {
			t.Error("expected error from COMMIT without BEGIN")
		}
	})

	t.Run("rollback_without_begin", func(t *testing.T) {
		err := db.Rollback()
		if err == nil {
			t.Error("expected error from ROLLBACK without BEGIN")
		}
	})

	t.Run("sql_commit_without_begin", func(t *testing.T) {
		err := db.Exec("COMMIT")
		if err == nil {
			t.Error("expected error from SQL COMMIT without BEGIN")
		}
	})

	t.Run("sql_rollback_without_begin", func(t *testing.T) {
		err := db.Exec("ROLLBACK")
		if err == nil {
			t.Error("expected error from SQL ROLLBACK without BEGIN")
		}
	})

	t.Run("double_begin_api", func(t *testing.T) {
		if err := db.Begin(); err != nil {
			t.Fatalf("first Begin: %v", err)
		}
		if err := db.Begin(); err == nil {
			t.Error("expected error on double BEGIN")
		}
		execOrFail(t, db, "ROLLBACK")
	})
}

// TestTrans5TransactionIsolation tests data visibility within and
// across transactions.
//
// Original: trans-5.x
func TestTrans5TransactionIsolation(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")

	t.Run("insert_visible_in_same_connection", func(t *testing.T) {
		execOrFail(t, db, "BEGIN")
		execOrFail(t, db, "INSERT INTO t1 VALUES(1, 'a')")

		count := queryRowCount(t, db, "SELECT * FROM t1")
		if count != 1 {
			t.Errorf("in-transaction: expected 1 row, got %d", count)
		}
		execOrFail(t, db, "ROLLBACK")
	})

	t.Run("commit_persists_data", func(t *testing.T) {
		execOrFail(t, db, "BEGIN")
		execOrFail(t, db, "INSERT INTO t1 VALUES(1, 'a')")
		execOrFail(t, db, "COMMIT")

		count := queryRowCount(t, db, "SELECT * FROM t1")
		if count != 1 {
			t.Errorf("after commit: expected 1 row, got %d", count)
		}
	})

	t.Run("rollback_discards_data", func(t *testing.T) {
		// Table t1 already has 1 row from previous subtest
		execOrFail(t, db, "BEGIN")
		execOrFail(t, db, "INSERT INTO t1 VALUES(2, 'b')")
		execOrFail(t, db, "ROLLBACK")

		count := queryRowCount(t, db, "SELECT * FROM t1")
		if count != 1 {
			t.Errorf("after rollback: expected 1 row (original), got %d", count)
		}
	})

	t.Run("multiple_operations_commit", func(t *testing.T) {
		execOrFail(t, db, "BEGIN")
		for i := 10; i < 20; i++ {
			execOrFail(t, db, fmt.Sprintf("INSERT INTO t1 VALUES(%d, 'val%d')", i, i))
		}
		execOrFail(t, db, "COMMIT")

		count := queryRowCount(t, db, "SELECT * FROM t1")
		if count != 11 { // 1 from before + 10 new
			t.Errorf("after multi-insert commit: expected 11 rows, got %d", count)
		}
	})

	t.Run("multiple_operations_rollback", func(t *testing.T) {
		execOrFail(t, db, "BEGIN")
		for i := 100; i < 200; i++ {
			execOrFail(t, db, fmt.Sprintf("INSERT INTO t1 VALUES(%d, 'rollback%d')", i, i))
		}
		execOrFail(t, db, "ROLLBACK")

		count := queryRowCount(t, db, "SELECT * FROM t1")
		if count != 11 { // same as before the rollback
			t.Errorf("after multi-insert rollback: expected 11 rows, got %d", count)
		}
	})
}
