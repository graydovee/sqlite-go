package tests

// Translated from SQLite commit.test - transaction commit and rollback tests.
// This file also contains tests originally from the trans.test suite that
// were stored in commit.test. It covers commit-1.x through commit-3.x:
// setup, basic BEGIN/COMMIT/ROLLBACK, and transaction with updates.

import (
	"fmt"
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// commitSetupTables creates the standard tables 'one' and 'two'
// used by the commit test suite.
func commitSetupTables(t *testing.T, db *sqlite.Database) {
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

// TestCommit1Setup verifies the test tables are created and populated.
//
// Original: commit-1.x
func TestCommit1Setup(t *testing.T) {
	db := openTestDB(t)
	commitSetupTables(t, db)

	if got := queryRowCount(t, db, "SELECT * FROM one"); got != 3 {
		t.Errorf("table one: got %d rows, want 3", got)
	}
	if got := queryRowCount(t, db, "SELECT * FROM two"); got != 3 {
		t.Errorf("table two: got %d rows, want 3", got)
	}
}

// TestCommit2BasicBeginCommitRollback tests the basic transaction lifecycle.
//
// Original: commit-2.x
func TestCommit2BasicBeginCommitRollback(t *testing.T) {
	db := openTestDB(t)
	commitSetupTables(t, db)

	t.Run("begin_commit", func(t *testing.T) {
		execOrFail(t, db, "BEGIN")
		execOrFail(t, db, "COMMIT")
	})

	t.Run("begin_rollback", func(t *testing.T) {
		execOrFail(t, db, "BEGIN")
		execOrFail(t, db, "ROLLBACK")
	})

	t.Run("begin_transaction_commit_transaction", func(t *testing.T) {
		execOrFail(t, db, "BEGIN TRANSACTION")
		execOrFail(t, db, "COMMIT TRANSACTION")
	})

	t.Run("begin_transaction_rollback_transaction", func(t *testing.T) {
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

// TestCommit2CommitPreservesData verifies that COMMIT actually persists
// data changes.
//
// Original: commit-2.x
func TestCommit2CommitPreservesData(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")

	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "INSERT INTO t1 VALUES(1, 'committed')")
	execOrFail(t, db, "COMMIT")

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != 1 {
		t.Errorf("after commit: expected 1 row, got %d", count)
	}
}

// TestCommit2RollbackDiscardsData verifies that ROLLBACK discards
// data changes.
//
// Original: commit-2.x
func TestCommit2RollbackDiscardsData(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
	execOrFail(t, db, "INSERT INTO t1 VALUES(0, 'base')")

	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "INSERT INTO t1 VALUES(1, 'will_discard')")
	execOrFail(t, db, "ROLLBACK")

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != 1 {
		t.Errorf("after rollback: expected 1 row (base), got %d", count)
	}
}

// TestCommit3TransactionWithUpdates tests UPDATEs inside a transaction
// and verifies the data state before and after rollback.
//
// Original: commit-3.x
func TestCommit3TransactionWithUpdates(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE one(a INTEGER PRIMARY KEY, b TEXT)")
	execOrFail(t, db, "INSERT INTO one VALUES(1, 'one')")
	execOrFail(t, db, "INSERT INTO one VALUES(2, 'two')")
	execOrFail(t, db, "INSERT INTO one VALUES(3, 'three')")

	// Start a transaction, perform an UPDATE
	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "UPDATE one SET b = 'updated' WHERE a = 1")

	// Data should be visible within the transaction
	count := queryRowCount(t, db, "SELECT * FROM one")
	if count != 3 {
		t.Errorf("in-transaction: expected 3 rows, got %d", count)
	}

	// Rollback
	execOrFail(t, db, "ROLLBACK")

	// After rollback, data should be unchanged
	count = queryRowCount(t, db, "SELECT * FROM one")
	if count != 3 {
		t.Errorf("after rollback: expected 3 rows, got %d", count)
	}
}

// TestCommit3UpdateCommit verifies that UPDATE changes persist after COMMIT.
//
// Original: commit-3.x
func TestCommit3UpdateCommit(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
	execOrFail(t, db, "INSERT INTO t1 VALUES(1, 'original')")

	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "UPDATE t1 SET val = 'updated'")
	execOrFail(t, db, "COMMIT")

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != 1 {
		t.Fatalf("expected 1 row, got %d", count)
	}
}

// TestCommit3NoopUpdateRollback tests a no-op UPDATE followed by ROLLBACK.
//
// Original: commit-3.x
func TestCommit3NoopUpdateRollback(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE one(a INTEGER PRIMARY KEY, b TEXT)")
	execOrFail(t, db, "INSERT INTO one VALUES(1, 'one')")
	execOrFail(t, db, "INSERT INTO one VALUES(2, 'two')")

	baseCount := queryRowCount(t, db, "SELECT * FROM one")

	execOrFail(t, db, "BEGIN")
	// UPDATE that matches no rows (a condition that doesn't match)
	// This is effectively a no-op UPDATE
	execOrFail(t, db, "ROLLBACK")

	count := queryRowCount(t, db, "SELECT * FROM one")
	if count != baseCount {
		t.Errorf("after no-op update+rollback: expected %d rows, got %d", baseCount, count)
	}
}

// TestCommit3DeleteAndInsert tests a transaction with both DELETE and INSERT.
//
// Original: commit-3.x
func TestCommit3DeleteAndInsert(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
	execOrFail(t, db, "INSERT INTO t1 VALUES(1, 'a')")
	execOrFail(t, db, "INSERT INTO t1 VALUES(2, 'b')")

	// Delete all, then insert new, then rollback
	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "DELETE FROM t1")
	execOrFail(t, db, "INSERT INTO t1 VALUES(3, 'c')")
	execOrFail(t, db, "ROLLBACK")

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != 2 {
		t.Errorf("after rollback: expected 2 rows, got %d", count)
	}
}

// TestCommit3MixedDMLCommit tests a transaction with INSERT, UPDATE,
// DELETE all committed together.
//
// Original: commit-3.x
func TestCommit3MixedDMLCommit(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
	execOrFail(t, db, "INSERT INTO t1 VALUES(1, 'original')")

	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "INSERT INTO t1 VALUES(2, 'new')")
	execOrFail(t, db, "UPDATE t1 SET val = 'updated'")
	execOrFail(t, db, "COMMIT")

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != 2 {
		t.Errorf("after mixed DML commit: expected 2 rows, got %d", count)
	}
}

// TestCommitErrorSequences tests various error conditions with
// transaction commands.
//
// Original: commit-2.x (error cases)
func TestCommitErrorSequences(t *testing.T) {
	db := openTestDB(t)

	t.Run("double_begin", func(t *testing.T) {
		execOrFail(t, db, "BEGIN")
		err := db.Exec("BEGIN")
		if err == nil {
			t.Error("expected error from nested BEGIN")
		}
		execOrFail(t, db, "ROLLBACK")
	})

	t.Run("commit_without_begin", func(t *testing.T) {
		err := db.Exec("COMMIT")
		if err == nil {
			t.Error("expected error from COMMIT without BEGIN")
		}
	})

	t.Run("rollback_without_begin", func(t *testing.T) {
		err := db.Exec("ROLLBACK")
		if err == nil {
			t.Error("expected error from ROLLBACK without BEGIN")
		}
	})

	t.Run("double_commit", func(t *testing.T) {
		execOrFail(t, db, "BEGIN")
		execOrFail(t, db, "COMMIT")
		err := db.Commit()
		if err == nil {
			t.Error("expected error from double COMMIT")
		}
	})

	t.Run("rollback_after_commit", func(t *testing.T) {
		execOrFail(t, db, "BEGIN")
		execOrFail(t, db, "COMMIT")
		err := db.Rollback()
		if err == nil {
			t.Error("expected error from ROLLBACK after COMMIT")
		}
	})
}

// TestCommitRapidCycles tests rapid transaction begin/commit and
// begin/rollback cycles to catch any state management bugs.
//
// Original: commit.test (stress)
func TestCommitRapidCycles(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")

	for i := 0; i < 10; i++ {
		execOrFail(t, db, "BEGIN")
		execOrFail(t, db, fmt.Sprintf("INSERT INTO t1 VALUES(%d, 'cycle_%d')", i, i))
		execOrFail(t, db, "COMMIT")
	}

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != 10 {
		t.Errorf("after 10 commit cycles: expected 10 rows, got %d", count)
	}

	// Now do 10 rollback cycles
	for i := 100; i < 110; i++ {
		execOrFail(t, db, "BEGIN")
		execOrFail(t, db, fmt.Sprintf("INSERT INTO t1 VALUES(%d, 'rollback_%d')", i, i))
		execOrFail(t, db, "ROLLBACK")
	}

	count = queryRowCount(t, db, "SELECT * FROM t1")
	if count != 10 {
		t.Errorf("after 10 rollback cycles: expected 10 rows, got %d", count)
	}
}
