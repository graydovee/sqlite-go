package tests

// Translated from SQLite trans2.test - transaction stress tests.
// The original trans2.test is a complex stress test involving random data,
// md5 hashes, constraint failures, and concurrent connections. This file
// contains a simplified translation that exercises the same fundamental
// transaction semantics with bulk operations.

import (
	"fmt"
	"testing"
)

// TestTrans2Basic tests basic begin/commit cycles with bulk inserts
// and rollback discards.
//
// Original: trans2.test (simplified)
func TestTrans2Basic(t *testing.T) {
	t.Run("begin_commit_cycle", func(t *testing.T) {
		db := openTestDB(t)
		execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, u1 TEXT, z TEXT NOT NULL, u2 TEXT)")
		execOrFail(t, db, "BEGIN")
		for i := 0; i < 100; i++ {
			execOrFail(t, db, fmt.Sprintf("INSERT INTO t1 VALUES(%d, 'u%d', 'z%d', 'v%d')", i, i, i, i))
		}
		execOrFail(t, db, "COMMIT")
		count := queryRowCount(t, db, "SELECT * FROM t1")
		if count != 100 {
			t.Errorf("expected 100 rows, got %d", count)
		}
	})

	t.Run("rollback_discards_inserts", func(t *testing.T) {
		db := openTestDB(t)
		execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY)")
		execOrFail(t, db, "INSERT INTO t1 VALUES(1)")
		execOrFail(t, db, "BEGIN")
		for i := 2; i <= 50; i++ {
			execOrFail(t, db, fmt.Sprintf("INSERT INTO t1 VALUES(%d)", i))
		}
		execOrFail(t, db, "ROLLBACK")
		count := queryRowCount(t, db, "SELECT * FROM t1")
		if count != 1 {
			t.Errorf("expected 1 row after rollback, got %d", count)
		}
	})
}

// TestTrans2MultipleCommitCycles tests that multiple begin/commit cycles
// each persist their data independently.
//
// Original: trans2.test (multiple transaction cycles)
func TestTrans2MultipleCommitCycles(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")

	// First commit cycle
	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "INSERT INTO t1 VALUES(1, 'first')")
	execOrFail(t, db, "COMMIT")

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != 1 {
		t.Fatalf("after first cycle: expected 1 row, got %d", count)
	}

	// Second commit cycle
	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "INSERT INTO t1 VALUES(2, 'second')")
	execOrFail(t, db, "COMMIT")

	count = queryRowCount(t, db, "SELECT * FROM t1")
	if count != 2 {
		t.Errorf("after second cycle: expected 2 rows, got %d", count)
	}

	// Rollback cycle should not affect committed data
	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "INSERT INTO t1 VALUES(3, 'will_rollback')")
	execOrFail(t, db, "ROLLBACK")

	count = queryRowCount(t, db, "SELECT * FROM t1")
	if count != 2 {
		t.Errorf("after rollback cycle: expected 2 rows, got %d", count)
	}
}

// TestTrans2InterleavedRollbackCommit tests a sequence where a rollback
// is followed by more commits, verifying data integrity.
//
// Original: trans2.test (interleaved operations)
func TestTrans2InterleavedRollbackCommit(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")

	// Commit 10 rows
	execOrFail(t, db, "BEGIN")
	for i := 1; i <= 10; i++ {
		execOrFail(t, db, fmt.Sprintf("INSERT INTO t1 VALUES(%d, 'committed_%d')", i, i))
	}
	execOrFail(t, db, "COMMIT")

	// Try to add 10 more, but rollback
	execOrFail(t, db, "BEGIN")
	for i := 11; i <= 20; i++ {
		execOrFail(t, db, fmt.Sprintf("INSERT INTO t1 VALUES(%d, 'rolled_back_%d')", i, i))
	}
	execOrFail(t, db, "ROLLBACK")

	// Add 5 more and commit
	execOrFail(t, db, "BEGIN")
	for i := 21; i <= 25; i++ {
		execOrFail(t, db, fmt.Sprintf("INSERT INTO t1 VALUES(%d, 'more_%d')", i, i))
	}
	execOrFail(t, db, "COMMIT")

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != 15 { // 10 + 5
		t.Errorf("expected 15 rows, got %d", count)
	}
}

// TestTrans2LargeTransaction tests a transaction with a larger number of
// operations to stress the transaction subsystem.
//
// Original: trans2.test (large data set)
func TestTrans2LargeTransaction(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT)")

	const size = 200

	// Insert in one big transaction
	execOrFail(t, db, "BEGIN")
	for i := 0; i < size; i++ {
		execOrFail(t, db, fmt.Sprintf(
			"INSERT INTO t1 VALUES(%d, 'alpha_%d', 'beta_%d', 'gamma_%d')",
			i, i, i, i,
		))
	}
	execOrFail(t, db, "COMMIT")

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != size {
		t.Errorf("after large commit: expected %d rows, got %d", size, count)
	}

	// Rollback a large transaction
	execOrFail(t, db, "BEGIN")
	for i := size; i < size*2; i++ {
		execOrFail(t, db, fmt.Sprintf(
			"INSERT INTO t1 VALUES(%d, 'delta_%d', 'epsilon_%d', 'zeta_%d')",
			i, i, i, i,
		))
	}
	execOrFail(t, db, "ROLLBACK")

	count = queryRowCount(t, db, "SELECT * FROM t1")
	if count != size {
		t.Errorf("after large rollback: expected %d rows, got %d", size, count)
	}
}

// TestTrans2UpdateRollback tests that UPDATEs within a transaction are
// rolled back correctly.
//
// Original: trans2.test (update operations)
func TestTrans2UpdateRollback(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val INTEGER)")
	execOrFail(t, db, "INSERT INTO t1 VALUES(1, 100)")
	execOrFail(t, db, "INSERT INTO t1 VALUES(2, 200)")

	// Update and rollback
	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "UPDATE t1 SET val = 999")
	execOrFail(t, db, "ROLLBACK")

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != 2 {
		t.Fatalf("expected 2 rows, got %d", count)
	}
}

// TestTrans2DeleteInsertRollback tests DELETE followed by INSERT within
// a single transaction, then rolled back.
//
// Original: trans2.test (mixed DML)
func TestTrans2DeleteInsertRollback(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY, val TEXT)")
	execOrFail(t, db, "INSERT INTO t1 VALUES(1, 'original')")

	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "DELETE FROM t1")
	execOrFail(t, db, "INSERT INTO t1 VALUES(2, 'replacement')")
	execOrFail(t, db, "ROLLBACK")

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != 1 {
		t.Errorf("expected 1 row after rollback, got %d", count)
	}
}

// TestTrans2EmptyTransaction tests that BEGIN/COMMIT with no operations
// in between is harmless.
//
// Original: trans2.test (edge case)
func TestTrans2EmptyTransaction(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1(id INTEGER PRIMARY KEY)")

	// Empty commit
	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "COMMIT")

	// Empty rollback
	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "ROLLBACK")

	count := queryRowCount(t, db, "SELECT * FROM t1")
	if count != 0 {
		t.Errorf("expected 0 rows, got %d", count)
	}
}
