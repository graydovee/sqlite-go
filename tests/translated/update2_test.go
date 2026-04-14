package tests

import "testing"

func TestUpdate2(t *testing.T) {
	t.Run("update2-1.1", func(t *testing.T) {
		t.Skip("WITHOUT ROWID tables not yet supported")
	})

	t.Run("update2-1.2", func(t *testing.T) {
		t.Skip("WITHOUT ROWID tables not yet supported")
	})

	t.Run("update2-1.3", func(t *testing.T) {
		t.Skip("WITHOUT ROWID tables not yet supported")
	})

	t.Run("update2-2.1", func(t *testing.T) {
		t.Skip("CTEs in UPDATE not yet supported")
	})

	t.Run("update2-2.2", func(t *testing.T) {
		t.Skip("CTEs in UPDATE not yet supported")
	})

	t.Run("update2-3.1", func(t *testing.T) {
		t.Skip("window functions in UPDATE not yet supported")
	})

	t.Run("update2-3.2", func(t *testing.T) {
		t.Skip("window functions in UPDATE not yet supported")
	})

	t.Run("update2-4.1", func(t *testing.T) {
		t.Skip("custom aggregate functions not yet supported")
	})

	t.Run("update2-4.2", func(t *testing.T) {
		t.Skip("custom aggregate functions not yet supported")
	})

	t.Run("update2-5.1", func(t *testing.T) {
		t.Skip("OR REPLACE / upsert not yet supported")
	})

	t.Run("update2-5.2", func(t *testing.T) {
		t.Skip("OR REPLACE / upsert not yet supported")
	})

	t.Run("update2-6.1", func(t *testing.T) {
		t.Skip("partial indexes not yet supported")
	})

	t.Run("update2-6.2", func(t *testing.T) {
		t.Skip("partial indexes not yet supported")
	})

	t.Run("update2-7.1", func(t *testing.T) {
		t.Skip("advanced UPDATE2 features not yet supported")
	})

	t.Run("update2-7.2", func(t *testing.T) {
		t.Skip("advanced UPDATE2 features not yet supported")
	})

	t.Run("update2-8.1", func(t *testing.T) {
		t.Skip("RETURNING clause not yet supported")
	})

	t.Run("update2-8.2", func(t *testing.T) {
		t.Skip("RETURNING clause not yet supported")
	})

	// update2-9.1: FROM clause in UPDATE (UPDATE ... FROM ... WHERE)
	t.Run("update2-9.1", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a INT, b INT)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1, 2)")
		mustExec(t, db, "INSERT INTO t1 VALUES(3, 4)")
		mustExec(t, db, "CREATE TABLE t2(c INT, d INT)")
		mustExec(t, db, "INSERT INTO t2 VALUES(1, 10)")
		mustExec(t, db, "INSERT INTO t2 VALUES(3, 30)")

		mustExec(t, db, "UPDATE t1 SET b = d FROM t2 WHERE a = c")
		got := queryFlatInts(t, db, "SELECT * FROM t1 ORDER BY a")
		want := []int64{1, 10, 3, 30}
		if !sliceEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	// update2-9.2: FROM clause with multiple columns
	t.Run("update2-9.2", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(id INT, val INT)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1, 100)")
		mustExec(t, db, "INSERT INTO t1 VALUES(2, 200)")
		mustExec(t, db, "INSERT INTO t1 VALUES(3, 300)")
		mustExec(t, db, "CREATE TABLE t2(id INT, new_val INT)")
		mustExec(t, db, "INSERT INTO t2 VALUES(1, 111)")
		mustExec(t, db, "INSERT INTO t2 VALUES(3, 333)")

		mustExec(t, db, "UPDATE t1 SET val = new_val FROM t2 WHERE t1.id = t2.id")
		if got := db.Changes(); got != 2 {
			t.Fatalf("db.Changes() = %d, want 2", got)
		}
		got := queryFlatInts(t, db, "SELECT * FROM t1 ORDER BY id")
		want := []int64{1, 111, 2, 200, 3, 333}
		if !sliceEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
}
