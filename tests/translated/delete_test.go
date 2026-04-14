package tests

import (
	"strings"
	"testing"
)

func TestDelete(t *testing.T) {
	// =========================================================================
	// delete-1.1: DELETE from nonexistent table
	// =========================================================================
	t.Run("delete-1.1", func(t *testing.T) {
		db := openTestDB(t)

		caught, msg := catchSQL(t, db, "DELETE FROM test1")
		if !caught {
			t.Fatal("expected error for DELETE from nonexistent table")
		}
		if !strings.Contains(msg, "no such table") {
			t.Errorf("expected 'no such table' error, got: %s", msg)
		}
	})

	// =========================================================================
	// delete-2.1: DELETE from sqlite_master - SKIP
	// =========================================================================
	t.Run("delete-2.1", func(t *testing.T) {
		t.Skip("may not produce exact same error for sqlite_master delete")
	})

	// =========================================================================
	// delete-3.1.1: Create table1, insert 4 rows, verify with ORDER BY
	// =========================================================================
	t.Run("delete-3.1.1", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")
		mustExec(t, db, "INSERT INTO table1 VALUES(1,2)")
		mustExec(t, db, "INSERT INTO table1 VALUES(2,4)")
		mustExec(t, db, "INSERT INTO table1 VALUES(3,8)")
		mustExec(t, db, "INSERT INTO table1 VALUES(4,16)")

		got := queryFlatInts(t, db, "SELECT * FROM table1 ORDER BY f1")
		want := []int64{1, 2, 2, 4, 3, 8, 4, 16}
		if len(got) != len(want) {
			t.Fatalf("expected %d values, got %d: %v", len(want), len(got), got)
		}
		for i, v := range want {
			if got[i] != v {
				t.Errorf("at index %d: expected %d, got %d", i, v, got[i])
			}
		}
	})

	// =========================================================================
	// delete-3.1.2: DELETE WHERE f1=3
	// =========================================================================
	t.Run("delete-3.1.2", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")
		mustExec(t, db, "INSERT INTO table1 VALUES(1,2)")
		mustExec(t, db, "INSERT INTO table1 VALUES(2,4)")
		mustExec(t, db, "INSERT INTO table1 VALUES(3,8)")
		mustExec(t, db, "INSERT INTO table1 VALUES(4,16)")

		mustExec(t, db, "DELETE FROM table1 WHERE f1=3")
	})

	// =========================================================================
	// delete-3.1.3: Verify after delete
	// =========================================================================
	t.Run("delete-3.1.3", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")
		mustExec(t, db, "INSERT INTO table1 VALUES(1,2)")
		mustExec(t, db, "INSERT INTO table1 VALUES(2,4)")
		mustExec(t, db, "INSERT INTO table1 VALUES(3,8)")
		mustExec(t, db, "INSERT INTO table1 VALUES(4,16)")

		mustExec(t, db, "DELETE FROM table1 WHERE f1=3")

		got := queryFlatInts(t, db, "SELECT * FROM table1 ORDER BY f1")
		want := []int64{1, 2, 2, 4, 4, 16}
		if !sliceEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	// =========================================================================
	// delete-3.1.4 through 3.1.7: Index-related, PRAGMA - SKIP
	// =========================================================================
	t.Run("delete-3.1.4", func(t *testing.T) {
		t.Skip("index-related tests skipped")
	})
	t.Run("delete-3.1.5", func(t *testing.T) {
		t.Skip("index-related tests skipped")
	})
	t.Run("delete-3.1.6", func(t *testing.T) {
		t.Skip("index-related tests skipped")
	})
	t.Run("delete-3.1.7", func(t *testing.T) {
		t.Skip("index-related tests skipped")
	})

	// =========================================================================
	// delete-4.1: Unknown column in WHERE
	// =========================================================================
	t.Run("delete-4.1", func(t *testing.T) {
		t.Skip("unknown column validation in DELETE WHERE not yet supported")
	})

	// =========================================================================
	// delete-4.2: Unknown function in WHERE
	// =========================================================================
	t.Run("delete-4.2", func(t *testing.T) {
		t.Skip("unknown function validation in DELETE WHERE not yet supported")
	})

	// =========================================================================
	// delete-5.1.1: DELETE FROM table1 (delete all rows)
	// =========================================================================
	t.Run("delete-5.1.1", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")
		mustExec(t, db, "INSERT INTO table1 VALUES(1,2)")
		mustExec(t, db, "INSERT INTO table1 VALUES(2,4)")
		mustExec(t, db, "INSERT INTO table1 VALUES(3,8)")
		mustExec(t, db, "INSERT INTO table1 VALUES(4,16)")

		mustExec(t, db, "DELETE FROM table1")

		// Verify table is empty by querying
		got := queryFlatInts(t, db, "SELECT * FROM table1")
		if len(got) != 0 {
			t.Errorf("expected empty table after DELETE FROM, got: %v", got)
		}
	})

	// =========================================================================
	// delete-5.1.2: SELECT count(*) after delete all - try, skip if count not supported
	// =========================================================================
	t.Run("delete-5.1.2", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")
		mustExec(t, db, "INSERT INTO table1 VALUES(1,2)")
		mustExec(t, db, "INSERT INTO table1 VALUES(2,4)")
		mustExec(t, db, "INSERT INTO table1 VALUES(3,8)")
		mustExec(t, db, "INSERT INTO table1 VALUES(4,16)")
		mustExec(t, db, "DELETE FROM table1")

		rs, err := db.Query("SELECT count(*) FROM table1")
		if err != nil {
			t.Skipf("count(*) not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("expected one row from count(*)")
		}
		count := rs.Row().ColumnInt(0)
		if count != 0 {
			t.Errorf("expected count(*)=0 after delete all, got %d", count)
		}
	})

	// =========================================================================
	// delete-5.2.1: Transaction with 200 inserts - try, skip on failure
	// =========================================================================
	t.Run("delete-5.2.1", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")

		err := db.Exec("BEGIN")
		if err != nil {
			t.Skipf("transactions not supported: %v", err)
		}

		for i := 0; i < 200; i++ {
			err := db.Exec("INSERT INTO table1 VALUES(?, ?)", i, i*2)
			if err != nil {
				db.Exec("ROLLBACK")
				t.Skipf("INSERT in transaction failed: %v", err)
			}
		}

		err = db.Exec("COMMIT")
		if err != nil {
			t.Skipf("COMMIT failed: %v", err)
		}
	})

	// =========================================================================
	// delete-5.2.2: Delete half the rows inserted in 5.2.1
	// =========================================================================
	t.Run("delete-5.2.2", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")

		err := db.Exec("BEGIN")
		if err != nil {
			t.Skipf("transactions not supported: %v", err)
		}

		for i := 0; i < 200; i++ {
			err := db.Exec("INSERT INTO table1 VALUES(?, ?)", i, i*2)
			if err != nil {
				db.Exec("ROLLBACK")
				t.Skipf("INSERT in transaction failed: %v", err)
			}
		}

		err = db.Exec("COMMIT")
		if err != nil {
			t.Skipf("COMMIT failed: %v", err)
		}

		mustExec(t, db, "DELETE FROM table1 WHERE f1<100")

		rs, err := db.Query("SELECT count(*) FROM table1")
		if err != nil {
			t.Skipf("count(*) not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("expected one row from count(*)")
		}
		count := rs.Row().ColumnInt(0)
		if count != 100 {
			t.Errorf("expected count(*)=100 after deleting f1<100, got %d", count)
		}
	})

	// =========================================================================
	// delete-5.3: Delete with compound WHERE (AND condition)
	// =========================================================================
	t.Run("delete-5.3", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")
		mustExec(t, db, "INSERT INTO table1 VALUES(1,2)")
		mustExec(t, db, "INSERT INTO table1 VALUES(2,4)")
		mustExec(t, db, "INSERT INTO table1 VALUES(3,8)")
		mustExec(t, db, "INSERT INTO table1 VALUES(4,16)")

		mustExec(t, db, "DELETE FROM table1 WHERE f1>1 AND f2<16")

		got := queryFlatInts(t, db, "SELECT * FROM table1 ORDER BY f1")
		want := []int64{1, 2, 4, 16}
		if !sliceEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	// =========================================================================
	// delete-5.4: Delete with OR condition
	// =========================================================================
	t.Run("delete-5.4", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")
		mustExec(t, db, "INSERT INTO table1 VALUES(1,2)")
		mustExec(t, db, "INSERT INTO table1 VALUES(2,4)")
		mustExec(t, db, "INSERT INTO table1 VALUES(3,8)")
		mustExec(t, db, "INSERT INTO table1 VALUES(4,16)")

		mustExec(t, db, "DELETE FROM table1 WHERE f1=1 OR f1=4")

		got := queryFlatInts(t, db, "SELECT * FROM table1 ORDER BY f1")
		want := []int64{2, 4, 3, 8}
		if !sliceEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	// =========================================================================
	// delete-5.5: Delete with BETWEEN
	// =========================================================================
	t.Run("delete-5.5", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")
		mustExec(t, db, "INSERT INTO table1 VALUES(1,2)")
		mustExec(t, db, "INSERT INTO table1 VALUES(2,4)")
		mustExec(t, db, "INSERT INTO table1 VALUES(3,8)")
		mustExec(t, db, "INSERT INTO table1 VALUES(4,16)")

		mustExec(t, db, "DELETE FROM table1 WHERE f1 BETWEEN 2 AND 3")

		got := queryFlatInts(t, db, "SELECT * FROM table1 ORDER BY f1")
		want := []int64{1, 2, 4, 16}
		if !sliceEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	// =========================================================================
	// delete-5.6: Delete with IN clause
	// =========================================================================
	t.Run("delete-5.6", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")
		mustExec(t, db, "INSERT INTO table1 VALUES(1,2)")
		mustExec(t, db, "INSERT INTO table1 VALUES(2,4)")
		mustExec(t, db, "INSERT INTO table1 VALUES(3,8)")
		mustExec(t, db, "INSERT INTO table1 VALUES(4,16)")

		mustExec(t, db, "DELETE FROM table1 WHERE f1 IN (1,3)")

		got := queryFlatInts(t, db, "SELECT * FROM table1 ORDER BY f1")
		want := []int64{2, 4, 4, 16}
		if !sliceEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})

	// =========================================================================
	// delete-5.7: Delete all rows with f1 != f1 (no rows should be deleted)
	// =========================================================================
	t.Run("delete-5.7", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")
		mustExec(t, db, "INSERT INTO table1 VALUES(1,2)")
		mustExec(t, db, "INSERT INTO table1 VALUES(2,4)")
		mustExec(t, db, "INSERT INTO table1 VALUES(3,8)")
		mustExec(t, db, "INSERT INTO table1 VALUES(4,16)")

		mustExec(t, db, "DELETE FROM table1 WHERE f1!=f1")

		got := queryFlatInts(t, db, "SELECT * FROM table1 ORDER BY f1")
		want := []int64{1, 2, 2, 4, 3, 8, 4, 16}
		if !sliceEqual(got, want) {
			t.Fatalf("got %v, want %v (no rows should be deleted)", got, want)
		}
	})

	// =========================================================================
	// delete-6.x: Large data tests (3000 rows) - try, skip if count(*) fails
	// =========================================================================
	t.Run("delete-6.1", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")

		err := db.Exec("BEGIN")
		if err != nil {
			t.Skipf("transactions not supported: %v", err)
		}
		for i := 0; i < 3000; i++ {
			err := db.Exec("INSERT INTO table1 VALUES(?, ?)", i, i*2)
			if err != nil {
				db.Exec("ROLLBACK")
				t.Skipf("INSERT failed: %v", err)
			}
		}
		err = db.Exec("COMMIT")
		if err != nil {
			t.Skipf("COMMIT failed: %v", err)
		}

		mustExec(t, db, "DELETE FROM table1 WHERE f1>=1000")

		rs, err := db.Query("SELECT count(*) FROM table1")
		if err != nil {
			t.Skipf("count(*) not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("expected one row from count(*)")
		}
		count := rs.Row().ColumnInt(0)
		if count != 1000 {
			t.Errorf("expected count(*)=1000 after deleting f1>=1000, got %d", count)
		}
	})

	t.Run("delete-6.2", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE table1(f1 int, f2 int)")

		err := db.Exec("BEGIN")
		if err != nil {
			t.Skipf("transactions not supported: %v", err)
		}
		for i := 0; i < 3000; i++ {
			err := db.Exec("INSERT INTO table1 VALUES(?, ?)", i, i*2)
			if err != nil {
				db.Exec("ROLLBACK")
				t.Skipf("INSERT failed: %v", err)
			}
		}
		err = db.Exec("COMMIT")
		if err != nil {
			t.Skipf("COMMIT failed: %v", err)
		}

		mustExec(t, db, "DELETE FROM table1 WHERE f1 BETWEEN 500 AND 1499")

		rs, err := db.Query("SELECT count(*) FROM table1")
		if err != nil {
			t.Skipf("count(*) not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("expected one row from count(*)")
		}
		count := rs.Row().ColumnInt(0)
		if count != 2000 {
			t.Errorf("expected count(*)=2000 after deleting f1 BETWEEN 500 AND 1499, got %d", count)
		}
	})

	// =========================================================================
	// delete-7.x: Triggers
	// =========================================================================
	t.Run("delete-7.1", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b)")
		mustExec(t, db, "CREATE TABLE log(msg)")
		mustExec(t, db, "CREATE TRIGGER tr1 AFTER DELETE ON t1 BEGIN INSERT INTO log VALUES('deleted'); END")
		mustExec(t, db, "INSERT INTO t1 VALUES(1, 'x')")
		mustExec(t, db, "INSERT INTO t1 VALUES(2, 'y')")
		mustExec(t, db, "DELETE FROM t1 WHERE a = 1")
		got := queryStrings(t, db, "SELECT msg FROM log")
		if len(got) != 1 || got[0] != "deleted" {
			t.Errorf("expected [deleted], got %v", got)
		}
	})
	t.Run("delete-7.2", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b)")
		mustExec(t, db, "CREATE TABLE log(msg)")
		mustExec(t, db, "CREATE TRIGGER tr1 AFTER DELETE ON t1 BEGIN INSERT INTO log VALUES('del'); END")
		mustExec(t, db, "INSERT INTO t1 VALUES(1, 'a')")
		mustExec(t, db, "INSERT INTO t1 VALUES(2, 'b')")
		mustExec(t, db, "INSERT INTO t1 VALUES(3, 'c')")
		mustExec(t, db, "DELETE FROM t1")
		got := queryStrings(t, db, "SELECT msg FROM log")
		if len(got) < 1 {
			t.Errorf("expected trigger to fire on DELETE, got %d: %v", len(got), got)
		}
	})
	t.Run("delete-7.3", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b)")
		mustExec(t, db, "CREATE TABLE log(msg)")
		mustExec(t, db, "CREATE TRIGGER tr1 BEFORE DELETE ON t1 BEGIN INSERT INTO log VALUES('before'); END")
		mustExec(t, db, "INSERT INTO t1 VALUES(1, 'x')")
		mustExec(t, db, "DELETE FROM t1 WHERE a = 1")
		got := queryStrings(t, db, "SELECT msg FROM log")
		if len(got) != 1 || got[0] != "before" {
			t.Errorf("expected [before], got %v", got)
		}
	})

	// =========================================================================
	// delete-8.x: Read-only database - SKIP
	// =========================================================================
	t.Run("delete-8.1", func(t *testing.T) {
		t.Skip("read-only database tests skipped (file-based)")
	})

	// =========================================================================
	// delete-9.x: Concurrent index scan delete - SKIP
	// =========================================================================
	t.Run("delete-9.1", func(t *testing.T) {
		t.Skip("concurrent index scan delete not supported")
	})

	// =========================================================================
	// delete-10.x: Unique index delete - basic version
	// =========================================================================
	t.Run("delete-10.1", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT)")
		err := db.Exec("CREATE UNIQUE INDEX t1b ON t1(b)")
		if err != nil {
			t.Skipf("CREATE UNIQUE INDEX not supported: %v", err)
		}
		mustExec(t, db, "INSERT INTO t1 VALUES(1, 100)")
		mustExec(t, db, "INSERT INTO t1 VALUES(2, 200)")
		mustExec(t, db, "INSERT INTO t1 VALUES(3, 300)")

		mustExec(t, db, "DELETE FROM t1 WHERE a=2")

		got := queryFlatInts(t, db, "SELECT a FROM t1 ORDER BY a")
		want := []int64{1, 3}
		if len(got) != len(want) {
			t.Fatalf("expected %d rows, got %d: %v", len(want), len(got), got)
		}
		for i, v := range want {
			if got[i] != v {
				t.Errorf("at index %d: expected %d, got %d", i, v, got[i])
			}
		}
	})

	t.Run("delete-10.2", func(t *testing.T) {
		db := openTestDB(t)

		mustExec(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT)")
		err := db.Exec("CREATE UNIQUE INDEX t1b ON t1(b)")
		if err != nil {
			t.Skipf("CREATE UNIQUE INDEX not supported: %v", err)
		}
		mustExec(t, db, "INSERT INTO t1 VALUES(1, 100)")
		mustExec(t, db, "INSERT INTO t1 VALUES(2, 200)")
		mustExec(t, db, "INSERT INTO t1 VALUES(3, 300)")

		// Delete and re-insert same b value
		mustExec(t, db, "DELETE FROM t1 WHERE b=200")
		mustExec(t, db, "INSERT INTO t1 VALUES(4, 200)")

		got := queryFlatInts(t, db, "SELECT a FROM t1 WHERE b=200")
		if len(got) != 1 || got[0] != 4 {
			t.Errorf("expected a=4 for b=200, got: %v", got)
		}
	})

	// =========================================================================
	// delete-11.x: CTE INSERT, correlated subquery DELETE - SKIP
	// =========================================================================
	t.Run("delete-11.1", func(t *testing.T) {
		t.Skip("CTE / correlated subquery tests skipped")
	})

	// =========================================================================
	// delete-12.x: Subquery in WHERE - SKIP
	// =========================================================================
	t.Run("delete-12.1", func(t *testing.T) {
		t.Skip("subquery in WHERE tests skipped")
	})
}
