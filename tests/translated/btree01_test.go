package tests

import (
	"fmt"
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// ============================================================================
// btree01.test translations
//
// Original tests verify b-tree balance() routine correctness with various
// blob sizes and page sizes. They use WITH RECURSIVE CTEs, zeroblob(),
// PRAGMA page_size, PRAGMA integrity_check, WITHOUT ROWID tables, and
// LEFT/RIGHT JOINs.
//
// Most tests depend on PRAGMA page_size, WITH RECURSIVE CTEs, and
// PRAGMA integrity_check, which are not yet supported. The full test
// logic is translated so they can be enabled as features land.
// ============================================================================

// insertZeroblobRows inserts n rows with INTEGER PRIMARY KEY and zeroblob(size).
// This replaces the WITH RECURSIVE CTE pattern used in the original tests:
//   WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<N)
//   INSERT INTO t1(a,b) SELECT i, zeroblob(SIZE) FROM c;
func insertZeroblobRows(t *testing.T, db *sqlite.Database, n, blobSize int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		sql := fmt.Sprintf("INSERT INTO t1(a,b) VALUES(%d, zeroblob(%d))", i, blobSize)
		if err := db.Exec(sql); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}
}

// TestBTree01_1_1 translates btree01-1.1:
// Basic balance() test with page_size=65536, 30 rows of zeroblob(6500),
// then update all to zeroblob(3000), then update row 2 to zeroblob(64000).
// Expected: PRAGMA integrity_check → "ok"
func TestBTree01_1_1(t *testing.T) {
	t.Skip("PRAGMA page_size and PRAGMA integrity_check not yet supported")

	db := openTestDB(t)

	execSQL(t, db, "PRAGMA page_size=65536")
	execSQL(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB)")
	insertZeroblobRows(t, db, 30, 6500)
	execSQL(t, db, "UPDATE t1 SET b=zeroblob(3000)")
	execSQL(t, db, "UPDATE t1 SET b=zeroblob(64000) WHERE a=2")

	got := queryText(t, db, "PRAGMA integrity_check")
	if got != "ok" {
		t.Errorf("integrity_check = %q, want 'ok'", got)
	}
}

// TestBTree01_1_2 translates btree01-1.2.$i:
// For each i in 1..30: delete all, insert 30 rows with zeroblob(6500),
// update all to zeroblob(3000), update row i to zeroblob(64000).
// Expected: PRAGMA integrity_check → "ok"
func TestBTree01_1_2(t *testing.T) {
	t.Skip("PRAGMA integrity_check and DELETE FROM not yet supported")

	db := openTestDB(t)
	execSQL(t, db, "PRAGMA page_size=65536")
	execSQL(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB)")

	for i := 1; i <= 30; i++ {
		i := i
		t.Run(fmt.Sprintf("btree01-1.2.%d", i), func(t *testing.T) {
			execSQL(t, db, "DELETE FROM t1")
			insertZeroblobRows(t, db, 30, 6500)
			execSQL(t, db, "UPDATE t1 SET b=zeroblob(3000)")
			execSQL(t, db, fmt.Sprintf("UPDATE t1 SET b=zeroblob(64000) WHERE a=%d", i))

			got := queryText(t, db, "PRAGMA integrity_check")
			if got != "ok" {
				t.Errorf("iteration %d: integrity_check = %q, want 'ok'", i, got)
			}
		})
	}
}

// TestBTree01_1_3 translates btree01-1.3.$i:
// Like 1.2 but with zeroblob(2000) for the bulk update.
func TestBTree01_1_3(t *testing.T) {
	t.Skip("PRAGMA integrity_check and DELETE FROM not yet supported")

	db := openTestDB(t)
	execSQL(t, db, "PRAGMA page_size=65536")
	execSQL(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB)")

	for i := 1; i <= 30; i++ {
		i := i
		t.Run(fmt.Sprintf("btree01-1.3.%d", i), func(t *testing.T) {
			execSQL(t, db, "DELETE FROM t1")
			insertZeroblobRows(t, db, 30, 6500)
			execSQL(t, db, "UPDATE t1 SET b=zeroblob(2000)")
			execSQL(t, db, fmt.Sprintf("UPDATE t1 SET b=zeroblob(64000) WHERE a=%d", i))

			got := queryText(t, db, "PRAGMA integrity_check")
			if got != "ok" {
				t.Errorf("iteration %d: integrity_check = %q, want 'ok'", i, got)
			}
		})
	}
}

// TestBTree01_1_4 translates btree01-1.4.$i:
// Insert 30 rows with zeroblob(6500), update in three groups by a%3 to
// zeroblob(6499), then update one row to zeroblob(64000).
func TestBTree01_1_4(t *testing.T) {
	t.Skip("PRAGMA integrity_check and DELETE FROM not yet supported")

	db := openTestDB(t)
	execSQL(t, db, "PRAGMA page_size=65536")
	execSQL(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB)")

	for i := 1; i <= 30; i++ {
		i := i
		t.Run(fmt.Sprintf("btree01-1.4.%d", i), func(t *testing.T) {
			execSQL(t, db, "DELETE FROM t1")
			insertZeroblobRows(t, db, 30, 6500)
			execSQL(t, db, "UPDATE t1 SET b=zeroblob(6499) WHERE (a%3)==0")
			execSQL(t, db, "UPDATE t1 SET b=zeroblob(6499) WHERE (a%3)==1")
			execSQL(t, db, "UPDATE t1 SET b=zeroblob(6499) WHERE (a%3)==2")
			execSQL(t, db, fmt.Sprintf("UPDATE t1 SET b=zeroblob(64000) WHERE a=%d", i))

			got := queryText(t, db, "PRAGMA integrity_check")
			if got != "ok" {
				t.Errorf("iteration %d: integrity_check = %q, want 'ok'", i, got)
			}
		})
	}
}

// TestBTree01_1_5 translates btree01-1.5.$i:
// Uses zeroblob(6542) for inserts, zeroblob(2331) for bulk update,
// then zeroblob(65496) for the single row update.
func TestBTree01_1_5(t *testing.T) {
	t.Skip("PRAGMA integrity_check and DELETE FROM not yet supported")

	db := openTestDB(t)
	execSQL(t, db, "PRAGMA page_size=65536")
	execSQL(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB)")

	for i := 1; i <= 30; i++ {
		i := i
		t.Run(fmt.Sprintf("btree01-1.5.%d", i), func(t *testing.T) {
			execSQL(t, db, "DELETE FROM t1")
			insertZeroblobRows(t, db, 30, 6542)
			execSQL(t, db, "UPDATE t1 SET b=zeroblob(2331)")
			execSQL(t, db, fmt.Sprintf("UPDATE t1 SET b=zeroblob(65496) WHERE a=%d", i))

			got := queryText(t, db, "PRAGMA integrity_check")
			if got != "ok" {
				t.Errorf("iteration %d: integrity_check = %q, want 'ok'", i, got)
			}
		})
	}
}

// TestBTree01_1_6 translates btree01-1.6.$i:
// Like 1.5 but with zeroblob(2332) for the bulk update.
func TestBTree01_1_6(t *testing.T) {
	t.Skip("PRAGMA integrity_check and DELETE FROM not yet supported")

	db := openTestDB(t)
	execSQL(t, db, "PRAGMA page_size=65536")
	execSQL(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB)")

	for i := 1; i <= 30; i++ {
		i := i
		t.Run(fmt.Sprintf("btree01-1.6.%d", i), func(t *testing.T) {
			execSQL(t, db, "DELETE FROM t1")
			insertZeroblobRows(t, db, 30, 6542)
			execSQL(t, db, "UPDATE t1 SET b=zeroblob(2332)")
			execSQL(t, db, fmt.Sprintf("UPDATE t1 SET b=zeroblob(65496) WHERE a=%d", i))

			got := queryText(t, db, "PRAGMA integrity_check")
			if got != "ok" {
				t.Errorf("iteration %d: integrity_check = %q, want 'ok'", i, got)
			}
		})
	}
}

// TestBTree01_1_7 translates btree01-1.7.$i:
// Insert 30 rows with zeroblob(6500), update all to zeroblob(1),
// then update one row to zeroblob(65000).
func TestBTree01_1_7(t *testing.T) {
	t.Skip("PRAGMA integrity_check and DELETE FROM not yet supported")

	db := openTestDB(t)
	execSQL(t, db, "PRAGMA page_size=65536")
	execSQL(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB)")

	for i := 1; i <= 30; i++ {
		i := i
		t.Run(fmt.Sprintf("btree01-1.7.%d", i), func(t *testing.T) {
			execSQL(t, db, "DELETE FROM t1")
			insertZeroblobRows(t, db, 30, 6500)
			execSQL(t, db, "UPDATE t1 SET b=zeroblob(1)")
			execSQL(t, db, fmt.Sprintf("UPDATE t1 SET b=zeroblob(65000) WHERE a=%d", i))

			got := queryText(t, db, "PRAGMA integrity_check")
			if got != "ok" {
				t.Errorf("iteration %d: integrity_check = %q, want 'ok'", i, got)
			}
		})
	}
}

// TestBTree01_1_8 translates btree01-1.8.$i:
// 31 rows instead of 30, zeroblob(6500) insert, zeroblob(4000) bulk update,
// zeroblob(65000) single row update.
func TestBTree01_1_8(t *testing.T) {
	t.Skip("PRAGMA integrity_check and DELETE FROM not yet supported")

	db := openTestDB(t)
	execSQL(t, db, "PRAGMA page_size=65536")
	execSQL(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b BLOB)")

	for i := 1; i <= 31; i++ {
		i := i
		t.Run(fmt.Sprintf("btree01-1.8.%d", i), func(t *testing.T) {
			execSQL(t, db, "DELETE FROM t1")
			insertZeroblobRows(t, db, 31, 6500)
			execSQL(t, db, "UPDATE t1 SET b=zeroblob(4000)")
			execSQL(t, db, fmt.Sprintf("UPDATE t1 SET b=zeroblob(65000) WHERE a=%d", i))

			got := queryText(t, db, "PRAGMA integrity_check")
			if got != "ok" {
				t.Errorf("iteration %d: integrity_check = %q, want 'ok'", i, got)
			}
		})
	}
}

// TestBTree01_2_1 translates btree01-2.1 (OSSFuzz issue 45329):
// WITHOUT ROWID table, WITH RECURSIVE insert, LEFT JOIN.
// Expected: {198 99 187 {} 100 50}
func TestBTree01_2_1(t *testing.T) {
	t.Skip("PRAGMA page_size, WITH RECURSIVE CTE not yet supported")

	db := openTestDB(t)

	execSQL(t, db, "PRAGMA page_size=1024")
	execSQL(t, db, "CREATE TABLE t1(a INT PRIMARY KEY, b BLOB, c INT) WITHOUT ROWID")

	// Insert 100 rows: a=x*2, b=zeroblob(100), c=x
	for x := 1; x <= 100; x++ {
		sql := fmt.Sprintf("INSERT INTO t1(a,b,c) VALUES(%d, zeroblob(100), %d)", x*2, x)
		execSQL(t, db, sql)
	}

	execSQL(t, db, "UPDATE t1 SET b=zeroblob(1000) WHERE a=198")
	execSQL(t, db, "CREATE TABLE t2(x INTEGER PRIMARY KEY, y INT)")
	execSQL(t, db, "INSERT INTO t2(y) VALUES(198),(187),(100)")

	rs, err := db.Query("SELECT y, c FROM t2 LEFT JOIN t1 ON y=a ORDER BY x")
	if err != nil {
		t.Fatalf("LEFT JOIN query: %v", err)
	}
	defer rs.Close()

	expected := []struct {
		y int64
		c interface{} // nil means NULL
	}{
		{198, int64(99)},
		{187, nil},
		{100, int64(50)},
	}
	rowIdx := 0
	for rs.Next() {
		if rowIdx >= len(expected) {
			t.Fatalf("too many rows, got more than %d", len(expected))
		}
		row := rs.Row()
		y := row.ColumnInt(0)
		if int64(y) != expected[rowIdx].y {
			t.Errorf("row %d: y=%d, want %d", rowIdx, y, expected[rowIdx].y)
		}
		if expected[rowIdx].c == nil {
			if !row.ColumnIsNull(1) {
				t.Errorf("row %d: c should be NULL", rowIdx)
			}
		} else {
			c := row.ColumnInt(1)
			if int64(c) != expected[rowIdx].c.(int64) {
				t.Errorf("row %d: c=%d, want %d", rowIdx, c, expected[rowIdx].c.(int64))
			}
		}
		rowIdx++
	}
	if rowIdx != len(expected) {
		t.Errorf("got %d rows, want %d", rowIdx, len(expected))
	}
}

// TestBTree01_2_2 translates btree01-2.2:
// RIGHT JOIN variant of 2.1.
// Expected: {198 99 187 {} 100 50}
func TestBTree01_2_2(t *testing.T) {
	t.Skip("PRAGMA page_size, WITH RECURSIVE CTE, RIGHT JOIN not yet supported")

	db := openTestDB(t)

	execSQL(t, db, "PRAGMA page_size=1024")
	execSQL(t, db, "CREATE TABLE t1(a INT PRIMARY KEY, b BLOB, c INT) WITHOUT ROWID")

	for x := 1; x <= 100; x++ {
		sql := fmt.Sprintf("INSERT INTO t1(a,b,c) VALUES(%d, zeroblob(100), %d)", x*2, x)
		execSQL(t, db, sql)
	}

	execSQL(t, db, "UPDATE t1 SET b=zeroblob(1000) WHERE a=198")
	execSQL(t, db, "CREATE TABLE t2(x INTEGER PRIMARY KEY, y INT)")
	execSQL(t, db, "INSERT INTO t2(y) VALUES(198),(187),(100)")

	rs, err := db.Query("SELECT y, c FROM t1 RIGHT JOIN t2 ON y=a ORDER BY x")
	if err != nil {
		t.Fatalf("RIGHT JOIN query: %v", err)
	}
	defer rs.Close()

	expected := []struct {
		y int64
		c interface{}
	}{
		{198, int64(99)},
		{187, nil},
		{100, int64(50)},
	}
	rowIdx := 0
	for rs.Next() {
		if rowIdx >= len(expected) {
			t.Fatalf("too many rows")
		}
		row := rs.Row()
		y := row.ColumnInt(0)
		if int64(y) != expected[rowIdx].y {
			t.Errorf("row %d: y=%d, want %d", rowIdx, y, expected[rowIdx].y)
		}
		if expected[rowIdx].c == nil {
			if !row.ColumnIsNull(1) {
				t.Errorf("row %d: c should be NULL", rowIdx)
			}
		} else {
			c := row.ColumnInt(1)
			if int64(c) != expected[rowIdx].c.(int64) {
				t.Errorf("row %d: c=%d, want %d", rowIdx, c, expected[rowIdx].c.(int64))
			}
		}
		rowIdx++
	}
	if rowIdx != len(expected) {
		t.Errorf("got %d rows, want %d", rowIdx, len(expected))
	}
}

// Ensure unused imports are referenced
var _ = fmt.Sprintf
var _ = sqlite.ColInteger
