package tests

import (
	"fmt"
	"testing"
)

// ============================================================================
// index2.test translations
//
// Original tests focus on:
// - Creating a table with 1000 columns
// - Inserting data into that table
// - Querying specific columns
// - Creating an index with 1000 columns
// - ORDER BY with many columns
// ============================================================================

// createTable1000Cols returns a SQL statement to create a table with 1000 columns.
func createTable1000Cols() string {
	sql := "CREATE TABLE t1("
	for i := 1; i < 1000; i++ {
		sql += fmt.Sprintf("c%d,", i)
	}
	sql += "c1000)"
	return sql
}

// insertRow1000Vals returns a SQL INSERT with 1000 values starting at base.
func insertRow1000Vals(base int) string {
	sql := "INSERT INTO t1 VALUES("
	for i := 1; i < 1000; i++ {
		sql += fmt.Sprintf("%d,", base+i)
	}
	sql += fmt.Sprintf("%d)", base+1000)
	return sql
}

// TestIndex2_1_1 translates index2-1.1:
// Create a table with 1000 columns.
// Expected: success
func TestIndex2_1_1(t *testing.T) {
	db := openTestDB(t)

	err := db.Exec(createTable1000Cols())
	if err != nil {
		t.Fatalf("CREATE TABLE with 1000 columns: %v", err)
	}
}

// TestIndex2_1_2 translates index2-1.2:
// Insert a row with 1000 values (1..1000).
// Expected: success
func TestIndex2_1_2(t *testing.T) {
	db := openTestDB(t)
	execSQLArgs(t, db, createTable1000Cols())

	err := db.Exec(insertRow1000Vals(0))
	if err != nil {
		t.Fatalf("INSERT with 1000 values: %v", err)
	}
}

// TestIndex2_1_3 translates index2-1.3:
// Query a specific column from a 1000-column table.
// Expected: c123 → 123
func TestIndex2_1_3(t *testing.T) {
	db := openTestDB(t)
	execSQLArgs(t, db, createTable1000Cols())
	execSQLArgs(t, db, insertRow1000Vals(0))

	got := queryText(t, db, "SELECT c123 FROM t1")
	if got != "123" {
		t.Errorf("SELECT c123: got %q, want '123'", got)
	}
}

// TestIndex2_1_4 translates index2-1.4:
// Insert 100 more rows in a transaction, then count.
// Expected: count(*) → 101
func TestIndex2_1_4(t *testing.T) {
	t.Skip("count() and large transactions not yet verified")

	db := openTestDB(t)
	execSQL(t, db, createTable1000Cols())
	execSQL(t, db, insertRow1000Vals(0))

	execSQL(t, db, "BEGIN")
	for j := 1; j <= 100; j++ {
		base := j * 10000
		sql := "INSERT INTO t1 VALUES("
		for i := 1; i < 1000; i++ {
			sql += fmt.Sprintf("%d,", base+i)
		}
		sql += fmt.Sprintf("%d)", base+1000)
		execSQL(t, db, sql)
	}
	execSQL(t, db, "COMMIT")

	got := queryInt(t, db, "SELECT count(*) FROM t1")
	if got != 101 {
		t.Errorf("count(*) = %d, want 101", got)
	}
}

// TestIndex2_1_5 translates index2-1.5:
// Sum a specific column.
// Expected: round(sum(c1000)) → 50601000.0
func TestIndex2_1_5(t *testing.T) {
	t.Skip("sum() aggregate not yet verified")

	db := openTestDB(t)
	execSQL(t, db, createTable1000Cols())
	execSQL(t, db, insertRow1000Vals(0))

	execSQL(t, db, "BEGIN")
	for j := 1; j <= 100; j++ {
		base := j * 10000
		sql := "INSERT INTO t1 VALUES("
		for i := 1; i < 1000; i++ {
			sql += fmt.Sprintf("%d,", base+i)
		}
		sql += fmt.Sprintf("%d)", base+1000)
		execSQL(t, db, sql)
	}
	execSQL(t, db, "COMMIT")

	got := queryFloat(t, db, "SELECT round(sum(c1000)) FROM t1")
	if got < 50600999 || got > 50601001 {
		t.Errorf("round(sum(c1000)) = %f, want 50601000.0", got)
	}
}

// TestIndex2_2_1 translates index2-2.1:
// Create an index with 1000 columns.
// Expected: success
func TestIndex2_2_1(t *testing.T) {
	t.Skip("CREATE INDEX not yet supported")

	db := openTestDB(t)
	execSQL(t, db, createTable1000Cols())

	idxSQL := "CREATE INDEX t1i1 ON t1("
	for i := 1; i < 1000; i++ {
		idxSQL += fmt.Sprintf("c%d,", i)
	}
	idxSQL += "c1000)"

	err := db.Exec(idxSQL)
	if err != nil {
		t.Fatalf("CREATE INDEX with 1000 columns: %v", err)
	}
}

// TestIndex2_2_2 translates index2-2.2:
// ORDER BY with 6 columns.
// Expected: 9 10009 20009 30009 40009
func TestIndex2_2_2(t *testing.T) {
	t.Skip("ORDER BY with 6 columns not yet supported")

	db := openTestDB(t)
	execSQL(t, db, createTable1000Cols())
	execSQL(t, db, insertRow1000Vals(0))

	execSQL(t, db, "BEGIN")
	for j := 1; j <= 100; j++ {
		base := j * 10000
		sql := "INSERT INTO t1 VALUES("
		for i := 1; i < 1000; i++ {
			sql += fmt.Sprintf("%d,", base+i)
		}
		sql += fmt.Sprintf("%d)", base+1000)
		execSQL(t, db, sql)
	}
	execSQL(t, db, "COMMIT")

	got := collectResults(t, db, "SELECT c9 FROM t1 ORDER BY c1, c2, c3, c4, c5, c6 LIMIT 5")
	want := []string{"9", "10009", "20009", "30009", "40009"}
	assertResults(t, got, want)
}

// Ensure unused imports are referenced
var _ = fmt.Sprintf
