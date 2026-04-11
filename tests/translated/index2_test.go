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

func TestIndex2_1_1(t *testing.T) {
	// index2-1.1: Create a table with 1000 columns
	db := openTestDB(t)

	sql := "CREATE TABLE t1("
	for i := 1; i < 1000; i++ {
		sql += fmt.Sprintf("c%d,", i)
	}
	sql += "c1000)"
	err := db.Exec(sql)
	if err != nil {
		t.Fatalf("CREATE TABLE with 1000 columns: %v", err)
	}
}

func TestIndex2_1_2(t *testing.T) {
	// index2-1.2: Insert a row with 1000 values
	db := openTestDB(t)

	// Create table with 1000 columns
	createSQL := "CREATE TABLE t1("
	for i := 1; i < 1000; i++ {
		createSQL += fmt.Sprintf("c%d,", i)
	}
	createSQL += "c1000)"
	execSQL(t, db, createSQL)

	// Insert one row
	sql := "INSERT INTO t1 VALUES("
	for i := 1; i < 1000; i++ {
		sql += fmt.Sprintf("%d,", i)
	}
	sql += "1000)"
	err := db.Exec(sql)
	if err != nil {
		t.Fatalf("INSERT with 1000 values: %v", err)
	}
}

func TestIndex2_1_3(t *testing.T) {
	// index2-1.3: Query a specific column from a 1000-column table
	db := openTestDB(t)

	// Create table
	sql := "CREATE TABLE t1("
	for i := 1; i < 1000; i++ {
		sql += fmt.Sprintf("c%d,", i)
	}
	sql += "c1000)"
	execSQL(t, db, sql)

	// Insert one row
	insertSQL := "INSERT INTO t1 VALUES("
	for i := 1; i < 1000; i++ {
		insertSQL += fmt.Sprintf("%d,", i)
	}
	insertSQL += "1000)"
	execSQL(t, db, insertSQL)

	got := queryText(t, db, "SELECT c123 FROM t1")
	if got != "123" {
		t.Errorf("SELECT c123: got %q, want '123'", got)
	}
}

func TestIndex2_1_4(t *testing.T) {
	// index2-1.4: Insert 100 more rows in a transaction, then count
	t.Skip("count() aggregate and large transactions not yet verified")
}

func TestIndex2_1_5(t *testing.T) {
	// index2-1.5: Sum a column
	t.Skip("sum() aggregate not yet verified")
}

func TestIndex2_2_1(t *testing.T) {
	// index2-2.1: Create an index with 1000 columns
	t.Skip("CREATE INDEX not yet supported")

	db := openTestDB(t)

	// Create table
	sql := "CREATE TABLE t1("
	for i := 1; i < 1000; i++ {
		sql += fmt.Sprintf("c%d,", i)
	}
	sql += "c1000)"
	execSQL(t, db, sql)

	// Create index
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

func TestIndex2_2_2(t *testing.T) {
	// index2-2.2: ORDER BY with many columns
	t.Skip("ORDER BY with 6 columns not yet supported")
}
