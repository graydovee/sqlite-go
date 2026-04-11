package tests

import (
	"testing"
)

func TestCTESimple(t *testing.T) {
	db := openTestDB(t)

	rs, err := db.Query("WITH c AS (SELECT 1) SELECT * FROM c")
	if err != nil {
		t.Fatalf("CTE query failed: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	colNames := rs.ColumnNames()
	if len(colNames) != 1 {
		t.Fatalf("expected 1 column, got %d", len(colNames))
	}

	val := rows[0][colNames[0]]
	if val != int64(1) {
		t.Errorf("expected 1, got %v", val)
	}
}

func TestCTEChained(t *testing.T) {
	db := openTestDB(t)

	rs, err := db.Query("WITH a(x) AS (SELECT 2), b(y) AS (SELECT x+1 FROM a) SELECT * FROM b")
	if err != nil {
		t.Fatalf("CTE chained query failed: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	colNames := rs.ColumnNames()
	if len(colNames) != 1 {
		t.Fatalf("expected 1 column, got %d", len(colNames))
	}

	val := rows[0][colNames[0]]
	if val != int64(3) {
		t.Errorf("expected 3 (2+1), got %v", val)
	}
}

func TestCTERecursive(t *testing.T) {
	db := openTestDB(t)

	rs, err := db.Query("WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cnt WHERE x<5) SELECT * FROM cnt")
	if err != nil {
		t.Fatalf("CTE recursive query failed: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}

	colNames := rs.ColumnNames()
	expected := []int64{1, 2, 3, 4, 5}
	for i, exp := range expected {
		val := rows[i][colNames[0]]
		if val != exp {
			t.Errorf("row %d: expected %d, got %v", i, exp, val)
		}
	}
}

// --- Additional non-recursive CTE tests ---

func TestCTEMultipleColumns(t *testing.T) {
	db := openTestDB(t)

	rs, err := db.Query("WITH c AS (SELECT 10 AS a, 20 AS b) SELECT * FROM c")
	if err != nil {
		t.Fatalf("CTE query failed: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	colNames := rs.ColumnNames()
	if len(colNames) != 2 {
		t.Fatalf("expected 2 columns, got %d: %v", len(colNames), colNames)
	}

	if rows[0][colNames[0]] != int64(10) {
		t.Errorf("expected col 0 = 10, got %v", rows[0][colNames[0]])
	}
	if rows[0][colNames[1]] != int64(20) {
		t.Errorf("expected col 1 = 20, got %v", rows[0][colNames[1]])
	}
}

func TestCTEWithTableData(t *testing.T) {
	db := openTestDB(t)

	if err := db.Exec("CREATE TABLE items(id INTEGER PRIMARY KEY, val TEXT)"); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if err := db.Exec("INSERT INTO items VALUES (1, 'hello')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := db.Exec("INSERT INTO items VALUES (2, 'world')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := db.Exec("INSERT INTO items VALUES (3, 'test')"); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rs, err := db.Query("WITH c AS (SELECT * FROM items WHERE id > 1) SELECT * FROM c")
	if err != nil {
		t.Fatalf("CTE query failed: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestCTEWithColumnAlias(t *testing.T) {
	db := openTestDB(t)

	rs, err := db.Query("WITH c(x) AS (SELECT 42) SELECT x FROM c")
	if err != nil {
		t.Fatalf("CTE query failed: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	colNames := rs.ColumnNames()
	if len(colNames) != 1 {
		t.Fatalf("expected 1 column, got %d", len(colNames))
	}
	if colNames[0] != "x" {
		t.Errorf("expected column name 'x', got %q", colNames[0])
	}
	if rows[0]["x"] != int64(42) {
		t.Errorf("expected 42, got %v", rows[0]["x"])
	}
}

func TestCTESelectSubset(t *testing.T) {
	db := openTestDB(t)

	rs, err := db.Query("WITH c AS (SELECT 1 AS a, 2 AS b, 3 AS c) SELECT b FROM c")
	if err != nil {
		t.Fatalf("CTE query failed: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	colNames := rs.ColumnNames()
	if len(colNames) != 1 {
		t.Fatalf("expected 1 column, got %d: %v", len(colNames), colNames)
	}
	if rows[0][colNames[0]] != int64(2) {
		t.Errorf("expected 2, got %v", rows[0][colNames[0]])
	}
}

func TestCTEMultipleDefs(t *testing.T) {
	db := openTestDB(t)

	rs, err := db.Query("WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a")
	if err != nil {
		t.Fatalf("CTE query failed: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	colNames := rs.ColumnNames()
	if rows[0][colNames[0]] != int64(1) {
		t.Errorf("expected 1, got %v", rows[0][colNames[0]])
	}
}

func TestCTEWithExpression(t *testing.T) {
	db := openTestDB(t)

	rs, err := db.Query("WITH c AS (SELECT 3 * 7 AS result) SELECT result FROM c")
	if err != nil {
		t.Fatalf("CTE query failed: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	colNames := rs.ColumnNames()
	if rows[0][colNames[0]] != int64(21) {
		t.Errorf("expected 21, got %v", rows[0][colNames[0]])
	}
}
