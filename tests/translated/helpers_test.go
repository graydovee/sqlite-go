package tests

// Shared test helpers for translated test files.
// These are duplicated from the parent tests/ package because Go compiles
// each directory as a separate package even when the package name matches.

import (
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// openTestDB opens an in-memory database for testing.
func openTestDB(t *testing.T) *sqlite.Database {
	t.Helper()
	db, err := sqlite.OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// queryInt executes a query and returns the first column as int64.
func queryInt(t *testing.T, db *sqlite.Database, sql string, args ...interface{}) int64 {
	t.Helper()
	rs, err := db.Query(sql, args...)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatalf("Query(%q): no rows", sql)
	}
	return rs.Row().ColumnInt(0)
}

// queryText executes a query and returns the first column as string.
func queryText(t *testing.T, db *sqlite.Database, sql string, args ...interface{}) string {
	t.Helper()
	rs, err := db.Query(sql, args...)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatalf("Query(%q): no rows", sql)
	}
	return rs.Row().ColumnText(0)
}

// queryFloat executes a query and returns the first column as float64.
func queryFloat(t *testing.T, db *sqlite.Database, sql string) float64 {
	t.Helper()
	rs, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatalf("Query(%q): no rows", sql)
	}
	return rs.Row().ColumnFloat(0)
}

// queryString executes a query and returns the first column as string.
func queryString(t *testing.T, db *sqlite.Database, sql string) string {
	t.Helper()
	rs, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatalf("Query(%q): no rows", sql)
	}
	return rs.Row().ColumnText(0)
}

// queryNull checks if the first column of a query is NULL.
func queryNull(t *testing.T, db *sqlite.Database, sql string) bool {
	t.Helper()
	rs, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatalf("Query(%q): no rows", sql)
	}
	return rs.Row().ColumnIsNull(0)
}

// queryRowCount executes a query and returns the number of rows.
func queryRowCount(t *testing.T, db *sqlite.Database, sql string) int {
	t.Helper()
	rs, err := db.Query(sql)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rs.Close()
	count := 0
	for rs.Next() {
		count++
	}
	return count
}

// execOrFail executes SQL or fails.
func execOrFail(t *testing.T, db *sqlite.Database, sql string, args ...interface{}) {
	t.Helper()
	if err := db.Exec(sql, args...); err != nil {
		t.Fatalf("Exec(%q): %v", sql, err)
	}
}

// collectRows collects all rows from a result set into a slice of maps.
func collectRows(t *testing.T, rs *sqlite.ResultSet) []map[string]interface{} {
	t.Helper()
	var rows []map[string]interface{}
	names := rs.ColumnNames()
	for rs.Next() {
		row := rs.Row()
		m := make(map[string]interface{})
		for i, name := range names {
			m[name] = row.ColumnValue(i)
		}
		rows = append(rows, m)
	}
	return rows
}
