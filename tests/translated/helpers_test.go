package tests

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

// mustExec executes SQL or fatals the test.
func mustExec(t *testing.T, db *sqlite.Database, sql string, args ...interface{}) {
	t.Helper()
	if err := db.Exec(sql, args...); err != nil {
		t.Fatalf("Exec(%q): %v", sql, err)
	}
}

// execOrFail executes SQL or fails.
func execOrFail(t *testing.T, db *sqlite.Database, sql string, args ...interface{}) {
	t.Helper()
	if err := db.Exec(sql, args...); err != nil {
		t.Fatalf("Exec(%q): %v", sql, err)
	}
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

// queryString executes a query and returns the first column as string.
func queryString(t *testing.T, db *sqlite.Database, sql string) string {
	t.Helper()
	return queryText(t, db, sql)
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

// queryInts executes a query and returns all first-column values as int64s.
func queryInts(t *testing.T, db *sqlite.Database, sql string, args ...interface{}) []int64 {
	t.Helper()
	rs, err := db.Query(sql, args...)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rs.Close()
	var result []int64
	for rs.Next() {
		result = append(result, rs.Row().ColumnInt(0))
	}
	return result
}

// queryStrings executes a query and returns all first-column values as strings.
func queryStrings(t *testing.T, db *sqlite.Database, sql string, args ...interface{}) []string {
	t.Helper()
	rs, err := db.Query(sql, args...)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rs.Close()
	var result []string
	for rs.Next() {
		result = append(result, rs.Row().ColumnText(0))
	}
	return result
}

// queryFlatStrings executes a query and returns all values flattened into a single string slice.
func queryFlatStrings(t *testing.T, db *sqlite.Database, sql string, args ...interface{}) []string {
	t.Helper()
	rs, err := db.Query(sql, args...)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rs.Close()
	var result []string
	for rs.Next() {
		row := rs.Row()
		for i := 0; i < row.ColumnCount(); i++ {
			result = append(result, row.ColumnText(i))
		}
	}
	return result
}

// queryFlatInts executes a query and returns all values flattened into a single int64 slice.
func queryFlatInts(t *testing.T, db *sqlite.Database, sql string, args ...interface{}) []int64 {
	t.Helper()
	rs, err := db.Query(sql, args...)
	if err != nil {
		t.Fatalf("Query(%q): %v", sql, err)
	}
	defer rs.Close()
	var result []int64
	for rs.Next() {
		row := rs.Row()
		for i := 0; i < row.ColumnCount(); i++ {
			result = append(result, row.ColumnInt(i))
		}
	}
	return result
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

// execSQL executes multiple SQL statements (ignoring errors for setup).
func execSQL(t *testing.T, db *sqlite.Database, sqls ...string) {
	t.Helper()
	for _, sql := range sqls {
		db.Exec(sql) // ignore errors for setup flexibility
	}
}

// catchSQL executes SQL and returns (errorOccurred, errorMessage).
func catchSQL(t *testing.T, db *sqlite.Database, sql string, args ...interface{}) (bool, string) {
	t.Helper()
	err := db.Exec(sql, args...)
	if err != nil {
		return true, err.Error()
	}
	return false, ""
}

// catchQuery executes a query and returns (errorOccurred, errorMessage).
func catchQuery(t *testing.T, db *sqlite.Database, sql string, args ...interface{}) (bool, string) {
	t.Helper()
	rs, err := db.Query(sql, args...)
	if err != nil {
		return true, err.Error()
	}
	rs.Close()
	return false, ""
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
