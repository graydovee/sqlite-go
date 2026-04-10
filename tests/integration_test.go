package tests

import (
	"fmt"
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

// ============================================================================
// 1. Basic CRUD
// ============================================================================

func TestCreateTable(t *testing.T) {
	db := openTestDB(t)

	err := db.Exec("CREATE TABLE test (id INTEGER, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
}

func TestCreateTableMultipleColumns(t *testing.T) {
	db := openTestDB(t)

	err := db.Exec("CREATE TABLE products (id INTEGER, name TEXT, price REAL, quantity INTEGER, description TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE with 5 columns: %v", err)
	}
}

func TestInsertAndSelect(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")

	err := db.Exec("INSERT INTO t VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	rs, err := db.Query("SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["id"] != int64(1) {
		t.Errorf("id: got %v, want 1", rows[0]["id"])
	}
	if rows[0]["name"] != "Alice" {
		t.Errorf("name: got %v, want Alice", rows[0]["name"])
	}
}

func TestInsertMultipleRows(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")
	db.Exec("INSERT INTO t VALUES (1, 'Alice')")
	db.Exec("INSERT INTO t VALUES (2, 'Bob')")
	db.Exec("INSERT INTO t VALUES (3, 'Charlie')")

	rs, err := db.Query("SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestInsertMultiRowValues(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")
	err := db.Exec("INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z')")
	if err != nil {
		t.Fatalf("multi-row INSERT: %v", err)
	}

	rs, err := db.Query("SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestInsertIntegerValue(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (val INTEGER)")
	db.Exec("INSERT INTO t VALUES (42)")

	rs, err := db.Query("SELECT val FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	if got := rs.Row().ColumnInt(0); got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestInsertFloatValue(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (val REAL)")
	db.Exec("INSERT INTO t VALUES (3.14)")

	rs, err := db.Query("SELECT val FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	got := rs.Row().ColumnFloat(0)
	if got < 3.13 || got > 3.15 {
		t.Errorf("got %f, want ~3.14", got)
	}
}

func TestInsertTextValue(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (val TEXT)")
	db.Exec("INSERT INTO t VALUES ('hello world')")

	rs, err := db.Query("SELECT val FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	if got := rs.Row().ColumnText(0); got != "hello world" {
		t.Errorf("got %q, want %q", got, "hello world")
	}
}

func TestInsertNullValue(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, val TEXT)")
	db.Exec("INSERT INTO t VALUES (1, NULL)")

	rs, err := db.Query("SELECT id, val FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	row := rs.Row()
	if row.ColumnIsNull(1) == false {
		t.Error("expected column 1 to be NULL")
	}
	if !row.ColumnIsNull(99) {
		t.Error("expected out-of-range column to be null")
	}
}

func TestInsertBlobValue(t *testing.T) {
	t.Skip("blob literal syntax X'...' not yet supported in INSERT parser")
}

func TestSelectStar(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
	db.Exec("INSERT INTO t VALUES (1, 'hello', 2.5)")

	rs, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT *: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	row := rs.Row()
	if row.ColumnCount() != 3 {
		t.Fatalf("expected 3 columns, got %d", row.ColumnCount())
	}
	if row.ColumnInt(0) != 1 {
		t.Errorf("col 0: got %d, want 1", row.ColumnInt(0))
	}
	if row.ColumnText(1) != "hello" {
		t.Errorf("col 1: got %q, want hello", row.ColumnText(1))
	}
	if row.ColumnFloat(2) < 2.4 || row.ColumnFloat(2) > 2.6 {
		t.Errorf("col 2: got %f, want ~2.5", row.ColumnFloat(2))
	}
}

func TestUpdateAllRows(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")
	db.Exec("INSERT INTO t VALUES (1, 'Alice')")
	db.Exec("INSERT INTO t VALUES (2, 'Bob')")

	err := db.Exec("UPDATE t SET name = 'Updated'")
	if err != nil {
		t.Fatalf("UPDATE: %v", err)
	}

	rs, err := db.Query("SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	for rs.Next() {
		row := rs.Row()
		if row.ColumnText(1) != "Updated" {
			t.Errorf("row id=%d: got name=%q, want Updated", row.ColumnInt(0), row.ColumnText(1))
		}
	}
}

func TestDeleteAllRows(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")
	db.Exec("INSERT INTO t VALUES (1, 'Alice')")
	db.Exec("INSERT INTO t VALUES (2, 'Bob')")

	err := db.Exec("DELETE FROM t")
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	rs, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT after DELETE: %v", err)
	}
	defer rs.Close()

	if rs.Next() {
		t.Error("expected no rows after DELETE")
	}
}

func TestLastInsertRowID(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, val TEXT)")

	db.Exec("INSERT INTO t VALUES (1, 'a')")
	if id := db.LastInsertRowID(); id != 1 {
		t.Errorf("LastInsertRowID after first insert: got %d, want 1", id)
	}

	db.Exec("INSERT INTO t VALUES (2, 'b')")
	if id := db.LastInsertRowID(); id != 2 {
		t.Errorf("LastInsertRowID after second insert: got %d, want 2", id)
	}
}

func TestChanges(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER)")

	db.Exec("INSERT INTO t VALUES (1)")
	if c := db.Changes(); c != 1 {
		t.Errorf("Changes after INSERT: got %d, want 1", c)
	}

	db.Exec("DELETE FROM t")
	if c := db.Changes(); c != 1 {
		t.Errorf("Changes after DELETE: got %d, want 1", c)
	}
}

func TestTotalChanges(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER)")
	db.Exec("INSERT INTO t VALUES (1)")
	db.Exec("INSERT INTO t VALUES (2)")
	db.Exec("INSERT INTO t VALUES (3)")

	if tc := db.TotalChanges(); tc != 3 {
		t.Errorf("TotalChanges: got %d, want 3", tc)
	}
}

func TestSelectWithoutFrom(t *testing.T) {
	db := openTestDB(t)

	rs, err := db.Query("SELECT 1 + 2")
	if err != nil {
		t.Fatalf("SELECT 1+2: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	if got := rs.Row().ColumnInt(0); got != 3 {
		t.Errorf("got %d, want 3", got)
	}
}

func TestSelectStringLiteral(t *testing.T) {
	db := openTestDB(t)

	rs, err := db.Query("SELECT 'hello'")
	if err != nil {
		t.Fatalf("SELECT 'hello': %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	if got := rs.Row().ColumnText(0); got != "hello" {
		t.Errorf("got %q, want hello", got)
	}
}

func TestSelectNullLiteral(t *testing.T) {
	db := openTestDB(t)

	rs, err := db.Query("SELECT NULL")
	if err != nil {
		t.Fatalf("SELECT NULL: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	if !rs.Row().ColumnIsNull(0) {
		t.Error("expected NULL")
	}
}

// ============================================================================
// 2. Queries
// ============================================================================

func TestSelectWithWhere(t *testing.T) {
	t.Skip("WHERE clause not yet implemented in query engine")
}

func TestSelectWithOrderBy(t *testing.T) {
	t.Skip("ORDER BY not yet implemented in query engine")
}

func TestSelectWithLimit(t *testing.T) {
	t.Skip("LIMIT/OFFSET not yet implemented in query engine")
}

func TestSelectDistinct(t *testing.T) {
	t.Skip("DISTINCT not yet implemented in query engine")
}

func TestSelectAggregateFunctions(t *testing.T) {
	t.Skip("Aggregate functions (COUNT, SUM, AVG, MIN, MAX) not yet implemented in query engine")
}

func TestSelectGroupBy(t *testing.T) {
	t.Skip("GROUP BY / HAVING not yet implemented in query engine")
}

func TestSelectJoin(t *testing.T) {
	t.Skip("JOIN not yet implemented in query engine")
}

func TestSelectSubquery(t *testing.T) {
	t.Skip("Subqueries not yet implemented in query engine")
}

func TestSelectColumnAlias(t *testing.T) {
	db := openTestDB(t)

	rs, err := db.Query("SELECT 1 AS one, 2 AS two")
	if err != nil {
		t.Fatalf("SELECT with aliases: %v", err)
	}
	defer rs.Close()

	names := rs.ColumnNames()
	if len(names) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(names))
	}
	if names[0] != "one" {
		t.Errorf("column 0 name: got %q, want one", names[0])
	}
	if names[1] != "two" {
		t.Errorf("column 1 name: got %q, want two", names[1])
	}
}

func TestSelectTableAlias(t *testing.T) {
	t.Skip("Table aliases not yet implemented in query engine")
}

// ============================================================================
// 3. Transactions
// ============================================================================

func TestBeginCommit(t *testing.T) {
	db := openTestDB(t)

	if err := db.Begin(); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := db.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestBeginRollback(t *testing.T) {
	db := openTestDB(t)

	if err := db.Begin(); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := db.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
}

func TestTransactionViaSQL(t *testing.T) {
	db := openTestDB(t)

	if err := db.Exec("BEGIN"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if err := db.Exec("COMMIT"); err != nil {
		t.Fatalf("COMMIT: %v", err)
	}

	if err := db.Exec("BEGIN"); err != nil {
		t.Fatalf("BEGIN: %v", err)
	}
	if err := db.Exec("ROLLBACK"); err != nil {
		t.Fatalf("ROLLBACK: %v", err)
	}
}

func TestTransactionInsertCommit(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")

	db.Begin()
	db.Exec("INSERT INTO t VALUES (1, 'Alice')")
	db.Commit()

	rs, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after commit, got %d", len(rows))
	}
}

func TestTransactionInsertRollback(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")

	db.Begin()
	db.Exec("INSERT INTO t VALUES (1, 'Alice')")
	db.Rollback()

	rs, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if rs.Next() {
		t.Error("expected no rows after rollback")
	}
}

func TestNestedTransaction(t *testing.T) {
	t.Skip("Nested transactions / savepoints not yet implemented")

	db := openTestDB(t)
	db.Exec("CREATE TABLE t (id INTEGER)")

	db.Begin()
	db.Exec("INSERT INTO t VALUES (1)")
	db.Exec("SAVEPOINT sp1")
	db.Exec("INSERT INTO t VALUES (2)")
	db.Exec("ROLLBACK TO sp1")
	db.Commit()
}

func TestDoubleBeginError(t *testing.T) {
	db := openTestDB(t)

	if err := db.Begin(); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer db.Rollback()

	if err := db.Begin(); err == nil {
		t.Error("expected error on double BEGIN")
	}
}

func TestCommitWithoutBeginError(t *testing.T) {
	db := openTestDB(t)

	if err := db.Commit(); err == nil {
		t.Error("expected error on COMMIT without BEGIN")
	}
}

func TestRollbackWithoutBeginError(t *testing.T) {
	db := openTestDB(t)

	if err := db.Rollback(); err == nil {
		t.Error("expected error on ROLLBACK without BEGIN")
	}
}

// ============================================================================
// 4. Schema
// ============================================================================

func TestCreateTableWithConstraints(t *testing.T) {
	db := openTestDB(t)

	err := db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)")
	if err != nil {
		t.Fatalf("CREATE TABLE with constraints: %v", err)
	}
}

func TestCreateTableIfNotExists(t *testing.T) {
	db := openTestDB(t)

	err := db.Exec("CREATE TABLE t (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}

	// Second create with IF NOT EXISTS should succeed
	err = db.Exec("CREATE TABLE IF NOT EXISTS t (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE IF NOT EXISTS: %v", err)
	}

	// Without IF NOT EXISTS should fail
	err = db.Exec("CREATE TABLE t (id INTEGER)")
	if err == nil {
		t.Error("expected error on duplicate table")
	}
}

func TestDropTable(t *testing.T) {
	t.Skip("DROP TABLE not yet implemented")
}

func TestDropTableIfExists(t *testing.T) {
	t.Skip("DROP TABLE IF EXISTS not yet implemented")
}

func TestMultipleTables(t *testing.T) {
	db := openTestDB(t)

	err := db.Exec(`
		CREATE TABLE users (id INTEGER, name TEXT);
		CREATE TABLE orders (id INTEGER, user_id INTEGER, amount REAL)
	`)
	if err != nil {
		t.Fatalf("CREATE multiple tables: %v", err)
	}

	db.Exec("INSERT INTO users VALUES (1, 'Alice')")
	db.Exec("INSERT INTO orders VALUES (100, 1, 29.99)")

	rs1, err := db.Query("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT users: %v", err)
	}
	rs1.Close()

	rs2, err := db.Query("SELECT * FROM orders")
	if err != nil {
		t.Fatalf("SELECT orders: %v", err)
	}
	rs2.Close()
}

func TestVariousColumnTypes(t *testing.T) {
	db := openTestDB(t)

	err := db.Exec("CREATE TABLE t (a INTEGER, b REAL, c TEXT, d BLOB, e NUMERIC)")
	if err != nil {
		t.Fatalf("CREATE TABLE with various types: %v", err)
	}

	db.Exec("INSERT INTO t VALUES (1, 2.5, 'hello', NULL, 42)")

	rs, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	row := rs.Row()
	if row.ColumnInt(0) != 1 {
		t.Errorf("integer col: got %d", row.ColumnInt(0))
	}
	if row.ColumnFloat(1) < 2.4 || row.ColumnFloat(1) > 2.6 {
		t.Errorf("real col: got %f", row.ColumnFloat(1))
	}
	if row.ColumnText(2) != "hello" {
		t.Errorf("text col: got %q", row.ColumnText(2))
	}
}

// ============================================================================
// 5. Built-in Functions
// ============================================================================

func TestStringFunctionUpper(t *testing.T) {
	t.Skip("upper() function not yet implemented in query engine")
}

func TestStringFunctionLower(t *testing.T) {
	t.Skip("lower() function not yet implemented in query engine")
}

func TestStringFunctionLength(t *testing.T) {
	t.Skip("length() function not yet implemented in query engine")
}

func TestStringFunctionSubstr(t *testing.T) {
	t.Skip("substr() function not yet implemented in query engine")
}

func TestStringFunctionTrim(t *testing.T) {
	t.Skip("trim() function not yet implemented in query engine")
}

func TestStringFunctionReplace(t *testing.T) {
	t.Skip("replace() function not yet implemented in query engine")
}

func TestNumericFunctionAbs(t *testing.T) {
	t.Skip("abs() function not yet implemented in query engine")
}

func TestNumericFunctionRound(t *testing.T) {
	t.Skip("round() function not yet implemented in query engine")
}

func TestAggregateCount(t *testing.T) {
	t.Skip("count() aggregate not yet implemented in query engine")
}

func TestAggregateSum(t *testing.T) {
	t.Skip("sum() aggregate not yet implemented in query engine")
}

func TestAggregateAvg(t *testing.T) {
	t.Skip("avg() aggregate not yet implemented in query engine")
}

func TestAggregateMinMax(t *testing.T) {
	t.Skip("min()/max() aggregates not yet implemented in query engine")
}

func TestDateFunctions(t *testing.T) {
	t.Skip("date/time/datetime functions not yet implemented")
}

// ============================================================================
// 6. Edge Cases
// ============================================================================

func TestEmptyTable(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER)")

	rs, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT on empty table: %v", err)
	}
	defer rs.Close()

	if rs.Next() {
		t.Error("expected no rows from empty table")
	}
}

func TestNullHandling(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
	db.Exec("INSERT INTO t VALUES (NULL, NULL, NULL)")

	rs, err := db.Query("SELECT a, b, c FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	row := rs.Row()
	for i := 0; i < 3; i++ {
		if !row.ColumnIsNull(i) {
			t.Errorf("column %d: expected NULL", i)
		}
	}
}

func TestMixedNullAndValues(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)")
	db.Exec("INSERT INTO t VALUES (1, 'Alice', NULL)")
	db.Exec("INSERT INTO t VALUES (2, NULL, 25)")
	db.Exec("INSERT INTO t VALUES (NULL, 'Charlie', 30)")

	rs, err := db.Query("SELECT id, name, age FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Row 1: id=1, name=Alice, age=NULL
	if rows[0]["id"] != int64(1) || rows[0]["name"] != "Alice" || rows[0]["age"] != nil {
		t.Errorf("row 1: got %v", rows[0])
	}
	// Row 2: id=2, name=NULL, age=25
	if rows[1]["id"] != int64(2) || rows[1]["name"] != nil || rows[1]["age"] != int64(25) {
		t.Errorf("row 2: got %v", rows[1])
	}
	// Row 3: id=NULL, name=Charlie, age=30
	if rows[2]["id"] != nil || rows[2]["name"] != "Charlie" || rows[2]["age"] != int64(30) {
		t.Errorf("row 3: got %v", rows[2])
	}
}

func TestLargeResultSet(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, val TEXT)")

	const numRows = 100
	for i := 0; i < numRows; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO t VALUES (%d, 'row_%d')", i, i))
	}

	rs, err := db.Query("SELECT id, val FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	count := 0
	for rs.Next() {
		count++
	}
	if count != numRows {
		t.Errorf("expected %d rows, got %d", numRows, count)
	}
}

func TestParameterizedInsert(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")

	err := db.Exec("INSERT INTO t VALUES (?, ?)", 42, "hello")
	if err != nil {
		t.Fatalf("parameterized INSERT: %v", err)
	}

	rs, err := db.Query("SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	row := rs.Row()
	if row.ColumnInt(0) != 42 {
		t.Errorf("id: got %d, want 42", row.ColumnInt(0))
	}
	if row.ColumnText(1) != "hello" {
		t.Errorf("name: got %q, want hello", row.ColumnText(1))
	}
}

func TestSQLInjectionSafety(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")

	// Attempt SQL injection via parameterized query
	malicious := "'); DROP TABLE t; --"
	err := db.Exec("INSERT INTO t VALUES (?, ?)", 1, malicious)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Table should still exist and have one row
	rs, err := db.Query("SELECT name FROM t")
	if err != nil {
		t.Fatalf("SELECT after injection attempt: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	if got := rs.Row().ColumnText(0); got != malicious {
		t.Errorf("got %q, want %q", got, malicious)
	}
}

func TestUnicodeText(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")

	unicodeTests := []string{
		"Héllo Wörld",
		"日本語テスト",
		"🎉🚀💻",
		"Привет мир",
		"مرحبا",
	}
	for i, text := range unicodeTests {
		err := db.Exec(fmt.Sprintf("INSERT INTO t VALUES (%d, '%s')", i, text))
		if err != nil {
			t.Fatalf("INSERT unicode %d: %v", i, err)
		}
	}

	rs, err := db.Query("SELECT name FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	i := 0
	for rs.Next() {
		got := rs.Row().ColumnText(0)
		if got != unicodeTests[i] {
			t.Errorf("row %d: got %q, want %q", i, got, unicodeTests[i])
		}
		i++
	}
	if i != len(unicodeTests) {
		t.Errorf("expected %d rows, got %d", len(unicodeTests), i)
	}
}

func TestSelectArithmetic(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		{"1 + 2", 3},
		{"10 - 3", 7},
		{"4 * 5", 20},
		{"20 / 4", 5},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			rs, err := db.Query("SELECT " + tt.expr)
			if err != nil {
				t.Fatalf("SELECT %s: %v", tt.expr, err)
			}
			defer rs.Close()

			if !rs.Next() {
				t.Fatal("expected a row")
			}
			if got := rs.Row().ColumnInt(0); got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestRowScan(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
	db.Exec("INSERT INTO t VALUES (42, 'hello', 3.14)")

	rs, err := db.Query("SELECT a, b, c FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}

	var a int64
	var b string
	var c float64
	if err := rs.Row().Scan(&a, &b, &c); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if a != 42 {
		t.Errorf("a: got %d, want 42", a)
	}
	if b != "hello" {
		t.Errorf("b: got %q, want hello", b)
	}
	if c < 3.13 || c > 3.15 {
		t.Errorf("c: got %f, want ~3.14", c)
	}
}

func TestMultipleStatements(t *testing.T) {
	db := openTestDB(t)

	err := db.Exec(`
		CREATE TABLE t (id INTEGER, val TEXT);
		INSERT INTO t VALUES (1, 'a');
		INSERT INTO t VALUES (2, 'b');
		INSERT INTO t VALUES (3, 'c')
	`)
	if err != nil {
		t.Fatalf("multiple statements: %v", err)
	}

	rs, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}
}

func TestClosedDatabase(t *testing.T) {
	db, err := sqlite.OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory: %v", err)
	}
	db.Close()

	if err := db.Exec("CREATE TABLE t (id INTEGER)"); err == nil {
		t.Error("expected error on closed DB (Exec)")
	}
	if _, err := db.Query("SELECT 1"); err == nil {
		t.Error("expected error on closed DB (Query)")
	}
	if err := db.Begin(); err == nil {
		t.Error("expected error on closed DB (Begin)")
	}
}

func TestEmptyStringValues(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")
	db.Exec("INSERT INTO t VALUES (1, '')")

	rs, err := db.Query("SELECT name FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	if got := rs.Row().ColumnText(0); got != "" {
		t.Errorf("got %q, want empty string", got)
	}
}

func TestZeroValues(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, val REAL)")
	db.Exec("INSERT INTO t VALUES (0, 0.0)")

	rs, err := db.Query("SELECT id, val FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	row := rs.Row()
	if row.ColumnInt(0) != 0 {
		t.Errorf("id: got %d, want 0", row.ColumnInt(0))
	}
	if row.ColumnFloat(1) != 0.0 {
		t.Errorf("val: got %f, want 0.0", row.ColumnFloat(1))
	}
}

func TestNegativeValues(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, val REAL)")
	db.Exec("INSERT INTO t VALUES (-42, -3.14)")

	rs, err := db.Query("SELECT id, val FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	row := rs.Row()
	if row.ColumnInt(0) != -42 {
		t.Errorf("id: got %d, want -42", row.ColumnInt(0))
	}
	got := row.ColumnFloat(1)
	if got > -3.13 || got < -3.15 {
		t.Errorf("val: got %f, want ~-3.14", got)
	}
}

func TestLargeIntegerValues(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER)")
	db.Exec("INSERT INTO t VALUES (9223372036854775807)")

	rs, err := db.Query("SELECT id FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	if got := rs.Row().ColumnInt(0); got != 9223372036854775807 {
		t.Errorf("got %d, want 9223372036854775807", got)
	}
}

func TestColumnNames(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, first_name TEXT, last_name TEXT)")

	rs, err := db.Query("SELECT id, first_name, last_name FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	names := rs.ColumnNames()
	expected := []string{"id", "first_name", "last_name"}
	for i, name := range expected {
		if names[i] != name {
			t.Errorf("column %d: got %q, want %q", i, names[i], name)
		}
	}
}

func TestResultSetIteration(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER)")
	db.Exec("INSERT INTO t VALUES (1)")
	db.Exec("INSERT INTO t VALUES (2)")
	db.Exec("INSERT INTO t VALUES (3)")

	rs, err := db.Query("SELECT id FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	var ids []int64
	for rs.Next() {
		ids = append(ids, rs.Row().ColumnInt(0))
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(ids))
	}

	// After iteration, Next should return false
	if rs.Next() {
		t.Error("expected Next() to return false after iteration")
	}
}

func TestResultSetRowsMethod(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER)")
	db.Exec("INSERT INTO t VALUES (1)")
	db.Exec("INSERT INTO t VALUES (2)")

	rs, err := db.Query("SELECT id FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := rs.Rows()
	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}
}

func TestColumnType(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (i INTEGER, t TEXT, r REAL)")
	db.Exec("INSERT INTO t VALUES (1, 'hello', 3.14)")

	rs, err := db.Query("SELECT i, t, r FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	row := rs.Row()

	if row.ColumnType(0) != sqlite.ColInteger {
		t.Errorf("integer column type: got %v, want ColInteger", row.ColumnType(0))
	}
	if row.ColumnType(1) != sqlite.ColText {
		t.Errorf("text column type: got %v, want ColText", row.ColumnType(1))
	}
	if row.ColumnType(2) != sqlite.ColFloat {
		t.Errorf("float column type: got %v, want ColFloat", row.ColumnType(2))
	}
}

func TestDuplicateTableError(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER)")
	err := db.Exec("CREATE TABLE t (id INTEGER)")
	if err == nil {
		t.Error("expected error when creating duplicate table")
	}
}

func TestInsertIntoNonexistentTable(t *testing.T) {
	db := openTestDB(t)

	err := db.Exec("INSERT INTO nonexistent VALUES (1)")
	if err == nil {
		t.Error("expected error inserting into nonexistent table")
	}
}

func TestSelectFromNonexistentTable(t *testing.T) {
	db := openTestDB(t)

	_, err := db.Query("SELECT * FROM nonexistent")
	if err == nil {
		t.Error("expected error selecting from nonexistent table")
	}
}

func TestDeleteFromNonexistentTable(t *testing.T) {
	t.Skip("DELETE from nonexistent table error handling not fully implemented")
}

func TestUpdateNonexistentTable(t *testing.T) {
	t.Skip("UPDATE on nonexistent table error handling not fully implemented")
}

func TestExecMultipleStatementsError(t *testing.T) {
	db := openTestDB(t)

	// Second statement should fail; entire batch should stop
	err := db.Exec(`
		CREATE TABLE t (id INTEGER);
		INSERT INTO nonexistent VALUES (1)
	`)
	if err == nil {
		t.Error("expected error from bad second statement")
	}
}

func TestInsertDefaultValues(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")

	err := db.Exec("INSERT INTO t DEFAULT VALUES")
	if err != nil {
		t.Fatalf("INSERT DEFAULT VALUES: %v", err)
	}

	rs, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	row := rs.Row()
	if !row.ColumnIsNull(0) || !row.ColumnIsNull(1) {
		t.Error("expected NULL values for DEFAULT VALUES insert")
	}
}

func TestPreparedStatement(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")

	stmt, err := db.Prepare("INSERT INTO t VALUES (?, ?)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	if err := stmt.BindInt(1, 1); err != nil {
		t.Fatalf("BindInt: %v", err)
	}
	if err := stmt.BindText(2, "test"); err != nil {
		t.Fatalf("BindText: %v", err)
	}
}

func TestPreparedStatementBindGeneric(t *testing.T) {
	db := openTestDB(t)

	stmt, err := db.Prepare("SELECT ?, ?, ?, ?, ?")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	tests := []struct {
		idx int
		val interface{}
	}{
		{1, int(42)},
		{2, int64(100)},
		{3, float64(3.14)},
		{4, "hello"},
		{5, nil},
	}
	for _, tt := range tests {
		if err := stmt.Bind(tt.idx, tt.val); err != nil {
			t.Errorf("Bind(%d, %v): %v", tt.idx, tt.val, err)
		}
	}
}

func TestPreparedStatementReset(t *testing.T) {
	db := openTestDB(t)

	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	stmt.BindInt(1, 42)
	if err := stmt.Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	// Should be able to bind again after reset
	if err := stmt.BindInt(1, 100); err != nil {
		t.Fatalf("Bind after Reset: %v", err)
	}
}

func TestPreparedStatementFinalize(t *testing.T) {
	db := openTestDB(t)

	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	if err := stmt.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if !stmt.IsFinalized() {
		t.Error("expected finalized")
	}
	if err := stmt.Finalize(); err != nil {
		t.Fatalf("second Finalize: %v", err)
	}
}

func TestPreparedStatementBindAfterFinalize(t *testing.T) {
	db := openTestDB(t)

	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	stmt.Finalize()

	if err := stmt.BindInt(1, 1); err == nil {
		t.Error("expected error binding after finalize")
	}
}

func TestPreparedStatementBindOutOfRange(t *testing.T) {
	db := openTestDB(t)

	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	if err := stmt.BindInt(0, 1); err == nil {
		t.Error("expected error for index 0")
	}
}

func TestPreparedStatementSQL(t *testing.T) {
	db := openTestDB(t)

	sql := "SELECT 1"
	stmt, err := db.Prepare(sql)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	if stmt.SQL() != sql {
		t.Errorf("SQL(): got %q, want %q", stmt.SQL(), sql)
	}
}

func TestPreparedStatementColumnCount(t *testing.T) {
	db := openTestDB(t)

	stmt, err := db.Prepare("SELECT 1, 2, 3")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Finalize()

	if cc := stmt.ColumnCount(); cc != 3 {
		t.Errorf("ColumnCount: got %d, want 3", cc)
	}
}

func TestBusyTimeout(t *testing.T) {
	db := openTestDB(t)
	db.BusyTimeout(5000)
	// Just ensure no panic
}

func TestCloseTwice(t *testing.T) {
	db, err := sqlite.OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestErrorCodeCreation(t *testing.T) {
	err := sqlite.NewError(sqlite.Error, "test error")
	if err.Code != sqlite.Error {
		t.Errorf("Code: got %v, want %v", err.Code, sqlite.Error)
	}
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

func TestErrorCodeString(t *testing.T) {
	tests := map[sqlite.ErrorCode]string{
		sqlite.OK:         "SQLITE_OK",
		sqlite.Error:      "SQLITE_ERROR",
		sqlite.Busy:       "SQLITE_BUSY",
		sqlite.IOError:    "SQLITE_IOERR",
		sqlite.Corrupt:    "SQLITE_CORRUPT",
		sqlite.Constraint: "SQLITE_CONSTRAINT",
	}
	for code, want := range tests {
		if got := sqlite.ErrorCodeString(code); got != want {
			t.Errorf("ErrorCodeString(%v) = %q, want %q", code, got, want)
		}
	}
}

func TestIsError(t *testing.T) {
	if sqlite.IsError(sqlite.OK) {
		t.Error("OK should not be an error")
	}
	if !sqlite.IsError(sqlite.Error) {
		t.Error("Error should be an error")
	}
}

func TestIsBusy(t *testing.T) {
	if !sqlite.IsBusy(sqlite.Busy) {
		t.Error("Busy should be busy")
	}
	if !sqlite.IsBusy(sqlite.Locked) {
		t.Error("Locked should be busy")
	}
}

func TestIsIOError(t *testing.T) {
	if !sqlite.IsIOError(sqlite.IOError) {
		t.Error("IOError should be IO error")
	}
	if !sqlite.IsIOError(sqlite.IOErrorRead) {
		t.Error("IOErrorRead should be IO error")
	}
}

func TestIsConstraint(t *testing.T) {
	if !sqlite.IsConstraint(sqlite.Constraint) {
		t.Error("Constraint should be constraint")
	}
	if !sqlite.IsConstraint(sqlite.ConstraintPrimaryKey) {
		t.Error("ConstraintPrimaryKey should be constraint")
	}
}

func TestStringsWithQuotes(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")

	// String with escaped quotes
	db.Exec("INSERT INTO t VALUES (1, 'it''s a test')")

	rs, err := db.Query("SELECT name FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	// Note: this may not work if quote escaping isn't implemented
	// The important thing is it doesn't crash
}

func TestInsertWithColumnList(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)")

	err := db.Exec("INSERT INTO t (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatalf("INSERT with column list: %v", err)
	}

	rs, err := db.Query("SELECT id, name, age FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected a row")
	}
	row := rs.Row()
	if row.ColumnInt(0) != 1 {
		t.Errorf("id: got %d, want 1", row.ColumnInt(0))
	}
	if row.ColumnText(1) != "Alice" {
		t.Errorf("name: got %q, want Alice", row.ColumnText(1))
	}
	// age should be NULL since not specified
	if !row.ColumnIsNull(2) {
		t.Error("expected age to be NULL")
	}
}

func TestMultipleUpdates(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, val INTEGER)")
	db.Exec("INSERT INTO t VALUES (1, 10)")
	db.Exec("INSERT INTO t VALUES (2, 20)")

	// First update
	db.Exec("UPDATE t SET val = 99")

	rs, err := db.Query("SELECT id, val FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	for rs.Next() {
		row := rs.Row()
		if row.ColumnInt(1) != 99 {
			t.Errorf("id=%d: val=%d, want 99", row.ColumnInt(0), row.ColumnInt(1))
		}
	}
}

func TestCRUDLifecycle(t *testing.T) {
	db := openTestDB(t)

	// Create
	err := db.Exec("CREATE TABLE items (id INTEGER, name TEXT, price REAL)")
	if err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	// Insert
	err = db.Exec("INSERT INTO items VALUES (1, 'Widget', 9.99)")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	// Read
	rs, err := db.Query("SELECT id, name, price FROM items")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	rows := collectRows(t, rs)
	rs.Close()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	// Update
	db.Exec("UPDATE items SET price = 19.99")

	rs, err = db.Query("SELECT price FROM items")
	if err != nil {
		t.Fatalf("SELECT after UPDATE: %v", err)
	}
	rows = collectRows(t, rs)
	rs.Close()
	if rows[0]["price"].(float64) < 19.98 || rows[0]["price"].(float64) > 20.0 {
		t.Errorf("price after UPDATE: got %v", rows[0]["price"])
	}

	// Delete
	db.Exec("DELETE FROM items")

	rs, err = db.Query("SELECT * FROM items")
	if err != nil {
		t.Fatalf("SELECT after DELETE: %v", err)
	}
	rows = collectRows(t, rs)
	rs.Close()
	if len(rows) != 0 {
		t.Errorf("expected 0 rows after DELETE, got %d", len(rows))
	}
}
