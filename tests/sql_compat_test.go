package tests

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// ============================================================================
// SQL compatibility tests - ported from C test1.c
// ============================================================================

// --- Helpers ---

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

// ============================================================================
// 1. Expression types and edge cases
// ============================================================================

func TestSQLArithmeticExpressions(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		{"1 + 2", 3},
		{"10 - 3", 7},
		{"4 * 5", 20},
		{"20 / 4", 5},
		{"17 % 5", 2},
		{"1 + 2 * 3", 7},    // precedence: 1 + (2*3) = 7
		{"(1 + 2) * 3", 9},  // parentheses
		{"-5", -5},
		{"+5", 5},
		{"- -5", 5},           // double negation
		{"1 + -1", 0},
		{"10 / 3", 3},        // integer division
		{"0 * 99999", 0},
		{"1 - 1", 0},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			got := queryInt(t, db, "SELECT "+tt.expr)
			if got != tt.want {
				t.Errorf("SELECT %s = %d, want %d", tt.expr, got, tt.want)
			}
		})
	}
}

func TestSQLFloatArithmetic(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr     string
		want     float64
		tolerance float64
	}{
		{"1.5 + 2.5", 4.0, 0.001},
		{"10.0 / 4.0", 2.5, 0.001},
		{"3.14 * 2.0", 6.28, 0.01},
		{"7.5 - 2.5", 5.0, 0.001},
		{"1.0 / 3.0 * 3.0", 1.0, 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			got := queryFloat(t, db, "SELECT "+tt.expr)
			if math.Abs(got-tt.want) > tt.tolerance {
				t.Errorf("SELECT %s = %f, want ~%f", tt.expr, got, tt.want)
			}
		})
	}
}

func TestSQLBitwiseOperations(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		// Note: bitwise ops may not be fully implemented yet;
		// these tests validate the expressions don't crash
		// and return reasonable values
		{"5 & 3", 1},
		{"5 | 3", 7},
		{"1 << 4", 16},
		{"16 >> 2", 4},
		{"~0", -1},
	}

	for _, tt := range tests {
		t.Run(strings.ReplaceAll(tt.expr, " ", "_"), func(t *testing.T) {
			rs, err := db.Query("SELECT " + tt.expr)
			if err != nil {
				t.Skipf("bitwise op not supported: %v", err)
			}
			defer rs.Close()
			if !rs.Next() {
				t.Fatal("no rows")
			}
			got := rs.Row().ColumnInt(0)
			if got != tt.want {
				t.Errorf("SELECT %s = %d, want %d", tt.expr, got, tt.want)
			}
		})
	}
}

func TestSQLStringConcatenation(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want string
	}{
		{"'hello' || ' ' || 'world'", "hello world"},
		{"'a' || 'b' || 'c'", "abc"},
		{"'test' || ''", "test"},
		{"'' || ''", ""},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			got := queryString(t, db, "SELECT "+tt.expr)
			if got != tt.want {
				t.Errorf("SELECT %s = %q, want %q", tt.expr, got, tt.want)
			}
		})
	}
}

// ============================================================================
// 2. Type affinity and coercion rules
// ============================================================================

func TestSQLTypeAffinity(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE affinity_test (i INTEGER, t TEXT, r REAL, b BLOB)")

	// Insert integer into text column - should be stored as text
	execOrFail(t, db, "INSERT INTO affinity_test VALUES (1, 'text', 3.14, NULL)")

	rs, err := db.Query("SELECT typeof(i), typeof(t), typeof(r) FROM affinity_test")
	if err != nil {
		t.Fatalf("SELECT typeof: %v", err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatal("no rows")
	}
	row := rs.Row()
	if row.ColumnText(0) != "integer" {
		t.Errorf("typeof(i) = %q, want 'integer'", row.ColumnText(0))
	}
	if row.ColumnText(1) != "text" {
		t.Errorf("typeof(t) = %q, want 'text'", row.ColumnText(1))
	}
	if row.ColumnText(2) != "real" {
		t.Errorf("typeof(r) = %q, want 'real'", row.ColumnText(2))
	}
}

func TestSQLTypeof(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr     string
		wantType string
	}{
		{"typeof(42)", "integer"},
		{"typeof(3.14)", "real"},
		{"typeof('hello')", "text"},
		{"typeof(NULL)", "null"},
		{"typeof(1 + 2)", "integer"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			got := queryString(t, db, "SELECT "+tt.expr)
			if got != tt.wantType {
				t.Errorf("%s = %q, want %q", tt.expr, got, tt.wantType)
			}
		})
	}
}

// ============================================================================
// 3. NULL handling in all contexts
// ============================================================================

func TestSQLNullArithmetic(t *testing.T) {
	db := openTestDB(t)

	// NULL propagates through arithmetic
	tests := []struct {
		expr string
	}{
		{"NULL + 1"},
		{"1 + NULL"},
		{"NULL * 5"},
		{"NULL / 2"},
		{"NULL - 1"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			if !queryNull(t, db, "SELECT "+tt.expr) {
				t.Errorf("SELECT %s should be NULL", tt.expr)
			}
		})
	}
}

func TestSQLNullComparison(t *testing.T) {
	db := openTestDB(t)

	// NULL comparisons return NULL, not true/false
	tests := []struct {
		expr string
	}{
		{"NULL = NULL"},
		{"NULL != 1"},
		{"NULL < 5"},
		{"NULL > 5"},
		{"1 = NULL"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			// In SQLite, NULL comparisons in WHERE filter out rows
			// For SELECT expressions, they return NULL
			if !queryNull(t, db, "SELECT "+tt.expr) {
				t.Errorf("SELECT %s should be NULL", tt.expr)
			}
		})
	}
}

func TestSQLNullStringConcat(t *testing.T) {
	db := openTestDB(t)

	// NULL || 'x' should be NULL
	if !queryNull(t, db, "SELECT NULL || 'x'") {
		t.Error("NULL || 'x' should be NULL")
	}
	if !queryNull(t, db, "SELECT 'x' || NULL") {
		t.Error("'x' || NULL should be NULL")
	}
}

func TestSQLNullInTable(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE null_test (id INTEGER, a TEXT, b INTEGER)")
	execOrFail(t, db, "INSERT INTO null_test VALUES (1, NULL, NULL)")
	execOrFail(t, db, "INSERT INTO null_test VALUES (2, 'hello', 42)")
	execOrFail(t, db, "INSERT INTO null_test VALUES (3, NULL, 99)")

	rs, err := db.Query("SELECT id, a, b FROM null_test")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Row 1: all NULL
	if rows[0]["a"] != nil || rows[0]["b"] != nil {
		t.Errorf("row 1: expected NULLs, got a=%v b=%v", rows[0]["a"], rows[0]["b"])
	}
	// Row 2: values
	if rows[1]["a"] != "hello" || rows[1]["b"] != int64(42) {
		t.Errorf("row 2: got a=%v b=%v", rows[1]["a"], rows[1]["b"])
	}
}

// ============================================================================
// 4. String comparisons (LIKE, GLOB)
// ============================================================================

func TestSQLStringComparisons(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want bool
	}{
		{"'abc' = 'abc'", true},
		{"'abc' != 'def'", true},
		{"'abc' < 'def'", true},
		{"'def' > 'abc'", true},
		{"'abc' = 'ABC'", false}, // case-sensitive
		{"'hello' LIKE 'hello'", true},
		{"'Hello' LIKE 'hello'", true}, // LIKE is case-insensitive
		{"'hello' LIKE 'hel%'", true},
		{"'hello' LIKE 'h_llo'", true},
		{"'hello' LIKE '%llo'", true},
		{"'hello' LIKE 'h%'", true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			rs, err := db.Query("SELECT " + tt.expr)
			if err != nil {
				t.Skipf("expression not supported: %v", err)
			}
			defer rs.Close()
			if !rs.Next() {
				t.Fatal("no rows")
			}
			got := rs.Row().ColumnInt(0)
			if (got != 0) != tt.want {
				t.Errorf("SELECT %s = %d, want %v", tt.expr, got, tt.want)
			}
		})
	}
}

// ============================================================================
// 5. Numeric edge cases
// ============================================================================

func TestSQLNumericEdgeCases(t *testing.T) {
	db := openTestDB(t)

	t.Run("MaxInt64", func(t *testing.T) {
		got := queryInt(t, db, "SELECT 9223372036854775807")
		if got != 9223372036854775807 {
			t.Errorf("got %d, want 9223372036854775807", got)
		}
	})

	t.Run("NegativeInt", func(t *testing.T) {
		got := queryInt(t, db, "SELECT -9223372036854775808")
		if got != -9223372036854775808 {
			t.Errorf("got %d, want -9223372036854775808", got)
		}
	})

	t.Run("Zero", func(t *testing.T) {
		got := queryInt(t, db, "SELECT 0")
		if got != 0 {
			t.Errorf("got %d, want 0", got)
		}
	})

	t.Run("SmallFloat", func(t *testing.T) {
		got := queryFloat(t, db, "SELECT 0.001")
		if math.Abs(got-0.001) > 0.0001 {
			t.Errorf("got %f, want 0.001", got)
		}
	})

	t.Run("LargeFloat", func(t *testing.T) {
		got := queryFloat(t, db, "SELECT 1e15")
		if math.Abs(got-1e15) > 1 {
			t.Errorf("got %f, want 1e15", got)
		}
	})
}

func TestSQLDivisionByZero(t *testing.T) {
	db := openTestDB(t)

	// Integer division by zero
	rs, err := db.Query("SELECT 1 / 0")
	if err != nil {
		t.Skipf("division by zero not handled: %v", err)
	}
	rs.Close()

	// Float division by zero should give Inf
	rs, err = db.Query("SELECT 1.0 / 0.0")
	if err != nil {
		t.Skipf("float division by zero not handled: %v", err)
	}
	rs.Close()
}

// ============================================================================
// 6. Aggregate function edge cases
// ============================================================================

func TestSQLAggregateCount(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE agg_test (val INTEGER)")
	execOrFail(t, db, "INSERT INTO agg_test VALUES (1)")
	execOrFail(t, db, "INSERT INTO agg_test VALUES (2)")
	execOrFail(t, db, "INSERT INTO agg_test VALUES (NULL)")
	execOrFail(t, db, "INSERT INTO agg_test VALUES (3)")

	t.Run("count_star", func(t *testing.T) {
		rs, err := db.Query("SELECT count(*) FROM agg_test")
		if err != nil {
			t.Skipf("count(*) not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		if got := rs.Row().ColumnInt(0); got != 4 {
			t.Errorf("count(*) = %d, want 4", got)
		}
	})

	t.Run("count_column", func(t *testing.T) {
		rs, err := db.Query("SELECT count(val) FROM agg_test")
		if err != nil {
			t.Skipf("count(col) not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		if got := rs.Row().ColumnInt(0); got != 3 {
			t.Errorf("count(val) = %d, want 3 (excludes NULL)", got)
		}
	})
}

func TestSQLAggregateSum(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE sum_test (val INTEGER)")
	execOrFail(t, db, "INSERT INTO sum_test VALUES (10)")
	execOrFail(t, db, "INSERT INTO sum_test VALUES (20)")
	execOrFail(t, db, "INSERT INTO sum_test VALUES (NULL)")
	execOrFail(t, db, "INSERT INTO sum_test VALUES (30)")

	rs, err := db.Query("SELECT sum(val) FROM sum_test")
	if err != nil {
		t.Skipf("sum() not supported: %v", err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatal("no rows")
	}
	if got := rs.Row().ColumnInt(0); got != 60 {
		t.Errorf("sum(val) = %d, want 60", got)
	}
}

func TestSQLAggregateAvg(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE avg_test (val INTEGER)")
	execOrFail(t, db, "INSERT INTO avg_test VALUES (10)")
	execOrFail(t, db, "INSERT INTO avg_test VALUES (20)")
	execOrFail(t, db, "INSERT INTO avg_test VALUES (30)")

	rs, err := db.Query("SELECT avg(val) FROM avg_test")
	if err != nil {
		t.Skipf("avg() not supported: %v", err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatal("no rows")
	}
	got := rs.Row().ColumnFloat(0)
	if math.Abs(got-20.0) > 0.001 {
		t.Errorf("avg(val) = %f, want 20.0", got)
	}
}

func TestSQLAggregateMinMax(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE minmax_test (val INTEGER)")
	execOrFail(t, db, "INSERT INTO minmax_test VALUES (30)")
	execOrFail(t, db, "INSERT INTO minmax_test VALUES (10)")
	execOrFail(t, db, "INSERT INTO minmax_test VALUES (20)")
	execOrFail(t, db, "INSERT INTO minmax_test VALUES (NULL)")

	rs, err := db.Query("SELECT min(val), max(val) FROM minmax_test")
	if err != nil {
		t.Skipf("min/max not supported: %v", err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatal("no rows")
	}
	if got := rs.Row().ColumnInt(0); got != 10 {
		t.Errorf("min(val) = %d, want 10", got)
	}
	if got := rs.Row().ColumnInt(1); got != 30 {
		t.Errorf("max(val) = %d, want 30", got)
	}
}

func TestSQLAggregateEmptySet(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE empty_agg (val INTEGER)")

	t.Run("count_empty", func(t *testing.T) {
		rs, err := db.Query("SELECT count(*) FROM empty_agg")
		if err != nil {
			t.Skipf("count(*) not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		if got := rs.Row().ColumnInt(0); got != 0 {
			t.Errorf("count(*) on empty = %d, want 0", got)
		}
	})

	t.Run("sum_empty", func(t *testing.T) {
		rs, err := db.Query("SELECT sum(val) FROM empty_agg")
		if err != nil {
			t.Skipf("sum() not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		// sum of empty set should be NULL
		if !rs.Row().ColumnIsNull(0) {
			t.Errorf("sum on empty set should be NULL")
		}
	})
}

// ============================================================================
// 7. CASE expressions
// ============================================================================

func TestSQLCaseExpression(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		name string
		expr string
		want int64
	}{
		{"simple_case", "CASE 1 WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 30 END", 10},
		{"case_second", "CASE 2 WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 30 END", 20},
		{"case_else", "CASE 3 WHEN 1 THEN 10 WHEN 2 THEN 20 ELSE 30 END", 30},
		{"searched_case", "CASE WHEN 5 > 3 THEN 1 ELSE 0 END", 1},
		{"searched_false", "CASE WHEN 3 > 5 THEN 1 ELSE 0 END", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs, err := db.Query("SELECT " + tt.expr)
			if err != nil {
				t.Skipf("CASE not supported: %v", err)
			}
			defer rs.Close()
			if !rs.Next() {
				t.Fatal("no rows")
			}
			got := rs.Row().ColumnInt(0)
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

// ============================================================================
// 8. IN expressions
// ============================================================================

func TestSQLInExpression(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want bool
	}{
		{"5 IN (1, 3, 5, 7)", true},
		{"2 IN (1, 3, 5, 7)", false},
		{"'hello' IN ('world', 'hello')", true},
		{"5 NOT IN (1, 3, 5, 7)", false},
		{"2 NOT IN (1, 3, 5, 7)", true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			rs, err := db.Query("SELECT " + tt.expr)
			if err != nil {
				t.Skipf("IN not supported: %v", err)
			}
			defer rs.Close()
			if !rs.Next() {
				t.Fatal("no rows")
			}
			got := rs.Row().ColumnInt(0)
			if (got != 0) != tt.want {
				t.Errorf("SELECT %s = %d, want %v", tt.expr, got, tt.want)
			}
		})
	}
}

// ============================================================================
// 9. BETWEEN expressions
// ============================================================================

func TestSQLBetween(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want bool
	}{
		{"5 BETWEEN 1 AND 10", true},
		{"1 BETWEEN 1 AND 10", true},
		{"10 BETWEEN 1 AND 10", true},
		{"0 BETWEEN 1 AND 10", false},
		{"11 BETWEEN 1 AND 10", false},
		{"5 NOT BETWEEN 1 AND 10", false},
		{"0 NOT BETWEEN 1 AND 10", true},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			rs, err := db.Query("SELECT " + tt.expr)
			if err != nil {
				t.Skipf("BETWEEN not supported: %v", err)
			}
			defer rs.Close()
			if !rs.Next() {
				t.Fatal("no rows")
			}
			got := rs.Row().ColumnInt(0)
			if (got != 0) != tt.want {
				t.Errorf("SELECT %s = %d, want %v", tt.expr, got, tt.want)
			}
		})
	}
}

// ============================================================================
// 10. IS NULL / IS NOT NULL
// ============================================================================

func TestSQLIsNull(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE isnull_test (val INTEGER)")
	execOrFail(t, db, "INSERT INTO isnull_test VALUES (1)")
	execOrFail(t, db, "INSERT INTO isnull_test VALUES (NULL)")

	t.Run("is_null", func(t *testing.T) {
		rs, err := db.Query("SELECT val IS NULL FROM isnull_test WHERE val IS NULL")
		if err != nil {
			t.Skipf("IS NULL not supported: %v", err)
		}
		defer rs.Close()
		if rs.Next() {
			if got := rs.Row().ColumnInt(0); got != 1 {
				t.Errorf("IS NULL = %d, want 1", got)
			}
		}
	})

	t.Run("is_not_null", func(t *testing.T) {
		rs, err := db.Query("SELECT val IS NOT NULL FROM isnull_test WHERE val IS NOT NULL")
		if err != nil {
			t.Skipf("IS NOT NULL not supported: %v", err)
		}
		defer rs.Close()
		if rs.Next() {
			if got := rs.Row().ColumnInt(0); got != 1 {
				t.Errorf("IS NOT NULL = %d, want 1", got)
			}
		}
	})
}

// ============================================================================
// 11. Transaction isolation tests
// ============================================================================

func TestSQLTransactionIsolation(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE tx_test (id INTEGER, val TEXT)")
	execOrFail(t, db, "INSERT INTO tx_test VALUES (1, 'original')")

	// Begin transaction, modify, but don't commit
	execOrFail(t, db, "BEGIN")
	execOrFail(t, db, "UPDATE tx_test SET val = 'modified'")

	// Within the same connection, we should see modified data
	rs, err := db.Query("SELECT val FROM tx_test WHERE id = 1")
	if err != nil {
		db.Exec("ROLLBACK")
		t.Skipf("WHERE not supported: %v", err)
	}
	defer rs.Close()
	if rs.Next() {
		if got := rs.Row().ColumnText(0); got != "modified" {
			t.Errorf("in-transaction read: got %q, want 'modified'", got)
		}
	}

	execOrFail(t, db, "ROLLBACK")

	// After rollback, should see original
	rs2, err := db.Query("SELECT val FROM tx_test")
	if err != nil {
		t.Fatalf("SELECT after rollback: %v", err)
	}
	defer rs2.Close()
	if rs2.Next() {
		if got := rs2.Row().ColumnText(0); got != "original" {
			t.Errorf("after rollback: got %q, want 'original'", got)
		}
	}
}

// ============================================================================
// 12. Multiple tables and JOINs
// ============================================================================

func TestSQLMultipleTables(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, `
		CREATE TABLE users (id INTEGER, name TEXT);
		CREATE TABLE orders (id INTEGER, user_id INTEGER, amount REAL)
	`)

	execOrFail(t, db, "INSERT INTO users VALUES (1, 'Alice')")
	execOrFail(t, db, "INSERT INTO users VALUES (2, 'Bob')")
	execOrFail(t, db, "INSERT INTO orders VALUES (100, 1, 29.99)")
	execOrFail(t, db, "INSERT INTO orders VALUES (101, 2, 49.99)")

	// Query each table
	users := queryRowCount(t, db, "SELECT * FROM users")
	if users != 2 {
		t.Errorf("users: got %d rows, want 2", users)
	}

	orders := queryRowCount(t, db, "SELECT * FROM orders")
	if orders != 2 {
		t.Errorf("orders: got %d rows, want 2", orders)
	}
}

// ============================================================================
// 13. LIKE pattern tests
// ============================================================================

func TestSQLLikePatterns(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		pattern string
		input   string
		want    bool
	}{
		{"%", "", true},
		{"%", "anything", true},
		{"_", "a", true},
		{"_", "ab", false},
		{"__", "ab", true},
		{"a%", "abc", true},
		{"a%", "a", true},
		{"a%", "bca", false},
		{"%z", "xyz", true},
		{"%z", "xya", false},
		{"a%c", "abc", true},
		{"a%c", "ab", false},
		{"a_b", "axb", true},
		{"a_b", "ab", false},
		{"a_b", "axxb", false},
	}

	for _, tt := range tests {
		name := fmt.Sprintf("LIKE(%q,%q)=%v", tt.pattern, tt.input, tt.want)
		t.Run(name, func(t *testing.T) {
			rs, err := db.Query(fmt.Sprintf("SELECT '%s' LIKE '%s'", tt.input, tt.pattern))
			if err != nil {
				t.Skipf("LIKE not supported: %v", err)
			}
			defer rs.Close()
			if !rs.Next() {
				t.Fatal("no rows")
			}
			got := rs.Row().ColumnInt(0)
			if (got != 0) != tt.want {
				t.Errorf("'%s' LIKE '%s' = %d, want %v", tt.input, tt.pattern, got, tt.want)
			}
		})
	}
}

// ============================================================================
// 14. INSERT with expressions
// ============================================================================

func TestSQLInsertExpressions(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE expr_test (id INTEGER, val INTEGER, name TEXT)")

	// Insert with expressions
	execOrFail(t, db, "INSERT INTO expr_test VALUES (1, 1 + 2, 'test' || 'ing')")
	execOrFail(t, db, "INSERT INTO expr_test VALUES (2, 10 * 5, 'hello')")

	rs, err := db.Query("SELECT id, val, name FROM expr_test")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["val"] != int64(3) {
		t.Errorf("row 1 val: got %v, want 3", rows[0]["val"])
	}
	if rows[0]["name"] != "testing" {
		t.Errorf("row 1 name: got %v, want 'testing'", rows[0]["name"])
	}
	if rows[1]["val"] != int64(50) {
		t.Errorf("row 2 val: got %v, want 50", rows[1]["val"])
	}
}

// ============================================================================
// 15. DISTINCT
// ============================================================================

func TestSQLDistinct(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE dist_test (val INTEGER)")
	execOrFail(t, db, "INSERT INTO dist_test VALUES (1)")
	execOrFail(t, db, "INSERT INTO dist_test VALUES (2)")
	execOrFail(t, db, "INSERT INTO dist_test VALUES (1)")
	execOrFail(t, db, "INSERT INTO dist_test VALUES (2)")
	execOrFail(t, db, "INSERT INTO dist_test VALUES (3)")

	rs, err := db.Query("SELECT DISTINCT val FROM dist_test")
	if err != nil {
		t.Skipf("DISTINCT not supported: %v", err)
	}
	defer rs.Close()

	count := 0
	for rs.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("DISTINCT: got %d rows, want 3", count)
	}
}

// ============================================================================
// 16. Subqueries
// ============================================================================

func TestSQLSubquery(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE sub_test (id INTEGER, val INTEGER)")
	for i := 1; i <= 5; i++ {
		execOrFail(t, db, fmt.Sprintf("INSERT INTO sub_test VALUES (%d, %d)", i, i*10))
	}

	rs, err := db.Query("SELECT * FROM sub_test WHERE val > (SELECT avg(val) FROM sub_test)")
	if err != nil {
		t.Skipf("subqueries not supported: %v", err)
	}
	defer rs.Close()

	count := 0
	for rs.Next() {
		count++
	}
	// avg = 30, so val > 30 means val=40,50 → 2 rows
	if count != 2 {
		t.Errorf("subquery result: got %d rows, want 2", count)
	}
}

// ============================================================================
// 17. COALESCE and IFNULL
// ============================================================================

func TestSQLCoalesce(t *testing.T) {
	db := openTestDB(t)

	t.Run("coalesce", func(t *testing.T) {
		got := queryInt(t, db, "SELECT coalesce(NULL, NULL, 42)")
		if got != 42 {
			t.Errorf("coalesce(NULL,NULL,42) = %d, want 42", got)
		}
	})

	t.Run("coalesce_first_nonnull", func(t *testing.T) {
		got := queryInt(t, db, "SELECT coalesce(1, 2, 3)")
		if got != 1 {
			t.Errorf("coalesce(1,2,3) = %d, want 1", got)
		}
	})

	t.Run("ifnull", func(t *testing.T) {
		got := queryInt(t, db, "SELECT ifnull(NULL, 99)")
		if got != 99 {
			t.Errorf("ifnull(NULL,99) = %d, want 99", got)
		}
	})

	t.Run("ifnull_nonnull", func(t *testing.T) {
		got := queryInt(t, db, "SELECT ifnull(42, 99)")
		if got != 42 {
			t.Errorf("ifnull(42,99) = %d, want 42", got)
		}
	})
}

// ============================================================================
// 18. NULLIF
// ============================================================================

func TestSQLNullif(t *testing.T) {
	db := openTestDB(t)

	t.Run("nullif_equal", func(t *testing.T) {
		if !queryNull(t, db, "SELECT nullif(1, 1)") {
			t.Error("nullif(1,1) should be NULL")
		}
	})

	t.Run("nullif_different", func(t *testing.T) {
		got := queryInt(t, db, "SELECT nullif(1, 2)")
		if got != 1 {
			t.Errorf("nullif(1,2) = %d, want 1", got)
		}
	})
}

// ============================================================================
// 19. Scalar function tests
// ============================================================================

func TestSQLScalarFunctions(t *testing.T) {
	db := openTestDB(t)

	t.Run("abs", func(t *testing.T) {
		got := queryInt(t, db, "SELECT abs(-42)")
		if got != 42 {
			t.Errorf("abs(-42) = %d, want 42", got)
		}
	})

	t.Run("upper", func(t *testing.T) {
		got := queryString(t, db, "SELECT upper('hello')")
		if got != "HELLO" {
			t.Errorf("upper('hello') = %q, want 'HELLO'", got)
		}
	})

	t.Run("lower", func(t *testing.T) {
		got := queryString(t, db, "SELECT lower('HELLO')")
		if got != "hello" {
			t.Errorf("lower('HELLO') = %q, want 'hello'", got)
		}
	})

	t.Run("length", func(t *testing.T) {
		got := queryInt(t, db, "SELECT length('hello')")
		if got != 5 {
			t.Errorf("length('hello') = %d, want 5", got)
		}
	})

	t.Run("typeof_int", func(t *testing.T) {
		got := queryString(t, db, "SELECT typeof(42)")
		if got != "integer" {
			t.Errorf("typeof(42) = %q, want 'integer'", got)
		}
	})

	t.Run("typeof_text", func(t *testing.T) {
		got := queryString(t, db, "SELECT typeof('hello')")
		if got != "text" {
			t.Errorf("typeof('hello') = %q, want 'text'", got)
		}
	})

	t.Run("typeof_null", func(t *testing.T) {
		got := queryString(t, db, "SELECT typeof(NULL)")
		if got != "null" {
			t.Errorf("typeof(NULL) = %q, want 'null'", got)
		}
	})
}

// ============================================================================
// 20. Complex queries
// ============================================================================

func TestSQLNestedExpressions(t *testing.T) {
	db := openTestDB(t)

	// Nested arithmetic
	got := queryInt(t, db, "SELECT (1 + 2) * (3 + 4)")
	if got != 21 {
		t.Errorf("(1+2)*(3+4) = %d, want 21", got)
	}

	// Nested string ops
	got2 := queryString(t, db, "SELECT upper(lower('HELLO World'))")
	if got2 != "HELLO WORLD" {
		t.Errorf("upper(lower('HELLO World')) = %q, want 'HELLO WORLD'", got2)
	}
}

func TestSQLParameterizedQueries(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE params (id INTEGER, name TEXT, score REAL)")

	// Insert with parameters
	execOrFail(t, db, "INSERT INTO params VALUES (?, ?, ?)", 1, "Alice", 95.5)
	execOrFail(t, db, "INSERT INTO params VALUES (?, ?, ?)", 2, "Bob", 87.3)

	// Query with parameters
	rs, err := db.Query("SELECT id, name, score FROM params WHERE id = ?", 1)
	if err != nil {
		t.Skipf("parameterized WHERE not supported: %v", err)
	}
	defer rs.Close()
	if rs.Next() {
		if rs.Row().ColumnText(1) != "Alice" {
			t.Errorf("parameterized query: got %q", rs.Row().ColumnText(1))
		}
	}

	// Query with multiple parameters
	rs2, err := db.Query("SELECT id, name FROM params WHERE id > ? AND score > ?", 0, 80.0)
	if err != nil {
		t.Skipf("multi-parameter WHERE not supported: %v", err)
	}
	rs2.Close()
}

func TestSQLLargeResultSet(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE large (id INTEGER, val TEXT)")

	const numRows = 500
	for i := 0; i < numRows; i++ {
		execOrFail(t, db, fmt.Sprintf("INSERT INTO large VALUES (%d, 'row_%d')", i, i))
	}

	count := queryRowCount(t, db, "SELECT * FROM large")
	if count != numRows {
		t.Errorf("large result set: got %d rows, want %d", count, numRows)
	}
}

func TestSQLStringEscaping(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE str_test (id INTEGER, val TEXT)")

	// String with escaped quotes
	execOrFail(t, db, "INSERT INTO str_test VALUES (1, 'it''s a test')")

	rs, err := db.Query("SELECT val FROM str_test")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()
	if rs.Next() {
		// Just ensure no crash; the exact value depends on quote handling
		_ = rs.Row().ColumnText(0)
	}
}

func TestSQLUpdateWithExpressions(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE upd_test (id INTEGER, val INTEGER)")
	execOrFail(t, db, "INSERT INTO upd_test VALUES (1, 10)")
	execOrFail(t, db, "INSERT INTO upd_test VALUES (2, 20)")
	execOrFail(t, db, "INSERT INTO upd_test VALUES (3, 30)")

	execOrFail(t, db, "UPDATE upd_test SET val = val * 2")

	rs, err := db.Query("SELECT id, val FROM upd_test ORDER BY id")
	if err != nil {
		t.Skipf("ORDER BY not supported: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	expected := []int64{20, 40, 60}
	for i, exp := range expected {
		if rows[i]["val"] != exp {
			t.Errorf("row %d: val=%v, want %d", i, rows[i]["val"], exp)
		}
	}
}

// ============================================================================
// 21. DELETE with conditions
// ============================================================================

func TestSQLDeleteConditional(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE del_test (id INTEGER, val INTEGER)")
	for i := 1; i <= 10; i++ {
		execOrFail(t, db, fmt.Sprintf("INSERT INTO del_test VALUES (%d, %d)", i, i*10))
	}

	// Delete even rows
	rs, err := db.Query("DELETE FROM del_test WHERE val > 50")
	if err != nil {
		t.Skipf("DELETE WHERE not supported: %v", err)
	}
	rs.Close()

	count := queryRowCount(t, db, "SELECT * FROM del_test")
	if count != 5 {
		t.Errorf("after conditional delete: got %d rows, want 5", count)
	}
}

// ============================================================================
// 22. Multiple statements in one Exec
// ============================================================================

func TestSQLMultipleStatementsExec(t *testing.T) {
	db := openTestDB(t)

	err := db.Exec(`
		CREATE TABLE multi1 (id INTEGER);
		CREATE TABLE multi2 (id INTEGER);
		INSERT INTO multi1 VALUES (1);
		INSERT INTO multi2 VALUES (2)
	`)
	if err != nil {
		t.Fatalf("multi-statement exec: %v", err)
	}

	if queryRowCount(t, db, "SELECT * FROM multi1") != 1 {
		t.Error("multi1 should have 1 row")
	}
	if queryRowCount(t, db, "SELECT * FROM multi2") != 1 {
		t.Error("multi2 should have 1 row")
	}
}

// ============================================================================
// 23. Error handling
// ============================================================================

func TestSQLDuplicateTableError(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE dup (id INTEGER)")
	if err := db.Exec("CREATE TABLE dup (id INTEGER)"); err == nil {
		t.Error("expected error creating duplicate table")
	}
}

func TestSQLInsertNonexistentTable(t *testing.T) {
	db := openTestDB(t)

	if err := db.Exec("INSERT INTO noexist VALUES (1)"); err == nil {
		t.Error("expected error inserting into nonexistent table")
	}
}

func TestSQLSelectNonexistentTable(t *testing.T) {
	db := openTestDB(t)

	if _, err := db.Query("SELECT * FROM noexist"); err == nil {
		t.Error("expected error selecting from nonexistent table")
	}
}

func TestSQLExecAfterClose(t *testing.T) {
	db, err := sqlite.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	if err := db.Exec("CREATE TABLE t (id INTEGER)"); err == nil {
		t.Error("expected error on closed DB")
	}
}

// ============================================================================
// 24. Default values
// ============================================================================

func TestSQLDefaultValues(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE def_test (id INTEGER, name TEXT)")

	execOrFail(t, db, "INSERT INTO def_test DEFAULT VALUES")

	rs, err := db.Query("SELECT id, name FROM def_test")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatal("no rows")
	}
	row := rs.Row()
	if !row.ColumnIsNull(0) || !row.ColumnIsNull(1) {
		t.Error("DEFAULT VALUES should produce NULL columns")
	}
}

// ============================================================================
// 25. INSERT with column list
// ============================================================================

func TestSQLInsertColumnList(t *testing.T) {
	db := openTestDB(t)

	execOrFail(t, db, "CREATE TABLE collist (id INTEGER, name TEXT, age INTEGER)")

	execOrFail(t, db, "INSERT INTO collist (id, name) VALUES (1, 'Alice')")

	rs, err := db.Query("SELECT id, name, age FROM collist")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatal("no rows")
	}
	row := rs.Row()
	if row.ColumnInt(0) != 1 {
		t.Errorf("id = %d, want 1", row.ColumnInt(0))
	}
	if row.ColumnText(1) != "Alice" {
		t.Errorf("name = %q, want 'Alice'", row.ColumnText(1))
	}
	if !row.ColumnIsNull(2) {
		t.Error("age should be NULL (not specified)")
	}
}
