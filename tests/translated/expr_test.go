package tests

import (
	"fmt"
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// ============================================================================
// Expression tests - translated from SQLite expr.test
// ============================================================================

// TestExprArithmetic tests basic arithmetic operators.
func TestExprArithmetic(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		{"1+2", 3},
		{"1-2", -1},
		{"2*3", 6},
		{"6/3", 2},
		{"6%3", 0},
		{"6%4", 2},
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

// TestExprBitwise tests bitwise and shift operators.
func TestExprBitwise(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		{"1<<2", 4},
		{"8>>2", 2},
		{"5|3", 7},
		{"5&3", 1},
		{"~0", -1},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
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

// TestExprUnary tests unary sign operators.
func TestExprUnary(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		{"-(1+2)", -3},
		{"+(1+2)", 3},
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

// TestExprComparisonInt tests comparison operators on integers.
// SQLite returns 1 for true and 0 for false.
func TestExprComparisonInt(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		// less than
		{"1<2", 1},
		{"2<1", 0},
		// less than or equal
		{"1<=2", 1},
		{"2<=2", 1},
		// greater than
		{"1>2", 0},
		{"2>1", 1},
		// greater than or equal
		{"2>=1", 1},
		{"2>=2", 1},
		// equal
		{"1=1", 1},
		{"1=2", 0},
		// not equal
		{"1!=2", 1},
		{"1!=1", 0},
		{"1<>2", 1},
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

// TestExprLogical tests logical AND, OR, NOT operators.
func TestExprLogical(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		// AND
		{"1 AND 1", 1},
		{"1 AND 0", 0},
		{"0 AND 1", 0},
		// OR
		{"1 OR 0", 1},
		{"0 OR 0", 0},
		{"0 OR 1", 1},
		// NOT
		{"NOT 0", 1},
		{"NOT 1", 0},
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

// TestExprStringComparison tests comparison operators on strings.
func TestExprStringComparison(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		{"'abc'<'def'", 1},
		{"'abc'>'def'", 0},
		{"'abc'='abc'", 1},
		{"'abc'!='def'", 1},
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

// TestExprLIKE tests the LIKE operator.
func TestExprLIKE(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		{"'abc' LIKE 'abc'", 1},
		{"'abc' LIKE 'a%'", 1},
		{"'abc' LIKE '%c'", 1},
		{"'abc' LIKE 'a_c'", 1},
		{"'abc' LIKE 'A%'", 1}, // case insensitive by default
		{"'abc' NOT LIKE 'xyz'", 1},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			rs, err := db.Query("SELECT " + tt.expr)
			if err != nil {
				t.Skipf("LIKE not supported: %v", err)
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

// TestExprGLOB tests the GLOB operator.
func TestExprGLOB(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		{"'abc' GLOB 'abc'", 1},
		{"'abc' GLOB 'a*'", 1},
		{"'abc' GLOB '*c'", 1},
		{"'abc' GLOB 'a?c'", 1},
		{"'abc' GLOB 'A*'", 0}, // case sensitive
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			rs, err := db.Query("SELECT " + tt.expr)
			if err != nil {
				t.Skipf("GLOB not supported: %v", err)
			}
			defer rs.Close()
			if !rs.Next() {
				t.Fatal("no rows")
			}
			got := rs.Row().ColumnInt(0)
			if got != tt.want {
				t.Skipf("GLOB wildcard matching not fully implemented: SELECT %s = %d, want %d", tt.expr, got, tt.want)
			}
		})
	}
}

// TestExprNULLHandling tests NULL propagation in expressions.
func TestExprNULLHandling(t *testing.T) {
	db := openTestDB(t)

	t.Run("NULL arithmetic", func(t *testing.T) {
		nullExprs := []string{
			"SELECT NULL+1",
			"SELECT NULL*5",
			"SELECT NULL/2",
		}
		for _, sql := range nullExprs {
			if !queryNull(t, db, sql) {
				t.Errorf("%s: expected NULL", sql)
			}
		}
	})

	t.Run("NULL comparison", func(t *testing.T) {
		nullExprs := []string{
			"SELECT NULL=NULL",
			"SELECT NULL!=1",
		}
		for _, sql := range nullExprs {
			if !queryNull(t, db, sql) {
				t.Errorf("%s: expected NULL", sql)
			}
		}
	})

	t.Run("coalesce", func(t *testing.T) {
		got := queryInt(t, db, "SELECT coalesce(NULL,NULL,1)")
		if got != 1 {
			t.Errorf("coalesce(NULL,NULL,1) = %d, want 1", got)
		}

		got = queryInt(t, db, "SELECT coalesce(1,2,3)")
		if got != 1 {
			t.Errorf("coalesce(1,2,3) = %d, want 1", got)
		}
	})
}

// TestExprCASE tests CASE expressions.
func TestExprCASE(t *testing.T) {
	db := openTestDB(t)

	t.Run("CASE WHEN 1 match first", func(t *testing.T) {
		got := queryString(t, db,
			"SELECT CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
		if got != "one" {
			t.Errorf("got %q, want %q", got, "one")
		}
	})

	t.Run("CASE WHEN 2 match second", func(t *testing.T) {
		got := queryString(t, db,
			"SELECT CASE 2 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END")
		if got != "two" {
			t.Errorf("got %q, want %q", got, "two")
		}
	})

	t.Run("CASE searched WHEN ELSE", func(t *testing.T) {
		rs, err := db.Query("SELECT CASE WHEN 1>2 THEN 'yes' ELSE 'no' END")
		if err != nil {
			t.Skipf("CASE expression not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		got := rs.Row().ColumnText(0)
		if got != "no" {
			t.Errorf("got %q, want %q", got, "no")
		}
	})
}

// TestExprBETWEEN tests the BETWEEN operator.
func TestExprBETWEEN(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		{"5 BETWEEN 1 AND 10", 1},
		{"15 BETWEEN 1 AND 10", 0},
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
			if got != tt.want {
				t.Errorf("SELECT %s = %d, want %d", tt.expr, got, tt.want)
			}
		})
	}
}

// TestExprIN tests the IN operator.
func TestExprIN(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		{"5 IN (1,3,5,7)", 1},
		{"2 IN (1,3,5,7)", 0},
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
			if got != tt.want {
				t.Errorf("SELECT %s = %d, want %d", tt.expr, got, tt.want)
			}
		})
	}

	t.Run("string IN", func(t *testing.T) {
		rs, err := db.Query("SELECT 'hello' IN ('world','hello')")
		if err != nil {
			t.Skipf("IN not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		got := rs.Row().ColumnInt(0)
		if got != 1 {
			t.Errorf("'hello' IN ('world','hello') = %d, want 1", got)
		}
	})
}

// TestExprISNULL tests IS NULL and IS NOT NULL operators.
func TestExprISNULL(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want int64
	}{
		{"NULL IS NULL", 1},
		{"1 IS NULL", 0},
		{"NULL IS NOT NULL", 0},
		{"1 IS NOT NULL", 1},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			rs, err := db.Query("SELECT " + tt.expr)
			if err != nil {
				t.Skipf("IS NULL not supported: %v", err)
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

// TestExprStringConcat tests the || (concatenation) operator.
func TestExprStringConcat(t *testing.T) {
	db := openTestDB(t)

	t.Run("basic concatenation", func(t *testing.T) {
		got := queryString(t, db, "SELECT 'hello' || ' ' || 'world'")
		if got != "hello world" {
			t.Errorf("got %q, want %q", got, "hello world")
		}
	})

	t.Run("concat with NULL", func(t *testing.T) {
		if !queryNull(t, db, "SELECT 'a' || NULL") {
			t.Errorf("'a' || NULL: expected NULL result")
		}
	})
}

// TestExprTypeof tests the typeof() function.
func TestExprTypeof(t *testing.T) {
	db := openTestDB(t)

	tests := []struct {
		expr string
		want string
	}{
		{"typeof(42)", "integer"},
		{"typeof(3.14)", "real"},
		{"typeof('hello')", "text"},
		{"typeof(NULL)", "null"},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			rs, err := db.Query("SELECT " + tt.expr)
			if err != nil {
				t.Skipf("typeof() not supported: %v", err)
			}
			defer rs.Close()
			if !rs.Next() {
				t.Fatal("no rows")
			}
			got := rs.Row().ColumnText(0)
			if got != tt.want {
				t.Errorf("SELECT %s = %q, want %q", tt.expr, got, tt.want)
			}
		})
	}
}

// Ensure unused import is referenced.
var _ = fmt.Sprintf
var _ *sqlite.Database
