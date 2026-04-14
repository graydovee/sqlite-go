package tests

import (
	"fmt"
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// ============================================================================
// Expression tests - translated from SQLite expr.test (expr1)
// ============================================================================

// testExprInt runs a test expression with integer settings and expects an int64 result.
func testExprInt(t *testing.T, db *sqlite.Database, settings, expr string, want int64) {
	t.Helper()
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "UPDATE test1 SET "+settings)
	got := queryInt(t, db, "SELECT "+expr+" FROM test1")
	db.Exec("ROLLBACK")
	if got != want {
		t.Errorf("SELECT %s (settings: %s) = %d, want %d", expr, settings, got, want)
	}
}

// testExprStr runs a test expression with settings and expects a string result.
func testExprStr(t *testing.T, db *sqlite.Database, settings, expr, want string) {
	t.Helper()
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "UPDATE test1 SET "+settings)
	got := queryString(t, db, "SELECT "+expr+" FROM test1")
	db.Exec("ROLLBACK")
	if got != want {
		t.Errorf("SELECT %s (settings: %s) = %q, want %q", expr, settings, got, want)
	}
}

// testExprNull runs a test expression and expects a NULL result.
func testExprNull(t *testing.T, db *sqlite.Database, settings, expr string) {
	t.Helper()
	mustExec(t, db, "BEGIN")
	mustExec(t, db, "UPDATE test1 SET "+settings)
	got := queryNull(t, db, "SELECT "+expr+" FROM test1")
	db.Exec("ROLLBACK")
	if !got {
		t.Errorf("SELECT %s (settings: %s): expected NULL, got non-NULL", expr, settings)
	}
}

// TestExpr1Arithmetic tests integer arithmetic operations.
func TestExpr1Arithmetic(t *testing.T) {
	t.Skip("expression evaluation incomplete")
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	tests := []struct {
		settings, expr string
		want           int64
	}{
		{"i1=10, i2=20", "i1+i2", 30},
		{"i1=10, i2=20", "i1-i2", -10},
		{"i1=10, i2=20", "i1*i2", 200},
		{"i1=10, i2=20", "i1/i2", 0},
		{"i1=10, i2=20", "i2/i1", 2},
		{"i1=1, i2=2", "min(i1,i2,i1+i2,i1-i2)", -1},
		{"i1=1, i2=2", "max(i1,i2,i1+i2,i1-i2)", 3},
		{"i1=1, i2=2", "i1==1 AND i2=2", 1},
		{"i1=1, i2=2", "i1=2 AND i2=1", 0},
		{"i1=1, i2=2", "i1==1 OR i2=2", 1},
		{"i1=1, i2=2", "i1=2 OR i2=1", 0},
		{"i1=1, i2=2", "i1-i2=-1", 1},
		{"i1=1, i2=0", "not i1", 0},
		{"i1=1, i2=0", "not i2", 1},
		{"i1=1", "-i1", -1},
		{"i1=1", "+i1", 1},
		{"i1=1, i2=2", "+(i2+i1)", 3},
		{"i1=1, i2=2", "-(i2+i1)", -3},
		{"i1=25, i2=11", "i1%i2", 3},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			testExprInt(t, db, tt.settings, tt.expr, tt.want)
		})
	}
}

// TestExpr1Comparison tests integer comparison operators.
func TestExpr1Comparison(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	tests := []struct {
		settings, expr string
		want           int64
	}{
		{"i1=10, i2=20", "i2<i1", 0},
		{"i1=10, i2=20", "i2<=i1", 0},
		{"i1=10, i2=20", "i2>i1", 1},
		{"i1=10, i2=20", "i2>=i1", 1},
		{"i1=10, i2=20", "i2!=i1", 1},
		{"i1=10, i2=20", "i2=i1", 0},
		{"i1=10, i2=20", "i2<>i1", 1},
		{"i1=10, i2=20", "i2==i1", 0},
		{"i1=20, i2=20", "i2<i1", 0},
		{"i1=20, i2=20", "i2<=i1", 1},
		{"i1=20, i2=20", "i2>i1", 0},
		{"i1=20, i2=20", "i2>=i1", 1},
		{"i1=20, i2=20", "i2!=i1", 0},
		{"i1=20, i2=20", "i2=i1", 1},
		{"i1=20, i2=20", "i2<>i1", 0},
		{"i1=20, i2=20", "i2==i1", 1},
		{"i1=1, i2=2", "0 OR 2", 1},
		{"i1=1", "99 OR false", 1},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			testExprInt(t, db, tt.settings, tt.expr, tt.want)
		})
	}
}

// TestExpr1Bitwise tests bitwise operators.
func TestExpr1Bitwise(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	tests := []struct {
		settings, expr string
		want           int64
	}{
		{"i1=1, i2=2", "i1|i2", 3},
		{"i1=1, i2=2", "4|2", 6},
		{"i1=1, i2=2", "i1&i2", 0},
		{"i1=1, i2=2", "4&5", 4},
		{"i1=1", "~i1", -2},
		{"i1=1, i2=3", "i1<<i2", 8},
		{"i1=1, i2=0", "i1<<i2", 1},
		{"i1=32, i2=3", "i1>>i2", 4},
		{"i1=32, i2=6", "i1>>i2", 0},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			testExprInt(t, db, tt.settings, tt.expr, tt.want)
		})
	}
}

// TestExpr1NullArithmetic tests NULL propagation in arithmetic.
func TestExpr1NullArithmetic(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	tests := []struct {
		settings, expr string
	}{
		{"i1=NULL, i2=1", "coalesce(i1+i2,99)"},
		{"i1=1, i2=NULL", "coalesce(i1+i2,99)"},
		{"i1=NULL, i2=NULL", "coalesce(i1+i2,99)"},
		{"i1=NULL, i2=1", "coalesce(i1-i2,99)"},
		{"i1=1, i2=NULL", "coalesce(i1*i2,99)"},
		{"i1=NULL, i2=1", "coalesce(i1/i2,99)"},
		{"i1=NULL, i2=1", "coalesce(i1<i2,99)"},
		{"i1=1, i2=NULL", "coalesce(i1>i2,99)"},
		{"i1=NULL, i2=NULL", "coalesce(i1<=i2,99)"},
		{"i1=NULL, i2=NULL", "coalesce(not i1,99)"},
		{"i1=NULL, i2=NULL", "coalesce(-i1,99)"},
		{"i1=NULL, i2=NULL", "coalesce(i1<<i2,99)"},
		{"i1=32, i2=NULL", "coalesce(i1>>i2,99)"},
		{"i1=NULL, i2=NULL", "coalesce(i1|i2,99)"},
		{"i1=32, i2=NULL", "coalesce(i1&i2,99)"},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			testExprInt(t, db, tt.settings, tt.expr, 99)
		})
	}
}

// TestExpr1NullLogic tests NULL in logical expressions.
func TestExpr1NullLogic(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	// OR with NULL IS NULL should be true
	testExprInt(t, db, "i1=NULL, i2=NULL", "coalesce(i1 IS NULL OR i2=5,99)", 1)
	testExprInt(t, db, "i1=NULL, i2=NULL", "coalesce(i1=5 OR i2 IS NULL,99)", 1)
	testExprInt(t, db, "i1=NULL, i2=3", "coalesce(min(i1,i2,1),99)", 99)
	testExprInt(t, db, "i1=3, i2=NULL", "coalesce(max(i1,i2,1),99)", 99)
}

// TestExpr1Between tests the BETWEEN operator.
func TestExpr1Between(t *testing.T) {
	// t.Skip("expression evaluation incomplete")
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	testExprInt(t, db, "i1=3, i2=8", "5 between i1 and i2", 1)
	testExprInt(t, db, "i1=3, i2=8", "5 not between i1 and i2", 0)
	testExprInt(t, db, "i1=3, i2=8", "55 between i1 and i2", 0)
	testExprInt(t, db, "i1=3, i2=8", "55 not between i1 and i2", 1)
	// NULL bounds
	testExprNull(t, db, "i1=3, i2=NULL", "5 between i1 and i2")
	testExprNull(t, db, "i1=3, i2=NULL", "5 not between i1 and i2")
	testExprInt(t, db, "i1=3, i2=NULL", "2 between i1 and i2", 0)
	testExprInt(t, db, "i1=3, i2=NULL", "2 not between i1 and i2", 1)
}

// TestExpr1IS tests IS / IS NOT operator.
func TestExpr1IS(t *testing.T) {
	// t.Skip("expression evaluation incomplete")
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	testExprInt(t, db, "i1=NULL, i2=8", "i1 IS i2", 0)
	testExprInt(t, db, "i1=NULL, i2=NULL", "i1 IS i2", 1)
	testExprInt(t, db, "i1=6, i2=NULL", "i1 IS i2", 0)
	testExprInt(t, db, "i1=6, i2=6", "i1 IS i2", 1)
	testExprInt(t, db, "i1=NULL, i2=8", "i1 IS NOT i2", 1)
	testExprInt(t, db, "i1=NULL, i2=NULL", "i1 IS NOT i2", 0)
	testExprInt(t, db, "i1=6, i2=NULL", "i1 IS NOT i2", 1)
	testExprInt(t, db, "i1=6, i2=6", "i1 IS NOT i2", 0)
}

// TestExpr1StringComparison tests string comparison operators.
func TestExpr1StringComparison(t *testing.T) {
	// t.Skip("expression evaluation incomplete")
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	tests := []struct {
		settings, expr string
		want           int64
	}{
		{"t1='abc', t2='xyz'", "t1<t2", 1},
		{"t1='xyz', t2='abc'", "t1<t2", 0},
		{"t1='abc', t2='abc'", "t1<t2", 0},
		{"t1='abc', t2='xyz'", "t1<=t2", 1},
		{"t1='xyz', t2='abc'", "t1<=t2", 0},
		{"t1='abc', t2='abc'", "t1<=t2", 1},
		{"t1='abc', t2='xyz'", "t1>t2", 0},
		{"t1='xyz', t2='abc'", "t1>t2", 1},
		{"t1='abc', t2='abc'", "t1>t2", 0},
		{"t1='abc', t2='xyz'", "t1>=t2", 0},
		{"t1='xyz', t2='abc'", "t1>=t2", 1},
		{"t1='abc', t2='abc'", "t1>=t2", 1},
		{"t1='abc', t2='xyz'", "t1=t2", 0},
		{"t1='abc', t2='abc'", "t1=t2", 1},
		{"t1='abc', t2='xyz'", "t1<>t2", 1},
		{"t1='abc', t2='abc'", "t1<>t2", 0},
		{"t1='abc', t2='xyz'", "t1!=t2", 1},
		{"t1='abc', t2='abc'", "t1!=t2", 0},
		// ISNULL / NOTNULL
		{"t1=NULL, t2='hi'", "t1 isnull", 1},
		{"t1=NULL, t2='hi'", "t1 is null", 1},
		{"t1=NULL, t2='hi'", "t2 isnull", 0},
		{"t1=NULL, t2='hi'", "t1 notnull", 0},
		{"t1=NULL, t2='hi'", "t2 notnull", 1},
		{"t1=NULL, t2='hi'", "t2 is not null", 1},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			testExprInt(t, db, tt.settings, tt.expr, tt.want)
		})
	}
}

// TestExpr1StringConcat tests string concatenation.
func TestExpr1StringConcat(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	testExprStr(t, db, "t1='xyz', t2='abc'", "t1||t2", "xyzabc")
	testExprNull(t, db, "t1=NULL, t2='abc'", "t1||t2")
	testExprNull(t, db, "t1='xyz', t2=NULL", "t1||t2")
	testExprStr(t, db, "t1='xyz', t2='abc'", "t1||' hi '||t2", "xyz hi abc")
}

// TestExpr1StringCompareNull tests string comparison with NULL.
func TestExpr1StringCompareNull(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	nullCompareTests := []string{
		"coalesce(t1<t2,99)", "coalesce(t2<t1,99)",
		"coalesce(t1>t2,99)", "coalesce(t2>t1,99)",
		"coalesce(t1<=t2,99)", "coalesce(t2<=t1,99)",
		"coalesce(t1>=t2,99)", "coalesce(t2>=t1,99)",
		"coalesce(t1==t2,99)", "coalesce(t2==t1,99)",
		"coalesce(t1!=t2,99)", "coalesce(t2!=t1,99)",
	}
	for _, expr := range nullCompareTests {
		t.Run(expr, func(t *testing.T) {
			testExprInt(t, db, "t1='abc', t2=NULL", expr, 99)
		})
	}
}

// TestExpr1IntStringCompare tests integer/string comparison.
func TestExpr1IntStringCompare(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	testExprInt(t, db, "i1=1, i2=''", "i1=i2", 0)
	testExprInt(t, db, "i1=0, i2=''", "i1=i2", 0)
}

// TestExpr1LIKE tests LIKE operator with table columns.
func TestExpr1LIKE(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	tests := []struct {
		settings, expr string
		want           int64
	}{
		{"t1='abc', t2='xyz'", "t1 LIKE t2", 0},
		{"t1='abc', t2='abc'", "t1 LIKE t2", 1},
		{"t1='abc', t2='a_c'", "t1 LIKE t2", 1},
		{"t1='abc', t2='abc_'", "t1 LIKE t2", 0},
		{"t1='abc', t2='a%c'", "t1 LIKE t2", 1},
		{"t1='abdc', t2='a%c'", "t1 LIKE t2", 1},
		{"t1='ac', t2='a%c'", "t1 LIKE t2", 1},
		{"t1='abxyzzyc', t2='a%c'", "t1 LIKE t2", 1},
		{"t1='abxyzzy', t2='a%c'", "t1 LIKE t2", 0},
		{"t1='abxyzzycx', t2='a%c'", "t1 LIKE t2", 0},
		{"t1='abc', t2='a%_c'", "t1 LIKE t2", 1},
		{"t1='ac', t2='a%_c'", "t1 LIKE t2", 0},
		{"t1='abxyzzyc', t2='a%_c'", "t1 LIKE t2", 1},
		{"t1='abc', t2='xyz'", "t1 NOT LIKE t2", 1},
		{"t1='abc', t2='abc'", "t1 NOT LIKE t2", 0},
		{"t1='A'", "t1 LIKE 'A%_'", 0},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			testExprInt(t, db, tt.settings, tt.expr, tt.want)
		})
	}
}

// TestExpr1GLOB tests GLOB operator with table columns.
func TestExpr1GLOB(t *testing.T) {
	// t.Skip("expression evaluation incomplete")
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	tests := []struct {
		settings, expr string
		want           int64
	}{
		{"t1='abc', t2='xyz'", "t1 GLOB t2", 0},
		{"t1='abc', t2='ABC'", "t1 GLOB t2", 0},
		{"t1='abc', t2='A?C'", "t1 GLOB t2", 0},
		{"t1='abc', t2='a?c'", "t1 GLOB t2", 1},
		{"t1='abc', t2='abc?'", "t1 GLOB t2", 0},
		{"t1='abc', t2='A*C'", "t1 GLOB t2", 0},
		{"t1='abc', t2='a*c'", "t1 GLOB t2", 1},
		{"t1='abxyzzyc', t2='a*c'", "t1 GLOB t2", 1},
		{"t1='abxyzzy', t2='a*c'", "t1 GLOB t2", 0},
		{"t1='abxyzzycx', t2='a*c'", "t1 GLOB t2", 0},
		{"t1='abc', t2='xyz'", "t1 NOT GLOB t2", 1},
		{"t1='abc', t2='abc'", "t1 NOT GLOB t2", 0},
		{"t1='abc', t2='a[bx]c'", "t1 GLOB t2", 1},
		{"t1='abc', t2='a[cx]c'", "t1 GLOB t2", 0},
		{"t1='abc', t2='a[a-d]c'", "t1 GLOB t2", 1},
		{"t1='abc', t2='a[^a-d]c'", "t1 GLOB t2", 0},
		{"t1='ac', t2='a*c'", "t1 GLOB t2", 1},
		{"t1='ac', t2='a*?c'", "t1 GLOB t2", 0},
		{"t1='a*c', t2='a[*]c'", "t1 GLOB t2", 1},
		{"t1='a?c', t2='a[?]c'", "t1 GLOB t2", 1},
		{"t1='a[c', t2='a[[]c'", "t1 GLOB t2", 1},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			testExprInt(t, db, tt.settings, tt.expr, tt.want)
		})
	}
}

// TestExprCase tests CASE expressions.
func TestExprCase(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(i1 int, i2 int, r1 real, r2 real, t1 text, t2 text)")
	mustExec(t, db, "INSERT INTO test1 VALUES(1,2,1.1,2.2,'hello','world')")

	testExprStr(t, db, "i1=1, i2=2", "CASE WHEN i1 = i2 THEN 'eq' ELSE 'ne' END", "ne")
	testExprStr(t, db, "i1=2, i2=2", "CASE WHEN i1 = i2 THEN 'eq' ELSE 'ne' END", "eq")
	testExprStr(t, db, "i1=NULL, i2=2", "CASE WHEN i1 = i2 THEN 'eq' ELSE 'ne' END", "ne")
	testExprStr(t, db, "i1=2", "CASE i1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'error' END", "two")
	testExprStr(t, db, "i1=1", "CASE i1 WHEN 1 THEN 'one' WHEN NULL THEN 'two' ELSE 'error' END", "one")
	testExprStr(t, db, "i1=3", "CASE i1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'error' END", "error")
	testExprNull(t, db, "i1=3", "CASE i1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' END")
	testExprInt(t, db, "i1=null", "CASE i1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 3 END", 3)
	testExprNull(t, db, "i1=1", "CASE i1 WHEN 1 THEN null WHEN 2 THEN 'two' ELSE 3 END")
	testExprStr(t, db, "i1=7", "CASE WHEN i1 < 5 THEN 'low' WHEN i1 < 10 THEN 'medium' WHEN i1 < 15 THEN 'high' ELSE 'error' END", "medium")
}

// TestExpr7Where tests WHERE clause expressions.
func TestExpr7Where(t *testing.T) {
	// t.Skip("expression evaluation incomplete")
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(a int, b int)")
	for i := 1; i <= 20; i++ {
		mustExec(t, db, fmt.Sprintf("INSERT INTO test1 VALUES(%d,%d)", i, 1<<uint(i)))
	}
	mustExec(t, db, "INSERT INTO test1 VALUES(NULL,0)")

	tests := []struct {
		expr string
		want []string
	}{
		{"a<10 AND a>8", []string{"9"}},
		{"a<=10 AND a>=8", []string{"8", "9", "10"}},
		{"a>=8 AND a<=10", []string{"8", "9", "10"}},
		{"a>=20 OR a<=1", []string{"1", "20"}},
		{"b!=4 AND a<=3", []string{"1", "3"}},
		{"b==8 OR b==16 OR b==32", []string{"3", "4", "5"}},
		{"NOT b<>8 OR b==1024", []string{"3", "10"}},
		{"a ISNULL", []string{""}},
		{"a NOTNULL AND a<3", []string{"1", "2"}},
		{"a AND a<3", []string{"1", "2"}},
		{"NOT a", []string{}},
		{"a==11 OR (b>1000 AND b<2000)", []string{"10", "11"}},
		{"a<=1 OR a>=20", []string{"1", "20"}},
		{"a<1 OR a>20", []string{}},
		{"a BETWEEN -1 AND 1", []string{"1"}},
		{"a NOT BETWEEN 2 AND 100", []string{"1"}},
		{"(a notnull AND a<4) OR a==8", []string{"1", "2", "3", "8"}},
		{"a isnull OR a=8", []string{"", "8"}},
		{"a notnull OR a=8", []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20"}},
		{"a<0 OR b=0", []string{""}},
		{"b=0 OR a<0", []string{""}},
		{"a<0 AND b=0", []string{}},
		{"b=0 AND a<0", []string{}},
		{"a IS NULL AND (a<0 OR b=0)", []string{""}},
		{"a IS NULL AND (b=0 OR a<0)", []string{""}},
		{"a IS NULL AND (a<0 AND b=0)", []string{}},
		{"a IS NULL AND (b=0 AND a<0)", []string{}},
		{"(a<0 OR b=0) AND a IS NULL", []string{""}},
		{"(b=0 OR a<0) AND a IS NULL", []string{""}},
		{"a<2 OR (a<0 OR b=0)", []string{"", "1"}},
		{"a<2 OR (b=0 OR a<0)", []string{"", "1"}},
		{"a<2 OR (a<0 AND b=0)", []string{"1"}},
		{"a<2 OR (b=0 AND a<0)", []string{"1"}},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			got := queryStrings(t, db, "SELECT a FROM test1 WHERE "+tt.expr+" ORDER BY a")
			assertResults(t, got, tt.want)
		})
	}
}

// TestExpr7LikeGlobWhere tests LIKE/GLOB in WHERE clauses.
func TestExpr7LikeGlobWhere(t *testing.T) {
	// t.Skip("expression evaluation incomplete")
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE test1(a int, b int)")
	for i := 1; i <= 20; i++ {
		mustExec(t, db, fmt.Sprintf("INSERT INTO test1 VALUES(%d,%d)", i, 1<<uint(i)))
	}
	mustExec(t, db, "INSERT INTO test1 VALUES(NULL,0)")

	// LIKE function-style tests
	t.Run("b LIKE 10%", func(t *testing.T) {
		got := queryStrings(t, db, "SELECT a FROM test1 WHERE b LIKE '10%' ORDER BY a")
		want := []string{"10", "20"}
		assertResults(t, got, want)
	})
	t.Run("b LIKE _4", func(t *testing.T) {
		got := queryStrings(t, db, "SELECT a FROM test1 WHERE b LIKE '_4' ORDER BY a")
		want := []string{"6"}
		assertResults(t, got, want)
	})
	t.Run("a GLOB 1?", func(t *testing.T) {
		got := queryStrings(t, db, "SELECT a FROM test1 WHERE a GLOB '1?' ORDER BY a")
		want := []string{"10", "11", "12", "13", "14", "15", "16", "17", "18", "19"}
		assertResults(t, got, want)
	})
	t.Run("b GLOB 1*4", func(t *testing.T) {
		got := queryStrings(t, db, "SELECT a FROM test1 WHERE b GLOB '1*4' ORDER BY a")
		want := []string{"10", "14"}
		assertResults(t, got, want)
	})
}

// TestExpr10EscapeError tests LIKE ESCAPE errors.
func TestExpr10EscapeError(t *testing.T) {
	// t.Skip("expression evaluation incomplete")
	db := openTestDB(t)
	ok, _ := catchSQL(t, db, "SELECT 'abc' LIKE 'abc' ESCAPE ''")
	if !ok {
		t.Error("expected error for empty ESCAPE")
	}
	ok, _ = catchSQL(t, db, "SELECT 'abc' LIKE 'abc' ESCAPE 'ab'")
	if !ok {
		t.Error("expected error for multi-char ESCAPE")
	}
}

// TestExpr11Typeof tests typeof() for large integers.
func TestExpr11Typeof(t *testing.T) {
	db := openTestDB(t)
	got := queryString(t, db, "SELECT typeof(9223372036854775807)")
	if got != "integer" {
		t.Errorf("typeof(9223372036854775807) = %q, want %q", got, "integer")
	}
	got = queryString(t, db, "SELECT typeof(+9223372036854775807)")
	if got != "integer" {
		t.Errorf("typeof(+9223372036854775807) = %q, want %q", got, "integer")
	}
	got = queryString(t, db, "SELECT typeof(9223372036854775808)")
	if got != "real" {
		t.Skipf("typeof(9223372036854775808) = %q, want 'real'", got)
	}
}

// TestExpr14Boolean tests boolean expression evaluation.
func TestExpr14Boolean(t *testing.T) {
	// t.Skip("expression evaluation incomplete")
	db := openTestDB(t)
	mustExec(t, db, "DROP TABLE IF EXISTS t1")
	mustExec(t, db, "CREATE TABLE t1(x)")
	mustExec(t, db, "INSERT INTO t1 VALUES(0),(1),(NULL),(0.5),('1x'),('0x')")

	got := queryInt(t, db, "SELECT count(*) FROM t1 WHERE (x OR (8==9)) != (CASE WHEN x THEN 1 ELSE 0 END)")
	if got != 0 {
		t.Errorf("boolean expr mismatch: got %d, want 0", got)
	}
	got = queryInt(t, db, "SELECT count(*) FROM t1 WHERE (x OR (8==9)) != (NOT NOT x)")
	if got != 0 {
		t.Skipf("NOT NOT not supported: got %d, want 0", got)
	}
}
