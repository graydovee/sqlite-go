package compile

import (
	"strings"
	"testing"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// =============================================================================
// Helper functions for tests
// =============================================================================

func parseOne(t *testing.T, sql string) *Statement {
	t.Helper()
	stmts, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse error for %q: %v", sql, err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d for %q", len(stmts), sql)
	}
	return stmts[0]
}

func mustParse(t *testing.T, sql string) []*Statement {
	t.Helper()
	stmts, err := Parse(sql)
	if err != nil {
		t.Fatalf("parse error for %q: %v", sql, err)
	}
	return stmts
}

func expectError(t *testing.T, sql string) {
	t.Helper()
	_, err := Parse(sql)
	if err == nil {
		t.Fatalf("expected parse error for %q, got nil", sql)
	}
}

// =============================================================================
// SELECT tests
// =============================================================================

func TestSelectStar(t *testing.T) {
	stmt := parseOne(t, "SELECT * FROM users")
	if stmt.Type != StmtSelect {
		t.Fatalf("expected StmtSelect, got %d", stmt.Type)
	}
	sel := stmt.SelectStmt
	if len(sel.Columns) != 1 || !sel.Columns[0].Star {
		t.Fatalf("expected one star column, got %v", sel.Columns)
	}
	if sel.From == nil || len(sel.From.Tables) != 1 {
		t.Fatalf("expected FROM users")
	}
	if sel.From.Tables[0].Name != "users" {
		t.Fatalf("expected table 'users', got %q", sel.From.Tables[0].Name)
	}
}

func TestSelectColumns(t *testing.T) {
	stmt := parseOne(t, "SELECT id, name FROM users")
	sel := stmt.SelectStmt
	if len(sel.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
	}
	if sel.Columns[0].Expr.Kind != ExprColumnRef || sel.Columns[0].Expr.Name != "id" {
		t.Fatalf("expected column 'id', got %v", sel.Columns[0].Expr)
	}
	if sel.Columns[1].Expr.Kind != ExprColumnRef || sel.Columns[1].Expr.Name != "name" {
		t.Fatalf("expected column 'name', got %v", sel.Columns[1].Expr)
	}
}

func TestSelectWithWhere(t *testing.T) {
	stmt := parseOne(t, "SELECT * FROM users WHERE id = 1")
	sel := stmt.SelectStmt
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if sel.Where.Kind != ExprBinaryOp || sel.Where.Op != "=" {
		t.Fatalf("expected = comparison, got %v", sel.Where)
	}
}

func TestSelectWithOrderLimit(t *testing.T) {
	stmt := parseOne(t, "SELECT * FROM users ORDER BY name ASC LIMIT 10")
	sel := stmt.SelectStmt
	if len(sel.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY item, got %d", len(sel.OrderBy))
	}
	if sel.OrderBy[0].Expr.Name != "name" {
		t.Fatalf("expected ORDER BY name, got %v", sel.OrderBy[0].Expr.Name)
	}
	if sel.OrderBy[0].Order != SortAsc {
		t.Fatalf("expected ASC, got %d", sel.OrderBy[0].Order)
	}
	if sel.Limit == nil {
		t.Fatal("expected LIMIT")
	}
}

func TestSelectWithOffset(t *testing.T) {
	stmt := parseOne(t, "SELECT * FROM users LIMIT 10 OFFSET 5")
	sel := stmt.SelectStmt
	if sel.Limit == nil || sel.Offset == nil {
		t.Fatal("expected LIMIT and OFFSET")
	}
}

func TestSelectDistinct(t *testing.T) {
	stmt := parseOne(t, "SELECT DISTINCT name FROM users")
	sel := stmt.SelectStmt
	if !sel.Distinct {
		t.Fatal("expected DISTINCT")
	}
}

func TestSelectGroupByHaving(t *testing.T) {
	stmt := parseOne(t, "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 5")
	sel := stmt.SelectStmt
	if len(sel.GroupBy) != 1 {
		t.Fatalf("expected 1 GROUP BY expr, got %d", len(sel.GroupBy))
	}
	if sel.Having == nil {
		t.Fatal("expected HAVING clause")
	}
}

func TestSelectColumnAliases(t *testing.T) {
	stmt := parseOne(t, "SELECT id AS user_id, name username FROM users")
	sel := stmt.SelectStmt
	if sel.Columns[0].As != "user_id" {
		t.Fatalf("expected alias 'user_id', got %q", sel.Columns[0].As)
	}
	if sel.Columns[1].As != "username" {
		t.Fatalf("expected alias 'username', got %q", sel.Columns[1].As)
	}
}

func TestSelectQualifiedColumns(t *testing.T) {
	stmt := parseOne(t, "SELECT u.id, u.name FROM users AS u")
	sel := stmt.SelectStmt
	if sel.Columns[0].Expr.Kind != ExprColumnRef {
		t.Fatalf("expected ExprColumnRef, got %d", sel.Columns[0].Expr.Kind)
	}
	if sel.Columns[0].Expr.Table != "u" || sel.Columns[0].Expr.Name != "id" {
		t.Fatalf("expected u.id, got %s.%s", sel.Columns[0].Expr.Table, sel.Columns[0].Expr.Name)
	}
}

func TestSelectTableStar(t *testing.T) {
	stmt := parseOne(t, "SELECT u.* FROM users u")
	sel := stmt.SelectStmt
	if len(sel.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(sel.Columns))
	}
	if sel.Columns[0].TableStar != "u" {
		t.Fatalf("expected u.*, got %v", sel.Columns[0])
	}
}

func TestSelectWithJoin(t *testing.T) {
	stmt := parseOne(t, "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id")
	sel := stmt.SelectStmt
	if sel.From == nil || len(sel.From.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(sel.From.Tables))
	}
	if sel.From.Tables[1].JoinType != JoinInner {
		t.Fatalf("expected INNER join, got %d", sel.From.Tables[1].JoinType)
	}
	if sel.From.Tables[1].On == nil {
		t.Fatal("expected ON clause for join")
	}
}

func TestSelectLeftJoin(t *testing.T) {
	stmt := parseOne(t, "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id")
	sel := stmt.SelectStmt
	if sel.From.Tables[1].JoinType != JoinLeft {
		t.Fatalf("expected LEFT join, got %d", sel.From.Tables[1].JoinType)
	}
}

func TestSelectMultipleJoins(t *testing.T) {
	sql := "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if len(sel.From.Tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(sel.From.Tables))
	}
}

func TestSelectSubquery(t *testing.T) {
	stmt := parseOne(t, "SELECT * FROM (SELECT id FROM users) AS sub")
	sel := stmt.SelectStmt
	if sel.From.Tables[0].Subquery == nil {
		t.Fatal("expected subquery in FROM")
	}
	if sel.From.Tables[0].Alias != "sub" {
		t.Fatalf("expected alias 'sub', got %q", sel.From.Tables[0].Alias)
	}
}

func TestSelectWithParenthesizedJoin(t *testing.T) {
	stmt := parseOne(t, "SELECT * FROM (a JOIN b ON a.id = b.a_id)")
	sel := stmt.SelectStmt
	if sel.From == nil {
		t.Fatal("expected FROM clause")
	}
}

// =============================================================================
// Expression tests
// =============================================================================

func TestExprBinaryOps(t *testing.T) {
	tests := []struct {
		sql string
		op  string
	}{
		{"SELECT a + b", "+"},
		{"SELECT a - b", "-"},
		{"SELECT a * b", "*"},
		{"SELECT a / b", "/"},
		{"SELECT a % b", "%"},
		{"SELECT a || b", "||"},
		{"SELECT a & b", "&"},
		{"SELECT a | b", "|"},
		{"SELECT a << b", "<<"},
		{"SELECT a >> b", ">>"},
	}
	for _, tt := range tests {
		stmt := parseOne(t, tt.sql)
		expr := stmt.SelectStmt.Columns[0].Expr
		if expr.Kind != ExprBinaryOp || expr.Op != tt.op {
			t.Errorf("expected binary op %q, got kind=%d op=%q for %q", tt.op, expr.Kind, expr.Op, tt.sql)
		}
	}
}

func TestExprComparisons(t *testing.T) {
	tests := []struct {
		sql string
		op  string
	}{
		{"SELECT a = b", "="},
		{"SELECT a == b", "=="},
		{"SELECT a <> b", "<>"},
		{"SELECT a != b", "!="},
		{"SELECT a < b", "<"},
		{"SELECT a <= b", "<="},
		{"SELECT a > b", ">"},
		{"SELECT a >= b", ">="},
	}
	for _, tt := range tests {
		stmt := parseOne(t, tt.sql)
		expr := stmt.SelectStmt.Columns[0].Expr
		if expr.Kind != ExprBinaryOp || expr.Op != tt.op {
			t.Errorf("expected comparison %q, got kind=%d op=%q for %q", tt.op, expr.Kind, expr.Op, tt.sql)
		}
	}
}

func TestExprPrecedence(t *testing.T) {
	// a + b * c should parse as a + (b * c)
	stmt := parseOne(t, "SELECT a + b * c")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Op != "+" {
		t.Fatalf("expected + at top, got %q", expr.Op)
	}
	if expr.Right == nil || expr.Right.Op != "*" {
		t.Fatalf("expected * on right side, got %v", expr.Right)
	}
}

func TestExprAndOrPrecedence(t *testing.T) {
	// a OR b AND c should parse as a OR (b AND c)
	stmt := parseOne(t, "SELECT a OR b AND c FROM t")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Op != "OR" {
		t.Fatalf("expected OR at top, got %q", expr.Op)
	}
	if expr.Right == nil || expr.Right.Op != "AND" {
		t.Fatalf("expected AND on right side, got %v", expr.Right)
	}
}

func TestExprNot(t *testing.T) {
	stmt := parseOne(t, "SELECT NOT a FROM t")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprUnaryOp || expr.Op != "NOT" {
		t.Fatalf("expected NOT unary op, got %v", expr)
	}
}

func TestExprUnaryMinus(t *testing.T) {
	stmt := parseOne(t, "SELECT -5")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind == ExprUnaryOp && expr.Op == "-" {
		// OK
	} else if expr.Kind == ExprLiteral && expr.LiteralType == "integer" && expr.IntValue == -5 {
		// Also OK (optimized)
	} else {
		t.Fatalf("expected unary minus or negative literal, got kind=%d", expr.Kind)
	}
}

func TestExprIsNull(t *testing.T) {
	stmt := parseOne(t, "SELECT a IS NULL")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprIsNull {
		t.Fatalf("expected ExprIsNull, got %d", expr.Kind)
	}
}

func TestExprIsNotNull(t *testing.T) {
	stmt := parseOne(t, "SELECT a IS NOT NULL")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprIsNotNull {
		t.Fatalf("expected ExprIsNotNull, got %d", expr.Kind)
	}
}

func TestExprBetween(t *testing.T) {
	stmt := parseOne(t, "SELECT a BETWEEN 1 AND 10")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprBetween {
		t.Fatalf("expected ExprBetween, got %d", expr.Kind)
	}
	if expr.Not {
		t.Fatal("expected Not=false")
	}
}

func TestExprNotBetween(t *testing.T) {
	stmt := parseOne(t, "SELECT a NOT BETWEEN 1 AND 10")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprBetween {
		t.Fatalf("expected ExprBetween, got %d", expr.Kind)
	}
	if !expr.Not {
		t.Fatal("expected Not=true")
	}
}

func TestExprInList(t *testing.T) {
	stmt := parseOne(t, "SELECT a IN (1, 2, 3)")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprInList {
		t.Fatalf("expected ExprInList, got %d", expr.Kind)
	}
	if len(expr.InValues) != 3 {
		t.Fatalf("expected 3 values, got %d", len(expr.InValues))
	}
}

func TestExprNotInList(t *testing.T) {
	stmt := parseOne(t, "SELECT a NOT IN (1, 2)")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprInList || !expr.Not {
		t.Fatalf("expected ExprInList with Not=true, got kind=%d not=%v", expr.Kind, expr.Not)
	}
}

func TestExprInSelect(t *testing.T) {
	stmt := parseOne(t, "SELECT a IN (SELECT id FROM t)")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprInSelect {
		t.Fatalf("expected ExprInSelect, got %d", expr.Kind)
	}
	if expr.InSelect == nil {
		t.Fatal("expected subquery in IN")
	}
}

func TestExprLike(t *testing.T) {
	stmt := parseOne(t, "SELECT a LIKE '%hello%'")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprLike {
		t.Fatalf("expected ExprLike, got %d", expr.Kind)
	}
}

func TestExprNotLike(t *testing.T) {
	stmt := parseOne(t, "SELECT a NOT LIKE '%hello%'")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprLike || !expr.Not {
		t.Fatalf("expected ExprLike with Not=true, got %d", expr.Kind)
	}
}

func TestExprLikeWithEscape(t *testing.T) {
	stmt := parseOne(t, "SELECT a LIKE 'x%' ESCAPE 'x'")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprLike {
		t.Fatalf("expected ExprLike, got %d", expr.Kind)
	}
	if expr.Escape == nil {
		t.Fatal("expected ESCAPE clause")
	}
}

func TestExprGlob(t *testing.T) {
	stmt := parseOne(t, "SELECT a GLOB '*.txt'")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprGlob {
		t.Fatalf("expected ExprGlob, got %d", expr.Kind)
	}
}

func TestExprFunctionCall(t *testing.T) {
	stmt := parseOne(t, "SELECT COUNT(*)")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprFunctionCall || expr.FunctionName != "COUNT" || !expr.StarArg {
		t.Fatalf("expected COUNT(*) function, got %v", expr)
	}
}

func TestExprFunctionWithArgs(t *testing.T) {
	stmt := parseOne(t, "SELECT SUM(salary), MAX(id)")
	expr1 := stmt.SelectStmt.Columns[0].Expr
	expr2 := stmt.SelectStmt.Columns[1].Expr
	if expr1.FunctionName != "SUM" || len(expr1.Args) != 1 {
		t.Fatalf("expected SUM(salary), got %v", expr1)
	}
	if expr2.FunctionName != "MAX" || len(expr2.Args) != 1 {
		t.Fatalf("expected MAX(id), got %v", expr2)
	}
}

func TestExprFunctionDistinct(t *testing.T) {
	stmt := parseOne(t, "SELECT COUNT(DISTINCT name)")
	expr := stmt.SelectStmt.Columns[0].Expr
	if !expr.Distinct {
		t.Fatal("expected DISTINCT in function call")
	}
}

func TestExprSubquery(t *testing.T) {
	stmt := parseOne(t, "SELECT (SELECT MAX(id) FROM users)")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprSubquery {
		t.Fatalf("expected ExprSubquery, got %d", expr.Kind)
	}
}

func TestExprExists(t *testing.T) {
	stmt := parseOne(t, "SELECT EXISTS (SELECT 1 FROM users)")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprExists {
		t.Fatalf("expected ExprExists, got %d", expr.Kind)
	}
}

func TestExprCase(t *testing.T) {
	sql := "SELECT CASE WHEN a > 0 THEN 'pos' WHEN a < 0 THEN 'neg' ELSE 'zero' END FROM t"
	stmt := parseOne(t, sql)
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprCase {
		t.Fatalf("expected ExprCase, got %d", expr.Kind)
	}
	if len(expr.WhenList) != 2 {
		t.Fatalf("expected 2 WHEN clauses, got %d", len(expr.WhenList))
	}
	if expr.ElseExpr == nil {
		t.Fatal("expected ELSE clause")
	}
}

func TestExprCaseWithOperand(t *testing.T) {
	sql := "SELECT CASE x WHEN 1 THEN 'one' WHEN 2 THEN 'two' END FROM t"
	stmt := parseOne(t, sql)
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Operand == nil || expr.Operand.Name != "x" {
		t.Fatal("expected CASE operand 'x'")
	}
}

func TestExprCast(t *testing.T) {
	stmt := parseOne(t, "SELECT CAST(x AS INTEGER)")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprCast {
		t.Fatalf("expected ExprCast, got %d", expr.Kind)
	}
	if expr.CastType != "INTEGER" {
		t.Fatalf("expected cast type INTEGER, got %q", expr.CastType)
	}
}

func TestExprCollate(t *testing.T) {
	stmt := parseOne(t, "SELECT a COLLATE NOCASE")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprCollate {
		t.Fatalf("expected ExprCollate, got %d", expr.Kind)
	}
	if expr.Collation != "NOCASE" {
		t.Fatalf("expected collation NOCASE, got %q", expr.Collation)
	}
}

func TestExprIsNullPostfix(t *testing.T) {
	stmt := parseOne(t, "SELECT a ISNULL, b NOTNULL FROM t")
	if stmt.SelectStmt.Columns[0].Expr.Kind != ExprIsNull {
		t.Fatalf("expected ExprIsNull, got %d", stmt.SelectStmt.Columns[0].Expr.Kind)
	}
	if stmt.SelectStmt.Columns[1].Expr.Kind != ExprIsNotNull {
		t.Fatalf("expected ExprIsNotNull, got %d", stmt.SelectStmt.Columns[1].Expr.Kind)
	}
}

func TestExprNotExists(t *testing.T) {
	stmt := parseOne(t, "SELECT NOT EXISTS (SELECT 1 FROM t)")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprUnaryOp || expr.Op != "NOT" {
		t.Fatalf("expected NOT unary op, got %v", expr)
	}
	if expr.Right == nil || expr.Right.Kind != ExprExists {
		t.Fatal("expected EXISTS as operand of NOT")
	}
}

func TestExprStringLiteral(t *testing.T) {
	stmt := parseOne(t, "SELECT 'hello world'")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprLiteral || expr.LiteralType != "string" || expr.StringValue != "hello world" {
		t.Fatalf("expected string literal 'hello world', got %v", expr)
	}
}

func TestExprBlobLiteral(t *testing.T) {
	stmt := parseOne(t, "SELECT X'deadbeef'")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprLiteral || expr.LiteralType != "blob" {
		t.Fatalf("expected blob literal, got %v", expr)
	}
}

func TestExprNull(t *testing.T) {
	stmt := parseOne(t, "SELECT NULL")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprLiteral || expr.LiteralType != "null" {
		t.Fatalf("expected null literal, got %v", expr)
	}
}

func TestExprVariable(t *testing.T) {
	stmt := parseOne(t, "SELECT ?, :name, @var, $param")
	sel := stmt.SelectStmt
	for i, expected := range []string{"?", ":name", "@var", "$param"} {
		expr := sel.Columns[i].Expr
		if expr.Kind != ExprVariable || expr.StringValue != expected {
			t.Errorf("column %d: expected variable %q, got %v", i, expected, expr)
		}
	}
}

func TestExprParenthesized(t *testing.T) {
	// (a + b) * c should parse with parens forcing addition first
	stmt := parseOne(t, "SELECT (a + b) * c")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Op != "*" {
		t.Fatalf("expected * at top, got %q", expr.Op)
	}
	if expr.Left == nil || expr.Left.Op != "+" {
		t.Fatalf("expected + on left side, got %v", expr.Left)
	}
}

func TestExprIsExpressions(t *testing.T) {
	stmt := parseOne(t, "SELECT a IS b, a IS NOT b")
	sel := stmt.SelectStmt
	if sel.Columns[0].Expr.Op != "IS" {
		t.Fatalf("expected IS, got %q", sel.Columns[0].Expr.Op)
	}
	if sel.Columns[1].Expr.Op != "IS NOT" {
		t.Fatalf("expected IS NOT, got %q", sel.Columns[1].Expr.Op)
	}
}

func TestExprInTable(t *testing.T) {
	stmt := parseOne(t, "SELECT a IN users")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprInTable {
		t.Fatalf("expected ExprInTable, got %d", expr.Kind)
	}
	if expr.InTable != "users" {
		t.Fatalf("expected table 'users', got %q", expr.InTable)
	}
}

// =============================================================================
// INSERT tests
// =============================================================================

func TestInsertValues(t *testing.T) {
	stmt := parseOne(t, "INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if stmt.Type != StmtInsert {
		t.Fatalf("expected StmtInsert, got %d", stmt.Type)
	}
	ins := stmt.InsertStmt
	if ins.Table.Name != "users" {
		t.Fatalf("expected table 'users', got %q", ins.Table.Name)
	}
	if len(ins.Columns) != 2 || ins.Columns[0] != "id" || ins.Columns[1] != "name" {
		t.Fatalf("expected columns [id name], got %v", ins.Columns)
	}
	if len(ins.Values) != 1 || len(ins.Values[0]) != 2 {
		t.Fatalf("expected 1 row with 2 values, got %v", ins.Values)
	}
}

func TestInsertWithoutColumns(t *testing.T) {
	stmt := parseOne(t, "INSERT INTO users VALUES (1, 'Alice')")
	ins := stmt.InsertStmt
	if len(ins.Columns) != 0 {
		t.Fatalf("expected no columns, got %v", ins.Columns)
	}
}

func TestInsertMultipleRows(t *testing.T) {
	stmt := parseOne(t, "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
	ins := stmt.InsertStmt
	if len(ins.Values) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(ins.Values))
	}
}

func TestInsertDefaultValues(t *testing.T) {
	stmt := parseOne(t, "INSERT INTO users DEFAULT VALUES")
	ins := stmt.InsertStmt
	if !ins.DefaultValues {
		t.Fatal("expected DefaultValues=true")
	}
}

func TestInsertOrReplace(t *testing.T) {
	stmt := parseOne(t, "INSERT OR REPLACE INTO users VALUES (1, 'Alice')")
	ins := stmt.InsertStmt
	if !ins.OrReplace {
		t.Fatal("expected OrReplace=true")
	}
}

func TestInsertOrIgnore(t *testing.T) {
	stmt := parseOne(t, "INSERT OR IGNORE INTO users VALUES (1, 'Alice')")
	ins := stmt.InsertStmt
	if !ins.OrIgnore {
		t.Fatal("expected OrIgnore=true")
	}
}

func TestInsertSelect(t *testing.T) {
	stmt := parseOne(t, "INSERT INTO users SELECT * FROM temp_users")
	ins := stmt.InsertStmt
	if ins.Select == nil {
		t.Fatal("expected SELECT in INSERT")
	}
}

func TestReplace(t *testing.T) {
	stmt := parseOne(t, "REPLACE INTO users VALUES (1, 'Alice')")
	ins := stmt.InsertStmt
	if !ins.OrReplace {
		t.Fatal("expected OrReplace=true for REPLACE")
	}
}

// =============================================================================
// UPDATE tests
// =============================================================================

func TestUpdate(t *testing.T) {
	stmt := parseOne(t, "UPDATE users SET name = 'Bob' WHERE id = 1")
	if stmt.Type != StmtUpdate {
		t.Fatalf("expected StmtUpdate, got %d", stmt.Type)
	}
	upd := stmt.UpdateStmt
	if upd.Table.Name != "users" {
		t.Fatalf("expected table 'users', got %q", upd.Table.Name)
	}
	if len(upd.Sets) != 1 {
		t.Fatalf("expected 1 SET clause, got %d", len(upd.Sets))
	}
	if upd.Sets[0].Columns[0] != "name" {
		t.Fatalf("expected SET name, got %v", upd.Sets[0].Columns)
	}
	if upd.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestUpdateMultipleSets(t *testing.T) {
	stmt := parseOne(t, "UPDATE users SET name = 'Bob', age = 30 WHERE id = 1")
	upd := stmt.UpdateStmt
	if len(upd.Sets) != 2 {
		t.Fatalf("expected 2 SET clauses, got %d", len(upd.Sets))
	}
}

func TestUpdateOrAbort(t *testing.T) {
	stmt := parseOne(t, "UPDATE OR ABORT users SET name = 'Bob'")
	upd := stmt.UpdateStmt
	if !upd.OrAbort {
		t.Fatal("expected OrAbort=true")
	}
}

// =============================================================================
// DELETE tests
// =============================================================================

func TestDelete(t *testing.T) {
	stmt := parseOne(t, "DELETE FROM users WHERE id = 1")
	if stmt.Type != StmtDelete {
		t.Fatalf("expected StmtDelete, got %d", stmt.Type)
	}
	del := stmt.DeleteStmt
	if del.Table.Name != "users" {
		t.Fatalf("expected table 'users', got %q", del.Table.Name)
	}
	if del.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestDeleteAll(t *testing.T) {
	stmt := parseOne(t, "DELETE FROM users")
	del := stmt.DeleteStmt
	if del.Where != nil {
		t.Fatal("expected no WHERE clause")
	}
}

// =============================================================================
// CREATE TABLE tests
// =============================================================================

func TestCreateTable(t *testing.T) {
	sql := `CREATE TABLE users (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		email TEXT UNIQUE,
		age INTEGER DEFAULT 0
	)`
	stmt := parseOne(t, sql)
	if stmt.Type != StmtCreateTable {
		t.Fatalf("expected StmtCreateTable, got %d", stmt.Type)
	}
	ct := stmt.CreateTable
	if ct.Name != "users" {
		t.Fatalf("expected table 'users', got %q", ct.Name)
	}
	if len(ct.Columns) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(ct.Columns))
	}
}

func TestCreateTableConstraints(t *testing.T) {
	sql := `CREATE TABLE orders (
		id INTEGER PRIMARY KEY,
		user_id INTEGER NOT NULL REFERENCES users(id),
		total REAL DEFAULT 0.0,
		CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	stmt := parseOne(t, sql)
	ct := stmt.CreateTable
	if len(ct.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(ct.Columns))
	}
	// Check NOT NULL on user_id
	foundNotNull := false
	for _, c := range ct.Columns {
		if c.Name == "user_id" {
			for _, cc := range c.Constraints {
				if cc.Type == CCNotNull {
					foundNotNull = true
				}
			}
		}
	}
	if !foundNotNull {
		t.Fatal("expected NOT NULL constraint on user_id")
	}
}

func TestCreateTableIfNotExists(t *testing.T) {
	stmt := parseOne(t, "CREATE TABLE IF NOT EXISTS users (id INTEGER)")
	ct := stmt.CreateTable
	if !ct.IfNotExists {
		t.Fatal("expected IfNotExists=true")
	}
}

func TestCreateTempTable(t *testing.T) {
	stmt := parseOne(t, "CREATE TEMP TABLE users (id INTEGER)")
	ct := stmt.CreateTable
	if !ct.Temp {
		t.Fatal("expected Temp=true")
	}
}

func TestCreateTableAsSelect(t *testing.T) {
	stmt := parseOne(t, "CREATE TABLE backup AS SELECT * FROM users")
	ct := stmt.CreateTable
	if ct.AsSelect == nil {
		t.Fatal("expected AS SELECT")
	}
}

func TestCreateTableAutoincrement(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT)"
	stmt := parseOne(t, sql)
	ct := stmt.CreateTable
	col := ct.Columns[0]
	foundAutoinc := false
	for _, c := range col.Constraints {
		if c.Autoincrement {
			foundAutoinc = true
		}
	}
	if !foundAutoinc {
		t.Fatal("expected AUTOINCREMENT")
	}
}

func TestCreateTableDefaultExpr(t *testing.T) {
	sql := "CREATE TABLE t (ts TEXT DEFAULT (datetime('now')))"
	stmt := parseOne(t, sql)
	ct := stmt.CreateTable
	col := ct.Columns[0]
	foundDefault := false
	for _, c := range col.Constraints {
		if c.Type == CCDefault && c.Default != nil {
			foundDefault = true
		}
	}
	if !foundDefault {
		t.Fatal("expected DEFAULT constraint")
	}
}

func TestCreateTableCheckConstraint(t *testing.T) {
	sql := "CREATE TABLE t (age INTEGER CHECK (age >= 0))"
	stmt := parseOne(t, sql)
	ct := stmt.CreateTable
	col := ct.Columns[0]
	foundCheck := false
	for _, c := range col.Constraints {
		if c.Type == CCCheck {
			foundCheck = true
		}
	}
	if !foundCheck {
		t.Fatal("expected CHECK constraint")
	}
}

func TestCreateTableCollate(t *testing.T) {
	sql := "CREATE TABLE t (name TEXT COLLATE NOCASE)"
	stmt := parseOne(t, sql)
	ct := stmt.CreateTable
	col := ct.Columns[0]
	foundCollate := false
	for _, c := range col.Constraints {
		if c.Type == CCCollate && c.Collation == "NOCASE" {
			foundCollate = true
		}
	}
	if !foundCollate {
		t.Fatal("expected COLLATE NOCASE constraint")
	}
}

func TestCreateTableUniqueConstraint(t *testing.T) {
	sql := "CREATE TABLE t (id INTEGER, name TEXT, UNIQUE (id, name))"
	stmt := parseOne(t, sql)
	ct := stmt.CreateTable
	if len(ct.Constraints) != 1 {
		t.Fatalf("expected 1 table constraint, got %d", len(ct.Constraints))
	}
	if ct.Constraints[0].Type != TCUnique {
		t.Fatal("expected UNIQUE table constraint")
	}
}

func TestCreateTablePrimaryKeyConstraint(t *testing.T) {
	sql := "CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a))"
	stmt := parseOne(t, sql)
	ct := stmt.CreateTable
	if len(ct.Constraints) < 1 {
		t.Fatal("expected table constraints")
	}
	foundPK := false
	for _, tc := range ct.Constraints {
		if tc.Type == TCPrimaryKey {
			foundPK = true
		}
	}
	if !foundPK {
		t.Fatal("expected PRIMARY KEY table constraint")
	}
}

func TestCreateTableForeignKeyConstraint(t *testing.T) {
	sql := `CREATE TABLE orders (
		id INTEGER,
		user_id INTEGER,
		FOREIGN KEY (user_id) REFERENCES users(id)
	)`
	stmt := parseOne(t, sql)
	ct := stmt.CreateTable
	foundFK := false
	for _, tc := range ct.Constraints {
		if tc.Type == TCForeignKey && tc.ForeignKey != nil {
			foundFK = true
			if tc.ForeignKey.RefTable != "users" {
				t.Fatalf("expected references 'users', got %q", tc.ForeignKey.RefTable)
			}
		}
	}
	if !foundFK {
		t.Fatal("expected FOREIGN KEY table constraint")
	}
}

// =============================================================================
// CREATE INDEX tests
// =============================================================================

func TestCreateIndex(t *testing.T) {
	stmt := parseOne(t, "CREATE INDEX idx_name ON users (name)")
	if stmt.Type != StmtCreateIndex {
		t.Fatalf("expected StmtCreateIndex, got %d", stmt.Type)
	}
	ci := stmt.CreateIndex
	if ci.Name != "idx_name" {
		t.Fatalf("expected index 'idx_name', got %q", ci.Name)
	}
	if ci.Table != "users" {
		t.Fatalf("expected table 'users', got %q", ci.Table)
	}
	if len(ci.Columns) != 1 {
		t.Fatalf("expected 1 column, got %d", len(ci.Columns))
	}
}

func TestCreateUniqueIndex(t *testing.T) {
	stmt := parseOne(t, "CREATE UNIQUE INDEX idx_email ON users (email)")
	ci := stmt.CreateIndex
	if !ci.Unique {
		t.Fatal("expected Unique=true")
	}
}

func TestCreateIndexIfNotExists(t *testing.T) {
	stmt := parseOne(t, "CREATE INDEX IF NOT EXISTS idx_name ON users (name)")
	ci := stmt.CreateIndex
	if !ci.IfNotExists {
		t.Fatal("expected IfNotExists=true")
	}
}

func TestCreateIndexWithOrder(t *testing.T) {
	stmt := parseOne(t, "CREATE INDEX idx_name ON users (name ASC, id DESC)")
	ci := stmt.CreateIndex
	if len(ci.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ci.Columns))
	}
	if ci.Columns[0].Order != SortAsc {
		t.Fatal("expected ASC on first column")
	}
	if ci.Columns[1].Order != SortDesc {
		t.Fatal("expected DESC on second column")
	}
}

func TestCreatePartialIndex(t *testing.T) {
	stmt := parseOne(t, "CREATE INDEX idx_active ON users (name) WHERE active = 1")
	ci := stmt.CreateIndex
	if ci.Where == nil {
		t.Fatal("expected WHERE clause for partial index")
	}
}

// =============================================================================
// DROP tests
// =============================================================================

func TestDropTable(t *testing.T) {
	stmt := parseOne(t, "DROP TABLE users")
	if stmt.Type != StmtDropTable {
		t.Fatalf("expected StmtDropTable, got %d", stmt.Type)
	}
	dt := stmt.DropTable
	if dt.Name != "users" {
		t.Fatalf("expected table 'users', got %q", dt.Name)
	}
}

func TestDropTableIfExists(t *testing.T) {
	stmt := parseOne(t, "DROP TABLE IF EXISTS users")
	dt := stmt.DropTable
	if !dt.IfExists {
		t.Fatal("expected IfExists=true")
	}
}

func TestDropIndex(t *testing.T) {
	stmt := parseOne(t, "DROP INDEX idx_name")
	if stmt.Type != StmtDropIndex {
		t.Fatalf("expected StmtDropIndex, got %d", stmt.Type)
	}
	di := stmt.DropIndex
	if di.Name != "idx_name" {
		t.Fatalf("expected index 'idx_name', got %q", di.Name)
	}
}

func TestDropIndexIfExists(t *testing.T) {
	stmt := parseOne(t, "DROP INDEX IF EXISTS idx_name")
	di := stmt.DropIndex
	if !di.IfExists {
		t.Fatal("expected IfExists=true")
	}
}

// =============================================================================
// Transaction tests
// =============================================================================

func TestBegin(t *testing.T) {
	stmt := parseOne(t, "BEGIN")
	if stmt.Type != StmtBegin {
		t.Fatalf("expected StmtBegin, got %d", stmt.Type)
	}
}

func TestBeginDeferred(t *testing.T) {
	stmt := parseOne(t, "BEGIN DEFERRED")
	if !stmt.BeginStmt.Deferred {
		t.Fatal("expected Deferred=true")
	}
}

func TestBeginImmediate(t *testing.T) {
	stmt := parseOne(t, "BEGIN IMMEDIATE")
	if !stmt.BeginStmt.Immediate {
		t.Fatal("expected Immediate=true")
	}
}

func TestBeginExclusive(t *testing.T) {
	stmt := parseOne(t, "BEGIN EXCLUSIVE")
	if !stmt.BeginStmt.Exclusive {
		t.Fatal("expected Exclusive=true")
	}
}

func TestBeginTransaction(t *testing.T) {
	stmt := parseOne(t, "BEGIN TRANSACTION")
	if stmt.Type != StmtBegin {
		t.Fatalf("expected StmtBegin, got %d", stmt.Type)
	}
}

func TestCommit(t *testing.T) {
	stmt := parseOne(t, "COMMIT")
	if stmt.Type != StmtCommit {
		t.Fatalf("expected StmtCommit, got %d", stmt.Type)
	}
}

func TestEnd(t *testing.T) {
	// END is synonym for COMMIT
	stmt := parseOne(t, "END")
	if stmt.Type != StmtCommit {
		t.Fatalf("expected StmtCommit for END, got %d", stmt.Type)
	}
}

func TestRollback(t *testing.T) {
	stmt := parseOne(t, "ROLLBACK")
	if stmt.Type != StmtRollback {
		t.Fatalf("expected StmtRollback, got %d", stmt.Type)
	}
}

func TestRollbackToSavepoint(t *testing.T) {
	stmt := parseOne(t, "ROLLBACK TO SAVEPOINT sp1")
	if stmt.RollbackStmt.Savepoint != "sp1" {
		t.Fatalf("expected savepoint 'sp1', got %q", stmt.RollbackStmt.Savepoint)
	}
}

// =============================================================================
// Compound SELECT tests
// =============================================================================

func TestUnion(t *testing.T) {
	stmt := parseOne(t, "SELECT a FROM t1 UNION SELECT b FROM t2")
	sel := stmt.SelectStmt
	if len(sel.CompoundOps) != 1 || sel.CompoundOps[0] != CompoundUnion {
		t.Fatalf("expected UNION compound, got %v", sel.CompoundOps)
	}
}

func TestUnionAll(t *testing.T) {
	stmt := parseOne(t, "SELECT a FROM t1 UNION ALL SELECT b FROM t2")
	sel := stmt.SelectStmt
	if len(sel.CompoundOps) != 1 || sel.CompoundOps[0] != CompoundUnionAll {
		t.Fatalf("expected UNION ALL compound, got %v", sel.CompoundOps)
	}
}

func TestIntersect(t *testing.T) {
	stmt := parseOne(t, "SELECT a FROM t1 INTERSECT SELECT b FROM t2")
	sel := stmt.SelectStmt
	if len(sel.CompoundOps) != 1 || sel.CompoundOps[0] != CompoundIntersect {
		t.Fatalf("expected INTERSECT compound, got %v", sel.CompoundOps)
	}
}

func TestExcept(t *testing.T) {
	stmt := parseOne(t, "SELECT a FROM t1 EXCEPT SELECT b FROM t2")
	sel := stmt.SelectStmt
	if len(sel.CompoundOps) != 1 || sel.CompoundOps[0] != CompoundExcept {
		t.Fatalf("expected EXCEPT compound, got %v", sel.CompoundOps)
	}
}

// =============================================================================
// Multiple statements
// =============================================================================

func TestMultipleStatements(t *testing.T) {
	sql := "BEGIN; INSERT INTO t VALUES (1); COMMIT;"
	stmts := mustParse(t, sql)
	if len(stmts) != 3 {
		t.Fatalf("expected 3 statements, got %d", len(stmts))
	}
	if stmts[0].Type != StmtBegin {
		t.Fatalf("expected StmtBegin, got %d", stmts[0].Type)
	}
	if stmts[1].Type != StmtInsert {
		t.Fatalf("expected StmtInsert, got %d", stmts[1].Type)
	}
	if stmts[2].Type != StmtCommit {
		t.Fatalf("expected StmtCommit, got %d", stmts[2].Type)
	}
}

func TestMultipleStatementsNoTrailingSemicolon(t *testing.T) {
	sql := "SELECT 1; SELECT 2"
	stmts := mustParse(t, sql)
	if len(stmts) != 2 {
		t.Fatalf("expected 2 statements, got %d", len(stmts))
	}
}

func TestEmptyInput(t *testing.T) {
	stmts, err := Parse("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmts) != 0 {
		t.Fatalf("expected 0 statements, got %d", len(stmts))
	}
}

func TestOnlySemicolons(t *testing.T) {
	stmts, err := Parse(";;;")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(stmts) != 0 {
		t.Fatalf("expected 0 statements, got %d", len(stmts))
	}
}

// =============================================================================
// EXPLAIN
// =============================================================================

func TestExplain(t *testing.T) {
	stmt := parseOne(t, "EXPLAIN SELECT * FROM t")
	if !stmt.Explain {
		t.Fatal("expected Explain=true")
	}
}

func TestExplainQueryPlan(t *testing.T) {
	stmt := parseOne(t, "EXPLAIN QUERY PLAN SELECT * FROM t")
	if !stmt.Explain || !stmt.ExplainQuery {
		t.Fatal("expected Explain=true and ExplainQuery=true")
	}
}

// =============================================================================
// Qualified names
// =============================================================================

func TestQualifiedTableName(t *testing.T) {
	stmt := parseOne(t, "SELECT * FROM main.users")
	sel := stmt.SelectStmt
	if sel.From.Tables[0].Schema != "main" || sel.From.Tables[0].Name != "users" {
		t.Fatalf("expected main.users, got %s.%s", sel.From.Tables[0].Schema, sel.From.Tables[0].Name)
	}
}

func TestDropQualifiedTable(t *testing.T) {
	stmt := parseOne(t, "DROP TABLE main.users")
	dt := stmt.DropTable
	if dt.Schema != "main" || dt.Name != "users" {
		t.Fatalf("expected main.users, got %s.%s", dt.Schema, dt.Name)
	}
}

// =============================================================================
// Complex expressions
// =============================================================================

func TestNestedExpressions(t *testing.T) {
	sql := "SELECT (a + b) * (c - d) / (e + f) FROM t"
	parseOne(t, sql) // Just ensure no error
}

func TestComplexWhere(t *testing.T) {
	sql := "SELECT * FROM t WHERE (a > 0 OR b < 10) AND c = 'hello'"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	// Top level should be AND
	if sel.Where.Op != "AND" {
		t.Fatalf("expected AND at top of WHERE, got %q", sel.Where.Op)
	}
}

func TestSelectWithComments(t *testing.T) {
	sql := "SELECT /* comment */ a, -- line comment\nb FROM t"
	parseOne(t, sql) // Just ensure no error
}

func TestColumnWithKeywordName(t *testing.T) {
	// SQLite allows many keywords as column names
	sql := "SELECT order, action, status FROM t"
	parseOne(t, sql) // Just ensure no error
}

func TestSelectDbTypeColumn(t *testing.T) {
	sql := "SELECT db.table.col FROM t"
	stmt := parseOne(t, sql)
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprColumnRef {
		t.Fatalf("expected ExprColumnRef, got %d", expr.Kind)
	}
	if expr.Database != "db" || expr.Table != "table" || expr.Name != "col" {
		t.Fatalf("expected db.table.col, got %s.%s.%s", expr.Database, expr.Table, expr.Name)
	}
}

// =============================================================================
// USING joins
// =============================================================================

func TestSelectUsingJoin(t *testing.T) {
	sql := "SELECT * FROM a JOIN b USING (id, name)"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if len(sel.From.Tables[1].Using) != 2 {
		t.Fatalf("expected 2 USING columns, got %v", sel.From.Tables[1].Using)
	}
	if sel.From.Tables[1].Using[0] != "id" || sel.From.Tables[1].Using[1] != "name" {
		t.Fatalf("expected USING (id, name), got %v", sel.From.Tables[1].Using)
	}
}

// =============================================================================
// Cross join
// =============================================================================

func TestCrossJoin(t *testing.T) {
	sql := "SELECT * FROM a CROSS JOIN b"
	stmt := parseOne(t, sql)
	if stmt.SelectStmt.From.Tables[1].JoinType != JoinCross {
		t.Fatalf("expected CROSS join, got %d", stmt.SelectStmt.From.Tables[1].JoinType)
	}
}

// =============================================================================
// VALUES clause
// =============================================================================

func TestValuesClause(t *testing.T) {
	stmt := parseOne(t, "VALUES (1, 'a'), (2, 'b')")
	if stmt.Type != StmtSelect {
		t.Fatalf("expected StmtSelect, got %d", stmt.Type)
	}
	sel := stmt.SelectStmt
	if len(sel.Columns) != 2 {
		t.Fatalf("expected 2 result columns in first row, got %d", len(sel.Columns))
	}
	if len(sel.CompoundSelects) != 1 {
		t.Fatalf("expected 1 compound select (second row), got %d", len(sel.CompoundSelects))
	}
}

// =============================================================================
// Natural join
// =============================================================================

func TestNaturalJoin(t *testing.T) {
	sql := "SELECT * FROM a NATURAL JOIN b"
	stmt := parseOne(t, sql)
	if stmt.SelectStmt.From.Tables[1].JoinType != JoinNatural {
		t.Fatalf("expected NATURAL join, got %d", stmt.SelectStmt.From.Tables[1].JoinType)
	}
}

// =============================================================================
// Full join
// =============================================================================

func TestFullJoin(t *testing.T) {
	sql := "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id"
	stmt := parseOne(t, sql)
	if stmt.SelectStmt.From.Tables[1].JoinType != JoinFull {
		t.Fatalf("expected FULL join, got %d", stmt.SelectStmt.From.Tables[1].JoinType)
	}
}

// =============================================================================
// Multiple function args
// =============================================================================

func TestFunctionMultipleArgs(t *testing.T) {
	sql := "SELECT COALESCE(a, b, c, d) FROM t"
	stmt := parseOne(t, sql)
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.FunctionName != "COALESCE" || len(expr.Args) != 4 {
		t.Fatalf("expected COALESCE with 4 args, got %s with %d args", expr.FunctionName, len(expr.Args))
	}
}

// =============================================================================
// Select with table alias (bare)
// =============================================================================

func TestSelectTableAlias(t *testing.T) {
	sql := "SELECT u.id FROM users u WHERE u.id > 0"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if sel.From.Tables[0].Name != "users" {
		t.Fatalf("expected table 'users', got %q", sel.From.Tables[0].Name)
	}
	if sel.From.Tables[0].Alias != "u" {
		t.Fatalf("expected alias 'u', got %q", sel.From.Tables[0].Alias)
	}
}

// =============================================================================
// Complex select
// =============================================================================

func TestComplexSelect(t *testing.T) {
	sql := `SELECT
		u.name,
		COUNT(o.id) AS order_count,
		SUM(o.total) AS total_spent
	FROM users u
	LEFT JOIN orders o ON u.id = o.user_id
	WHERE u.active = 1
	GROUP BY u.name
	HAVING COUNT(o.id) > 0
	ORDER BY total_spent DESC
	LIMIT 10`
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if len(sel.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(sel.Columns))
	}
	if len(sel.From.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(sel.From.Tables))
	}
	if sel.From.Tables[1].JoinType != JoinLeft {
		t.Fatalf("expected LEFT join")
	}
	if sel.Where == nil || sel.Having == nil {
		t.Fatal("expected WHERE and HAVING")
	}
	if len(sel.GroupBy) != 1 {
		t.Fatalf("expected 1 GROUP BY expr, got %d", len(sel.GroupBy))
	}
	if len(sel.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY item, got %d", len(sel.OrderBy))
	}
	if sel.OrderBy[0].Order != SortDesc {
		t.Fatal("expected DESC order")
	}
	if sel.Limit == nil {
		t.Fatal("expected LIMIT")
	}
}

// =============================================================================
// CREATE TABLE with various types
// =============================================================================

func TestCreateTableWithTypes(t *testing.T) {
	sql := `CREATE TABLE t (
		a INTEGER,
		b TEXT,
		c REAL,
		d BLOB,
		e VARCHAR(255),
		f NUMERIC(10,2)
	)`
	stmt := parseOne(t, sql)
	ct := stmt.CreateTable
	if len(ct.Columns) != 6 {
		t.Fatalf("expected 6 columns, got %d", len(ct.Columns))
	}
	// Check type names
	expectedTypes := []string{"INTEGER", "TEXT", "REAL", "BLOB", "VARCHAR(255)", "NUMERIC(10,2)"}
	for i, expected := range expectedTypes {
		// Normalize comparison
		got := strings.ToUpper(ct.Columns[i].Type)
		exp := strings.ToUpper(expected)
		if !strings.HasPrefix(got, exp[:min(len(exp), 4)]) {
			t.Errorf("column %d: expected type matching %q, got %q", i, expected, ct.Columns[i].Type)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// =============================================================================
// UPDATE with FROM
// =============================================================================

func TestUpdateFrom(t *testing.T) {
	sql := "UPDATE orders SET total = new_total FROM staging WHERE orders.id = staging.id"
	stmt := parseOne(t, sql)
	upd := stmt.UpdateStmt
	if upd.From == nil {
		t.Fatal("expected FROM clause in UPDATE")
	}
}

// =============================================================================
// Error cases
// =============================================================================

func TestErrorInvalidSQL(t *testing.T) {
	expectError(t, "INVALID SQL STATEMENT HERE")
}

// =============================================================================
// Select limit comma offset
// =============================================================================

func TestSelectLimitCommaOffset(t *testing.T) {
	// LIMIT offset, count (SQLite syntax)
	stmt := parseOne(t, "SELECT * FROM t LIMIT 10, 20")
	sel := stmt.SelectStmt
	if sel.Limit == nil || sel.Offset == nil {
		t.Fatal("expected LIMIT and OFFSET")
	}
}

// =============================================================================
// Join with comma
// =============================================================================

func TestCommaJoin(t *testing.T) {
	sql := "SELECT * FROM a, b WHERE a.id = b.id"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if len(sel.From.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(sel.From.Tables))
	}
	// Second table should have inner join type (comma join)
	if sel.From.Tables[1].JoinType != JoinInner {
		t.Fatalf("expected JoinInner for comma join, got %d", sel.From.Tables[1].JoinType)
	}
}

// =============================================================================
// Empty function args
// =============================================================================

func TestEmptyFunctionArgs(t *testing.T) {
	sql := "SELECT random() FROM t"
	stmt := parseOne(t, sql)
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.FunctionName != "random" {
		t.Fatalf("expected 'random' function, got %q", expr.FunctionName)
	}
	if len(expr.Args) != 0 {
		t.Fatalf("expected 0 args, got %d", len(expr.Args))
	}
}

// =============================================================================
// Insert with qualified table
// =============================================================================

func TestInsertQualifiedTable(t *testing.T) {
	stmt := parseOne(t, "INSERT INTO main.users VALUES (1)")
	ins := stmt.InsertStmt
	if ins.Table.Schema != "main" || ins.Table.Name != "users" {
		t.Fatalf("expected main.users, got %s.%s", ins.Table.Schema, ins.Table.Name)
	}
}

// =============================================================================
// Subquery in WHERE
// =============================================================================

func TestSubqueryInWhere(t *testing.T) {
	sql := "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if sel.Where == nil {
		t.Fatal("expected WHERE")
	}
	if sel.Where.Kind != ExprInSelect {
		t.Fatalf("expected ExprInSelect, got %d", sel.Where.Kind)
	}
}

// =============================================================================
// Nested function calls
// =============================================================================

func TestNestedFunctions(t *testing.T) {
	sql := "SELECT UPPER(SUBSTR(name, 1, 5)) FROM t"
	parseOne(t, sql) // Just ensure no error
}

// =============================================================================
// IS DISTINCT FROM
// =============================================================================

func TestIsDistinctFrom(t *testing.T) {
	sql := "SELECT a IS DISTINCT FROM b FROM t"
	stmt := parseOne(t, sql)
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Op != "IS NOT" {
		t.Fatalf("expected IS NOT (for IS DISTINCT FROM), got %q", expr.Op)
	}
}

func TestIsNotDistinctFrom(t *testing.T) {
	sql := "SELECT a IS NOT DISTINCT FROM b FROM t"
	stmt := parseOne(t, sql)
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Op != "IS" {
		t.Fatalf("expected IS (for IS NOT DISTINCT FROM), got %q", expr.Op)
	}
}

// =============================================================================
// Case-insensitive keywords
// =============================================================================

func TestCaseInsensitiveKeywords(t *testing.T) {
	stmt := parseOne(t, "select * from users")
	if stmt.Type != StmtSelect {
		t.Fatalf("expected StmtSelect for lowercase, got %d", stmt.Type)
	}
}

// =============================================================================
// Insert with DEFAULT keyword value
// =============================================================================

func TestInsertWithExpressions(t *testing.T) {
	sql := "INSERT INTO t VALUES (1 + 2, UPPER('hello'), NULL)"
	stmt := parseOne(t, sql)
	ins := stmt.InsertStmt
	if len(ins.Values) != 1 {
		t.Fatalf("expected 1 row, got %d", len(ins.Values))
	}
	if len(ins.Values[0]) != 3 {
		t.Fatalf("expected 3 values in row, got %d", len(ins.Values[0]))
	}
}

// =============================================================================
// RETURNING clause tests
// =============================================================================

func TestInsertReturning(t *testing.T) {
	stmt := parseOne(t, "INSERT INTO users (id, name) VALUES (1, 'Alice') RETURNING id, name")
	ins := stmt.InsertStmt
	if len(ins.Returning) != 2 {
		t.Fatalf("expected 2 RETURNING columns, got %d", len(ins.Returning))
	}
}

func TestInsertReturningStar(t *testing.T) {
	stmt := parseOne(t, "INSERT INTO users VALUES (1, 'Alice') RETURNING *")
	ins := stmt.InsertStmt
	if len(ins.Returning) != 1 || !ins.Returning[0].Star {
		t.Fatalf("expected RETURNING *, got %v", ins.Returning)
	}
}

func TestUpdateReturning(t *testing.T) {
	stmt := parseOne(t, "UPDATE users SET name = 'Bob' WHERE id = 1 RETURNING id, name AS new_name")
	upd := stmt.UpdateStmt
	if len(upd.Returning) != 2 {
		t.Fatalf("expected 2 RETURNING columns, got %d", len(upd.Returning))
	}
	if upd.Returning[1].As != "new_name" {
		t.Fatalf("expected alias 'new_name', got %q", upd.Returning[1].As)
	}
}

func TestDeleteReturning(t *testing.T) {
	stmt := parseOne(t, "DELETE FROM users WHERE id = 1 RETURNING *")
	del := stmt.DeleteStmt
	if len(del.Returning) != 1 || !del.Returning[0].Star {
		t.Fatalf("expected RETURNING *, got %v", del.Returning)
	}
}

func TestDeleteReturningColumns(t *testing.T) {
	stmt := parseOne(t, "DELETE FROM users WHERE id = 1 RETURNING id, name")
	del := stmt.DeleteStmt
	if len(del.Returning) != 2 {
		t.Fatalf("expected 2 RETURNING columns, got %d", len(del.Returning))
	}
}

// =============================================================================
// CTE (WITH clause) tests
// =============================================================================

func TestWithCTE(t *testing.T) {
	sql := "WITH cte AS (SELECT 1) SELECT * FROM cte"
	stmt := parseOne(t, sql)
	if len(stmt.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(stmt.CTEs))
	}
	if stmt.CTEs[0].Name != "cte" {
		t.Fatalf("expected CTE name 'cte', got %q", stmt.CTEs[0].Name)
	}
	if stmt.CTEs[0].Body == nil {
		t.Fatal("expected CTE body")
	}
}

func TestWithRecursiveCTE(t *testing.T) {
	sql := "WITH RECURSIVE cte(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM cte WHERE x<10) SELECT * FROM cte"
	stmt := parseOne(t, sql)
	if !stmt.Recursive {
		t.Fatal("expected Recursive=true")
	}
	if len(stmt.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(stmt.CTEs))
	}
	if len(stmt.CTEs[0].Columns) != 1 || stmt.CTEs[0].Columns[0] != "x" {
		t.Fatalf("expected CTE column list [x], got %v", stmt.CTEs[0].Columns)
	}
}

func TestWithMultipleCTEs(t *testing.T) {
	sql := "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b"
	stmt := parseOne(t, sql)
	if len(stmt.CTEs) != 2 {
		t.Fatalf("expected 2 CTEs, got %d", len(stmt.CTEs))
	}
	if stmt.CTEs[0].Name != "a" {
		t.Fatalf("expected first CTE 'a', got %q", stmt.CTEs[0].Name)
	}
	if stmt.CTEs[1].Name != "b" {
		t.Fatalf("expected second CTE 'b', got %q", stmt.CTEs[1].Name)
	}
}

// =============================================================================
// DELETE with ORDER BY and LIMIT
// =============================================================================

func TestDeleteOrderByLimit(t *testing.T) {
	stmt := parseOne(t, "DELETE FROM users ORDER BY id DESC LIMIT 10")
	del := stmt.DeleteStmt
	if len(del.Order) != 1 {
		t.Fatalf("expected 1 ORDER BY item, got %d", len(del.Order))
	}
	if del.Order[0].Order != SortDesc {
		t.Fatal("expected DESC order")
	}
	if del.Limit == nil {
		t.Fatal("expected LIMIT")
	}
}

// =============================================================================
// CAST expression with various types
// =============================================================================

func TestCastAsReal(t *testing.T) {
	stmt := parseOne(t, "SELECT CAST(x AS REAL)")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprCast {
		t.Fatalf("expected ExprCast, got %d", expr.Kind)
	}
	if expr.CastType != "REAL" {
		t.Fatalf("expected cast type REAL, got %q", expr.CastType)
	}
}

func TestCastAsText(t *testing.T) {
	stmt := parseOne(t, "SELECT CAST(123 AS TEXT)")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprCast {
		t.Fatalf("expected ExprCast, got %d", expr.Kind)
	}
	if expr.CastType != "TEXT" {
		t.Fatalf("expected cast type TEXT, got %q", expr.CastType)
	}
}

// =============================================================================
// BETWEEN with expressions
// =============================================================================

func TestBetweenWithExpressions(t *testing.T) {
	stmt := parseOne(t, "SELECT a BETWEEN b AND c FROM t")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprBetween {
		t.Fatalf("expected ExprBetween, got %d", expr.Kind)
	}
	if expr.Low == nil || expr.Low.Name != "b" {
		t.Fatal("expected lower bound 'b'")
	}
	if expr.High == nil || expr.High.Name != "c" {
		t.Fatal("expected upper bound 'c'")
	}
}

// =============================================================================
// IN with subquery
// =============================================================================

func TestInSubquery(t *testing.T) {
	sql := "SELECT * FROM t WHERE id IN (SELECT id FROM other)"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
	if sel.Where.Kind != ExprInSelect {
		t.Fatalf("expected ExprInSelect, got %d", sel.Where.Kind)
	}
	if sel.Where.InSelect == nil {
		t.Fatal("expected subquery in IN")
	}
}

// =============================================================================
// CASE with no ELSE
// =============================================================================

func TestCaseNoElse(t *testing.T) {
	sql := "SELECT CASE WHEN a > 0 THEN 'pos' END FROM t"
	stmt := parseOne(t, sql)
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprCase {
		t.Fatalf("expected ExprCase, got %d", expr.Kind)
	}
	if len(expr.WhenList) != 1 {
		t.Fatalf("expected 1 WHEN clause, got %d", len(expr.WhenList))
	}
	if expr.ElseExpr != nil {
		t.Fatal("expected no ELSE")
	}
}

// =============================================================================
// EXISTS in WHERE
// =============================================================================

func TestExistsInWhere(t *testing.T) {
	sql := "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM other WHERE other.id = t.id)"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if sel.Where == nil {
		t.Fatal("expected WHERE")
	}
	if sel.Where.Kind != ExprExists {
		t.Fatalf("expected ExprExists, got %d", sel.Where.Kind)
	}
}

// =============================================================================
// Nested CASE
// =============================================================================

func TestNestedCase(t *testing.T) {
	sql := "SELECT CASE WHEN a > 0 THEN CASE WHEN a > 10 THEN 'big' ELSE 'small' END ELSE 'neg' END FROM t"
	parseOne(t, sql) // just ensure no error
}

// =============================================================================
// Complex IN expressions
// =============================================================================

func TestInWithExpressions(t *testing.T) {
	sql := "SELECT * FROM t WHERE a IN (1+2, 3*4, ABS(-5))"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if sel.Where.Kind != ExprInList {
		t.Fatalf("expected ExprInList, got %d", sel.Where.Kind)
	}
	if len(sel.Where.InValues) != 3 {
		t.Fatalf("expected 3 IN values, got %d", len(sel.Where.InValues))
	}
}

// =============================================================================
// NOT EXISTS
// =============================================================================

func TestNotExistsInWhere(t *testing.T) {
	sql := "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM other)"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if sel.Where == nil {
		t.Fatal("expected WHERE")
	}
	if sel.Where.Kind != ExprUnaryOp || sel.Where.Op != "NOT" {
		t.Fatalf("expected NOT unary op, got %v", sel.Where)
	}
	if sel.Where.Right == nil || sel.Where.Right.Kind != ExprExists {
		t.Fatal("expected EXISTS under NOT")
	}
}

// =============================================================================
// REGEXP
// =============================================================================

func TestExprRegexp(t *testing.T) {
	stmt := parseOne(t, "SELECT a REGEXP '^[0-9]+$'")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprRegexp {
		t.Fatalf("expected ExprRegexp, got %d", expr.Kind)
	}
}

func TestExprNotRegexp(t *testing.T) {
	stmt := parseOne(t, "SELECT a NOT REGEXP '^[0-9]+$'")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprRegexp || !expr.Not {
		t.Fatalf("expected ExprRegexp with Not=true, got kind=%d not=%v", expr.Kind, expr.Not)
	}
}

// =============================================================================
// MATCH expression
// =============================================================================

func TestExprMatch(t *testing.T) {
	stmt := parseOne(t, "SELECT a MATCH 'pattern'")
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprMatch {
		t.Fatalf("expected ExprMatch, got %d", expr.Kind)
	}
}

// =============================================================================
// Error message with location
// =============================================================================

func TestErrorHasLocation(t *testing.T) {
	_, err := Parse("SELECT * FROM t WHERE a =")
	if err == nil {
		t.Fatal("expected error")
	}
	// Error message should include "parse error"
	errStr := err.Error()
	if !strings.Contains(errStr, "parse error") {
		t.Errorf("expected error to contain 'parse error', got: %s", errStr)
	}
}

// =============================================================================
// Complex WITH queries
// =============================================================================

func TestWithInsert(t *testing.T) {
	sql := "WITH cte AS (SELECT 1 AS id) INSERT INTO t SELECT * FROM cte"
	stmt := parseOne(t, sql)
	if len(stmt.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(stmt.CTEs))
	}
	if stmt.Type != StmtInsert {
		t.Fatalf("expected StmtInsert, got %d", stmt.Type)
	}
}

func TestWithUpdate(t *testing.T) {
	sql := "WITH cte AS (SELECT 1) UPDATE t SET a = 1"
	stmt := parseOne(t, sql)
	if len(stmt.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(stmt.CTEs))
	}
	if stmt.Type != StmtUpdate {
		t.Fatalf("expected StmtUpdate, got %d", stmt.Type)
	}
}

func TestWithDelete(t *testing.T) {
	sql := "WITH cte AS (SELECT 1) DELETE FROM t"
	stmt := parseOne(t, sql)
	if len(stmt.CTEs) != 1 {
		t.Fatalf("expected 1 CTE, got %d", len(stmt.CTEs))
	}
	if stmt.Type != StmtDelete {
		t.Fatalf("expected StmtDelete, got %d", stmt.Type)
	}
}

// =============================================================================
// Window function parsing (basic)
// =============================================================================

func TestWindowFunctionOver(t *testing.T) {
	sql := "SELECT ROW_NUMBER() OVER (ORDER BY id) FROM t"
	parseOne(t, sql) // just ensure no error
}

func TestWindowFunctionPartitionBy(t *testing.T) {
	sql := "SELECT COUNT(*) OVER (PARTITION BY dept ORDER BY name) FROM t"
	parseOne(t, sql) // just ensure no error
}

func TestWindowFunctionNamedWindow(t *testing.T) {
	sql := "SELECT COUNT(*) OVER w FROM t WINDOW w AS (ORDER BY id)"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	// The WINDOW clause should be parsed and stored
	if len(sel.Windows) != 1 {
		t.Fatalf("expected 1 window definition, got %d", len(sel.Windows))
	}
	if sel.Windows[0].Name != "w" {
		t.Fatalf("expected window name 'w', got %q", sel.Windows[0].Name)
	}
	if len(sel.Windows[0].OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY in window, got %d", len(sel.Windows[0].OrderBy))
	}
}

// =============================================================================
// Schema management tests
// =============================================================================

func TestSchemaAddTable(t *testing.T) {
	s := NewSchema()
	tbl := &TableInfo{
		Name:     "test",
		RootPage: 5,
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "TEXT"},
		},
	}
	s.AddTable(tbl)
	found, err := newBuild(s).lookupTable("test")
	if err != nil {
		t.Fatal(err)
	}
	if found.Name != "test" {
		t.Fatalf("expected 'test', got %q", found.Name)
	}
}

func TestSchemaAddIndex(t *testing.T) {
	s := NewSchema()
	idx := &IndexInfo{
		Name:     "idx_test",
		Table:    "test",
		RootPage: 6,
	}
	s.AddIndex(idx)
	found, err := newBuild(s).lookupIndex("idx_test")
	if err != nil {
		t.Fatal(err)
	}
	if found.Name != "idx_test" {
		t.Fatalf("expected 'idx_test', got %q", found.Name)
	}
}

func TestTableInfoFindColumn(t *testing.T) {
	tbl := &TableInfo{
		Columns: []ColumnInfo{
			{Name: "id"},
			{Name: "name"},
			{Name: "email"},
		},
	}
	if idx := tbl.FindColumn("id"); idx != 0 {
		t.Errorf("expected 0 for 'id', got %d", idx)
	}
	if idx := tbl.FindColumn("NAME"); idx != 1 {
		t.Errorf("expected 1 for 'NAME', got %d", idx)
	}
	if idx := tbl.FindColumn("missing"); idx != -1 {
		t.Errorf("expected -1 for 'missing', got %d", idx)
	}
}

func TestTableInfoColumnCount(t *testing.T) {
	tbl := &TableInfo{
		Columns: []ColumnInfo{
			{Name: "a"}, {Name: "b"}, {Name: "c"},
		},
	}
	if cnt := tbl.ColumnCount(); cnt != 3 {
		t.Errorf("expected 3, got %d", cnt)
	}
	if cnt := (*TableInfo)(nil).ColumnCount(); cnt != 0 {
		t.Errorf("expected 0 for nil, got %d", cnt)
	}
}

// =============================================================================
// CAST compilation test
// =============================================================================

func TestCompileCastExpression(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{
		Kind:     ExprCast,
		Left:     &Expr{Kind: ExprLiteral, LiteralType: "string", StringValue: "123"},
		CastType: "INTEGER",
	}
	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	hasCast := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpCast {
			hasCast = true
		}
	}
	if !hasCast {
		t.Fatal("expected Cast instruction")
	}
}

// =============================================================================
// IS NULL fix - operand in Right field
// =============================================================================

func TestCompileIsNullWithRight(t *testing.T) {
	bld := newBuild(helperSchema())
	bld.addTableRef("users", "users", bld.schema.Tables["users"], 0)
	reg := bld.b.AllocReg(1)
	// Parser puts operand in Right for IS NULL
	expr := &Expr{
		Kind:  ExprIsNull,
		Right: &Expr{Kind: ExprColumnRef, Name: "name"},
	}
	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	hasIsNull := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpIsNull {
			hasIsNull = true
		}
	}
	if !hasIsNull {
		t.Fatal("expected IsNull instruction")
	}
}

// =============================================================================
// Edge case: BETWEEN in WHERE with outer AND
// =============================================================================

func TestBetweenInWhereOuterAnd(t *testing.T) {
	sql := "SELECT * FROM t WHERE a BETWEEN 1 AND 100 AND b > 0"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if sel.Where == nil {
		t.Fatal("expected WHERE")
	}
	// Top should be AND (BETWEEN ... AND ... should not consume the outer AND)
	if sel.Where.Op != "AND" {
		t.Fatalf("expected AND at top, got %q", sel.Where.Op)
	}
}

// =============================================================================
// Edge case: multiple BETWEEN
// =============================================================================

func TestMultipleBetween(t *testing.T) {
	sql := "SELECT * FROM t WHERE a BETWEEN 1 AND 10 AND b BETWEEN 20 AND 30"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if sel.Where == nil {
		t.Fatal("expected WHERE")
	}
	if sel.Where.Op != "AND" {
		t.Fatalf("expected AND at top, got %q", sel.Where.Op)
	}
}

// =============================================================================
// Edge case: CAST in WHERE
// =============================================================================

func TestCastInWhere(t *testing.T) {
	sql := "SELECT * FROM t WHERE CAST(a AS INTEGER) > 5"
	parseOne(t, sql) // just ensure no error
}

// =============================================================================
// Edge case: CASE in WHERE
// =============================================================================

func TestCaseInWhere(t *testing.T) {
	sql := "SELECT * FROM t WHERE CASE WHEN a > 0 THEN 1 ELSE 0 END = 1"
	parseOne(t, sql) // just ensure no error
}

// =============================================================================
// Edge case: double NOT
// =============================================================================

func TestDoubleNot(t *testing.T) {
	sql := "SELECT NOT NOT a FROM t"
	stmt := parseOne(t, sql)
	expr := stmt.SelectStmt.Columns[0].Expr
	if expr.Kind != ExprUnaryOp || expr.Op != "NOT" {
		t.Fatalf("expected outer NOT, got %v", expr)
	}
	if expr.Right == nil || expr.Right.Op != "NOT" {
		t.Fatal("expected inner NOT")
	}
}

// =============================================================================
// Edge case: nested IN expressions
// =============================================================================

func TestNestedInExpression(t *testing.T) {
	sql := "SELECT * FROM t WHERE a IN (1, 2) AND b IN (SELECT x FROM other)"
	parseOne(t, sql) // just ensure no error
}

// =============================================================================
// SELECT without FROM (complex constant expressions)
// =============================================================================

func TestSelectNoFromComplex(t *testing.T) {
	sql := "SELECT 1 + 2 * 3, UPPER('hello'), CAST(3.14 AS INTEGER)"
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if len(sel.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(sel.Columns))
	}
}

// =============================================================================
// UPDATE with FROM (complex)
// =============================================================================

func TestUpdateFromComplex(t *testing.T) {
	sql := "UPDATE orders SET total = s.total FROM staging s WHERE orders.id = s.id"
	stmt := parseOne(t, sql)
	upd := stmt.UpdateStmt
	if upd.From == nil {
		t.Fatal("expected FROM clause")
	}
	if len(upd.From.Tables) != 1 {
		t.Fatalf("expected 1 table in FROM, got %d", len(upd.From.Tables))
	}
	if upd.From.Tables[0].Alias != "s" {
		t.Fatalf("expected alias 's', got %q", upd.From.Tables[0].Alias)
	}
}

// =============================================================================
// SELECT with quoted identifiers
// =============================================================================

func TestQuotedIdentifiers(t *testing.T) {
	sql := `SELECT "user"."id", "user"."name" FROM "user"`
	stmt := parseOne(t, sql)
	sel := stmt.SelectStmt
	if sel.From.Tables[0].Name != "user" {
		t.Fatalf("expected table 'user', got %q", sel.From.Tables[0].Name)
	}
}

// =============================================================================
// INSERT with RETURNING expression alias
// =============================================================================

func TestInsertReturningExpr(t *testing.T) {
	sql := "INSERT INTO t VALUES (1) RETURNING id, UPPER(name) AS upper_name"
	stmt := parseOne(t, sql)
	ins := stmt.InsertStmt
	if len(ins.Returning) != 2 {
		t.Fatalf("expected 2 RETURNING columns, got %d", len(ins.Returning))
	}
	if ins.Returning[1].As != "upper_name" {
		t.Fatalf("expected alias 'upper_name', got %q", ins.Returning[1].As)
	}
}

// =============================================================================
// INSERT with DEFAULT keyword value
// =============================================================================

func TestInsertDefaultKeyword(t *testing.T) {
	sql := "INSERT INTO t (a, b) VALUES (1, DEFAULT)"
	stmt := parseOne(t, sql)
	ins := stmt.InsertStmt
	if len(ins.Values) != 1 {
		t.Fatalf("expected 1 row, got %d", len(ins.Values))
	}
	if len(ins.Values[0]) != 2 {
		t.Fatalf("expected 2 values, got %d", len(ins.Values[0]))
	}
}
