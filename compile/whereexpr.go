package compile

import (
	"strings"
)

// wo flags indicate what kind of operator a whereTerm uses.
const (
	woEq      = 1 << iota // =
	woIn                   // IN
	woIsNull               // IS NULL
	woLT                   // <
	woLE                   // <=
	woGT                   // >
	woGE                   // >=
	woBetween              // BETWEEN
)

// whereTerm represents a single condition extracted from a WHERE clause.
type whereTerm struct {
	Expr     *Expr // Original expression
	TableIdx int   // Which table (-1 if none)
	ColIdx   int   // Column in the table (-1 if none)
	OpFlags  int   // woEq, woLT, etc.

	// Equality: right-hand side expression
	EqExpr *Expr
	// Range: bound expression
	RangeExpr *Expr
	// BETWEEN: low and high
	LowExpr  *Expr
	HighExpr *Expr
	// IN: the IN expression
	InExpr *Expr

	Consumed bool // Already handled by an index plan
}

// whereClause holds all terms from a WHERE expression.
type whereClause struct {
	Terms    []*whereTerm
	Original *Expr
}

// analyzeWhere decomposes a WHERE expression into individual terms.
func analyzeWhere(expr *Expr, tables []*tableEntry) *whereClause {
	if expr == nil {
		return &whereClause{}
	}
	wc := &whereClause{Original: expr}
	for _, t := range flattenAnd(expr) {
		wt := analyzeTerm(t, tables)
		if wt != nil {
			wc.Terms = append(wc.Terms, wt)
		}
	}
	return wc
}

// flattenAnd flattens an AND tree into a list of non-AND expressions.
func flattenAnd(expr *Expr) []*Expr {
	if expr == nil {
		return nil
	}
	if expr.Kind == ExprBinaryOp && strings.ToUpper(expr.Op) == "AND" {
		return append(flattenAnd(expr.Left), flattenAnd(expr.Right)...)
	}
	return []*Expr{expr}
}

// analyzeTerm analyzes a single non-AND expression.
func analyzeTerm(expr *Expr, tables []*tableEntry) *whereTerm {
	if expr == nil {
		return nil
	}
	switch expr.Kind {
	case ExprBinaryOp:
		return analyzeBinaryTerm(expr, tables)
	case ExprBetween:
		return analyzeBetweenTerm(expr, tables)
	case ExprInList, ExprInSelect:
		return analyzeInTerm(expr, tables)
	case ExprIsNull, ExprIsNotNull:
		return analyzeNullTerm(expr, tables)
	}
	return &whereTerm{Expr: expr, TableIdx: -1, ColIdx: -1}
}

// analyzeBinaryTerm handles comparison operators.
func analyzeBinaryTerm(expr *Expr, tables []*tableEntry) *whereTerm {
	op := strings.ToUpper(expr.Op)
	var opFlags int
	switch op {
	case "=", "==":
		opFlags = woEq
	case "<":
		opFlags = woLT
	case "<=":
		opFlags = woLE
	case ">":
		opFlags = woGT
	case ">=":
		opFlags = woGE
	case "IS":
		opFlags = woEq
	default:
		return &whereTerm{Expr: expr, TableIdx: -1, ColIdx: -1}
	}

	// Try column OP value
	lt, lc := findColRef(expr.Left, tables)
	rt, rc := findColRef(expr.Right, tables)

	if lt >= 0 && lc >= 0 && !exprUsesTable(expr.Right, lt, tables) {
		wt := &whereTerm{Expr: expr, TableIdx: lt, ColIdx: lc, OpFlags: opFlags}
		if opFlags == woEq {
			wt.EqExpr = expr.Right
		} else {
			wt.RangeExpr = expr.Right
		}
		return wt
	}

	// value OP column → flip
	if rt >= 0 && rc >= 0 && !exprUsesTable(expr.Left, rt, tables) {
		flipped := flipOp(opFlags)
		wt := &whereTerm{Expr: expr, TableIdx: rt, ColIdx: rc, OpFlags: flipped}
		if flipped == woEq {
			wt.EqExpr = expr.Left
		} else {
			wt.RangeExpr = expr.Left
		}
		return wt
	}

	return &whereTerm{Expr: expr, TableIdx: -1, ColIdx: -1}
}

// analyzeBetweenTerm handles BETWEEN expressions.
func analyzeBetweenTerm(expr *Expr, tables []*tableEntry) *whereTerm {
	if expr.Low == nil || expr.High == nil {
		return &whereTerm{Expr: expr, TableIdx: -1, ColIdx: -1}
	}
	tIdx, cIdx := findColRef(expr.Left, tables)
	if tIdx < 0 {
		return &whereTerm{Expr: expr, TableIdx: -1, ColIdx: -1}
	}
	return &whereTerm{
		Expr: expr, TableIdx: tIdx, ColIdx: cIdx,
		OpFlags: woBetween, LowExpr: expr.Low, HighExpr: expr.High,
	}
}

// analyzeInTerm handles IN expressions.
func analyzeInTerm(expr *Expr, tables []*tableEntry) *whereTerm {
	tIdx, cIdx := findColRef(expr.Left, tables)
	if tIdx < 0 {
		return &whereTerm{Expr: expr, TableIdx: -1, ColIdx: -1}
	}
	return &whereTerm{
		Expr: expr, TableIdx: tIdx, ColIdx: cIdx,
		OpFlags: woIn, InExpr: expr,
	}
}

// analyzeNullTerm handles IS NULL / IS NOT NULL.
func analyzeNullTerm(expr *Expr, tables []*tableEntry) *whereTerm {
	operand := expr.Left
	if operand == nil {
		operand = expr.Right
	}
	tIdx, cIdx := findColRef(operand, tables)
	if tIdx < 0 {
		return &whereTerm{Expr: expr, TableIdx: -1, ColIdx: -1}
	}
	return &whereTerm{Expr: expr, TableIdx: tIdx, ColIdx: cIdx}
}

// findColRef checks if an expression is a simple column reference
// and returns (table_index, column_index) or (-1, -1).
func findColRef(expr *Expr, tables []*tableEntry) (int, int) {
	if expr == nil || expr.Kind != ExprColumnRef {
		return -1, -1
	}
	if expr.Table != "" {
		tUpper := strings.ToUpper(expr.Table)
		for i, t := range tables {
			if strings.ToUpper(t.alias) == tUpper || strings.ToUpper(t.name) == tUpper {
				colIdx := t.table.FindColumn(expr.Name)
				if colIdx >= 0 {
					return i, colIdx
				}
			}
		}
		return -1, -1
	}
	colUpper := strings.ToUpper(expr.Name)
	for i, t := range tables {
		for j, c := range t.table.Columns {
			if strings.ToUpper(c.Name) == colUpper {
				return i, j
			}
		}
	}
	return -1, -1
}

// exprUsesTable checks if an expression references any column from the given table.
func exprUsesTable(expr *Expr, tableIdx int, tables []*tableEntry) bool {
	if expr == nil {
		return false
	}
	ti, _ := findColRef(expr, tables)
	if ti == tableIdx {
		return true
	}
	return exprUsesTable(expr.Left, tableIdx, tables) ||
		exprUsesTable(expr.Right, tableIdx, tables) ||
		exprUsesTable(expr.Low, tableIdx, tables) ||
		exprUsesTable(expr.High, tableIdx, tables) ||
		exprUsesTable(expr.Pattern, tableIdx, tables)
}

// flipOp flips comparison direction (e.g., < becomes >).
func flipOp(flags int) int {
	switch flags {
	case woLT:
		return woGT
	case woLE:
		return woGE
	case woGT:
		return woLT
	case woGE:
		return woLE
	case woEq:
		return woEq
	default:
		return flags
	}
}

// Helper predicates on whereTerm.
func (t *whereTerm) hasEq() bool      { return t.OpFlags&woEq != 0 }
func (t *whereTerm) hasRange() bool    { return t.OpFlags&(woLT|woLE|woGT|woGE) != 0 }
func (t *whereTerm) hasBetween() bool  { return t.OpFlags&woBetween != 0 }
func (t *whereTerm) hasIn() bool       { return t.OpFlags&woIn != 0 }
func (t *whereTerm) isLower() bool     { return t.OpFlags&(woGT|woGE) != 0 }
func (t *whereTerm) isUpper() bool     { return t.OpFlags&(woLT|woLE) != 0 }
func (t *whereTerm) isInclusive() bool { return t.OpFlags&(woGE|woLE) != 0 }

// indexableTermsForTable returns terms with a valid column ref on the given table.
func (wc *whereClause) indexableTermsForTable(tableIdx int) []*whereTerm {
	var result []*whereTerm
	for _, t := range wc.Terms {
		if t.TableIdx == tableIdx && t.ColIdx >= 0 && !t.Consumed {
			result = append(result, t)
		}
	}
	return result
}

// indexesForTable returns all indexes for the given table from the schema.
func indexesForTable(schema *Schema, tableName string) []*IndexInfo {
	if schema == nil {
		return nil
	}
	var result []*IndexInfo
	for _, idx := range schema.Indexes {
		if strings.EqualFold(idx.Table, tableName) {
			result = append(result, idx)
		}
	}
	return result
}
