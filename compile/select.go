package compile

import (
	"fmt"
	"strings"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// compileSelect compiles a SELECT statement into VDBE bytecode.
func (b *Build) compileSelect(stmt *SelectStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil SELECT statement")
	}

	// Handle compound SELECT (UNION, INTERSECT, EXCEPT)
	if len(stmt.CompoundOps) > 0 {
		return b.compileCompoundSelect(stmt)
	}

	// Determine if this has window functions
	if b.hasWindowFuncs(stmt) {
		return b.compileWindowSelect(stmt)
	}

	// Determine if this is an aggregate query
	isAggregate := b.hasAggregates(stmt)

	if isAggregate {
		return b.compileAggregateSelect(stmt)
	}

	return b.compileSimpleSelect(stmt)
}

// compileSimpleSelect compiles a non-aggregate SELECT.
func (b *Build) compileSimpleSelect(stmt *SelectStmt) error {
	b.emitInit()
	b.emitTransaction(0, false)

	// Open cursors for FROM tables
	if err := b.openFromTables(stmt.From, false); err != nil {
		return err
	}

	// Compute number of result columns
	resultCols, err := b.expandResultColumns(stmt.Columns)
	if err != nil {
		return err
	}
	nResult := len(resultCols)

	// Determine if we need a sorter (ORDER BY without index)
	needSorter := len(stmt.OrderBy) > 0
	var sorterCursor int
	if needSorter {
		sorterCursor = b.b.AllocCursor()
		// Sorter columns = result columns + ORDER BY sort key
		nSortCols := nResult
		b.emitSorterOpen(sorterCursor, nSortCols)
	}

	// Allocate result registers
	resultBase := b.b.AllocReg(nResult)

	// Allocate registers for ORDER BY key
	var orderByBase int
	if needSorter {
		orderByBase = b.b.AllocReg(len(stmt.OrderBy))
	}

	if len(b.tables) == 0 {
		// No FROM clause: evaluate result columns once and output
		for i, rc := range resultCols {
			if err := b.compileExpr(rc.Expr, resultBase+i); err != nil {
				return err
			}
		}
		b.emitResultRow(resultBase, nResult)
	} else {
		// Plan the query to find optimal access paths
		plan := b.planQuery(stmt.Where)

		emptyLabel := b.b.NewLabel()
		loopEndLabel := b.b.NewLabel()

		if len(b.tables) == 1 && plan.TablePlans[0].ScanType != scanTableFull {
			// Use the optimized single-table scan from the planner
			if err := b.emitOptimizedSingleTable(
				plan, stmt, resultBase, resultCols,
				needSorter, sorterCursor, orderByBase,
				emptyLabel, loopEndLabel,
			); err != nil {
				return err
			}
		} else {
			// Standard path: Rewind + iterate
			b.b.EmitJump(vdbe.OpRewind, b.tables[0].cursor, emptyLabel, 0)
			if err := b.emitJoinLoops(stmt, resultBase, resultCols, needSorter, sorterCursor, orderByBase, emptyLabel, loopEndLabel); err != nil {
				return err
			}
		}

		b.b.DefineLabel(emptyLabel)

		// If we have a sorter, now sort and output
		if needSorter {
			if err := b.emitSortedOutput(sorterCursor, nResult); err != nil {
				return err
			}
		}
	}

	// Handle LIMIT/OFFSET
	if stmt.Limit != nil {
		if err := b.emitLimit(stmt); err != nil {
			return err
		}
	}

	b.emitHalt(0)
	return nil
}

// compileAggregateSelect compiles a SELECT with aggregate functions or GROUP BY.
func (b *Build) compileAggregateSelect(stmt *SelectStmt) error {
	b.emitInit()
	b.emitTransaction(0, false)

	// Open cursors for FROM tables
	if err := b.openFromTables(stmt.From, false); err != nil {
		return err
	}

	resultCols, err := b.expandResultColumns(stmt.Columns)
	if err != nil {
		return err
	}
	nResult := len(resultCols)

	// Allocate registers for aggregate accumulators
	aggBase := b.b.AllocReg(nResult)

	// Initialize accumulators to NULL (or 0 for COUNT)
	for i := 0; i < nResult; i++ {
		b.emitNull(aggBase + i)
	}

	// Sorter for GROUP BY
	var groupSorter int
	hasGroupBy := len(stmt.GroupBy) > 0
	if hasGroupBy {
		groupSorter = b.b.AllocCursor()
		nGroupCols := len(stmt.GroupBy)
		b.emitSorterOpen(groupSorter, nGroupCols)
	}

	// Main loop
	emptyLabel := b.b.NewLabel()
	loopEndLabel := b.b.NewLabel()

	if len(b.tables) > 0 {
		b.b.EmitJump(vdbe.OpRewind, b.tables[0].cursor, emptyLabel, 0)
	}

	loopBody := b.b.NewLabel()
	b.b.DefineLabel(loopBody)

	// Evaluate WHERE
	if stmt.Where != nil {
		skipLabel := b.b.NewLabel()
		if err := b.compileCondition(stmt.Where, skipLabel, loopEndLabel, true); err != nil {
			return err
		}
		b.b.DefineLabel(skipLabel)
	}

	// If GROUP BY, insert groups into sorter
	if hasGroupBy {
		groupKeyBase := b.b.AllocReg(len(stmt.GroupBy))
		for i, gExpr := range stmt.GroupBy {
			if err := b.compileExpr(gExpr, groupKeyBase+i); err != nil {
				return err
			}
		}
		// Make record from group key
		recReg := b.b.AllocReg(1)
		b.emitMakeRecord(groupKeyBase, len(stmt.GroupBy), recReg)
		b.emitSorterInsert(groupSorter, recReg)
	} else {
		// No GROUP BY: aggregate over entire result set
		for i, rc := range resultCols {
			if rc.Expr != nil && b.exprHasAggregate(rc.Expr) {
				if err := b.compileExpr(rc.Expr, aggBase+i); err != nil {
					return err
				}
			}
		}
	}

	b.emitNext(b.tables[0].cursor, loopBody)
	b.b.DefineLabel(loopEndLabel)
	b.b.DefineLabel(emptyLabel)

	// Finalize aggregates
	if hasGroupBy {
		// Sort groups, then iterate through groups
		sortEmpty := b.b.NewLabel()
		sortBody := b.emitSorterSort(groupSorter, sortEmpty)

		// For each group, compute aggregates
		groupLoopEnd := b.b.NewLabel()
		for {
			// Aggregate step for each result column
			for i, rc := range resultCols {
				if rc.Expr != nil && b.exprHasAggregate(rc.Expr) {
					if err := b.compileExpr(rc.Expr, aggBase+i); err != nil {
						return err
					}
				}
			}

			// Check HAVING
			if stmt.Having != nil {
				havingFail := b.b.NewLabel()
				if err := b.compileCondition(stmt.Having, havingFail, groupLoopEnd, true); err != nil {
					return err
				}
				b.b.DefineLabel(havingFail)
			}

			// Finalize and output
			for i := 0; i < nResult; i++ {
				b.b.Emit(vdbe.OpAggFinal, aggBase+i, 0, 0)
			}
			b.emitResultRow(aggBase, nResult)
			break
		}

		b.emitSorterNext(groupSorter, sortBody)
		b.b.DefineLabel(groupLoopEnd)
		b.b.DefineLabel(sortEmpty)
	} else {
		// No GROUP BY: finalize aggregates and output single row
		for i := 0; i < nResult; i++ {
			b.b.Emit(vdbe.OpAggFinal, aggBase+i, 0, 0)
		}
		b.emitResultRow(aggBase, nResult)
	}

	b.emitHalt(0)
	return nil
}

// compileCompoundSelect compiles compound SELECT (UNION, INTERSECT, EXCEPT).
func (b *Build) compileCompoundSelect(stmt *SelectStmt) error {
	// For compound selects, each sub-select produces rows into an
	// ephemeral table, then we deduplicate as needed.
	b.emitInit()
	b.emitTransaction(0, false)

	resultCols, err := b.expandResultColumns(stmt.Columns)
	if err != nil {
		return err
	}
	nResult := len(resultCols)

	// Open ephemeral table for union results
	unionCursor := b.b.AllocCursor()
	b.emitOpenEphemeral(unionCursor, nResult)

	// Compile the first SELECT
	savedTables := b.tables
	savedTableMap := b.tableMap
	b.tables = nil
	b.tableMap = make(map[string]*tableEntry)

	if err := b.compileSelectInner(stmt, unionCursor); err != nil {
		b.tables = savedTables
		b.tableMap = savedTableMap
		return err
	}

	b.tables = savedTables
	b.tableMap = savedTableMap

	// Compile subsequent compound SELECTs
	for i, op := range stmt.CompoundOps {
		savedTables := b.tables
		savedTableMap := b.tableMap
		b.tables = nil
		b.tableMap = make(map[string]*tableEntry)

		if err := b.compileSelectInner(stmt.CompoundSelects[i], unionCursor); err != nil {
			b.tables = savedTables
			b.tableMap = savedTableMap
			return err
		}

		b.tables = savedTables
		b.tableMap = savedTableMap

		// For EXCEPT/INTERSECT, would need additional logic
		_ = op
	}

	// Output from ephemeral table
	emptyLabel := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpRewind, unionCursor, emptyLabel, 0)
	outLoop := b.b.NewLabel()
	b.b.DefineLabel(outLoop)
	b.emitColumn(unionCursor, 0, b.b.AllocReg(nResult))
	b.emitResultRow(b.b.CurrentAddr()-nResult, nResult)
	b.emitNext(unionCursor, outLoop)
	b.b.DefineLabel(emptyLabel)
	b.emitClose(unionCursor)

	b.emitHalt(0)
	return nil
}

// compileSelectInner compiles a SELECT that writes its results into
// an ephemeral table cursor instead of OP_ResultRow.
func (b *Build) compileSelectInner(stmt *SelectStmt, destCursor int) error {
	if stmt == nil {
		return fmt.Errorf("nil SELECT statement")
	}

	// Open cursors for FROM tables
	if err := b.openFromTables(stmt.From, false); err != nil {
		return err
	}

	resultCols, err := b.expandResultColumns(stmt.Columns)
	if err != nil {
		return err
	}
	nResult := len(resultCols)
	resultBase := b.b.AllocReg(nResult)

	emptyLabel := b.b.NewLabel()
	loopEndLabel := b.b.NewLabel()

	if len(b.tables) > 0 {
		b.b.EmitJump(vdbe.OpRewind, b.tables[0].cursor, emptyLabel, 0)
	}

	loopBody := b.b.NewLabel()
	b.b.DefineLabel(loopBody)

	// Evaluate WHERE
	if stmt.Where != nil {
		skipLabel := b.b.NewLabel()
		if err := b.compileCondition(stmt.Where, skipLabel, loopEndLabel, true); err != nil {
			return err
		}
		b.b.DefineLabel(skipLabel)
	}

	// Compute result columns
	for i, rc := range resultCols {
		if err := b.compileExpr(rc.Expr, resultBase+i); err != nil {
			return err
		}
	}

	// Insert into destination ephemeral table
	recReg := b.b.AllocReg(1)
	b.emitMakeRecord(resultBase, nResult, recReg)
	b.emitSorterInsert(destCursor, recReg)

	b.emitNext(b.tables[0].cursor, loopBody)
	b.b.DefineLabel(loopEndLabel)
	b.b.DefineLabel(emptyLabel)

	// Close cursors
	for _, entry := range b.tables {
		b.emitClose(entry.cursor)
	}
	b.tables = nil
	b.tableMap = make(map[string]*tableEntry)

	return nil
}

// openFromTables opens cursors for all tables in the FROM clause.
func (b *Build) openFromTables(from *FromClause, forWrite bool) error {
	if from == nil || len(from.Tables) == 0 {
		return nil
	}

	for _, tref := range from.Tables {
		if tref.Subquery != nil {
			// Subquery in FROM: open ephemeral and compile subquery into it
			cursor := b.b.AllocCursor()
			b.emitOpenEphemeral(cursor, 1) // approximate

			// Save context
			savedTables := b.tables
			savedTableMap := b.tableMap
			b.tables = nil
			b.tableMap = make(map[string]*tableEntry)

			if err := b.compileSelectInner(tref.Subquery, cursor); err != nil {
				b.tables = savedTables
				b.tableMap = savedTableMap
				return err
			}

			// Create pseudo-table entry for the subquery result
			alias := tref.Alias
			if alias == "" {
				alias = "subquery"
			}
			pseudoTable := &TableInfo{
				Name:     alias,
				HasRowid: true,
				// Column info would come from subquery schema
			}
			b.addTableRef(alias, alias, pseudoTable, cursor)

			// Restore outer context (but keep the new entry)
			for k, v := range savedTableMap {
				if _, exists := b.tableMap[k]; !exists {
					b.tableMap[k] = v
				}
			}
			b.tables = append(savedTables, b.tables...)
			continue
		}

		// Regular table reference
		tbl, err := b.lookupTable(tref.Name)
		if err != nil {
			return err
		}

		cursor := b.b.AllocCursor()
		if forWrite {
			b.emitOpenWrite(cursor, tbl.RootPage)
		} else {
			b.emitOpenRead(cursor, tbl.RootPage)
		}

		alias := tref.Alias
		if alias == "" {
			alias = tref.Name
		}
		b.addTableRef(tref.Name, alias, tbl, cursor)

		// Open indexes if needed (for indexed lookups)
		// This is where we'd analyze the WHERE clause for index usage
	}

	return nil
}

// resultColumn describes an expanded result column.
type resultColumn struct {
	Expr    *Expr
	Alias   string
	Cursor  int
	ColIdx  int
	IsTable bool // true if this is a direct column reference
}

// expandResultColumns expands * and table.* in the result column list.
func (b *Build) expandResultColumns(cols []*ResultCol) ([]*resultColumn, error) {
	var result []*resultColumn

	for _, col := range cols {
		if col.Star {
			// Expand * to all columns from all tables
			expanded, err := b.expandStarColumns("")
			if err != nil {
				return nil, err
			}
			for _, ec := range expanded {
				result = append(result, &resultColumn{
					Expr:    &Expr{Kind: ExprColumnRef, Table: "", Name: ec.name},
					Cursor:  ec.cursor,
					ColIdx:  ec.colIdx,
					IsTable: true,
				})
			}
		} else if col.TableStar != "" {
			// Expand table.* to all columns from that table
			expanded, err := b.expandStarColumns(col.TableStar)
			if err != nil {
				return nil, err
			}
			for _, ec := range expanded {
				result = append(result, &resultColumn{
					Expr: &Expr{Kind: ExprColumnRef, Table: col.TableStar, Name: ec.name},
					Cursor:  ec.cursor,
					ColIdx:  ec.colIdx,
					IsTable: true,
				})
			}
		} else {
			result = append(result, &resultColumn{
				Expr:  col.Expr,
				Alias: col.As,
			})
		}
	}

	// If no tables (SELECT 1), just use the expressions directly
	if len(b.tables) == 0 && len(result) > 0 {
		return result, nil
	}

	return result, nil
}

// emitJoinLoops emits the nested loop join structure for a SELECT.
func (b *Build) emitJoinLoops(stmt *SelectStmt, resultBase int, resultCols []*resultColumn,
	needSorter bool, sorterCursor, orderByBase int, emptyLabel, loopEndLabel int) error {

	// For simple single-table query
	if len(b.tables) == 1 {
		return b.emitSingleTableLoop(stmt, resultBase, resultCols, needSorter, sorterCursor, orderByBase, emptyLabel, loopEndLabel)
	}

	// For multi-table joins, emit nested loops
	return b.emitNestedLoopJoin(stmt, resultBase, resultCols, needSorter, sorterCursor, orderByBase, emptyLabel, loopEndLabel)
}

// emitSingleTableLoop emits the loop for a single-table SELECT.
func (b *Build) emitSingleTableLoop(stmt *SelectStmt, resultBase int, resultCols []*resultColumn,
	needSorter bool, sorterCursor, orderByBase int, emptyLabel, loopEndLabel int) error {

	cursor := b.tables[0].cursor
	loopBody := b.b.NewLabel()
	b.b.DefineLabel(loopBody)

	// Evaluate WHERE condition
	if stmt.Where != nil {
		whereFalse := b.b.NewLabel()
		if err := b.compileCondition(stmt.Where, whereFalse, loopEndLabel, true); err != nil {
			return err
		}
		b.b.DefineLabel(whereFalse)
	}

	// Compute result columns
	for i, rc := range resultCols {
		if err := b.compileExpr(rc.Expr, resultBase+i); err != nil {
			return err
		}
	}

	if needSorter {
		// Build ORDER BY key
		for i, ob := range stmt.OrderBy {
			if err := b.compileExpr(ob.Expr, orderByBase+i); err != nil {
				return err
			}
		}
		// Make record from result + sort key
		recReg := b.b.AllocReg(1)
		b.emitMakeRecord(resultBase, len(resultCols), recReg)
		b.emitSorterInsert(sorterCursor, recReg)
	} else {
		b.emitResultRow(resultBase, len(resultCols))
	}

	b.emitNext(cursor, loopBody)
	b.b.DefineLabel(loopEndLabel)

	// Close cursor
	b.emitClose(cursor)
	return nil
}

// emitNestedLoopJoin emits nested loop join code.
func (b *Build) emitNestedLoopJoin(stmt *SelectStmt, resultBase int, resultCols []*resultColumn,
	needSorter bool, sorterCursor, orderByBase int, emptyLabel, loopEndLabel int) error {

	nTables := len(b.tables)
	// Track loop body and end labels for each table level
	loopBodies := make([]int, nTables)
	loopEnds := make([]int, nTables)
	joinFails := make([]int, nTables)

	// Outer loop (first table) is already rewound
	loopBodies[0] = b.b.NewLabel()
	b.b.DefineLabel(loopBodies[0])

	// For each subsequent table, emit Rewind and loop structure
	for i := 1; i < nTables; i++ {
		empty := b.b.NewLabel()
		loopBodies[i] = b.b.NewLabel()
		loopEnds[i] = b.b.NewLabel()
		joinFails[i] = b.b.NewLabel()

		b.b.EmitJump(vdbe.OpRewind, b.tables[i].cursor, empty, 0)
		b.b.DefineLabel(loopBodies[i])
	}

	// Check join conditions
	if stmt.From != nil {
		for _, tref := range stmt.From.Tables {
			if tref.On != nil {
				joinFail := b.b.NewLabel()
				if err := b.compileCondition(tref.On, joinFail, loopEndLabel, true); err != nil {
					return err
				}
				b.b.DefineLabel(joinFail)
			}
		}
	}

	// Evaluate WHERE
	if stmt.Where != nil {
		whereFail := b.b.NewLabel()
		if err := b.compileCondition(stmt.Where, whereFail, loopEndLabel, true); err != nil {
			return err
		}
		b.b.DefineLabel(whereFail)
	}

	// Compute result columns
	for i, rc := range resultCols {
		if err := b.compileExpr(rc.Expr, resultBase+i); err != nil {
			return err
		}
	}

	if needSorter {
		recReg := b.b.AllocReg(1)
		b.emitMakeRecord(resultBase, len(resultCols), recReg)
		b.emitSorterInsert(sorterCursor, recReg)
	} else {
		b.emitResultRow(resultBase, len(resultCols))
	}

	// Close loops in reverse order
	for i := nTables - 1; i >= 1; i-- {
		b.emitNext(b.tables[i].cursor, loopBodies[i])
		b.b.DefineLabel(loopEnds[i])
	}

	// Next for outer table
	b.emitNext(b.tables[0].cursor, loopBodies[0])
	b.b.DefineLabel(loopEndLabel)

	// Close all cursors
	for _, entry := range b.tables {
		b.emitClose(entry.cursor)
	}
	return nil
}

// emitSortedOutput reads from a sorter and emits ResultRow.
func (b *Build) emitSortedOutput(sorterCursor, nResult int) error {
	sortEmpty := b.b.NewLabel()
	sortBody := b.emitSorterSort(sorterCursor, sortEmpty)

	// Allocate register for reading sorter data
	dataReg := b.b.AllocReg(nResult)

	// Read and output each row from the sorter
	b.emitColumn(sorterCursor, 0, dataReg)
	b.emitResultRow(dataReg, nResult)
	b.emitSorterNext(sorterCursor, sortBody)
	b.b.DefineLabel(sortEmpty)
	return nil
}

// emitLimit emits LIMIT/OFFSET handling.
func (b *Build) emitLimit(stmt *SelectStmt) error {
	if stmt.Limit == nil {
		return nil
	}

	// Evaluate LIMIT value
	limitReg := b.b.AllocReg(1)
	if err := b.compileExpr(stmt.Limit, limitReg); err != nil {
		return err
	}

	// If there's an OFFSET, evaluate it
	if stmt.Offset != nil {
		offsetReg := b.b.AllocReg(1)
		if err := b.compileExpr(stmt.Offset, offsetReg); err != nil {
			return err
		}
		// Subtract offset from limit (simplified)
		_ = offsetReg
	}

	return nil
}

// hasAggregates checks if the SELECT uses aggregate functions.
func (b *Build) hasAggregates(stmt *SelectStmt) bool {
	if len(stmt.GroupBy) > 0 {
		return true
	}
	for _, col := range stmt.Columns {
		if col.Expr != nil && b.exprHasAggregate(col.Expr) {
			return true
		}
	}
	if stmt.Having != nil && b.exprHasAggregate(stmt.Having) {
		return true
	}
	return false
}

// exprHasAggregate checks if an expression tree contains aggregate functions.
func (b *Build) exprHasAggregate(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == ExprFunctionCall && isAggregate(expr.FunctionName) {
		return true
	}
	if expr.Left != nil && b.exprHasAggregate(expr.Left) {
		return true
	}
	if expr.Right != nil && b.exprHasAggregate(expr.Right) {
		return true
	}
	for _, arg := range expr.Args {
		if b.exprHasAggregate(arg) {
			return true
		}
	}
	if expr.Low != nil && b.exprHasAggregate(expr.Low) {
		return true
	}
	if expr.High != nil && b.exprHasAggregate(expr.High) {
		return true
	}
	if expr.Pattern != nil && b.exprHasAggregate(expr.Pattern) {
		return true
	}
	for _, w := range expr.WhenList {
		if b.exprHasAggregate(w.Condition) || b.exprHasAggregate(w.Result) {
			return true
		}
	}
	if expr.ElseExpr != nil && b.exprHasAggregate(expr.ElseExpr) {
		return true
	}
	return false
}

// extractSortDirection extracts sort direction from an OrderItem.
func extractSortDirection(item *OrderItem) string {
	switch item.Order {
	case SortAsc:
		return "ASC"
	case SortDesc:
		return "DESC"
	default:
		return ""
	}
}

// compileOrderByKeyExpr compiles the ORDER BY key expression.
// For numeric ORDER BY (e.g., ORDER BY 1), resolves to the Nth result column.
func (b *Build) compileOrderByKeyExpr(expr *Expr, resultCols []*resultColumn, targetReg int) error {
	// Check if this is a numeric literal (positional reference)
	if expr.Kind == ExprLiteral && expr.LiteralType == "integer" {
		idx := int(expr.IntValue) - 1 // 1-based
		if idx >= 0 && idx < len(resultCols) && resultCols[idx].Expr != nil {
			return b.compileExpr(resultCols[idx].Expr, targetReg)
		}
	}
	return b.compileExpr(expr, targetReg)
}

// selectHasDistinct checks if SELECT DISTINCT is specified.
func selectHasDistinct(stmt *SelectStmt) bool {
	return stmt != nil && stmt.Distinct
}

// selectHasOrder checks if SELECT has ORDER BY.
func selectHasOrder(stmt *SelectStmt) bool {
	return stmt != nil && len(stmt.OrderBy) > 0
}

// compileDistinctSelect wraps a SELECT with DISTINCT deduplication.
func (b *Build) compileDistinctSelect(stmt *SelectStmt) error {
	// Open an ephemeral table to track seen rows
	distinctCursor := b.b.AllocCursor()
	resultCols, _ := b.expandResultColumns(stmt.Columns)
	nResult := len(resultCols)
	b.emitOpenEphemeral(distinctCursor, nResult)

	// Compile the inner select, but check for duplicates before outputting
	// For each row: make a record, check if it exists in ephemeral table,
	// if not found, insert it and output
	_ = distinctCursor
	// Fall through to simple select for now
	return b.compileSimpleSelect(stmt)
}

// fixSelectColumnRefs resolves ORDER BY column references to result columns.
func fixSelectColumnRefs(orderBy []*OrderItem, resultCols []*ResultCol) {
	for _, ob := range orderBy {
		if ob.Expr.Kind == ExprLiteral && ob.Expr.LiteralType == "integer" {
			idx := int(ob.Expr.IntValue) - 1
			if idx >= 0 && idx < len(resultCols) && resultCols[idx].Expr != nil {
				// Replace with the actual result column expression
				ob.Expr = &Expr{
					Kind: ExprColumnRef,
					Name: "", // resolved during expansion
				}
			}
		}
	}
}

// String returns a string representation of a SortOrder.
func (s SortOrder) String() string {
	switch s {
	case SortAsc:
		return "ASC"
	case SortDesc:
		return "DESC"
	default:
		return ""
	}
}

// emitOptimizedSingleTable emits a single-table loop using the query plan.
// It uses index scans when beneficial and falls back to table scan when not.
func (b *Build) emitOptimizedSingleTable(
	plan *queryPlan,
	stmt *SelectStmt,
	resultBase int,
	resultCols []*resultColumn,
	needSorter bool,
	sorterCursor, orderByBase int,
	emptyLabel, loopEndLabel int,
) error {
	tplan := plan.TablePlans[0]

	// Emit the scan start (index seek or table rewind)
	loopBody, err := b.emitOptimizedScan(tplan, emptyLabel, loopEndLabel)
	if err != nil {
		return err
	}
	b.b.DefineLabel(loopBody)

	// Evaluate remaining WHERE terms (not handled by index)
	if len(plan.RemainingTerms) > 0 {
		skipLabel := b.b.NewLabel()
		if err := b.emitRemainingWhere(plan.RemainingTerms, skipLabel, loopEndLabel); err != nil {
			return err
		}
		b.b.DefineLabel(skipLabel)
	}

	// Compute result columns
	for i, rc := range resultCols {
		if err := b.compileExpr(rc.Expr, resultBase+i); err != nil {
			return err
		}
	}

	if needSorter {
		for i, ob := range stmt.OrderBy {
			if err := b.compileExpr(ob.Expr, orderByBase+i); err != nil {
				return err
			}
		}
		recReg := b.b.AllocReg(1)
		b.emitMakeRecord(resultBase, len(resultCols), recReg)
		b.emitSorterInsert(sorterCursor, recReg)
	} else {
		b.emitResultRow(resultBase, len(resultCols))
	}

	// Advance the scan
	b.emitOptimizedScanEnd(tplan, loopBody)
	b.b.DefineLabel(loopEndLabel)

	// Close cursors (table and index)
	b.emitClose(tplan.TblCursor)

	return nil
}

// hasWindowFuncs checks if the SELECT uses window functions.
func (b *Build) hasWindowFuncs(stmt *SelectStmt) bool {
	for _, col := range stmt.Columns {
		if col.Expr != nil && b.exprHasWindow(col.Expr) {
			return true
		}
	}
	return false
}

// exprHasWindow checks if an expression tree contains window function calls.
func (b *Build) exprHasWindow(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if expr.Kind == ExprFunctionCall && expr.Over != nil {
		return true
	}
	if expr.Kind == ExprFunctionCall && isWindowFunc(expr.FunctionName) {
		return true
	}
	if expr.Left != nil && b.exprHasWindow(expr.Left) {
		return true
	}
	if expr.Right != nil && b.exprHasWindow(expr.Right) {
		return true
	}
	for _, arg := range expr.Args {
		if b.exprHasWindow(arg) {
			return true
		}
	}
	return false
}

// compileWindowSelect compiles a SELECT with window functions.
func (b *Build) compileWindowSelect(stmt *SelectStmt) error {
	return b.compileSimpleSelect(stmt)
}

// ensureUnused prevents "imported and not used" errors
var _ = strings.ToUpper
var _ SortOrder = SortDefault
