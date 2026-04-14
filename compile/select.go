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
		nSortCols := nResult
		b.emitSorterOpen(sorterCursor, nSortCols)
	}

	// DISTINCT: use ephemeral table for deduplication
	needDistinct := stmt.Distinct
	var distinctCursor int
	if needDistinct && !needSorter {
		distinctCursor = b.b.AllocCursor()
		b.emitOpenEphemeral(distinctCursor, nResult)
	}

	// Allocate result registers
	resultBase := b.b.AllocReg(nResult)

	// Allocate registers for ORDER BY key
	var orderByBase int
	if needSorter {
		orderByBase = b.b.AllocReg(len(stmt.OrderBy))
	}

	// Set up LIMIT/OFFSET registers before the main body
	if err := b.emitLimit(stmt); err != nil {
		return err
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
				needDistinct, distinctCursor,
				emptyLabel, loopEndLabel,
			); err != nil {
				return err
			}
		} else {
			// Standard path: Rewind + iterate
			b.b.EmitJump(vdbe.OpRewind, b.tables[0].cursor, emptyLabel, 0)
			if err := b.emitJoinLoops(stmt, resultBase, resultCols, needSorter, sorterCursor, orderByBase, needDistinct, distinctCursor, emptyLabel, loopEndLabel); err != nil {
				return err
			}
		}

		b.b.DefineLabel(emptyLabel)

		// If we have a sorter, now sort and output
		if needSorter {
			if needDistinct {
				if err := b.emitSortedDistinctOutput(sorterCursor, nResult); err != nil {
					return err
				}
			} else {
				if err := b.emitSortedOutput(sorterCursor, nResult); err != nil {
					return err
				}
			}
		} else if needDistinct {
			b.emitClose(distinctCursor)
		}
	}

	// Define the LIMIT end label (jump target when limit is exhausted)
	if b.limitEndLabel != 0 {
		b.b.DefineLabel(b.limitEndLabel)
		// Reset limit state for clean reuse
		b.limitReg = 0
		b.offsetReg = 0
		b.limitEndLabel = 0
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

	hasGroupBy := len(stmt.GroupBy) > 0

	if hasGroupBy {
		return b.compileGroupBySelect(stmt, resultCols, nResult)
	}

	// No GROUP BY: aggregate over entire result set
	return b.compileAggregateNoGroup(stmt, resultCols, nResult)
}

// compileAggregateNoGroup compiles an aggregate SELECT without GROUP BY.
// It scans all rows, accumulating aggregate state, then outputs a single result row.
func (b *Build) compileAggregateNoGroup(stmt *SelectStmt, resultCols []*resultColumn, nResult int) error {
	// Collect all aggregate Expr nodes from result columns
	var allAggs []*Expr
	for _, rc := range resultCols {
		b.collectAggFuncs(rc.Expr, &allAggs)
	}

	// Allocate dedicated registers for each aggregate function
	b.aggFuncRegs = make(map[*Expr]int)
	for _, agg := range allAggs {
		if _, exists := b.aggFuncRegs[agg]; !exists {
			reg := b.b.AllocReg(1)
			b.aggFuncRegs[agg] = reg
		}
	}

	// Allocate result registers
	aggBase := b.b.AllocReg(nResult)

	// Flag register: tracks whether any rows matched the WHERE clause
	rowFlagReg := b.b.AllocReg(1)
	b.emitInteger(0, rowFlagReg)

	// Initialize aggregate accumulator registers to NULL
	for _, reg := range b.aggFuncRegs {
		b.emitNull(reg)
	}
	for i := 0; i < nResult; i++ {
		b.emitNull(aggBase + i)
	}

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

	// Mark that at least one row was processed
	b.emitInteger(1, rowFlagReg)

	// Run aggregate step functions only
	for agg, reg := range b.aggFuncRegs {
		if err := b.compileAggregate(agg, reg); err != nil {
			return err
		}
	}

	// Also evaluate non-aggregate columns in the loop (to capture last row's value)
	for i, rc := range resultCols {
		if rc.Expr != nil && !b.exprHasAggregate(rc.Expr) {
			if err := b.compileExpr(rc.Expr, aggBase+i); err != nil {
				return err
			}
		}
	}

	b.emitNext(b.tables[0].cursor, loopBody)
	b.b.DefineLabel(loopEndLabel)
	b.b.DefineLabel(emptyLabel)

	// Finalize all aggregate registers
	for agg, reg := range b.aggFuncRegs {
		afi := b.makeAggFuncInfo(agg)
		b.b.EmitP4(vdbe.OpAggFinal, reg, reg, 0, afi, "AGG FINAL")
	}

	// Evaluate result columns in output mode (aggregates read from registers)
	// Skip non-aggregate columns: they were already evaluated in the loop body
	// (and remain NULL if no rows matched)
	b.inAggOutput = true
	for i, rc := range resultCols {
		if rc.Expr != nil && b.exprHasAggregate(rc.Expr) {
			if err := b.compileExpr(rc.Expr, aggBase+i); err != nil {
				return err
			}
		}
	}
	b.inAggOutput = false
	b.aggFuncRegs = nil

	b.emitResultRow(aggBase, nResult)
	b.emitHalt(0)
	return nil
}

// compileGroupBySelect compiles a SELECT with GROUP BY.
//
// Strategy:
//  1. Scan all source rows, insert (group_key, row_data) into a sorter
//  2. Sort by group key
//  3. Iterate sorted rows, detect group boundaries via SorterCompare
//  4. For each group: accumulate aggregates, finalize, apply HAVING, output
func (b *Build) compileGroupBySelect(stmt *SelectStmt, resultCols []*resultColumn, nResult int) error {
	// Resolve GROUP BY expressions: aliases and column numbers
	for i, gExpr := range stmt.GroupBy {
		resolved, err := b.resolveGroupByExpr(gExpr, resultCols)
		if err != nil {
			return err
		}
		stmt.GroupBy[i] = resolved
	}

	// Resolve aliases in HAVING
	if stmt.Having != nil {
		stmt.Having = b.resolveHavingAliases(stmt.Having, resultCols)
	}

	nGroupCols := len(stmt.GroupBy)

	// Count source columns across all tables
	nSourceCols := 0
	for _, entry := range b.tables {
		if entry.table != nil {
			nSourceCols += len(entry.table.Columns)
		}
	}

	// Sorter for storing (group_key, row_data)
	groupSorter := b.b.AllocCursor()
	b.emitSorterOpen(groupSorter, nGroupCols)

	// === Phase 1: Scan source rows and insert into sorter ===
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

	// Evaluate GROUP BY key columns
	groupKeyBase := b.b.AllocReg(nGroupCols)
	for i, gExpr := range stmt.GroupBy {
		if err := b.compileExpr(gExpr, groupKeyBase+i); err != nil {
			return err
		}
	}

	// Build the sort key from group columns
	keyRecReg := b.b.AllocReg(1)
	b.emitMakeRecord(groupKeyBase, nGroupCols, keyRecReg)

	// Build the data record from all source table columns
	if nSourceCols > 0 {
		dataBase := b.b.AllocReg(nSourceCols)
		idx := 0
		for _, entry := range b.tables {
			if entry.table != nil {
				for colIdx := range entry.table.Columns {
					b.emitColumn(entry.cursor, colIdx, dataBase+idx)
					idx++
				}
			}
		}
		dataRecReg := b.b.AllocReg(1)
		b.emitMakeRecord(dataBase, nSourceCols, dataRecReg)
		// Insert with key = group key record, data = full row record
		b.b.Emit(vdbe.OpSorterInsert, groupSorter, keyRecReg, dataRecReg)
	} else {
		b.emitSorterInsert(groupSorter, keyRecReg)
	}

	b.emitNext(b.tables[0].cursor, loopBody)
	b.b.DefineLabel(loopEndLabel)
	b.b.DefineLabel(emptyLabel)

	// === Phase 2: Sort by group key and iterate groups ===
	sortEmptyLabel := b.b.NewLabel()
	_ = b.emitSorterSort(groupSorter, sortEmptyLabel)

	// Advance sorter to the first row (SorterSort rewinds to iterIdx=0,
	// but OpColumn reads from the sorter data which is nil at iterIdx=0).
	b.emitSorterNext(groupSorter, sortEmptyLabel)

	// We need an output sorter if there's ORDER BY
	var outSorter int
	needOutSorter := len(stmt.OrderBy) > 0
	if needOutSorter {
		outSorter = b.b.AllocCursor()
		b.emitSorterOpen(outSorter, nResult)
	}

	// Set up GROUP BY mode: compileColumnRef will read directly from the sorter cursor
	b.groupSorterCursor = groupSorter
	b.groupColOffsets = make(map[string]int)
	off := 0
	for _, entry := range b.tables {
		if entry.table != nil {
			key := entry.name
			if entry.alias != "" {
				key = entry.alias
			}
			b.groupColOffsets[strings.ToUpper(key)] = off
			off += len(entry.table.Columns)
		}
	}

	// Collect all aggregate Expr nodes from result columns and HAVING
	var allAggs []*Expr
	for _, rc := range resultCols {
		b.collectAggFuncs(rc.Expr, &allAggs)
	}
	if stmt.Having != nil {
		b.collectAggFuncs(stmt.Having, &allAggs)
	}

	// Allocate dedicated registers for each aggregate function
	b.aggFuncRegs = make(map[*Expr]int)
	for _, agg := range allAggs {
		if _, exists := b.aggFuncRegs[agg]; !exists {
			reg := b.b.AllocReg(1)
			b.aggFuncRegs[agg] = reg
		}
	}

	// Registers for aggregate accumulators and group key
	aggBase := b.b.AllocReg(nResult)

	// Initialize accumulators to NULL
	for i := 0; i < nResult; i++ {
		b.emitNull(aggBase + i)
	}
	for _, reg := range b.aggFuncRegs {
		b.emitNull(reg)
	}

	// Register for saved group key values (for comparison)
	savedKeyBase := b.b.AllocReg(nGroupCols)
	for i := 0; i < nGroupCols; i++ {
		b.emitNull(savedKeyBase + i)
	}

	rowLabel := b.b.NewLabel()
	b.b.DefineLabel(rowLabel)

	// Re-initialize accumulators for a new group
	for i := 0; i < nResult; i++ {
		b.emitNull(aggBase + i)
	}
	for _, reg := range b.aggFuncRegs {
		b.emitNull(reg)
	}

	// Inner loop: accumulate rows within a group
	accLoop := b.b.NewLabel()
	b.b.DefineLabel(accLoop)

	// Run aggregate steps only (not surrounding arithmetic)
	for agg, reg := range b.aggFuncRegs {
		if err := b.compileAggregate(agg, reg); err != nil {
			return err
		}
	}

	// Also evaluate non-aggregate columns (like `log` in `SELECT log, count(*)`)
	for i, rc := range resultCols {
		if rc.Expr != nil && !b.exprHasAggregate(rc.Expr) {
			if err := b.compileExpr(rc.Expr, aggBase+i); err != nil {
				return err
			}
		}
	}

	// Compute current group key from source columns
	curKeyBase := b.b.AllocReg(nGroupCols)
	for i, gExpr := range stmt.GroupBy {
		if err := b.compileExpr(gExpr, curKeyBase+i); err != nil {
			return err
		}
	}

	// Copy current key to saved key for comparison
	for i := 0; i < nGroupCols; i++ {
		b.emitSCopy(curKeyBase+i, savedKeyBase+i)
	}

	// Advance to next row
	groupEnd := b.b.NewLabel()
	b.emitSorterNext(groupSorter, groupEnd)

	// If we get here, there IS a next row. Compute its group key.
	nextKeyBase := b.b.AllocReg(nGroupCols)
	for i, gExpr := range stmt.GroupBy {
		if err := b.compileExpr(gExpr, nextKeyBase+i); err != nil {
			return err
		}
	}

	// Compare saved key with next row's key
	keyChangedLabel := b.b.NewLabel()
	compareBase := b.b.AllocReg(2)
	for i := 0; i < nGroupCols; i++ {
		b.emitSCopy(savedKeyBase+i, compareBase)
		b.emitSCopy(nextKeyBase+i, compareBase+1)
		b.b.EmitJump(vdbe.OpNe, compareBase, keyChangedLabel, compareBase+1)
	}

	// All columns matched: same group, continue accumulating
	b.b.EmitJump(vdbe.OpGoto, 0, accLoop, 0)

	b.b.DefineLabel(keyChangedLabel)

	// Key changed: finalize current group, output, then start new group
	b.emitGroupOutput(stmt, resultCols, nResult, aggBase, outSorter, needOutSorter)
	b.b.EmitJump(vdbe.OpGoto, 0, rowLabel, 0)

	b.b.DefineLabel(groupEnd)

	// Finalize last group
	b.emitGroupOutput(stmt, resultCols, nResult, aggBase, outSorter, needOutSorter)

	b.b.DefineLabel(sortEmptyLabel)

	// Clear GROUP BY mode
	b.groupSorterCursor = 0
	b.groupColOffsets = nil
	b.aggFuncRegs = nil
	b.inAggOutput = false

	// If ORDER BY, output from outSorter
	if needOutSorter {
		b.emitClose(groupSorter)
		b.emitSortedOutput(outSorter, nResult)
	}

	b.emitHalt(0)
	return nil
}

// emitGroupOutput finalizes aggregates and emits a result row for the current group.
func (b *Build) emitGroupOutput(stmt *SelectStmt, resultCols []*resultColumn, nResult int, aggBase int, outSorter int, needOutSorter bool) {
	// Finalize all aggregate registers
	for agg, reg := range b.aggFuncRegs {
		afi := b.makeAggFuncInfo(agg)
		b.b.EmitP4(vdbe.OpAggFinal, reg, reg, 0, afi, "AGG FINAL")
	}

	// Evaluate result columns in output mode (aggregates read from registers)
	b.inAggOutput = true
	for i, rc := range resultCols {
		if rc.Expr != nil {
			if err := b.compileExpr(rc.Expr, aggBase+i); err != nil {
				_ = err
			}
		}
	}

	// Check HAVING
	if stmt.Having != nil {
		havingSkip := b.b.NewLabel()
		havingReg := b.b.AllocReg(1)
		if err := b.compileExpr(stmt.Having, havingReg); err != nil {
			_ = err
		}
		b.b.EmitJump(vdbe.OpIfNot, havingReg, havingSkip, 0)
		if needOutSorter {
			recReg := b.b.AllocReg(1)
			b.emitMakeRecord(aggBase, nResult, recReg)
			b.emitSorterInsert(outSorter, recReg)
		} else {
			b.emitResultRow(aggBase, nResult)
		}
		b.b.DefineLabel(havingSkip)
	} else {
		if needOutSorter {
			recReg := b.b.AllocReg(1)
			b.emitMakeRecord(aggBase, nResult, recReg)
			b.emitSorterInsert(outSorter, recReg)
		} else {
			b.emitResultRow(aggBase, nResult)
		}
	}
	b.inAggOutput = false
}

// emitAggFinal emits an OpAggFinal for the given register with the correct AggFuncInfo.
func (b *Build) emitAggFinalForCol(expr *Expr, reg int) {
	if expr == nil || !b.exprHasAggregate(expr) {
		return
	}
	afi := b.makeAggFuncInfo(expr)
	b.b.EmitP4(vdbe.OpAggFinal, reg, reg, 0, afi, "AGG FINAL")
}

// makeAggFuncInfo creates an AggFuncInfo for the given aggregate expression.
func (b *Build) makeAggFuncInfo(expr *Expr) *vdbe.AggFuncInfo {
	if expr == nil {
		return nil
	}
	// For compound expressions containing aggregates, find the innermost aggregate
	if expr.Kind == ExprBinaryOp || expr.Kind == ExprUnaryOp {
		if expr.Left != nil && b.exprHasAggregate(expr.Left) {
			return b.makeAggFuncInfo(expr.Left)
		}
		if expr.Right != nil && b.exprHasAggregate(expr.Right) {
			return b.makeAggFuncInfo(expr.Right)
		}
	}
	if expr.Kind != ExprFunctionCall || !isAggregate(expr.FunctionName) {
		return nil
	}

	fnName := strings.ToUpper(expr.FunctionName)
	switch fnName {
	case "COUNT":
		if expr.StarArg {
			return &vdbe.AggFuncInfo{
				Name: "COUNT",
				Step: func(state interface{}, args []*vdbe.Mem) {
					s := aggStateCount(state)
					s.count++
				},
				Finalize: func(state interface{}) *vdbe.Mem {
					s, ok := state.(*interface{})
					if !ok || *s == nil {
						return vdbe.NewMemInt(0)
					}
					cs := (*s).(*countState)
					return vdbe.NewMemInt(cs.count)
				},
			}
		}
		return &vdbe.AggFuncInfo{
			Name: "COUNT",
			Step: func(state interface{}, args []*vdbe.Mem) {
				s := aggStateCount(state)
				if len(args) > 0 && args[0].Type != vdbe.MemNull {
					s.count++
				}
			},
			Finalize: func(state interface{}) *vdbe.Mem {
				s, ok := state.(*interface{})
				if !ok || *s == nil {
					return vdbe.NewMemInt(0)
				}
				cs := (*s).(*countState)
				return vdbe.NewMemInt(cs.count)
			},
		}
	case "SUM":
		return &vdbe.AggFuncInfo{
			Name: "SUM",
			Step: func(state interface{}, args []*vdbe.Mem) {
				s := aggStateSum(state)
				if len(args) == 0 || args[0].Type == vdbe.MemNull {
					return
				}
				s.hasValue = true
				arg := args[0]
				if s.useFloat {
					s.sum += arg.FloatValue()
					return
				}
				if arg.Type == vdbe.MemInt {
					newSum := s.intSum + arg.IntVal
					if (newSum > 0) != (s.intSum > 0 || arg.IntVal > 0) &&
						s.intSum != 0 && arg.IntVal != 0 {
						s.sum = float64(s.intSum) + float64(arg.IntVal)
						s.useFloat = true
					} else {
						s.intSum = newSum
					}
				} else {
					s.sum = float64(s.intSum) + arg.FloatValue()
					s.useFloat = true
				}
			},
			Finalize: func(state interface{}) *vdbe.Mem {
				sp := state.(*interface{})
				if *sp == nil {
					return vdbe.NewMemNull()
				}
				s := (*sp).(*sumState)
				if !s.hasValue {
					return vdbe.NewMemNull()
				}
				if s.useFloat {
					return vdbe.NewMemFloat(s.sum)
				}
				return vdbe.NewMemInt(s.intSum)
			},
		}
	case "AVG":
		return &vdbe.AggFuncInfo{
			Name: "AVG",
			Step: func(state interface{}, args []*vdbe.Mem) {
				s := aggStateAvg(state)
				if len(args) == 0 || args[0].Type == vdbe.MemNull {
					return
				}
				s.sum += args[0].FloatValue()
				s.count++
			},
			Finalize: func(state interface{}) *vdbe.Mem {
				sp := state.(*interface{})
				if *sp == nil {
					return vdbe.NewMemNull()
				}
				s := (*sp).(*avgState)
				if s.count == 0 {
					return vdbe.NewMemNull()
				}
				return vdbe.NewMemFloat(s.sum / float64(s.count))
			},
		}
	case "MIN":
		return &vdbe.AggFuncInfo{
			Name: "MIN",
			Step: func(state interface{}, args []*vdbe.Mem) {
				if len(args) == 0 || args[0].Type == vdbe.MemNull {
					return
				}
				sp := state.(*interface{})
				s, ok := (*sp).(*minMaxState)
				if !ok || *sp == nil {
					*sp = &minMaxState{value: args[0].Copy()}
					return
				}
				if s.value == nil || s.value.Type == vdbe.MemNull {
					s.value = args[0].Copy()
					return
				}
				cmp := vdbe.MemCompare(s.value, args[0])
				if cmp > 0 {
					s.value = args[0].Copy()
				}
			},
			Finalize: func(state interface{}) *vdbe.Mem {
				sp := state.(*interface{})
				if *sp == nil {
					return vdbe.NewMemNull()
				}
				s := (*sp).(*minMaxState)
				if s.value == nil {
					return vdbe.NewMemNull()
				}
				return s.value.Copy()
			},
		}
	case "MAX":
		return &vdbe.AggFuncInfo{
			Name: "MAX",
			Step: func(state interface{}, args []*vdbe.Mem) {
				if len(args) == 0 || args[0].Type == vdbe.MemNull {
					return
				}
				sp := state.(*interface{})
				s, ok := (*sp).(*minMaxState)
				if !ok || *sp == nil {
					*sp = &minMaxState{value: args[0].Copy()}
					return
				}
				if s.value == nil || s.value.Type == vdbe.MemNull {
					s.value = args[0].Copy()
					return
				}
				cmp := vdbe.MemCompare(s.value, args[0])
				if cmp < 0 {
					s.value = args[0].Copy()
				}
			},
			Finalize: func(state interface{}) *vdbe.Mem {
				sp := state.(*interface{})
				if *sp == nil {
					return vdbe.NewMemNull()
				}
				s := (*sp).(*minMaxState)
				if s.value == nil {
					return vdbe.NewMemNull()
				}
				return s.value.Copy()
			},
		}
	}
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

	// Check if this SELECT has aggregate functions
	if b.hasAggregates(stmt) {
		return b.compileInnerAggregateSelect(stmt, destCursor, resultCols, nResult)
	}

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

	if len(b.tables) > 0 {
		b.emitNext(b.tables[0].cursor, loopBody)
	}
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

// compileInnerAggregateSelect compiles an aggregate SELECT for use inside a subquery.
// It scans all rows, accumulates aggregates, finalizes, and inserts one row into destCursor.
func (b *Build) compileInnerAggregateSelect(stmt *SelectStmt, destCursor int, resultCols []*resultColumn, nResult int) error {
	aggBase := b.b.AllocReg(nResult)

	// Initialize accumulators to NULL
	for i := 0; i < nResult; i++ {
		b.emitNull(aggBase + i)
	}

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

	// Accumulate aggregates for each result column
	for i, rc := range resultCols {
		if rc.Expr != nil && b.exprHasAggregate(rc.Expr) {
			if err := b.compileExpr(rc.Expr, aggBase+i); err != nil {
				return err
			}
		}
	}

	if len(b.tables) > 0 {
		b.emitNext(b.tables[0].cursor, loopBody)
	}
	b.b.DefineLabel(loopEndLabel)
	b.b.DefineLabel(emptyLabel)

	// Finalize aggregates and output single row
	for i, rc := range resultCols {
		if rc.Expr != nil && b.exprHasAggregate(rc.Expr) {
			b.emitAggFinalForCol(rc.Expr, aggBase+i)
		} else if rc.Expr != nil {
			// Non-aggregate column without GROUP BY: evaluate once
			if err := b.compileExpr(rc.Expr, aggBase+i); err != nil {
				return err
			}
		}
	}

	// Insert into destination ephemeral table
	recReg := b.b.AllocReg(1)
	b.emitMakeRecord(aggBase, nResult, recReg)
	b.emitSorterInsert(destCursor, recReg)

	// Close cursors
	for _, entry := range b.tables {
		b.emitClose(entry.cursor)
	}
	b.tables = nil
	b.tableMap = make(map[string]*tableEntry)

	return nil
}

// openFromTables opens cursors for all tables in the FROM clause.
// It also computes USING/NATURAL column information for SELECT * deduplication.
func (b *Build) openFromTables(from *FromClause, forWrite bool) error {
	if from == nil || len(from.Tables) == 0 {
		return nil
	}

	for tableIdx, tref := range from.Tables {
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

		// For NATURAL or USING joins, compute which columns are shared
		// so that SELECT * can skip duplicates from the right table.
		if tableIdx > 0 && (tref.Natural || len(tref.Using) > 0) {
			entry := b.tables[len(b.tables)-1]
			entry.usingCols = make(map[int]bool)

			var usingNames []string
			if len(tref.Using) > 0 {
				usingNames = tref.Using
			} else if tref.Natural {
				// Find common column names with previous tables
				for prevIdx := 0; prevIdx < len(b.tables)-1; prevIdx++ {
					prevInfo := b.tables[prevIdx].table
					if prevInfo == nil {
						continue
					}
					for _, pc := range prevInfo.Columns {
						for _, rc := range tbl.Columns {
							if strings.EqualFold(pc.Name, rc.Name) {
								// Check not already in list
								found := false
								for _, un := range usingNames {
									if strings.EqualFold(un, rc.Name) {
										found = true
										break
									}
								}
								if !found {
									usingNames = append(usingNames, rc.Name)
								}
							}
						}
					}
				}
			}

			// Map column names to column indices for the right table
			for _, colName := range usingNames {
				colIdx := tbl.FindColumn(colName)
				if colIdx >= 0 {
					entry.usingCols[colIdx] = true
				}
			}
		}

		// Open indexes if needed (for indexed lookups)
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
	needSorter bool, sorterCursor, orderByBase int, needDistinct bool, distinctCursor int, emptyLabel, loopEndLabel int) error {

	// For simple single-table query
	if len(b.tables) == 1 {
		return b.emitSingleTableLoop(stmt, resultBase, resultCols, needSorter, sorterCursor, orderByBase, needDistinct, distinctCursor, emptyLabel, loopEndLabel)
	}

	// For multi-table joins, emit nested loops
	return b.emitNestedLoopJoin(stmt, resultBase, resultCols, needSorter, sorterCursor, orderByBase, needDistinct, distinctCursor, emptyLabel, loopEndLabel)
}

// emitSingleTableLoop emits the loop for a single-table SELECT.
func (b *Build) emitSingleTableLoop(stmt *SelectStmt, resultBase int, resultCols []*resultColumn,
	needSorter bool, sorterCursor, orderByBase int, needDistinct bool, distinctCursor int, emptyLabel, loopEndLabel int) error {

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
	} else if needDistinct {
		skipLabel := b.b.NewLabel()
		b.emitDistinctCheck(distinctCursor, resultBase, len(resultCols), skipLabel)
		b.emitResultRow(resultBase, len(resultCols))
		b.b.DefineLabel(skipLabel)
	} else {
		b.emitResultRow(resultBase, len(resultCols))
	}

	b.emitNext(cursor, loopBody)
	b.b.DefineLabel(loopEndLabel)

	// Close cursor
	b.emitClose(cursor)
	return nil
}

// emitDistinctCheck emits a deduplication check using an ephemeral table.
// If the row already exists, jumps to skipLabel. Otherwise inserts and continues.
func (b *Build) emitDistinctCheck(distinctCursor, resultBase, nResult int, skipLabel int) {
	recReg := b.b.AllocReg(1)
	b.emitMakeRecord(resultBase, nResult, recReg)
	// If found, skip this row (it's a duplicate)
	b.b.EmitJump(vdbe.OpFound, distinctCursor, skipLabel, recReg)
	// Not found: insert into dedup table and continue
	b.emitIdxInsert(distinctCursor, recReg)
}

// emitNestedLoopJoin emits nested loop join code with support for
// INNER JOIN, LEFT JOIN, NATURAL JOIN, and USING.
func (b *Build) emitNestedLoopJoin(stmt *SelectStmt, resultBase int, resultCols []*resultColumn,
	needSorter bool, sorterCursor, orderByBase int, needDistinct bool, distinctCursor int, emptyLabel, loopEndLabel int) error {

	nTables := len(b.tables)
	from := stmt.From
	if from == nil {
		return fmt.Errorf("no FROM clause for join")
	}

	// Build join conditions for each table (from NATURAL/USING/ON)
	joinConds := make([]*Expr, nTables)
	for i := 1; i < nTables; i++ {
		if i < len(from.Tables) {
			tref := from.Tables[i]
			if tref.On != nil {
				joinConds[i] = tref.On
			} else if tref.Natural || len(tref.Using) > 0 {
				joinConds[i] = b.buildJoinCondition(from, i)
			}
		}
	}

	// Determine which tables are LEFT JOINs and allocate match flag registers
	isLeftJoin := make([]bool, nTables)
	matchFlags := make([]int, nTables)
	for i := 1; i < nTables; i++ {
		if i < len(from.Tables) {
			tref := from.Tables[i]
			isLeftJoin[i] = tref.JoinType == JoinLeft
			if isLeftJoin[i] {
				matchFlags[i] = b.b.AllocReg(1)
			}
		}
	}

	// Labels for each loop level
	loopBodies := make([]int, nTables)
	endLabels := make([]int, nTables)   // target when Rewind finds empty table
	skipLabels := make([]int, nTables)  // target when join condition fails

	// Outer loop body (table 0 is already rewound by caller)
	loopBodies[0] = b.b.NewLabel()
	b.b.DefineLabel(loopBodies[0])

	// For each table from 1 to N-1, emit Rewind, loop body, and condition check
	for i := 1; i < nTables; i++ {
		// For LEFT JOIN: initialize match flag to 0
		if matchFlags[i] != 0 {
			b.emitInteger(0, matchFlags[i])
		}

		// Rewind inner table; jump to endLabels[i] if empty
		endLabels[i] = b.b.NewLabel()
		b.b.EmitJump(vdbe.OpRewind, b.tables[i].cursor, endLabels[i], 0)

		// Loop body
		loopBodies[i] = b.b.NewLabel()
		b.b.DefineLabel(loopBodies[i])

		// Check join condition for this table
		if joinConds[i] != nil {
			skipLabels[i] = b.b.NewLabel()
			condReg := b.b.AllocReg(1)
			if err := b.compileExpr(joinConds[i], condReg); err != nil {
				return err
			}
			// If condition is false (0), skip to skipLabels[i]
			b.b.EmitJump(vdbe.OpIfNot, condReg, skipLabels[i], 0)

			// For LEFT JOIN: set match flag when condition passes
			if matchFlags[i] != 0 {
				b.emitInteger(1, matchFlags[i])
			}
		}
	}

	// === At deepest nesting level ===

	// Evaluate WHERE condition
	if stmt.Where != nil {
		whereSkip := b.b.NewLabel()
		whereReg := b.b.AllocReg(1)
		if err := b.compileExpr(stmt.Where, whereReg); err != nil {
			return err
		}
		b.b.EmitJump(vdbe.OpIfNot, whereReg, whereSkip, 0)

		// Compute result columns and output
		if err := b.emitJoinResultRow(resultBase, resultCols, needSorter, sorterCursor, orderByBase, needDistinct, distinctCursor); err != nil {
			return err
		}

		b.b.DefineLabel(whereSkip)
	} else {
		if err := b.emitJoinResultRow(resultBase, resultCols, needSorter, sorterCursor, orderByBase, needDistinct, distinctCursor); err != nil {
			return err
		}
	}

	// === Close loops from inner to outer ===
	for i := nTables - 1; i >= 1; i-- {
		// Define skip label (jumped to when join condition fails)
		if skipLabels[i] != 0 {
			b.b.DefineLabel(skipLabels[i])
		}

		// Next for this table's loop
		b.emitNext(b.tables[i].cursor, loopBodies[i])

		// Define end label (jumped to when Rewind finds empty table)
		b.b.DefineLabel(endLabels[i])

		// For LEFT JOIN: check match flag and emit NULL row
		if matchFlags[i] != 0 {
			hasMatch := b.b.NewLabel()
			b.b.EmitJump(vdbe.OpIfPos, matchFlags[i], hasMatch, 0)

			// No match: set NullRow for this table and all subsequent tables
			for j := i; j < nTables; j++ {
				b.b.Emit(vdbe.OpNullRow, b.tables[j].cursor, 0, 0)
			}

			// Emit output row with NULLs for unmatched tables
			if err := b.emitJoinResultRow(resultBase, resultCols, needSorter, sorterCursor, orderByBase, needDistinct, distinctCursor); err != nil {
				return err
			}

			b.b.DefineLabel(hasMatch)
		}
	}

	// Next for outer table (table 0)
	b.emitNext(b.tables[0].cursor, loopBodies[0])
	b.b.DefineLabel(loopEndLabel)

	// Close all cursors
	for _, entry := range b.tables {
		b.emitClose(entry.cursor)
	}
	return nil
}

// emitJoinResultRow computes result columns and emits a ResultRow (or SorterInsert).
func (b *Build) emitJoinResultRow(resultBase int, resultCols []*resultColumn,
	needSorter bool, sorterCursor, orderByBase int, needDistinct bool, distinctCursor int) error {

	for i, rc := range resultCols {
		if err := b.compileExpr(rc.Expr, resultBase+i); err != nil {
			return err
		}
	}

	if needSorter {
		recReg := b.b.AllocReg(1)
		b.emitMakeRecord(resultBase, len(resultCols), recReg)
		b.emitSorterInsert(sorterCursor, recReg)
	} else if needDistinct {
		skipLabel := b.b.NewLabel()
		b.emitDistinctCheck(distinctCursor, resultBase, len(resultCols), skipLabel)
		b.emitResultRow(resultBase, len(resultCols))
		b.b.DefineLabel(skipLabel)
	} else {
		b.emitResultRow(resultBase, len(resultCols))
	}
	return nil
}

// buildJoinCondition generates the join condition expression for NATURAL or USING joins.
// For NATURAL, it finds common columns between the left tables and the current table.
// For USING, it uses the specified columns.
func (b *Build) buildJoinCondition(from *FromClause, tableIdx int) *Expr {
	if tableIdx >= len(from.Tables) || tableIdx < 1 {
		return nil
	}

	tref := from.Tables[tableIdx]
	var usingCols []string

	if len(tref.Using) > 0 {
		usingCols = tref.Using
	} else if tref.Natural {
		// Find common columns between the left side and this table
		rightInfo := b.tables[tableIdx].table
		if rightInfo == nil {
			return nil
		}

		rightCols := make(map[string]bool)
		for _, c := range rightInfo.Columns {
			rightCols[strings.ToUpper(c.Name)] = true
		}

		// Check all previous tables for common columns
		for prevIdx := 0; prevIdx < tableIdx; prevIdx++ {
			prevInfo := b.tables[prevIdx].table
			if prevInfo == nil {
				continue
			}
			for _, c := range prevInfo.Columns {
				if rightCols[strings.ToUpper(c.Name)] {
					// Check if already in usingCols
					found := false
					for _, uc := range usingCols {
						if strings.EqualFold(uc, c.Name) {
							found = true
							break
						}
					}
					if !found {
						usingCols = append(usingCols, c.Name)
					}
				}
			}
		}
	}

	if len(usingCols) == 0 {
		return nil
	}

	// Build equality expressions: left.col = right.col AND ...
	var cond *Expr
	for _, col := range usingCols {
		// Find which previous table has this column
		var leftAlias string
		for prevIdx := 0; prevIdx < tableIdx; prevIdx++ {
			prevInfo := b.tables[prevIdx].table
			if prevInfo != nil && prevInfo.FindColumn(col) >= 0 {
				// Use the alias or name of the previous table
				leftAlias = b.tables[prevIdx].alias
				break
			}
		}

		rightAlias := b.tables[tableIdx].alias

		eq := &Expr{
			Kind: ExprBinaryOp,
			Op:   "=",
			Left: &Expr{Kind: ExprColumnRef, Table: leftAlias, Name: col},
			Right: &Expr{Kind: ExprColumnRef, Table: rightAlias, Name: col},
		}

		if cond == nil {
			cond = eq
		} else {
			cond = &Expr{
				Kind: ExprBinaryOp,
				Op:   "AND",
				Left: cond,
				Right: eq,
			}
		}
	}

	return cond
}

// emitSortedOutput reads from a sorter and emits ResultRow.
func (b *Build) emitSortedOutput(sorterCursor, nResult int) error {
	sortEmpty := b.b.NewLabel()
	sortBody := b.emitSorterSort(sorterCursor, sortEmpty)

	// Allocate registers for reading sorter data
	dataReg := b.b.AllocReg(nResult)

	// Read all columns from the sorter
	for i := 0; i < nResult; i++ {
		b.emitColumn(sorterCursor, i, dataReg+i)
	}
	b.emitResultRow(dataReg, nResult)
	b.emitSorterNext(sorterCursor, sortBody)
	b.b.DefineLabel(sortEmpty)
	return nil
}

// emitSortedDistinctOutput reads from a sorter and emits ResultRow, deduplicating.
func (b *Build) emitSortedDistinctOutput(sorterCursor, nResult int) error {
	sortEmpty := b.b.NewLabel()
	sortBody := b.emitSorterSort(sorterCursor, sortEmpty)

	// Open ephemeral table for dedup
	distinctCursor := b.b.AllocCursor()
	b.emitOpenEphemeral(distinctCursor, nResult)

	loopEnd := b.b.NewLabel()
	dataReg := b.b.AllocReg(nResult)

	// Read all columns from the sorter
	for i := 0; i < nResult; i++ {
		b.emitColumn(sorterCursor, i, dataReg+i)
	}

	// Dedup check
	skipLabel := b.b.NewLabel()
	b.emitDistinctCheck(distinctCursor, dataReg, nResult, skipLabel)
	b.emitResultRow(dataReg, nResult)

	b.b.DefineLabel(skipLabel)
	b.emitSorterNext(sorterCursor, sortBody)
	b.b.DefineLabel(loopEnd)
	b.b.DefineLabel(sortEmpty)

	b.emitClose(distinctCursor)
	b.emitClose(sorterCursor)
	return nil
}

// emitLimit emits LIMIT/OFFSET handling.
func (b *Build) emitLimit(stmt *SelectStmt) error {
	if stmt.Limit == nil {
		return nil
	}

	b.limitEndLabel = b.b.NewLabel()

	// Evaluate LIMIT value
	b.limitReg = b.b.AllocReg(1)
	if err := b.compileExpr(stmt.Limit, b.limitReg); err != nil {
		return err
	}

	// If there's an OFFSET, evaluate it
	if stmt.Offset != nil {
		b.offsetReg = b.b.AllocReg(1)
		if err := b.compileExpr(stmt.Offset, b.offsetReg); err != nil {
			return err
		}
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
	needDistinct bool, distinctCursor int,
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
	} else if needDistinct {
		skipLabel := b.b.NewLabel()
		b.emitDistinctCheck(distinctCursor, resultBase, len(resultCols), skipLabel)
		b.emitResultRow(resultBase, len(resultCols))
		b.b.DefineLabel(skipLabel)
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

// resolveGroupByExpr resolves GROUP BY expressions:
// - Column number references (e.g., GROUP BY 1) → resolved to result column expression
// - Alias references (e.g., GROUP BY x where x is SELECT ... AS x) → resolved to aliased expression
func (b *Build) resolveGroupByExpr(expr *Expr, resultCols []*resultColumn) (*Expr, error) {
	// Check for column number reference (integer literal)
	if expr.Kind == ExprLiteral && expr.LiteralType == "integer" {
		idx := int(expr.IntValue)
		if idx < 1 || idx > len(resultCols) {
			return nil, fmt.Errorf("GROUP BY column number %d is out of range (1..%d)", idx, len(resultCols))
		}
		// Use the corresponding result column's expression
		return resultCols[idx-1].Expr, nil
	}

	// Check for alias reference (column ref matching a result column alias)
	if expr.Kind == ExprColumnRef && expr.Table == "" {
		for _, rc := range resultCols {
			if rc.Alias != "" && strings.EqualFold(rc.Alias, expr.Name) {
				return rc.Expr, nil
			}
		}
	}

	return expr, nil
}

// resolveHavingAliases resolves alias references in HAVING expressions.
// It replaces column references that match result column aliases with the aliased expression.
func (b *Build) resolveHavingAliases(expr *Expr, resultCols []*resultColumn) *Expr {
	if expr == nil {
		return nil
	}

	// If this is a column ref matching an alias, replace it
	if expr.Kind == ExprColumnRef && expr.Table == "" {
		for _, rc := range resultCols {
			if rc.Alias != "" && strings.EqualFold(rc.Alias, expr.Name) {
				return rc.Expr
			}
		}
	}

	// Recursively resolve in sub-expressions
	if expr.Left != nil {
		expr.Left = b.resolveHavingAliases(expr.Left, resultCols)
	}
	if expr.Right != nil {
		expr.Right = b.resolveHavingAliases(expr.Right, resultCols)
	}
	for i, arg := range expr.Args {
		expr.Args[i] = b.resolveHavingAliases(arg, resultCols)
	}
	if expr.Low != nil {
		expr.Low = b.resolveHavingAliases(expr.Low, resultCols)
	}
	if expr.High != nil {
		expr.High = b.resolveHavingAliases(expr.High, resultCols)
	}
	if expr.Pattern != nil {
		expr.Pattern = b.resolveHavingAliases(expr.Pattern, resultCols)
	}
	for _, w := range expr.WhenList {
		w.Condition = b.resolveHavingAliases(w.Condition, resultCols)
		w.Result = b.resolveHavingAliases(w.Result, resultCols)
	}
	if expr.ElseExpr != nil {
		expr.ElseExpr = b.resolveHavingAliases(expr.ElseExpr, resultCols)
	}

	return expr
}

// collectAggFuncs walks an expression tree and collects all aggregate function Expr nodes.
func (b *Build) collectAggFuncs(expr *Expr, result *[]*Expr) {
	if expr == nil {
		return
	}
	if expr.Kind == ExprFunctionCall && isAggregate(expr.FunctionName) {
		*result = append(*result, expr)
		return
	}
	b.collectAggFuncs(expr.Left, result)
	b.collectAggFuncs(expr.Right, result)
	for _, arg := range expr.Args {
		b.collectAggFuncs(arg, result)
	}
	b.collectAggFuncs(expr.Low, result)
	b.collectAggFuncs(expr.High, result)
	b.collectAggFuncs(expr.Pattern, result)
	for _, w := range expr.WhenList {
		b.collectAggFuncs(w.Condition, result)
		b.collectAggFuncs(w.Result, result)
	}
	b.collectAggFuncs(expr.ElseExpr, result)
}
