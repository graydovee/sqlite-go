package compile

import (
	"fmt"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// compileUpdate compiles an UPDATE statement into VDBE bytecode.
func (b *Build) compileUpdate(stmt *UpdateStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil UPDATE statement")
	}
	if stmt.Table == nil {
		return fmt.Errorf("UPDATE missing table reference")
	}

	// Look up the target table
	tbl, err := b.lookupTable(stmt.Table.Name)
	if err != nil {
		return err
	}

	b.emitInit()
	b.emitTransaction(0, true)

	// Open the table for writing
	cursor := b.b.AllocCursor()
	b.emitOpenWrite(cursor, tbl.RootPage)

	// Register table for column resolution
	b.addTableRef(stmt.Table.Name, stmt.Table.Alias, tbl, cursor)

	// Open indexes for updating
	indexCursors := b.openTableIndexes(tbl, true)

	nCols := len(tbl.Columns)

	// Allocate registers for old values, new values, and rowid
	oldBase := b.b.AllocReg(nCols)
	newBase := b.b.AllocReg(nCols)
	rowidReg := b.b.AllocReg(1)
	recReg := b.b.AllocReg(1)

	// Handle RETURNING columns
	var returningCols []*resultColumn
	var returningBase int
	if len(stmt.Returning) > 0 {
		returningCols, err = b.expandResultColumns(stmt.Returning)
		if err != nil {
			return err
		}
		returningBase = b.b.AllocReg(len(returningCols))
	}

	// Determine which columns are being updated
	updateCols := make([]bool, nCols)
	for _, set := range stmt.Sets {
		for _, colName := range set.Columns {
			idx := findColumnIndex(tbl, colName)
			if idx < 0 {
				return fmt.Errorf("no such column: %s", colName)
			}
			updateCols[idx] = true
		}
	}

	if stmt.From != nil && len(stmt.From.Tables) > 0 {
		// UPDATE ... FROM ... pattern: use nested loop join
		return b.compileUpdateFrom(stmt, tbl, cursor, indexCursors,
			oldBase, newBase, rowidReg, recReg, nCols, updateCols,
			returningCols, returningBase)
	}

	// Simple UPDATE (no FROM clause)
	// Scan loop
	emptyLabel := b.b.NewLabel()
	loopEndLabel := b.b.NewLabel()
	loopBody := b.b.NewLabel()

	b.b.EmitJump(vdbe.OpRewind, cursor, emptyLabel, 0)
	b.b.DefineLabel(loopBody)

	// Read the current rowid
	b.emitRowid(cursor, rowidReg)

	// Read current column values into oldBase
	for i := 0; i < nCols; i++ {
		b.emitColumn(cursor, i, oldBase+i)
	}

	// Copy old values to new values
	for i := 0; i < nCols; i++ {
		b.emitSCopy(oldBase+i, newBase+i)
	}

	// Apply SET clauses to compute new values
	if err := b.applyUpdateSets(stmt.Sets, tbl, newBase); err != nil {
		return err
	}

	// Evaluate WHERE condition
	var skipUpdate int
	if stmt.Where != nil {
		skipUpdate = b.b.NewLabel()
		doUpdate := b.b.NewLabel()
		if err := b.compileCondition(stmt.Where, doUpdate, skipUpdate, true); err != nil {
			return err
		}
		b.b.DefineLabel(doUpdate)
	}

	// Delete old index entries
	b.deleteFromIndexes(indexCursors, oldBase, nCols, rowidReg)

	// Build new record and update
	b.emitMakeRecord(newBase, nCols, recReg)
	b.emitUpdate(cursor, rowidReg, recReg)

	// Insert new index entries
	b.updateIndexes(indexCursors, newBase, nCols, rowidReg)

	// Emit RETURNING after update
	if len(stmt.Returning) > 0 {
		for i, rc := range returningCols {
			if err := b.compileExpr(rc.Expr, returningBase+i); err != nil {
				return err
			}
		}
		b.emitResultRow(returningBase, len(returningCols))
	}

	if stmt.Where != nil {
		b.b.DefineLabel(skipUpdate)
	}

	b.emitNext(cursor, loopBody)
	b.b.DefineLabel(loopEndLabel)
	b.b.DefineLabel(emptyLabel)

	// Close cursors
	b.emitClose(cursor)
	for _, ic := range indexCursors {
		b.emitClose(ic.cursor)
	}

	b.emitHalt(0)
	return nil
}

// compileUpdateFrom handles UPDATE ... FROM ... WHERE with a nested loop join.
// For each row in the target table, scans the FROM table(s) to find matches
// based on the WHERE condition.
func (b *Build) compileUpdateFrom(stmt *UpdateStmt, tbl *TableInfo, cursor int,
	indexCursors []indexCursor, oldBase, newBase, rowidReg, recReg, nCols int,
	updateCols []bool, returningCols []*resultColumn, returningBase int) error {

	// Open FROM table cursors and register them for column resolution
	type fromInfo struct {
		cursor int
		tbl    *TableInfo
		nCols  int
	}
	var fromTables []fromInfo

	for _, tref := range stmt.From.Tables {
		fromTbl, err := b.lookupTable(tref.Name)
		if err != nil {
			return err
		}
		fromCursor := b.b.AllocCursor()
		b.emitOpenRead(fromCursor, fromTbl.RootPage)

		alias := tref.Alias
		if alias == "" {
			alias = tref.Name
		}
		b.addTableRef(tref.Name, alias, fromTbl, fromCursor)
		fromTables = append(fromTables, fromInfo{
			cursor: fromCursor,
			tbl:    fromTbl,
			nCols:  len(fromTbl.Columns),
		})
	}

	// Outer loop: scan the target table
	emptyLabel := b.b.NewLabel()
	loopEndLabel := b.b.NewLabel()
	outerBody := b.b.NewLabel()

	b.b.EmitJump(vdbe.OpRewind, cursor, emptyLabel, 0)
	b.b.DefineLabel(outerBody)

	// Read current target row
	b.emitRowid(cursor, rowidReg)
	for i := 0; i < nCols; i++ {
		b.emitColumn(cursor, i, oldBase+i)
	}

	// Copy old to new
	for i := 0; i < nCols; i++ {
		b.emitSCopy(oldBase+i, newBase+i)
	}

	// Apply SET clauses (before inner loop, since SET may reference FROM columns
	// that will be available during inner loop evaluation)
	// Actually, SET expressions may reference FROM columns, so we must defer
	// SET evaluation to inside the inner loop where FROM columns are available.

	// Inner loop: for single FROM table, scan it
	// For now, support single FROM table (most common case)
	if len(fromTables) == 1 {
		ft := fromTables[0]
		noMatch := b.b.NewLabel()
		innerBody := b.b.NewLabel()
		nextOuterRow := b.b.NewLabel()

		// Rewind the FROM table
		b.b.EmitJump(vdbe.OpRewind, ft.cursor, noMatch, 0)
		b.b.DefineLabel(innerBody)

		// Read FROM table columns into dedicated registers
		fromBase := b.b.AllocReg(ft.nCols)
		for i := 0; i < ft.nCols; i++ {
			b.emitColumn(ft.cursor, i, fromBase+i)
		}

		// Evaluate WHERE condition (join predicate)
		if stmt.Where != nil {
			notMatch := b.b.NewLabel()
			matchLabel := b.b.NewLabel()
			if err := b.compileCondition(stmt.Where, matchLabel, notMatch, true); err != nil {
				return err
			}
			b.b.DefineLabel(matchLabel)

			// Match found: apply SET clauses and update
			if err := b.applyUpdateSets(stmt.Sets, tbl, newBase); err != nil {
				return err
			}

			// Delete old index entries, update record, insert new index entries
			b.deleteFromIndexes(indexCursors, oldBase, nCols, rowidReg)
			b.emitMakeRecord(newBase, nCols, recReg)
			b.emitUpdate(cursor, rowidReg, recReg)
			b.updateIndexes(indexCursors, newBase, nCols, rowidReg)

			// Emit RETURNING
			if len(stmt.Returning) > 0 {
				for i, rc := range returningCols {
					if err := b.compileExpr(rc.Expr, returningBase+i); err != nil {
						return err
					}
				}
				b.emitResultRow(returningBase, len(returningCols))
			}

			// After first match, move to next outer row
			b.b.EmitJump(vdbe.OpGoto, 0, nextOuterRow, 0)

			b.b.DefineLabel(notMatch)
		}

		// Continue inner loop
		b.emitNext(ft.cursor, innerBody)
		b.b.DefineLabel(noMatch)

		// No match found for this outer row - continue to next
		b.b.DefineLabel(nextOuterRow)

		// Reset newBase back to oldBase for next iteration
		for i := 0; i < nCols; i++ {
			b.emitSCopy(oldBase+i, newBase+i)
		}
	}

	b.emitNext(cursor, outerBody)
	b.b.DefineLabel(loopEndLabel)
	b.b.DefineLabel(emptyLabel)

	// Close cursors
	b.emitClose(cursor)
	for _, ic := range indexCursors {
		b.emitClose(ic.cursor)
	}
	for _, ft := range fromTables {
		b.emitClose(ft.cursor)
	}

	b.emitHalt(0)
	return nil
}

// applyUpdateSets applies SET clauses to compute new column values.
func (b *Build) applyUpdateSets(sets []*SetClause, tbl *TableInfo, newBase int) error {
	for _, set := range sets {
		if len(set.Columns) == 1 {
			idx := findColumnIndex(tbl, set.Columns[0])
			if idx < 0 {
				return fmt.Errorf("no such column: %s", set.Columns[0])
			}
			if err := b.compileExpr(set.Value, newBase+idx); err != nil {
				return err
			}
		} else {
			for i, colName := range set.Columns {
				idx := findColumnIndex(tbl, colName)
				if idx < 0 {
					return fmt.Errorf("no such column: %s", colName)
				}
				_ = i
				if err := b.compileExpr(set.Value, newBase+idx); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
