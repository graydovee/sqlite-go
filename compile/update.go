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
	for _, set := range stmt.Sets {
		if len(set.Columns) == 1 {
			// Simple SET col = expr
			idx := findColumnIndex(tbl, set.Columns[0])
			if idx < 0 {
				return fmt.Errorf("no such column: %s", set.Columns[0])
			}
			if err := b.compileExpr(set.Value, newBase+idx); err != nil {
				return err
			}
		} else {
			// Tuple SET: (a, b) = expr
			// For now, assume the expr is a subquery or row value
			for i, colName := range set.Columns {
				idx := findColumnIndex(tbl, colName)
				if idx < 0 {
					return fmt.Errorf("no such column: %s", colName)
				}
				// This is simplified; proper tuple support would require
				// expanding the RHS expression
				_ = i
				if err := b.compileExpr(set.Value, newBase+idx); err != nil {
					return err
				}
			}
		}
	}

	// Evaluate WHERE condition
	if stmt.Where != nil {
		skipUpdate := b.b.NewLabel()
		if err := b.compileCondition(stmt.Where, skipUpdate, loopEndLabel, true); err != nil {
			return err
		}
		b.b.DefineLabel(skipUpdate)
	}

	// Delete old index entries
	b.deleteFromIndexes(indexCursors, oldBase, nCols, rowidReg)

	// Build new record and update
	b.emitMakeRecord(newBase, nCols, recReg)
	b.emitUpdate(cursor, recReg)

	// Insert new index entries
	b.updateIndexes(indexCursors, newBase, nCols, rowidReg)

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
