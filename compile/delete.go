package compile

import (
	"fmt"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// compileDelete compiles a DELETE statement into VDBE bytecode.
func (b *Build) compileDelete(stmt *DeleteStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil DELETE statement")
	}
	if stmt.Table == nil {
		return fmt.Errorf("DELETE missing table reference")
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
	valueBase := b.b.AllocReg(nCols)
	rowidReg := b.b.AllocReg(1)

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

	emptyLabel := b.b.NewLabel()
	loopEndLabel := b.b.NewLabel()
	loopBody := b.b.NewLabel()

	b.b.EmitJump(vdbe.OpRewind, cursor, emptyLabel, 0)
	b.b.DefineLabel(loopBody)

	// Read the current rowid
	b.emitRowid(cursor, rowidReg)

	// Read current column values for index key building
	for i := 0; i < nCols; i++ {
		b.emitColumn(cursor, i, valueBase+i)
	}

	var skipDelete int
	if stmt.Where != nil {
		// Evaluate WHERE condition
		skipDelete = b.b.NewLabel()
		doDelete := b.b.NewLabel()
		if err := b.compileCondition(stmt.Where, doDelete, skipDelete, true); err != nil {
			return err
		}
		b.b.DefineLabel(doDelete)
	}

	// Emit RETURNING before deleting (so we can still read the row)
	if len(stmt.Returning) > 0 {
		for i, rc := range returningCols {
			if err := b.compileExpr(rc.Expr, returningBase+i); err != nil {
				return err
			}
		}
		b.emitResultRow(returningBase, len(returningCols))
	}

	// Delete from indexes
	b.deleteFromIndexes(indexCursors, valueBase, nCols, rowidReg)

	// Delete from the main table
	b.emitDelete(cursor)

	if stmt.Where != nil {
		b.b.DefineLabel(skipDelete)
	}

	// Handle ORDER BY + LIMIT for DELETE (SQLite extension)
	// For ORDER BY + LIMIT, we need a counter
	if stmt.Limit != nil {
		limitReg := b.b.AllocReg(1)
		if err := b.compileExpr(stmt.Limit, limitReg); err != nil {
			return err
		}
		b.b.EmitJump(vdbe.OpIfPos, limitReg, loopBody, 0)
	} else {
		b.emitNext(cursor, loopBody)
	}

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
