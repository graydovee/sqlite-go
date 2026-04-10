package compile

import (
	"fmt"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// compileInsert compiles an INSERT statement into VDBE bytecode.
func (b *Build) compileInsert(stmt *InsertStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil INSERT statement")
	}
	if stmt.Table == nil {
		return fmt.Errorf("INSERT missing table reference")
	}

	// Look up the target table
	tbl, err := b.lookupTable(stmt.Table.Name)
	if err != nil {
		return err
	}

	startLabel := b.emitInit()
	b.emitTransaction(0, true)

	// Open the table for writing
	cursor := b.b.AllocCursor()
	b.emitOpenWrite(cursor, tbl.RootPage)

	// Register the table for column resolution
	b.addTableRef(stmt.Table.Name, stmt.Table.Alias, tbl, cursor)

	// Also open any indexes on this table for updating
	indexCursors := b.openTableIndexes(tbl, true)

	if stmt.DefaultValues {
		err := b.compileInsertDefaultValues(cursor, tbl, indexCursors)
		if err != nil {
			return err
		}
	} else if stmt.Select != nil {
		err := b.compileInsertSelect(cursor, tbl, indexCursors, stmt)
		if err != nil {
			return err
		}
	} else if len(stmt.Values) > 0 {
		err := b.compileInsertValues(cursor, tbl, indexCursors, stmt)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("INSERT has no VALUES, SELECT, or DEFAULT VALUES")
	}

	b.emitHalt(0)
	b.b.DefineLabel(startLabel)
	return nil
}

// compileInsertValues compiles INSERT ... VALUES (...), (...), ...
func (b *Build) compileInsertValues(cursor int, tbl *TableInfo, indexCursors []indexCursor, stmt *InsertStmt) error {
	nCols := len(tbl.Columns)
	valueBase := b.b.AllocReg(nCols)
	rowidReg := b.b.AllocReg(1)
	recReg := b.b.AllocReg(1)

	for _, row := range stmt.Values {
		// Generate a new rowid
		b.emitNewRowid(cursor, rowidReg)

		// Evaluate each column value
		if len(stmt.Columns) > 0 {
			// Explicit column list: only those columns are set
			// Initialize all to NULL first
			for i := 0; i < nCols; i++ {
				b.emitNull(valueBase + i)
			}
			// Set specified columns
			for i, colName := range stmt.Columns {
				colIdx := findColumnIndex(tbl, colName)
				if colIdx < 0 {
					return fmt.Errorf("no such column: %s", colName)
				}
				if i < len(row) {
					if err := b.compileExpr(row[i], valueBase+colIdx); err != nil {
						return err
					}
				}
			}
		} else {
			// No column list: values correspond to all columns in order
			for i := 0; i < nCols; i++ {
				if i < len(row) {
					if err := b.compileExpr(row[i], valueBase+i); err != nil {
						return err
					}
				} else {
					b.emitNull(valueBase + i)
				}
			}
		}

		// Build the record
		b.emitMakeRecord(valueBase, nCols, recReg)

		// Insert the record
		b.emitInsert(cursor, recReg, rowidReg)

		// Update indexes
		b.updateIndexes(indexCursors, valueBase, nCols, rowidReg)
	}

	b.emitClose(cursor)
	for _, ic := range indexCursors {
		b.emitClose(ic.cursor)
	}
	return nil
}

// compileInsertSelect compiles INSERT ... SELECT ...
func (b *Build) compileInsertSelect(cursor int, tbl *TableInfo, indexCursors []indexCursor, stmt *InsertStmt) error {
	nCols := len(tbl.Columns)

	// Open an ephemeral table to collect SELECT results
	selectCursor := b.b.AllocCursor()
	b.emitOpenEphemeral(selectCursor, nCols)

	// Compile the SELECT into the ephemeral table
	savedTables := b.tables
	savedTableMap := b.tableMap
	// Keep the table reference for column resolution
	b.tables = nil
	b.tableMap = make(map[string]*tableEntry)

	if err := b.compileSelectInner(stmt.Select, selectCursor); err != nil {
		b.tables = savedTables
		b.tableMap = savedTableMap
		return err
	}

	b.tables = savedTables
	b.tableMap = savedTableMap

	// Now read from the ephemeral table and insert into the target table
	valueBase := b.b.AllocReg(nCols)
	rowidReg := b.b.AllocReg(1)
	recReg := b.b.AllocReg(1)

	emptyLabel := b.b.NewLabel()
	loopBody := b.b.NewLabel()

	b.b.EmitJump(vdbe.OpRewind, selectCursor, emptyLabel, 0)
	b.b.DefineLabel(loopBody)

	// Read values from the select cursor
	for i := 0; i < nCols; i++ {
		b.emitColumn(selectCursor, i, valueBase+i)
	}

	// Generate rowid
	b.emitNewRowid(cursor, rowidReg)

	// Build and insert record
	b.emitMakeRecord(valueBase, nCols, recReg)
	b.emitInsert(cursor, recReg, rowidReg)

	// Update indexes
	b.updateIndexes(indexCursors, valueBase, nCols, rowidReg)

	b.emitNext(selectCursor, loopBody)
	b.b.DefineLabel(emptyLabel)
	b.emitClose(selectCursor)
	b.emitClose(cursor)
	for _, ic := range indexCursors {
		b.emitClose(ic.cursor)
	}
	return nil
}

// compileInsertDefaultValues compiles INSERT DEFAULT VALUES.
func (b *Build) compileInsertDefaultValues(cursor int, tbl *TableInfo, indexCursors []indexCursor) error {
	nCols := len(tbl.Columns)
	valueBase := b.b.AllocReg(nCols)
	rowidReg := b.b.AllocReg(1)
	recReg := b.b.AllocReg(1)

	// Generate rowid
	b.emitNewRowid(cursor, rowidReg)

	// Set all columns to their defaults (or NULL)
	for i, col := range tbl.Columns {
		if col.Default != nil {
			if err := b.compileExpr(col.Default, valueBase+i); err != nil {
				return err
			}
		} else {
			b.emitNull(valueBase + i)
		}
	}

	// Build and insert record
	b.emitMakeRecord(valueBase, nCols, recReg)
	b.emitInsert(cursor, recReg, rowidReg)

	// Update indexes
	b.updateIndexes(indexCursors, valueBase, nCols, rowidReg)

	b.emitClose(cursor)
	for _, ic := range indexCursors {
		b.emitClose(ic.cursor)
	}
	return nil
}

// indexCursor tracks a cursor opened for an index.
type indexCursor struct {
	cursor int
	index  *IndexInfo
}

// openTableIndexes opens all indexes on a table for writing.
func (b *Build) openTableIndexes(tbl *TableInfo, forWrite bool) []indexCursor {
	var cursors []indexCursor
	if b.schema == nil {
		return cursors
	}
	for _, idx := range b.schema.Indexes {
		if idx.Table == tbl.Name {
			cursor := b.b.AllocCursor()
			if forWrite {
				b.emitOpenWrite(cursor, idx.RootPage)
			} else {
				b.emitOpenRead(cursor, idx.RootPage)
			}
			cursors = append(cursors, indexCursor{cursor: cursor, index: idx})
		}
	}
	return cursors
}

// updateIndexes emits OP_IdxInsert for each index on the table.
func (b *Build) updateIndexes(indexCursors []indexCursor, valueBase, nCols int, rowidReg int) {
	for _, ic := range indexCursors {
		// Build index key from indexed columns
		idxColCount := len(ic.index.Columns)
		keyBase := b.b.AllocReg(idxColCount + 1) // +1 for rowid

		for i, col := range ic.index.Columns {
			colIdx := findColumnIndexByName(b.schema, ic.index.Table, col.Name)
			if colIdx >= 0 {
				b.emitSCopy(valueBase+colIdx, keyBase+i)
			} else {
				b.emitNull(keyBase + i)
			}
		}
		// Append rowid to index key
		b.emitSCopy(rowidReg, keyBase+idxColCount)

		// Make index record and insert
		recReg := b.b.AllocReg(1)
		b.emitMakeRecord(keyBase, idxColCount+1, recReg)
		b.emitIdxInsert(ic.cursor, recReg)
	}
}

// deleteFromIndexes emits OP_IdxDelete for each index on the table.
func (b *Build) deleteFromIndexes(indexCursors []indexCursor, valueBase, nCols int, rowidReg int) {
	for _, ic := range indexCursors {
		idxColCount := len(ic.index.Columns)
		keyBase := b.b.AllocReg(idxColCount + 1)

		for i, col := range ic.index.Columns {
			colIdx := findColumnIndexByName(b.schema, ic.index.Table, col.Name)
			if colIdx >= 0 {
				b.emitSCopy(valueBase+colIdx, keyBase+i)
			} else {
				b.emitNull(keyBase + i)
			}
		}
		b.emitSCopy(rowidReg, keyBase+idxColCount)

		recReg := b.b.AllocReg(1)
		b.emitMakeRecord(keyBase, idxColCount+1, recReg)
		b.emitIdxDelete(ic.cursor)
	}
}

// findColumnIndex returns the 0-based index of a column in a table, or -1.
func findColumnIndex(tbl *TableInfo, name string) int {
	for i, col := range tbl.Columns {
		if caseInsensitiveEqual(col.Name, name) {
			return i
		}
	}
	return -1
}

// findColumnIndexByName finds a column index in a table by name, using the schema.
func findColumnIndexByName(schema *Schema, tableName, colName string) int {
	if schema == nil {
		return -1
	}
	tbl, ok := schema.Tables[tableName]
	if !ok {
		return -1
	}
	return findColumnIndex(tbl, colName)
}

// caseInsensitiveEqual compares two strings case-insensitively.
func caseInsensitiveEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if toUpper(a[i]) != toUpper(b[i]) {
			return false
		}
	}
	return true
}

func toUpper(c byte) byte {
	if c >= 'a' && c <= 'z' {
		return c - 32
	}
	return c
}
