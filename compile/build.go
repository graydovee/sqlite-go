package compile

import (
	"fmt"
	"strings"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// Schema holds the database schema information needed during compilation.
type Schema struct {
	Tables  map[string]*TableInfo
	Indexes map[string]*IndexInfo
}

// NewSchema creates an empty schema.
func NewSchema() *Schema {
	return &Schema{
		Tables:  make(map[string]*TableInfo),
		Indexes: make(map[string]*IndexInfo),
	}
}

// AddTable adds a table to the schema.
func (s *Schema) AddTable(tbl *TableInfo) {
	if s == nil || tbl == nil {
		return
	}
	s.Tables[tbl.Name] = tbl
}

// AddIndex adds an index to the schema.
func (s *Schema) AddIndex(idx *IndexInfo) {
	if s == nil || idx == nil {
		return
	}
	s.Indexes[idx.Name] = idx
}

// FindColumn finds a column by name in a table, returning its index or -1.
func (t *TableInfo) FindColumn(name string) int {
	if t == nil {
		return -1
	}
	upper := strings.ToUpper(name)
	for i, c := range t.Columns {
		if strings.ToUpper(c.Name) == upper {
			return i
		}
	}
	return -1
}

// ColumnCount returns the number of columns in the table.
func (t *TableInfo) ColumnCount() int {
	if t == nil {
		return 0
	}
	return len(t.Columns)
}

// TableInfo describes a table in the schema.
type TableInfo struct {
	Name      string
	Columns   []ColumnInfo
	HasRowid  bool // true if table has implicit rowid
	RootPage  int
	AutoIndex bool // true if autoindex created for this table
}

// ColumnInfo describes a column in a table.
type ColumnInfo struct {
	Name         string
	Type         string
	NotNull      bool
	Default      *Expr
	PrimaryKey   bool
	Autoincrement bool
	Collation    string
}

// IndexInfo describes an index in the schema.
type IndexInfo struct {
	Name     string
	Table    string
	Columns  []IndexColumn
	Unique   bool
	RootPage int
	Where    *Expr // partial index predicate
}

// IndexColumn describes a column in an index.
type IndexColumn struct {
	Name  string
	Order SortOrder // ascending or descending
}

// tableEntry tracks a table reference during compilation.
type tableEntry struct {
	cursor  int
	table   *TableInfo
	alias   string
	name    string
	isOuter bool // true for LEFT/RIGHT/FULL join outer table
}

// Build holds the compilation context for generating VDBE bytecode.
type Build struct {
	b         *Builder
	schema    *Schema
	tables    []*tableEntry // ordered list of table entries for current statement
	tableMap  map[string]*tableEntry
	constants map[string]int // register holding constant values
}

// newBuild creates a new compilation context.
func newBuild(schema *Schema) *Build {
	return &Build{
		b:         NewBuilder(),
		schema:    schema,
		tableMap:  make(map[string]*tableEntry),
		constants: make(map[string]int),
	}
}

// Compile compiles a Statement into a VDBE Program.
func Compile(stmt *Statement, schema *Schema) (*Program, error) {
	if stmt == nil {
		return nil, fmt.Errorf("nil statement")
	}

	bld := newBuild(schema)
	// Reserve register 0 (used as a general-purpose temp).
	bld.b.AllocReg(1)

	switch stmt.Type {
	case StmtSelect:
		if err := bld.compileSelect(stmt.SelectStmt); err != nil {
			return nil, err
		}
	case StmtInsert:
		if err := bld.compileInsert(stmt.InsertStmt); err != nil {
			return nil, err
		}
	case StmtUpdate:
		if err := bld.compileUpdate(stmt.UpdateStmt); err != nil {
			return nil, err
		}
	case StmtDelete:
		if err := bld.compileDelete(stmt.DeleteStmt); err != nil {
			return nil, err
		}
	case StmtCreateTable:
		if err := bld.compileCreateTable(stmt.CreateTable); err != nil {
			return nil, err
		}
	case StmtDropTable:
		if err := bld.compileDropTable(stmt.DropTable); err != nil {
			return nil, err
		}
	case StmtCreateIndex:
		if err := bld.compileCreateIndex(stmt.CreateIndex); err != nil {
			return nil, err
		}
	case StmtDropIndex:
		if err := bld.compileDropIndex(stmt.DropIndex); err != nil {
			return nil, err
		}
	case StmtBegin:
		bld.compileBegin(stmt.BeginStmt)
	case StmtCommit:
		bld.compileCommit()
	case StmtRollback:
		bld.compileRollback(stmt.RollbackStmt)
	default:
		return nil, fmt.Errorf("unsupported statement type: %v", stmt.Type)
	}

	return bld.b.BuildProgram()
}

// lookupTable finds a table in the schema by name (case-insensitive).
func (b *Build) lookupTable(name string) (*TableInfo, error) {
	if b.schema == nil {
		return nil, fmt.Errorf("no schema available")
	}
	upper := strings.ToUpper(name)
	for k, t := range b.schema.Tables {
		if strings.ToUpper(k) == upper {
			return t, nil
		}
	}
	return nil, fmt.Errorf("no such table: %s", name)
}

// lookupIndex finds an index in the schema by name (case-insensitive).
func (b *Build) lookupIndex(name string) (*IndexInfo, error) {
	if b.schema == nil {
		return nil, fmt.Errorf("no schema available")
	}
	upper := strings.ToUpper(name)
	for k, idx := range b.schema.Indexes {
		if strings.ToUpper(k) == upper {
			return idx, nil
		}
	}
	return nil, fmt.Errorf("no such index: %s", name)
}

// addTableRef registers a table reference in the current compilation context.
func (b *Build) addTableRef(name string, alias string, tbl *TableInfo, cursor int) {
	entry := &tableEntry{
		cursor: cursor,
		table:  tbl,
		alias:  alias,
		name:   name,
	}
	b.tables = append(b.tables, entry)
	// Register by alias and by name.
	if alias != "" {
		b.tableMap[strings.ToUpper(alias)] = entry
	}
	b.tableMap[strings.ToUpper(name)] = entry
}

// resolveColumnRef resolves a column reference to (cursor, column_index).
func (b *Build) resolveColumnRef(table, col string) (cursor int, colIdx int, err error) {
	colUpper := strings.ToUpper(col)

	if table != "" {
		entry, ok := b.tableMap[strings.ToUpper(table)]
		if !ok {
			return 0, 0, fmt.Errorf("no such table: %s", table)
		}
		for i, c := range entry.table.Columns {
			if strings.ToUpper(c.Name) == colUpper {
				return entry.cursor, i, nil
			}
		}
		return 0, 0, fmt.Errorf("no such column: %s.%s", table, col)
	}

	// No table qualifier: search all tables.
	var found bool
	for _, entry := range b.tables {
		for i, c := range entry.table.Columns {
			if strings.ToUpper(c.Name) == colUpper {
				if found {
					return 0, 0, fmt.Errorf("ambiguous column: %s", col)
				}
				cursor = entry.cursor
				colIdx = i
				found = true
			}
		}
	}
	if !found {
		return 0, 0, fmt.Errorf("no such column: %s", col)
	}
	return cursor, colIdx, nil
}

// emitHalt emits an OP_Halt instruction.
func (b *Build) emitHalt(errCode int) {
	b.b.Emit(vdbe.OpHalt, errCode, 0, 0)
}

// emitInit emits the standard program prologue.
// Init jumps to the instruction immediately following itself.
func (b *Build) emitInit() {
	b.b.Emit(vdbe.OpInit, 0, b.b.CurrentAddr()+1, 0)
}

// emitTransaction emits OP_Transaction for the given database and mode.
func (b *Build) emitTransaction(db int, write bool) {
	p2 := 0
	if write {
		p2 = 1
	}
	b.b.Emit(vdbe.OpTransaction, db, p2, 0)
}

// emitGoto emits an unconditional jump.
func (b *Build) emitGoto(label int) int {
	return b.b.EmitJump(vdbe.OpGoto, 0, label, 0)
}

// emitNull emits OP_Null to set a register to NULL.
func (b *Build) emitNull(reg int) {
	b.b.Emit(vdbe.OpNull, 0, reg, 0)
}

// emitInteger emits an integer constant into a register.
func (b *Build) emitInteger(val int64, reg int) {
	b.b.EmitP4(vdbe.OpInt64, 0, reg, 0, val, fmt.Sprintf("int(%d)", val))
}

// emitString emits a string constant into a register.
func (b *Build) emitString(s string, reg int) {
	b.b.EmitP4(vdbe.OpString8, 0, reg, 0, s, s)
}

// emitReal emits a float constant into a register.
func (b *Build) emitReal(val float64, reg int) {
	b.b.EmitP4(vdbe.OpReal, 0, reg, 0, val, fmt.Sprintf("real(%g)", val))
}

// emitBlob emits a blob constant into a register.
func (b *Build) emitBlob(data []byte, reg int) {
	b.b.EmitP4(vdbe.OpBlob, len(data), reg, 0, data, "blob")
}

// emitOpenRead opens a cursor for reading a table.
func (b *Build) emitOpenRead(cursor, rootPage int) {
	b.b.EmitComment(vdbe.OpOpenRead, cursor, rootPage, 0, fmt.Sprintf("table root=%d", rootPage))
}

// emitOpenWrite opens a cursor for writing to a table.
func (b *Build) emitOpenWrite(cursor, rootPage int) {
	b.b.EmitComment(vdbe.OpOpenWrite, cursor, rootPage, 0, fmt.Sprintf("table root=%d", rootPage))
}

// emitOpenEphemeral opens an ephemeral table for intermediate results.
func (b *Build) emitOpenEphemeral(cursor, nCol int) {
	b.b.Emit(vdbe.OpOpenEphemeral, cursor, nCol, 0)
}

// emitClose closes a cursor.
func (b *Build) emitClose(cursor int) {
	b.b.Emit(vdbe.OpClose, cursor, 0, 0)
}

// emitRewind emits OP_Rewind. Returns the label for the loop body.
func (b *Build) emitRewind(cursor int, emptyLabel int) int {
	bodyLabel := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpRewind, cursor, emptyLabel, 0)
	b.b.DefineLabel(bodyLabel)
	return bodyLabel
}

// emitNext emits OP_Next.
func (b *Build) emitNext(cursor int, loopLabel int) {
	b.b.EmitJump(vdbe.OpNext, cursor, loopLabel, 0)
}

// emitMakeRecord emits OP_MakeRecord.
func (b *Build) emitMakeRecord(startReg, nCol, destReg int) {
	b.b.Emit(vdbe.OpMakeRecord, startReg, nCol, destReg)
}

// emitResultRow emits OP_ResultRow.
func (b *Build) emitResultRow(startReg, nCol int) {
	b.b.Emit(vdbe.OpResultRow, startReg, nCol, 0)
}

// emitNewRowid generates a new rowid for a table.
func (b *Build) emitNewRowid(cursor, destReg int) {
	b.b.Emit(vdbe.OpNewRowid, cursor, destReg, 0)
}

// emitInsert emits OP_Insert to insert a record.
func (b *Build) emitInsert(cursor, recordReg, rowidReg int) {
	b.b.Emit(vdbe.OpInsert, cursor, recordReg, rowidReg)
}

// emitDelete emits OP_Delete to delete the current row.
func (b *Build) emitDelete(cursor int) {
	b.b.Emit(vdbe.OpDelete, cursor, 0, 0)
}

// emitUpdate emits OP_Update to update the current row.
func (b *Build) emitUpdate(cursor, recordReg int) {
	b.b.Emit(vdbe.OpUpdate, cursor, recordReg, 0)
}

// emitColumn reads a column from the current row.
func (b *Build) emitColumn(cursor, colIdx, destReg int) {
	b.b.Emit(vdbe.OpColumn, cursor, colIdx, destReg)
}

// emitRowid reads the rowid of the current row.
func (b *Build) emitRowid(cursor, destReg int) {
	b.b.Emit(vdbe.OpRowid, cursor, destReg, 0)
}

// emitSCopy does a shallow copy from srcReg to destReg.
func (b *Build) emitSCopy(srcReg, destReg int) {
	b.b.Emit(vdbe.OpSCopy, srcReg, destReg, 0)
}

// emitDecrJumpZero decrements a register and jumps if zero.
func (b *Build) emitDecrJumpZero(reg, label int) {
	b.b.EmitJump(vdbe.OpDecrJumpZero, reg, label, 0)
}

// emitCopy copies a range of registers.
func (b *Build) emitCopy(srcStart, destStart, n int) {
	for i := 0; i < n; i++ {
		b.b.Emit(vdbe.OpCopy, srcStart+i, destStart+i, 0)
	}
}

// emitSorterOpen opens a sorter cursor.
func (b *Build) emitSorterOpen(cursor, nCol int) {
	b.b.Emit(vdbe.OpSorterOpen, cursor, nCol, 0)
}

// emitSorterInsert inserts a record into the sorter.
func (b *Build) emitSorterInsert(cursor, recordReg int) {
	b.b.Emit(vdbe.OpSorterInsert, cursor, recordReg, 0)
}

// emitSorterSort sorts the sorter. Returns body label.
func (b *Build) emitSorterSort(cursor int, emptyLabel int) int {
	bodyLabel := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpSorterSort, cursor, emptyLabel, 0)
	b.b.DefineLabel(bodyLabel)
	return bodyLabel
}

// emitSorterData reads data from the sorter.
func (b *Build) emitSorterData(cursor, destReg int) {
	b.b.Emit(vdbe.OpSorterData, cursor, destReg, 0)
}

// emitSorterNext advances to the next row in the sorter.
func (b *Build) emitSorterNext(cursor, loopLabel int) {
	b.b.EmitJump(vdbe.OpSorterNext, cursor, loopLabel, 0)
}

// emitIdxInsert inserts a record into an index.
func (b *Build) emitIdxInsert(cursor, recordReg int) {
	b.b.Emit(vdbe.OpIdxInsert, cursor, recordReg, 0)
}

// emitIdxDelete deletes a record from an index.
func (b *Build) emitIdxDelete(cursor int) {
	b.b.Emit(vdbe.OpIdxDelete, cursor, 0, 0)
}

// emitCreateBTree creates a new B-tree (table or index).
func (b *Build) emitCreateBTree(rootPage int, isIndex bool) {
	flags := 0
	if isIndex {
		flags = 1
	}
	b.b.Emit(vdbe.OpCreateBTree, 0, rootPage, flags)
}

// emitDestroy destroys a B-tree.
func (b *Build) emitDestroy(rootPage int) {
	b.b.Emit(vdbe.OpDestroy, rootPage, 0, 0)
}

// emitParseSchema emits OP_ParseSchema to re-read the schema table.
func (b *Build) emitParseSchema() {
	b.b.EmitP4(vdbe.OpParseSchema, 0, 0, 0, nil, "reparse schema")
}

// emitSetCookie emits OP_SetCookie to update the schema cookie.
func (b *Build) emitSetCookie(cookie int) {
	b.b.Emit(vdbe.OpSetCookie, 0, 1, cookie) // P2=1 is schema cookie
}

// emitAutoCommit emits OP_AutoCommit.
func (b *Build) emitAutoCommit(commit bool) {
	v := 1
	if !commit {
		v = 0
	}
	b.b.Emit(vdbe.OpAutoCommit, v, 0, 0)
}

// getOrCreateIntConst returns a register holding an integer constant.
func (b *Build) getOrCreateIntConst(val int64) int {
	key := fmt.Sprintf("int:%d", val)
	if reg, ok := b.constants[key]; ok {
		return reg
	}
	reg := b.b.AllocReg(1)
	b.emitInteger(val, reg)
	b.constants[key] = reg
	return reg
}

// expandStarColumns expands a * or table.* into a list of (cursor, colIdx) pairs.
func (b *Build) expandStarColumns(table string) ([]struct {
	cursor int
	colIdx int
	name   string
}, error) {
	var result []struct {
		cursor int
		colIdx int
		name   string
	}

	if table != "" {
		entry, ok := b.tableMap[strings.ToUpper(table)]
		if !ok {
			return nil, fmt.Errorf("no such table: %s", table)
		}
		for i, col := range entry.table.Columns {
			result = append(result, struct {
				cursor int
				colIdx int
				name   string
			}{entry.cursor, i, col.Name})
		}
	} else {
		for _, entry := range b.tables {
			for i, col := range entry.table.Columns {
				result = append(result, struct {
					cursor int
					colIdx int
					name   string
				}{entry.cursor, i, col.Name})
			}
		}
	}
	return result, nil
}

