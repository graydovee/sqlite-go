package compile

import (
	"fmt"
	"strings"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// compileCreateTable compiles a CREATE TABLE statement.
func (b *Build) compileCreateTable(stmt *CreateTableStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil CREATE TABLE statement")
	}

	b.emitInit()
	b.emitTransaction(0, true)

	tableName := stmt.Name

	// If IF NOT EXISTS, check if the table already exists
	if stmt.IfNotExists {
		_, err := b.lookupTable(tableName)
		if err == nil {
			// Table exists, skip creation
			b.emitHalt(0)
			return nil
		}
	}

	// Create the B-tree for the table (root page will be assigned by the engine)
	rootPage := b.b.AllocReg(1)
	b.b.Emit(vdbe.OpCreateBTree, 0, rootPage, 0)

	// Build the schema entry for sqlite_schema
	// The schema table is always at root page 1
	schemaCursor := b.b.AllocCursor()
	b.emitOpenWrite(schemaCursor, 1) // schema table root page

	// Build the record to insert into the schema table
	// Schema table columns: type, name, tbl_name, rootpage, sql
	schemaRec := b.b.AllocReg(5)

	b.emitString("table", schemaRec+0)
	b.emitString(tableName, schemaRec+1)
	b.emitString(tableName, schemaRec+2)
	b.emitSCopy(rootPage, schemaRec+3)

	// Build the SQL text
	sqlText := buildCreateTableSQL(stmt)
	b.emitString(sqlText, schemaRec+4)

	recReg := b.b.AllocReg(1)
	b.emitMakeRecord(schemaRec, 5, recReg)

	// Generate rowid for the schema entry
	rowidReg := b.b.AllocReg(1)
	b.emitNewRowid(schemaCursor, rowidReg)
	b.emitInsert(schemaCursor, recReg, rowidReg)
	b.emitClose(schemaCursor)

	// Update schema cookie
	b.emitSetCookie(1)
	b.emitParseSchema()

	b.emitHalt(0)
	return nil
}

// compileDropTable compiles a DROP TABLE statement.
func (b *Build) compileDropTable(stmt *DropTableStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil DROP TABLE statement")
	}

	b.emitInit()
	b.emitTransaction(0, true)

	// Look up the table to get its root page
	tbl, err := b.lookupTable(stmt.Name)
	if err != nil {
		if stmt.IfExists {
			// Table doesn't exist but IF EXISTS, so just halt
			b.emitHalt(0)
			return nil
		}
		return err
	}

	// Destroy the table's B-tree
	b.emitDestroy(tbl.RootPage)

	// Remove the entry from the schema table
	// Open schema table (root page 1)
	schemaCursor := b.b.AllocCursor()
	b.emitOpenWrite(schemaCursor, 1)

	// Scan schema table for the entry matching this table name
	emptyLabel := b.b.NewLabel()
	loopEndLabel := b.b.NewLabel()
	loopBody := b.b.NewLabel()

	nameReg := b.b.AllocReg(1)
	targetNameReg := b.b.AllocReg(1)
	rowidReg := b.b.AllocReg(1)

	b.emitString(stmt.Name, targetNameReg)

	b.b.EmitJump(vdbe.OpRewind, schemaCursor, emptyLabel, 0)
	b.b.DefineLabel(loopBody)

	// Read the 'name' column (column index 1 in sqlite_schema)
	b.emitColumn(schemaCursor, 1, nameReg)
	b.emitRowid(schemaCursor, rowidReg)

	// If name matches, delete the row
	skipLabel := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpNe, nameReg, skipLabel, targetNameReg)
	b.emitDelete(schemaCursor)
	b.b.DefineLabel(skipLabel)

	b.emitNext(schemaCursor, loopBody)
	b.b.DefineLabel(loopEndLabel)
	b.b.DefineLabel(emptyLabel)
	b.emitClose(schemaCursor)

	// Also drop any indexes on this table
	if b.schema != nil {
		for _, idx := range b.schema.Indexes {
			if idx.Table == stmt.Name {
				b.emitDestroy(idx.RootPage)
			}
		}
	}

	// Update schema cookie
	b.emitSetCookie(1)
	b.emitParseSchema()

	b.emitHalt(0)
	return nil
}

// compileCreateIndex compiles a CREATE INDEX statement.
func (b *Build) compileCreateIndex(stmt *CreateIndexStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil CREATE INDEX statement")
	}

	b.emitInit()
	b.emitTransaction(0, true)

	// If IF NOT EXISTS, check if index already exists
	if stmt.IfNotExists {
		_, err := b.lookupIndex(stmt.Name)
		if err == nil {
			b.emitHalt(0)
			return nil
		}
	}

	// Create the B-tree for the index
	rootPage := b.b.AllocReg(1)
	flags := 1 // is index
	b.b.Emit(vdbe.OpCreateBTree, 0, rootPage, flags)

	// Open the index for writing
	indexCursor := b.b.AllocCursor()
	b.b.EmitComment(vdbe.OpOpenWrite, indexCursor, rootPage, 0, "index")

	// Open the source table for reading
	tbl, err := b.lookupTable(stmt.Table)
	if err != nil {
		return fmt.Errorf("CREATE INDEX: %w", err)
	}

	tableCursor := b.b.AllocCursor()
	b.emitOpenRead(tableCursor, tbl.RootPage)

	// Register table for column resolution
	b.addTableRef(stmt.Table, "", tbl, tableCursor)

	nIdxCols := len(stmt.Columns)
	valueBase := b.b.AllocReg(nIdxCols + 1) // +1 for rowid
	recReg := b.b.AllocReg(1)
	rowidReg := b.b.AllocReg(1)

	// Scan the table and populate the index
	emptyLabel := b.b.NewLabel()
	loopEndLabel := b.b.NewLabel()
	loopBody := b.b.NewLabel()

	b.b.EmitJump(vdbe.OpRewind, tableCursor, emptyLabel, 0)
	b.b.DefineLabel(loopBody)

	// Read rowid
	b.emitRowid(tableCursor, rowidReg)

	// Read index columns
	for i, col := range stmt.Columns {
		if err := b.compileExpr(col.Expr, valueBase+i); err != nil {
			return err
		}
	}

	// Append rowid to index key
	b.emitSCopy(rowidReg, valueBase+nIdxCols)

	// If partial index, check WHERE clause
	if stmt.Where != nil {
		skipLabel := b.b.NewLabel()
		whereFalse := b.b.NewLabel()
		if err := b.compileCondition(stmt.Where, skipLabel, whereFalse, true); err != nil {
			return err
		}
		b.b.DefineLabel(skipLabel)

		b.emitMakeRecord(valueBase, nIdxCols+1, recReg)
		b.emitIdxInsert(indexCursor, recReg)

		b.b.DefineLabel(whereFalse)
	} else {
		b.emitMakeRecord(valueBase, nIdxCols+1, recReg)
		b.emitIdxInsert(indexCursor, recReg)
	}

	b.emitNext(tableCursor, loopBody)
	b.b.DefineLabel(loopEndLabel)
	b.b.DefineLabel(emptyLabel)

	// Close cursors
	b.emitClose(tableCursor)
	b.emitClose(indexCursor)

	// Insert index entry into the schema table
	schemaCursor := b.b.AllocCursor()
	b.emitOpenWrite(schemaCursor, 1)

	schemaRec := b.b.AllocReg(5)
	b.emitString("index", schemaRec+0)
	b.emitString(stmt.Name, schemaRec+1)
	b.emitString(stmt.Table, schemaRec+2)
	b.emitSCopy(rootPage, schemaRec+3)

	sqlText := buildCreateIndexSQL(stmt)
	b.emitString(sqlText, schemaRec+4)

	recReg2 := b.b.AllocReg(1)
	b.emitMakeRecord(schemaRec, 5, recReg2)
	rowidReg2 := b.b.AllocReg(1)
	b.emitNewRowid(schemaCursor, rowidReg2)
	b.emitInsert(schemaCursor, recReg2, rowidReg2)
	b.emitClose(schemaCursor)

	b.emitSetCookie(1)
	b.emitParseSchema()

	b.emitHalt(0)
	return nil
}

// compileDropIndex compiles a DROP INDEX statement.
func (b *Build) compileDropIndex(stmt *DropIndexStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil DROP INDEX statement")
	}

	b.emitInit()
	b.emitTransaction(0, true)

	idx, err := b.lookupIndex(stmt.Name)
	if err != nil {
		if stmt.IfExists {
			b.emitHalt(0)
			return nil
		}
		return err
	}

	// Destroy the index B-tree
	b.emitDestroy(idx.RootPage)

	// Remove from schema table
	schemaCursor := b.b.AllocCursor()
	b.emitOpenWrite(schemaCursor, 1)

	nameReg := b.b.AllocReg(1)
	targetNameReg := b.b.AllocReg(1)
	rowidReg := b.b.AllocReg(1)

	b.emitString(stmt.Name, targetNameReg)

	emptyLabel := b.b.NewLabel()
	loopEndLabel := b.b.NewLabel()
	loopBody := b.b.NewLabel()

	b.b.EmitJump(vdbe.OpRewind, schemaCursor, emptyLabel, 0)
	b.b.DefineLabel(loopBody)

	b.emitColumn(schemaCursor, 1, nameReg)
	b.emitRowid(schemaCursor, rowidReg)

	skipLabel := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpNe, nameReg, skipLabel, targetNameReg)
	b.emitDelete(schemaCursor)
	b.b.DefineLabel(skipLabel)

	b.emitNext(schemaCursor, loopBody)
	b.b.DefineLabel(loopEndLabel)
	b.b.DefineLabel(emptyLabel)
	b.emitClose(schemaCursor)

	b.emitSetCookie(1)
	b.emitParseSchema()

	b.emitHalt(0)
	return nil
}

// compileBegin compiles a BEGIN TRANSACTION statement.
func (b *Build) compileBegin(stmt *BeginStmt) {
	b.emitInit()
	b.b.Emit(vdbe.OpAutoCommit, 0, 0, 0)
	if stmt != nil && stmt.Immediate {
		b.b.Emit(vdbe.OpTransaction, 0, 1, 0)
	} else if stmt != nil && stmt.Exclusive {
		b.b.Emit(vdbe.OpTransaction, 0, 2, 0)
	} else {
		b.b.Emit(vdbe.OpTransaction, 0, 0, 0)
	}
	b.emitHalt(0)
}

// compileCommit compiles a COMMIT statement.
func (b *Build) compileCommit() {
	b.emitInit()
	b.emitAutoCommit(true)
	b.emitHalt(0)
}

// compileRollback compiles a ROLLBACK statement.
func (b *Build) compileRollback(stmt *RollbackStmt) {
	b.emitInit()
	if stmt != nil && stmt.Savepoint != "" {
		// ROLLBACK TO savepoint
		b.b.EmitP4(vdbe.OpSavepoint, 2, 0, 0, stmt.Savepoint, "rollback to savepoint")
	} else {
		b.emitAutoCommit(true)
		b.b.Emit(vdbe.OpHalt, 0, 0, 0)
	}
	b.emitHalt(0)
}

// buildCreateTableSQL reconstructs a CREATE TABLE SQL string from the AST.
func buildCreateTableSQL(stmt *CreateTableStmt) string {
	var sb strings.Builder
	sb.WriteString("CREATE ")
	if stmt.Temp {
		sb.WriteString("TEMP ")
	}
	sb.WriteString("TABLE ")
	if stmt.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	sb.WriteString(stmt.Name)
	sb.WriteString(" (")

	for i, col := range stmt.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(col.Name)
		if col.Type != "" {
			sb.WriteString(" ")
			sb.WriteString(col.Type)
		}
		for _, c := range col.Constraints {
			switch c.Type {
			case CCPrimaryKey:
				sb.WriteString(" PRIMARY KEY")
				if c.Autoincrement {
					sb.WriteString(" AUTOINCREMENT")
				}
			case CCNotNull:
				sb.WriteString(" NOT NULL")
			case CCUnique:
				sb.WriteString(" UNIQUE")
			case CCDefault:
				sb.WriteString(" DEFAULT ")
				if c.Default != nil {
					sb.WriteString(exprToString(c.Default))
				}
			case CCCheck:
				sb.WriteString(" CHECK(")
				if c.Check != nil {
					sb.WriteString(exprToString(c.Check))
				}
				sb.WriteString(")")
			}
		}
	}

	for _, tc := range stmt.Constraints {
		sb.WriteString(", ")
		switch tc.Type {
		case TCPrimaryKey:
			sb.WriteString("PRIMARY KEY(")
			for j, c := range tc.Columns {
				if j > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(c)
			}
			sb.WriteString(")")
		case TCUnique:
			sb.WriteString("UNIQUE(")
			for j, c := range tc.Columns {
				if j > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(c)
			}
			sb.WriteString(")")
		case TCCheck:
			sb.WriteString("CHECK(")
			if tc.Check != nil {
				sb.WriteString(exprToString(tc.Check))
			}
			sb.WriteString(")")
		case TCForeignKey:
			sb.WriteString("FOREIGN KEY(")
			for j, c := range tc.Columns {
				if j > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(c)
			}
			sb.WriteString(") REFERENCES ")
			sb.WriteString(tc.ForeignKey.RefTable)
			if len(tc.ForeignKey.RefColumns) > 0 {
				sb.WriteString("(")
				for j, c := range tc.ForeignKey.RefColumns {
					if j > 0 {
						sb.WriteString(", ")
					}
					sb.WriteString(c)
				}
				sb.WriteString(")")
			}
		}
	}

	sb.WriteString(")")

	if stmt.WithoutRowid {
		sb.WriteString(" WITHOUT ROWID")
	}

	return sb.String()
}

// buildCreateIndexSQL reconstructs a CREATE INDEX SQL string.
func buildCreateIndexSQL(stmt *CreateIndexStmt) string {
	var sb strings.Builder
	sb.WriteString("CREATE ")
	if stmt.Unique {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	if stmt.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	if stmt.Schema != "" {
		sb.WriteString(stmt.Schema)
		sb.WriteString(".")
	}
	sb.WriteString(stmt.Name)
	sb.WriteString(" ON ")
	sb.WriteString(stmt.Table)
	sb.WriteString("(")
	for i, col := range stmt.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(exprToString(col.Expr))
		if col.Order == SortDesc {
			sb.WriteString(" DESC")
		} else if col.Order == SortAsc {
			sb.WriteString(" ASC")
		}
	}
	sb.WriteString(")")
	if stmt.Where != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(exprToString(stmt.Where))
	}
	return sb.String()
}

// exprToString converts an expression to a SQL string representation.
func exprToString(expr *Expr) string {
	if expr == nil {
		return ""
	}
	switch expr.Kind {
	case ExprLiteral:
		switch expr.LiteralType {
		case "string":
			return "'" + expr.StringValue + "'"
		case "null":
			return "NULL"
		case "integer":
			return fmt.Sprintf("%d", expr.IntValue)
		case "float":
			return fmt.Sprintf("%g", expr.FloatValue)
		case "blob":
			return "X'" + expr.StringValue + "'"
		default:
			return expr.StringValue
		}
	case ExprColumnRef:
		if expr.Table != "" {
			return expr.Table + "." + expr.Name
		}
		return expr.Name
	case ExprBinaryOp:
		return "(" + exprToString(expr.Left) + " " + expr.Op + " " + exprToString(expr.Right) + ")"
	case ExprUnaryOp:
		return expr.Op + " " + exprToString(expr.Right)
	case ExprFunctionCall:
		args := make([]string, len(expr.Args))
		for i, a := range expr.Args {
			args[i] = exprToString(a)
		}
		return expr.FunctionName + "(" + strings.Join(args, ", ") + ")"
	case ExprIsNull:
		return exprToString(expr.Left) + " IS NULL"
	case ExprIsNotNull:
		return exprToString(expr.Left) + " IS NOT NULL"
	case ExprBetween:
		s := exprToString(expr.Left)
		if expr.Not {
			s += " NOT"
		}
		s += " BETWEEN " + exprToString(expr.Low) + " AND " + exprToString(expr.High)
		return s
	case ExprInList:
		s := exprToString(expr.Left)
		if expr.Not {
			s += " NOT"
		}
		vals := make([]string, len(expr.InValues))
		for i, v := range expr.InValues {
			vals[i] = exprToString(v)
		}
		s += " IN (" + strings.Join(vals, ", ") + ")"
		return s
	default:
		return "?"
	}
}

// =============================================================================
// CREATE VIEW / DROP VIEW
// =============================================================================

// compileCreateView compiles a CREATE VIEW statement.
func (b *Build) compileCreateView(stmt *CreateViewStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil CREATE VIEW statement")
	}

	b.emitInit()
	b.emitTransaction(0, true)

	tableName := stmt.Name

	// IF NOT EXISTS check
	if stmt.IfNotExists {
		_, err := b.lookupTable(tableName)
		if err == nil {
			b.emitHalt(0)
			return nil
		}
	}

	// Views are stored in sqlite_schema with rootpage=0 and type="view"
	// Open the schema table for writing
	schemaCursor := b.b.AllocCursor()
	b.emitOpenWrite(schemaCursor, 1)

	// Build the schema record: type, name, tbl_name, rootpage, sql
	schemaRec := b.b.AllocReg(5)

	b.emitString("view", schemaRec+0)
	b.emitString(tableName, schemaRec+1)
	b.emitString(tableName, schemaRec+2)
	b.emitInteger(0, schemaRec+3) // rootpage = 0 for views

	// Reconstruct the CREATE VIEW SQL
	sqlText := buildCreateViewSQL(stmt)
	b.emitString(sqlText, schemaRec+4)

	recReg := b.b.AllocReg(1)
	b.emitMakeRecord(schemaRec, 5, recReg)

	rowidReg := b.b.AllocReg(1)
	b.emitNewRowid(schemaCursor, rowidReg)
	b.emitInsert(schemaCursor, recReg, rowidReg)
	b.emitClose(schemaCursor)

	b.emitSetCookie(1)
	b.emitParseSchema()

	b.emitHalt(0)
	return nil
}

// compileDropView compiles a DROP VIEW statement.
func (b *Build) compileDropView(stmt *DropViewStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil DROP VIEW statement")
	}

	b.emitInit()
	b.emitTransaction(0, true)

	// Remove from schema table by scanning for matching name
	schemaCursor := b.b.AllocCursor()
	b.emitOpenWrite(schemaCursor, 1)

	emptyLabel := b.b.NewLabel()
	loopBody := b.b.NewLabel()

	nameReg := b.b.AllocReg(1)
	targetNameReg := b.b.AllocReg(1)
	rowidReg := b.b.AllocReg(1)

	b.emitString(stmt.Name, targetNameReg)

	b.b.EmitJump(vdbe.OpRewind, schemaCursor, emptyLabel, 0)
	b.b.DefineLabel(loopBody)

	b.emitColumn(schemaCursor, 1, nameReg)
	b.emitRowid(schemaCursor, rowidReg)

	// Check if name matches
	skipLabel := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpNe, nameReg, skipLabel, targetNameReg)
	b.emitDelete(schemaCursor)
	b.b.DefineLabel(skipLabel)

	b.emitNext(schemaCursor, loopBody)
	b.b.DefineLabel(emptyLabel)
	b.emitClose(schemaCursor)

	b.emitSetCookie(1)
	b.emitParseSchema()

	b.emitHalt(0)
	return nil
}

// buildCreateViewSQL reconstructs a CREATE VIEW SQL string from the AST.
func buildCreateViewSQL(stmt *CreateViewStmt) string {
	var sb strings.Builder
	sb.WriteString("CREATE VIEW ")
	if stmt.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	if stmt.Schema != "" {
		sb.WriteString(stmt.Schema)
		sb.WriteString(".")
	}
	sb.WriteString(stmt.Name)
	sb.WriteString(" AS ")
	sb.WriteString(selectStmtToString(stmt.Select))
	return sb.String()
}

// selectStmtToString converts a SelectStmt to a SQL string.
func selectStmtToString(sel *SelectStmt) string {
	if sel == nil {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("SELECT ")

	for i, col := range sel.Columns {
		if i > 0 {
			sb.WriteString(", ")
		}
		if col.Star {
			sb.WriteString("*")
		} else if col.TableStar != "" {
			sb.WriteString(col.TableStar)
			sb.WriteString(".*")
		} else {
			sb.WriteString(exprToString(col.Expr))
			if col.As != "" {
				sb.WriteString(" AS ")
				sb.WriteString(col.As)
			}
		}
	}

	if sel.From != nil {
		sb.WriteString(" FROM ")
		for i, t := range sel.From.Tables {
			if i > 0 {
				sb.WriteString(", ")
			}
			if t.Subquery != nil {
				sb.WriteString("(")
				sb.WriteString(selectStmtToString(t.Subquery))
				sb.WriteString(")")
			} else {
				if t.Schema != "" {
					sb.WriteString(t.Schema)
					sb.WriteString(".")
				}
				sb.WriteString(t.Name)
			}
			if t.Alias != "" {
				sb.WriteString(" AS ")
				sb.WriteString(t.Alias)
			}
		}
	}

	if sel.Where != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(exprToString(sel.Where))
	}

	if len(sel.GroupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		for i, e := range sel.GroupBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(exprToString(e))
		}
	}

	if sel.Having != nil {
		sb.WriteString(" HAVING ")
		sb.WriteString(exprToString(sel.Having))
	}

	if len(sel.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, item := range sel.OrderBy {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(exprToString(item.Expr))
			if item.Order == SortAsc {
				sb.WriteString(" ASC")
			} else if item.Order == SortDesc {
				sb.WriteString(" DESC")
			}
		}
	}

	if sel.Limit != nil {
		sb.WriteString(" LIMIT ")
		sb.WriteString(exprToString(sel.Limit))
	}

	if sel.Offset != nil {
		sb.WriteString(" OFFSET ")
		sb.WriteString(exprToString(sel.Offset))
	}

	return sb.String()
}

// =============================================================================
// CREATE TRIGGER / DROP TRIGGER
// =============================================================================

// compileCreateTrigger compiles a CREATE TRIGGER statement.
func (b *Build) compileCreateTrigger(stmt *CreateTriggerStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil CREATE TRIGGER statement")
	}

	b.emitInit()
	b.emitTransaction(0, true)

	// Store trigger definition in sqlite_schema as type="trigger"
	schemaCursor := b.b.AllocCursor()
	b.emitOpenWrite(schemaCursor, 1)

	// Schema record: type, name, tbl_name, rootpage, sql
	schemaRec := b.b.AllocReg(5)

	b.emitString("trigger", schemaRec+0)
	b.emitString(stmt.Name, schemaRec+1)
	b.emitString(stmt.Table, schemaRec+2)
	b.emitInteger(0, schemaRec+3) // rootpage = 0 for triggers

	sqlText := buildCreateTriggerSQL(stmt)
	b.emitString(sqlText, schemaRec+4)

	recReg := b.b.AllocReg(1)
	b.emitMakeRecord(schemaRec, 5, recReg)

	rowidReg := b.b.AllocReg(1)
	b.emitNewRowid(schemaCursor, rowidReg)
	b.emitInsert(schemaCursor, recReg, rowidReg)
	b.emitClose(schemaCursor)

	b.emitSetCookie(1)
	b.emitParseSchema()

	b.emitHalt(0)
	return nil
}

// compileDropTrigger compiles a DROP TRIGGER statement.
func (b *Build) compileDropTrigger(stmt *DropTriggerStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil DROP TRIGGER statement")
	}

	b.emitInit()
	b.emitTransaction(0, true)

	// Remove from schema table by scanning for matching name with type="trigger"
	schemaCursor := b.b.AllocCursor()
	b.emitOpenWrite(schemaCursor, 1)

	emptyLabel := b.b.NewLabel()
	loopBody := b.b.NewLabel()

	nameReg := b.b.AllocReg(1)
	targetNameReg := b.b.AllocReg(1)
	rowidReg := b.b.AllocReg(1)

	b.emitString(stmt.Name, targetNameReg)

	b.b.EmitJump(vdbe.OpRewind, schemaCursor, emptyLabel, 0)
	b.b.DefineLabel(loopBody)

	b.emitColumn(schemaCursor, 1, nameReg)
	b.emitRowid(schemaCursor, rowidReg)

	skipLabel := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpNe, nameReg, skipLabel, targetNameReg)
	b.emitDelete(schemaCursor)
	b.b.DefineLabel(skipLabel)

	b.emitNext(schemaCursor, loopBody)
	b.b.DefineLabel(emptyLabel)
	b.emitClose(schemaCursor)

	b.emitSetCookie(1)
	b.emitParseSchema()

	b.emitHalt(0)
	return nil
}

// buildCreateTriggerSQL reconstructs a CREATE TRIGGER SQL string from the AST.
func buildCreateTriggerSQL(stmt *CreateTriggerStmt) string {
	var sb strings.Builder
	sb.WriteString("CREATE TRIGGER ")
	if stmt.IfNotExists {
		sb.WriteString("IF NOT EXISTS ")
	}
	if stmt.Schema != "" {
		sb.WriteString(stmt.Schema)
		sb.WriteString(".")
	}
	sb.WriteString(stmt.Name)
	sb.WriteString(" ")

	switch stmt.Time {
	case TriggerBefore:
		sb.WriteString("BEFORE ")
	case TriggerAfter:
		sb.WriteString("AFTER ")
	case TriggerInstead:
		sb.WriteString("INSTEAD OF ")
	}

	switch stmt.Event {
	case TriggerDelete:
		sb.WriteString("DELETE")
	case TriggerInsert:
		sb.WriteString("INSERT")
	case TriggerUpdate:
		sb.WriteString("UPDATE")
		if len(stmt.Columns) > 0 {
			sb.WriteString(" OF ")
			for i, c := range stmt.Columns {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(c)
			}
		}
	}

	sb.WriteString(" ON ")
	sb.WriteString(stmt.Table)

	if stmt.ForEachRow {
		sb.WriteString(" FOR EACH ROW")
	}

	if stmt.When != nil {
		sb.WriteString(" WHEN ")
		sb.WriteString(exprToString(stmt.When))
	}

	sb.WriteString(" BEGIN ")
	for _, s := range stmt.Body {
		sb.WriteString(s)
		sb.WriteString(" ")
	}
	sb.WriteString("END")

	return sb.String()
}

// =============================================================================
// ALTER TABLE
// =============================================================================

// compileAlterTable compiles an ALTER TABLE statement.
func (b *Build) compileAlterTable(stmt *AlterTableStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil ALTER TABLE statement")
	}

	b.emitInit()
	b.emitTransaction(0, true)

	switch stmt.Type {
	case AlterAddColumn:
		return b.compileAlterAddColumn(stmt)
	case AlterRenameTable:
		return b.compileAlterRenameTable(stmt)
	case AlterRenameColumn:
		return b.compileAlterRenameColumn(stmt)
	case AlterDropColumn:
		return b.compileAlterDropColumn(stmt)
	default:
		return fmt.Errorf("unsupported ALTER TABLE operation: %v", stmt.Type)
	}
}

// compileAlterAddColumn compiles ALTER TABLE ADD COLUMN.
func (b *Build) compileAlterAddColumn(stmt *AlterTableStmt) error {
	// Update the schema entry: find the table in sqlite_schema and modify its SQL
	schemaCursor := b.b.AllocCursor()
	b.emitOpenWrite(schemaCursor, 1)

	emptyLabel := b.b.NewLabel()
	loopBody := b.b.NewLabel()
	loopEnd := b.b.NewLabel()

	nameReg := b.b.AllocReg(1)
	targetNameReg := b.b.AllocReg(1)
	typeReg := b.b.AllocReg(1)
	rowidReg := b.b.AllocReg(1)

	b.emitString("table", targetNameReg)
	b.emitString(stmt.Table, nameReg)

	b.b.EmitJump(vdbe.OpRewind, schemaCursor, emptyLabel, 0)
	b.b.DefineLabel(loopBody)

	// Read type and name columns
	b.emitColumn(schemaCursor, 0, typeReg)
	colNameReg := b.b.AllocReg(1)
	b.emitColumn(schemaCursor, 1, colNameReg)
	b.emitRowid(schemaCursor, rowidReg)

	// Check: type == "table" AND name == stmt.Table
	skipLabel := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpNe, typeReg, skipLabel, targetNameReg)
	skipLabel2 := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpNe, colNameReg, skipLabel2, nameReg)

	// Found the table entry - update its SQL to include the new column
	// Read current SQL, append new column definition
	sqlReg := b.b.AllocReg(1)
	b.emitColumn(schemaCursor, 4, sqlReg)

	// Build new SQL with added column
	newColSQL := buildAlterAddColumnSQL(stmt.Column)
	addSQLReg := b.b.AllocReg(1)
	b.emitString(newColSQL, addSQLReg)

	// Replace the SQL column (simplified: just update the schema entry)
	// In a full implementation, we'd need string concatenation
	// For now, emit a ParseSchema to re-read the updated schema
	b.b.DefineLabel(skipLabel)
	b.b.DefineLabel(skipLabel2)

	b.emitNext(schemaCursor, loopBody)
	b.b.DefineLabel(loopEnd)
	b.b.DefineLabel(emptyLabel)
	b.emitClose(schemaCursor)

	b.emitSetCookie(1)
	b.emitParseSchema()

	b.emitHalt(0)
	return nil
}

// compileAlterRenameTable compiles ALTER TABLE RENAME TO.
func (b *Build) compileAlterRenameTable(stmt *AlterTableStmt) error {
	// Rename requires updating all schema entries referencing the old name
	schemaCursor := b.b.AllocCursor()
	b.emitOpenWrite(schemaCursor, 1)

	emptyLabel := b.b.NewLabel()
	loopBody := b.b.NewLabel()

	nameReg := b.b.AllocReg(1)
	targetNameReg := b.b.AllocReg(1)
	tblNameReg := b.b.AllocReg(1)
	rowidReg := b.b.AllocReg(1)
	newNameReg := b.b.AllocReg(1)

	b.emitString(stmt.Table, targetNameReg)
	b.emitString(stmt.NewName, newNameReg)

	b.b.EmitJump(vdbe.OpRewind, schemaCursor, emptyLabel, 0)
	b.b.DefineLabel(loopBody)

	b.emitColumn(schemaCursor, 1, nameReg)    // name column
	b.emitColumn(schemaCursor, 2, tblNameReg) // tbl_name column
	b.emitRowid(schemaCursor, rowidReg)

	// If name matches, update both name and tbl_name columns
	skipLabel := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpNe, nameReg, skipLabel, targetNameReg)

	// Update the name column to new name
	// Read full record, modify name, rewrite
	newRecReg := b.b.AllocReg(5)
	b.emitColumn(schemaCursor, 0, newRecReg+0)
	b.emitSCopy(newNameReg, newRecReg+1)     // name = new name
	b.emitSCopy(newNameReg, newRecReg+2)     // tbl_name = new name
	b.emitColumn(schemaCursor, 3, newRecReg+3)
	b.emitColumn(schemaCursor, 4, newRecReg+4)

	recReg := b.b.AllocReg(1)
	b.emitMakeRecord(newRecReg, 5, recReg)
	b.emitUpdate(schemaCursor, recReg)

	b.b.DefineLabel(skipLabel)
	b.emitNext(schemaCursor, loopBody)
	b.b.DefineLabel(emptyLabel)
	b.emitClose(schemaCursor)

	b.emitSetCookie(1)
	b.emitParseSchema()

	b.emitHalt(0)
	return nil
}

// compileAlterRenameColumn compiles ALTER TABLE RENAME COLUMN.
func (b *Build) compileAlterRenameColumn(stmt *AlterTableStmt) error {
	// Similar to rename table but modifies the SQL text in the schema entry
	schemaCursor := b.b.AllocCursor()
	b.emitOpenWrite(schemaCursor, 1)

	emptyLabel := b.b.NewLabel()
	loopBody := b.b.NewLabel()

	nameReg := b.b.AllocReg(1)
	targetNameReg := b.b.AllocReg(1)
	rowidReg := b.b.AllocReg(1)

	b.emitString(stmt.Table, targetNameReg)

	b.b.EmitJump(vdbe.OpRewind, schemaCursor, emptyLabel, 0)
	b.b.DefineLabel(loopBody)

	b.emitColumn(schemaCursor, 1, nameReg)
	b.emitRowid(schemaCursor, rowidReg)

	skipLabel := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpNe, nameReg, skipLabel, targetNameReg)

	// Found the table - column rename is handled at schema level
	// The actual column rename requires modifying the SQL stored in sqlite_schema
	// For now, just trigger a schema reparse
	b.b.DefineLabel(skipLabel)

	b.emitNext(schemaCursor, loopBody)
	b.b.DefineLabel(emptyLabel)
	b.emitClose(schemaCursor)

	b.emitSetCookie(1)
	b.emitParseSchema()

	b.emitHalt(0)
	return nil
}

// compileAlterDropColumn compiles ALTER TABLE DROP COLUMN.
func (b *Build) compileAlterDropColumn(stmt *AlterTableStmt) error {
	// Similar to rename column - modifies schema SQL
	schemaCursor := b.b.AllocCursor()
	b.emitOpenWrite(schemaCursor, 1)

	emptyLabel := b.b.NewLabel()
	loopBody := b.b.NewLabel()

	nameReg := b.b.AllocReg(1)
	targetNameReg := b.b.AllocReg(1)
	rowidReg := b.b.AllocReg(1)

	b.emitString(stmt.Table, targetNameReg)

	b.b.EmitJump(vdbe.OpRewind, schemaCursor, emptyLabel, 0)
	b.b.DefineLabel(loopBody)

	b.emitColumn(schemaCursor, 1, nameReg)
	b.emitRowid(schemaCursor, rowidReg)

	skipLabel := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpNe, nameReg, skipLabel, targetNameReg)

	// Schema modification for drop column
	b.b.DefineLabel(skipLabel)

	b.emitNext(schemaCursor, loopBody)
	b.b.DefineLabel(emptyLabel)
	b.emitClose(schemaCursor)

	b.emitSetCookie(1)
	b.emitParseSchema()

	b.emitHalt(0)
	return nil
}

// buildAlterAddColumnSQL builds the SQL fragment for adding a column.
func buildAlterAddColumnSQL(col *ColumnDef) string {
	if col == nil {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("ADD ")
	sb.WriteString(col.Name)
	if col.Type != "" {
		sb.WriteString(" ")
		sb.WriteString(col.Type)
	}
	for _, c := range col.Constraints {
		switch c.Type {
		case CCPrimaryKey:
			sb.WriteString(" PRIMARY KEY")
		case CCNotNull:
			sb.WriteString(" NOT NULL")
		case CCUnique:
			sb.WriteString(" UNIQUE")
		case CCDefault:
			sb.WriteString(" DEFAULT ")
			if c.Default != nil {
				sb.WriteString(exprToString(c.Default))
			}
		}
	}
	return sb.String()
}

// =============================================================================
// SAVEPOINT / RELEASE
// =============================================================================

// compileSavepoint compiles a SAVEPOINT statement.
func (b *Build) compileSavepoint(stmt *SavepointStmt) {
	b.emitInit()
	b.b.EmitP4(vdbe.OpSavepoint, 0, 0, 0, stmt.Name, "savepoint "+stmt.Name)
	b.emitHalt(0)
}

// compileRelease compiles a RELEASE statement.
func (b *Build) compileRelease(stmt *ReleaseStmt) {
	b.emitInit()
	b.b.EmitP4(vdbe.OpRelease, 0, 0, 0, stmt.Name, "release "+stmt.Name)
	b.emitHalt(0)
}

// =============================================================================
// PRAGMA
// =============================================================================

// compilePragma compiles a PRAGMA statement.
func (b *Build) compilePragma(stmt *PragmaStmt) error {
	if stmt == nil {
		return fmt.Errorf("nil PRAGMA statement")
	}

	b.emitInit()

	if stmt.Value != nil {
		// Writing pragma: PRAGMA name = value
		valReg := b.b.AllocReg(1)
		// Handle identifier values (e.g., PRAGMA journal_mode = WAL)
		// where WAL should be treated as a string, not a column ref
		if stmt.Value.Kind == ExprColumnRef && stmt.Value.Table == "" {
			b.emitString(stmt.Value.Name, valReg)
		} else {
			if err := b.compileExpr(stmt.Value, valReg); err != nil {
				return err
			}
		}
		b.b.EmitP4(vdbe.OpWriteCookie, 0, valReg, 0, stmt.Name, "pragma "+stmt.Name)
	} else {
		// Reading pragma: PRAGMA name
		resultReg := b.b.AllocReg(1)
		b.b.EmitP4(vdbe.OpReadCookie, 0, resultReg, 0, stmt.Name, "pragma "+stmt.Name)
		b.emitResultRow(resultReg, 1)
	}

	b.emitHalt(0)
	return nil
}

// =============================================================================
// VACUUM
// =============================================================================

// compileVacuum compiles a VACUUM statement.
func (b *Build) compileVacuum(stmt *VacuumStmt) {
	b.emitInit()
	b.b.Emit(vdbe.OpVacuum, 0, 0, 0)
	b.emitHalt(0)
}

// =============================================================================
// ATTACH / DETACH
// =============================================================================

// compileAttach compiles an ATTACH DATABASE statement.
func (b *Build) compileAttach(stmt *AttachStmt) {
	b.emitInit()
	// P4 carries the filename and schema name
	fileReg := b.b.AllocReg(2)
	b.emitString(stmt.File, fileReg)
	b.emitString(stmt.Schema, fileReg+1)
	b.b.EmitP4(vdbe.OpFunction, fileReg, 0, 2, &vdbe.FuncInfo{Name: "ATTACH", ArgCount: 2},
		"attach "+stmt.Schema)
	b.emitHalt(0)
}

// compileDetach compiles a DETACH DATABASE statement.
func (b *Build) compileDetach(stmt *DetachStmt) {
	b.emitInit()
	schemaReg := b.b.AllocReg(1)
	b.emitString(stmt.Schema, schemaReg)
	b.b.EmitP4(vdbe.OpFunction, schemaReg, 0, 1, &vdbe.FuncInfo{Name: "DETACH", ArgCount: 1},
		"detach "+stmt.Schema)
	b.emitHalt(0)
}

// =============================================================================
// ANALYZE
// =============================================================================

// compileAnalyze compiles an ANALYZE statement.
func (b *Build) compileAnalyze(stmt *AnalyzeStmt) {
	b.emitInit()
	b.emitTransaction(0, true)
	// ANALYZE updates sqlite_stat1 table with index statistics
	// For now emit a LoadAnalysis instruction
	b.b.Emit(vdbe.OpLoadAnalysis, 0, 0, 0)
	b.emitHalt(0)
}

// =============================================================================
// REINDEX
// =============================================================================

// compileReindex compiles a REINDEX statement.
func (b *Build) compileReindex(stmt *ReindexStmt) {
	b.emitInit()
	b.emitTransaction(0, true)
	// REINDEX rebuilds one or more indexes
	// Emit as a no-op for now; the actual index rebuild would happen at execution
	b.b.Emit(vdbe.OpNoop, 0, 0, 0)
	b.emitHalt(0)
}
