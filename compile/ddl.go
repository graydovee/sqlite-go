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
