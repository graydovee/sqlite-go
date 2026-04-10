package sql

import (
	"fmt"
	"strings"

	"github.com/sqlite-go/sqlite-go/compile"
)

// AlterType indicates what kind of ALTER TABLE operation.
type AlterType int

const (
	AlterRename   AlterType = iota // ALTER TABLE RENAME TO
	AlterAddColumn                 // ALTER TABLE ADD COLUMN
)

// AlterStmt represents an ALTER TABLE statement.
type AlterStmt struct {
	Type      AlterType
	TableName string // Original table name
	NewName   string // New table name (for RENAME TO)
	NewColumn ColumnInfo // New column definition (for ADD COLUMN)
	Schema    string    // Schema name (e.g., "main")
}

// execAlterTable handles ALTER TABLE statements.
func (e *Engine) execAlterTable(tokens []compile.Token) error {
	pos := 0
	expectKeyword(tokens, &pos, "alter")
	expectKeyword(tokens, &pos, "table")

	// Optional schema prefix
	schema := ""
	if pos < len(tokens) {
		name1 := tokens[pos].Value
		pos++
		if pos < len(tokens) && tokens[pos].Type == compile.TokenDot {
			pos++ // skip .
			schema = name1
			if pos >= len(tokens) {
				return fmt.Errorf("expected table name after schema")
			}
			name1 = tokens[pos].Value
			pos++
		}

		// name1 is the table name
		if pos >= len(tokens) {
			return fmt.Errorf("expected RENAME TO or ADD COLUMN after table name")
		}

		tbl, ok := e.tables[name1]
		if !ok {
			return fmt.Errorf("no such table: %s", name1)
		}

		// Determine the alter type
		if isKeyword(tokens[pos], "rename") {
			return e.execAlterRename(tbl, tokens, pos, schema)
		} else if isKeyword(tokens[pos], "add") {
			return e.execAlterAddColumn(tbl, tokens, pos, schema)
		}

		return fmt.Errorf("expected RENAME TO or ADD COLUMN, got: %s", tokens[pos].Value)
	}

	return fmt.Errorf("incomplete ALTER TABLE statement")
}

// execAlterRename handles ALTER TABLE ... RENAME TO new_name.
func (e *Engine) execAlterRename(tbl *TableInfo, tokens []compile.Token, pos int, schema string) error {
	expectKeyword(tokens, &pos, "rename")

	// Optional COLUMN keyword (RENAME COLUMN ... TO ...)
	if pos < len(tokens) && isKeyword(tokens[pos], "column") {
		// ALTER TABLE x RENAME COLUMN old TO new (column rename, not supported yet)
		return fmt.Errorf("RENAME COLUMN is not yet supported")
	}

	// Optional TO keyword
	if pos < len(tokens) && isKeyword(tokens[pos], "to") {
		pos++
	}

	if pos >= len(tokens) {
		return fmt.Errorf("expected new table name after RENAME TO")
	}

	newName := tokens[pos].Value
	pos++

	// Check if new name already exists
	if _, exists := e.tables[newName]; exists {
		return fmt.Errorf("there is already another table or index with this name: %s", newName)
	}

	// Rename the table
	oldName := tbl.Name
	tbl.Name = newName
	e.tables[newName] = tbl
	delete(e.tables, oldName)

	stmt := &AlterStmt{
		Type:      AlterRename,
		TableName: oldName,
		NewName:   newName,
		Schema:    schema,
	}
	_ = stmt

	return nil
}

// execAlterAddColumn handles ALTER TABLE ... ADD COLUMN col_def.
func (e *Engine) execAlterAddColumn(tbl *TableInfo, tokens []compile.Token, pos int, schema string) error {
	expectKeyword(tokens, &pos, "add")

	// Optional COLUMN keyword
	if pos < len(tokens) && isKeyword(tokens[pos], "column") {
		pos++
	}

	if pos >= len(tokens) {
		return fmt.Errorf("expected column definition after ADD COLUMN")
	}

	// Parse column name
	colName := tokens[pos].Value
	pos++

	// Parse column type
	colType := ""
	if pos < len(tokens) && (tokens[pos].Type == compile.TokenID ||
		(tokens[pos].Type == compile.TokenKeyword && isTypeNameKeyword(tokens[pos].Value))) {
		colType = tokens[pos].Value
		pos++
	}

	// Parse column constraints
	notNull := false
	var defaultVal string
	for pos < len(tokens) && tokens[pos].Type != compile.TokenSemi {
		if isKeyword(tokens[pos], "not") && pos+1 < len(tokens) && isKeyword(tokens[pos+1], "null") {
			notNull = true
			pos += 2
			continue
		}
		if isKeyword(tokens[pos], "default") {
			pos++
			if pos < len(tokens) {
				defaultVal = tokens[pos].Value
				// Strip quotes from string defaults
				if len(defaultVal) >= 2 && defaultVal[0] == '\'' && defaultVal[len(defaultVal)-1] == '\'' {
					defaultVal = defaultVal[1 : len(defaultVal)-1]
				}
				pos++
			}
			continue
		}
		// Skip other constraints
		pos++
	}

	// SQLite restriction: the new column may not have a PRIMARY KEY or UNIQUE constraint
	// For now we just add it

	newCol := ColumnInfo{
		CID:          len(tbl.Columns),
		Name:         colName,
		Type:         colType,
		NotNull:      notNull,
		DefaultValue: defaultVal,
	}

	tbl.Columns = append(tbl.Columns, newCol)

	stmt := &AlterStmt{
		Type:      AlterAddColumn,
		TableName: tbl.Name,
		NewColumn: newCol,
		Schema:    schema,
	}
	_ = stmt

	return nil
}

// ParseAlter parses an ALTER TABLE statement from tokens.
func ParseAlter(tokens []compile.Token) (*AlterStmt, error) {
	pos := 0
	if len(tokens) == 0 || !isKeyword(tokens[pos], "alter") {
		return nil, fmt.Errorf("expected ALTER")
	}
	pos++
	if pos >= len(tokens) || !isKeyword(tokens[pos], "table") {
		return nil, fmt.Errorf("expected TABLE after ALTER")
	}
	pos++

	schema := ""
	if pos >= len(tokens) {
		return nil, fmt.Errorf("expected table name")
	}
	name1 := tokens[pos].Value
	pos++

	if pos < len(tokens) && tokens[pos].Type == compile.TokenDot {
		pos++
		schema = name1
		if pos >= len(tokens) {
			return nil, fmt.Errorf("expected table name after schema")
		}
		name1 = tokens[pos].Value
		pos++
	}

	if pos >= len(tokens) {
		return nil, fmt.Errorf("expected RENAME TO or ADD COLUMN")
	}

	if isKeyword(tokens[pos], "rename") {
		pos++
		if pos < len(tokens) && isKeyword(tokens[pos], "to") {
			pos++
		}
		if pos >= len(tokens) {
			return nil, fmt.Errorf("expected new table name")
		}
		newName := tokens[pos].Value
		return &AlterStmt{
			Type:      AlterRename,
			TableName: name1,
			NewName:   newName,
			Schema:    schema,
		}, nil
	}

	if isKeyword(tokens[pos], "add") {
		pos++
		if pos < len(tokens) && isKeyword(tokens[pos], "column") {
			pos++
		}
		if pos >= len(tokens) {
			return nil, fmt.Errorf("expected column name")
		}
		colName := tokens[pos].Value
		pos++

		colType := ""
		if pos < len(tokens) && (tokens[pos].Type == compile.TokenID ||
			(tokens[pos].Type == compile.TokenKeyword && isTypeNameKeyword(strings.ToLower(tokens[pos].Value)))) {
			colType = tokens[pos].Value
		}

		return &AlterStmt{
			Type:      AlterAddColumn,
			TableName: name1,
			NewColumn: ColumnInfo{Name: colName, Type: colType},
			Schema:    schema,
		}, nil
	}

	return nil, fmt.Errorf("expected RENAME TO or ADD COLUMN")
}
