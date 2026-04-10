package sql

import (
	"fmt"

	"github.com/sqlite-go/sqlite-go/compile"
)

// AttachStmt represents an ATTACH DATABASE statement.
type AttachStmt struct {
	Filename string // Database file path or :memory:
	Schema   string // Schema name (AS clause)
}

// DetachStmt represents a DETACH DATABASE statement.
type DetachStmt struct {
	Schema string // Schema name to detach
}

// execAttach handles ATTACH DATABASE statements.
// ATTACH DATABASE 'filename' AS schema_name
// TODO: Full implementation requires multi-database support.
func (e *Engine) execAttach(tokens []compile.Token) error {
	pos := 0
	expectKeyword(tokens, &pos, "attach")

	// Optional DATABASE keyword
	if pos < len(tokens) && isKeyword(tokens[pos], "database") {
		pos++
	}

	// Filename (string literal)
	if pos >= len(tokens) {
		return fmt.Errorf("ATTACH requires a filename")
	}
	filename := tokens[pos].Value
	pos++

	// Strip quotes
	if len(filename) >= 2 && filename[0] == '\'' && filename[len(filename)-1] == '\'' {
		filename = filename[1 : len(filename)-1]
	}

	// AS keyword
	schemaName := ""
	if pos < len(tokens) && isKeyword(tokens[pos], "as") {
		pos++
		if pos < len(tokens) {
			schemaName = tokens[pos].Value
			pos++
		}
	}

	if schemaName == "" {
		return fmt.Errorf("ATTACH requires AS schema_name")
	}

	// Cannot attach "main" or "temp"
	if schemaName == "main" || schemaName == "temp" {
		return fmt.Errorf("cannot attach database as reserved name: %s", schemaName)
	}

	stmt := &AttachStmt{Filename: filename, Schema: schemaName}
	_ = stmt

	// TODO: Implement multi-database support:
	// 1. Open a new pager/btree for the attached database
	// 2. Add to the engine's database map
	// 3. Support cross-database queries (schema.table)
	// 4. Handle DETACH by closing and removing from the map
	//
	// For now, ATTACH is accepted but does not actually attach a database.

	return nil
}

// execDetach handles DETACH DATABASE statements.
func (e *Engine) execDetach(tokens []compile.Token) error {
	pos := 0
	expectKeyword(tokens, &pos, "detach")

	// Optional DATABASE keyword
	if pos < len(tokens) && isKeyword(tokens[pos], "database") {
		pos++
	}

	// Schema name
	if pos >= len(tokens) {
		return fmt.Errorf("DETACH requires a schema name")
	}
	schemaName := tokens[pos].Value
	pos++

	if schemaName == "main" || schemaName == "temp" {
		return fmt.Errorf("cannot detach reserved database: %s", schemaName)
	}

	stmt := &DetachStmt{Schema: schemaName}
	_ = stmt

	// TODO: Implement actual detach logic (reverse of attach).

	return nil
}

// ParseAttach parses an ATTACH statement from tokens.
func ParseAttach(tokens []compile.Token) (*AttachStmt, error) {
	pos := 0
	if len(tokens) == 0 || !isKeyword(tokens[pos], "attach") {
		return nil, fmt.Errorf("expected ATTACH")
	}
	pos++

	if pos < len(tokens) && isKeyword(tokens[pos], "database") {
		pos++
	}

	if pos >= len(tokens) {
		return nil, fmt.Errorf("ATTACH requires a filename")
	}
	filename := tokens[pos].Value
	pos++

	if len(filename) >= 2 && filename[0] == '\'' && filename[len(filename)-1] == '\'' {
		filename = filename[1 : len(filename)-1]
	}

	schemaName := ""
	if pos < len(tokens) && isKeyword(tokens[pos], "as") {
		pos++
		if pos < len(tokens) {
			schemaName = tokens[pos].Value
			pos++
		}
	}

	return &AttachStmt{Filename: filename, Schema: schemaName}, nil
}

// ParseDetach parses a DETACH statement from tokens.
func ParseDetach(tokens []compile.Token) (*DetachStmt, error) {
	pos := 0
	if len(tokens) == 0 || !isKeyword(tokens[pos], "detach") {
		return nil, fmt.Errorf("expected DETACH")
	}
	pos++

	if pos < len(tokens) && isKeyword(tokens[pos], "database") {
		pos++
	}

	if pos >= len(tokens) {
		return nil, fmt.Errorf("DETACH requires a schema name")
	}
	schemaName := tokens[pos].Value

	return &DetachStmt{Schema: schemaName}, nil
}
