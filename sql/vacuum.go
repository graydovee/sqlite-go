package sql

import (
	"fmt"

	"github.com/sqlite-go/sqlite-go/compile"
)

// VacuumStmt represents a VACUUM statement.
type VacuumStmt struct {
	// Schema is the database to vacuum (empty means "main")
	Schema string
	// Into is the output database for VACUUM INTO (empty if not specified)
	Into string
}

// execVacuum handles VACUUM statements.
// VACUUM rebuilds the entire database file, repacking it into a minimal
// amount of disk space. For now, this is a stub.
func (e *Engine) execVacuum(tokens []compile.Token) error {
	pos := 0
	expectKeyword(tokens, &pos, "vacuum")

	schema := ""
	into := ""

	// Optional schema name
	if pos < len(tokens) && tokens[pos].Type != compile.TokenSemi {
		if !isKeyword(tokens[pos], "into") {
			schema = tokens[pos].Value
			pos++
		}
	}

	// Optional INTO clause: VACUUM [schema] INTO 'filename'
	if pos < len(tokens) && isKeyword(tokens[pos], "into") {
		pos++
		if pos >= len(tokens) {
			return fmt.Errorf("VACUUM INTO requires a filename")
		}
		into = tokens[pos].Value
		pos++
		// Strip quotes
		if len(into) >= 2 && into[0] == '\'' && into[len(into)-1] == '\'' {
			into = into[1 : len(into)-1]
		}
	}

	stmt := &VacuumStmt{Schema: schema, Into: into}
	_ = stmt

	// TODO: Implement actual VACUUM logic:
	// 1. Create a new temporary database
	// 2. Copy all schema (tables, indices, views, triggers)
	// 3. Copy all data from old to new
	// 4. Rebuild indices
	// 5. Replace old database with new one
	//
	// For now, VACUUM is accepted but is a no-op.

	return nil
}

// ParseVacuum parses a VACUUM statement from tokens.
func ParseVacuum(tokens []compile.Token) (*VacuumStmt, error) {
	pos := 0
	if len(tokens) == 0 || !isKeyword(tokens[pos], "vacuum") {
		return nil, fmt.Errorf("expected VACUUM")
	}
	pos++

	schema := ""
	into := ""

	if pos < len(tokens) && tokens[pos].Type != compile.TokenSemi {
		if !isKeyword(tokens[pos], "into") {
			schema = tokens[pos].Value
			pos++
		}
	}

	if pos < len(tokens) && isKeyword(tokens[pos], "into") {
		pos++
		if pos >= len(tokens) {
			return nil, fmt.Errorf("VACUUM INTO requires a filename")
		}
		into = tokens[pos].Value
		if len(into) >= 2 && into[0] == '\'' && into[len(into)-1] == '\'' {
			into = into[1 : len(into)-1]
		}
	}

	return &VacuumStmt{Schema: schema, Into: into}, nil
}
