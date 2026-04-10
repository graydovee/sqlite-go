package sql

import (
	"fmt"

	"github.com/sqlite-go/sqlite-go/compile"
)

// AnalyzeStmt represents an ANALYZE statement.
type AnalyzeStmt struct {
	// Target is the object to analyze: "", "table", "schema.table", or "index"
	Target string
}

// execAnalyze handles ANALYZE statements.
// ANALYZE collects statistics about tables and indices and stores them in
// the sqlite_stat1, sqlite_stat2, etc. tables. For now, this is a stub
// that validates the syntax.
func (e *Engine) execAnalyze(tokens []compile.Token) error {
	pos := 0
	expectKeyword(tokens, &pos, "analyze")

	// Parse optional target
	target := ""
	if pos < len(tokens) && tokens[pos].Type != compile.TokenSemi {
		// Could be: table-name, schema.table-name, or index-name
		target = tokens[pos].Value
		pos++
		if pos < len(tokens) && tokens[pos].Type == compile.TokenDot {
			target += "."
			pos++
			if pos < len(tokens) {
				target += tokens[pos].Value
				pos++
			}
		}
	}

	stmt := &AnalyzeStmt{Target: target}
	_ = stmt

	// TODO: Implement actual ANALYZE logic:
	// 1. Create sqlite_stat1 (and sqlite_stat2/3/4) tables if they don't exist
	// 2. Scan tables/indices to gather statistics
	// 3. Store row count estimates and column value distribution
	// 4. Use statistics for query planning (in the optimizer)
	//
	// For now, ANALYZE is a no-op that accepts the syntax correctly.

	return nil
}

// ParseAnalyze parses an ANALYZE statement from tokens.
// This is exported for use by the compile/ package if needed.
func ParseAnalyze(tokens []compile.Token) (*AnalyzeStmt, error) {
	pos := 0
	if len(tokens) == 0 || !isKeyword(tokens[pos], "analyze") {
		return nil, fmt.Errorf("expected ANALYZE")
	}
	pos++

	target := ""
	if pos < len(tokens) && tokens[pos].Type != compile.TokenSemi {
		target = tokens[pos].Value
		pos++
		if pos < len(tokens) && tokens[pos].Type == compile.TokenDot {
			target += "."
			pos++
			if pos < len(tokens) {
				target += tokens[pos].Value
				pos++
			}
		}
	}

	return &AnalyzeStmt{Target: target}, nil
}
