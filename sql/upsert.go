package sql

import (
	"fmt"
	"strings"

	"github.com/sqlite-go/sqlite-go/compile"
)

// UpsertClause represents the ON CONFLICT ... DO UPDATE part of an INSERT.
type UpsertClause struct {
	// ConflictTarget specifies which unique constraint triggers the upsert.
	// If empty, any conflict triggers it.
	ConflictColumns []string // ON CONFLICT (col1, col2) ...
	ConflictIndex   string   // ON CONFLICT ON CONSTRAINT index_name

	// Where on the conflict target
	ConflictWhere string

	// DoUpdate is true for DO UPDATE, false for DO NOTHING.
	DoUpdate bool

	// SetClauses are the SET assignments for DO UPDATE.
	SetClauses []UpsertSet

	// Where condition on the UPDATE
	UpdateWhere string
}

// UpsertSet represents a single SET assignment in an UPSERT.
type UpsertSet struct {
	Column string
	// Excluded table reference for the value expression
	ValueExpr string
}

// ParseUpsert parses ON CONFLICT ... DO UPDATE/NOTHING from a token stream.
// This is called during INSERT parsing when ON CONFLICT is encountered.
func ParseUpsert(tokens []compile.Token, startPos int) (*UpsertClause, int, error) {
	pos := startPos

	// ON
	if pos >= len(tokens) || !isKeyword(tokens[pos], "on") {
		return nil, pos, fmt.Errorf("expected ON")
	}
	pos++

	// CONFLICT
	if pos >= len(tokens) || !isKeyword(tokens[pos], "conflict") {
		return nil, pos, fmt.Errorf("expected CONFLICT after ON")
	}
	pos++

	upsert := &UpsertClause{}

	// Optional conflict target: (col1, col2) or ON CONSTRAINT index_name
	if pos < len(tokens) {
		if tokens[pos].Type == compile.TokenLParen {
			// ON CONFLICT (col1, col2, ...)
			pos++ // skip (
			for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
				upsert.ConflictColumns = append(upsert.ConflictColumns, tokens[pos].Value)
				pos++
				if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
					pos++
				}
			}
			if pos < len(tokens) && tokens[pos].Type == compile.TokenRParen {
				pos++ // skip )
			}
		} else if isKeyword(tokens[pos], "on") {
			// ON CONFLICT ON CONSTRAINT index_name
			pos++ // ON
			if pos < len(tokens) && isKeyword(tokens[pos], "constraint") {
				pos++ // CONSTRAINT
				if pos < len(tokens) {
					upsert.ConflictIndex = tokens[pos].Value
					pos++
				}
			}
		}

		// Optional WHERE after conflict target
		if pos < len(tokens) && isKeyword(tokens[pos], "where") {
			pos++
			var whereParts []string
			for pos < len(tokens) && !isKeyword(tokens[pos], "do") {
				whereParts = append(whereParts, tokens[pos].Value)
				pos++
			}
			upsert.ConflictWhere = strings.Join(whereParts, " ")
		}
	}

	// DO
	if pos >= len(tokens) || !isKeyword(tokens[pos], "do") {
		return nil, pos, fmt.Errorf("expected DO in ON CONFLICT clause")
	}
	pos++

	// NOTHING or UPDATE
	if pos >= len(tokens) {
		return nil, pos, fmt.Errorf("expected NOTHING or UPDATE after DO")
	}

	if isKeyword(tokens[pos], "nothing") {
		pos++
		upsert.DoUpdate = false
		return upsert, pos, nil
	}

	if isKeyword(tokens[pos], "update") {
		pos++
		upsert.DoUpdate = true

		// Parse SET clause
		expectKeyword(tokens, &pos, "set")

		for pos < len(tokens) {
			// Column name
			if pos >= len(tokens) {
				break
			}
			colName := tokens[pos].Value
			pos++

			// =
			if pos < len(tokens) && tokens[pos].Type == compile.TokenEq {
				pos++
			}

			// Value expression (collect until comma or WHERE or end)
			var exprParts []string
			for pos < len(tokens) &&
				tokens[pos].Type != compile.TokenComma &&
				!isKeyword(tokens[pos], "where") {
				exprParts = append(exprParts, tokens[pos].Value)
				pos++
			}

			upsert.SetClauses = append(upsert.SetClauses, UpsertSet{
				Column:   colName,
				ValueExpr: strings.Join(exprParts, " "),
			})

			if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
				pos++
			} else {
				break
			}
		}

		// Optional WHERE
		if pos < len(tokens) && isKeyword(tokens[pos], "where") {
			pos++
			var whereParts []string
			for pos < len(tokens) {
				whereParts = append(whereParts, tokens[pos].Value)
				pos++
			}
			upsert.UpdateWhere = strings.Join(whereParts, " ")
		}

		return upsert, pos, nil
	}

	return nil, pos, fmt.Errorf("expected NOTHING or UPDATE after DO, got: %s", tokens[pos].Value)
}

// UpsertSQL returns a SQL string representation of an upsert clause.
func (u *UpsertClause) UpsertSQL() string {
	var sb strings.Builder
	sb.WriteString("ON CONFLICT")

	if len(u.ConflictColumns) > 0 {
		sb.WriteString(" (")
		sb.WriteString(strings.Join(u.ConflictColumns, ", "))
		sb.WriteString(")")
	} else if u.ConflictIndex != "" {
		sb.WriteString(" ON CONSTRAINT ")
		sb.WriteString(u.ConflictIndex)
	}

	if u.ConflictWhere != "" {
		sb.WriteString(" WHERE ")
		sb.WriteString(u.ConflictWhere)
	}

	if !u.DoUpdate {
		sb.WriteString(" DO NOTHING")
	} else {
		sb.WriteString(" DO UPDATE SET ")
		for i, s := range u.SetClauses {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(s.Column)
			sb.WriteString(" = ")
			sb.WriteString(s.ValueExpr)
		}
		if u.UpdateWhere != "" {
			sb.WriteString(" WHERE ")
			sb.WriteString(u.UpdateWhere)
		}
	}

	return sb.String()
}
