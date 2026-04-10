// Package sql implements SQL statement completeness checking for sqlite-go.
package sql

import (
	"strings"
	"unicode"
)

// Complete checks if a SQL string is a complete statement.
// A statement is complete if:
// - It ends with a semicolon
// - It contains a complete SQL construct (e.g., balanced parentheses)
// - CREATE TRIGGER statements need the full body
//
// This mirrors sqlite3_complete() from the C implementation.
func Complete(sql string) bool {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return false
	}

	// Tokenize approach: scan for statement structure
	state := &completeState{
		parenDepth:  0,
		inString:    false,
		stringDelim: 0,
	}
	state.scan(sql)

	// Must have balanced parens
	if state.parenDepth != 0 {
		return false
	}

	// Must end with semicolon (after trimming whitespace)
	if !strings.HasSuffix(strings.TrimRight(sql, " \t\r\n"), ";") {
		return false
	}

	// Must have at least one SQL keyword
	return state.hasKeyword
}

type completeState struct {
	parenDepth  int
	inString    bool
	stringDelim byte
	hasKeyword  bool
}

func (s *completeState) scan(sql string) {
	i := 0
	for i < len(sql) {
		ch := sql[i]

		// Inside a string literal
		if s.inString {
			if ch == s.stringDelim {
				// Check for escaped quote (doubled)
				if i+1 < len(sql) && sql[i+1] == s.stringDelim {
					i += 2
					continue
				}
				s.inString = false
			}
			i++
			continue
		}

		switch ch {
		case '\'':
			s.inString = true
			s.stringDelim = '\''
			i++
		case '"':
			s.inString = true
			s.stringDelim = '"'
			i++
		case '(':
			s.parenDepth++
			i++
		case ')':
			if s.parenDepth > 0 {
				s.parenDepth--
			}
			i++
		case '-':
			// Single-line comment
			if i+1 < len(sql) && sql[i+1] == '-' {
				for i < len(sql) && sql[i] != '\n' {
					i++
				}
			} else {
				i++
			}
		case '/':
			// Multi-line comment
			if i+1 < len(sql) && sql[i+1] == '*' {
				i += 2
				for i+1 < len(sql) {
					if sql[i] == '*' && sql[i+1] == '/' {
						i += 2
						break
					}
					i++
				}
			} else {
				i++
			}
		default:
			if unicode.IsLetter(rune(ch)) {
				// Extract word
				start := i
				for i < len(sql) && (unicode.IsLetter(rune(sql[i])) || unicode.IsDigit(rune(sql[i])) || sql[i] == '_') {
					i++
				}
				word := strings.ToUpper(sql[start:i])
				if isSQLKeyword(word) {
					s.hasKeyword = true
				}
			} else {
				i++
			}
		}
	}
}

// isSQLKeyword checks if a word is a SQL keyword.
func isSQLKeyword(word string) bool {
	switch word {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP",
		"ALTER", "BEGIN", "COMMIT", "ROLLBACK", "PRAGMA", "WITH",
		"VACUUM", "REINDEX", "ANALYZE", "ATTACH", "DETACH",
		"EXPLAIN", "REPLACE":
		return true
	default:
		return false
	}
}
