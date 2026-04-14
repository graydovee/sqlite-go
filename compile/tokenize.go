package compile

import (
	"strings"
	"unicode"
)

// keywordMap maps lowercase keyword strings to their Keyword constant.
var keywordMap map[string]Keyword

func init() {
	keywordMap = map[string]Keyword{
		"abort": KwAbort, "action": KwAction, "add": KwAdd, "after": KwAfter,
		"all": KwAll, "alter": KwAlter, "always": KwAlways, "analyze": KwAnalyze,
		"and": KwAnd, "as": KwAs, "asc": KwAsc, "attach": KwAttach,
		"autoincrement": KwAutoincrement, "before": KwBefore, "begin": KwBegin,
		"between": KwBetween, "by": KwBy, "cascade": KwCascade, "case": KwCase,
		"cast": KwCast, "check": KwCheck, "collate": KwCollate, "column": KwColumn,
		"commit": KwCommit, "conflict": KwConflict, "constraint": KwConstraint,
		"create": KwCreate, "cross": KwCross, "current": KwCurrent,
		"current_date": KwCurrentDate, "current_time": KwCurrentTime,
		"current_timestamp": KwCurrentTimestamp, "database": KwDatabase,
		"default": KwDefault, "deferrable": KwDeferrable, "deferred": KwDeferred,
		"delete": KwDelete, "desc": KwDesc, "detach": KwDetach, "distinct": KwDistinct,
		"do": KwDo, "drop": KwDrop, "each": KwEach, "else": KwElse, "end": KwEnd,
		"escape": KwEscape, "except": KwExcept, "exclude": KwExclude,
		"exclusive": KwExclusive, "exists": KwExists, "explain": KwExplain,
		"fail": KwFail, "false": KwFalse, "filter": KwFilter, "first": KwFirst,
		"following": KwFollowing, "for": KwFor, "foreign": KwForeign,
		"from": KwFrom, "full": KwFull, "generated": KwGenerated, "glob": KwGlob,
		"group": KwGroup, "groups": KwGroups, "having": KwHaving, "if": KwIf,
		"ignore": KwIgnore, "immediate": KwImmediate, "in": KwIn, "index": KwIndex,
		"indexed": KwIndexed, "initially": KwInitially, "inner": KwInner,
		"insert": KwInsert, "instead": KwInstead, "intersect": KwIntersect,
		"into": KwInto, "is": KwIs, "isnull": KwIsnull, "join": KwJoin,
		"key": KwKey, "last": KwLast, "left": KwLeft, "like": KwLike,
		"limit": KwLimit, "match": KwMatch, "materialized": KwMaterialized,
		"natural": KwNatural, "no": KwNo, "not": KwNot, "nothing": KwNothing,
		"notnull": KwNotnull, "null": KwNull, "nulls": KwNulls, "of": KwOf,
		"offset": KwOffset, "on": KwOn, "or": KwOr, "order": KwOrder,
		"others": KwOthers, "outer": KwOuter, "over": KwOver,
		"partition": KwPartition, "plan": KwPlan, "pragma": KwPragma,
		"preceding": KwPreceding, "primary": KwPrimary, "query": KwQuery,
		"raise": KwRaise, "range": KwRange, "recursive": KwRecursive,
		"references": KwReferences, "regexp": KwRegexp, "reindex": KwReindex,
		"release": KwRelease, "rename": KwRename, "replace": KwReplace,
		"restrict": KwRestrict, "returning": KwReturning, "right": KwRight,
		"rollback": KwRollback, "row": KwRow, "rows": KwRows,
		"savepoint": KwSavepoint, "select": KwSelect, "set": KwSet,
		"table": KwTable, "temp": KwTemp, "temporary": KwTemporary,
		"then": KwThen, "ties": KwTies, "to": KwTo, "transaction": KwTransaction,
		"trigger": KwTrigger, "true": KwTrue, "unbounded": KwUnbounded, "union": KwUnion,
		"unique": KwUnique, "update": KwUpdate, "using": KwUsing,
		"vacuum": KwVacuum, "values": KwValues, "view": KwView,
		"virtual": KwVirtual, "when": KwWhen, "where": KwWhere,
		"window": KwWindow, "with": KwWith, "without": KwWithout,
	}
}

// isIdChar returns true if c can appear in an identifier (after the first char).
func isIdChar(c byte) bool {
	return unicode.IsLetter(rune(c)) || c == '_' || c == '$' || (c >= 0x80) || (c >= '0' && c <= '9')
}

// isIdStart returns true if c can start an identifier.
func isIdStart(c byte) bool {
	return unicode.IsLetter(rune(c)) || c == '_' || (c >= 0x80)
}

// isHexDigit returns true if c is a hexadecimal digit.
func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// Tokenize splits a SQL string into a sequence of tokens.
func Tokenize(sql string) []Token {
	var tokens []Token
	pos := 0
	line := 1
	col := 1

	for pos < len(sql) {
		tok := scanToken(sql, pos, line, col)
		if tok.Type == TokenEOF {
			break
		}
		tokens = append(tokens, tok)
		// Advance line/col
		for i := pos; i < pos+len(tok.Value); i++ {
			if sql[i] == '\n' {
				line++
				col = 1
			} else {
				col++
			}
		}
		pos += len(tok.Value)
	}
	return tokens
}

// scanToken scans one token starting at sql[pos].
func scanToken(sql string, pos, line, col int) Token {
	if pos >= len(sql) {
		return Token{Type: TokenEOF, Line: line, Col: col}
	}

	c := sql[pos]
	start := pos

	switch {
	// Whitespace
	case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f':
		i := 1
		for start+i < len(sql) && isSpace(sql[start+i]) {
			i++
		}
		return tok(TokenWhitespace, sql[start:start+i], line, col)

	// Single-line comment --
	case c == '-' && pos+1 < len(sql) && sql[pos+1] == '-':
		i := 2
		for start+i < len(sql) && sql[start+i] != '\n' {
			i++
		}
		return tok(TokenComment, sql[start:start+i], line, col)

	// C-style comment /* */
	case c == '/' && pos+1 < len(sql) && sql[pos+1] == '*':
		i := 2
		for start+i+1 < len(sql) {
			if sql[start+i] == '*' && sql[start+i+1] == '/' {
				i += 2
				return tok(TokenComment, sql[start:start+i], line, col)
			}
			i++
		}
		// Unterminated comment
		return tok(TokenComment, sql[start:], line, col)

	case c == '/':
		return tok(TokenSlash, "/", line, col)

	// String literal '...'
	case c == '\'':
		return scanString(sql, pos, line, col)

	// Double-quoted identifier "..."
	case c == '"':
		return scanQuotedID(sql, pos, line, col)

	// Backtick-quoted identifier `...`
	case c == '`':
		return scanBacktickID(sql, pos, line, col)

	// Bracket-quoted identifier [...]
	case c == '[':
		return scanBracketID(sql, pos, line, col)

	// Operators and punctuation
	case c == '(':
		return tok(TokenLParen, "(", line, col)
	case c == ')':
		return tok(TokenRParen, ")", line, col)
	case c == '{':
		return tok(TokenLBrace, "{", line, col)
	case c == '}':
		return tok(TokenRBrace, "}", line, col)
	case c == ';':
		return tok(TokenSemi, ";", line, col)
	case c == ',':
		return tok(TokenComma, ",", line, col)
	case c == '*':
		return tok(TokenStar, "*", line, col)
	case c == '+':
		return tok(TokenPlus, "+", line, col)
	case c == '-':
		// Note: -- comment is handled above
		// Check for -> and ->> JSON operators
		if pos+1 < len(sql) && sql[pos+1] == '>' {
			if pos+2 < len(sql) && sql[pos+2] == '>' {
				return tok(TokenArrow2, "->>", line, col)
			}
			return tok(TokenArrow, "->", line, col)
		}
		return tok(TokenMinus, "-", line, col)
	case c == '%':
		return tok(TokenRem, "%", line, col)
	case c == '&':
		return tok(TokenBitAnd, "&", line, col)
	case c == '~':
		return tok(TokenBitNot, "~", line, col)

	case c == '|':
		if pos+1 < len(sql) && sql[pos+1] == '|' {
			return tok(TokenConcat, "||", line, col)
		}
		return tok(TokenBitOr, "|", line, col)

	case c == '=':
		if pos+1 < len(sql) && sql[pos+1] == '=' {
			return tok(TokenEq, "==", line, col)
		}
		return tok(TokenEq, "=", line, col)

	case c == '<':
		if pos+1 < len(sql) {
			switch sql[pos+1] {
			case '=':
				return tok(TokenLe, "<=", line, col)
			case '>':
				return tok(TokenNe, "<>", line, col)
			case '<':
				return tok(TokenLShift, "<<", line, col)
			}
		}
		return tok(TokenLt, "<", line, col)

	case c == '>':
		if pos+1 < len(sql) {
			switch sql[pos+1] {
			case '=':
				return tok(TokenGe, ">=", line, col)
			case '>':
				return tok(TokenRShift, ">>", line, col)
			}
		}
		return tok(TokenGt, ">", line, col)

	case c == '!':
		if pos+1 < len(sql) && sql[pos+1] == '=' {
			return tok(TokenNe, "!=", line, col)
		}
		return tok(TokenIllegal, "!", line, col)

	// Dot: either standalone or start of a float like .5
	case c == '.':
		if pos+1 < len(sql) && sql[pos+1] >= '0' && sql[pos+1] <= '9' {
			return scanNumber(sql, pos, line, col)
		}
		return tok(TokenDot, ".", line, col)

	// Numbers
	case c >= '0' && c <= '9':
		return scanNumber(sql, pos, line, col)

	// Variables: ? ?NNN :name @name $name
	case c == '?':
		i := 1
		for start+i < len(sql) && sql[start+i] >= '0' && sql[start+i] <= '9' {
			i++
		}
		return tok(TokenVariable, sql[start:start+i], line, col)

	case c == ':' || c == '@' || c == '$':
		i := 1
		n := 0
		for start+i < len(sql) && isIdChar(sql[start+i]) {
			i++
			n++
		}
		if n == 0 {
			return tok(TokenIllegal, string(c), line, col)
		}
		return tok(TokenVariable, sql[start:start+i], line, col)

	// Blob literal X'...'
	case (c == 'x' || c == 'X') && pos+1 < len(sql) && sql[pos+1] == '\'':
		return scanBlob(sql, pos, line, col)

	// Identifiers and keywords
	case isIdStart(c):
		return scanIdent(sql, pos, line, col)

	default:
		return tok(TokenIllegal, string(c), line, col)
	}
}

// scanString scans a single-quoted string literal.
func scanString(sql string, pos, line, col int) Token {
	i := pos + 1
	for i < len(sql) {
		if sql[i] == '\'' {
			if i+1 < len(sql) && sql[i+1] == '\'' {
				// Escaped quote ''
				i += 2
				continue
			}
			i++ // consume closing quote
			return tok(TokenString, sql[pos:i], line, col)
		}
		i++
	}
	// Unterminated string
	return tok(TokenIllegal, sql[pos:i], line, col)
}

// scanQuotedID scans a double-quoted identifier.
func scanQuotedID(sql string, pos, line, col int) Token {
	i := pos + 1
	for i < len(sql) {
		if sql[i] == '"' {
			if i+1 < len(sql) && sql[i+1] == '"' {
				i += 2
				continue
			}
			i++
			return tok(TokenID, sql[pos:i], line, col)
		}
		i++
	}
	return tok(TokenIllegal, sql[pos:i], line, col)
}

// scanBacktickID scans a backtick-quoted identifier.
func scanBacktickID(sql string, pos, line, col int) Token {
	i := pos + 1
	for i < len(sql) {
		if sql[i] == '`' {
			if i+1 < len(sql) && sql[i+1] == '`' {
				i += 2
				continue
			}
			i++
			return tok(TokenID, sql[pos:i], line, col)
		}
		i++
	}
	return tok(TokenIllegal, sql[pos:i], line, col)
}

// scanBracketID scans a [bracket]-quoted identifier.
func scanBracketID(sql string, pos, line, col int) Token {
	i := pos + 1
	for i < len(sql) {
		if sql[i] == ']' {
			i++
			return tok(TokenID, sql[pos:i], line, col)
		}
		i++
	}
	return tok(TokenIllegal, sql[pos:i], line, col)
}

// scanNumber scans an integer or float literal.
func scanNumber(sql string, pos, line, col int) Token {
	i := pos
	typ := TokenInteger

	// Hex integer 0x...
	if sql[i] == '0' && i+1 < len(sql) && (sql[i+1] == 'x' || sql[i+1] == 'X') {
		i += 2
		for i < len(sql) && isHexDigit(sql[i]) {
			i++
		}
		// Trailing id chars make it illegal
		for i < len(sql) && isIdChar(sql[i]) {
			typ = TokenIllegal
			i++
		}
		return tok(typ, sql[pos:i], line, col)
	}

	// Integer part (may start with . for .5 style)
	for i < len(sql) && sql[i] >= '0' && sql[i] <= '9' {
		i++
	}

	// Fractional part
	if i < len(sql) && sql[i] == '.' {
		typ = TokenFloat
		i++
		for i < len(sql) && sql[i] >= '0' && sql[i] <= '9' {
			i++
		}
	}

	// Exponent part
	if i < len(sql) && (sql[i] == 'e' || sql[i] == 'E') {
		if i+1 < len(sql) && (sql[i+1] >= '0' && sql[i+1] <= '9') {
			typ = TokenFloat
			i += 2
			for i < len(sql) && sql[i] >= '0' && sql[i] <= '9' {
				i++
			}
		} else if i+2 < len(sql) && (sql[i+1] == '+' || sql[i+1] == '-') && (sql[i+2] >= '0' && sql[i+2] <= '9') {
			typ = TokenFloat
			i += 3
			for i < len(sql) && sql[i] >= '0' && sql[i] <= '9' {
				i++
			}
		}
	}

	// Trailing id chars make it illegal
	for i < len(sql) && isIdChar(sql[i]) {
		typ = TokenIllegal
		i++
	}

	return tok(typ, sql[pos:i], line, col)
}

// scanBlob scans a blob literal X'...'.
func scanBlob(sql string, pos, line, col int) Token {
	i := pos + 2 // skip X'
	for i < len(sql) && isHexDigit(sql[i]) {
		i++
	}
	if i < len(sql) && sql[i] == '\'' && (i-pos-2)%2 == 0 {
		i++ // consume closing quote
		return tok(TokenBlob, sql[pos:i], line, col)
	}
	// Invalid blob literal
	for i < len(sql) && sql[i] != '\'' {
		i++
	}
	if i < len(sql) {
		i++
	}
	return tok(TokenIllegal, sql[pos:i], line, col)
}

// scanIdent scans an identifier or keyword.
func scanIdent(sql string, pos, line, col int) Token {
	i := pos + 1
	for i < len(sql) && isIdChar(sql[i]) {
		i++
	}
	val := sql[pos:i]
	lower := strings.ToLower(val)
	if _, ok := keywordMap[lower]; ok {
		return Token{Type: TokenKeyword, Value: val, Line: line, Col: col}
	}
	return tok(TokenID, val, line, col)
}

// tok is a helper to create a Token.
func tok(typ TokenType, val string, line, col int) Token {
	return Token{Type: typ, Value: val, Line: line, Col: col}
}

// isSpace reports whether c is a SQL whitespace character.
func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f'
}
