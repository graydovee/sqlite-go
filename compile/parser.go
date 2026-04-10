package compile

import (
	"fmt"
	"strings"
)

// =============================================================================
// Parser
// =============================================================================

// Parser is a hand-written recursive descent parser for SQL.
type Parser struct {
	tokens []Token
	pos    int
	errs   []error
}

// Parse parses a SQL string into a list of statements.
func Parse(sql string) ([]*Statement, error) {
	p := &Parser{
		tokens: filterTokens(Tokenize(sql)),
	}
	return p.parseAll()
}

// filterTokens returns only meaningful tokens (no whitespace, no comments).
func filterTokens(tokens []Token) []Token {
	result := make([]Token, 0, len(tokens))
	for _, t := range tokens {
		if t.Type == TokenWhitespace || t.Type == TokenComment {
			continue
		}
		result = append(result, t)
	}
	return result
}

// parseAll parses all statements in the token stream.
func (p *Parser) parseAll() ([]*Statement, error) {
	var stmts []*Statement
	for {
		p.skipSemis()
		if p.atEnd() {
			break
		}
		stmt := p.parseStatement()
		if stmt != nil {
			stmts = append(stmts, stmt)
		}
		if p.hasError() {
			return stmts, p.err()
		}
		// Consume optional semicolon
		if p.matchType(TokenSemi) {
			continue
		}
	}
	return stmts, nil
}

// =============================================================================
// Token access helpers
// =============================================================================

func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) advance() Token {
	tok := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

func (p *Parser) atEnd() bool {
	return p.pos >= len(p.tokens) || p.tokens[p.pos].Type == TokenEOF
}

func (p *Parser) hasError() bool {
	return len(p.errs) > 0
}

func (p *Parser) err() error {
	if len(p.errs) == 0 {
		return nil
	}
	return p.errs[0]
}

func (p *Parser) errorf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	p.errs = append(p.errs, fmt.Errorf("parse error: %s", msg))
}

func (p *Parser) expectType(t TokenType) Token {
	tok := p.peek()
	if tok.Type != t {
		p.errorf("expected token type %d, got %d (%q)", t, tok.Type, tok.Value)
		return tok
	}
	return p.advance()
}

func (p *Parser) matchType(t TokenType) bool {
	if p.peek().Type == t {
		p.advance()
		return true
	}
	return false
}

// Keyword matching helpers

// kwLower returns the lowercase value of the current token if it's a keyword,
// or empty string otherwise.
func (p *Parser) kwLower() string {
	tok := p.peek()
	if tok.Type != TokenKeyword {
		return ""
	}
	return strings.ToLower(tok.Value)
}

// isKw checks if the current token is the given keyword.
func (p *Parser) isKw(kw Keyword) bool {
	tok := p.peek()
	if tok.Type != TokenKeyword {
		return false
	}
	lower := strings.ToLower(tok.Value)
	kwStr := keywordStr(kw)
	return lower == kwStr
}

// matchKw advances and returns true if the current token is the given keyword.
func (p *Parser) matchKw(kw Keyword) bool {
	if p.isKw(kw) {
		p.advance()
		return true
	}
	return false
}

// expectKw advances if the current token is the given keyword, otherwise error.
func (p *Parser) expectKw(kw Keyword) Token {
	if p.isKw(kw) {
		return p.advance()
	}
	p.errorf("expected keyword %s, got %q", keywordStr(kw), p.peek().Value)
	return p.peek()
}

// keywordStr returns the lowercase string for a keyword constant.
func keywordStr(kw Keyword) string {
	for s, k := range keywordMap {
		if k == kw {
			return s
		}
	}
	return ""
}

func (p *Parser) skipSemis() {
	for p.peek().Type == TokenSemi {
		p.advance()
	}
}

// =============================================================================
// Identifier helpers
// =============================================================================

// parseIdentOrKeyword parses an identifier or keyword-as-identifier.
// In SQLite, most keywords can also serve as identifiers.
func (p *Parser) parseIdentOrKeyword() string {
	tok := p.peek()
	switch tok.Type {
	case TokenID:
		p.advance()
		return stripQuotes(tok.Value)
	case TokenKeyword:
		// Most keywords can be used as identifiers
		p.advance()
		return tok.Value
	case TokenString:
		// SQLite allows 'string' as an identifier in some contexts (nm rule)
		p.advance()
		return stripQuotes(tok.Value)
	default:
		p.errorf("expected identifier, got %q", tok.Value)
		return ""
	}
}

// parseName parses a name (identifier, keyword-as-id, or string).
func (p *Parser) parseName() string {
	return p.parseIdentOrKeyword()
}

// parseQualifiedName parses schema.name or just name.
func (p *Parser) parseQualifiedName() (schema, name string) {
	first := p.parseName()
	if p.peek().Type == TokenDot {
		p.advance()
		second := p.parseName()
		return first, second
	}
	return "", first
}

// stripQuotes removes surrounding quotes from a quoted identifier or string.
func stripQuotes(s string) string {
	if len(s) < 2 {
		return s
	}
	switch s[0] {
	case '"':
		// Remove surrounding " and replace "" with "
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	case '`':
		return strings.ReplaceAll(s[1:len(s)-1], "``", "`")
	case '[':
		if s[len(s)-1] == ']' {
			return s[1 : len(s)-1]
		}
	case '\'':
		return strings.ReplaceAll(s[1:len(s)-1], `''`, `'`)
	}
	return s
}

// =============================================================================
// Statement dispatch
// =============================================================================

func (p *Parser) parseStatement() *Statement {
	tok := p.peek()
	if tok.Type == TokenKeyword {
		lower := strings.ToLower(tok.Value)
		switch lower {
		case "select", "values":
			return p.parseSelectStatement()
		case "insert", "replace":
			return p.parseInsert()
		case "update":
			return p.parseUpdate()
		case "delete":
			return p.parseDelete()
		case "create":
			return p.parseCreate()
		case "drop":
			return p.parseDrop()
		case "begin":
			return p.parseBegin()
		case "commit", "end":
			return p.parseCommit()
		case "rollback":
			return p.parseRollback()
		case "with":
			return p.parseWithOrSelect()
		case "explain":
			return p.parseExplain()
		default:
			p.errorf("unexpected keyword: %q", tok.Value)
			return nil
		}
	}
	if tok.Type == TokenLParen {
		// Could be a parenthesized SELECT or expression
		return p.parseSelectStatement()
	}
	p.errorf("unexpected token: %q", tok.Value)
	return nil
}

// =============================================================================
// EXPLAIN
// =============================================================================

func (p *Parser) parseExplain() *Statement {
	p.advance() // consume EXPLAIN
	explainQP := false
	if p.isKw(KwQuery) {
		// EXPLAIN QUERY PLAN
		p.advance() // QUERY
		p.expectKw(KwPlan)
		explainQP = true
	}
	stmt := p.parseStatement()
	if stmt != nil {
		stmt.Explain = true
		stmt.ExplainQuery = explainQP
	}
	return stmt
}

// =============================================================================
// WITH / CTE
// =============================================================================

func (p *Parser) parseWithOrSelect() *Statement {
	// WITH ... SELECT ...
	// For now, skip the WITH clause and parse the SELECT
	if p.isKw(KwWith) {
		p.advance() // WITH
		if p.isKw(KwRecursive) {
			p.advance() // RECURSIVE
		}
		// Parse CTE definitions: name AS (select) [, ...]
		p.parseCTEList()
	}
	return p.parseSelectStatement()
}

func (p *Parser) parseCTEList() {
	for {
		p.parseName() // CTE name
		// Optional column list
		if p.peek().Type == TokenLParen {
			p.advance()
			for {
				p.parseName()
				if !p.matchType(TokenComma) {
					break
				}
			}
			p.expectType(TokenRParen)
		}
		p.expectKw(KwAs)
		// Optional MATERIALIZED / NOT MATERIALIZED
		if p.isKw(KwNot) {
			p.advance()
			p.expectKw(KwMaterialized)
		} else if p.isKw(KwMaterialized) {
			p.advance()
		}
		p.expectType(TokenLParen)
		p.parseSelectBody()
		p.expectType(TokenRParen)
		if !p.matchType(TokenComma) {
			break
		}
	}
}

// =============================================================================
// SELECT
// =============================================================================

func (p *Parser) parseSelectStatement() *Statement {
	sel := p.parseSelectBody()
	if sel == nil {
		return nil
	}
	return &Statement{
		Type:       StmtSelect,
		SelectStmt: sel,
	}
}

func (p *Parser) parseSelectBody() *SelectStmt {
	sel := p.parseSelectCore()
	if sel == nil {
		return nil
	}

	// Handle compound selects (UNION, INTERSECT, EXCEPT)
	for {
		var op CompoundOp
		if p.isKw(KwUnion) {
			p.advance()
			if p.isKw(KwAll) {
				p.advance()
				op = CompoundUnionAll
			} else {
				op = CompoundUnion
			}
		} else if p.isKw(KwIntersect) {
			p.advance()
			op = CompoundIntersect
		} else if p.isKw(KwExcept) {
			p.advance()
			op = CompoundExcept
		} else {
			break
		}

		right := p.parseSelectCore()
		if right == nil {
			return sel
		}
		sel.CompoundOps = append(sel.CompoundOps, op)
		sel.CompoundSelects = append(sel.CompoundSelects, right)
	}

	return sel
}

func (p *Parser) parseSelectCore() *SelectStmt {
	sel := &SelectStmt{}

	// Handle VALUES clause
	if p.isKw(KwValues) {
		return p.parseValuesClause()
	}

	if !p.matchKw(KwSelect) {
		p.errorf("expected SELECT, got %q", p.peek().Value)
		return nil
	}

	// DISTINCT or ALL
	if p.matchKw(KwDistinct) {
		sel.Distinct = true
	} else if p.matchKw(KwAll) {
		sel.All = true
	}

	// Result columns
	sel.Columns = p.parseResultColumns()

	// FROM clause
	if p.matchKw(KwFrom) {
		sel.From = p.parseFromClause()
	}

	// WHERE clause
	if p.matchKw(KwWhere) {
		sel.Where = p.parseExpr()
	}

	// GROUP BY
	if p.isKw(KwGroup) {
		p.advance() // GROUP
		p.expectKw(KwBy)
		sel.GroupBy = p.parseExprList()
	}

	// HAVING
	if p.matchKw(KwHaving) {
		sel.Having = p.parseExpr()
	}

	// ORDER BY
	if p.isKw(KwOrder) {
		p.advance() // ORDER
		p.expectKw(KwBy)
		sel.OrderBy = p.parseOrderByList()
	}

	// LIMIT
	if p.matchKw(KwLimit) {
		sel.Limit = p.parseExpr()
		// OFFSET or comma form
		if p.matchKw(KwOffset) {
			sel.Offset = p.parseExpr()
		} else if p.peek().Type == TokenComma {
			// LIMIT offset, count (SQLite syntax)
			p.advance()
			sel.Offset = sel.Limit
			sel.Limit = p.parseExpr()
		}
	}

	return sel
}

// parseValuesClause parses a VALUES clause as a SELECT.
func (p *Parser) parseValuesClause() *SelectStmt {
	p.advance() // VALUES
	sel := &SelectStmt{}
	row := p.parseExprListInParens()
	sel.Columns = exprsToResultCols(row)
	for p.matchType(TokenComma) {
		row = p.parseExprListInParens()
		sel.Columns = append(sel.Columns, exprsToResultCols(row)...)
	}
	return sel
}

func exprsToResultCols(exprs []*Expr) []*ResultCol {
	cols := make([]*ResultCol, len(exprs))
	for i, e := range exprs {
		cols[i] = &ResultCol{Expr: e}
	}
	return cols
}

// parseResultColumns parses the column list after SELECT.
func (p *Parser) parseResultColumns() []*ResultCol {
	var cols []*ResultCol
	first := true
	for {
		// Check for terminators
		tok := p.peek()
		if tok.Type == TokenEOF || tok.Type == TokenSemi {
			break
		}
		if tok.Type == TokenKeyword {
			lower := strings.ToLower(tok.Value)
			switch lower {
			case "from", "where", "group", "having", "order", "limit",
				"union", "intersect", "except", "into", "set",
				"values", "on", "using", "window":
				if !first {
					goto done
				}
			}
		}
		if tok.Type == TokenRParen {
			break
		}

		rc := p.parseResultColumn()
		if rc == nil {
			break
		}
		cols = append(cols, rc)
		first = false

		if !p.matchType(TokenComma) {
			break
		}
	}
done:
	return cols
}

func (p *Parser) parseResultColumn() *ResultCol {
	// Check for * or table.*
	if p.peek().Type == TokenStar {
		p.advance()
		return &ResultCol{Star: true}
	}

	// Check for ident.* pattern
	if p.isIdentLike() {
		saved := p.pos
		name := p.parseName()
		if p.peek().Type == TokenDot {
			p.advance()
			if p.peek().Type == TokenStar {
				p.advance()
				return &ResultCol{TableStar: name}
			}
			// Not table.*, backtrack and parse as expression
			p.pos = saved
		} else {
			p.pos = saved
		}
	}

	// Parse expression
	expr := p.parseExpr()
	if expr == nil {
		return nil
	}

	rc := &ResultCol{Expr: expr}

	// Optional alias: AS name or bare identifier
	if p.matchKw(KwAs) {
		rc.As = p.parseName()
	} else if p.isIdentLike() && !p.isReservedWord() {
		rc.As = p.parseName()
	}

	return rc
}

// isIdentLike returns true if the current token can be used as an identifier.
func (p *Parser) isIdentLike() bool {
	tok := p.peek()
	return tok.Type == TokenID || tok.Type == TokenKeyword || tok.Type == TokenString
}

// isReservedWord returns true if the current token is a keyword that shouldn't
// be used as an alias in certain contexts.
func (p *Parser) isReservedWord() bool {
	tok := p.peek()
	if tok.Type != TokenKeyword {
		return false
	}
	lower := strings.ToLower(tok.Value)
	reserved := map[string]bool{
		"from": true, "where": true, "group": true, "having": true,
		"order": true, "limit": true, "union": true, "intersect": true,
		"except": true, "on": true, "join": true, "inner": true,
		"left": true, "right": true, "full": true, "cross": true,
		"natural": true, "using": true, "and": true, "or": true,
		"not": true, "as": true, "into": true, "values": true,
		"set": true, "select": true, "insert": true, "update": true,
		"delete": true, "create": true, "drop": true, "begin": true,
		"commit": true, "rollback": true, "then": true, "when": true,
		"else": true, "end": true, "case": true, "between": true,
		"like": true, "in": true, "is": true, "null": true,
		"exists": true, "distinct": true, "all": true, "window": true,
	}
	return reserved[lower]
}

// =============================================================================
// FROM clause
// =============================================================================

func (p *Parser) parseFromClause() *FromClause {
	fc := &FromClause{}

	// Parse first table reference
	ref := p.parseTableRef()
	if ref != nil {
		fc.Tables = append(fc.Tables, ref)
	}

	// Parse additional joins
	for {
		jt := p.parseJoinType()
		if jt == JoinNone && !p.peekJoinKeyword() {
			break
		}
		if jt == JoinNone {
			// Comma join
			if !p.matchType(TokenComma) {
				break
			}
			jt = JoinInner
		}

		ref := p.parseTableRef()
		if ref == nil {
			break
		}
		ref.JoinType = jt

		// ON or USING
		if p.matchKw(KwOn) {
			ref.On = p.parseExpr()
		} else if p.isKw(KwUsing) {
			p.advance()
			ref.Using = p.parseIdentList()
		}

		fc.Tables = append(fc.Tables, ref)
	}

	return fc
}

// peekJoinKeyword returns true if the current token starts a JOIN keyword sequence.
func (p *Parser) peekJoinKeyword() bool {
	tok := p.peek()
	if tok.Type == TokenComma {
		return true
	}
	if tok.Type != TokenKeyword {
		return false
	}
	lower := strings.ToLower(tok.Value)
	return lower == "join" || lower == "inner" || lower == "left" ||
		lower == "right" || lower == "full" || lower == "cross" ||
		lower == "natural"
}

// parseJoinType parses and returns the join type, consuming tokens.
// Returns JoinNone if no join keyword is found (i.e., just a comma).
func (p *Parser) parseJoinType() JoinType {
	// Check for NATURAL prefix
	natural := false
	if p.isKw(KwNatural) {
		natural = true
		p.advance()
	}

	// Join type keyword
	jt := JoinInner
	if p.isKw(KwLeft) {
		p.advance()
		p.matchKw(KwOuter) // optional OUTER
		jt = JoinLeft
	} else if p.isKw(KwRight) {
		p.advance()
		p.matchKw(KwOuter)
		jt = JoinRight
	} else if p.isKw(KwFull) {
		p.advance()
		p.matchKw(KwOuter)
		jt = JoinFull
	} else if p.isKw(KwCross) {
		p.advance()
		jt = JoinCross
	} else if p.isKw(KwInner) {
		p.advance()
		jt = JoinInner
	}

	// JOIN keyword
	if natural || jt != JoinInner || p.isKw(KwJoin) {
		if !p.matchKw(KwJoin) {
			if !natural {
				return JoinNone
			}
			p.errorf("expected JOIN after NATURAL")
			return JoinNone
		}
	} else {
		return JoinNone
	}

	if natural {
		jt = JoinNatural
	}
	return jt
}

// parseTableRef parses a single table reference (name, subquery, etc.).
func (p *Parser) parseTableRef() *TableRef {
	ref := &TableRef{}

	if p.peek().Type == TokenLParen {
		p.advance()
		// Subquery
		if p.isKw(KwSelect) || p.isKw(KwValues) || p.isKw(KwWith) {
			ref.Subquery = p.parseSelectBody()
			p.expectType(TokenRParen)
		} else {
			// Parenthesized table list (joined subquery)
			inner := p.parseFromClause()
			p.expectType(TokenRParen)
			if len(inner.Tables) == 1 {
				*ref = *inner.Tables[0]
			} else {
				// Wrap as subquery
				ref.Subquery = &SelectStmt{From: inner}
			}
		}
	} else {
		// Table name or function call
		schema, name := p.parseQualifiedName()
		ref.Schema = schema
		ref.Name = name

		// Check for function call: name(args)
		if p.peek().Type == TokenLParen && ref.Schema == "" {
			p.advance()
			if p.peek().Type == TokenStar && p.lookaheadRParen() {
				p.advance() // *
				p.expectType(TokenRParen)
				// Table-valued function with star
				ref.FuncArgs = nil // Mark as function
			} else {
				args := p.parseExprList()
				ref.FuncArgs = args
				p.expectType(TokenRParen)
			}
			// This is a table-valued function, not a regular table
		}
	}

	// Optional alias
	if p.matchKw(KwAs) {
		ref.Alias = p.parseName()
	} else if p.isIdentLike() && !p.peekJoinKeyword() && !p.isReservedForOnUsing() {
		ref.Alias = p.parseName()
	}

	return ref
}

func (p *Parser) isReservedForOnUsing() bool {
	tok := p.peek()
	if tok.Type != TokenKeyword {
		return false
	}
	lower := strings.ToLower(tok.Value)
	return lower == "on" || lower == "using" || lower == "where" ||
		lower == "group" || lower == "having" || lower == "order" ||
		lower == "limit" || lower == "join" || lower == "inner" ||
		lower == "left" || lower == "right" || lower == "cross" ||
		lower == "natural" || lower == "full"
}

func (p *Parser) lookaheadRParen() bool {
	if p.pos+1 < len(p.tokens) {
		return p.tokens[p.pos].Type == TokenRParen
	}
	return false
}

// =============================================================================
// ORDER BY
// =============================================================================

func (p *Parser) parseOrderByList() []*OrderItem {
	var items []*OrderItem
	for {
		expr := p.parseExpr()
		if expr == nil {
			break
		}
		item := &OrderItem{Expr: expr}

		// ASC or DESC
		if p.matchKw(KwAsc) {
			item.Order = SortAsc
		} else if p.matchKw(KwDesc) {
			item.Order = SortDesc
		}

		// NULLS FIRST / NULLS LAST
		if p.isKw(KwNulls) {
			p.advance()
			if p.matchKw(KwFirst) {
				// Keep current sort order but note nulls first
			} else if p.matchKw(KwLast) {
				// Keep current sort order but note nulls last
			}
		}

		items = append(items, item)
		if !p.matchType(TokenComma) {
			break
		}
	}
	return items
}

// =============================================================================
// INSERT
// =============================================================================

func (p *Parser) parseInsert() *Statement {
	stmt := &InsertStmt{}

	orReplace := false
	if p.isKw(KwReplace) {
		p.advance()
		orReplace = true
	} else {
		p.expectKw(KwInsert)
		// OR clause
		if p.isKw(KwOr) {
			p.advance()
			switch {
			case p.matchKw(KwReplace):
				orReplace = true
			case p.matchKw(KwAbort):
				stmt.OrAbort = true
			case p.matchKw(KwFail):
				stmt.OrFail = true
			case p.matchKw(KwIgnore):
				stmt.OrIgnore = true
			default:
				p.errorf("expected REPLACE, ABORT, FAIL, or IGNORE after OR")
			}
		}
	}
	stmt.OrReplace = orReplace

	p.expectKw(KwInto)

	// Table name with optional alias
	schema, name := p.parseQualifiedName()
	stmt.Table = &TableRef{Schema: schema, Name: name}

	// Optional column list
	if p.peek().Type == TokenLParen && !p.isKw(KwSelect) && !p.isKw(KwDefault) {
		p.advance()
		stmt.Columns = p.parseIdentList()
		p.expectType(TokenRParen)
	}

	// VALUES or SELECT or DEFAULT VALUES
	if p.isKw(KwDefault) {
		p.advance()
		p.expectKw(KwValues)
		stmt.DefaultValues = true
	} else if p.isKw(KwSelect) || p.isKw(KwValues) {
		sel := p.parseSelectBody()
		if sel != nil {
			if len(sel.Columns) > 0 && len(sel.CompoundOps) == 0 && sel.From == nil && sel.Where == nil {
				// This looks like VALUES - convert to rows
				stmt.Values = extractValueRows(sel)
			} else {
				stmt.Select = sel
			}
		}
	} else if p.isKw(KwValues) {
		p.advance()
		row := p.parseExprListInParens()
		stmt.Values = append(stmt.Values, row)
		for p.matchType(TokenComma) {
			row = p.parseExprListInParens()
			stmt.Values = append(stmt.Values, row)
		}
	}

	return &Statement{
		Type:       StmtInsert,
		InsertStmt: stmt,
	}
}

// extractValueRows tries to extract row values from a VALUES-style SELECT.
func extractValueRows(sel *SelectStmt) [][]*Expr {
	var rows [][]*Expr
	row := make([]*Expr, len(sel.Columns))
	for i, rc := range sel.Columns {
		row[i] = rc.Expr
	}
	rows = append(rows, row)

	// Check compound selects for more rows
	for _, cs := range sel.CompoundSelects {
		row := make([]*Expr, len(cs.Columns))
		for i, rc := range cs.Columns {
			row[i] = rc.Expr
		}
		rows = append(rows, row)
	}
	return rows
}

// =============================================================================
// UPDATE
// =============================================================================

func (p *Parser) parseUpdate() *Statement {
	stmt := &UpdateStmt{}

	p.expectKw(KwUpdate)

	// OR clause
	if p.isKw(KwOr) {
		p.advance()
		switch {
		case p.matchKw(KwReplace):
			stmt.OrReplace = true
		case p.matchKw(KwAbort):
			stmt.OrAbort = true
		case p.matchKw(KwFail):
			stmt.OrFail = true
		case p.matchKw(KwIgnore):
			stmt.OrIgnore = true
		default:
			p.errorf("expected REPLACE, ABORT, FAIL, or IGNORE after OR")
		}
	}

	// Table name
	schema, name := p.parseQualifiedName()
	stmt.Table = &TableRef{Schema: schema, Name: name}

	// Optional alias
	if p.matchKw(KwAs) {
		stmt.Table.Alias = p.parseName()
	} else if p.isIdentLike() && !p.isKw(KwSet) {
		stmt.Table.Alias = p.parseName()
	}

	p.expectKw(KwSet)

	// SET clauses
	stmt.Sets = p.parseSetClauses()

	// Optional FROM clause
	if p.isKw(KwFrom) {
		p.advance()
		stmt.From = p.parseFromClause()
	}

	// WHERE
	if p.matchKw(KwWhere) {
		stmt.Where = p.parseExpr()
	}

	return &Statement{
		Type:       StmtUpdate,
		UpdateStmt: stmt,
	}
}

func (p *Parser) parseSetClauses() []*SetClause {
	var sets []*SetClause
	for {
		sc := p.parseSetClause()
		if sc == nil {
			break
		}
		sets = append(sets, sc)
		if !p.matchType(TokenComma) {
			break
		}
	}
	return sets
}

func (p *Parser) parseSetClause() *SetClause {
	// Check for (col1, col2) = expr form
	if p.peek().Type == TokenLParen {
		p.advance()
		cols := p.parseIdentList()
		p.expectType(TokenRParen)
		p.expectType(TokenEq)
		expr := p.parseExpr()
		return &SetClause{Columns: cols, Value: expr}
	}

	name := p.parseName()
	p.expectType(TokenEq)
	expr := p.parseExpr()
	return &SetClause{Columns: []string{name}, Value: expr}
}

// =============================================================================
// DELETE
// =============================================================================

func (p *Parser) parseDelete() *Statement {
	stmt := &DeleteStmt{}

	p.expectKw(KwDelete)
	p.expectKw(KwFrom)

	// Table name with optional alias
	schema, name := p.parseQualifiedName()
	stmt.Table = &TableRef{Schema: schema, Name: name}

	// Optional alias
	if p.matchKw(KwAs) {
		stmt.Table.Alias = p.parseName()
	} else if p.isIdentLike() && !p.isKw(KwWhere) && !p.isKw(KwWhere) && p.peek().Type != TokenSemi && p.peek().Type != TokenEOF {
		// In SQLite, aliases are allowed in DELETE's FROM
		if p.peek().Type == TokenKeyword {
			lower := strings.ToLower(p.peek().Value)
			if lower != "where" && lower != "order" && lower != "limit" && lower != "indexed" {
				stmt.Table.Alias = p.parseName()
			}
		}
	}

	// WHERE
	if p.matchKw(KwWhere) {
		stmt.Where = p.parseExpr()
	}

	return &Statement{
		Type:       StmtDelete,
		DeleteStmt: stmt,
	}
}

// =============================================================================
// CREATE statements
// =============================================================================

func (p *Parser) parseCreate() *Statement {
	p.expectKw(KwCreate)

	if p.isKw(KwTemp) || p.isKw(KwTemporary) {
		// CREATE TEMP/TABLE ...
		p.advance()
		return p.parseCreateTable(true)
	}
	if p.isKw(KwTable) {
		return p.parseCreateTable(false)
	}
	if p.isKw(KwUnique) {
		return p.parseCreateIndex(true)
	}
	if p.isKw(KwIndex) {
		return p.parseCreateIndex(false)
	}
	if p.isKw(KwView) {
		// CREATE VIEW - parse minimally
		p.advance()
		p.parseQualifiedName() // view name
		if p.matchKw(KwAs) {
			p.parseSelectBody()
		}
		return &Statement{Type: StmtCreateView}
	}
	if p.isKw(KwVirtual) {
		// CREATE VIRTUAL TABLE - parse minimally
		p.advance()
		p.expectKw(KwTable)
		p.parseQualifiedName() // table name
		// consume rest until semicolon
		for !p.atEnd() && p.peek().Type != TokenSemi {
			p.advance()
		}
		return &Statement{Type: StmtCreateTable}
	}
	if p.isKw(KwTrigger) {
		// CREATE TRIGGER - parse minimally
		p.advance()
		for !p.atEnd() && p.peek().Type != TokenSemi {
			p.advance()
		}
		return &Statement{Type: StmtCreateTrigger}
	}

	p.errorf("expected TABLE, INDEX, UNIQUE, VIEW, TRIGGER, or VIRTUAL after CREATE")
	return nil
}

// =============================================================================
// CREATE TABLE
// =============================================================================

func (p *Parser) parseCreateTable(temp bool) *Statement {
	stmt := &CreateTableStmt{Temp: temp}
	p.expectKw(KwTable)

	// IF NOT EXISTS
	if p.isKw(KwIf) {
		p.advance()
		p.expectKw(KwNot)
		p.expectKw(KwExists)
		stmt.IfNotExists = true
	}

	// Table name
	schema, name := p.parseQualifiedName()
	stmt.Schema = schema
	stmt.Name = name

	if p.peek().Type == TokenLParen {
		p.advance()
		p.parseColumnAndConstraintList(stmt)
		p.expectType(TokenRParen)

		// Table options (WITHOUT ROWID, STRICT)
		for p.isKw(KwWithout) || (p.peek().Type == TokenComma) {
			if p.peek().Type == TokenComma {
				p.advance()
			}
			if p.isKw(KwWithout) {
				p.advance()
				opt := p.parseName()
				if strings.ToLower(opt) == "rowid" {
					stmt.WithoutRowid = true
				}
			} else if p.peek().Type == TokenID || p.peek().Type == TokenKeyword {
				opt := p.parseName()
				if strings.ToLower(opt) == "strict" {
					stmt.Strict = true
				}
			}
		}
	} else if p.matchKw(KwAs) {
		stmt.AsSelect = p.parseSelectBody()
	}

	return &Statement{
		Type:        StmtCreateTable,
		CreateTable: stmt,
	}
}

func (p *Parser) parseColumnAndConstraintList(stmt *CreateTableStmt) {
	for {
		// Check for table-level constraint keywords
		if p.isTableConstraint() {
			tc := p.parseTableConstraint()
			if tc != nil {
				stmt.Constraints = append(stmt.Constraints, tc)
			}
		} else {
			cd := p.parseColumnDef()
			if cd != nil {
				stmt.Columns = append(stmt.Columns, cd)
			}
		}
		if !p.matchType(TokenComma) {
			break
		}
	}
}

func (p *Parser) isTableConstraint() bool {
	tok := p.peek()
	if tok.Type == TokenKeyword {
		lower := strings.ToLower(tok.Value)
		if lower == "primary" || lower == "unique" || lower == "check" ||
			lower == "foreign" || lower == "constraint" {
			return true
		}
	}
	return false
}

func (p *Parser) parseColumnDef() *ColumnDef {
	cd := &ColumnDef{
		Name: p.parseName(),
	}

	// Type name (optional, can be multiple tokens)
	cd.Type = p.parseTypeName()

	// Column constraints
	for {
		cc := p.tryParseColumnConstraint()
		if cc == nil {
			break
		}
		cd.Constraints = append(cd.Constraints, cc)
	}

	return cd
}

// parseTypeName parses a SQL type name like INTEGER, TEXT(100), VARCHAR(255), etc.
func (p *Parser) parseTypeName() string {
	var parts []string

	// First word of type name
	if p.isTypeNameToken() {
		parts = append(parts, p.advance().Value)
	}

	// Additional type name tokens
	for p.isTypeNameToken() {
		parts = append(parts, p.advance().Value)
	}

	// Optional (length) or (precision, scale)
	if p.peek().Type == TokenLParen {
		p.advance()
		parts = append(parts, "(")
		// Signed number
		if p.peek().Type == TokenPlus {
			p.advance()
		}
		parts = append(parts, p.advance().Value) // number
		if p.matchType(TokenComma) {
			parts = append(parts, ",")
			if p.peek().Type == TokenPlus {
				p.advance()
			}
			parts = append(parts, p.advance().Value) // number
		}
		p.expectType(TokenRParen)
		parts = append(parts, ")")
	}

	return strings.Join(parts, "")
}

func (p *Parser) isTypeNameToken() bool {
	tok := p.peek()
	if tok.Type == TokenID {
		return true
	}
	if tok.Type == TokenKeyword {
		lower := strings.ToLower(tok.Value)
		switch lower {
		case "int", "integer", "text", "real", "blob", "varchar", "nvarchar",
			"char", "nchar", "clob", "float", "double", "boolean",
			"bigint", "smallint", "tinyint", "mediumint",
			"decimal", "numeric", "datetime", "date", "time",
			"varying", "character":
			return true
		}
	}
	return false
}

func (p *Parser) tryParseColumnConstraint() *ColumnConstraint {
	// CONSTRAINT name
	if p.isKw(KwConstraint) {
		p.advance()
		name := p.parseName()
		cc := p.parseColumnConstraintBody()
		if cc != nil {
			cc.Name = name
		}
		return cc
	}
	return p.parseColumnConstraintBody()
}

func (p *Parser) parseColumnConstraintBody() *ColumnConstraint {
	// PRIMARY KEY
	if p.isKw(KwPrimary) {
		p.advance()
		p.expectKw(KwKey)
		cc := &ColumnConstraint{Type: CCPrimaryKey, PrimaryKey: true}
		// ASC/DESC
		p.matchKw(KwAsc)
		p.matchKw(KwDesc)
		// AUTOINCREMENT
		if p.matchKw(KwAutoincrement) {
			cc.Autoincrement = true
		}
		// ON CONFLICT
		cc.OnConflict = p.tryParseOnConflict()
		return cc
	}

	// NOT NULL
	if p.isKw(KwNot) {
		p.advance()
		p.expectKw(KwNull)
		cc := &ColumnConstraint{Type: CCNotNull, NotNull: true}
		cc.OnConflict = p.tryParseOnConflict()
		return cc
	}

	// NULL
	if p.isKw(KwNull) {
		p.advance()
		// NULL constraint is a no-op but legal in SQLite
		return &ColumnConstraint{Type: CCNotNull}
	}

	// UNIQUE
	if p.matchKw(KwUnique) {
		return &ColumnConstraint{Type: CCUnique, Unique: true}
	}

	// CHECK
	if p.isKw(KwCheck) {
		p.advance()
		p.expectType(TokenLParen)
		expr := p.parseExpr()
		p.expectType(TokenRParen)
		return &ColumnConstraint{Type: CCCheck, Check: expr}
	}

	// DEFAULT
	if p.isKw(KwDefault) {
		p.advance()
		var expr *Expr
		// DEFAULT (expr) form
		if p.peek().Type == TokenLParen {
			p.advance()
			expr = p.parseExpr()
			p.expectType(TokenRParen)
		} else if p.peek().Type == TokenPlus || p.peek().Type == TokenMinus {
			// DEFAULT +num or DEFAULT -num
			op := p.advance().Value
			num := p.parseExpr()
			expr = &Expr{
				Kind:  ExprUnaryOp,
				Op:    op,
				Right: num,
			}
		} else {
			expr = p.parseExpr()
		}
		return &ColumnConstraint{Type: CCDefault, Default: expr}
	}

	// COLLATE
	if p.isKw(KwCollate) {
		p.advance()
		collName := p.parseName()
		return &ColumnConstraint{Type: CCCollate, Collation: collName}
	}

	// REFERENCES
	if p.isKw(KwReferences) {
		p.advance()
		table := p.parseName()
		var cols []string
		if p.peek().Type == TokenLParen {
			p.advance()
			cols = p.parseIdentList()
			p.expectType(TokenRParen)
		}
		fk := &ForeignKeyRef{Table: table, Columns: cols}
		// Consume optional referential actions
		p.parseRefArgs()
		return &ColumnConstraint{Type: CCForeignKey, ForeignKey: fk}
	}

	// GENERATED ALWAYS AS or just AS (generated column)
	if p.isKw(KwGenerated) {
		p.advance()
		p.matchKw(KwAlways)
		p.expectKw(KwAs)
		p.expectType(TokenLParen)
		p.parseExpr()
		p.expectType(TokenRParen)
		// Optional VIRTUAL/STORED
		if p.isKw(KwVirtual) {
			p.advance()
		}
		return &ColumnConstraint{Type: CCGenerated}
	}

	return nil
}

func (p *Parser) tryParseOnConflict() string {
	if p.isKw(KwOn) {
		saved := p.pos
		p.advance()
		if p.isKw(KwConflict) {
			p.advance()
			action := p.parseName()
			return strings.ToUpper(action)
		}
		p.pos = saved
	}
	return ""
}

func (p *Parser) parseRefArgs() {
	for {
		if p.isKw(KwOn) {
			saved := p.pos
			p.advance()
			if p.matchKw(KwDelete) || p.matchKw(KwUpdate) {
				// SET NULL, SET DEFAULT, CASCADE, RESTRICT, NO ACTION
				if p.matchKw(KwCascade) || p.matchKw(KwRestrict) {
					// consumed
				} else if p.isKw(KwSet) {
					p.advance()
					p.parseName() // NULL or DEFAULT
				} else if p.isKw(KwNo) {
					p.advance()
					p.expectKw(KwAction)
				}
				continue
			}
			p.pos = saved
		}
		break
	}
}

// =============================================================================
// Table constraints
// =============================================================================

func (p *Parser) parseTableConstraint() *TableConstraint {
	tc := &TableConstraint{}

	// CONSTRAINT name
	if p.isKw(KwConstraint) {
		p.advance()
		tc.Name = p.parseName()
	}

	// PRIMARY KEY
	if p.isKw(KwPrimary) {
		p.advance()
		p.expectKw(KwKey)
		tc.Type = TCPrimaryKey
		p.expectType(TokenLParen)
		tc.OrderBy = p.parseOrderByList()
		p.expectType(TokenRParen)
		if p.matchKw(KwAutoincrement) {
			// autoincrement on table-level PK
		}
		tc.OnConflict = p.tryParseOnConflict()
		return tc
	}

	// UNIQUE
	if p.matchKw(KwUnique) {
		tc.Type = TCUnique
		p.expectType(TokenLParen)
		tc.OrderBy = p.parseOrderByList()
		p.expectType(TokenRParen)
		tc.OnConflict = p.tryParseOnConflict()
		return tc
	}

	// CHECK
	if p.isKw(KwCheck) {
		p.advance()
		p.expectType(TokenLParen)
		tc.Check = p.parseExpr()
		p.expectType(TokenRParen)
		tc.Type = TCCheck
		return tc
	}

	// FOREIGN KEY
	if p.isKw(KwForeign) {
		p.advance()
		p.expectKw(KwKey)
		p.expectType(TokenLParen)
		cols := p.parseIdentList()
		p.expectType(TokenRParen)
		p.expectKw(KwReferences)
		refTable := p.parseName()
		var refCols []string
		if p.peek().Type == TokenLParen {
			p.advance()
			refCols = p.parseIdentList()
			p.expectType(TokenRParen)
		}
		fkc := &ForeignKeyClause{
			Columns:    cols,
			RefTable:   refTable,
			RefColumns: refCols,
		}
		p.parseRefArgs()
		tc.Type = TCForeignKey
		tc.ForeignKey = fkc
		return tc
	}

	p.errorf("expected table constraint")
	return nil
}

// =============================================================================
// CREATE INDEX
// =============================================================================

func (p *Parser) parseCreateIndex(unique bool) *Statement {
	if unique {
		p.expectKw(KwUnique)
	}
	stmt := &CreateIndexStmt{Unique: unique}
	p.expectKw(KwIndex)

	// IF NOT EXISTS
	if p.isKw(KwIf) {
		p.advance()
		p.expectKw(KwNot)
		p.expectKw(KwExists)
		stmt.IfNotExists = true
	}

	// Index name
	schema, name := p.parseQualifiedName()
	stmt.Schema = schema
	stmt.Name = name

	p.expectKw(KwOn)

	// Table name
	stmt.Table = p.parseName()

	// Column list
	p.expectType(TokenLParen)
	stmt.Columns = p.parseOrderByList()
	p.expectType(TokenRParen)

	// WHERE clause for partial index
	if p.matchKw(KwWhere) {
		stmt.Where = p.parseExpr()
	}

	return &Statement{
		Type:        StmtCreateIndex,
		CreateIndex: stmt,
	}
}

// =============================================================================
// DROP statements
// =============================================================================

func (p *Parser) parseDrop() *Statement {
	p.expectKw(KwDrop)

	if p.isKw(KwTable) {
		return p.parseDropTable()
	}
	if p.isKw(KwIndex) {
		return p.parseDropIndex()
	}
	if p.isKw(KwView) {
		p.advance()
		p.parseIfExists()
		p.parseQualifiedName()
		return &Statement{Type: StmtDropView}
	}
	if p.isKw(KwTrigger) {
		p.advance()
		ifExists := p.parseIfExists()
		p.parseQualifiedName()
		_ = ifExists
		return &Statement{Type: StmtDropTrigger}
	}

	p.errorf("expected TABLE, INDEX, VIEW, or TRIGGER after DROP")
	return nil
}

func (p *Parser) parseDropTable() *Statement {
	p.expectKw(KwTable)
	stmt := &DropTableStmt{}
	stmt.IfExists = p.parseIfExists()
	schema, name := p.parseQualifiedName()
	stmt.Schema = schema
	stmt.Name = name
	return &Statement{
		Type:     StmtDropTable,
		DropTable: stmt,
	}
}

func (p *Parser) parseDropIndex() *Statement {
	p.expectKw(KwIndex)
	stmt := &DropIndexStmt{}
	stmt.IfExists = p.parseIfExists()
	schema, name := p.parseQualifiedName()
	stmt.Schema = schema
	stmt.Name = name
	return &Statement{
		Type:     StmtDropIndex,
		DropIndex: stmt,
	}
}

func (p *Parser) parseIfExists() bool {
	if p.isKw(KwIf) {
		p.advance()
		p.expectKw(KwExists)
		return true
	}
	return false
}

// =============================================================================
// Transaction statements
// =============================================================================

func (p *Parser) parseBegin() *Statement {
	p.expectKw(KwBegin)
	stmt := &BeginStmt{}

	// Optional transaction type
	if p.isKw(KwDeferred) {
		p.advance()
		stmt.Deferred = true
	} else if p.isKw(KwImmediate) {
		p.advance()
		stmt.Immediate = true
	} else if p.isKw(KwExclusive) {
		p.advance()
		stmt.Exclusive = true
	}

	// Optional TRANSACTION keyword
	p.matchKw(KwTransaction)

	return &Statement{
		Type:      StmtBegin,
		BeginStmt: stmt,
	}
}

func (p *Parser) parseCommit() *Statement {
	p.advance() // COMMIT or END
	p.matchKw(KwTransaction)
	return &Statement{
		Type:       StmtCommit,
		CommitStmt: &CommitStmt{},
	}
}

func (p *Parser) parseRollback() *Statement {
	p.expectKw(KwRollback)
	stmt := &RollbackStmt{}
	p.matchKw(KwTransaction)

	// Optional TO savepoint
	if p.matchKw(KwTo) {
		p.matchKw(KwSavepoint)
		stmt.Savepoint = p.parseName()
	}

	return &Statement{
		Type:         StmtRollback,
		RollbackStmt: stmt,
	}
}

// =============================================================================
// Expression parsing - precedence climbing
// =============================================================================

// Precedence levels (lower binds more loosely):
//
//	1: OR
//	2: AND
//	3: NOT
//	4: comparison (=, <, >, <=, >=, !=, <>, IS, IS NULL, IN, BETWEEN, LIKE)
//	5: addition (+, -)
//	6: multiplication (*, /, %)
//	7: unary (+, -, ~, NOT)
//	8: primary (literals, identifiers, parens, function calls, etc.)

func (p *Parser) parseExpr() *Expr {
	return p.parseOrExpr()
}

func (p *Parser) parseOrExpr() *Expr {
	left := p.parseAndExpr()
	for p.isKw(KwOr) {
		p.advance()
		right := p.parseAndExpr()
		left = &Expr{Kind: ExprBinaryOp, Op: "OR", Left: left, Right: right}
	}
	return left
}

func (p *Parser) parseAndExpr() *Expr {
	left := p.parseNotExpr()
	for p.isKw(KwAnd) {
		p.advance()
		right := p.parseNotExpr()
		left = &Expr{Kind: ExprBinaryOp, Op: "AND", Left: left, Right: right}
	}
	return left
}

func (p *Parser) parseNotExpr() *Expr {
	if p.isKw(KwNot) {
		// Check for NOT EXISTS
		if p.isNextKw(KwExists) {
			p.advance() // consume NOT
			p.advance() // consume EXISTS
			p.expectType(TokenLParen)
			sel := p.parseSelectBody()
			p.expectType(TokenRParen)
			return &Expr{
				Kind: ExprUnaryOp,
				Op:   "NOT",
				Right: &Expr{
					Kind:    ExprExists,
					Select: sel,
				},
			}
		}
		// Standalone NOT (logical negation)
		p.advance() // consume NOT
		right := p.parseNotExpr()
		return &Expr{Kind: ExprUnaryOp, Op: "NOT", Right: right}
	}
	return p.parseComparisonExpr()
}

func (p *Parser) parseComparisonExpr() *Expr {
	left := p.parseAddExpr()

	for {
		// =, <, >, <=, >=, !=, <>
		if p.peek().Type == TokenEq || p.peek().Type == TokenNe ||
			p.peek().Type == TokenLt || p.peek().Type == TokenLe ||
			p.peek().Type == TokenGt || p.peek().Type == TokenGe {
			op := p.advance().Value
			right := p.parseAddExpr()
			left = &Expr{Kind: ExprBinaryOp, Op: op, Left: left, Right: right}
			continue
		}

		// IS [NOT] NULL / IS [NOT] expr
		if p.isKw(KwIs) {
			p.advance()
			not := false
			if p.matchKw(KwNot) {
				not = true
			}
			if p.isKw(KwNull) {
				p.advance()
				if not {
					left = &Expr{Kind: ExprIsNotNull, Right: left}
				} else {
					left = &Expr{Kind: ExprIsNull, Right: left}
				}
			} else if p.isKw(KwDistinct) {
				// IS [NOT] DISTINCT FROM
				p.advance()
				p.expectKw(KwFrom)
				right := p.parseAddExpr()
				if not {
					// IS NOT DISTINCT FROM → IS
					left = &Expr{Kind: ExprBinaryOp, Op: "IS", Left: left, Right: right}
				} else {
					// IS DISTINCT FROM → IS NOT
					left = &Expr{Kind: ExprBinaryOp, Op: "IS NOT", Left: left, Right: right}
				}
			} else {
				right := p.parseAddExpr()
				if not {
					left = &Expr{Kind: ExprBinaryOp, Op: "IS NOT", Left: left, Right: right}
				} else {
					left = &Expr{Kind: ExprBinaryOp, Op: "IS", Left: left, Right: right}
				}
			}
			continue
		}

		// ISNULL / NOTNULL postfix operators
		if p.isKw(KwIsnull) {
			p.advance()
			left = &Expr{Kind: ExprIsNull, Right: left}
			continue
		}
		if p.isKw(KwNotnull) {
			p.advance()
			left = &Expr{Kind: ExprIsNotNull, Right: left}
			continue
		}

		// NOT NULL (postfix)
		if p.isKw(KwNot) {
			saved := p.pos
			p.advance()
			if p.isKw(KwNull) {
				p.advance()
				left = &Expr{Kind: ExprIsNotNull, Right: left}
				continue
			}
			p.pos = saved
		}

		// IN
		if p.isKw(KwIn) || (p.isKw(KwNot) && p.isNextKw(KwIn)) {
			not := false
			if p.isKw(KwNot) {
				p.advance()
				not = true
			}
			p.advance() // IN
			left = p.parseInExpr(left, not)
			continue
		}

		// BETWEEN
		if p.isKw(KwBetween) || (p.isKw(KwNot) && p.isNextKw(KwBetween)) {
			not := false
			if p.isKw(KwNot) {
				p.advance()
				not = true
			}
			p.advance() // BETWEEN
			low := p.parseAddExpr()
			p.expectKw(KwAnd)
			high := p.parseAddExpr()
			left = &Expr{
				Kind: ExprBetween,
				Low:  low,
				High: high,
				Left: left,
				Not:  not,
			}
			continue
		}

		// LIKE, GLOB, MATCH, REGEXP
		if p.isKw(KwLike) || p.isKw(KwGlob) || p.isKw(KwMatch) || p.isKw(KwRegexp) ||
			(p.isKw(KwNot) && p.isNextOneOfKw(KwLike, KwGlob, KwMatch, KwRegexp)) {
			not := false
			if p.isKw(KwNot) {
				p.advance()
				not = true
			}
			var kind ExprKind
			var opName string
			switch {
			case p.isKw(KwLike):
				kind = ExprLike
				opName = "LIKE"
				p.advance()
			case p.isKw(KwGlob):
				kind = ExprGlob
				opName = "GLOB"
				p.advance()
			case p.isKw(KwMatch):
				kind = ExprMatch
				opName = "MATCH"
				p.advance()
			case p.isKw(KwRegexp):
				kind = ExprRegexp
				opName = "REGEXP"
				p.advance()
			}
			pattern := p.parseAddExpr()
			var escape *Expr
			if p.matchKw(KwEscape) {
				escape = p.parseAddExpr()
			}
			left = &Expr{
				Kind:    kind,
				Op:      opName,
				Left:    left,
				Pattern: pattern,
				Escape:  escape,
				Not:     not,
			}
			continue
		}

		// COLLATE
		if p.isKw(KwCollate) {
			p.advance()
			collName := p.parseName()
			left = &Expr{Kind: ExprCollate, Left: left, Collation: collName}
			continue
		}

		break
	}

	return left
}

// isNextKw checks if the token after the current one is the given keyword.
func (p *Parser) isNextKw(kw Keyword) bool {
	if p.pos+1 >= len(p.tokens) {
		return false
	}
	tok := p.tokens[p.pos+1]
	if tok.Type != TokenKeyword {
		return false
	}
	return strings.ToLower(tok.Value) == keywordStr(kw)
}

func (p *Parser) isNextOneOfKw(kws ...Keyword) bool {
	for _, kw := range kws {
		if p.isNextKw(kw) {
			return true
		}
	}
	return false
}

// parseInExpr parses the right side of an IN expression.
func (p *Parser) parseInExpr(left *Expr, not bool) *Expr {
	// Check for subquery: (SELECT ...)
	if p.peek().Type == TokenLParen {
		saved := p.pos
		p.advance()
		if p.isKw(KwSelect) || p.isKw(KwValues) || p.isKw(KwWith) {
			sel := p.parseSelectBody()
			p.expectType(TokenRParen)
			return &Expr{
				Kind:     ExprInSelect,
				Left:     left,
				InSelect: sel,
				Not:      not,
			}
		}
		p.pos = saved

		// List of values: (expr1, expr2, ...)
		p.advance() // (
		values := p.parseExprList()
		p.expectType(TokenRParen)
		return &Expr{
			Kind:     ExprInList,
			Left:     left,
			InValues: values,
			Not:      not,
		}
	}

	// Table or function reference: table_name or schema.table_name
	schema, name := p.parseQualifiedName()
	_ = schema
	return &Expr{
		Kind:   ExprInTable,
		Left:   left,
		InTable: name,
		Not:    not,
	}
}

func (p *Parser) parseAddExpr() *Expr {
	left := p.parseMulExpr()
	for {
		if p.peek().Type == TokenPlus || p.peek().Type == TokenMinus {
			op := p.advance().Value
			right := p.parseMulExpr()
			left = &Expr{Kind: ExprBinaryOp, Op: op, Left: left, Right: right}
		} else if p.peek().Type == TokenConcat {
			op := p.advance().Value
			right := p.parseMulExpr()
			left = &Expr{Kind: ExprBinaryOp, Op: op, Left: left, Right: right}
		} else if p.peek().Type == TokenBitAnd || p.peek().Type == TokenBitOr ||
			p.peek().Type == TokenLShift || p.peek().Type == TokenRShift {
			op := p.advance().Value
			right := p.parseMulExpr()
			left = &Expr{Kind: ExprBinaryOp, Op: op, Left: left, Right: right}
		} else {
			break
		}
	}
	return left
}

func (p *Parser) parseMulExpr() *Expr {
	left := p.parseUnaryExpr()
	for {
		if p.peek().Type == TokenStar || p.peek().Type == TokenSlash || p.peek().Type == TokenRem {
			op := p.advance().Value
			right := p.parseUnaryExpr()
			left = &Expr{Kind: ExprBinaryOp, Op: op, Left: left, Right: right}
		} else {
			break
		}
	}
	return left
}

func (p *Parser) parseUnaryExpr() *Expr {
	// Unary minus/plus
	if p.peek().Type == TokenMinus {
		p.advance()
		right := p.parseUnaryExpr()
		// Optimize: -integer literal
		if right.Kind == ExprLiteral && right.LiteralType == "integer" {
			return &Expr{
				Kind:        ExprLiteral,
				LiteralType: "integer",
				IntValue:    -right.IntValue,
				StringValue: fmt.Sprintf("-%d", right.IntValue),
			}
		}
		return &Expr{Kind: ExprUnaryOp, Op: "-", Right: right}
	}
	if p.peek().Type == TokenPlus {
		p.advance()
		right := p.parseUnaryExpr()
		return &Expr{Kind: ExprUnaryOp, Op: "+", Right: right}
	}
	// Bitwise NOT
	if p.peek().Type == TokenBitNot {
		p.advance()
		right := p.parseUnaryExpr()
		return &Expr{Kind: ExprUnaryOp, Op: "~", Right: right}
	}
	return p.parsePrimaryExpr()
}

func (p *Parser) parsePrimaryExpr() *Expr {
	tok := p.peek()

	// NULL
	if p.isKw(KwNull) {
		p.advance()
		return &Expr{Kind: ExprLiteral, LiteralType: "null", StringValue: "NULL"}
	}

	// TRUE / FALSE (as keywords)
	if p.isKw(KwCurrentDate) || p.isKw(KwCurrentTime) || p.isKw(KwCurrentTimestamp) {
		p.advance()
		return &Expr{Kind: ExprFunctionCall, FunctionName: tok.Value, StarArg: false}
	}

	// Integer literal
	if tok.Type == TokenInteger {
		p.advance()
		return p.parseIntegerLiteral(tok.Value)
	}

	// Float literal
	if tok.Type == TokenFloat {
		p.advance()
		return p.parseFloatLiteral(tok.Value)
	}

	// String literal
	if tok.Type == TokenString {
		p.advance()
		return &Expr{
			Kind:        ExprLiteral,
			LiteralType: "string",
			StringValue: stripQuotes(tok.Value),
		}
	}

	// Blob literal
	if tok.Type == TokenBlob {
		p.advance()
		return &Expr{
			Kind:        ExprLiteral,
			LiteralType: "blob",
			StringValue: tok.Value,
		}
	}

	// Variable
	if tok.Type == TokenVariable {
		p.advance()
		return &Expr{
			Kind:        ExprVariable,
			StringValue: tok.Value,
		}
	}

	// EXISTS (subquery)
	if p.isKw(KwExists) {
		p.advance()
		p.expectType(TokenLParen)
		sel := p.parseSelectBody()
		p.expectType(TokenRParen)
		return &Expr{Kind: ExprExists, Select: sel}
	}

	// CASE expression
	if p.isKw(KwCase) {
		return p.parseCaseExpr()
	}

	// CAST expression
	if p.isKw(KwCast) {
		return p.parseCastExpr()
	}

	// RAISE expression
	if p.isKw(KwRaise) {
		p.advance()
		p.expectType(TokenLParen)
		p.parseName() // IGNORE, ROLLBACK, ABORT, FAIL
		if p.matchType(TokenComma) {
			p.parseExpr()
		}
		p.expectType(TokenRParen)
		return &Expr{Kind: ExprFunctionCall, FunctionName: "RAISE"}
	}

	// Parenthesized expression or subquery
	if tok.Type == TokenLParen {
		p.advance()
		if p.isKw(KwSelect) || p.isKw(KwValues) || p.isKw(KwWith) {
			sel := p.parseSelectBody()
			p.expectType(TokenRParen)
			return &Expr{Kind: ExprSubquery, Select: sel}
		}

		// Could be a vector: (expr, expr, ...)
		exprs := p.parseExprList()
		if len(exprs) == 1 {
			exprs[0].Parenthesized = true
			p.expectType(TokenRParen)
			return exprs[0]
		}
		// Multi-expression vector - store as a special node
		p.expectType(TokenRParen)
		if len(exprs) > 1 {
			// Wrap in a function-like expression
			return &Expr{
				Kind: ExprFunctionCall,
				FunctionName: "__vector__",
				Args: exprs,
			}
		}
		return nil
	}

	// Identifier or qualified identifier or function call
	if tok.Type == TokenID || tok.Type == TokenKeyword {
		return p.parseIdentExpr()
	}

	p.errorf("unexpected token in expression: %q (type=%d)", tok.Value, tok.Type)
	return nil
}

func (p *Parser) parseIntegerLiteral(s string) *Expr {
	var val int64
	fmt.Sscanf(s, "%d", &val)
	return &Expr{
		Kind:        ExprLiteral,
		LiteralType: "integer",
		IntValue:    val,
		StringValue: s,
	}
}

func (p *Parser) parseFloatLiteral(s string) *Expr {
	var val float64
	fmt.Sscanf(s, "%f", &val)
	return &Expr{
		Kind:        ExprLiteral,
		LiteralType: "float",
		FloatValue:  val,
		StringValue: s,
	}
}

func (p *Parser) parseIdentExpr() *Expr {
	name := p.parseName()

	// Check for function call: name(args)
	if p.peek().Type == TokenLParen {
		p.advance()

		// COUNT(*) or other func(*)
		if p.peek().Type == TokenStar {
			p.advance()
			p.expectType(TokenRParen)
			return &Expr{
				Kind:         ExprFunctionCall,
				FunctionName: name,
				StarArg:      true,
			}
		}

		// Check for DISTINCT
		distinct := false
		if p.matchKw(KwDistinct) {
			distinct = true
		}

		// Empty args: func()
		if p.peek().Type == TokenRParen {
			p.advance()
			return &Expr{
				Kind:         ExprFunctionCall,
				FunctionName: name,
				Distinct:     distinct,
			}
		}

		args := p.parseExprList()
		p.expectType(TokenRParen)

		// OVER clause (window function) - parse but don't store details
		if p.isKw(KwOver) {
			p.advance()
			if p.peek().Type == TokenLParen {
				p.advance()
				// Skip window spec
				depth := 1
				for depth > 0 && !p.atEnd() {
					if p.peek().Type == TokenLParen {
						depth++
					} else if p.peek().Type == TokenRParen {
						depth--
						if depth == 0 {
							break
						}
					}
					p.advance()
				}
				p.expectType(TokenRParen)
			} else {
				p.parseName() // window name
			}
		}

		// FILTER clause
		if p.isKw(KwFilter) {
			p.advance()
			p.expectType(TokenLParen)
			p.expectKw(KwWhere)
			p.parseExpr()
			p.expectType(TokenRParen)
		}

		return &Expr{
			Kind:         ExprFunctionCall,
			FunctionName: name,
			Distinct:     distinct,
			Args:         args,
		}
	}

	// Check for . (qualified name)
	if p.peek().Type == TokenDot {
		p.advance()
		second := p.parseName()

		// Check for third . : db.table.column
		if p.peek().Type == TokenDot {
			p.advance()
			third := p.parseName()
			return &Expr{
				Kind:     ExprColumnRef,
				Database: name,
				Table:    second,
				Name:     third,
			}
		}

		// Check for .* (table.* in expression context)
		if p.peek().Type == TokenStar {
			p.advance()
			return &Expr{
				Kind:  ExprDot,
				Table: name,
				Name:  second,
			}
		}

		return &Expr{
			Kind:  ExprColumnRef,
			Table: name,
			Name:  second,
		}
	}

	// Simple identifier - could be a column ref
	return &Expr{
		Kind: ExprColumnRef,
		Name: name,
	}
}

// =============================================================================
// CASE expression
// =============================================================================

func (p *Parser) parseCaseExpr() *Expr {
	p.expectKw(KwCase)

	expr := &Expr{Kind: ExprCase}

	// Optional operand (simple CASE)
	if !p.isKw(KwWhen) {
		expr.Operand = p.parseExpr()
	}

	// WHEN ... THEN ... clauses
	for p.matchKw(KwWhen) {
		wc := &WhenClause{}
		wc.Condition = p.parseExpr()
		p.expectKw(KwThen)
		wc.Result = p.parseExpr()
		expr.WhenList = append(expr.WhenList, wc)
	}

	// Optional ELSE
	if p.matchKw(KwElse) {
		expr.ElseExpr = p.parseExpr()
	}

	p.expectKw(KwEnd)

	return expr
}

// =============================================================================
// CAST expression
// =============================================================================

func (p *Parser) parseCastExpr() *Expr {
	p.expectKw(KwCast)
	p.expectType(TokenLParen)
	expr := p.parseExpr()
	p.expectKw(KwAs)
	castType := p.parseTypeName()
	p.expectType(TokenRParen)

	return &Expr{
		Kind:     ExprCast,
		Left:     expr,
		CastType: castType,
	}
}

// =============================================================================
// Helper: parse list of expressions
// =============================================================================

func (p *Parser) parseExprList() []*Expr {
	var exprs []*Expr
	for {
		expr := p.parseExpr()
		if expr == nil {
			break
		}
		exprs = append(exprs, expr)
		if !p.matchType(TokenComma) {
			break
		}
	}
	return exprs
}

func (p *Parser) parseExprListInParens() []*Expr {
	p.expectType(TokenLParen)
	exprs := p.parseExprList()
	p.expectType(TokenRParen)
	return exprs
}

func (p *Parser) parseIdentList() []string {
	var names []string
	names = append(names, p.parseName())
	for p.matchType(TokenComma) {
		names = append(names, p.parseName())
	}
	return names
}
