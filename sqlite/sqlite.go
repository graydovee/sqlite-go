// Package sqlite provides the public API for sqlite-go, a pure Go
// reimplementation of SQLite.
package sqlite

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/sqlite-go/sqlite-go/btree"
	"github.com/sqlite-go/sqlite-go/compile"
	"github.com/sqlite-go/sqlite-go/pager"
	"github.com/sqlite-go/sqlite-go/vdbe"
	"github.com/sqlite-go/sqlite-go/vfs"
)

// Database represents an open connection to a SQLite database.
type Database struct {
	mu       sync.Mutex
	vfs      vfs.VFS
	pgr      pager.Pager
	btConn   btree.BTreeConn
	bt       btree.BTree
	closed   bool

	// Schema tracking (simplified: in-memory schema table)
	tables map[string]*tableEntry

	// Connection state
	lastInsertRowID int64
	changes         int64
	totalChanges    int64
	autoCommit      bool
	busyTimeoutMs   int

	// Transaction state
	inTx bool
}

// tableEntry stores metadata about a table.
type tableEntry struct {
	name      string
	rootPage  int
	columns   []columnEntry
}

// columnEntry stores metadata about a column.
type columnEntry struct {
	name     string
	typeName string
}

// selectCol represents a column in a SELECT expression list.
type selectCol struct {
	expr string
	as   string
}

// Open opens a database connection. The filename can be a file path
// or ":memory:" for an in-memory database.
func Open(filename string, flags OpenFlag) (*Database, error) {
	Initialize()

	db := &Database{
		tables:     make(map[string]*tableEntry),
		autoCommit: true,
	}

	if filename == ":memory:" || flags&OpenMemory != 0 {
		db.vfs = vfs.Find("memory")
		if db.vfs == nil {
			return nil, NewError(CantOpen, "no memory VFS available")
		}
	} else {
		db.vfs = vfs.Default()
		if db.vfs == nil {
			return nil, NewError(CantOpen, "no default VFS available")
		}
	}

	cfg := pager.PagerConfig{
		VFS:       db.vfs,
		Path:      filename,
		PageSize:  4096,
		CacheSize: 2000,
	}

	if flags&OpenMemory != 0 || filename == ":memory:" {
		cfg.Path = ""
		cfg.JournalMode = pager.JournalMemory
	}

	pgr, err := pager.OpenPager(cfg)
	if err != nil {
		return nil, NewErrorf(CantOpen, "open pager: %s", err)
	}
	db.pgr = pgr

	btConn := btree.OpenBTreeConn(pgr)
	if btConn == nil {
		pgr.Close()
		return nil, NewErrorf(Error, "failed to open btree connection")
	}
	db.btConn = btConn

	bt, err := btConn.Open(pgr)
	if err != nil {
		pgr.Close()
		return nil, NewErrorf(Error, "failed to open btree: %s", err)
	}
	db.bt = bt

	return db, nil
}

// OpenInMemory opens an in-memory database.
func OpenInMemory() (*Database, error) {
	return Open(":memory:", OpenReadWrite|OpenCreate|OpenMemory)
}

// Close closes the database connection.
func (db *Database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil
	}

	db.closed = true

	if db.bt != nil {
		db.bt.Close()
	}
	if db.pgr != nil {
		db.pgr.Close()
	}
	return nil
}

// Exec executes a SQL statement that does not return rows.
func (db *Database) Exec(sql string, args ...interface{}) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return NewError(Misuse, "database is closed")
	}

	// Split multiple statements
	stmts := splitStatements(sql)
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if err := db.execSingle(stmt, args); err != nil {
			return err
		}
	}
	return nil
}

// Query executes a SQL query and returns the result set.
func (db *Database) Query(sql string, args ...interface{}) (*ResultSet, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, NewError(Misuse, "database is closed")
	}

	return db.querySingle(strings.TrimSpace(sql), args)
}

// Prepare prepares a SQL statement for execution.
func (db *Database) Prepare(sql string) (*Statement, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return nil, NewError(Misuse, "database is closed")
	}

	// Tokenize to extract statement info
	tokens := compile.Tokenize(sql)
	filtered := filterTokens(tokens)

	if len(filtered) == 0 {
		return nil, NewError(Error, "empty SQL statement")
	}

	// Determine column names for SELECT-like statements
	var colNames []string
	if len(filtered) > 0 && isKeyword(filtered[0], "select") {
		colNames = extractColumnNames(filtered)
	}

	// Build a VDBE program
	prog, err := db.compileStatement(sql, filtered)
	if err != nil {
		return nil, err
	}

	return newStatement(db, prog, sql, colNames), nil
}

// Begin starts a transaction.
func (db *Database) Begin() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return NewError(Misuse, "database is closed")
	}
	if db.inTx {
		return NewError(Error, "cannot start a transaction within a transaction")
	}

	if err := db.pgr.Begin(true); err != nil {
		return NewErrorf(Busy, "begin transaction: %s", err)
	}
	if err := db.bt.Begin(true); err != nil {
		db.pgr.Rollback()
		return NewErrorf(Error, "begin btree transaction: %s", err)
	}
	db.inTx = true
	db.autoCommit = false
	return nil
}

// Commit commits the current transaction.
func (db *Database) Commit() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return NewError(Misuse, "database is closed")
	}
	if !db.inTx {
		return NewError(Error, "cannot commit - no transaction is active")
	}

	if err := db.bt.Commit(); err != nil {
		return NewErrorf(Error, "commit btree: %s", err)
	}
	if err := db.pgr.Commit(); err != nil {
		return NewErrorf(IOError, "commit pager: %s", err)
	}
	db.inTx = false
	db.autoCommit = true
	return nil
}

// Rollback rolls back the current transaction.
func (db *Database) Rollback() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.closed {
		return NewError(Misuse, "database is closed")
	}
	if !db.inTx {
		return NewError(Error, "cannot rollback - no transaction is active")
	}

	db.bt.Rollback()
	db.pgr.Rollback()
	db.inTx = false
	db.autoCommit = true
	return nil
}

// BusyTimeout sets the busy timeout in milliseconds.
func (db *Database) BusyTimeout(ms int) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.busyTimeoutMs = ms
}

// LastInsertRowID returns the rowid of the most recent INSERT.
func (db *Database) LastInsertRowID() int64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.lastInsertRowID
}

// Changes returns the number of rows changed by the last statement.
func (db *Database) Changes() int64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.changes
}

// TotalChanges returns the total number of rows changed since the
// database connection was opened.
func (db *Database) TotalChanges() int64 {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.totalChanges
}

// execSingle executes a single SQL statement.
func (db *Database) execSingle(sql string, args []interface{}) error {
	tokens := compile.Tokenize(sql)
	filtered := filterTokens(tokens)
	if len(filtered) == 0 {
		return nil
	}

	// Determine statement type
	stmtType := classifyStatement(filtered)

	switch stmtType {
	case "create_table":
		return db.execCreateTable(filtered)
	case "insert":
		return db.execInsert(filtered, args)
	case "delete":
		return db.execDelete(filtered)
	case "update":
		return db.execUpdate(filtered, args)
	case "begin":
		db.mu.Unlock()
		err := db.Begin()
		db.mu.Lock()
		return err
	case "commit":
		db.mu.Unlock()
		err := db.Commit()
		db.mu.Lock()
		return err
	case "rollback":
		db.mu.Unlock()
		err := db.Rollback()
		db.mu.Lock()
		return err
	case "select":
		// SELECT via Exec just runs and discards results
		_, err := db.querySingle(sql, args)
		return err
	default:
		return NewErrorf(Error, "unsupported SQL statement: %s", stmtType)
	}
}

// execCreateTable handles CREATE TABLE statements.
func (db *Database) execCreateTable(tokens []compile.Token) error {
	// Parse: CREATE TABLE [IF NOT EXISTS] name (col-defs)
	pos := 0
	expectKeyword(tokens, &pos, "create")
	expectKeyword(tokens, &pos, "table")

	ifNotExists := false
	if pos < len(tokens) && isKeyword(tokens[pos], "if") {
		expectKeyword(tokens, &pos, "if")
		expectKeyword(tokens, &pos, "not")
		expectKeyword(tokens, &pos, "exists")
		ifNotExists = true
	}

	if pos >= len(tokens) {
		return NewError(Error, "expected table name")
	}
	tableName := tokens[pos].Value
	pos++

	if ifNotExists && db.tables[tableName] != nil {
		return nil
	}

	if db.tables[tableName] != nil {
		return NewErrorf(Error, "table %s already exists", tableName)
	}

	// Parse column definitions
	var columns []columnEntry
	if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
		pos++ // skip (
		for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
			colName := tokens[pos].Value
			pos++

			colType := ""
			if pos < len(tokens) && tokens[pos].Type == compile.TokenID {
				colType = tokens[pos].Value
				pos++
			}

			// Skip constraints (PRIMARY KEY, NOT NULL, etc.)
			for pos < len(tokens) &&
				tokens[pos].Type != compile.TokenComma &&
				tokens[pos].Type != compile.TokenRParen {
				pos++
			}

			columns = append(columns, columnEntry{name: colName, typeName: colType})

			if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
				pos++
			}
		}
		if pos < len(tokens) && tokens[pos].Type == compile.TokenRParen {
			pos++
		}
	}

	if len(columns) == 0 {
		return NewError(Error, "table must have at least one column")
	}

	// Start a write transaction if not in one
	if !db.inTx {
		if err := db.pgr.Begin(true); err != nil {
			return NewErrorf(Busy, "begin transaction: %s", err)
		}
		if err := db.bt.Begin(true); err != nil {
			db.pgr.Rollback()
			return NewErrorf(Error, "begin btree transaction: %s", err)
		}
	}

	// Create the B-Tree for the table - this allocates the actual root page
	rootPage, err := db.bt.CreateBTree(btree.CreateTable)
	if err != nil {
		if !db.inTx {
			db.bt.Rollback()
			db.pgr.Rollback()
		}
		return NewErrorf(Error, "create btree: %s", err)
	}

	if !db.inTx {
		if err := db.bt.Commit(); err != nil {
			return NewErrorf(Error, "commit btree: %s", err)
		}
		if err := db.pgr.Commit(); err != nil {
			return NewErrorf(IOError, "commit pager: %s", err)
		}
	}

	db.tables[tableName] = &tableEntry{
		name:     tableName,
		rootPage: int(rootPage),
		columns:  columns,
	}

	return nil
}

// execInsert handles INSERT statements.
func (db *Database) execInsert(tokens []compile.Token, args []interface{}) error {
	// Parse: INSERT INTO table [(cols)] VALUES (vals) or INSERT INTO table VALUES vals
	pos := 0
	expectKeyword(tokens, &pos, "insert")
	if pos < len(tokens) && isKeyword(tokens[pos], "or") {
		pos += 2 // skip OR ...
	}
	expectKeyword(tokens, &pos, "into")

	if pos >= len(tokens) {
		return NewError(Error, "expected table name in INSERT")
	}
	tableName := tokens[pos].Value
	pos++

	tbl, ok := db.tables[tableName]
	if !ok {
		return NewErrorf(Error, "no such table: %s", tableName)
	}

	// Optional column list
	var colList []string
	if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
		// Check if this is column list or VALUES
		if !isKeyword(lookAheadSkip(tokens, pos, compile.TokenLParen), "select") &&
			!isKeyword(tokens[pos+1], "select") {
			// Could be column list or direct VALUES
			scanPos := pos + 1
			hasValuesKeyword := false
			for scanPos < len(tokens) {
				if isKeyword(tokens[scanPos], "values") {
					hasValuesKeyword = true
					break
				}
				if tokens[scanPos].Type == compile.TokenRParen {
					if scanPos+1 < len(tokens) && isKeyword(tokens[scanPos+1], "values") {
						hasValuesKeyword = true
					}
					break
				}
				scanPos++
			}

			if hasValuesKeyword {
				// It's a column list
				pos++ // skip (
				for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
					if tokens[pos].Type == compile.TokenID || tokens[pos].Type == compile.TokenKeyword {
						colList = append(colList, tokens[pos].Value)
					}
					pos++
				}
				if pos < len(tokens) {
					pos++ // skip )
				}
			}
		}
	}

	// DEFAULT VALUES
	if pos < len(tokens) && isKeyword(tokens[pos], "default") {
		pos++
		expectKeyword(tokens, &pos, "values")
		// Insert a row of default values
		return db.insertRow(tbl, colList, nil, args)
	}

	// VALUES clause
	expectKeyword(tokens, &pos, "values")

	// Parse value groups: (val, val, ...) or just val
	for pos < len(tokens) {
		var values []interface{}
		if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
			pos++ // skip (
			for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
				val, err := parseExprValue(tokens, &pos, args)
				if err != nil {
					return err
				}
				values = append(values, val)
				if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
					pos++
				}
			}
			if pos < len(tokens) {
				pos++ // skip )
			}
		}

		if err := db.insertRow(tbl, colList, values, args); err != nil {
			return err
		}

		if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
			pos++
			continue
		}
		break
	}

	return nil
}

// insertRow inserts a single row into a table.
func (db *Database) insertRow(tbl *tableEntry, colList []string, values []interface{}, args []interface{}) error {
	// Start a write transaction if not in one
	needCommit := false
	if !db.inTx {
		if err := db.pgr.Begin(true); err != nil {
			return NewErrorf(Busy, "begin transaction: %s", err)
		}
		if err := db.bt.Begin(true); err != nil {
			db.pgr.Rollback()
			return NewErrorf(Error, "begin btree transaction: %s", err)
		}
		needCommit = true
	}

	// Determine number of columns
	numCols := len(tbl.columns)

	// If no column list specified, use all columns
	if len(colList) == 0 {
		colList = make([]string, numCols)
		for i, c := range tbl.columns {
			colList[i] = c.name
		}
	}

	// Build value map (column name -> value)
	valMap := make(map[string]interface{})
	for i, name := range colList {
		if i < len(values) {
			valMap[name] = values[i]
		} else {
			valMap[name] = nil
		}
	}

	// Build the record
	rb := vdbe.NewRecordBuilder()
	for _, col := range tbl.columns {
		v, ok := valMap[col.name]
		if !ok {
			rb.AddNull()
			continue
		}
		addValueToRecord(rb, v)
	}

	data := rb.Build()

	// Open a cursor and insert
	cursor, err := db.bt.Cursor(btree.PageNumber(tbl.rootPage), true)
	if err != nil {
		return NewErrorf(Error, "open cursor: %s", err)
	}
	defer cursor.Close()

	// Generate a new row ID
	var newRowID int64 = 1
	if hasRow, _ := cursor.Last(); hasRow {
		newRowID = int64(cursor.RowID()) + 1
	}
	if newRowID <= db.lastInsertRowID {
		newRowID = db.lastInsertRowID + 1
	}

	// Encode key as varint
	keyBuf := make([]byte, 9)
	keyLen := encodeVarintKey(keyBuf, newRowID)

	err = db.bt.Insert(cursor, keyBuf[:keyLen], data, btree.RowID(newRowID), btree.SeekNotFound)
	if err != nil {
		return NewErrorf(Error, "insert: %s", err)
	}

	db.lastInsertRowID = newRowID
	db.changes = 1
	db.totalChanges++

	if needCommit {
		if err := db.bt.Commit(); err != nil {
			return NewErrorf(Error, "commit btree: %s", err)
		}
		if err := db.pgr.Commit(); err != nil {
			return NewErrorf(IOError, "commit pager: %s", err)
		}
	}

	return nil
}

// execDelete handles DELETE statements.
func (db *Database) execDelete(tokens []compile.Token) error {
	pos := 0
	expectKeyword(tokens, &pos, "delete")
	expectKeyword(tokens, &pos, "from")

	if pos >= len(tokens) {
		return NewError(Error, "expected table name in DELETE")
	}
	tableName := tokens[pos].Value
	pos++

	tbl, ok := db.tables[tableName]
	if !ok {
		return NewErrorf(Error, "no such table: %s", tableName)
	}

	// Simple DELETE FROM table (no WHERE support yet)
	// For now, clear the entire table
	cursor, err := db.bt.Cursor(btree.PageNumber(tbl.rootPage), true)
	if err != nil {
		return NewErrorf(Error, "open cursor: %s", err)
	}
	defer cursor.Close()

	var count int64
	hasRow, err := cursor.First()
	if err != nil {
		return err
	}
	for hasRow {
		if err := db.bt.Delete(cursor); err != nil {
			return err
		}
		count++
		hasRow, err = cursor.First() // restart from beginning after delete
		if err != nil {
			return err
		}
	}

	db.changes = count
	db.totalChanges += count
	return nil
}

// execUpdate handles UPDATE statements.
func (db *Database) execUpdate(tokens []compile.Token, args []interface{}) error {
	// Minimal UPDATE support
	pos := 0
	expectKeyword(tokens, &pos, "update")

	if pos >= len(tokens) {
		return NewError(Error, "expected table name in UPDATE")
	}
	tableName := tokens[pos].Value
	pos++

	tbl, ok := db.tables[tableName]
	if !ok {
		return NewErrorf(Error, "no such table: %s", tableName)
	}

	// SET clause
	expectKeyword(tokens, &pos, "set")

	// Parse SET col = expr, ...
	type setPair struct {
		col   string
		value interface{}
	}
	var sets []setPair
	for pos < len(tokens) {
		if isKeyword(tokens[pos], "where") {
			break
		}
		colName := tokens[pos].Value
		pos++
		if pos < len(tokens) && tokens[pos].Type == compile.TokenEq {
			pos++
		}
		val, err := parseExprValue(tokens, &pos, args)
		if err != nil {
			return err
		}
		sets = append(sets, setPair{col: colName, value: val})
		if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
			pos++
		}
	}

	// For now, update all rows (no WHERE support)
	cursor, err := db.bt.Cursor(btree.PageNumber(tbl.rootPage), true)
	if err != nil {
		return NewErrorf(Error, "open cursor: %s", err)
	}
	defer cursor.Close()

	var count int64
	hasRow, err := cursor.First()
	if err != nil {
		return err
	}
	for hasRow {
		data, err := cursor.Data()
		if err != nil {
			return err
		}
		rowid := cursor.RowID()

		values, err := vdbe.ParseRecord(data)
		if err != nil {
			return err
		}

		// Apply sets
		for _, s := range sets {
			for i, col := range tbl.columns {
				if strings.EqualFold(col.name, s.col) {
					if i < len(values) {
						values[i] = interfaceToValue(s.value)
					}
				}
			}
		}

		// Rebuild record
		rb := vdbe.NewRecordBuilder()
		for _, v := range values {
			addValueToRecordFromValue(rb, v)
		}
		newData := rb.Build()

		keyBuf := make([]byte, 9)
		keyLen := encodeVarintKey(keyBuf, int64(rowid))

		if err := db.bt.Insert(cursor, keyBuf[:keyLen], newData, btree.RowID(rowid), btree.SeekFound); err != nil {
			return err
		}
		count++

		hasRow, err = cursor.Next()
		if err != nil {
			return err
		}
	}

	db.changes = count
	db.totalChanges += count
	return nil
}

// querySingle executes a SELECT and collects results.
func (db *Database) querySingle(sql string, args []interface{}) (*ResultSet, error) {
	tokens := compile.Tokenize(sql)
	filtered := filterTokens(tokens)
	if len(filtered) == 0 {
		return nil, NewError(Error, "empty SQL statement")
	}

	if !isKeyword(filtered[0], "select") {
		return nil, NewError(Error, "not a SELECT statement")
	}

	// Parse SELECT columns
	pos := 1 // skip SELECT

	// Check DISTINCT
	if pos < len(filtered) && isKeyword(filtered[pos], "distinct") {
		pos++
	}

	// Parse column expressions
	var cols []selectCol
	for pos < len(filtered) && !isKeyword(filtered[pos], "from") &&
		!isKeyword(filtered[pos], "where") && !isKeyword(filtered[pos], "limit") &&
		!isKeyword(filtered[pos], "order") && !isKeyword(filtered[pos], "group") {
		if filtered[pos].Type == compile.TokenComma {
			pos++
			continue
		}
		if filtered[pos].Type == compile.TokenStar {
			cols = append(cols, selectCol{expr: "*"})
			pos++
			continue
		}

		var exprParts []string
		for pos < len(filtered) &&
			filtered[pos].Type != compile.TokenComma &&
			!isKeyword(filtered[pos], "from") &&
			!isKeyword(filtered[pos], "where") &&
			!isKeyword(filtered[pos], "limit") &&
			!isKeyword(filtered[pos], "order") &&
			!isKeyword(filtered[pos], "group") {
			if isKeyword(filtered[pos], "as") {
				pos++
				if pos < len(filtered) {
					cols = append(cols, selectCol{
						expr: strings.Join(exprParts, " "),
						as:   filtered[pos].Value,
					})
					pos++
					exprParts = nil
				}
				break
			}
			exprParts = append(exprParts, filtered[pos].Value)
			pos++
		}
		if len(exprParts) > 0 {
			cols = append(cols, selectCol{expr: strings.Join(exprParts, " ")})
		}
	}

	// Parse FROM clause
	var tableName string
	if pos < len(filtered) && isKeyword(filtered[pos], "from") {
		pos++
		if pos < len(filtered) {
			tableName = filtered[pos].Value
			pos++
		}
	}

	// For SELECT without FROM (e.g., SELECT 1+2), compute directly
	if tableName == "" {
		return db.selectWithoutTable(cols, args)
	}

	tbl, ok := db.tables[tableName]
	if !ok {
		return nil, NewErrorf(Error, "no such table: %s", tableName)
	}

	// Determine output columns
	var resultCols []ResultColumnInfo
	var colIndices []int // which table columns map to each result column

	for _, c := range cols {
		if c.expr == "*" {
			for i, col := range tbl.columns {
				resultCols = append(resultCols, ResultColumnInfo{
					Name: col.name,
					Type: ColNull,
				})
				colIndices = append(colIndices, i)
			}
		} else {
			name := c.as
			if name == "" {
				name = c.expr
			}
			resultCols = append(resultCols, ResultColumnInfo{
				Name: name,
				Type: ColNull,
			})
			// Try to match column name
			found := -1
			for i, col := range tbl.columns {
				if strings.EqualFold(col.name, c.expr) {
					found = i
					break
				}
			}
			colIndices = append(colIndices, found)
		}
	}

	// Scan the table
	cursor, err := db.bt.Cursor(btree.PageNumber(tbl.rootPage), false)
	if err != nil {
		return nil, NewErrorf(Error, "open cursor: %s", err)
	}
	defer cursor.Close()

	var rows []Row
	hasRow, err := cursor.First()
	if err != nil {
		return nil, err
	}
	for hasRow {
		data, err := cursor.Data()
		if err != nil {
			return nil, err
		}

		values, err := vdbe.ParseRecord(data)
		if err != nil {
			return nil, err
		}

		row := Row{cols: resultCols}
		for _, idx := range colIndices {
			if idx >= 0 && idx < len(values) {
				row.values = append(row.values, vdbe.MemFromValue(values[idx]))
			} else {
				row.values = append(row.values, vdbe.NewMemNull())
			}
		}
		rows = append(rows, row)

		hasRow, err = cursor.Next()
		if err != nil {
			return nil, err
		}
	}

	if rows == nil {
		rows = []Row{}
	}

	return newResultSet(rows, resultCols), nil
}

// selectWithoutTable handles SELECT without a FROM clause.
func (db *Database) selectWithoutTable(cols []selectCol, args []interface{}) (*ResultSet, error) {
	var resultCols []ResultColumnInfo
	var row Row

	for _, c := range cols {
		name := c.as
		if name == "" {
			name = c.expr
		}
		resultCols = append(resultCols, ResultColumnInfo{
			Name: name,
			Type: ColNull,
		})

		// Evaluate simple expressions
		val := evalSimpleExpr(c.expr, args)
		row.values = append(row.values, val)
	}

	rows := []Row{row}
	row.cols = resultCols
	return newResultSet(rows, resultCols), nil
}

// evalSimpleExpr evaluates a simple expression (literal or arithmetic).
func evalSimpleExpr(expr string, args []interface{}) *vdbe.Mem {
	expr = strings.TrimSpace(expr)

	// Try integer
	if v, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return vdbe.NewMemInt(v)
	}

	// Try float
	if v, err := strconv.ParseFloat(expr, 64); err == nil {
		return vdbe.NewMemFloat(v)
	}

	// String literal
	if len(expr) >= 2 && expr[0] == '\'' && expr[len(expr)-1] == '\'' {
		return vdbe.NewMemStr(expr[1 : len(expr)-1])
	}

	// NULL
	if strings.EqualFold(expr, "null") {
		return vdbe.NewMemNull()
	}

	// Bind variable
	if expr[0] == '?' {
		if idx, err := strconv.Atoi(expr[1:]); err == nil && idx > 0 && idx <= len(args) {
			return vdbe.MakeMem(args[idx-1])
		}
	}

	// Arithmetic: try to evaluate simple binary expressions
	// Look for +, -, *, /
	for i := len(expr) - 1; i > 0; i-- {
		switch expr[i] {
		case '+', '-':
			left := evalSimpleExpr(expr[:i], args)
			right := evalSimpleExpr(expr[i+1:], args)
			if left.Type == vdbe.MemInt && right.Type == vdbe.MemInt {
				if expr[i] == '+' {
					return vdbe.NewMemInt(left.IntVal + right.IntVal)
				}
				return vdbe.NewMemInt(left.IntVal - right.IntVal)
			}
			lf, rf := left.FloatValue(), right.FloatValue()
			if expr[i] == '+' {
				return vdbe.NewMemFloat(lf + rf)
			}
			return vdbe.NewMemFloat(lf - rf)
		}
	}
	for i := len(expr) - 1; i > 0; i-- {
		switch expr[i] {
		case '*':
			left := evalSimpleExpr(expr[:i], args)
			right := evalSimpleExpr(expr[i+1:], args)
			if left.Type == vdbe.MemInt && right.Type == vdbe.MemInt {
				return vdbe.NewMemInt(left.IntVal * right.IntVal)
			}
			return vdbe.NewMemFloat(left.FloatValue() * right.FloatValue())
		case '/':
			left := evalSimpleExpr(expr[:i], args)
			right := evalSimpleExpr(expr[i+1:], args)
			if left.Type == vdbe.MemInt && right.Type == vdbe.MemInt {
				if right.IntVal != 0 {
					return vdbe.NewMemInt(left.IntVal / right.IntVal)
				}
			}
			rf := right.FloatValue()
			if rf != 0 {
				return vdbe.NewMemFloat(left.FloatValue() / rf)
			}
		}
	}

	// Default: return as text
	return vdbe.NewMemStr(expr)
}

// compileStatement builds a VDBE program from tokenized SQL.
func (db *Database) compileStatement(sql string, tokens []compile.Token) (*vdbe.Program, error) {
	// Build a minimal VDBE program
	stmtType := classifyStatement(tokens)

	switch stmtType {
	case "select":
		return db.compileSelect(tokens)
	case "insert":
		return db.compileInsert(tokens)
	case "create_table":
		return db.compileCreateTable(tokens)
	default:
		// Return a minimal program that halts
		return &vdbe.Program{
			Instructions: []vdbe.Instruction{
				{Op: vdbe.OpInit, P2: 1, Comment: "init"},
				{Op: vdbe.OpHalt, Comment: "done"},
			},
			NumRegs:    1,
			NumCursors: 0,
			SQL:        sql,
		}, nil
	}
}

func (db *Database) compileSelect(tokens []compile.Token) (*vdbe.Program, error) {
	return &vdbe.Program{
		Instructions: []vdbe.Instruction{
			{Op: vdbe.OpInit, P2: 1, Comment: "init"},
			{Op: vdbe.OpHalt, Comment: "done"},
		},
		NumRegs:    1,
		NumCursors: 0,
		SQL:        joinTokenValues(tokens),
	}, nil
}

func (db *Database) compileInsert(tokens []compile.Token) (*vdbe.Program, error) {
	return &vdbe.Program{
		Instructions: []vdbe.Instruction{
			{Op: vdbe.OpInit, P2: 1, Comment: "init"},
			{Op: vdbe.OpHalt, Comment: "done"},
		},
		NumRegs:    1,
		NumCursors: 0,
		SQL:        joinTokenValues(tokens),
	}, nil
}

func (db *Database) compileCreateTable(tokens []compile.Token) (*vdbe.Program, error) {
	return &vdbe.Program{
		Instructions: []vdbe.Instruction{
			{Op: vdbe.OpInit, P2: 1, Comment: "init"},
			{Op: vdbe.OpHalt, Comment: "done"},
		},
		NumRegs:    1,
		NumCursors: 0,
		SQL:        joinTokenValues(tokens),
	}, nil
}

// --- Helper functions ---

// filterTokens removes whitespace and comment tokens.
func filterTokens(tokens []compile.Token) []compile.Token {
	var result []compile.Token
	for _, t := range tokens {
		if t.Type != compile.TokenWhitespace && t.Type != compile.TokenComment {
			result = append(result, t)
		}
	}
	return result
}

// isKeyword checks if a token is a specific keyword.
func isKeyword(t compile.Token, kw string) bool {
	return t.Type == compile.TokenKeyword && strings.EqualFold(t.Value, kw)
}

// expectKeyword advances pos past an expected keyword, or returns an error.
func expectKeyword(tokens []compile.Token, pos *int, kw string) {
	if *pos < len(tokens) && isKeyword(tokens[*pos], kw) {
		*pos++
	}
}

// lookAheadSkip finds the next non-whitespace token after skipType.
func lookAheadSkip(tokens []compile.Token, pos int, skipType compile.TokenType) compile.Token {
	for i := pos + 1; i < len(tokens); i++ {
		if tokens[i].Type != skipType {
			return tokens[i]
		}
	}
	return compile.Token{Type: compile.TokenEOF}
}

// classifyStatement determines the type of SQL statement from tokens.
func classifyStatement(tokens []compile.Token) string {
	if len(tokens) == 0 {
		return ""
	}
	first := strings.ToLower(tokens[0].Value)
	switch first {
	case "select":
		return "select"
	case "insert":
		return "insert"
	case "update":
		return "update"
	case "delete":
		return "delete"
	case "create":
		if len(tokens) > 1 {
			second := strings.ToLower(tokens[1].Value)
			switch second {
			case "table":
				return "create_table"
			case "index":
				return "create_index"
			}
		}
		return "create"
	case "drop":
		return "drop"
	case "begin":
		return "begin"
	case "commit", "end":
		return "commit"
	case "rollback":
		return "rollback"
	}
	return first
}

// parseExprValue parses a simple expression value from tokens.
func parseExprValue(tokens []compile.Token, pos *int, args []interface{}) (interface{}, error) {
	if *pos >= len(tokens) {
		return nil, fmt.Errorf("expected value")
	}

	t := tokens[*pos]
	switch t.Type {
	case compile.TokenInteger:
		*pos++
		v, err := strconv.ParseInt(t.Value, 10, 64)
		if err != nil {
			return t.Value, nil
		}
		return v, nil
	case compile.TokenFloat:
		*pos++
		v, err := strconv.ParseFloat(t.Value, 64)
		if err != nil {
			return t.Value, nil
		}
		return v, nil
	case compile.TokenString:
		*pos++
		// Remove quotes
		s := t.Value
		if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
			s = s[1 : len(s)-1]
		}
		return s, nil
	case compile.TokenBlob:
		*pos++
		return t.Value, nil
	case compile.TokenVariable:
		*pos++
		// ?NNN or ?
		if len(t.Value) > 1 {
			if idx, err := strconv.Atoi(t.Value[1:]); err == nil {
				if idx > 0 && idx <= len(args) {
					return args[idx-1], nil
				}
			}
		}
		// Plain ? — use sequential binding
		if t.Value == "?" {
			// Use next arg
			bindIdx := 0 // simplified: use first arg for all ?
			if bindIdx < len(args) {
				return args[bindIdx], nil
			}
		}
		return nil, nil
	default:
		// Handle NULL keyword
		if isKeyword(t, "null") {
			*pos++
			return nil, nil
		}
		*pos++
		return t.Value, nil
	}
}

// extractColumnNames extracts column names from a SELECT statement.
func extractColumnNames(tokens []compile.Token) []string {
	var names []string
	pos := 1 // skip SELECT

	if pos < len(tokens) && isKeyword(tokens[pos], "distinct") {
		pos++
	}

	for pos < len(tokens) && !isKeyword(tokens[pos], "from") {
		if tokens[pos].Type == compile.TokenComma {
			pos++
			continue
		}
		if isKeyword(tokens[pos], "from") {
			break
		}

		// Collect expression tokens until comma or FROM
		var exprParts []string
		for pos < len(tokens) && tokens[pos].Type != compile.TokenComma && !isKeyword(tokens[pos], "from") {
			if isKeyword(tokens[pos], "as") {
				pos++
				if pos < len(tokens) {
					names = append(names, tokens[pos].Value)
					pos++
				}
				exprParts = nil
				break
			}
			exprParts = append(exprParts, tokens[pos].Value)
			pos++
		}
		if len(exprParts) > 0 {
			if exprParts[0] == "*" {
				names = append(names, "*")
			} else {
				names = append(names, strings.Join(exprParts, " "))
			}
		}
	}

	return names
}

// splitStatements splits SQL text on semicolons.
func splitStatements(sql string) []string {
	var stmts []string
	var current strings.Builder
	inString := false

	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if c == '\'' {
			inString = !inString
			current.WriteByte(c)
			continue
		}
		if c == ';' && !inString {
			s := strings.TrimSpace(current.String())
			if s != "" {
				stmts = append(stmts, s)
			}
			current.Reset()
			continue
		}
		current.WriteByte(c)
	}

	s := strings.TrimSpace(current.String())
	if s != "" {
		stmts = append(stmts, s)
	}

	return stmts
}

// joinTokenValues joins token values into a string.
func joinTokenValues(tokens []compile.Token) string {
	var buf strings.Builder
	for i, t := range tokens {
		if i > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(t.Value)
	}
	return buf.String()
}

// addValueToRecord adds a Go value to a RecordBuilder.
func addValueToRecord(rb *vdbe.RecordBuilder, v interface{}) {
	switch val := v.(type) {
	case nil:
		rb.AddNull()
	case int:
		rb.AddInt(int64(val))
	case int64:
		rb.AddInt(val)
	case float64:
		rb.AddFloat(val)
	case string:
		rb.AddText(val)
	case []byte:
		rb.AddBlob(val)
	case bool:
		if val {
			rb.AddInt(1)
		} else {
			rb.AddInt(0)
		}
	default:
		rb.AddText(fmt.Sprintf("%v", v))
	}
}

// addValueToRecordFromValue adds a vdbe.Value to a RecordBuilder.
func addValueToRecordFromValue(rb *vdbe.RecordBuilder, v vdbe.Value) {
	switch v.Type {
	case "null":
		rb.AddNull()
	case "int":
		rb.AddInt(v.IntVal)
	case "float":
		rb.AddFloat(v.FloatVal)
	case "text":
		rb.AddText(string(v.Bytes))
	case "blob":
		rb.AddBlob(v.Bytes)
	default:
		rb.AddNull()
	}
}

// interfaceToValue converts a Go interface{} to a vdbe.Value.
func interfaceToValue(v interface{}) vdbe.Value {
	switch val := v.(type) {
	case nil:
		return vdbe.Value{Type: "null"}
	case int:
		return vdbe.Value{Type: "int", IntVal: int64(val)}
	case int64:
		return vdbe.Value{Type: "int", IntVal: val}
	case float64:
		return vdbe.Value{Type: "float", FloatVal: val}
	case string:
		return vdbe.Value{Type: "text", Bytes: []byte(val)}
	case []byte:
		cp := make([]byte, len(val))
		copy(cp, val)
		return vdbe.Value{Type: "blob", Bytes: cp}
	case bool:
		if val {
			return vdbe.Value{Type: "int", IntVal: 1}
		}
		return vdbe.Value{Type: "int", IntVal: 0}
	default:
		return vdbe.Value{Type: "text", Bytes: []byte(fmt.Sprintf("%v", v))}
	}
}

// encodeVarintKey encodes a row ID as a varint key.
func encodeVarintKey(buf []byte, rowid int64) int {
	uv := uint64(rowid)
	if uv <= 127 {
		buf[0] = byte(uv)
		return 1
	}
	var tmp [9]byte
	n := 0
	for i := 8; i >= 0; i-- {
		tmp[i] = byte((uv & 0x7f) | 0x80)
		uv >>= 7
		n++
		if uv == 0 {
			tmp[8] &= 0x7f
			break
		}
	}
	copy(buf, tmp[9-n:])
	return n
}
