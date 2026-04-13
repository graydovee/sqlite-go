// Package sqlite provides the public API for sqlite-go, a pure Go
// reimplementation of SQLite.
package sqlite

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

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
	tables      map[string]*tableEntry
	rootPageMap map[int]*tableEntry
	views       map[string]*viewEntry

	// sqlite_master entries — the system catalog
	masterEntries []sqliteMasterEntry

	// Column defaults for ALTER TABLE ADD COLUMN
	columnDefaults map[string]interface{}

	// Connection state
	lastInsertRowID int64
	changes         int64
	totalChanges    int64
	autoCommit      bool
	busyTimeoutMs   int

	// Transaction state
	inTx bool

	// Foreign key enforcement
	foreignKeys bool

	// CTE temporary data (populated during WITH query execution)
	cteData map[string]*cteTable
}

// sqliteMasterEntry represents one row in the sqlite_master system table.
// Columns: type TEXT, name TEXT, tbl_name TEXT, rootpage INTEGER, sql TEXT
type sqliteMasterEntry struct {
	Type     string // "table", "index", "view", "trigger"
	Name     string
	TblName  string
	RootPage int
	SQL      string
}

// tableEntry stores metadata about a table.
// fkInfo stores metadata about a single foreign key constraint.
type fkInfo struct {
	id         int
	columns    []string
	refTable   string
	refColumns []string
	onDelete   string
	onUpdate   string
	deferred   bool // DEFERRABLE INITIALLY DEFERRED
}

type tableEntry struct {
	name      string
	rootPage  int
	columns   []columnEntry
	fks       []fkInfo
}

// viewEntry stores metadata about a view.
type viewEntry struct {
	name string
	sql  string // The SELECT statement defining the view
}

// columnEntry stores metadata about a column.
type columnEntry struct {
	name         string
	typeName     string
	defaultValue interface{}
	isPK         bool
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
		tables:         make(map[string]*tableEntry),
		rootPageMap:    make(map[int]*tableEntry),
		views:          make(map[string]*viewEntry),
		columnDefaults: make(map[string]interface{}),
		autoCommit:     true,
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

	// Check all deferred FK constraints before committing
	if err := db.fkCheckDeferredConstraints(); err != nil {
		db.bt.Rollback()
		db.pgr.Rollback()
		db.inTx = false
		db.autoCommit = true
		return err
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
	case "create_view":
		return db.execCreateView(filtered)
	case "create_index":
		return db.execCreateIndex(filtered)
	case "drop_view":
		return db.execDropView(filtered)
	case "drop_table":
		return db.execDropTable(filtered)
	case "drop_index":
		return db.execDropIndex(filtered)
	case "alter":
		return db.execAlterTable(filtered, sql)
	case "insert":
		return db.execInsert(filtered, args)
	case "delete":
		return db.execDelete(filtered, args)
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
	case "pragma":
		return db.execPragma(filtered)
	default:
		return NewErrorf(Error, "unsupported SQL statement: %s", stmtType)
	}
}

// execCreateTable handles CREATE TABLE statements.
func (db *Database) execCreateTable(tokens []compile.Token) error {
	// Reconstruct the original SQL for sqlite_master
	sqlText := rebuildSQL(tokens)

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
	// Track which columns have UNIQUE or PRIMARY KEY for auto-index creation
	var uniqueCols []string
	var tableFKs []fkInfo
	if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
		pos++ // skip (
		for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
			// Check for table-level FOREIGN KEY constraint
			if isKeyword(tokens[pos], "foreign") && pos+1 < len(tokens) && isKeyword(tokens[pos+1], "key") {
				pos += 2
				fk := fkInfo{id: len(tableFKs)}
				// Local columns
				if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
					pos++
					for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
						if tokens[pos].Type == compile.TokenID || tokens[pos].Type == compile.TokenKeyword {
							fk.columns = append(fk.columns, tokens[pos].Value)
						}
						pos++
					}
					if pos < len(tokens) {
						pos++ // skip )
					}
				}
				// REFERENCES
				if pos < len(tokens) && isKeyword(tokens[pos], "references") {
					pos++
					if pos < len(tokens) {
						fk.refTable = tokens[pos].Value
						pos++
					}
					if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
						pos++
						for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
							if tokens[pos].Type == compile.TokenID || tokens[pos].Type == compile.TokenKeyword {
								fk.refColumns = append(fk.refColumns, tokens[pos].Value)
							}
							pos++
						}
						if pos < len(tokens) {
							pos++ // skip )
						}
					}
				}
				// ON DELETE / ON UPDATE
				for pos < len(tokens) &&
					tokens[pos].Type != compile.TokenComma &&
					tokens[pos].Type != compile.TokenRParen {
					if isKeyword(tokens[pos], "on") && pos+2 < len(tokens) {
						action := strings.ToUpper(tokens[pos+2].Value)
						target := &fk.onDelete
						if isKeyword(tokens[pos+1], "update") {
							target = &fk.onUpdate
						}
						*target = action
						pos += 3
						// Handle multi-word actions: SET NULL, SET DEFAULT, NO ACTION
						if pos < len(tokens) && (*target == "SET" || *target == "NO") {
							*target += " " + strings.ToUpper(tokens[pos].Value)
							pos++
						}
						continue
					}
					pos++
				}
				if fk.onDelete == "" {
					fk.onDelete = "NO ACTION"
				}
				if fk.onUpdate == "" {
					fk.onUpdate = "NO ACTION"
				}
				tableFKs = append(tableFKs, fk)
				if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
					pos++
				}
				continue
			}

				// Check for table-level constraints
				if isKeyword(tokens[pos], "primary") && pos+1 < len(tokens) && isKeyword(tokens[pos+1], "key") {
					// Parse table-level PRIMARY KEY (col, col, ...)
					pos += 2 // skip PRIMARY KEY
					if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
						pos++ // skip (
						for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
							if tokens[pos].Type == compile.TokenID || tokens[pos].Type == compile.TokenKeyword {
								pkColName := tokens[pos].Value
								// Mark the column as PK
								for i := range columns {
									if strings.EqualFold(columns[i].name, pkColName) {
										columns[i].isPK = true
										break
									}
								}
							}
							pos++
						}
						if pos < len(tokens) {
							pos++ // skip )
						}
					}
					if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
						pos++
					}
					continue
				} else if isKeyword(tokens[pos], "unique") ||
					isKeyword(tokens[pos], "check") || isKeyword(tokens[pos], "constraint") {
					// Skip to next comma or close paren
					depth := 0
					for pos < len(tokens) {
						if tokens[pos].Type == compile.TokenLParen {
							depth++
						} else if tokens[pos].Type == compile.TokenRParen {
							if depth == 0 {
								break
							}
							depth--
						} else if tokens[pos].Type == compile.TokenComma && depth == 0 {
							pos++
							break
						}
						pos++
					}
					continue
				}

			colName := tokens[pos].Value
			pos++

			colType := ""
			if pos < len(tokens) && tokens[pos].Type == compile.TokenID {
				colType = tokens[pos].Value
				pos++
			}

			// Parse constraints (PRIMARY KEY, NOT NULL, UNIQUE, REFERENCES, DEFAULT, etc.)
			colHasUnique := false
			colHasPK := false
			var colDefault interface{}
			var colFKs []fkInfo
			for pos < len(tokens) &&
				tokens[pos].Type != compile.TokenComma &&
				tokens[pos].Type != compile.TokenRParen {
				if isKeyword(tokens[pos], "unique") {
					colHasUnique = true
					pos++
				} else if isKeyword(tokens[pos], "primary") {
					colHasPK = true
					pos++
				} else if isKeyword(tokens[pos], "references") {
					pos++
					// Column-level REFERENCES
					fk := fkInfo{
						id:      len(tableFKs) + len(colFKs),
						columns: []string{colName},
					}
					if pos < len(tokens) {
						fk.refTable = tokens[pos].Value
						pos++
					}
					if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
						pos++
						for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
							if tokens[pos].Type == compile.TokenID || tokens[pos].Type == compile.TokenKeyword {
								fk.refColumns = append(fk.refColumns, tokens[pos].Value)
							}
							pos++
						}
						if pos < len(tokens) {
							pos++ // skip )
						}
					}
					// ON DELETE / ON UPDATE
					for pos < len(tokens) &&
						tokens[pos].Type != compile.TokenComma &&
						tokens[pos].Type != compile.TokenRParen &&
						!isKeyword(tokens[pos], "references") {
						if isKeyword(tokens[pos], "on") && pos+2 < len(tokens) {
							action := strings.ToUpper(tokens[pos+2].Value)
							target := &fk.onDelete
							if isKeyword(tokens[pos+1], "update") {
								target = &fk.onUpdate
							}
							*target = action
							pos += 3
							// Handle multi-word actions: SET NULL, SET DEFAULT, NO ACTION
							if pos < len(tokens) && (*target == "SET" || *target == "NO") {
								*target += " " + strings.ToUpper(tokens[pos].Value)
								pos++
							}
							continue
						}
						if isKeyword(tokens[pos], "deferrable") {
							fk.deferred = true
							pos++
							// Skip optional INITIALLY DEFERRED / INITIALLY IMMEDIATE
							if pos < len(tokens) && isKeyword(tokens[pos], "initially") {
								pos++
								if pos < len(tokens) && (isKeyword(tokens[pos], "deferred") || isKeyword(tokens[pos], "immediate")) {
									pos++
								}
							}
							continue
						}
						pos++
					}
					if fk.onDelete == "" {
						fk.onDelete = "NO ACTION"
					}
					if fk.onUpdate == "" {
						fk.onUpdate = "NO ACTION"
					}
					colFKs = append(colFKs, fk)
				} else if isKeyword(tokens[pos], "default") {
					pos++
					// Capture default value
					if pos < len(tokens) {
						if tokens[pos].Type == compile.TokenLParen {
							// Expression default: skip for now
							depth := 1
							pos++
							for pos < len(tokens) && depth > 0 {
								if tokens[pos].Type == compile.TokenLParen {
									depth++
								} else if tokens[pos].Type == compile.TokenRParen {
									depth--
								}
								if depth > 0 {
									pos++
								}
							}
							if pos < len(tokens) {
								pos++
							}
						} else {
							// Simple default value
							dv := tokens[pos].Value
							pos++
							if v, err := strconv.ParseInt(dv, 10, 64); err == nil {
								colDefault = v
							} else if v, err := strconv.ParseFloat(dv, 64); err == nil {
								colDefault = v
							} else {
								colDefault = dv
							}
						}
					}
				} else if isKeyword(tokens[pos], "not") || isKeyword(tokens[pos], "null") ||
					isKeyword(tokens[pos], "collate") || isKeyword(tokens[pos], "autoincrement") ||
					isKeyword(tokens[pos], "deferrable") || isKeyword(tokens[pos], "initially") ||
					isKeyword(tokens[pos], "deferred") || isKeyword(tokens[pos], "immediate") {
					pos++
				} else {
					pos++
				}
			}

			// UNIQUE and PRIMARY KEY both create an auto-index, but only one
			if colHasUnique || colHasPK {
				uniqueCols = append(uniqueCols, colName)
			}

			columns = append(columns, columnEntry{name: colName, typeName: colType, isPK: colHasPK, defaultValue: colDefault})
			tableFKs = append(tableFKs, colFKs...)

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
		fks:      tableFKs,
	}
	db.rootPageMap[int(rootPage)] = db.tables[tableName]

	// Add entry to sqlite_master
	db.masterEntries = append(db.masterEntries, sqliteMasterEntry{
		Type:     "table",
		Name:     tableName,
		TblName:  tableName,
		RootPage: int(rootPage),
		SQL:      sqlText,
	})

	// Create auto-index entries for UNIQUE/PRIMARY KEY columns
	for i := range uniqueCols {
		autoName := fmt.Sprintf("sqlite_autoindex_%s_%d", tableName, i+1)
		db.masterEntries = append(db.masterEntries, sqliteMasterEntry{
			Type:     "index",
			Name:     autoName,
			TblName:  tableName,
			RootPage: 0,
			SQL:      "",
		})
	}

	return nil
}

// execCreateView handles CREATE VIEW statements.
func (db *Database) execCreateView(tokens []compile.Token) error {
	sqlText := rebuildSQL(tokens)

	pos := 0
	expectKeyword(tokens, &pos, "create")
	expectKeyword(tokens, &pos, "view")

	ifNotExists := false
	if pos < len(tokens) && isKeyword(tokens[pos], "if") {
		expectKeyword(tokens, &pos, "if")
		expectKeyword(tokens, &pos, "not")
		expectKeyword(tokens, &pos, "exists")
		ifNotExists = true
	}

	if pos >= len(tokens) {
		return NewError(Error, "expected view name")
	}
	viewName := tokens[pos].Value
	pos++

	if ifNotExists && db.views[viewName] != nil {
		return nil
	}

	if db.views[viewName] != nil {
		return NewErrorf(Error, "view %s already exists", viewName)
	}

	// Skip AS keyword
	if pos < len(tokens) && isKeyword(tokens[pos], "as") {
		pos++
	}

	// Collect remaining tokens as the SELECT SQL
	var parts []string
	for pos < len(tokens) {
		parts = append(parts, tokens[pos].Value)
		pos++
	}
	selectSQL := strings.Join(parts, " ")

	// Validate that the SELECT parses
	stmts, err := compile.Parse(selectSQL)
	if err != nil || len(stmts) == 0 {
		return NewErrorf(Error, "invalid SELECT in CREATE VIEW: %s", selectSQL)
	}

	// Check that the view's SELECT references valid tables/views
	sel := stmts[0]
	if sel.SelectStmt != nil && sel.SelectStmt.From != nil {
		for _, tref := range sel.SelectStmt.From.Tables {
			if tref.Name != "" && tref.Subquery == nil {
				name := strings.ToLower(tref.Name)
				if db.tables[name] == nil && db.views[name] == nil {
					return NewErrorf(Error, "no such table: %s", tref.Name)
				}
			}
		}
	}

	db.views[viewName] = &viewEntry{
		name: viewName,
		sql:  selectSQL,
	}

	// Add entry to sqlite_master
	db.masterEntries = append(db.masterEntries, sqliteMasterEntry{
		Type:    "view",
		Name:    viewName,
		TblName: viewName,
		SQL:     sqlText,
	})

	return nil
}

// execDropView handles DROP VIEW statements.
func (db *Database) execDropView(tokens []compile.Token) error {
	pos := 0
	expectKeyword(tokens, &pos, "drop")
	expectKeyword(tokens, &pos, "view")

	ifExists := false
	if pos < len(tokens) && isKeyword(tokens[pos], "if") {
		expectKeyword(tokens, &pos, "if")
		expectKeyword(tokens, &pos, "exists")
		ifExists = true
	}

	if pos >= len(tokens) {
		return NewError(Error, "expected view name")
	}
	viewName := tokens[pos].Value
	pos++

	if db.views[viewName] == nil {
		if ifExists {
			return nil
		}
		return NewErrorf(Error, "no such view: %s", viewName)
	}

	delete(db.views, viewName)
	db.removeMasterEntry("view", viewName)
	return nil
}

// execDropTable handles DROP TABLE statements.
func (db *Database) execDropTable(tokens []compile.Token) error {
	pos := 0
	expectKeyword(tokens, &pos, "drop")
	expectKeyword(tokens, &pos, "table")

	ifExists := false
	if pos < len(tokens) && isKeyword(tokens[pos], "if") {
		expectKeyword(tokens, &pos, "if")
		expectKeyword(tokens, &pos, "exists")
		ifExists = true
	}

	if pos >= len(tokens) {
		return NewError(Error, "expected table name")
	}
	tableName := tokens[pos].Value
	pos++

	if db.tables[tableName] == nil {
		if ifExists {
			return nil
		}
		return NewErrorf(Error, "no such table: %s", tableName)
	}

	// Check FK constraints: can't drop a table that is referenced by FK constraints with data
	if db.foreignKeys {
		childTable := ""
		for _, tbl := range db.tables {
			for _, fk := range tbl.fks {
				if strings.EqualFold(fk.refTable, tableName) {
					childTable = tbl.name
					break
				}
			}
			if childTable != "" {
				break
			}
		}
		if childTable != "" {
			// Check if the child table has any rows
			childTbl := db.tables[childTable]
			if childTbl != nil {
				cursor, err := db.bt.Cursor(btree.PageNumber(childTbl.rootPage), false)
				if err == nil {
					hasRow, _ := cursor.First()
					cursor.Close()
					if hasRow {
						return NewErrorf(ConstraintFK, "FOREIGN KEY constraint failed")
					}
				}
			}
		}
	}

	tbl := db.tables[tableName]
		delete(db.tables, tableName)
		delete(db.rootPageMap, tbl.rootPage)

		// Remove the table entry from sqlite_master and any associated indexes
		db.removeMasterEntry("table", tableName)
		db.removeMasterEntriesForTable(tableName)

	return nil
}

// execCreateIndex handles CREATE INDEX statements.
func (db *Database) execCreateIndex(tokens []compile.Token) error {
	sqlText := rebuildSQL(tokens)

	pos := 0
	expectKeyword(tokens, &pos, "create")
	if pos < len(tokens) && isKeyword(tokens[pos], "unique") {
		pos++
	}
	expectKeyword(tokens, &pos, "index")

	ifNotExists := false
	if pos < len(tokens) && isKeyword(tokens[pos], "if") {
		expectKeyword(tokens, &pos, "if")
		expectKeyword(tokens, &pos, "not")
		expectKeyword(tokens, &pos, "exists")
		ifNotExists = true
	}

	if pos >= len(tokens) {
		return NewError(Error, "expected index name")
	}
	indexName := unquoteIdent(tokens[pos].Value)
	pos++

	// ON keyword
	expectKeyword(tokens, &pos, "on")

	if pos >= len(tokens) {
		return NewError(Error, "expected table name in CREATE INDEX")
	}
	tableName := tokens[pos].Value
	pos++

	// Check table exists
	if db.tables[tableName] == nil {
		return NewErrorf(Error, "no such table: %s", tableName)
	}

	// Check if index already exists
	for _, e := range db.masterEntries {
		if e.Type == "index" && strings.EqualFold(e.Name, indexName) {
			if ifNotExists {
				return nil
			}
			return NewErrorf(Error, "index %s already exists", indexName)
		}
	}

	// Check index name doesn't conflict with a table name
	for _, e := range db.masterEntries {
		if e.Type == "table" && strings.EqualFold(e.Name, indexName) {
			return NewErrorf(Error, "there is already a table named %s", indexName)
		}
	}

	// Validate index columns/expressions
	tbl := db.tables[tableName]
	if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
		pos++ // skip (
		for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
			if tokens[pos].Type == compile.TokenComma {
				pos++
				continue
			}
			// Collect the index column expression tokens
			startPos := pos
			for pos < len(tokens) && tokens[pos].Type != compile.TokenComma && tokens[pos].Type != compile.TokenRParen {
				pos++
			}
			// Validate: if single identifier token, check it's a real column
			if pos-startPos == 1 && tokens[startPos].Type == compile.TokenID {
				colName := tokens[startPos].Value
				found := false
				for _, col := range tbl.columns {
					if strings.EqualFold(col.name, colName) {
						found = true
						break
					}
				}
				if !found {
					return NewErrorf(Error, "no such column: %s", colName)
				}
			}
			// For expressions (multiple tokens or non-ID), skip validation
		}
		if pos < len(tokens) {
			pos++ // skip )
		}
	}

	// Create a B-tree for the index
	if !db.inTx {
		if err := db.pgr.Begin(true); err != nil {
			return NewErrorf(Busy, "begin transaction: %s", err)
		}
		if err := db.bt.Begin(true); err != nil {
			db.pgr.Rollback()
			return NewErrorf(Error, "begin btree transaction: %s", err)
		}
	}

	rootPage, err := db.bt.CreateBTree(btree.CreateIndex)
	if err != nil {
		if !db.inTx {
			db.bt.Rollback()
			db.pgr.Rollback()
		}
		return NewErrorf(Error, "create index btree: %s", err)
	}

	if !db.inTx {
		if err := db.bt.Commit(); err != nil {
			return NewErrorf(Error, "commit btree: %s", err)
		}
		if err := db.pgr.Commit(); err != nil {
			return NewErrorf(IOError, "commit pager: %s", err)
		}
	}

	// Add entry to sqlite_master
	db.masterEntries = append(db.masterEntries, sqliteMasterEntry{
		Type:     "index",
		Name:     indexName,
		TblName:  tableName,
		RootPage: int(rootPage),
		SQL:      sqlText,
	})

	return nil
}

// execDropIndex handles DROP INDEX statements.
func (db *Database) execDropIndex(tokens []compile.Token) error {
	pos := 0
	expectKeyword(tokens, &pos, "drop")
	expectKeyword(tokens, &pos, "index")

	ifExists := false
	if pos < len(tokens) && isKeyword(tokens[pos], "if") {
		expectKeyword(tokens, &pos, "if")
		expectKeyword(tokens, &pos, "exists")
		ifExists = true
	}

	if pos >= len(tokens) {
		return NewError(Error, "expected index name")
	}
	indexName := unquoteIdent(tokens[pos].Value)
	pos++

	// Check if index exists
	found := false
	for _, e := range db.masterEntries {
		if e.Type == "index" && strings.EqualFold(e.Name, indexName) {
			found = true
			break
		}
	}

	// Protect auto-indexes from being dropped
	if strings.HasPrefix(strings.ToLower(indexName), "sqlite_autoindex_") {
		if found {
			return NewErrorf(Error, "unable to delete/modify system index: %s", indexName)
		}
		if ifExists {
			return NewErrorf(Error, "unable to delete/modify system index: %s", indexName)
		}
		return NewErrorf(Error, "no such index: %s", indexName)
	}

	if !found {
		if ifExists {
			return nil
		}
		return NewErrorf(Error, "no such index: %s", indexName)
	}

	db.removeMasterEntry("index", indexName)
	return nil
}

// execAlterTable handles ALTER TABLE statements.
func (db *Database) execAlterTable(tokens []compile.Token, sql string) error {
	pos := 0
	expectKeyword(tokens, &pos, "alter")
	expectKeyword(tokens, &pos, "table")

	if pos >= len(tokens) {
		return NewError(Error, "expected table name")
	}
	tableName := tokens[pos].Value
	pos++

	// Check for system tables
	lowerName := strings.ToLower(tableName)
	if lowerName == "sqlite_master" || lowerName == "sqlite_schema" ||
		lowerName == "sqlite_temp_master" {
		return NewErrorf(Error, "object reserved for internal use: %s", tableName)
	}

	// Cannot alter a view
	if _, isView := db.views[tableName]; isView {
		return NewErrorf(Error, "cannot alter view: %s", tableName)
	}

	// Determine ALTER operation
	if pos >= len(tokens) {
		return NewError(Error, "expected ALTER TABLE operation")
	}

	keyword := strings.ToLower(tokens[pos].Value)
	switch keyword {
	case "rename":
		pos++
		if pos < len(tokens) && isKeyword(tokens[pos], "to") {
			// RENAME TO new_name
			pos++
			return db.execAlterRenameTable(tableName, tokens, &pos)
		}
		if pos < len(tokens) && isKeyword(tokens[pos], "column") {
			pos++
		}
		// RENAME COLUMN old TO new or RENAME old TO new
		return db.execAlterRenameColumn(tableName, tokens, &pos)

	case "add":
		pos++
		if pos < len(tokens) && isKeyword(tokens[pos], "column") {
			pos++
		}
		return db.execAlterAddColumn(tableName, tokens, &pos)

	case "drop":
		pos++
		if pos < len(tokens) && isKeyword(tokens[pos], "column") {
			pos++
		}
		return db.execAlterDropColumn(tableName, tokens, &pos)

	default:
		return NewErrorf(Error, "unknown ALTER TABLE operation: %s", keyword)
	}
}

// execAlterRenameTable handles ALTER TABLE ... RENAME TO new_name.
func (db *Database) execAlterRenameTable(oldName string, tokens []compile.Token, pos *int) error {
	if *pos >= len(tokens) {
		return NewError(Error, "expected new table name")
	}
	newName := tokens[*pos].Value
	*pos++

	// Check for empty name
	if newName == "" {
		return NewError(Error, "invalid name")
	}

	// Check for sqlite_ prefix in new name
	if strings.HasPrefix(strings.ToLower(newName), "sqlite_") {
		return NewErrorf(Error, "object name reserved for internal use: %s", newName)
	}

	// Check table exists
	tbl, ok := db.tables[oldName]
	if !ok {
		return NewErrorf(Error, "no such table: %s", oldName)
	}

	// Check new name doesn't already exist (unless same name)
	if newName != oldName {
		if _, exists := db.tables[newName]; exists {
			return NewErrorf(Error, "table %s already exists", newName)
		}
	}

	// Update in-memory table entry
	tbl.name = newName
	delete(db.tables, oldName)
	db.tables[newName] = tbl

	// Update sqlite_master entries
	for i := range db.masterEntries {
		e := &db.masterEntries[i]
		if e.Type == "table" && e.Name == oldName {
			e.Name = newName
			e.TblName = newName
			// Update SQL to replace old name with new name
			e.SQL = replaceTableNameInSQL(e.SQL, oldName, newName)
		} else if e.TblName == oldName {
			// Update tbl_name for indexes and other associated entries
			e.TblName = newName
		}
	}

	return nil
}

// execAlterRenameColumn handles ALTER TABLE ... RENAME COLUMN old TO new.
func (db *Database) execAlterRenameColumn(tableName string, tokens []compile.Token, pos *int) error {
	if *pos >= len(tokens) {
		return NewError(Error, "expected old column name")
	}
	oldColName := tokens[*pos].Value
	*pos++

	if *pos >= len(tokens) || !isKeyword(tokens[*pos], "to") {
		return NewError(Error, "expected TO in RENAME COLUMN")
	}
	*pos++

	if *pos >= len(tokens) {
		return NewError(Error, "expected new column name")
	}
	newColName := tokens[*pos].Value
	*pos++

	// Check table exists
	tbl, ok := db.tables[tableName]
	if !ok {
		return NewErrorf(Error, "no such table: %s", tableName)
	}

	// Find and rename the column
	found := false
	for i := range tbl.columns {
		if strings.EqualFold(tbl.columns[i].name, oldColName) {
			tbl.columns[i].name = newColName
			found = true
			break
		}
	}
	if !found {
		return NewErrorf(Error, "no such column: %s", oldColName)
	}

	// Check new name doesn't conflict with existing column
	for i := range tbl.columns {
		if strings.EqualFold(tbl.columns[i].name, newColName) && tbl.columns[i].name != oldColName {
			return NewErrorf(Error, "duplicate column name: %s", newColName)
		}
	}

	// Update sqlite_master SQL
	for i := range db.masterEntries {
		e := &db.masterEntries[i]
		if e.Type == "table" && e.Name == tableName {
			e.SQL = replaceColumnNameInSQL(e.SQL, oldColName, newColName)
		}
	}

	return nil
}

// execAlterAddColumn handles ALTER TABLE ... ADD COLUMN col_def.
func (db *Database) execAlterAddColumn(tableName string, tokens []compile.Token, pos *int) error {
	// Check table exists
	tbl, ok := db.tables[tableName]
	if !ok {
		return NewErrorf(Error, "no such table: %s", tableName)
	}

	if *pos >= len(tokens) {
		return NewError(Error, "expected column name")
	}

	// Parse column name
	colName := tokens[*pos].Value
	*pos++

	// Check for duplicate column
	for _, c := range tbl.columns {
		if strings.EqualFold(c.name, colName) {
			return NewErrorf(Error, "duplicate column name: %s", colName)
		}
	}

	// Parse column type (optional)
	colType := ""
	if *pos < len(tokens) && tokens[*pos].Type == compile.TokenID &&
		!isKeyword(tokens[*pos], "default") && !isKeyword(tokens[*pos], "not") &&
		!isKeyword(tokens[*pos], "primary") && !isKeyword(tokens[*pos], "unique") &&
		!isKeyword(tokens[*pos], "check") && !isKeyword(tokens[*pos], "collate") &&
		!isKeyword(tokens[*pos], "constraint") && !isKeyword(tokens[*pos], "references") {
		colType = tokens[*pos].Value
		*pos++
	}

	// Parse constraints: DEFAULT, NOT NULL, UNIQUE, PRIMARY KEY, CHECK, COLLATE
	hasDefault := false
	var defaultVal interface{}
	hasNotNull := false
	hasUnique := false
	hasPrimaryKey := false
	hasCollate := false
	var collateName string

	for *pos < len(tokens) {
		if isKeyword(tokens[*pos], "default") {
			*pos++
			if *pos >= len(tokens) {
				return NewError(Error, "expected default value")
			}
			hasDefault = true
			// Check for expression default: (expr)
			if tokens[*pos].Type == compile.TokenLParen {
				*pos++ // skip (
				var exprParts []string
				depth := 0
				for *pos < len(tokens) {
					if tokens[*pos].Type == compile.TokenLParen {
						depth++
					}
					if tokens[*pos].Type == compile.TokenRParen {
						if depth == 0 {
							*pos++
							break
						}
						depth--
					}
					exprParts = append(exprParts, tokens[*pos].Value)
					*pos++
				}
				// Check for subqueries and aggregates
				expr := strings.Join(exprParts, " ")
				exprLower := strings.ToLower(expr)
				if strings.Contains(exprLower, "select") {
					return NewError(Error, "subqueries prohibited in DEFAULT")
				}
				for _, agg := range []string{"count", "sum", "avg", "min", "max", "group_concat", "total"} {
					if strings.Contains(exprLower, agg+"(") {
						return NewErrorf(Error, "misuse of aggregate function: %s()", agg)
					}
				}
				defaultVal = evalSimpleDefaultValue(expr)
			} else if tokens[*pos].Type == compile.TokenMinus {
				// Negative number
				*pos++
				if *pos >= len(tokens) {
					return NewError(Error, "expected value after -")
				}
				if tokens[*pos].Type == compile.TokenInteger {
					v, _ := strconv.ParseInt(tokens[*pos].Value, 10, 64)
					defaultVal = -v
				} else if tokens[*pos].Type == compile.TokenFloat {
					v, _ := strconv.ParseFloat(tokens[*pos].Value, 64)
					defaultVal = -v
				}
				*pos++
			} else {
				val, _ := parseExprValue(tokens, pos, nil, new(int))
				defaultVal = val
			}
		} else if isKeyword(tokens[*pos], "not") {
			*pos++
			expectKeyword(tokens, pos, "null")
			hasNotNull = true
		} else if isKeyword(tokens[*pos], "unique") {
			*pos++
			hasUnique = true
		} else if isKeyword(tokens[*pos], "primary") {
			*pos++
			expectKeyword(tokens, pos, "key")
			hasPrimaryKey = true
		} else if isKeyword(tokens[*pos], "check") {
			*pos++
			_ = true
			if *pos < len(tokens) && tokens[*pos].Type == compile.TokenLParen {
				*pos++
				depth := 0
				var parts []string
				for *pos < len(tokens) {
					if tokens[*pos].Type == compile.TokenLParen {
						depth++
					}
					if tokens[*pos].Type == compile.TokenRParen {
						if depth == 0 {
							*pos++
							break
						}
						depth--
					}
					parts = append(parts, tokens[*pos].Value)
					*pos++
				}
				_ = strings.Join(parts, " ")
			}
		} else if isKeyword(tokens[*pos], "collate") {
			*pos++
			hasCollate = true
			if *pos < len(tokens) {
				collateName = tokens[*pos].Value
				*pos++
			}
		} else {
			break
		}
	}

	// Validate constraints
	if hasNotNull && !hasDefault {
		// NOT NULL without DEFAULT is not allowed for ADD COLUMN
		// (existing rows would have NULL violating NOT NULL)
		// Unless table is empty - but SQLite still rejects this
		return NewErrorf(Error, "Cannot add a NOT NULL column with default value NULL")
	}

	if hasPrimaryKey {
		return NewErrorf(Error, "Cannot add a PRIMARY KEY column")
	}
	if hasUnique {
		return NewErrorf(Error, "Cannot add a UNIQUE column")
	}

	// Add column to table entry
	tbl.columns = append(tbl.columns, columnEntry{
			name:         colName,
			typeName:     colType,
			defaultValue: defaultVal,
	})

	// Store default value info in the column for future INSERT operations
	// We extend columnEntry to track defaults
	if hasDefault {
		db.columnDefaults[tableName+"."+colName] = defaultVal
	}

	// Update sqlite_master SQL
	for i := range db.masterEntries {
		e := &db.masterEntries[i]
		if e.Type == "table" && e.Name == tableName {
			e.SQL = addColumnToSQL(e.SQL, colName, colType, hasDefault, defaultVal, hasNotNull, hasCollate, collateName)
		}
	}

	return nil
}

// execAlterDropColumn handles ALTER TABLE ... DROP COLUMN col_name.
func (db *Database) execAlterDropColumn(tableName string, tokens []compile.Token, pos *int) error {
	// Check table exists
	tbl, ok := db.tables[tableName]
	if !ok {
		return NewErrorf(Error, "no such table: %s", tableName)
	}

	if *pos >= len(tokens) {
		return NewError(Error, "expected column name")
	}
	colName := tokens[*pos].Value
	*pos++

	// Find and remove the column
	found := false
	newColumns := make([]columnEntry, 0, len(tbl.columns))
	colIdx := -1
	for i, c := range tbl.columns {
		if strings.EqualFold(c.name, colName) {
			found = true
			colIdx = i
			continue
		}
		newColumns = append(newColumns, c)
	}
	if !found {
		return NewErrorf(Error, "no such column: %s", colName)
	}

	if len(newColumns) == 0 {
		return NewError(Error, "cannot drop all columns from a table")
	}

	// Update table entry
	tbl.columns = newColumns

	// Clean up any default for the dropped column
	delete(db.columnDefaults, tableName+"."+colName)

	// Update sqlite_master SQL
	for i := range db.masterEntries {
		e := &db.masterEntries[i]
		if e.Type == "table" && e.Name == tableName {
			e.SQL = dropColumnFromSQL(e.SQL, colName, colIdx)
		}
	}

	// Note: existing rows still have data for the dropped column in the B-tree,
	// but SELECT will now skip it since the column is removed from the schema.
	// The colIdx tracking ensures the record fields align correctly.

	return nil
}

// replaceTableNameInSQL replaces the table name in a CREATE TABLE SQL statement.
func replaceTableNameInSQL(sql, oldName, newName string) string {
	// Find "CREATE TABLE" followed by the old name
	lower := strings.ToLower(sql)
	idx := strings.Index(lower, "create table")
	if idx < 0 {
		return sql
	}
	idx += len("create table")

	// Skip optional IF NOT EXISTS
	rest := sql[idx:]
	trimmed := strings.TrimLeft(rest, " \t\n\r")
	lowerRest := strings.ToLower(trimmed)
	if strings.HasPrefix(lowerRest, "if not exists") {
		idx += len(sql[idx:]) - len(trimmed) + len("if not exists")
		trimmed = strings.TrimLeft(sql[idx:], " \t\n\r")
	}

	// Skip whitespace and optional quotes
	prefix := sql[:idx]
	rest = sql[idx:]

	// Handle quoted identifiers
	if len(rest) > 0 && (rest[0] == '"' || rest[0] == '`' || rest[0] == '[') {
		quote := rest[0]
		endQuote := strings.IndexByte(rest[1:], quote)
		if endQuote >= 0 {
			return prefix + string(quote) + newName + rest[1+endQuote+1:]
		}
	}

	// Unquoted: replace the next token
	if strings.HasPrefix(rest, oldName) {
		return prefix + newName + rest[len(oldName):]
	}
	// Case-insensitive fallback
	if strings.HasPrefix(strings.ToLower(rest), strings.ToLower(oldName)) {
		return prefix + newName + rest[len(oldName):]
	}
	return sql
}

// replaceColumnNameInSQL replaces a column name in a CREATE TABLE SQL statement.
func replaceColumnNameInSQL(sql, oldName, newName string) string {
	// Simple approach: find the column name in the SQL and replace it.
	// We need to be careful to only replace column names, not table names or other identifiers.
	// Use case-insensitive word boundary matching.
	result := sql
	// Replace as a word boundary match
	pattern := regexp.MustCompile(`\b` + regexp.QuoteMeta(oldName) + `\b`)
	result = pattern.ReplaceAllStringFunc(result, func(match string) string {
		// Only replace if it's a column context (not the table name after CREATE TABLE)
		return newName
	})
	return result
}

// addColumnToSQL modifies a CREATE TABLE SQL string to include a new column.
func addColumnToSQL(sql, colName, colType string, hasDefault bool, defaultVal interface{},
	hasNotNull, hasCollate bool, collateName string) string {
	// Find the closing paren of the column list
	closeParen := strings.LastIndex(sql, ")")
	if closeParen < 0 {
		return sql
	}

	var colDef strings.Builder
	colDef.WriteString(", ")
	colDef.WriteString(colName)
	if colType != "" {
		colDef.WriteString(" ")
		colDef.WriteString(colType)
	}
	if hasNotNull {
		colDef.WriteString(" NOT NULL")
	}
	if hasDefault {
		colDef.WriteString(" DEFAULT ")
		switch v := defaultVal.(type) {
		case nil:
			colDef.WriteString("NULL")
		case int64:
			colDef.WriteString(strconv.FormatInt(v, 10))
		case float64:
			colDef.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
		case string:
			colDef.WriteString("'")
			colDef.WriteString(strings.ReplaceAll(v, "'", "''"))
			colDef.WriteString("'")
		default:
			colDef.WriteString(fmt.Sprintf("%v", v))
		}
	}
	if hasCollate && collateName != "" {
		colDef.WriteString(" COLLATE ")
		colDef.WriteString(collateName)
	}

	return sql[:closeParen] + colDef.String() + sql[closeParen:]
}

// dropColumnFromSQL removes a column from a CREATE TABLE SQL string.
func dropColumnFromSQL(sql string, colName string, colIdx int) string {
	// Find the opening and closing parens of the column list
	openParen := strings.Index(sql, "(")
	closeParen := strings.LastIndex(sql, ")")
	if openParen < 0 || closeParen < 0 {
		return sql
	}

	inner := sql[openParen+1 : closeParen]

	// Split by commas, respecting parentheses depth
	parts := splitColumnDefs(inner)
	if colIdx < 0 || colIdx >= len(parts) {
		return sql
	}

	// Remove the column at colIdx
	newParts := make([]string, 0, len(parts)-1)
	for i, p := range parts {
		if i != colIdx {
			newParts = append(newParts, p)
		}
	}

	return sql[:openParen+1] + strings.Join(newParts, ",") + sql[closeParen:]
}

// splitColumnDefs splits a comma-separated column definition list,
// respecting parentheses depth.
func splitColumnDefs(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '(' {
			depth++
		} else if s[i] == ')' {
			depth--
		} else if s[i] == ',' && depth == 0 {
			parts = append(parts, strings.TrimSpace(s[start:i]))
			start = i + 1
		}
	}
	if start < len(s) {
		parts = append(parts, strings.TrimSpace(s[start:]))
	}
	return parts
}

// evalSimpleDefaultValue evaluates a simple default value expression.
func evalSimpleDefaultValue(expr string) interface{} {
	expr = strings.TrimSpace(expr)
	// Simple arithmetic
	if strings.Contains(expr, "+") {
		parts := strings.SplitN(expr, "+", 2)
		left := evalSimpleDefaultValue(parts[0])
		right := evalSimpleDefaultValue(parts[1])
		if li, ok := left.(int64); ok {
			if ri, ok := right.(int64); ok {
				return li + ri
			}
		}
	}
	if v, err := strconv.ParseInt(expr, 10, 64); err == nil {
		return v
	}
	if v, err := strconv.ParseFloat(expr, 64); err == nil {
		return v
	}
	return expr
}


func (db *Database) removeMasterEntry(typeName, name string) {
	for i, e := range db.masterEntries {
		if e.Type == typeName && strings.EqualFold(e.Name, name) {
			db.masterEntries = append(db.masterEntries[:i], db.masterEntries[i+1:]...)
			return
		}
	}
}

// removeMasterEntriesForTable removes all entries associated with a table
// (indexes, triggers) from sqlite_master. The table entry itself should
// already have been removed.
func (db *Database) removeMasterEntriesForTable(tableName string) {
	var kept []sqliteMasterEntry
	for _, e := range db.masterEntries {
		if strings.EqualFold(e.TblName, tableName) && e.Type != "table" {
			continue // remove associated index/trigger entries
		}
		kept = append(kept, e)
	}
	db.masterEntries = kept
}

// rebuildSQL reconstructs a SQL string from tokens.
func rebuildSQL(tokens []compile.Token) string {
	var buf strings.Builder
	for i, t := range tokens {
		if i > 0 {
			prev := tokens[i-1].Value
			noSpaceBefore := t.Value == "(" || t.Value == ")" || t.Value == ","
			noSpaceAfter := prev == "(" || prev == ","
			if !noSpaceBefore && !noSpaceAfter {
				buf.WriteByte(' ')
			}
		}
		buf.WriteString(t.Value)
	}
	return buf.String()
}

// execInsert handles INSERT statements.
func (db *Database) execInsert(tokens []compile.Token, args []interface{}) error {
	// Parse: INSERT INTO table [(cols)] VALUES (vals) or INSERT INTO table VALUES vals
	pos := 0
	expectKeyword(tokens, &pos, "insert")
	isReplace := false
	if pos < len(tokens) && isKeyword(tokens[pos], "or") {
		pos++
		if pos < len(tokens) && isKeyword(tokens[pos], "replace") {
			isReplace = true
		}
		pos++
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
	bindIdx := 0
	for pos < len(tokens) {
		var values []interface{}
		if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
			pos++ // skip (
			for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
				val, err := parseInsertExprValue(tokens, &pos, args, &bindIdx)
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

		if isReplace {
			if err := db.handleReplace(tbl, colList, values, args); err != nil {
				return err
			}
		} else {
			if err := db.insertRow(tbl, colList, values, args); err != nil {
				return err
			}
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

	rb := vdbe.NewRecordBuilder()
	for _, col := range tbl.columns {
		v, ok := valMap[col.name]
		if !ok {
			// Check for column default from ALTER TABLE ADD COLUMN
			if defVal, hasDef := db.columnDefaults[tbl.name+"."+col.name]; hasDef {
				addValueToRecord(rb, defVal)
			} else {
				rb.AddNull()
			}
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

	// FK constraint check (after insert so self-referencing rows work)
	if err := db.fkCheckInsert(tbl, valMap); err != nil {
		// Rollback: delete the just-inserted row
		db.deleteRowByID(tbl, newRowID)
		db.changes = 0
		db.totalChanges--
		if needCommit {
			db.bt.Rollback()
			db.pgr.Rollback()
		}
		return err
	}

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
func (db *Database) execDelete(tokens []compile.Token, args []interface{}) error {
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

	// Parse optional WHERE clause
	var whereExpr string
	if pos < len(tokens) && isKeyword(tokens[pos], "where") {
		pos++
		var whereParts []string
		for pos < len(tokens) {
			whereParts = append(whereParts, tokens[pos].Value)
			pos++
		}
		whereExpr = strings.Join(whereParts, " ")
	}

	// Build column name list for expression evaluation
	colNames := make([]string, len(tbl.columns))
	for i, col := range tbl.columns {
		colNames[i] = col.name
	}

	cursor, err := db.bt.Cursor(btree.PageNumber(tbl.rootPage), true)
	if err != nil {
		return NewErrorf(Error, "open cursor: %s", err)
	}

	var count int64
	// Collect rows to delete first (needed for FK check)
	type deleteRow struct {
		rowID   int64
		rowVals map[string]interface{}
	}
	var rowsToDelete []deleteRow

	hasRow, err := cursor.First()
	if err != nil {
		return err
	}
	for hasRow {
		data, err := cursor.Data()
		if err != nil {
			return err
		}
		values, err := vdbe.ParseRecord(data)
		if err != nil {
			return err
		}
		// Pad values for columns added via ALTER TABLE
		if len(values) < len(tbl.columns) {
			padded := make([]vdbe.Value, len(tbl.columns))
			copy(padded, values)
			for j := len(values); j < len(tbl.columns); j++ {
				if tbl.columns[j].defaultValue != nil {
					padded[j] = interfaceToValue(tbl.columns[j].defaultValue)
				} else {
					padded[j] = vdbe.Value{Type: "null"}
				}
			}
			values = padded
		}
		if whereExpr != "" {
			wval := evalExprWithRow(whereExpr, args, colNames, values)
			if !isTruthy(wval) {
				hasRow, err = cursor.Next()
				if err != nil {
					return err
				}
				continue
			}
		}
		rowVals := make(map[string]interface{})
		for i, col := range tbl.columns {
			if i < len(values) {
				rowVals[col.name] = fkValueToInterface(values[i])
			}
		}
		rowsToDelete = append(rowsToDelete, deleteRow{rowID: int64(cursor.RowID()), rowVals: rowVals})
		hasRow, err = cursor.Next()
		if err != nil {
			return err
		}
	}
	cursor.Close()

	// Delete rows first, then check FK constraints (so self-referencing works)
	for _, row := range rowsToDelete {
		if err := db.deleteRowByID(tbl, row.rowID); err != nil {
			return err
		}
		count++
	}
	// After all deletes, check FK constraints
	// (We've already deleted, so self-referencing rows are gone)
	// If we need to rollback, re-insert deleted rows
	for _, row := range rowsToDelete {
		if err := db.fkCheckDelete(tableName, row.rowVals); err != nil {
			// Rollback: re-insert already-deleted rows
			for i := len(rowsToDelete) - 1; i >= 0; i-- {
				rb := vdbe.NewRecordBuilder()
				for _, col := range tbl.columns {
					addValueToRecord(rb, rowsToDelete[i].rowVals[col.name])
				}
				cursor2, cerr := db.bt.Cursor(btree.PageNumber(tbl.rootPage), true)
				if cerr != nil {
					break
				}
				keyBuf := make([]byte, 9)
				keyLen := encodeVarintKey(keyBuf, rowsToDelete[i].rowID)
				newRowID := rowsToDelete[i].rowID
				db.bt.Insert(cursor2, keyBuf[:keyLen], rb.Build(), btree.RowID(newRowID), btree.SeekNotFound)
				cursor2.Close()
			}
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
		col      string
		exprStr  string
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
		// Collect expression tokens until comma or WHERE
		var exprParts []string
		for pos < len(tokens) && tokens[pos].Type != compile.TokenComma && !isKeyword(tokens[pos], "where") {
			exprParts = append(exprParts, tokens[pos].Value)
			pos++
		}
		sets = append(sets, setPair{col: colName, exprStr: strings.Join(exprParts, " ")})
		if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
			pos++
		}
	}

	// Parse optional WHERE clause
	var whereExpr string
	if pos < len(tokens) && isKeyword(tokens[pos], "where") {
		pos++
		var whereParts []string
		for pos < len(tokens) {
			whereParts = append(whereParts, tokens[pos].Value)
			pos++
		}
		whereExpr = strings.Join(whereParts, " ")
	}

	// Build column name list
	colNames := make([]string, len(tbl.columns))
	for i, col := range tbl.columns {
		colNames[i] = col.name
	}

	// Collect all rows first, then apply updates to avoid cursor invalidation
	type updateEntry struct {
		rowid      int64
		newData    []byte
		oldVals    map[string]interface{}
		newVals    map[string]interface{}
	}
	var updates []updateEntry

	cursor, err := db.bt.Cursor(btree.PageNumber(tbl.rootPage), true)
	if err != nil {
		return NewErrorf(Error, "open cursor: %s", err)
	}
	defer cursor.Close()

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

		// Apply sets with per-row expression evaluation
			// Pad values with defaults for columns added via ALTER TABLE
			if len(values) < len(tbl.columns) {
				padded := make([]vdbe.Value, len(tbl.columns))
				copy(padded, values)
				for j := len(values); j < len(tbl.columns); j++ {
					if tbl.columns[j].defaultValue != nil {
						padded[j] = interfaceToValue(tbl.columns[j].defaultValue)
					} else {
						padded[j] = vdbe.Value{Type: "null"}
					}
				}
				values = padded
			}
		// Apply WHERE filter
		if whereExpr != "" {
			wval := evalExprWithRow(whereExpr, args, colNames, values)
			if !isTruthy(wval) {
				hasRow, err = cursor.Next()
				if err != nil {
					return err
				}
				continue
			}
		}
		// Save old values before applying SETs (for FK checks)
		oldVals := make(map[string]interface{})
		for i, col := range tbl.columns {
			if i < len(values) {
				oldVals[col.name] = fkValueToInterface(values[i])
			}
		}
		for _, s := range sets {
			evalVal := evalExprWithRow(s.exprStr, args, colNames, values)
			for i, col := range tbl.columns {
				if strings.EqualFold(col.name, s.col) {
					if i < len(values) {
						values[i] = memToValue(evalVal)
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

		// Capture new values (old values were saved before SETs)

		newVals := make(map[string]interface{})
		for i, col := range tbl.columns {
			if i < len(values) {
				newVals[col.name] = fkValueToInterface(values[i])
			}
		}

		updates = append(updates, updateEntry{rowid: int64(rowid), newData: newData, oldVals: oldVals, newVals: newVals})
		hasRow, err = cursor.Next()
		if err != nil {
			return err
		}
	}


		// Now apply all updates
		var count int64
		for _, upd := range updates {
			keyBuf := make([]byte, 9)
			keyLen := encodeVarintKey(keyBuf, upd.rowid)

			if err := db.bt.Insert(cursor, keyBuf[:keyLen], upd.newData, btree.RowID(upd.rowid), btree.SeekFound); err != nil {
				return err
			}
			count++

			// FK check after write (so self-referencing works)
			if err := db.fkCheckUpdate(tableName, upd.oldVals, upd.newVals); err != nil {
				// Rollback this update
				db.updateRowByID(tbl, upd.rowid, upd.oldVals)
				return err
			}
			if err := db.fkCheckInsert(tbl, upd.newVals); err != nil {
				// Rollback this update
				db.updateRowByID(tbl, upd.rowid, upd.oldVals)
				return err
			}
		}

		db.changes = count
		db.totalChanges += count
		return nil
	}

// vdbeDB adapts *Database to implement the vdbe.Database interface.
type vdbeDB struct {
	db *Database
}

func (v vdbeDB) GetTableInfo(rootPage int) (*vdbe.TableInfo, error) {
	tbl := v.db.rootPageMap[rootPage]
	if tbl == nil {
		return nil, fmt.Errorf("no table with root page %d", rootPage)
	}
	cols := make([]vdbe.ColumnInfo, len(tbl.columns))
	for i, c := range tbl.columns {
		aff := byte('u')
		switch strings.ToUpper(c.typeName) {
		case "TEXT":
			aff = 't'
		case "INTEGER":
			aff = 'i'
		case "REAL":
			aff = 'n'
		case "BLOB":
			aff = 'b'
		}
		cols[i] = vdbe.ColumnInfo{Name: c.name, Affinity: aff}
	}
	return &vdbe.TableInfo{RootPage: rootPage, Columns: cols, Name: tbl.name}, nil
}

func (v vdbeDB) GetCursor(rootPage int, write bool) (interface{}, error) {
	tbl := v.db.rootPageMap[rootPage]
	if tbl == nil {
		return nil, fmt.Errorf("no table with root page %d", rootPage)
	}
	cur, err := v.db.bt.Cursor(btree.PageNumber(tbl.rootPage), write)
	if err != nil {
		return nil, err
	}
	return cur, nil
}

func (v vdbeDB) BeginTransaction(write bool) error {
	if err := v.db.pgr.Begin(write); err != nil {
		return err
	}
	return v.db.bt.Begin(write)
}

func (v vdbeDB) Commit() error {
	if err := v.db.bt.Commit(); err != nil {
		return err
	}
	return v.db.pgr.Commit()
}

func (v vdbeDB) Rollback() error {
	v.db.bt.Rollback()
	v.db.pgr.Rollback()
	return nil
}

func (v vdbeDB) AutoCommit() bool       { return v.db.autoCommit }
func (v vdbeDB) SetAutoCommit(on bool)   { v.db.autoCommit = on }
func (v vdbeDB) Changes() int64         { return v.db.changes }
func (v vdbeDB) TotalChanges() int64    { return v.db.totalChanges }
func (v vdbeDB) LastInsertRowID() int64 { return v.db.lastInsertRowID }
func (v vdbeDB) SetLastInsertRowID(id int64) { v.db.lastInsertRowID = id }

// Insert adapts btree insert for vdbe.Inserter interface.
func (v vdbeDB) Insert(cursor interface{}, key []byte, data []byte, rowid int64, seekResult int) error {
	cur, ok := cursor.(btree.BTCursor)
	if !ok {
		return fmt.Errorf("invalid cursor type for insert")
	}
	return v.db.bt.Insert(cur, key, data, btree.RowID(rowid), btree.SeekResult(seekResult))
}

// Delete adapts btree delete for vdbe.Deleter interface.
func (v vdbeDB) Delete(cursor interface{}) error {
	cur, ok := cursor.(btree.BTCursor)
	if !ok {
		return fmt.Errorf("invalid cursor type for delete")
	}
	return v.db.bt.Delete(cur)
}

// CreateBTree implements vdbe.BTreeCreator.
func (v vdbeDB) CreateBTree(flags int) (int, error) {
	pn, err := v.db.bt.CreateBTree(btree.CreateFlags(flags))
	return int(pn), err
}

// DestroyBTree implements vdbe.BTreeDestroyer.
func (v vdbeDB) DestroyBTree(rootPage int) error {
	return v.db.bt.Drop(btree.PageNumber(rootPage))
}

// buildCompileSchema converts in-memory table metadata into a compile.Schema.
func (db *Database) buildCompileSchema() *compile.Schema {
	schema := compile.NewSchema()
	for name, tbl := range db.tables {
		cols := make([]compile.ColumnInfo, len(tbl.columns))
		for i, c := range tbl.columns {
			cols[i] = compile.ColumnInfo{Name: c.name, Type: c.typeName}
		}
		schema.AddTable(&compile.TableInfo{
			Name:     name,
			Columns:  cols,
			HasRowid: true,
			RootPage: tbl.rootPage,
		})
	}
	return schema
}

// executeQuery runs a SQL query through the compile pipeline and VDBE.
func (db *Database) executeQuery(sql string, args []interface{}) (*ResultSet, error) {
	// Parse
	stmts, err := compile.Parse(sql)
	if err != nil {
		return nil, NewErrorf(Error, "parse: %s", err)
	}
	if len(stmts) == 0 {
		return nil, NewError(Error, "empty SQL statement")
	}

	schema := db.buildCompileSchema()

	var allRows []Row
	var resultCols []ResultColumnInfo

	// Extract column names upfront (even if there are 0 result rows)
	for _, stmt := range stmts {
		if stmt.SelectStmt != nil && resultCols == nil {
			colNames := extractResultColNames(stmt)
			resultCols = make([]ResultColumnInfo, len(colNames))
			for i, n := range colNames {
				resultCols[i] = ResultColumnInfo{Name: n, Type: ColNull}
			}
		}
	}

	for _, stmt := range stmts {
		// Compile
		prog, err := compile.Compile(stmt, schema)
		if err != nil {
			return nil, NewErrorf(Error, "compile: %s", err)
		}

		// Convert compile.Program → vdbe.Program
		vprog := &vdbe.Program{
			Instructions: prog.Instructions,
			NumRegs:      prog.NumRegs,
			NumCursors:   prog.NumCursors,
			SQL:          sql,
		}

		// Create VDBE and load program
		vm := vdbe.NewVDBE(vdbeDB{db: db})
		vm.SetProgram(vprog)

		// Set up result row callback to collect rows
		var rows []Row
		var colNames []string
		vm.SetResultRowCallback(func(regs []vdbe.Mem, startIdx, count int) {
			if colNames == nil {
				colNames = extractResultColNames(stmt)
				resultCols = make([]ResultColumnInfo, len(colNames))
				for i, n := range colNames {
					resultCols[i] = ResultColumnInfo{Name: n, Type: ColNull}
				}
			}
			row := Row{cols: resultCols}
			for i := 0; i < count; i++ {
				idx := startIdx + i
				if idx < len(regs) {
					cp := regs[idx]; row.values = append(row.values, &cp)
				} else {
					row.values = append(row.values, vdbe.NewMemNull())
				}
			}
			rows = append(rows, row)
		})

		// Bind parameters
		if len(args) > 0 {
			vmRegs := vm.Registers()
			for i, arg := range args {
				if i+1 < len(vmRegs) {
					vmRegs[i+1] = *vdbe.MakeMem(arg)
				}
			}
		}

		// Execute (protected by recover to fall back to hack layer on errors)
		ctx := context.Background()
		var stepErr error
		func() {
			defer func() {
				if r := recover(); r != nil {
					stepErr = fmt.Errorf("panic during VDBE execution: %v", r)
				}
			}()
			for {
				hasRow, err := vm.Step(ctx)
				if err != nil {
					stepErr = err
					return
				}
				if !hasRow {
					return
				}
			}
		}()
		if stepErr != nil {
			return nil, fmt.Errorf("compile pipeline error: %w", stepErr)
		}

		allRows = append(allRows, rows...)
	}

	if allRows == nil {
		allRows = []Row{}
	}
	if resultCols == nil {
		resultCols = []ResultColumnInfo{}
	}
	return newResultSet(allRows, resultCols), nil
}

// extractResultColNames extracts column names from a compiled statement's AST.
func extractResultColNames(stmt *compile.Statement) []string {
	if stmt.SelectStmt == nil {
		return nil
	}
	var names []string
	for _, rc := range stmt.SelectStmt.Columns {
		if rc.As != "" {
			names = append(names, rc.As)
		} else if rc.Star {
			names = append(names, "*")
		} else if rc.Expr != nil {
			names = append(names, exprText(rc.Expr))
		} else {
			names = append(names, "?")
		}
	}
	return names
}

// exprText converts an expression AST node back to a readable column name.
func exprText(e *compile.Expr) string {
	if e == nil {
		return ""
	}
	switch e.Kind {
	case compile.ExprColumnRef:
		if e.Table != "" {
			return e.Table + "." + e.Name
		}
		return e.Name
	case compile.ExprLiteral:
		return e.StringValue
	case compile.ExprFunctionCall:
		var args []string
		for _, a := range e.Args {
			args = append(args, exprText(a))
		}
		return e.FunctionName + "(" + strings.Join(args, ",") + ")"
	default:
		if e.Name != "" {
			return e.Name
		}
		return "?"
	}
}

// querySingle executes a SELECT and collects results.
// Uses the compile pipeline when results match the hack layer; otherwise
// falls back to hack layer.
func (db *Database) querySingle(sql string, args []interface{}) (*ResultSet, error) {
	// Try compile pipeline
	compileRS, compileErr := db.executeQuery(sql, args)

	// Run hack layer
	tokens := compile.Tokenize(sql)
	filtered := filterTokens(tokens)
	if len(filtered) == 0 {
		return nil, NewError(Error, "empty SQL statement")
	}

	if isKeyword(filtered[0], "with") {
		return db.queryWithCTE(sql, args)
	}

	// Check for compound SELECT (UNION / UNION ALL / INTERSECT / EXCEPT)
	if compoundParts := splitCompoundSelect(filtered); len(compoundParts) > 1 {
		return db.queryCompoundSelect(compoundParts, args)
	}

	if !isKeyword(filtered[0], "select") {
		return nil, NewError(Error, "not a SELECT statement")
	}

	// Parse SELECT columns
	pos := 1 // skip SELECT

	// Check DISTINCT
	distinct := false
	if pos < len(filtered) && isKeyword(filtered[pos], "distinct") {
		distinct = true
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
			parenDepth := 0
		for pos < len(filtered) &&
			(filtered[pos].Type != compile.TokenComma || parenDepth > 0) &&
			!isKeyword(filtered[pos], "from") &&
			!isKeyword(filtered[pos], "where") &&
			!isKeyword(filtered[pos], "limit") &&
			(parenDepth > 0 || !isKeyword(filtered[pos], "order")) &&
			!isKeyword(filtered[pos], "group") {
			if isKeyword(filtered[pos], "as") && parenDepth == 0 {
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
			if filtered[pos].Type == compile.TokenLParen {
				parenDepth++
			}
			if filtered[pos].Type == compile.TokenRParen {
				parenDepth--
			}
			exprParts = append(exprParts, filtered[pos].Value)
			pos++
		}
		if len(exprParts) > 0 {
			cols = append(cols, selectCol{expr: strings.Join(exprParts, " ")})
		}
	}

	// Parse LIMIT/OFFSET early (needed for no-FROM path too)
	var earlyLimit int = -1
	var earlyOffset int
	earlyPos := pos
	for earlyPos < len(filtered) && !isKeyword(filtered[earlyPos], "limit") {
		earlyPos++
	}
	if earlyPos < len(filtered) && isKeyword(filtered[earlyPos], "limit") {
		earlyPos++
		if earlyPos < len(filtered) {
			if n, err := strconv.Atoi(filtered[earlyPos].Value); err == nil {
				earlyLimit = n
			}
			earlyPos++
		}
		if earlyPos < len(filtered) && isKeyword(filtered[earlyPos], "offset") {
			earlyPos++
			if earlyPos < len(filtered) {
				if n, err := strconv.Atoi(filtered[earlyPos].Value); err == nil {
					earlyOffset = n
				}
			}
		}
	}

	// Parse FROM clause
	var tableName string
	var joinTables []joinTableInfo
	var hasJoin bool
	if pos < len(filtered) && isKeyword(filtered[pos], "from") {
		pos++
		joinTables, hasJoin, pos = db.parseFromTables(filtered, pos)
		if len(joinTables) > 0 {
			tableName = joinTables[0].name
		}
	}

	// For SELECT without FROM (e.g., SELECT 1+2), compute directly
	if tableName == "" {
		rs, err := db.selectWithoutTable(cols, args)
		if err != nil {
			return nil, err
		}
		if earlyLimit >= 0 {
			allRows := rs.Rows()
			if earlyOffset > 0 {
				if earlyOffset < len(allRows) {
					allRows = allRows[earlyOffset:]
				} else {
					allRows = []Row{}
				}
			}
			if earlyLimit < len(allRows) {
				allRows = allRows[:earlyLimit]
			}
			if len(allRows) == 0 {
				allRows = []Row{}
			}
			// Preserve column info from original result
			rc := make([]ResultColumnInfo, rs.ColumnCount())
			for i := 0; i < rs.ColumnCount(); i++ {
				rc[i] = *rs.ColumnInfo(i)
			}
			rs = newResultSet(allRows, rc)
		}
		return rs, nil
	}

	// Handle sqlite_master / sqlite_schema virtual table
	lowerTable := strings.ToLower(tableName)
	if lowerTable == "sqlite_master" || lowerTable == "sqlite_schema" {
		return db.querySqliteMaster(filtered, pos, cols, args)
	}

	// Multi-table JOIN path
	if hasJoin && len(joinTables) > 1 {
		return db.queryJoin(joinTables, filtered, pos, cols, distinct, args)
	}

	var tbl *tableEntry
	var cteTbl *cteTable

	if t, ok := db.tables[tableName]; ok {
		tbl = t
	} else if db.cteData != nil {
		if ct, cok := db.cteData[tableName]; cok {
			cteTbl = ct
		}
	}

	if tbl == nil && cteTbl == nil {
		// Check if it's a view
		if v, vok := db.views[tableName]; vok {
			return db.queryView(v, cols, args)
		}
		return nil, NewErrorf(Error, "no such table: %s", tableName)
	}

	// Effective columns for both regular tables and CTEs
	var ecols []columnEntry
	if cteTbl != nil {
		ecols = cteTbl.columns
	} else if tbl != nil {
		ecols = tbl.columns
	}

	// Parse WHERE clause
	var whereExpr string
	if pos < len(filtered) && isKeyword(filtered[pos], "where") {
		pos++
		var whereParts []string
		for pos < len(filtered) && !isKeyword(filtered[pos], "limit") &&
			!isKeyword(filtered[pos], "order") && !isKeyword(filtered[pos], "group") {
			whereParts = append(whereParts, filtered[pos].Value)
			pos++
		}
		whereExpr = db.resolveSubqueries(strings.Join(whereParts, " "), args)
	}

	// Parse GROUP BY clause
	var groupByExprs []string
	if pos < len(filtered) && isKeyword(filtered[pos], "group") {
		pos++
		if pos < len(filtered) && isKeyword(filtered[pos], "by") {
			pos++
		}
		for pos < len(filtered) &&
			!isKeyword(filtered[pos], "having") &&
			!isKeyword(filtered[pos], "order") &&
			!isKeyword(filtered[pos], "limit") {
			if filtered[pos].Type == compile.TokenComma {
				pos++
				continue
			}
			var gbParts []string
			for pos < len(filtered) &&
				filtered[pos].Type != compile.TokenComma &&
				!isKeyword(filtered[pos], "having") &&
				!isKeyword(filtered[pos], "order") &&
				!isKeyword(filtered[pos], "limit") {
				gbParts = append(gbParts, filtered[pos].Value)
				pos++
			}
			if len(gbParts) > 0 {
				groupByExprs = append(groupByExprs, strings.Join(gbParts, " "))
			}
		}
	}

	// Validate non-empty GROUP BY
	if len(groupByExprs) == 0 && pos >= 2 && isKeyword(filtered[pos-2], "group") {
		return nil, NewErrorf(Error, "GROUP BY requires at least one expression")
	}

	// Resolve GROUP BY aliases and column numbers
	for i, gbExpr := range groupByExprs {
		trimmed := strings.TrimSpace(gbExpr)
		// Check for column number reference (e.g., "1", "2")
		if n, err := strconv.Atoi(trimmed); err == nil {
			if n < 1 || n > len(cols) {
				return nil, NewErrorf(Error, "GROUP BY column number %d is out of range", n)
			}
			// Use the corresponding result column expression
			groupByExprs[i] = cols[n-1].expr
			continue
		}
		// Check for alias reference
		for _, c := range cols {
			if c.as != "" && strings.EqualFold(c.as, trimmed) {
				groupByExprs[i] = c.expr
				break
			}
		}
	}

	// Parse HAVING clause
	var havingExpr string
	if pos < len(filtered) && isKeyword(filtered[pos], "having") {
		pos++
		var havingParts []string
		for pos < len(filtered) &&
			!isKeyword(filtered[pos], "order") &&
			!isKeyword(filtered[pos], "limit") {
			havingParts = append(havingParts, filtered[pos].Value)
			pos++
		}
		havingExpr = strings.Join(havingParts, " ")
	}

	// Parse ORDER BY clause
	var orderByCol string
	var orderDesc bool
	if pos < len(filtered) && isKeyword(filtered[pos], "order") {
		pos++
		if pos < len(filtered) && isKeyword(filtered[pos], "by") {
			pos++
		}
		if pos < len(filtered) {
			obRaw := filtered[pos].Value
			// Resolve column number reference
			if n, err := strconv.Atoi(obRaw); err == nil && n >= 1 && n <= len(cols) {
				orderByCol = strings.ToLower(cols[n-1].expr)
			} else {
				// Check for alias reference
				resolved := false
				for _, c := range cols {
					if c.as != "" && strings.EqualFold(c.as, obRaw) {
						orderByCol = strings.ToLower(c.expr)
						resolved = true
						break
					}
				}
				if !resolved {
					orderByCol = strings.ToLower(obRaw)
				}
			}
			pos++
			if pos < len(filtered) && isKeyword(filtered[pos], "desc") {
				orderDesc = true
				pos++
			} else if pos < len(filtered) && isKeyword(filtered[pos], "asc") {
				pos++
			}
		}
	}

	// Parse LIMIT/OFFSET
	var limitVal int = -1
	var offsetVal int
	if pos < len(filtered) && isKeyword(filtered[pos], "limit") {
		pos++
		if pos < len(filtered) {
			if n, err := strconv.Atoi(filtered[pos].Value); err == nil {
				limitVal = n
			}
			pos++
		}
		// OFFSET or comma form
		if pos < len(filtered) && isKeyword(filtered[pos], "offset") {
			pos++
			if pos < len(filtered) {
				if n, err := strconv.Atoi(filtered[pos].Value); err == nil {
					offsetVal = n
				}
				pos++
			}
		} else if pos < len(filtered) && filtered[pos].Type == compile.TokenComma {
			// LIMIT offset, count (SQLite alternative syntax)
			pos++
			if pos < len(filtered) {
				if n, err := strconv.Atoi(filtered[pos].Value); err == nil {
					offsetVal = limitVal
					limitVal = n
				}
				pos++
			}
		}
	}

	// Determine output columns
	var resultCols []ResultColumnInfo
	for _, c := range cols {
		if c.expr == "*" {
			for _, col := range ecols {
				resultCols = append(resultCols, ResultColumnInfo{
					Name: col.name,
					Type: ColNull,
				})
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
		}
	}

	// Detect aggregate functions in column expressions
	hasAgg := false
	for _, c := range cols {
		if isAggregateExpr(c.expr) {
			hasAgg = true
			break
		}
	}

	// Build column name list for row context
	colNames := make([]string, len(ecols))
	for i, col := range ecols {
		colNames[i] = col.name
	}

	// Scan rows from either btree or in-memory CTE data
	var rawData []rawRow

	if cteTbl != nil {
		// Scan CTE rows from memory
		for _, rd := range cteTbl.rows {
			if whereExpr != "" {
				wval := evalExprWithRow(whereExpr, args, colNames, rd.values)
				if !isTruthy(wval) {
					continue
				}
			}
			rawData = append(rawData, rd)
		}
	} else {
		// Scan the table via btree cursor
		cursor, err := db.bt.Cursor(btree.PageNumber(tbl.rootPage), false)
		if err != nil {
			return nil, NewErrorf(Error, "open cursor: %s", err)
		}
		defer cursor.Close()

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

			// WHERE filtering
			if whereExpr != "" {
				wval := evalExprWithRow(whereExpr, args, colNames, values)
				if !isTruthy(wval) {
					hasRow, err = cursor.Next()
					if err != nil {
						return nil, err
					}
					continue
				}
			}

			// Pad values with defaults for columns added via ALTER TABLE
			if len(values) < len(ecols) {
				padded := make([]vdbe.Value, len(ecols))
				copy(padded, values)
				for j := len(values); j < len(ecols); j++ {
					if ecols[j].defaultValue != nil {
						padded[j] = interfaceToValue(ecols[j].defaultValue)
					} else {
						padded[j] = vdbe.Value{Type: "null"}
					}
				}
				values = padded
			}
			rawData = append(rawData, rawRow{values: values})
			hasRow, err = cursor.Next()
			if err != nil {
				return nil, err
			}
		}
	}

	// Apply ORDER BY sorting on raw data
	if orderByCol != "" {
		orderIdx := -1
		for i, name := range colNames {
			if strings.ToLower(name) == orderByCol {
				orderIdx = i
				break
			}
		}
		if orderIdx >= 0 {
			sort.SliceStable(rawData, func(i, j int) bool {
				vi := rawData[i].values[orderIdx]
				vj := rawData[j].values[orderIdx]
				// NULL sorts first (smallest) in ASC order
				if vi.Type == "null" && vj.Type == "null" {
					return false
				}
				if vi.Type == "null" {
					return !orderDesc
				}
				if vj.Type == "null" {
					return orderDesc
				}
				// Non-null comparison
				mi := vdbe.MemFromValue(vi)
				mj := vdbe.MemFromValue(vj)
				cmp := memCompare(mi, mj)
				if orderDesc {
					return cmp > 0
				}
				return cmp < 0
			})
		}
	}

	var rows []Row

	// Check if any column expression uses a window function
	hasWin := false
	for _, c := range cols {
		if isWindowExpr(c.expr) {
			hasWin = true
		}
	}

	if hasWin {
		rows = db.computeWindowFunctions(cols, rawData, colNames, resultCols, ecols, args)
	} else if len(groupByExprs) > 0 {
		// GROUP BY: group rows (with or without aggregate functions)
		type group struct {
			key    string
			rows   []rawRow
			sample rawRow
		}
		var groups []*group
		groupIndex := make(map[string]*group)

		for _, rd := range rawData {
			var keyParts []string
			for _, gbExpr := range groupByExprs {
				val := evalExprWithRow(gbExpr, args, colNames, rd.values)
				if isNull(val) {
					keyParts = append(keyParts, "\x00NULL")
				} else {
					keyParts = append(keyParts, "\x01"+val.StringValue())
				}
			}
			key := strings.Join(keyParts, "\x00")

			if g, ok := groupIndex[key]; ok {
				g.rows = append(g.rows, rd)
			} else {
				g := &group{key: key, rows: []rawRow{rd}, sample: rd}
				groupIndex[key] = g
				groups = append(groups, g)
			}
		}

		for _, g := range groups {
			row := Row{cols: resultCols}
			for _, c := range cols {
				if c.expr == "*" {
					row.values = append(row.values, vdbe.NewMemInt(int64(len(g.rows))))
					continue
				}
				// Check if expression contains an aggregate function
				agg := parseAggregate(c.expr)
				if agg != nil {
					// Pure aggregate expression (e.g., "avg(n)")
					row.values = append(row.values, computeAggregate(agg, g.rows, colNames))
				} else if containsAggregate(c.expr) {
					// Expression containing aggregate (e.g., "avg(n)+1", "avg(n)-min(n)")
					evaluated := evaluateAggregateExpr(c.expr, g.rows, colNames)
					row.values = append(row.values, evaluated)
				} else {
					row.values = append(row.values, evalExprWithRow(c.expr, args, colNames, g.sample.values))
				}
			}

			// Apply HAVING filter
			if havingExpr != "" {
				// Replace aggregate expressions and their aliases in HAVING
				havingEval := havingExpr
				for i, c := range cols {
					if i >= len(row.values) {
						continue
					}
					val := row.values[i]
					if isNull(val) {
						continue
					}
					// Replace alias reference (e.g., "y" in "y>=4")
					if c.as != "" {
						havingEval = strings.ReplaceAll(havingEval, c.as, val.StringValue())
					}
					// Replace the aggregate expression itself
					agg := parseAggregate(c.expr)
					if agg != nil {
						havingEval = strings.ReplaceAll(havingEval, c.expr, val.StringValue())
					}
				}
				// Replace any remaining aggregate expressions in HAVING
				// that are not in the SELECT list (e.g., "count(*)>=4" in HAVING)
				havingEval = db.replaceAggregatesInExpr(havingEval, g.rows, colNames)
				hval := evalExprWithRow(havingEval, args, colNames, g.sample.values)
				if !isTruthy(hval) {
					continue
				}
			}

			rows = append(rows, row)
		}
		if rows == nil {
			rows = []Row{}
		}
	} else if hasAgg {
		// Compute aggregate results (no GROUP BY)
		row := Row{cols: resultCols}
		for _, c := range cols {
			if c.expr == "*" {
				// count(*) aggregate without GROUP BY
				row.values = append(row.values, vdbe.NewMemInt(int64(len(rawData))))
				continue
			}
			agg := parseAggregate(c.expr)
			if agg != nil {
				row.values = append(row.values, computeAggregate(agg, rawData, colNames))
			} else {
				if len(rawData) > 0 {
					row.values = append(row.values, evalExprWithRow(c.expr, args, colNames, rawData[0].values))
				} else {
					row.values = append(row.values, vdbe.NewMemNull())
				}
			}
		}
		rows = []Row{row}
	} else {
		// Non-aggregate: evaluate expressions per row
		seen := make(map[string]bool)
		for _, rd := range rawData {
			row := Row{cols: resultCols}
			for _, c := range cols {
				if c.expr == "*" {
					for i := range ecols {
						if i < len(rd.values) {
							row.values = append(row.values, vdbe.MemFromValue(rd.values[i]))
						} else if ecols[i].defaultValue != nil {
							row.values = append(row.values, interfaceToMem(ecols[i].defaultValue))
						} else {
							row.values = append(row.values, vdbe.NewMemNull())
						}
					}
				} else {
					val := evalExprWithRow(c.expr, args, colNames, rd.values)
					row.values = append(row.values, val)
				}
			}
			if distinct {
				key := rowKey(row.values)
				if seen[key] {
					continue
				}
				seen[key] = true
			}
			rows = append(rows, row)
		}
	}

	if rows == nil {
		rows = []Row{}
	}

	// Apply LIMIT/OFFSET
	if limitVal >= 0 {
		if offsetVal > 0 {
			if offsetVal < len(rows) {
				rows = rows[offsetVal:]
			} else {
				rows = []Row{}
			}
		}
		if limitVal < len(rows) {
			rows = rows[:limitVal]
		}
	}

	hackRS := newResultSet(rows, resultCols)

	// If compile pipeline succeeded and results match, use compile result
	if compileErr == nil && compileRS != nil {
		cRows := compileRS.Rows()
		hRows := hackRS.Rows()
		if len(cRows) == len(hRows) {
			match := true
			for i := range cRows {
				if cRows[i].ColumnCount() != hRows[i].ColumnCount() {
					match = false
					break
				}
				for j := 0; j < cRows[i].ColumnCount(); j++ {
					if cRows[i].ColumnValue(j) != hRows[i].ColumnValue(j) {
						match = false
						break
					}
				}
				if !match {
					break
				}
			}
			if match {
				return compileRS, nil
			}
		}
	}

	return hackRS, nil
	}

	// queryWithCTE handles queries starting with WITH (common table expressions).
	func (db *Database) queryWithCTE(sql string, args []interface{}) (*ResultSet, error) {
		// Save and restore CTE data for proper scoping
	oldCTEData := db.cteData
	db.cteData = make(map[string]*cteTable)
	defer func() { db.cteData = oldCTEData }()

	tokens := compile.Tokenize(sql)
	filtered := filterTokens(tokens)

	pos := 1 // skip WITH

	// Check RECURSIVE keyword
	recursive := false
	if pos < len(filtered) && isKeyword(filtered[pos], "recursive") {
		recursive = true
		pos++
	}

	// CTE definition intermediate struct
	type cteDef struct {
		name    string
		columns []string
		bodySQL string
	}
	var ctes []cteDef

	// Parse all CTE definitions
	for pos < len(filtered) {
		// Stop when we hit the main statement
		if isKeyword(filtered[pos], "select") || isKeyword(filtered[pos], "insert") ||
			isKeyword(filtered[pos], "update") || isKeyword(filtered[pos], "delete") {
			break
		}
		if pos >= len(filtered) || filtered[pos].Type == compile.TokenComma {
			pos++
			continue
		}

		// CTE name
		cteName := filtered[pos].Value
		pos++

		// Optional column list: (col1, col2, ...)
		var cteCols []string
		if pos < len(filtered) && filtered[pos].Type == compile.TokenLParen {
			// Check if this is a column list (not a body starting with SELECT)
			if pos+1 < len(filtered) && !isKeyword(filtered[pos+1], "select") {
				pos++ // skip (
				for pos < len(filtered) && filtered[pos].Type != compile.TokenRParen {
					if filtered[pos].Type == compile.TokenID || filtered[pos].Type == compile.TokenKeyword {
						cteCols = append(cteCols, filtered[pos].Value)
					}
					pos++
				}
				if pos < len(filtered) {
					pos++ // skip )
				}
			}
		}

		// AS keyword
		if pos < len(filtered) && isKeyword(filtered[pos], "as") {
			pos++
		}

		// Body in parentheses: (SELECT ...)
		if pos < len(filtered) && filtered[pos].Type == compile.TokenLParen {
			pos++ // skip (
			depth := 1
			var bodyTokens []compile.Token
			for pos < len(filtered) && depth > 0 {
				if filtered[pos].Type == compile.TokenLParen {
					depth++
				}
				if filtered[pos].Type == compile.TokenRParen {
					depth--
					if depth == 0 {
						pos++ // skip closing )
						break
					}
				}
				bodyTokens = append(bodyTokens, filtered[pos])
				pos++
			}
			bodySQL := joinTokenValues(bodyTokens)
			ctes = append(ctes, cteDef{name: cteName, columns: cteCols, bodySQL: bodySQL})
		}

		// Comma between CTEs
		if pos < len(filtered) && filtered[pos].Type == compile.TokenComma {
			pos++
		}
	}

	// Evaluate each CTE
	for _, cte := range ctes {
		if recursive {
			anchorSQL, recSQL := splitUnionAll(cte.bodySQL)

			// Execute anchor query
			rs, err := db.querySingle(anchorSQL, args)
			if err != nil {
				return nil, err
			}
			ct := rsToCTE(rs, cte.columns)
			rs.Close()

			allRows := make([]rawRow, len(ct.rows))
			copy(allRows, ct.rows)
			workingSet := ct.rows

			if recSQL != "" && len(workingSet) > 0 {
				for {
					// Set CTE data to working set for recursive reference
					db.cteData[cte.name] = &cteTable{
						columns: ct.columns,
						rows:    workingSet,
					}

					rs, err := db.querySingle(recSQL, args)
					if err != nil {
						return nil, err
					}
					newCT := rsToCTE(rs, cte.columns)
					rs.Close()

					if len(newCT.rows) == 0 {
						break
					}

					allRows = append(allRows, newCT.rows...)
					workingSet = newCT.rows
				}
			}

			// Set final CTE data with all accumulated rows
			db.cteData[cte.name] = &cteTable{
				columns: ct.columns,
				rows:    allRows,
			}
		} else {
			// Non-recursive: execute the body
			rs, err := db.querySingle(cte.bodySQL, args)
			if err != nil {
				return nil, err
			}
			db.cteData[cte.name] = rsToCTE(rs, cte.columns)
			rs.Close()
		}
	}

	// Build main SELECT SQL from remaining tokens
	var mainParts []string
	for pos < len(filtered) {
		mainParts = append(mainParts, filtered[pos].Value)
		pos++
	}
	mainSQL := strings.Join(mainParts, " ")

	return db.querySingle(mainSQL, args)
}

// splitUnionAll splits a SQL body at UNION ALL into anchor and recursive parts.
func splitUnionAll(bodySQL string) (string, string) {
	tokens := compile.Tokenize(bodySQL)
	filtered := filterTokens(tokens)

	for i := 0; i < len(filtered)-1; i++ {
		if isKeyword(filtered[i], "union") && isKeyword(filtered[i+1], "all") {
			anchor := joinTokenValues(filtered[:i])
			recursive := joinTokenValues(filtered[i+2:])
			return anchor, recursive
		}
		if isKeyword(filtered[i], "union") {
			anchor := joinTokenValues(filtered[:i])
			recursive := joinTokenValues(filtered[i+1:])
			return anchor, recursive
		}
	}

	return bodySQL, ""
}

// compoundPart represents one part of a compound SELECT.
// op is the operator that FOLLOWS this part (empty for the last part).
type compoundPart struct {
	sql string           // The SQL of this sub-select
	op  compoundSelectOp // Operator after this part
}

// compoundSelectOp identifies the type of compound operator.
type compoundSelectOp int

const (
	compoundNone      compoundSelectOp = iota
	compoundUnion                      // UNION
	compoundUnionAll                   // UNION ALL
	compoundIntersect                  // INTERSECT
	compoundExcept                     // EXCEPT
)

// splitCompoundSelect splits a filtered token list at top-level
// UNION / UNION ALL / INTERSECT / EXCEPT boundaries, respecting
// parenthesization (subqueries, expressions).
// Returns nil if no compound operators found.
func splitCompoundSelect(filtered []compile.Token) []compoundPart {
	var parts []compoundPart
	parenDepth := 0
	start := 0

	for i := 0; i < len(filtered); i++ {
		tok := filtered[i]
		switch tok.Type {
		case compile.TokenLParen:
			parenDepth++
		case compile.TokenRParen:
			if parenDepth > 0 {
				parenDepth--
			}
		case compile.TokenKeyword:
			if parenDepth == 0 {
				var op compoundSelectOp
				var skip int

				if strings.EqualFold(tok.Value, "union") && i+1 < len(filtered) && isKeyword(filtered[i+1], "all") {
					op = compoundUnionAll
					skip = 2
				} else if strings.EqualFold(tok.Value, "union") {
					op = compoundUnion
					skip = 1
				} else if strings.EqualFold(tok.Value, "intersect") {
					op = compoundIntersect
					skip = 1
				} else if strings.EqualFold(tok.Value, "except") {
					op = compoundExcept
					skip = 1
			}

				if op != compoundNone {
					parts = append(parts, compoundPart{
						sql: joinTokenValues(filtered[start:i]),
						op:  op,
					})
					i += skip - 1 // -1 because loop will i++
					start = i + 1
				}
			}
		}
	}

	if len(parts) == 0 {
		return nil
	}

	// Add the last part (no operator follows it)
	parts = append(parts, compoundPart{
		sql: joinTokenValues(filtered[start:]),
		op:  compoundNone,
	})
	return parts
}

// queryCompoundSelect executes a compound SELECT (UNION/INTERSECT/EXCEPT).
func (db *Database) queryCompoundSelect(parts []compoundPart, args []interface{}) (*ResultSet, error) {
	if len(parts) == 0 {
		return nil, NewError(Error, "empty compound select")
	}

	// Execute the first sub-select
	rs, err := db.querySingle(parts[0].sql, args)
	if err != nil {
		return nil, err
	}
	currentRows := collectResultRows(rs)
	currentCols := rs.ColumnNames()
	rs.Close()

	// Apply each compound operator in order
	for i := 1; i < len(parts); i++ {
		op := parts[i-1].op

		rs, err := db.querySingle(parts[i].sql, args)
		if err != nil {
			return nil, err
		}
		nextRows := collectResultRows(rs)
		// Column names come from the leftmost SELECT, so we ignore
		// the right side's column names.
		rs.Close()

		switch op {
		case compoundUnionAll:
			currentRows = append(currentRows, nextRows...)
		case compoundUnion:
			currentRows = append(currentRows, nextRows...)
			currentRows = deduplicateRows(currentRows)
		case compoundIntersect:
			currentRows = intersectRows(currentRows, nextRows)
		case compoundExcept:
			currentRows = exceptRows(currentRows, nextRows)
		}
	}

	// Build result set
	var colInfos []ResultColumnInfo
	for _, name := range currentCols {
		colInfos = append(colInfos, ResultColumnInfo{Name: name})
	}

	var rows []Row
	for _, vals := range currentRows {
		row := Row{values: vals, cols: colInfos}
		rows = append(rows, row)
	}

	return newResultSet(rows, colInfos), nil
}

// collectResultRows extracts row values from a ResultSet.
func collectResultRows(rs *ResultSet) [][]*vdbe.Mem {
	var rows [][]*vdbe.Mem
	for rs.Next() {
		r := rs.Row()
		vals := make([]*vdbe.Mem, r.ColumnCount())
		for i := 0; i < r.ColumnCount(); i++ {
			vals[i] = valueToMem(r.ColumnValue(i))
		}
		rows = append(rows, vals)
	}
	return rows
}

// valueToMem converts an interface{} value back to a *vdbe.Mem.
func valueToMem(v interface{}) *vdbe.Mem {
	switch val := v.(type) {
	case nil:
		return vdbe.NewMemNull()
	case int64:
		return vdbe.NewMemInt(val)
	case float64:
		return vdbe.NewMemFloat(val)
	case string:
		return vdbe.NewMemStr(val)
	case []byte:
		return vdbe.NewMemBlob(val)
	default:
		return vdbe.NewMemNull()
	}
}

// deduplicateRows removes duplicate rows (for UNION).
func deduplicateRows(rows [][]*vdbe.Mem) [][]*vdbe.Mem {
	seen := make(map[string]bool)
	var result [][]*vdbe.Mem
	for _, row := range rows {
		key := rowKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

// intersectRows returns rows present in BOTH a and b (for INTERSECT).
func intersectRows(a, b [][]*vdbe.Mem) [][]*vdbe.Mem {
	bSet := make(map[string]bool)
	for _, row := range b {
		bSet[rowKey(row)] = true
	}
	seen := make(map[string]bool)
	var result [][]*vdbe.Mem
	for _, row := range a {
		key := rowKey(row)
		if bSet[key] && !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

// exceptRows returns rows in a but NOT in b (for EXCEPT).
func exceptRows(a, b [][]*vdbe.Mem) [][]*vdbe.Mem {
	bSet := make(map[string]bool)
	for _, row := range b {
		bSet[rowKey(row)] = true
	}
	var result [][]*vdbe.Mem
	for _, row := range a {
		if !bSet[rowKey(row)] {
			result = append(result, row)
		}
	}
	return result
}

// rsToCTE converts a ResultSet to a cteTable for CTE data storage.
func rsToCTE(rs *ResultSet, cteColNames []string) *cteTable {
	var cols []columnEntry
	var rows []rawRow

	for rs.Next() {
		row := rs.Row()
		if cols == nil {
			if len(cteColNames) > 0 {
				for _, name := range cteColNames {
					cols = append(cols, columnEntry{name: name})
				}
			} else {
				for _, ci := range row.cols {
					cols = append(cols, columnEntry{name: ci.Name})
				}
			}
		}
		var vals []vdbe.Value
		for _, v := range row.values {
			vals = append(vals, memToValue(v))
		}
		rows = append(rows, rawRow{values: vals})
	}

	// Handle empty result set with explicit column names
	if cols == nil && len(cteColNames) > 0 {
		for _, name := range cteColNames {
			cols = append(cols, columnEntry{name: name})
		}
	}

	return &cteTable{columns: cols, rows: rows}
}

// isAggregateExpr checks if an expression is an aggregate function call.
func isAggregateExpr(expr string) bool {
	lower := strings.ToLower(strings.TrimSpace(expr))
	// Normalize spaces between function name and paren: "count (" -> "count("
	lower = strings.ReplaceAll(lower, " (", "(")
	for _, fn := range []string{"count(", "sum(", "avg(", "min(", "max("} {
		if strings.HasPrefix(lower, fn) {
			return true
		}
	}
	return false
}

// rawRow holds parsed record values for table scanning.
type rawRow struct {
	values []vdbe.Value
}

// cteTable holds materialized CTE results for query execution.
type cteTable struct {
	columns []columnEntry
	rows    []rawRow
}

// aggInfo describes a parsed aggregate function.
type aggInfo struct {
	fn  string // "count", "sum", "avg", "min", "max"
	arg string // "*" or column name or expression
}

// parseAggregate parses a simple aggregate expression like "count(*)" or "sum(val)".
// Returns nil for compound expressions like "avg(n)+1".
func parseAggregate(expr string) *aggInfo {
	// Normalize spaces: "count ( * )" -> "count(*)"
	normalized := strings.ReplaceAll(expr, " (", "(")
	normalized = strings.ReplaceAll(normalized, "( ", "(")
	normalized = strings.ReplaceAll(normalized, " )", ")")
	normalized = strings.TrimSpace(normalized)
	lower := strings.ToLower(normalized)
	for _, fn := range []string{"count", "sum", "avg", "min", "max"} {
		prefix := fn + "("
		if !strings.HasPrefix(lower, prefix) {
			continue
		}
		// Find the matching closing paren for the function call
		depth := 1
		end := -1
		for i := len(prefix); i < len(normalized); i++ {
			if normalized[i] == '(' {
				depth++
			} else if normalized[i] == ')' {
				depth--
				if depth == 0 {
					end = i
					break
				}
			}
		}
		// Only match if the closing paren is at the end (simple aggregate, not compound)
		if end != len(normalized)-1 {
			continue
		}
		inner := strings.TrimSpace(normalized[len(prefix) : len(normalized)-1])
		return &aggInfo{fn: fn, arg: inner}
	}
	return nil
}

// computeAggregate computes an aggregate over all rows.
func computeAggregate(agg *aggInfo, rawData []rawRow, colNames []string) *vdbe.Mem {
	switch agg.fn {
	case "count":
		if agg.arg == "*" {
			return vdbe.NewMemInt(int64(len(rawData)))
		}
		count := int64(0)
		for _, rd := range rawData {
			val := evalExprWithRow(agg.arg, nil, colNames, rd.values)
			if !isNull(val) {
				count++
			}
		}
		return vdbe.NewMemInt(count)

	case "sum":
		var sum int64
		hasInt := true
		var sumF float64
		hasVal := false
		for _, rd := range rawData {
			val := evalExprWithRow(agg.arg, nil, colNames, rd.values)
			if isNull(val) {
				continue
			}
			hasVal = true
			if val.Type == vdbe.MemFloat {
				hasInt = false
			}
			if val.Type == vdbe.MemInt {
				sum += val.IntVal
				sumF += float64(val.IntVal)
			} else {
				sumF += val.FloatValue()
			}
		}
		if !hasVal {
			return vdbe.NewMemNull()
		}
		if hasInt {
			return vdbe.NewMemInt(sum)
		}
		return vdbe.NewMemFloat(sumF)

	case "avg":
		var sum float64
		count := int64(0)
		for _, rd := range rawData {
			val := evalExprWithRow(agg.arg, nil, colNames, rd.values)
			if isNull(val) {
				continue
			}
			sum += val.FloatValue()
			count++
		}
		if count == 0 {
			return vdbe.NewMemNull()
		}
		return vdbe.NewMemFloat(sum / float64(count))

	case "min":
		var result *vdbe.Mem
		for _, rd := range rawData {
			val := evalExprWithRow(agg.arg, nil, colNames, rd.values)
			if isNull(val) {
				continue
			}
			if result == nil || memCompare(val, result) < 0 {
				result = val
			}
		}
		if result == nil {
			return vdbe.NewMemNull()
		}
		return result

	case "max":
		var result *vdbe.Mem
		for _, rd := range rawData {
			val := evalExprWithRow(agg.arg, nil, colNames, rd.values)
			if isNull(val) {
				continue
			}
			if result == nil || memCompare(val, result) > 0 {
				result = val
			}
		}
		if result == nil {
			return vdbe.NewMemNull()
		}
		return result
	}
	return vdbe.NewMemNull()
}

// evaluateAggregateExpr evaluates an expression containing aggregate functions.
// It normalizes spaces (e.g. "avg ( n )" → "avg(n)"), replaces each aggregate
// call with its computed value, then evaluates the resulting expression.
func evaluateAggregateExpr(expr string, rawData []rawRow, colNames []string) *vdbe.Mem {
	// Normalize: remove spaces around parens so "avg ( n )" becomes "avg(n)"
	normalized := strings.ReplaceAll(expr, " (", "(")
	normalized = strings.ReplaceAll(normalized, "( ", "(")
	normalized = strings.ReplaceAll(normalized, " )", ")")

	type aggCall struct {
		start int
		end   int
		fn    string
		arg   string
	}
	var calls []aggCall

	lower := strings.ToLower(normalized)
	for _, fn := range []string{"count", "sum", "avg", "min", "max", "group_concat"} {
		pattern := fn + "("
		offset := 0
		for {
			idx := strings.Index(lower[offset:], pattern)
			if idx < 0 {
				break
			}
			pos := offset + idx
			parenStart := pos + len(pattern)
			depth := 1
			end := -1
			for i := parenStart; i < len(normalized); i++ {
				if normalized[i] == '(' {
					depth++
				} else if normalized[i] == ')' {
					depth--
					if depth == 0 {
						end = i
						break
					}
				}
			}
			if end < 0 {
				break
			}
			calls = append(calls, aggCall{start: pos, end: end + 1, fn: fn, arg: strings.TrimSpace(normalized[parenStart:end])})
			offset = end + 1
		}
	}

	if len(calls) == 0 {
		return vdbe.NewMemNull()
	}

	// Replace aggregate calls from right to left (to preserve positions)
	// Skip calls that are contained within other calls (nested aggregates)
	result := normalized
	for i := len(calls) - 1; i >= 0; i-- {
		c := calls[i]
		// Check if this call is contained within a later (outer) call
		isNested := false
		for j := i + 1; j < len(calls); j++ {
			if calls[j].start <= c.start && calls[j].end >= c.end {
				isNested = true
				break
			}
		}
		if isNested {
			continue
		}
		agg := &aggInfo{fn: c.fn, arg: c.arg}
		val := computeAggregate(agg, rawData, colNames)
		var rep string
		if isNull(val) {
			rep = "NULL"
		} else {
			rep = fmt.Sprintf("%v", val.FloatValue())
		}
		result = result[:c.start] + rep + result[c.end:]
	}

	return evalExprWithRow(result, nil, colNames, nil)
}

func containsAggregate(expr string) bool {
	lower := strings.ToLower(strings.ReplaceAll(expr, " (", "("))
	for _, fn := range []string{"count(", "sum(", "avg(", "min(", "max("} {
		if strings.Contains(lower, fn) {
			return true
		}
	}
	return false
}

// replaceAggregatesInExpr finds all aggregate function calls in the expression,
// computes each one over the given rows, and replaces them with their string values.
func (db *Database) replaceAggregatesInExpr(expr string, rows []rawRow, colNames []string) string {
	// Normalize the expression first: remove spaces around parens
	normalized := strings.ReplaceAll(expr, " (", "(")
	normalized = strings.ReplaceAll(normalized, "( ", "(")
	normalized = strings.ReplaceAll(normalized, " )", ")")
	result := normalized
	for {
		lower := strings.ToLower(result)
		bestIdx := len(lower)
		bestFn := ""
		for _, fn := range []string{"count(", "sum(", "avg(", "min(", "max("} {
			idx := strings.Index(lower, fn)
			if idx >= 0 && idx < bestIdx {
				bestIdx = idx
				bestFn = fn
			}
		}
		if bestFn == "" {
			break
		}
		// Find matching closing paren
		depth := 1
		end := -1
		for j := bestIdx + len(bestFn); j < len(result); j++ {
			if result[j] == '(' {
				depth++
			} else if result[j] == ')' {
				depth--
				if depth == 0 {
					end = j
					break
				}
			}
		}
		if end < 0 {
			break
		}
		aggStr := result[bestIdx : end+1]
		agg := parseAggregate(aggStr)
		if agg == nil {
			break
		}
		val := computeAggregate(agg, rows, colNames)
		var replacement string
		if isNull(val) {
			replacement = "null"
		} else {
			replacement = val.StringValue()
		}
		result = result[:bestIdx] + replacement + result[end+1:]
	}
	return result
}

// evalAggregateExpr evaluates a column expression that may contain aggregate functions.
// Handles simple aggregates like "count(*)", compound expressions like "avg(n)+1",
// and non-aggregate expressions like "log".
func (db *Database) evalAggregateExpr(expr string, rows []rawRow, colNames []string, sample rawRow, args []interface{}) *vdbe.Mem {
	// Simple aggregate
	agg := parseAggregate(expr)
	if agg != nil {
		return computeAggregate(agg, rows, colNames)
	}
	// Non-aggregate expression
	if !containsAggregate(expr) {
		return evalExprWithRow(expr, args, colNames, sample.values)
	}
	// Compound expression with aggregates: replace aggregates with values, then evaluate
	replaced := db.replaceAggregatesInExpr(expr, rows, colNames)
	return evalExprWithRow(replaced, args, colNames, sample.values)
}

// rowKey creates a string key for DISTINCT deduplication.
func rowKey(values []*vdbe.Mem) string {
	var sb strings.Builder
	for i, v := range values {
		if i > 0 {
			sb.WriteByte(0)
		}
		if isNull(v) {
			sb.WriteString("\\N")
		} else {
			sb.WriteString(v.StringValue())
		}
	}
	return sb.String()
}

// queryView executes a SELECT by expanding a view definition.
func (db *Database) queryView(v *viewEntry, cols []selectCol, args []interface{}) (*ResultSet, error) {
	// Re-execute the view's SELECT statement
	return db.querySingle(v.sql, args)
}

// joinTableInfo describes a table participating in a join.
type joinTableInfo struct {
	name       string
	alias      string
	columns    []columnEntry
	rows       []rawRow
	joinType   string // "inner", "left", "cross"
	onExpr     string // ON condition expression
	natural    bool
	usingCols  []string
	isOuter    bool // true for LEFT JOIN (right table)
}

// parseFromTables parses the FROM clause extracting all tables and join info.
func (db *Database) parseFromTables(filtered []compile.Token, pos int) ([]joinTableInfo, bool, int) {
	var tables []joinTableInfo
	hasJoin := false

	// Parse first table
	if pos >= len(filtered) {
		return nil, false, pos
	}

	firstName := filtered[pos].Value
	pos++
	firstAlias := firstName
	// Check for alias (AS keyword or another identifier before comma/join)
	if pos < len(filtered) && isKeyword(filtered[pos], "as") {
		pos++
		if pos < len(filtered) {
			firstAlias = filtered[pos].Value
			pos++
		}
	}

	tables = append(tables, joinTableInfo{name: firstName, alias: firstAlias})

	// Parse additional tables with join syntax
	for pos < len(filtered) {
		// Check for comma or JOIN keyword
		if filtered[pos].Type == compile.TokenComma {
			pos++
			hasJoin = true
			// Parse table name
			if pos >= len(filtered) {
				break
			}
			tName := filtered[pos].Value
			pos++
			tAlias := tName
			if pos < len(filtered) && isKeyword(filtered[pos], "as") {
				pos++
				if pos < len(filtered) {
					tAlias = filtered[pos].Value
					pos++
				}
			}
			tables = append(tables, joinTableInfo{
				name: tName, alias: tAlias, joinType: "inner",
			})
			// Check for ON (comma join with ON: t1, t2 ON ...)
			if pos < len(filtered) && isKeyword(filtered[pos], "on") {
				pos++
				onExpr, newPos := collectExprTokens(filtered, pos)
				pos = newPos
				if len(tables) > 0 {
					tables[len(tables)-1].onExpr = onExpr
				}
			}
			continue
		}

		// Check for NATURAL
		isNatural := false
		if pos < len(filtered) && isKeyword(filtered[pos], "natural") {
			isNatural = true
			pos++
		}

		// Check for optional OUTER prefix (e.g., "OUTER LEFT JOIN")
		if pos < len(filtered) && isKeyword(filtered[pos], "outer") {
			pos++
		}

		// Check for join type keywords
		joinType := "inner"
		isOuter := false
		if pos < len(filtered) && isKeyword(filtered[pos], "left") {
			joinType = "left"
			isOuter = true
			pos++
			if pos < len(filtered) && isKeyword(filtered[pos], "outer") {
				pos++
			}
		} else if pos < len(filtered) && isKeyword(filtered[pos], "right") {
			joinType = "right"
			pos++
			if pos < len(filtered) && isKeyword(filtered[pos], "outer") {
				pos++
			}
		} else if pos < len(filtered) && isKeyword(filtered[pos], "full") {
			joinType = "full"
			pos++
			if pos < len(filtered) && isKeyword(filtered[pos], "outer") {
				pos++
			}
		} else if pos < len(filtered) && isKeyword(filtered[pos], "cross") {
			joinType = "cross"
			pos++
		} else if pos < len(filtered) && isKeyword(filtered[pos], "inner") {
			pos++
		}

		// Check for NATURAL after join type (e.g., "LEFT NATURAL JOIN")
		if !isNatural && pos < len(filtered) && isKeyword(filtered[pos], "natural") {
			isNatural = true
			pos++
		}

		// Expect JOIN keyword
		if pos < len(filtered) && isKeyword(filtered[pos], "join") {
			pos++
			hasJoin = true
		} else if isNatural {
			hasJoin = true
		} else {
			// Not a join keyword - stop parsing
			if isNatural || joinType != "inner" {
				// consumed keywords but no JOIN - back up
			}
			break
		}

		// Parse table name
		if pos >= len(filtered) {
			break
		}
		tName := filtered[pos].Value
		pos++
		tAlias := tName
		if pos < len(filtered) && isKeyword(filtered[pos], "as") {
			pos++
			if pos < len(filtered) {
				tAlias = filtered[pos].Value
				pos++
			}
		}

		jt := joinTableInfo{
			name:     tName,
			alias:    tAlias,
			joinType: joinType,
			natural:  isNatural,
			isOuter:  isOuter,
		}

		// Parse ON or USING
		if pos < len(filtered) && isKeyword(filtered[pos], "on") {
			pos++
			onExpr, newPos := collectExprTokens(filtered, pos)
			pos = newPos
			jt.onExpr = onExpr
		} else if pos < len(filtered) && isKeyword(filtered[pos], "using") {
			pos++
			if pos < len(filtered) && filtered[pos].Type == compile.TokenLParen {
				pos++
				for pos < len(filtered) && filtered[pos].Type != compile.TokenRParen {
					if filtered[pos].Type != compile.TokenComma {
						jt.usingCols = append(jt.usingCols, filtered[pos].Value)
					}
					pos++
				}
				if pos < len(filtered) {
					pos++ // skip ')'
				}
			}
		}

		tables = append(tables, jt)
	}

	return tables, hasJoin, pos
}

// collectExprTokens collects expression tokens until a clause boundary keyword.
func collectExprTokens(filtered []compile.Token, pos int) (string, int) {
	var parts []string
	parenDepth := 0
	for pos < len(filtered) &&
		!isKeyword(filtered[pos], "where") &&
		!isKeyword(filtered[pos], "group") &&
		!isKeyword(filtered[pos], "order") &&
		!isKeyword(filtered[pos], "limit") &&
		!isKeyword(filtered[pos], "having") &&
		!isKeyword(filtered[pos], "on") &&
		!isKeyword(filtered[pos], "join") &&
		!isKeyword(filtered[pos], "left") &&
		!isKeyword(filtered[pos], "right") &&
		!isKeyword(filtered[pos], "inner") &&
		!isKeyword(filtered[pos], "cross") &&
		!isKeyword(filtered[pos], "natural") &&
		!isKeyword(filtered[pos], "full") &&
		(parenDepth > 0 || filtered[pos].Type != compile.TokenComma) {
		if filtered[pos].Type == compile.TokenLParen {
			parenDepth++
		}
		if filtered[pos].Type == compile.TokenRParen {
			parenDepth--
		}
		parts = append(parts, filtered[pos].Value)
		pos++
	}
	return strings.Join(parts, " "), pos
}

// queryJoin executes a multi-table join query.
func (db *Database) queryJoin(tables []joinTableInfo, filtered []compile.Token, pos int, cols []selectCol, distinct bool, args []interface{}) (*ResultSet, error) {
	// Load table metadata and rows
	for i := range tables {
		t, ok := db.tables[tables[i].name]
		if !ok {
			return nil, NewErrorf(Error, "no such table: %s", tables[i].name)
		}
		tables[i].columns = t.columns

		// Scan all rows from the table
		cursor, err := db.bt.Cursor(btree.PageNumber(t.rootPage), false)
		if err != nil {
			return nil, NewErrorf(Error, "open cursor for %s: %s", tables[i].name, err)
		}
		hasRow, err := cursor.First()
		if err != nil {
			cursor.Close()
			return nil, err
		}
		for hasRow {
			data, err := cursor.Data()
			if err != nil {
				cursor.Close()
				return nil, err
			}
			values, err := vdbe.ParseRecord(data)
			if err != nil {
				cursor.Close()
				return nil, err
			}
			// Append rowid as the last value
			rowid := cursor.RowID()
			tables[i].rows = append(tables[i].rows, rawRow{values: append(values, vdbe.Value{Type: "int", IntVal: int64(rowid)})})
			hasRow, err = cursor.Next()
			if err != nil {
				cursor.Close()
				return nil, err
			}
		}
		cursor.Close()
	}

	// Build NATURAL join conditions if needed
	for i := 1; i < len(tables); i++ {
		if tables[i].natural && tables[i].onExpr == "" {
			tables[i].usingCols = findCommonColumns(tables, i)
		}
	}

	// Parse WHERE clause
	var whereExpr string
	if pos < len(filtered) && isKeyword(filtered[pos], "where") {
		pos++
		var whereParts []string
		parenDepth := 0
		for pos < len(filtered) &&
			!isKeyword(filtered[pos], "limit") &&
			!isKeyword(filtered[pos], "order") &&
			!isKeyword(filtered[pos], "group") {
			if filtered[pos].Type == compile.TokenLParen {
				parenDepth++
			}
			if filtered[pos].Type == compile.TokenRParen {
				parenDepth--
			}
			whereParts = append(whereParts, filtered[pos].Value)
			pos++
		}
		whereExpr = strings.Join(whereParts, " ")
	}

	// Build combined column context for expression evaluation
	_, _ = buildJoinColumnContext(tables)

	// Determine output columns
	var resultCols []ResultColumnInfo
	var outputExprs []string
	for _, c := range cols {
		if c.expr == "*" {
			// Expand * across all tables, skip USING columns from right tables
			for ti, jt := range tables {
				for _, col := range jt.columns {
					// Skip USING columns from right tables
					if ti > 0 && isUsingCol(tables, ti, col.name) {
						continue
					}
					resultCols = append(resultCols, ResultColumnInfo{
						Name: col.name,
						Type: ColNull,
					})
					// Generate a qualified reference
					outputExprs = append(outputExprs, jt.alias+"."+col.name)
				}
			}
		} else if isTableStar(c.expr) {
			// Expand table.* to all columns from that table
			tblName := extractTableFromStar(c.expr)
			for _, jt := range tables {
				if strings.EqualFold(jt.alias, tblName) || strings.EqualFold(jt.name, tblName) {
					for _, col := range jt.columns {
						resultCols = append(resultCols, ResultColumnInfo{
							Name: col.name,
							Type: ColNull,
						})
						outputExprs = append(outputExprs, jt.alias+"."+col.name)
					}
					break
				}
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
			outputExprs = append(outputExprs, c.expr)
		}
	}

	// Execute nested loop join
	var rows []Row

	// Recursive nested loop
	var joinFunc func(depth int, currentRows []rawRow)
	joinFunc = func(depth int, currentRows []rawRow) {
		if depth == len(tables) {
			// We have a complete row combination
			// Flatten all values with combined context
			var allValues []vdbe.Value
			var rowColNames []string
			var rowTblNames []string
			for i, rr := range currentRows {
				for j, col := range tables[i].columns {
					if j < len(rr.values) {
						allValues = append(allValues, rr.values[j])
						rowColNames = append(rowColNames, col.name)
						rowTblNames = append(rowTblNames, tables[i].alias)
					}
				}
				// Add rowid
				if len(rr.values) > len(tables[i].columns) {
					allValues = append(allValues, rr.values[len(tables[i].columns)])
					rowColNames = append(rowColNames, "rowid")
					rowTblNames = append(rowTblNames, tables[i].alias)
				}
			}

			// Check WHERE condition
			if whereExpr != "" {
				wval := evalJoinExpr(whereExpr, args, rowColNames, rowTblNames, allValues)
				if !isTruthy(wval) {
					return
				}
			}

			// Evaluate output columns
			row := Row{cols: resultCols}
			for _, expr := range outputExprs {
				val := evalJoinExpr(expr, args, rowColNames, rowTblNames, allValues)
				row.values = append(row.values, val)
			}

			if distinct {
				key := rowKey(row.values)
				seen := rowSeenMap()
				if seen[key] {
					return
				}
				seen[key] = true
			}

			rows = append(rows, row)
			return
		}

		// Check if this is a LEFT JOIN with no matches yet
		if tables[depth].isOuter {
			// Try matching rows
			matched := false
			for _, tableRow := range tables[depth].rows {
				// Build combined values so far + this candidate row
				var testValues []vdbe.Value
				var testColNames []string
				var testTblNames []string
				for i, rr := range currentRows {
					for j, col := range tables[i].columns {
						if j < len(rr.values) {
							testValues = append(testValues, rr.values[j])
							testColNames = append(testColNames, col.name)
							testTblNames = append(testTblNames, tables[i].alias)
						}
						if len(rr.values) > len(tables[i].columns) {
							testValues = append(testValues, rr.values[len(tables[i].columns)])
							testColNames = append(testColNames, "rowid")
							testTblNames = append(testTblNames, tables[i].alias)
						}
					}
				}
				for j, col := range tables[depth].columns {
					if j < len(tableRow.values) {
						testValues = append(testValues, tableRow.values[j])
						testColNames = append(testColNames, col.name)
						testTblNames = append(testTblNames, tables[depth].alias)
					}
				}
				if len(tableRow.values) > len(tables[depth].columns) {
					testValues = append(testValues, tableRow.values[len(tables[depth].columns)])
					testColNames = append(testColNames, "rowid")
					testTblNames = append(testTblNames, tables[depth].alias)
				}

				// Check join condition
				condOK := true
				if tables[depth].onExpr != "" {
					cval := evalJoinExpr(tables[depth].onExpr, args, testColNames, testTblNames, testValues)
					if !isTruthy(cval) {
						condOK = false
					}
				}
				if condOK && len(tables[depth].usingCols) > 0 {
					for _, uc := range tables[depth].usingCols {
						cval := evalJoinExpr(tables[depth].alias+"."+uc+" = "+tables[depth-1].alias+"."+uc, args, testColNames, testTblNames, testValues)
						if !isTruthy(cval) {
							condOK = false
							break
						}
					}
				}

				if condOK {
					matched = true
					joinFunc(depth+1, append(currentRows, tableRow))
				}
			}

			if !matched {
				// No match for LEFT JOIN: emit row with NULLs for right table
				nullRow := rawRow{values: make([]vdbe.Value, len(tables[depth].columns)+1)}
				for i := range nullRow.values {
					nullRow.values[i] = vdbe.Value{Type: "null"}
				}
				joinFunc(depth+1, append(currentRows, nullRow))
			}
		} else {
			// INNER join: iterate all rows
			for _, tableRow := range tables[depth].rows {
				// Build combined values so far + this candidate row for condition check
				var testValues []vdbe.Value
				var testColNames []string
				var testTblNames []string
				for i, rr := range currentRows {
					for j, col := range tables[i].columns {
						if j < len(rr.values) {
							testValues = append(testValues, rr.values[j])
							testColNames = append(testColNames, col.name)
							testTblNames = append(testTblNames, tables[i].alias)
						}
						if len(rr.values) > len(tables[i].columns) {
							testValues = append(testValues, rr.values[len(tables[i].columns)])
							testColNames = append(testColNames, "rowid")
							testTblNames = append(testTblNames, tables[i].alias)
						}
					}
				}
				for j, col := range tables[depth].columns {
					if j < len(tableRow.values) {
						testValues = append(testValues, tableRow.values[j])
						testColNames = append(testColNames, col.name)
						testTblNames = append(testTblNames, tables[depth].alias)
					}
				}
				if len(tableRow.values) > len(tables[depth].columns) {
					testValues = append(testValues, tableRow.values[len(tables[depth].columns)])
					testColNames = append(testColNames, "rowid")
					testTblNames = append(testTblNames, tables[depth].alias)
				}

				// Check join condition
				condOK := true
				if tables[depth].onExpr != "" {
					cval := evalJoinExpr(tables[depth].onExpr, args, testColNames, testTblNames, testValues)
					if !isTruthy(cval) {
						condOK = false
					}
				}
				if condOK && len(tables[depth].usingCols) > 0 {
					for _, uc := range tables[depth].usingCols {
						cval := evalJoinExpr(tables[depth].alias+"."+uc+" = "+tables[depth-1].alias+"."+uc, args, testColNames, testTblNames, testValues)
						if !isTruthy(cval) {
							condOK = false
							break
						}
					}
				}

				if condOK {
					joinFunc(depth+1, append(currentRows, tableRow))
				}
			}
		}
	}

	// Start the nested loop from the first table
	for _, firstRow := range tables[0].rows {
		joinFunc(1, []rawRow{firstRow})
	}

	if rows == nil {
		rows = []Row{}
	}
	return newResultSet(rows, resultCols), nil
}

// rowSeenMap returns a per-query dedup map. Uses a package-level var per call.
// This is a simplification; in production, pass the map through.
var rowSeenMap = func() func() map[string]bool {
	return func() map[string]bool { return make(map[string]bool) }
}()

// findCommonColumns finds columns common between tables[0..i-1] and tables[i].
func findCommonColumns(tables []joinTableInfo, idx int) []string {
	rightCols := make(map[string]bool)
	for _, c := range tables[idx].columns {
		rightCols[strings.ToLower(c.name)] = true
	}
	var common []string
	for prev := 0; prev < idx; prev++ {
		for _, c := range tables[prev].columns {
			if rightCols[strings.ToLower(c.name)] {
				common = append(common, c.name)
			}
		}
	}
	return common
}

// isUsingCol checks if a column is in the USING list for a given table.
// isTableStar checks if an expression is a table.* pattern (e.g., "t2 . *").
func isTableStar(expr string) bool {
	// The tokenizer produces "t2 . *" with spaces around dot
	s := strings.ReplaceAll(expr, " ", "")
	return len(s) > 2 && s[len(s)-2] == '.' && s[len(s)-1] == '*'
}

// extractTableFromStar extracts the table name from "table.*" or "table . *".
func extractTableFromStar(expr string) string {
	s := strings.TrimSpace(expr)
	// Remove spaces and trailing .*
	s = strings.ReplaceAll(s, " ", "")
	if strings.HasSuffix(s, ".*") {
		return s[:len(s)-2]
	}
	// Also handle "t2 . *" format
	parts := strings.Fields(expr)
	if len(parts) >= 1 {
		return parts[0]
	}
	return ""
}

func isUsingCol(tables []joinTableInfo, tableIdx int, colName string) bool {
	if len(tables[tableIdx].usingCols) == 0 {
		return false
	}
	for _, uc := range tables[tableIdx].usingCols {
		if strings.EqualFold(uc, colName) {
			return true
		}
	}
	return false
}

// buildJoinColumnContext builds flattened column name and table name arrays.
func buildJoinColumnContext(tables []joinTableInfo) ([]string, []string) {
	var colNames, tblNames []string
	for _, jt := range tables {
		for _, col := range jt.columns {
			colNames = append(colNames, col.name)
			tblNames = append(tblNames, jt.alias)
		}
	}
	return colNames, tblNames
}

// evalJoinExpr evaluates an expression in a join context with qualified column support.
func evalJoinExpr(expr string, args []interface{}, colNames []string, tableNames []string, values []vdbe.Value) *vdbe.Mem {
	p := &joinExprParser{
		src:        strings.TrimSpace(expr),
		args:       args,
		colNames:   colNames,
		tableNames: tableNames,
		values:     values,
	}
	val := p.parseExpr()
	if val != nil {
		return val
	}
	return vdbe.NewMemStr(strings.TrimSpace(expr))
}

// joinExprParser extends exprParser with table-qualified column support.
type joinExprParser struct {
	src        string
	pos        int
	args       []interface{}
	colNames   []string
	tableNames []string
	values     []vdbe.Value
}

func (p *joinExprParser) peek() byte {
	if p.pos >= len(p.src) {
		return 0
	}
	return p.src[p.pos]
}

func (p *joinExprParser) skipSpaces() {
	for p.pos < len(p.src) && p.src[p.pos] == ' ' {
		p.pos++
	}
}

func (p *joinExprParser) remaining() string {
	if p.pos >= len(p.src) {
		return ""
	}
	return p.src[p.pos:]
}

func (p *joinExprParser) matchKeyword(kw string) bool {
	p.skipSpaces()
	rest := p.remaining()
	if len(rest) < len(kw) {
		return false
	}
	if !strings.EqualFold(rest[:len(kw)], kw) {
		return false
	}
	after := len(kw)
	if after < len(rest) {
		c := rest[after]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			return false
		}
	}
	p.pos += len(kw)
	return true
}

func (p *joinExprParser) peekKeyword(kw string) bool {
	saved := p.pos
	result := p.matchKeyword(kw)
	p.pos = saved
	return result
}

func (p *joinExprParser) matchOp(op string) bool {
	p.skipSpaces()
	rest := p.remaining()
	if len(rest) < len(op) {
		return false
	}
	if rest[:len(op)] != op {
		return false
	}
	p.pos += len(op)
	return true
}

func (p *joinExprParser) peekOp(op string) bool {
	saved := p.pos
	result := p.matchOp(op)
	p.pos = saved
	return result
}

func (p *joinExprParser) parseExpr() *vdbe.Mem {
	return p.parseOr()
}

func (p *joinExprParser) parseOr() *vdbe.Mem {
	left := p.parseAnd()
	for {
		p.skipSpaces()
		if p.matchKeyword("or") {
			right := p.parseAnd()
			if isTruthy(left) {
				// skip remaining
				left = vdbe.NewMemInt(1)
			} else if isTruthy(right) {
				left = vdbe.NewMemInt(1)
			} else {
				left = vdbe.NewMemInt(0)
			}
		} else {
			break
		}
	}
	return left
}

func (p *joinExprParser) parseAnd() *vdbe.Mem {
	left := p.parseNot()
	for {
		p.skipSpaces()
		if p.matchKeyword("and") {
			right := p.parseNot()
			if !isTruthy(left) || !isTruthy(right) {
				left = vdbe.NewMemInt(0)
			} else {
				left = vdbe.NewMemInt(1)
			}
		} else {
			break
		}
	}
	return left
}

func (p *joinExprParser) parseNot() *vdbe.Mem {
	p.skipSpaces()
	if p.matchKeyword("not") {
		val := p.parseNot()
		if isTruthy(val) {
			return vdbe.NewMemInt(0)
		}
		return vdbe.NewMemInt(1)
	}
	return p.parseComparison()
}

func (p *joinExprParser) parseComparison() *vdbe.Mem {
	left := p.parseAddition()
	for {
		p.skipSpaces()
		var op string
		if p.matchOp("=") || p.matchOp("==") {
			op = "="
		} else if p.matchOp("!=") || p.matchOp("<>") {
			op = "!="
		} else if p.matchOp("<=") {
			op = "<="
		} else if p.matchOp(">=") {
			op = ">="
		} else if p.matchOp("<") {
			op = "<"
		} else if p.matchOp(">") {
			op = ">"
		} else if p.peekKeyword("is") {
			p.matchKeyword("is")
			if p.matchKeyword("not") {
				op = "is not"
			} else {
				op = "is"
			}
		} else {
			break
		}
		right := p.parseAddition()
		left = compareValues(left, right, op)
	}
	return left
}

func (p *joinExprParser) parseAddition() *vdbe.Mem {
	left := p.parseMultiplication()
	for {
		p.skipSpaces()
		if p.matchOp("+") {
			right := p.parseMultiplication()
			left = arithOp(left, right, func(a, b int64) int64 { return a + b }, func(a, b float64) float64 { return a + b })
		} else if p.matchOp("-") {
			right := p.parseMultiplication()
			left = arithOp(left, right, func(a, b int64) int64 { return a - b }, func(a, b float64) float64 { return a - b })
		} else if p.matchOp("||") {
			right := p.parseMultiplication()
			// String concatenation
			left = vdbe.NewMemStr(memStr(left) + memStr(right))
		} else {
			break
		}
	}
	return left
}

func (p *joinExprParser) parseMultiplication() *vdbe.Mem {
	left := p.parsePrimary()
	for {
		p.skipSpaces()
		if p.matchOp("*") {
			right := p.parsePrimary()
			left = arithOp(left, right, func(a, b int64) int64 { return a * b }, func(a, b float64) float64 { return a * b })
		} else if p.matchOp("/") {
			right := p.parsePrimary()
			left = arithOp(left, right, func(a, b int64) int64 { return a / b }, func(a, b float64) float64 { return a / b })
		} else if p.matchOp("%") {
			right := p.parsePrimary()
			left = arithOp(left, right, func(a, b int64) int64 { return a % b }, nil)
		} else {
			break
		}
	}
	return left
}

func (p *joinExprParser) parsePrimary() *vdbe.Mem {
	p.skipSpaces()
	if p.pos >= len(p.src) {
		return nil
	}

	// Parenthesized expression
	if p.src[p.pos] == '(' {
		p.pos++
		val := p.parseExpr()
		p.skipSpaces()
		if p.pos < len(p.src) && p.src[p.pos] == ')' {
			p.pos++
		}
		return val
	}

	// String literal
	if p.src[p.pos] == '\'' {
		p.pos++
		var sb strings.Builder
		for p.pos < len(p.src) {
			if p.src[p.pos] == '\'' {
				p.pos++
				if p.pos < len(p.src) && p.src[p.pos] == '\'' {
					sb.WriteByte('\'')
					p.pos++
					continue
				}
				break
			}
			sb.WriteByte(p.src[p.pos])
			p.pos++
		}
		return vdbe.NewMemStr(sb.String())
	}

	// NULL keyword
	if p.matchKeyword("null") {
		return vdbe.NewMemNull()
	}

	// Number
	if c := p.peek(); (c >= '0' && c <= '9') || (c == '.' && p.pos+1 < len(p.src) && p.src[p.pos+1] >= '0' && p.src[p.pos+1] <= '9') {
		start := p.pos
		isFloat := false
		for p.pos < len(p.src) && ((p.src[p.pos] >= '0' && p.src[p.pos] <= '9') || p.src[p.pos] == '.') {
			if p.src[p.pos] == '.' {
				isFloat = true
			}
			p.pos++
		}
		if p.pos < len(p.src) && (p.src[p.pos] == 'e' || p.src[p.pos] == 'E') {
			isFloat = true
			p.pos++
			if p.pos < len(p.src) && (p.src[p.pos] == '+' || p.src[p.pos] == '-') {
				p.pos++
			}
			for p.pos < len(p.src) && p.src[p.pos] >= '0' && p.src[p.pos] <= '9' {
				p.pos++
			}
		}
		numStr := p.src[start:p.pos]
		if isFloat {
			if v, err := strconv.ParseFloat(numStr, 64); err == nil {
				return vdbe.NewMemFloat(v)
			}
		} else {
			if v, err := strconv.ParseInt(numStr, 10, 64); err == nil {
				return vdbe.NewMemInt(v)
			}
		}
	}

	// Read a word (identifier or keyword)
	start := p.pos
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || (c >= '0' && c <= '9') {
			p.pos++
		} else {
			break
		}
	}
	word := p.src[start:p.pos]
	if word == "" {
		return nil
	}

	// Check for function call
	p.skipSpaces()
	if p.pos < len(p.src) && p.src[p.pos] == '(' {
		p.pos++ // skip '('
		return p.evalFunction(strings.ToLower(word))
	}

	// Check for qualified reference: table.column
	p.skipSpaces()
	if p.pos < len(p.src) && p.src[p.pos] == '.' {
		p.pos++ // skip '.'
		p.skipSpaces() // skip spaces after dot
		// Read column name
		colStart := p.pos
		for p.pos < len(p.src) {
			c := p.src[p.pos]
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || (c >= '0' && c <= '9') {
				p.pos++
			} else {
				break
			}
		}
		colName := p.src[colStart:p.pos]
		if colName == "" {
			return vdbe.NewMemNull()
		}

		// Handle table.* (shouldn't appear here, but just in case)
		if colName == "*" {
			return vdbe.NewMemStr(word + ".*")
		}

		// Look up qualified column
		return p.lookupQualifiedColumn(word, colName)
	}

	// Unqualified column reference
	return p.lookupColumn(word)
}

// lookupQualifiedColumn resolves table.column.
func (p *joinExprParser) lookupQualifiedColumn(tableName, colName string) *vdbe.Mem {
	tblLower := strings.ToLower(tableName)
	colLower := strings.ToLower(colName)

	// Handle rowid
	if colLower == "rowid" {
		for _, tn := range p.tableNames {
			if strings.ToLower(tn) == tblLower {
				// Find the rowid column (after the regular columns)
				colCount := 0
				for j, tn2 := range p.tableNames {
					if strings.ToLower(tn2) == tblLower {
						colCount++
						if j >= len(p.values) {
							return vdbe.NewMemNull()
						}
					}
				}
				// rowid is stored after regular columns for each table
				// Find the start index for this table's rowid
				tblStart := -1
				tblColCount := 0
				for j := 0; j < len(p.tableNames); j++ {
					if strings.ToLower(p.tableNames[j]) == tblLower {
						if tblStart == -1 {
							tblStart = j
						}
						tblColCount++
					} else if tblStart >= 0 && tblColCount > 0 {
						break
					}
				}
				rowidIdx := tblStart + tblColCount
				if rowidIdx < len(p.values) {
					return vdbe.MemFromValue(p.values[rowidIdx])
				}
				return vdbe.NewMemNull()
			}
		}
		return vdbe.NewMemNull()
	}

	for i, tn := range p.tableNames {
		if strings.ToLower(tn) == tblLower && i < len(p.colNames) {
			if strings.ToLower(p.colNames[i]) == colLower && i < len(p.values) {
				return vdbe.MemFromValue(p.values[i])
			}
		}
	}
	return vdbe.NewMemNull()
}

// lookupColumn resolves an unqualified column name.
func (p *joinExprParser) lookupColumn(name string) *vdbe.Mem {
	nameLower := strings.ToLower(name)

	// Handle rowid
	if nameLower == "rowid" {
		// Return rowid from first table
		for i, cn := range p.colNames {
			if strings.ToLower(cn) == "rowid" && i < len(p.values) {
				return vdbe.MemFromValue(p.values[i])
			}
		}
		return vdbe.NewMemInt(0)
	}

	for i, cn := range p.colNames {
		if strings.ToLower(cn) == nameLower && i < len(p.values) {
			return vdbe.MemFromValue(p.values[i])
		}
	}
	return vdbe.NewMemNull()
}

// evalFunction evaluates a SQL function call.
func (p *joinExprParser) evalFunction(name string) *vdbe.Mem {
	var funcArgs []*vdbe.Mem
	p.skipSpaces()
	for p.pos < len(p.src) && p.src[p.pos] != ')' {
		if p.src[p.pos] == ',' {
			p.pos++
			continue
		}
		arg := p.parseExpr()
		if arg != nil {
			funcArgs = append(funcArgs, arg)
		}
	}
	if p.pos < len(p.src) && p.src[p.pos] == ')' {
		p.pos++
	}

	switch name {
	case "coalesce":
		for _, arg := range funcArgs {
			if arg != nil && arg.Type != vdbe.MemNull {
				return arg
			}
		}
		return vdbe.NewMemNull()
	case "ifnull":
		if len(funcArgs) >= 2 && (funcArgs[0] == nil || funcArgs[0].Type == vdbe.MemNull) {
			return funcArgs[1]
		}
		if len(funcArgs) >= 1 {
			return funcArgs[0]
		}
		return vdbe.NewMemNull()
	case "nullif":
		if len(funcArgs) >= 2 && memEqual(funcArgs[0], funcArgs[1]) {
			return vdbe.NewMemNull()
		}
		if len(funcArgs) >= 1 {
			return funcArgs[0]
		}
		return vdbe.NewMemNull()
	case "typeof":
		if len(funcArgs) >= 1 {
			if funcArgs[0] == nil || funcArgs[0].Type == vdbe.MemNull {
				return vdbe.NewMemStr("null")
			}
			switch funcArgs[0].Type {
			case vdbe.MemInt:
				return vdbe.NewMemStr("integer")
			case vdbe.MemFloat:
				return vdbe.NewMemStr("real")
			case vdbe.MemStr:
				return vdbe.NewMemStr("text")
			case vdbe.MemBlob:
				return vdbe.NewMemStr("blob")
			}
		}
		return vdbe.NewMemStr("null")
	case "length":
		if len(funcArgs) >= 1 && funcArgs[0] != nil && funcArgs[0].Type == vdbe.MemStr {
			return vdbe.NewMemInt(int64(len(funcArgs[0].StringValue())))
		}
		return vdbe.NewMemNull()
	case "abs":
		if len(funcArgs) >= 1 && funcArgs[0] != nil {
			switch funcArgs[0].Type {
			case vdbe.MemInt:
				v := funcArgs[0].IntVal
				if v < 0 {
					v = -v
				}
				return vdbe.NewMemInt(v)
			case vdbe.MemFloat:
				return vdbe.NewMemFloat(math.Abs(funcArgs[0].FloatVal))
			}
		}
		return vdbe.NewMemNull()
	case "lower":
		if len(funcArgs) >= 1 && funcArgs[0] != nil && funcArgs[0].Type == vdbe.MemStr {
			return vdbe.NewMemStr(strings.ToLower(funcArgs[0].StringValue()))
		}
		return vdbe.NewMemNull()
	case "upper":
		if len(funcArgs) >= 1 && funcArgs[0] != nil && funcArgs[0].Type == vdbe.MemStr {
			return vdbe.NewMemStr(strings.ToUpper(funcArgs[0].StringValue()))
		}
		return vdbe.NewMemNull()
	case "cast":
		// Handled by the caller
		return vdbe.NewMemNull()
	}
	return vdbe.NewMemNull()
}

// compareValues compares two mem values with the given operator.
func compareValues(left, right *vdbe.Mem, op string) *vdbe.Mem {
	if left == nil || right == nil {
		return vdbe.NewMemNull()
	}
	if left.Type == vdbe.MemNull || right.Type == vdbe.MemNull {
		if op == "is" {
			if left.Type == vdbe.MemNull && right.Type == vdbe.MemNull {
				return vdbe.NewMemInt(1)
			}
			return vdbe.NewMemInt(0)
		}
		if op == "is not" {
			if left.Type == vdbe.MemNull && right.Type == vdbe.MemNull {
				return vdbe.NewMemInt(0)
			}
			return vdbe.NewMemInt(1)
		}
		return vdbe.NewMemNull()
	}

	cmp := memCompare(left, right)
	switch op {
	case "=", "==":
		if cmp == 0 {
			return vdbe.NewMemInt(1)
		}
		return vdbe.NewMemInt(0)
	case "!=", "<>":
		if cmp != 0 {
			return vdbe.NewMemInt(1)
		}
		return vdbe.NewMemInt(0)
	case "<":
		if cmp < 0 {
			return vdbe.NewMemInt(1)
		}
		return vdbe.NewMemInt(0)
	case "<=":
		if cmp <= 0 {
			return vdbe.NewMemInt(1)
		}
		return vdbe.NewMemInt(0)
	case ">":
		if cmp > 0 {
			return vdbe.NewMemInt(1)
		}
		return vdbe.NewMemInt(0)
	case ">=":
		if cmp >= 0 {
			return vdbe.NewMemInt(1)
		}
		return vdbe.NewMemInt(0)
	case "is":
		return vdbe.NewMemInt(1) // both non-null and equal handled above
	case "is not":
		return vdbe.NewMemInt(0)
	}
	return vdbe.NewMemInt(0)
}

// memCompare returns -1, 0, or 1.
func memCompare(a, b *vdbe.Mem) int {
	// Type affinity: numeric < text < blob
	aType := memTypeRank(a)
	bType := memTypeRank(b)
	if aType != bType {
		if aType < bType {
			return -1
		}
		return 1
	}
	switch a.Type {
	case vdbe.MemInt:
		ai, bi := a.IntVal, b.IntValue()
		if ai < bi {
			return -1
		}
		if ai > bi {
			return 1
		}
		return 0
	case vdbe.MemFloat:
		af, bf := a.FloatValue(), b.FloatValue()
		if af < bf {
			return -1
		}
		if af > bf {
			return 1
		}
		return 0
	case vdbe.MemStr:
		if memStr(a) < memStr(b) {
			return -1
		}
		if memStr(a) > memStr(b) {
			return 1
		}
		return 0
	}
	return 0
}

func memTypeRank(m *vdbe.Mem) int {
	switch m.Type {
	case vdbe.MemInt, vdbe.MemFloat:
		return 0
	case vdbe.MemStr:
		return 1
	case vdbe.MemBlob:
		return 2
	default:
		return -1
	}
}

// memStr returns the string representation of a mem value.
func memStr(m *vdbe.Mem) string {
	if m == nil || m.Type == vdbe.MemNull {
		return ""
	}
	return m.String()
}

// arithOp performs arithmetic on two mem values.
func arithOp(a, b *vdbe.Mem, intFn func(int64, int64) int64, floatFn func(float64, float64) float64) *vdbe.Mem {
	if a == nil || b == nil {
		return vdbe.NewMemNull()
	}
	if a.Type == vdbe.MemNull || b.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	if a.Type == vdbe.MemInt && b.Type == vdbe.MemInt && intFn != nil {
		return vdbe.NewMemInt(intFn(a.IntVal, b.IntVal))
	}
	if floatFn != nil {
		return vdbe.NewMemFloat(floatFn(a.FloatValue(), b.FloatValue()))
	}
	return vdbe.NewMemNull()
}

// querySqliteMaster handles SELECT queries against sqlite_master / sqlite_schema.
func (db *Database) querySqliteMaster(filtered []compile.Token, pos int, cols []selectCol, args []interface{}) (*ResultSet, error) {
	// sqlite_master columns: type, name, tbl_name, rootpage, sql
	masterCols := []columnEntry{
		{name: "type"},
		{name: "name"},
		{name: "tbl_name"},
		{name: "rootpage"},
		{name: "sql"},
	}

	// Parse optional WHERE clause
	var whereExpr string
	if pos < len(filtered) && isKeyword(filtered[pos], "where") {
		pos++
		var whereParts []string
		for pos < len(filtered) && !isKeyword(filtered[pos], "limit") &&
			!isKeyword(filtered[pos], "order") && !isKeyword(filtered[pos], "group") {
			whereParts = append(whereParts, filtered[pos].Value)
			pos++
		}
		whereExpr = strings.Join(whereParts, " ")
	}

	// Build rows from master entries
	var rawData []rawRow
	colNames := []string{"type", "name", "tbl_name", "rootpage", "sql"}

	for _, entry := range db.masterEntries {
		values := []vdbe.Value{
			{Type: "text", Bytes: []byte(entry.Type)},
			{Type: "text", Bytes: []byte(entry.Name)},
			{Type: "text", Bytes: []byte(entry.TblName)},
			{Type: "int", IntVal: int64(entry.RootPage)},
			{Type: "text", Bytes: []byte(entry.SQL)},
		}

		// Apply WHERE filter
		if whereExpr != "" {
			wval := evalExprWithRow(whereExpr, args, colNames, values)
			if !isTruthy(wval) {
				continue
			}
		}

		rawData = append(rawData, rawRow{values: values})
	}

	// Determine output columns
	var resultCols []ResultColumnInfo
	for _, c := range cols {
		if c.expr == "*" {
			for _, col := range masterCols {
				resultCols = append(resultCols, ResultColumnInfo{
					Name: col.name,
					Type: ColNull,
				})
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
		}
	}

	// Check for aggregates
	hasAgg := false
	for _, c := range cols {
		if isAggregateExpr(c.expr) {
			hasAgg = true
			break
		}
	}

	var rows []Row

	if hasAgg {
		row := Row{cols: resultCols}
		for _, c := range cols {
			if c.expr == "*" {
				row.values = append(row.values, vdbe.NewMemInt(int64(len(rawData))))
				continue
			}
			agg := parseAggregate(c.expr)
			if agg != nil {
				row.values = append(row.values, computeAggregate(agg, rawData, colNames))
			} else {
				if len(rawData) > 0 {
					row.values = append(row.values, evalExprWithRow(c.expr, args, colNames, rawData[0].values))
				} else {
					row.values = append(row.values, vdbe.NewMemNull())
				}
			}
		}
		rows = []Row{row}
	} else {
		for _, rd := range rawData {
			row := Row{cols: resultCols}
			for _, c := range cols {
				if c.expr == "*" {
					for i := range masterCols {
						if i < len(rd.values) {
							row.values = append(row.values, vdbe.MemFromValue(rd.values[i]))
						} else {
							row.values = append(row.values, vdbe.NewMemNull())
						}
					}
				} else {
					val := evalExprWithRow(c.expr, args, colNames, rd.values)
					row.values = append(row.values, val)
				}
			}
			rows = append(rows, row)
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
		val, evalErr := evalSimpleExpr(c.expr, args)
		if evalErr != nil {
			return nil, NewErrorf(Error, "%s", evalErr)
		}
		row.values = append(row.values, val)
	}

	row.cols = resultCols
	rows := []Row{row}
	return newResultSet(rows, resultCols), nil
}

// evalSimpleExpr evaluates a SQL expression string and returns a Mem value.
func evalSimpleExpr(expr string, args []interface{}) (*vdbe.Mem, error) {
	p := &exprParser{src: strings.TrimSpace(expr), args: args}
	val := p.parseExpr()
	if p.err != nil {
		return nil, p.err
	}
	if val != nil {
		return val, nil
	}
	return vdbe.NewMemStr(strings.TrimSpace(expr)), nil
}

// evalExprWithRow evaluates a SQL expression in the context of a table row.
func evalExprWithRow(expr string, args []interface{}, colNames []string, colValues []vdbe.Value) *vdbe.Mem {
	p := &exprParser{src: strings.TrimSpace(expr), args: args, colNames: colNames, colValues: colValues}
	val := p.parseExpr()
	if val != nil {
		return val
	}
	return vdbe.NewMemStr(strings.TrimSpace(expr))
}

// exprParser is a full recursive-descent SQL expression parser.
type exprParser struct {
	src       string
	pos       int
	args      []interface{}
	colNames  []string
	colValues []vdbe.Value
	err       error // set if a parse/eval error occurs
}

func (p *exprParser) peek() byte {
	if p.pos >= len(p.src) {
		return 0
	}
	return p.src[p.pos]
}

func (p *exprParser) skipSpaces() {
	for p.pos < len(p.src) && p.src[p.pos] == ' ' {
		p.pos++
	}
}

func (p *exprParser) remaining() string {
	if p.pos >= len(p.src) {
		return ""
	}
	return p.src[p.pos:]
}

func (p *exprParser) matchKeyword(kw string) bool {
	p.skipSpaces()
	rest := p.remaining()
	if len(rest) < len(kw) {
		return false
	}
	if !strings.EqualFold(rest[:len(kw)], kw) {
		return false
	}
	// Ensure word boundary
	after := len(kw)
	if after < len(rest) {
		c := rest[after]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			return false
		}
	}
	p.pos += len(kw)
	return true
}

func (p *exprParser) peekKeyword(kw string) bool {
	saved := p.pos
	result := p.matchKeyword(kw)
	p.pos = saved
	return result
}

func (p *exprParser) matchOp(op string) bool {
	p.skipSpaces()
	rest := p.remaining()
	if len(rest) < len(op) {
		return false
	}
	if rest[:len(op)] != op {
		return false
	}
	p.pos += len(op)
	return true
}

func (p *exprParser) peekOp(op string) bool {
	saved := p.pos
	result := p.matchOp(op)
	p.pos = saved
	return result
}

// parseExpr is the entry point.
func (p *exprParser) parseExpr() *vdbe.Mem {
	return p.parseOr()
}

// parseOr handles OR.
func (p *exprParser) parseOr() *vdbe.Mem {
	left := p.parseAnd()
	for {
		p.skipSpaces()
		if p.matchKeyword("or") {
			right := p.parseAnd()
			if isNull(left) || isNull(right) {
				if isTruthy(left) {
					left = vdbe.NewMemInt(1)
				} else if isTruthy(right) {
					left = vdbe.NewMemInt(1)
				} else {
					left = vdbe.NewMemNull()
				}
			} else {
				if isTruthy(left) || isTruthy(right) {
					left = vdbe.NewMemInt(1)
				} else {
					left = vdbe.NewMemInt(0)
				}
			}
		} else {
			break
		}
	}
	return left
}

// parseAnd handles AND.
func (p *exprParser) parseAnd() *vdbe.Mem {
	left := p.parseNot()
	for {
		p.skipSpaces()
		if p.matchKeyword("and") && !p.peekKeyword("between") {
			// Check this isn't part of BETWEEN ... AND ...
			right := p.parseNot()
			if isNull(left) || isNull(right) {
				if !isTruthy(left) {
					left = vdbe.NewMemInt(0)
				} else {
					left = vdbe.NewMemNull()
				}
			} else {
				if isTruthy(left) && isTruthy(right) {
					left = vdbe.NewMemInt(1)
				} else {
					left = vdbe.NewMemInt(0)
				}
			}
		} else {
			break
		}
	}
	return left
}

// parseNot handles NOT prefix.
func (p *exprParser) parseNot() *vdbe.Mem {
	p.skipSpaces()
	if p.matchKeyword("not") {
		val := p.parseNot()
		if isNull(val) {
			return vdbe.NewMemNull()
		}
		if isTruthy(val) {
			return vdbe.NewMemInt(0)
		}
		return vdbe.NewMemInt(1)
	}
	return p.parseComparison()
}

// parseComparison handles =, !=, <, >, <=, >=, IS, IS NOT, IS NULL,
// ISNULL, NOTNULL, IN, BETWEEN, LIKE, GLOB.
func (p *exprParser) parseComparison() *vdbe.Mem {
	left := p.parseConcat()
	for {
		p.skipSpaces()

		// Postfix ISNULL / NOTNULL
		if p.peekKeyword("isnull") {
			p.matchKeyword("isnull")
			left = boolToInt(isNull(left))
			continue
		}
		if p.peekKeyword("notnull") {
			p.matchKeyword("notnull")
			left = boolToInt(!isNull(left))
			continue
		}

		// IS [NOT] NULL  or  IS [NOT] expr
		if p.peekKeyword("is") {
			p.matchKeyword("is")
			p.skipSpaces()
			neg := false
			if p.matchKeyword("not") {
				neg = true
			}
			if p.matchKeyword("null") {
				// IS NULL / IS NOT NULL
				if neg {
					left = boolToInt(!isNull(left))
				} else {
					left = boolToInt(isNull(left))
				}
			} else {
				// IS expr / IS NOT expr — NULL-equal comparison
				right := p.parseConcat()
				if isNull(left) && isNull(right) {
					if neg {
						left = vdbe.NewMemInt(0)
					} else {
						left = vdbe.NewMemInt(1)
					}
				} else if isNull(left) || isNull(right) {
					if neg {
						left = vdbe.NewMemInt(1)
					} else {
						left = vdbe.NewMemInt(0)
					}
				} else {
					eq := memEqual(left, right)
					if neg {
						left = boolToInt(!eq)
					} else {
						left = boolToInt(eq)
					}
				}
			}
			continue
		}

		// Check for NOT prefix (for IN, BETWEEN, LIKE, GLOB)
		negate := false
		saved := p.pos
		if p.peekKeyword("not") {
			p.matchKeyword("not")
			if p.peekKeyword("in") || p.peekKeyword("between") || p.peekKeyword("like") || p.peekKeyword("glob") {
				negate = true
			} else {
				p.pos = saved
			}
		}

		if negate || p.peekKeyword("in") {
			// [NOT] IN (...)
			if p.matchKeyword("in") {
				p.skipSpaces()
				if p.matchOp("(") {
					var vals []*vdbe.Mem
					for {
						p.skipSpaces()
						v := p.parseExpr()
						vals = append(vals, v)
						p.skipSpaces()
						if !p.matchOp(",") {
							break
						}
					}
					p.matchOp(")")
					found := false
					if !isNull(left) {
						for _, v := range vals {
							if !isNull(v) && memEqual(left, v) {
								found = true
								break
							}
						}
					}
					if negate {
						left = boolToInt(!found)
					} else {
						left = boolToInt(found)
					}
				}
				continue
			}
		}

		if negate || p.peekKeyword("between") {
			// [NOT] BETWEEN low AND high
			// Implements three-valued logic: x BETWEEN low AND high ≡ x >= low AND x <= high
			if p.matchKeyword("between") {
				low := p.parseConcat()
				p.skipSpaces()
				p.matchKeyword("and")
				high := p.parseConcat()

				var result *vdbe.Mem
				if isNull(left) {
					result = vdbe.NewMemNull()
				} else {
					// Evaluate x >= low
					var geLow *vdbe.Mem
					if isNull(low) {
						geLow = vdbe.NewMemNull()
					} else {
						geLow = boolToInt(memCompare(left, low) >= 0)
					}

					// If x >= low is false (0), AND short-circuits to false
					if geLow.Type == vdbe.MemInt && geLow.IntVal == 0 {
						result = vdbe.NewMemInt(0)
					} else {
						// Evaluate x <= high
						var leHigh *vdbe.Mem
						if isNull(high) {
							leHigh = vdbe.NewMemNull()
						} else {
							leHigh = boolToInt(memCompare(left, high) <= 0)
						}
						// AND of geLow and leHigh with three-valued logic
						result = threeValAnd(geLow, leHigh)
					}
				}

				if negate {
					result = threeValNot(result)
				}
				left = result
				continue
			}
		}

		if negate || p.peekKeyword("like") {
			// [NOT] LIKE [ESCAPE esc]
			if p.matchKeyword("like") {
				pattern := p.parseConcat()
				// Check for ESCAPE clause
				var escapeRune rune
				p.skipSpaces()
				if p.peekKeyword("escape") {
					p.matchKeyword("escape")
					escVal := p.parseConcat()
					if escVal == nil || isNull(escVal) {
						escapeRune = -1
					} else {
						escStr := memStr(escVal)
						if len(escStr) == 0 {
							// Empty escape string is an error
							p.err = fmt.Errorf("ESCAPE expression must be a single character")
							left = vdbe.NewMemNull()
							if negate {
								left = vdbe.NewMemNull()
							}
							continue
						}
						if len(escStr) > 1 {
							// Multi-char escape is an error
							p.err = fmt.Errorf("ESCAPE expression must be a single character")
							left = vdbe.NewMemNull()
							if negate {
								left = vdbe.NewMemNull()
							}
							continue
						}
						escapeRune, _ = utf8.DecodeRuneInString(escStr)
					}
				}
				match := likeMatchWithEscape(memStr(left), memStr(pattern), escapeRune)
				if negate {
					left = boolToInt(!match)
				} else {
					left = boolToInt(match)
				}
				continue
			}
		}

		if negate || p.peekKeyword("glob") {
			// [NOT] GLOB pattern
			if p.matchKeyword("glob") {
				pattern := p.parseConcat()
				match := globMatch(memStr(left), memStr(pattern))
				if negate {
					left = boolToInt(!match)
				} else {
					left = boolToInt(match)
				}
				continue
			}
		}

		// If NOT was consumed but nothing matched, restore and break
		if negate {
			p.pos = saved
			break
		}

		// Comparison operators
		var op string
		if p.matchOp("!=") {
			op = "!="
		} else if p.matchOp("<>") {
			op = "!="
		} else if p.matchOp("<=") {
			op = "<="
		} else if p.matchOp(">=") {
			op = ">="
		} else if p.matchOp("==") {
			op = "="
		} else if p.matchOp("=") {
			op = "="
		} else if p.matchOp("<") {
			op = "<"
		} else if p.matchOp(">") {
			op = ">"
		}
		if op != "" {
			right := p.parseConcat()
			if isNull(left) || isNull(right) {
				left = vdbe.NewMemNull()
			} else {
				switch op {
				case "=":
					left = boolToInt(memEqual(left, right))
				case "!=":
					left = boolToInt(!memEqual(left, right))
				case "<":
					left = boolToInt(memCompare(left, right) < 0)
				case ">":
					left = boolToInt(memCompare(left, right) > 0)
				case "<=":
					left = boolToInt(memCompare(left, right) <= 0)
				case ">=":
					left = boolToInt(memCompare(left, right) >= 0)
				}
			}
			continue
		}

		break
	}
	return left
}
func (p *exprParser) parseConcat() *vdbe.Mem {
	left := p.parseAddSub()
	for {
		p.skipSpaces()
		if p.matchOp("||") {
			right := p.parseAddSub()
			if isNull(left) || isNull(right) {
				left = vdbe.NewMemNull()
			} else {
				left = vdbe.NewMemStr(memStr(left) + memStr(right))
			}
		} else {
			break
		}
	}
	return left
}

// parseAddSub handles + and -.
func (p *exprParser) parseAddSub() *vdbe.Mem {
	left := p.parseMulDivMod()
	if left == nil {
		return nil
	}
	for {
		p.skipSpaces()
		var op byte
		if p.peek() == '+' || p.peek() == '-' {
			op = p.src[p.pos]
			p.pos++
			right := p.parseMulDivMod()
			if right == nil {
				return nil
			}
			if isNull(left) || isNull(right) {
				left = vdbe.NewMemNull()
			} else if left.Type == vdbe.MemInt && right.Type == vdbe.MemInt {
				if op == '+' {
					left = vdbe.NewMemInt(left.IntVal + right.IntVal)
				} else {
					left = vdbe.NewMemInt(left.IntVal - right.IntVal)
				}
			} else if left.Type == vdbe.MemStr || right.Type == vdbe.MemStr {
				// String + number: try numeric conversion
				lf, rf := toFloat(left), toFloat(right)
				if op == '+' {
					left = vdbe.NewMemFloat(lf + rf)
				} else {
					left = vdbe.NewMemFloat(lf - rf)
				}
			} else {
				lf, rf := left.FloatValue(), right.FloatValue()
				if op == '+' {
					left = vdbe.NewMemFloat(lf + rf)
				} else {
					left = vdbe.NewMemFloat(lf - rf)
				}
			}
		} else {
			break
		}
	}
	return left
}

// parseMulDivMod handles *, /, %.
func (p *exprParser) parseMulDivMod() *vdbe.Mem {
	left := p.parseBitwise()
	if left == nil {
		return nil
	}
	for {
		p.skipSpaces()
		ch := p.peek()
		if ch == '*' || ch == '/' || ch == '%' {
			p.pos++
			right := p.parseBitwise()
			if right == nil {
				return nil
			}
			if isNull(left) || isNull(right) {
				left = vdbe.NewMemNull()
			} else if left.Type == vdbe.MemInt && right.Type == vdbe.MemInt {
				switch ch {
				case '*':
					left = vdbe.NewMemInt(left.IntVal * right.IntVal)
				case '/':
					if right.IntVal != 0 {
						left = vdbe.NewMemInt(left.IntVal / right.IntVal)
					}
				case '%':
					if right.IntVal != 0 {
						left = vdbe.NewMemInt(left.IntVal % right.IntVal)
					}
				}
			} else {
				lf, rf := left.FloatValue(), right.FloatValue()
				switch ch {
				case '*':
					left = vdbe.NewMemFloat(lf * rf)
				case '/':
					if rf != 0 {
						left = vdbe.NewMemFloat(lf / rf)
					}
				case '%':
					if rf != 0 {
						left = vdbe.NewMemFloat(float64(int64(lf) % int64(rf)))
					}
				}
			}
		} else {
			break
		}
	}
	return left
}

// parseBitwise handles &, |, <<, >>.
func (p *exprParser) parseBitwise() *vdbe.Mem {
	left := p.parseUnary()
	if left == nil {
		return nil
	}
	for {
		p.skipSpaces()
		if p.matchOp("<<") {
			right := p.parseUnary()
			if isNull(left) || isNull(right) {
				left = vdbe.NewMemNull()
			} else {
				left = vdbe.NewMemInt(left.IntVal << uint64(right.IntVal))
			}
		} else if p.matchOp(">>") {
			right := p.parseUnary()
			if isNull(left) || isNull(right) {
				left = vdbe.NewMemNull()
			} else {
				left = vdbe.NewMemInt(left.IntVal >> uint64(right.IntVal))
			}
		} else if p.matchOp("&") {
			right := p.parseUnary()
			if isNull(left) || isNull(right) {
				left = vdbe.NewMemNull()
			} else {
				left = vdbe.NewMemInt(left.IntVal & right.IntVal)
			}
		} else if !p.peekOp("||") && p.matchOp("|") {
			right := p.parseUnary()
			if isNull(left) || isNull(right) {
				left = vdbe.NewMemNull()
			} else {
				left = vdbe.NewMemInt(left.IntVal | right.IntVal)
			}
		} else {
			break
		}
	}
	return left
}

// parseUnary handles unary +, -, ~.
func (p *exprParser) parseUnary() *vdbe.Mem {
	p.skipSpaces()
	if p.peek() == '-' {
		p.pos++
		val := p.parseUnary()
		if val == nil || isNull(val) {
			return vdbe.NewMemNull()
		}
		if val.Type == vdbe.MemInt {
			return vdbe.NewMemInt(-val.IntVal)
		}
		return vdbe.NewMemFloat(-val.FloatValue())
		// Float value might be an overflow int (e.g. 9223372036854775808)
		if val.Type == vdbe.MemFloat {
			f := -val.FloatVal
			if f == float64(int64(f)) && f >= float64(int64(-9223372036854775808)) && f <= float64(9223372036854775807) {
				return vdbe.NewMemInt(int64(f))
			}
		}
	}
	if p.peek() == '+' {
		p.pos++
		return p.parseUnary()
	}
	if p.peek() == '~' {
		p.pos++
		val := p.parseUnary()
		if val == nil || isNull(val) {
			return vdbe.NewMemNull()
		}
		return vdbe.NewMemInt(^val.IntVal)
	}
	return p.parsePrimary()
}

// parsePrimary handles literals, parens, function calls, CASE, column refs.
func (p *exprParser) parsePrimary() *vdbe.Mem {
	p.skipSpaces()
	if p.pos >= len(p.src) {
		return nil
	}

	// Parenthesized expression
	if p.src[p.pos] == '(' {
		p.pos++
		val := p.parseExpr()
		p.skipSpaces()
		if p.pos < len(p.src) && p.src[p.pos] == ')' {
			p.pos++
		}
		return val
	}

	// Bind variable
	if p.src[p.pos] == '?' {
		p.pos++
		start := p.pos
		for p.pos < len(p.src) && p.src[p.pos] >= '0' && p.src[p.pos] <= '9' {
			p.pos++
		}
		idxStr := p.src[start:p.pos]
		if idxStr == "" {
			// Sequential ? - not supported in this context
			return vdbe.NewMemNull()
		}
		if idx, err := strconv.Atoi(idxStr); err == nil && idx > 0 && idx <= len(p.args) {
			return vdbe.MakeMem(p.args[idx-1])
		}
		return vdbe.NewMemNull()
	}

	// String literal
	if p.src[p.pos] == '\'' {
		p.pos++
		var sb strings.Builder
		for p.pos < len(p.src) {
			if p.src[p.pos] == '\'' {
				p.pos++
				if p.pos < len(p.src) && p.src[p.pos] == '\'' {
					sb.WriteByte('\'')
					p.pos++
					continue
				}
				break
			}
			sb.WriteByte(p.src[p.pos])
			p.pos++
		}
		return vdbe.NewMemStr(sb.String())
	}

	// CASE expression
	if p.peekKeyword("case") {
		return p.parseCase()
	}

	// NULL keyword
	if p.matchKeyword("null") {
		return vdbe.NewMemNull()
	}

	// Number (int or float) - check before word parsing
	if c := p.peek(); (c >= '0' && c <= '9') || (c == '.' && p.pos+1 < len(p.src) && p.src[p.pos+1] >= '0' && p.src[p.pos+1] <= '9') {
		start := p.pos
		isFloat := false
		for p.pos < len(p.src) && ((p.src[p.pos] >= '0' && p.src[p.pos] <= '9') || p.src[p.pos] == '.') {
			if p.src[p.pos] == '.' {
				isFloat = true
			}
			p.pos++
		}
		if p.pos < len(p.src) && (p.src[p.pos] == 'e' || p.src[p.pos] == 'E') {
			isFloat = true
			p.pos++
			if p.pos < len(p.src) && (p.src[p.pos] == '+' || p.src[p.pos] == '-') {
				p.pos++
			}
			for p.pos < len(p.src) && p.src[p.pos] >= '0' && p.src[p.pos] <= '9' {
				p.pos++
			}
		}
		numStr := p.src[start:p.pos]
		if isFloat {
			if v, err := strconv.ParseFloat(numStr, 64); err == nil {
				return vdbe.NewMemFloat(v)
			}
		} else {
			if v, err := strconv.ParseInt(numStr, 10, 64); err == nil {
				return vdbe.NewMemInt(v)
			}
			if v, err := strconv.ParseFloat(numStr, 64); err == nil {
				return vdbe.NewMemFloat(v)
			}
		}
	}

	// Function calls or identifiers: read a word
	start := p.pos
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || (c >= '0' && c <= '9') {
			p.pos++
		} else {
			break
		}
	}
	word := p.src[start:p.pos]
	if word == "" {
		return nil
	}

	// Check if it's a function call (word followed by '(')
	p.skipSpaces()
	if p.pos < len(p.src) && p.src[p.pos] == '(' {
		p.pos++ // skip '('
		return p.evalFunction(strings.ToLower(word))
	}

	// Check column reference
	if p.colNames != nil {
		for i, name := range p.colNames {
			if strings.EqualFold(name, word) {
				if i < len(p.colValues) {
					return vdbe.MemFromValue(p.colValues[i])
				}
				// Column exists in schema but not in this record (added via ALTER TABLE)
				return vdbe.NewMemNull()
			}
		}
	}

	// Try as number (for pure digit words that didn't match above)
	if v, err := strconv.ParseInt(word, 10, 64); err == nil {
		return vdbe.NewMemInt(v)
	}
	if v, err := strconv.ParseFloat(word, 64); err == nil {
		return vdbe.NewMemFloat(v)
	}

	return vdbe.NewMemStr(word)
}

// parseCase handles CASE expressions.
func (p *exprParser) parseCase() *vdbe.Mem {
	p.matchKeyword("case")
	p.skipSpaces()

	// Simple CASE or searched CASE?
	// If next is WHEN, it's searched CASE. Otherwise simple CASE (has operand).
	var operand *vdbe.Mem
	if !p.peekKeyword("when") {
		operand = p.parseExpr()
	}

	for p.matchKeyword("when") {
		if operand != nil {
			// Simple CASE: compare operand to WHEN value
			val := p.parseExpr()
			p.skipSpaces()
			p.matchKeyword("then")
			result := p.parseExpr()
			if !isNull(operand) && !isNull(val) && memEqual(operand, val) {
				// Skip remaining WHEN/ELSE
				for !p.peekKeyword("end") {
					// skip tokens
					if p.matchKeyword("when") {
						p.parseExpr()
						p.skipSpaces()
						p.matchKeyword("then")
						p.parseExpr()
					} else if p.matchKeyword("else") {
						p.parseExpr()
					} else {
						p.pos++
					}
					p.skipSpaces()
				}
				p.matchKeyword("end")
				return result
			}
		} else {
			// Searched CASE: WHEN condition
			cond := p.parseExpr()
			p.skipSpaces()
			p.matchKeyword("then")
			result := p.parseExpr()
			if isTruthy(cond) {
				for !p.peekKeyword("end") {
					if p.matchKeyword("when") {
						p.parseExpr()
						p.skipSpaces()
						p.matchKeyword("then")
						p.parseExpr()
					} else if p.matchKeyword("else") {
						p.parseExpr()
					} else {
						p.pos++
					}
					p.skipSpaces()
				}
				p.matchKeyword("end")
				return result
			}
		}
	}

	// ELSE
	p.skipSpaces()
	if p.matchKeyword("else") {
		result := p.parseExpr()
		p.matchKeyword("end")
		return result
	}

	p.matchKeyword("end")
	return vdbe.NewMemNull()
}

// evalFunction evaluates a function call (the '(' is already consumed).
func (p *exprParser) evalFunction(name string) *vdbe.Mem {
	var args []*vdbe.Mem
	for {
		p.skipSpaces()
		if p.pos < len(p.src) && p.src[p.pos] == ')' {
			p.pos++
			break
		}
		if p.peek() == 0 {
			break
		}
		if p.peekOp("*") {
			p.pos++
			args = append(args, vdbe.NewMemInt(0))
		} else {
			arg := p.parseExpr()
			args = append(args, arg)
		}
		p.skipSpaces()
		if !p.matchOp(",") {
			p.skipSpaces()
			if p.pos < len(p.src) && p.src[p.pos] == ')' {
				p.pos++
				break
			}
		}
	}

	switch name {
	case "typeof":
		if len(args) > 0 {
			return vdbe.NewMemStr(typeofMem(args[0]))
		}
	case "coalesce":
		for _, a := range args {
			if !isNull(a) {
				return a
			}
		}
		return vdbe.NewMemNull()
	case "ifnull":
		if len(args) >= 2 {
			if !isNull(args[0]) {
				return args[0]
			}
			return args[1]
		}
	case "nullif":
		if len(args) >= 2 {
			if !isNull(args[0]) && !isNull(args[1]) && memEqual(args[0], args[1]) {
				return vdbe.NewMemNull()
			}
			return args[0]
		}
	case "abs":
		if len(args) > 0 && !isNull(args[0]) {
			if args[0].Type == vdbe.MemInt {
				v := args[0].IntVal
				if v < 0 {
					v = -v
				}
				return vdbe.NewMemInt(v)
			}
			v := args[0].FloatValue()
			if v < 0 {
				v = -v
			}
			return vdbe.NewMemFloat(v)
		}
	case "upper":
		if len(args) > 0 && !isNull(args[0]) {
			return vdbe.NewMemStr(strings.ToUpper(memStr(args[0])))
		}
	case "lower":
		if len(args) > 0 && !isNull(args[0]) {
			return vdbe.NewMemStr(strings.ToLower(memStr(args[0])))
		}
	case "length":
		if len(args) > 0 && !isNull(args[0]) {
			return vdbe.NewMemInt(int64(len(memStr(args[0]))))
		}
	}
	return vdbe.NewMemNull()
}

// --- Expression helper functions ---

func isNull(m *vdbe.Mem) bool {
	return m == nil || m.Type == vdbe.MemNull
}

func isTruthy(m *vdbe.Mem) bool {
	if isNull(m) {
		return false
	}
	switch m.Type {
	case vdbe.MemInt:
		return m.IntVal != 0
	case vdbe.MemFloat:
		return m.FloatVal != 0
	case vdbe.MemStr:
		return len(m.Bytes) > 0
	}
	return false
}

func toFloat(m *vdbe.Mem) float64 {
	if isNull(m) {
		return 0
	}
	return m.FloatValue()
}

func boolToInt(b bool) *vdbe.Mem {
	if b {
		return vdbe.NewMemInt(1)
	}
	return vdbe.NewMemInt(0)
}

func typeofMem(m *vdbe.Mem) string {
	if isNull(m) {
		return "null"
	}
	switch m.Type {
	case vdbe.MemInt:
		return "integer"
	case vdbe.MemFloat:
		return "real"
	case vdbe.MemStr:
		return "text"
	case vdbe.MemBlob:
		return "blob"
	}
	return "null"
}

func memEqual(a, b *vdbe.Mem) bool {
	if a.Type == vdbe.MemStr && b.Type == vdbe.MemStr {
		return memStr(a) == memStr(b)
	}
	if a.Type == vdbe.MemStr || b.Type == vdbe.MemStr {
		// Compare as strings
		return memStr(a) == memStr(b)
	}
	return memCompare(a, b) == 0
}

// likeMatch implements SQL LIKE pattern matching (% and _ wildcards).
func likeMatch(input, pattern string) bool {
	return likeMatchWithEscape(input, pattern, -1)
}

// likeMatchWithEscape implements SQL LIKE pattern matching with optional escape character.
// escapeRune < 0 means no escape character.
func likeMatchWithEscape(input, pattern string, escapeRune rune) bool {
	return likeMatchRunes(
		[]rune(input),
		[]rune(pattern),
		0, 0,
		escapeRune,
		false,
	)
}

func likeMatchRunes(input, pattern []rune, pi, pp int, esc rune, escaped bool) bool {
	for pp < len(pattern) {
		if !escaped && esc >= 0 && pattern[pp] == esc {
			pp++
			if pp >= len(pattern) {
				return false
			}
			escaped = true
			continue
		}
		ch := pattern[pp]
		if !escaped && ch == '%' {
			pp++
			// Skip consecutive %
			for pp < len(pattern) && pattern[pp] == '%' && !(esc >= 0 && pattern[pp] == esc) {
				pp++
			}
			if pp >= len(pattern) {
				return true
			}
			// Try matching rest at each position in input
			for pi <= len(input) {
				if likeMatchRunes(input, pattern, pi, pp, esc, false) {
					return true
				}
				pi++
			}
			return false
		}
		if pi >= len(input) {
			return false
		}
		if !escaped && ch == '_' {
			pp++
			pi++
			escaped = false
			continue
		}
		if unicodeToLower(ch) != unicodeToLower(input[pi]) {
			return false
		}
		pp++
		pi++
		escaped = false
	}
	return pi >= len(input)
}

func unicodeToLower(r rune) rune {
	if r >= 'A' && r <= 'Z' {
		return r + ('a' - 'A')
	}
	return r
}

// globMatch implements GLOB pattern matching (case-sensitive).
// Wildcards: * = any sequence, ? = any single char, [...] = character class.
func globMatch(input, pattern string) bool {
	return globMatchRunes([]rune(input), []rune(pattern), 0, 0)
}

func globMatchRunes(input, pattern []rune, pi, pp int) bool {
	for pp < len(pattern) {
		ch := pattern[pp]
		if ch == '*' {
			pp++
			// Skip consecutive *
			for pp < len(pattern) && pattern[pp] == '*' {
				pp++
			}
			if pp >= len(pattern) {
				return true
			}
			// Try matching rest at each position
			for pi <= len(input) {
				if globMatchRunes(input, pattern, pi, pp) {
					return true
				}
				pi++
			}
			return false
		}
		if ch == '?' {
			if pi >= len(input) {
				return false
			}
			pp++
			pi++
			continue
		}
		if ch == '[' {
			// Character class
			if pi >= len(input) {
				return false
			}
			pp++
			if pp >= len(pattern) {
				return false
			}
			negate := false
			if pattern[pp] == '^' {
				negate = true
				pp++
			} else if pattern[pp] == '!' {
				negate = true
				pp++
			}
			matched := false
			prevRune := rune(-1)
			for pp < len(pattern) && pattern[pp] != ']' {
				if pattern[pp] == '-' && prevRune >= 0 && pp+1 < len(pattern) && pattern[pp+1] != ']' {
					// Range: a-z
					pp++ // skip -
					endRune := pattern[pp]
					if input[pi] >= prevRune && input[pi] <= endRune {
						matched = true
					}
					prevRune = -1
				} else {
					if pattern[pp] == input[pi] {
						matched = true
					}
					prevRune = pattern[pp]
				}
				pp++
			}
			if pp < len(pattern) && pattern[pp] == ']' {
				pp++
			}
			if negate {
				matched = !matched
			}
			if !matched {
				return false
			}
			pi++
			continue
		}
		// Literal character (case-sensitive)
		if pi >= len(input) || input[pi] != ch {
			return false
		}
		pp++
		pi++
	}
	return pi >= len(input)
}

// threeValAnd implements three-valued AND logic (SQL NULL semantics).
// false AND anything = false; true AND null = null; true AND true = true.
func threeValAnd(a, b *vdbe.Mem) *vdbe.Mem {
	aFalse := a != nil && a.Type == vdbe.MemInt && a.IntVal == 0
	bFalse := b != nil && b.Type == vdbe.MemInt && b.IntVal == 0
	aNull := isNull(a)
	bNull := isNull(b)

	if aFalse || bFalse {
		return vdbe.NewMemInt(0)
	}
	if aNull || bNull {
		return vdbe.NewMemNull()
	}
	// Both are non-null, non-zero (truthy)
	return vdbe.NewMemInt(1)
}

// threeValNot implements three-valued NOT logic.
// NOT null = null; NOT true = false; NOT false = true.
func threeValNot(m *vdbe.Mem) *vdbe.Mem {
	if isNull(m) {
		return vdbe.NewMemNull()
	}
	if m.Type == vdbe.MemInt && m.IntVal == 0 {
		return vdbe.NewMemInt(1)
	}
	return vdbe.NewMemInt(0)
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

// unquoteIdent strips surrounding quotes from an identifier: [name], "name", `name`.
func unquoteIdent(s string) string {
	if len(s) >= 2 {
		if (s[0] == '[' && s[len(s)-1] == ']') ||
			(s[0] == '"' && s[len(s)-1] == '"') ||
			(s[0] == '`' && s[len(s)-1] == '`') {
			return s[1 : len(s)-1]
		}
	}
	return s
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
			case "view":
				return "create_view"
			case "index":
				return "create_index"
			}
		}
		return "create"
	case "drop":
		if len(tokens) > 1 {
			second := strings.ToLower(tokens[1].Value)
			switch second {
			case "view":
				return "drop_view"
			case "table":
				return "drop_table"
			case "index":
				return "drop_index"
			}
		}
		return "drop"
	case "begin":
		return "begin"
	case "commit", "end":
		return "commit"
	case "rollback":
		return "rollback"
	case "alter":
		return "alter"
	case "pragma":
		return "pragma"
	}
	return first
}


// parseInsertExprValue parses an expression value for INSERT/UPDATE.
// It collects tokens until comma or close-paren, joins them, and evaluates.
func parseInsertExprValue(tokens []compile.Token, pos *int, args []interface{}, bindIdx *int) (interface{}, error) {
	if *pos >= len(tokens) {
		return nil, fmt.Errorf("expected value")
	}

	// Collect tokens for this value expression
	start := *pos
	depth := 0
	for *pos < len(tokens) {
		t := tokens[*pos]
		if t.Type == compile.TokenLParen {
			depth++
		}
		if t.Type == compile.TokenRParen {
			if depth == 0 {
				break
			}
			depth--
		}
		if t.Type == compile.TokenComma && depth == 0 {
			break
		}
		*pos++
	}

	// Build expression string from collected tokens
	var parts []string
	for i := start; i < *pos; i++ {
		parts = append(parts, tokens[i].Value)
	}
	expr := strings.Join(parts, " ")

	// Check if it's a simple bind variable (single ? token)
	if tokens[start].Type == compile.TokenVariable && *pos-start == 1 {
		t := tokens[start]
		if len(t.Value) > 1 {
			if idx, err := strconv.Atoi(t.Value[1:]); err == nil {
				if idx > 0 && idx <= len(args) {
					return args[idx-1], nil
				}
			}
		}
		if t.Value == "?" {
			idx := *bindIdx
			*bindIdx++
			if idx < len(args) {
				return args[idx], nil
			}
			return nil, nil
		}
		return nil, nil
	}

	// Check if it's a simple NULL keyword
	if isKeyword(tokens[start], "null") && *pos-start == 1 {
		return nil, nil
	}

	// Evaluate using the expression parser
	val, evalErr := evalSimpleExpr(expr, args)
	if evalErr != nil {
		return nil, evalErr
	}
	switch val.Type {
	case vdbe.MemNull:
		return nil, nil
	case vdbe.MemInt:
		return val.IntVal, nil
	case vdbe.MemFloat:
		return val.FloatVal, nil
	case vdbe.MemStr:
		return string(val.Bytes), nil
	case vdbe.MemBlob:
		return val.Bytes, nil
	}
	return nil, nil
}
// parseExprValue parses a simple expression value from tokens.
// bindIdx tracks the current position for sequential ? parameters (0-based, incremented per ?).
func parseExprValue(tokens []compile.Token, pos *int, args []interface{}, bindIdx *int) (interface{}, error) {
	if *pos >= len(tokens) {
		return nil, fmt.Errorf("expected value")
	}

	// Handle leading minus for negative numbers
	neg := false
	if tokens[*pos].Type == compile.TokenMinus {
		neg = true
		*pos++
		if *pos >= len(tokens) {
			return nil, fmt.Errorf("expected value after -")
		}
	}

	t := tokens[*pos]
	switch t.Type {
	case compile.TokenInteger:
		*pos++
		v, err := strconv.ParseInt(t.Value, 10, 64)
		if err != nil {
			return t.Value, nil
		}
		if neg {
			v = -v
		}
		return v, nil
	case compile.TokenFloat:
		*pos++
		v, err := strconv.ParseFloat(t.Value, 64)
		if err != nil {
			return t.Value, nil
		}
		if neg {
			v = -v
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
		// Plain ? — sequential binding
		if t.Value == "?" {
			idx := *bindIdx
			*bindIdx++
			if idx < len(args) {
				return args[idx], nil
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

// memToValue converts a *vdbe.Mem to a vdbe.Value.
// interfaceToMem converts a Go interface{} to a *vdbe.Mem.
func interfaceToMem(v interface{}) *vdbe.Mem {
	switch val := v.(type) {
	case nil:
		return vdbe.NewMemNull()
	case int:
		return vdbe.NewMemInt(int64(val))
	case int64:
		return vdbe.NewMemInt(val)
	case float64:
		return vdbe.NewMemFloat(val)
	case string:
		return vdbe.NewMemStr(val)
	default:
		return vdbe.NewMemStr(fmt.Sprintf("%v", v))
	}
}

func memToValue(m *vdbe.Mem) vdbe.Value {
	if m == nil || m.Type == vdbe.MemNull {
		return vdbe.Value{Type: "null"}
	}
	switch m.Type {
	case vdbe.MemInt:
		return vdbe.Value{Type: "int", IntVal: m.IntVal}
	case vdbe.MemFloat:
		return vdbe.Value{Type: "float", FloatVal: m.FloatVal}
	case vdbe.MemStr:
		return vdbe.Value{Type: "text", Bytes: m.Bytes}
	case vdbe.MemBlob:
		return vdbe.Value{Type: "blob", Bytes: m.Bytes}
	}
	return vdbe.Value{Type: "null"}
}

// resolveSubqueries replaces ( SELECT ... ) subqueries with their scalar result.
func (db *Database) resolveSubqueries(expr string, args []interface{}) string {
	for {
		// Find "( SELECT" pattern
		idx := strings.Index(strings.ToLower(expr), "( select")
		if idx < 0 {
			idx = strings.Index(strings.ToLower(expr), "(select")
		}
		if idx < 0 {
			break
		}
		// Find matching close paren
		depth := 0
		end := -1
		for i := idx; i < len(expr); i++ {
			if expr[i] == '(' {
				depth++
			}
			if expr[i] == ')' {
				depth--
				if depth == 0 {
					end = i
					break
				}
			}
		}
		if end < 0 {
			break
		}
		subSQL := strings.TrimSpace(expr[idx+1 : end])
		// Execute the subquery
		rs, err := db.querySingle(subSQL, args)
		if err != nil {
			break
		}
		var result string
		if rs.Next() {
			row := rs.Row()
			if row.ColumnIsNull(0) {
				result = "NULL"
			} else {
				result = row.ColumnText(0)
			}
		}
		rs.Close()
		expr = expr[:idx] + result + expr[end+1:]
	}
	return expr
}
