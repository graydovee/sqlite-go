// Package sql implements the SQL engine upper-level features for sqlite-go,
// including PRAGMA, ALTER TABLE, triggers, UPSERT, and related commands.
package sql

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

// Engine provides high-level SQL operations on a SQLite database.
// It wraps the lower-level components (VFS, Pager, BTree) and provides
// methods for executing SQL commands like PRAGMA, ALTER TABLE, etc.
type Engine struct {
	mu sync.Mutex

	vfs    vfs.VFS
	pgr    pager.Pager
	btConn btree.BTreeConn
	bt     btree.BTree

	// Schema tracking
	tables map[string]*TableInfo

	// Configuration state
	userVersion  int
	journalMode  pager.JournalMode
	synchronous  int
	foreignKeys  bool
	cacheSize    int
	pageSize     int
	closed       bool

	// Transaction state
	inTx bool

	// Stats
	lastInsertRowID int64
	changes         int64
	totalChanges    int64
	autoCommit      bool
}

// TableInfo stores metadata about a table.
type TableInfo struct {
	Name     string
	RootPage int
	Columns  []ColumnInfo
}

// ColumnInfo stores metadata about a column.
type ColumnInfo struct {
	CID          int    // Column ID (0-based index)
	Name         string // Column name
	Type         string // Declared type affinity
	NotNull      bool   // NOT NULL constraint
	DefaultValue string // DEFAULT value as string
	IsPK         bool   // Part of PRIMARY KEY
}

// PragmaRow represents a single row returned by a PRAGMA command.
type PragmaRow struct {
	Values []interface{}
}

// OpenEngine creates a new Engine backed by an in-memory database.
func OpenEngine() (*Engine, error) {
	memVFS := vfs.Find("memory")
	if memVFS == nil {
		return nil, fmt.Errorf("no memory VFS available")
	}

	cfg := pager.PagerConfig{
		VFS:         memVFS,
		Path:        "",
		PageSize:    4096,
		CacheSize:   2000,
		JournalMode: pager.JournalMemory,
	}

	pgr, err := pager.OpenPager(cfg)
	if err != nil {
		return nil, fmt.Errorf("open pager: %w", err)
	}

	btConn := btree.OpenBTreeConn(pgr)
	if btConn == nil {
		pgr.Close()
		return nil, fmt.Errorf("failed to open btree connection")
	}

	bt, err := btConn.Open(pgr)
	if err != nil {
		pgr.Close()
		return nil, fmt.Errorf("failed to open btree: %w", err)
	}

	return &Engine{
		vfs:         memVFS,
		pgr:         pgr,
		btConn:      btConn,
		bt:          bt,
		tables:      make(map[string]*TableInfo),
		journalMode: pager.JournalMemory,
		synchronous: 2, // default FULL
		cacheSize:   2000,
		pageSize:    4096,
		autoCommit:  true,
	}, nil
}

// Close closes the engine and releases all resources.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}
	e.closed = true

	if e.bt != nil {
		e.bt.Close()
	}
	if e.pgr != nil {
		e.pgr.Close()
	}
	return nil
}

// ExecSQL executes a SQL statement. It dispatches to the appropriate
// handler based on the statement type.
func (e *Engine) ExecSQL(sqlStr string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return fmt.Errorf("database is closed")
	}

	tokens := compile.Tokenize(sqlStr)
	filtered := filterTokens(tokens)
	if len(filtered) == 0 {
		return nil
	}

	stmtType := classifyStatement(filtered)
	switch stmtType {
	case "create_table":
		return e.execCreateTable(filtered)
	case "insert":
		return e.execInsert(filtered, nil)
	case "pragma":
		return e.execPragma(filtered)
	case "alter":
		return e.execAlterTable(filtered)
	case "analyze":
		return e.execAnalyze(filtered)
	case "attach":
		return e.execAttach(filtered)
	case "detach":
		return e.execDetach(filtered)
	case "vacuum":
		return e.execVacuum(filtered)
	case "create_trigger":
		return e.execCreateTrigger(filtered)
	case "drop_trigger":
		return e.execDropTrigger(filtered)
	default:
		return fmt.Errorf("unsupported SQL statement: %s", stmtType)
	}
}

// ExecPragmaAndQuery executes a PRAGMA that returns rows.
func (e *Engine) ExecPragmaAndQuery(sqlStr string) ([]PragmaRow, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil, fmt.Errorf("database is closed")
	}

	tokens := compile.Tokenize(sqlStr)
	filtered := filterTokens(tokens)
	if len(filtered) == 0 {
		return nil, fmt.Errorf("empty statement")
	}

	stmtType := classifyStatement(filtered)
	if stmtType != "pragma" {
		return nil, fmt.Errorf("not a PRAGMA statement")
	}

	return e.queryPragma(filtered)
}

// GetTableInfo returns metadata about a table.
func (e *Engine) GetTableInfo(name string) (*TableInfo, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	t, ok := e.tables[name]
	return t, ok
}

// TableNames returns the names of all tables.
func (e *Engine) TableNames() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var names []string
	for n := range e.tables {
		names = append(names, n)
	}
	return names
}

// UserVersion returns the current user_version pragma value.
func (e *Engine) UserVersion() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.userVersion
}

// JournalMode returns the current journal_mode.
func (e *Engine) JournalMode() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return journalModeString(e.journalMode)
}

// Synchronous returns the current synchronous setting.
func (e *Engine) Synchronous() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.synchronous
}

// ForeignKeys returns whether foreign keys are enabled.
func (e *Engine) ForeignKeys() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.foreignKeys
}

// CacheSize returns the current cache size.
func (e *Engine) CacheSize() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cacheSize
}

// PageSize returns the page size.
func (e *Engine) PageSize() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.pageSize
}

// --- Internal SQL execution helpers ---

// execCreateTable handles CREATE TABLE statements.
func (e *Engine) execCreateTable(tokens []compile.Token) error {
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
		return fmt.Errorf("expected table name")
	}
	tableName := tokens[pos].Value
	pos++

	if ifNotExists && e.tables[tableName] != nil {
		return nil
	}

	if e.tables[tableName] != nil {
		return fmt.Errorf("table %s already exists", tableName)
	}

	// Parse column definitions
	var columns []ColumnInfo
	if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
		pos++ // skip (
		colIdx := 0
		for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
			colName := tokens[pos].Value
			pos++

			colType := ""
			if pos < len(tokens) && (tokens[pos].Type == compile.TokenID ||
				(tokens[pos].Type == compile.TokenKeyword && isTypeNameKeyword(tokens[pos].Value))) {
				colType = tokens[pos].Value
				pos++
			}

			// Skip constraints
			notNull := false
			isPK := false
			var defaultVal string
			for pos < len(tokens) &&
				tokens[pos].Type != compile.TokenComma &&
				tokens[pos].Type != compile.TokenRParen {
				if isKeyword(tokens[pos], "not") && pos+1 < len(tokens) && isKeyword(tokens[pos+1], "null") {
					notNull = true
					pos += 2
					continue
				}
				if isKeyword(tokens[pos], "primary") && pos+1 < len(tokens) && isKeyword(tokens[pos+1], "key") {
					isPK = true
					pos += 2
					continue
				}
				if isKeyword(tokens[pos], "default") {
					pos++
					if pos < len(tokens) {
						defaultVal = tokens[pos].Value
						pos++
					}
					continue
				}
				pos++
			}

			columns = append(columns, ColumnInfo{
				CID:          colIdx,
				Name:         colName,
				Type:         colType,
				NotNull:      notNull,
				DefaultValue: defaultVal,
				IsPK:         isPK,
			})
			colIdx++

			if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
				pos++
			}
		}
		if pos < len(tokens) && tokens[pos].Type == compile.TokenRParen {
			pos++
		}
	}

	if len(columns) == 0 {
		return fmt.Errorf("table must have at least one column")
	}

	// Create the B-Tree
	if !e.inTx {
		if err := e.pgr.Begin(true); err != nil {
			return fmt.Errorf("begin transaction: %w", err)
		}
		if err := e.bt.Begin(true); err != nil {
			e.pgr.Rollback()
			return fmt.Errorf("begin btree: %w", err)
		}
	}

	rootPage, err := e.bt.CreateBTree(btree.CreateTable)
	if err != nil {
		if !e.inTx {
			e.bt.Rollback()
			e.pgr.Rollback()
		}
		return fmt.Errorf("create btree: %w", err)
	}

	if !e.inTx {
		if err := e.bt.Commit(); err != nil {
			return fmt.Errorf("commit btree: %w", err)
		}
		if err := e.pgr.Commit(); err != nil {
			return fmt.Errorf("commit pager: %w", err)
		}
	}

	e.tables[tableName] = &TableInfo{
		Name:     tableName,
		RootPage: int(rootPage),
		Columns:  columns,
	}

	return nil
}

// execInsert handles INSERT statements (simplified).
func (e *Engine) execInsert(tokens []compile.Token, args []interface{}) error {
	pos := 0
	expectKeyword(tokens, &pos, "insert")
	if pos < len(tokens) && isKeyword(tokens[pos], "or") {
		pos += 2 // skip OR ...
	}
	expectKeyword(tokens, &pos, "into")

	if pos >= len(tokens) {
		return fmt.Errorf("expected table name in INSERT")
	}
	tableName := tokens[pos].Value
	pos++

	tbl, ok := e.tables[tableName]
	if !ok {
		return fmt.Errorf("no such table: %s", tableName)
	}

	// Optional column list
	var colList []string
	if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
		// Peek ahead to see if this is a column list or VALUES
		scanPos := pos + 1
		hasValuesKw := false
		for scanPos < len(tokens) {
			if isKeyword(tokens[scanPos], "values") {
				hasValuesKw = true
				break
			}
			if tokens[scanPos].Type == compile.TokenRParen {
				if scanPos+1 < len(tokens) && isKeyword(tokens[scanPos+1], "values") {
					hasValuesKw = true
				}
				break
			}
			scanPos++
		}
		if hasValuesKw {
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

	// DEFAULT VALUES
	if pos < len(tokens) && isKeyword(tokens[pos], "default") {
		pos++
		expectKeyword(tokens, &pos, "values")
		return e.insertRow(tbl, colList, nil, args)
	}

	// VALUES clause
	expectKeyword(tokens, &pos, "values")

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

		if err := e.insertRow(tbl, colList, values, args); err != nil {
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
func (e *Engine) insertRow(tbl *TableInfo, colList []string, values []interface{}, args []interface{}) error {
	needCommit := false
	if !e.inTx {
		if err := e.pgr.Begin(true); err != nil {
			return fmt.Errorf("begin transaction: %w", err)
		}
		if err := e.bt.Begin(true); err != nil {
			e.pgr.Rollback()
			return fmt.Errorf("begin btree: %w", err)
		}
		needCommit = true
	}

	numCols := len(tbl.Columns)
	if len(colList) == 0 {
		colList = make([]string, numCols)
		for i, c := range tbl.Columns {
			colList[i] = c.Name
		}
	}

	valMap := make(map[string]interface{})
	for i, name := range colList {
		if i < len(values) {
			valMap[name] = values[i]
		} else {
			valMap[name] = nil
		}
	}

	rb := vdbe.NewRecordBuilder()
	for _, col := range tbl.Columns {
		v, ok := valMap[col.Name]
		if !ok {
			rb.AddNull()
			continue
		}
		addValueToRecord(rb, v)
	}

	data := rb.Build()

	cursor, err := e.bt.Cursor(btree.PageNumber(tbl.RootPage), true)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer cursor.Close()

	var newRowID int64 = 1
	if hasRow, _ := cursor.Last(); hasRow {
		newRowID = int64(cursor.RowID()) + 1
	}
	if newRowID <= e.lastInsertRowID {
		newRowID = e.lastInsertRowID + 1
	}

	keyBuf := make([]byte, 9)
	keyLen := encodeVarintKey(keyBuf, newRowID)

	err = e.bt.Insert(cursor, keyBuf[:keyLen], data, btree.RowID(newRowID), btree.SeekNotFound)
	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	e.lastInsertRowID = newRowID
	e.changes = 1
	e.totalChanges++

	if needCommit {
		if err := e.bt.Commit(); err != nil {
			return fmt.Errorf("commit btree: %w", err)
		}
		if err := e.pgr.Commit(); err != nil {
			return fmt.Errorf("commit pager: %w", err)
		}
	}

	return nil
}

// --- Token helpers ---

func filterTokens(tokens []compile.Token) []compile.Token {
	var result []compile.Token
	for _, t := range tokens {
		if t.Type != compile.TokenWhitespace && t.Type != compile.TokenComment {
			result = append(result, t)
		}
	}
	return result
}

func isKeyword(t compile.Token, kw string) bool {
	return t.Type == compile.TokenKeyword && strings.EqualFold(t.Value, kw)
}

func expectKeyword(tokens []compile.Token, pos *int, kw string) {
	if *pos < len(tokens) && isKeyword(tokens[*pos], kw) {
		*pos++
	}
}

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
			case "trigger":
				return "create_trigger"
			}
		}
		return "create"
	case "drop":
		if len(tokens) > 1 {
			second := strings.ToLower(tokens[1].Value)
			switch second {
			case "trigger":
				return "drop_trigger"
			}
		}
		return "drop"
	case "begin":
		return "begin"
	case "commit", "end":
		return "commit"
	case "rollback":
		return "rollback"
	case "pragma":
		return "pragma"
	case "alter":
		return "alter"
	case "analyze":
		return "analyze"
	case "attach":
		return "attach"
	case "detach":
		return "detach"
	case "vacuum":
		return "vacuum"
	}
	return first
}

func isTypeNameKeyword(val string) bool {
	lower := strings.ToLower(val)
	switch lower {
	case "int", "integer", "text", "real", "blob", "varchar", "nvarchar",
		"char", "nchar", "clob", "float", "double", "boolean",
		"bigint", "smallint", "tinyint", "mediumint",
		"decimal", "numeric", "datetime", "date", "time",
		"varying", "character":
		return true
	}
	return false
}

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
		if len(t.Value) > 1 {
			if idx, err := strconv.Atoi(t.Value[1:]); err == nil {
				if idx > 0 && idx <= len(args) {
					return args[idx-1], nil
				}
			}
		}
		return nil, nil
	default:
		if isKeyword(t, "null") {
			*pos++
			return nil, nil
		}
		*pos++
		return t.Value, nil
	}
}

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

func journalModeString(m pager.JournalMode) string {
	switch m {
	case pager.JournalDelete:
		return "delete"
	case pager.JournalPersist:
		return "persist"
	case pager.JournalOff:
		return "off"
	case pager.JournalTruncate:
		return "truncate"
	case pager.JournalMemory:
		return "memory"
	case pager.JournalWAL:
		return "wal"
	default:
		return "unknown"
	}
}

func journalModeFromString(s string) (pager.JournalMode, error) {
	switch strings.ToLower(s) {
	case "delete":
		return pager.JournalDelete, nil
	case "persist":
		return pager.JournalPersist, nil
	case "off":
		return pager.JournalOff, nil
	case "truncate":
		return pager.JournalTruncate, nil
	case "memory":
		return pager.JournalMemory, nil
	case "wal":
		return pager.JournalWAL, nil
	default:
		return pager.JournalDelete, fmt.Errorf("unknown journal mode: %s", s)
	}
}
