package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/sqlite-go/sqlite-go/btree"
	"github.com/sqlite-go/sqlite-go/compile"
)

// execPragma executes a PRAGMA statement that does not return rows.
func (e *Engine) execPragma(tokens []compile.Token) error {
	_, isQuery, err := e.handlePragma(tokens)
	if err != nil {
		return err
	}
	_ = isQuery
	return nil
}

// queryPragma executes a PRAGMA statement and returns result rows.
func (e *Engine) queryPragma(tokens []compile.Token) ([]PragmaRow, error) {
	rows, _, err := e.handlePragma(tokens)
	return rows, err
}

// handlePragma processes PRAGMA commands and returns result rows if applicable.
// The boolean return indicates whether the PRAGMA was a read (query) operation.
func (e *Engine) handlePragma(tokens []compile.Token) ([]PragmaRow, bool, error) {
	pos := 0
	expectKeyword(tokens, &pos, "pragma")
	if pos >= len(tokens) {
		return nil, false, fmt.Errorf("expected pragma name")
	}

	// Parse optional schema prefix: PRAGMA schema.pragma_name
	schema := ""
	pragmaName := tokens[pos].Value
	pos++

	// Check for schema.name form
	if pos < len(tokens) && tokens[pos].Type == compile.TokenDot {
		pos++ // skip .
		schema = pragmaName
		if pos >= len(tokens) {
			return nil, false, fmt.Errorf("expected pragma name after schema")
		}
		pragmaName = tokens[pos].Value
		pos++
	}
	_ = schema // schema is currently unused (only "main" supported)

	pragmaName = strings.ToLower(pragmaName)

	// Check if there's a value assignment: PRAGMA name = value  or  PRAGMA name(value)
	hasValue := false
	var value interface{}
	var valueStr string

	if pos < len(tokens) && tokens[pos].Type == compile.TokenEq {
		// PRAGMA name = value
		pos++ // skip =
		hasValue = true
		if pos < len(tokens) {
			valueStr = tokens[pos].Value
			value = parsePragmaValue(tokens[pos])
			pos++
		}
	} else if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
		// PRAGMA name(value)
		pos++ // skip (
		if pos < len(tokens) {
			valueStr = tokens[pos].Value
			value = parsePragmaValue(tokens[pos])
			pos++
		}
		if pos < len(tokens) && tokens[pos].Type == compile.TokenRParen {
			pos++ // skip )
		}
		hasValue = true
	}

	// Dispatch based on pragma name
	switch pragmaName {
	case "table_info":
		return e.pragmaTableInfo(valueStr)
	case "database_list":
		return e.pragmaDatabaseList()
	case "user_version":
		if hasValue {
			return e.pragmaSetUserVersion(value)
		}
		return e.pragmaGetUserVersion()
	case "journal_mode":
		if hasValue {
			return e.pragmaSetJournalMode(valueStr)
		}
		return e.pragmaGetJournalMode()
	case "synchronous":
		if hasValue {
			return e.pragmaSetSynchronous(value)
		}
		return e.pragmaGetSynchronous()
	case "foreign_keys":
		if hasValue {
			return e.pragmaSetForeignKeys(valueStr)
		}
		return e.pragmaGetForeignKeys()
	case "cache_size":
		if hasValue {
			return e.pragmaSetCacheSize(value)
		}
		return e.pragmaGetCacheSize()
	case "page_size":
		return e.pragmaGetPageSize()
	case "integrity_check":
		return e.pragmaIntegrityCheck()
	case "compile_options":
		return e.pragmaCompileOptions()
	case "foreign_key_list":
		return e.pragmaForeignKeyList(valueStr)
	case "foreign_key_check":
		return e.pragmaForeignKeyCheck()
	default:
		return nil, false, fmt.Errorf("unknown pragma: %s", pragmaName)
	}
}

// parsePragmaValue extracts a Go value from a pragma value token.
func parsePragmaValue(tok compile.Token) interface{} {
	switch tok.Type {
	case compile.TokenInteger:
		v, err := strconv.ParseInt(tok.Value, 10, 64)
		if err == nil {
			return v
		}
		return tok.Value
	case compile.TokenFloat:
		v, err := strconv.ParseFloat(tok.Value, 64)
		if err == nil {
			return v
		}
		return tok.Value
	case compile.TokenString:
		s := tok.Value
		if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
			return s[1 : len(s)-1]
		}
		return s
	default:
		return tok.Value
	}
}

// pragmaTableInfo returns column info for a table.
// Columns: cid, name, type, notnull, dflt_value, pk
func (e *Engine) pragmaTableInfo(tableName string) ([]PragmaRow, bool, error) {
	if tableName == "" {
		return nil, false, fmt.Errorf("table_info requires a table name")
	}

	tbl, ok := e.tables[tableName]
	if !ok {
		return nil, false, fmt.Errorf("no such table: %s", tableName)
	}

	var rows []PragmaRow
	for _, col := range tbl.Columns {
		notNull := 0
		if col.NotNull {
			notNull = 1
		}
		pk := 0
		if col.IsPK {
			pk = 1
		}
		var defaultVal interface{}
		if col.DefaultValue != "" {
			defaultVal = col.DefaultValue
		}
		rows = append(rows, PragmaRow{
			Values: []interface{}{col.CID, col.Name, col.Type, notNull, defaultVal, pk},
		})
	}
	return rows, true, nil
}

// pragmaDatabaseList returns the list of attached databases.
// Columns: seq, name, file
func (e *Engine) pragmaDatabaseList() ([]PragmaRow, bool, error) {
	rows := []PragmaRow{
		{Values: []interface{}{0, "main", ""}},
	}
	return rows, true, nil
}

// pragmaGetUserVersion returns the user_version.
// Columns: user_version
func (e *Engine) pragmaGetUserVersion() ([]PragmaRow, bool, error) {
	return []PragmaRow{{Values: []interface{}{e.userVersion}}}, true, nil
}

// pragmaSetUserVersion sets the user_version and returns the new value.
func (e *Engine) pragmaSetUserVersion(value interface{}) ([]PragmaRow, bool, error) {
	v := toInt(value)
	e.userVersion = v
	return []PragmaRow{{Values: []interface{}{v}}}, false, nil
}

// pragmaGetJournalMode returns the journal_mode.
// Columns: journal_mode
func (e *Engine) pragmaGetJournalMode() ([]PragmaRow, bool, error) {
	return []PragmaRow{{Values: []interface{}{journalModeString(e.journalMode)}}}, true, nil
}

// pragmaSetJournalMode sets the journal_mode and returns the new mode.
func (e *Engine) pragmaSetJournalMode(mode string) ([]PragmaRow, bool, error) {
	newMode, err := journalModeFromString(mode)
	if err != nil {
		return nil, false, err
	}

	if e.pgr != nil {
		actual, err := e.pgr.SetJournalMode(newMode)
		if err != nil {
			return nil, false, err
		}
		e.journalMode = actual
	} else {
		e.journalMode = newMode
	}

	return []PragmaRow{{Values: []interface{}{journalModeString(e.journalMode)}}}, false, nil
}

// pragmaGetSynchronous returns the synchronous setting.
// Columns: synchronous
func (e *Engine) pragmaGetSynchronous() ([]PragmaRow, bool, error) {
	return []PragmaRow{{Values: []interface{}{e.synchronous}}}, true, nil
}

// pragmaSetSynchronous sets the synchronous level.
func (e *Engine) pragmaSetSynchronous(value interface{}) ([]PragmaRow, bool, error) {
	v := toInt(value)
	if v < 0 || v > 3 {
		return nil, false, fmt.Errorf("synchronous must be 0, 1, 2, or 3")
	}
	e.synchronous = v
	return []PragmaRow{{Values: []interface{}{v}}}, false, nil
}

// pragmaGetForeignKeys returns the foreign_keys setting.
// Columns: foreign_keys
func (e *Engine) pragmaGetForeignKeys() ([]PragmaRow, bool, error) {
	val := 0
	if e.foreignKeys {
		val = 1
	}
	return []PragmaRow{{Values: []interface{}{val}}}, true, nil
}

// pragmaSetForeignKeys enables or disables foreign key enforcement.
func (e *Engine) pragmaSetForeignKeys(value string) ([]PragmaRow, bool, error) {
	switch strings.ToLower(value) {
	case "on", "1", "true", "yes":
		e.foreignKeys = true
	case "off", "0", "false", "no":
		e.foreignKeys = false
	default:
		return nil, false, fmt.Errorf("foreign_keys must be ON or OFF")
	}
	val := 0
	if e.foreignKeys {
		val = 1
	}
	return []PragmaRow{{Values: []interface{}{val}}}, false, nil
}

// pragmaGetCacheSize returns the cache size.
// Columns: cache_size
func (e *Engine) pragmaGetCacheSize() ([]PragmaRow, bool, error) {
	return []PragmaRow{{Values: []interface{}{e.cacheSize}}}, true, nil
}

// pragmaSetCacheSize sets the page cache size.
func (e *Engine) pragmaSetCacheSize(value interface{}) ([]PragmaRow, bool, error) {
	v := toInt(value)
	if v == 0 {
		v = 2000 // default
	}
	e.cacheSize = v

	if e.pgr != nil {
		if err := e.pgr.SetCacheSize(v); err != nil {
			return nil, false, err
		}
	}

	return []PragmaRow{{Values: []interface{}{v}}}, false, nil
}

// pragmaGetPageSize returns the database page size.
// Columns: page_size
func (e *Engine) pragmaGetPageSize() ([]PragmaRow, bool, error) {
	if e.pgr != nil {
		return []PragmaRow{{Values: []interface{}{e.pgr.PageSize()}}}, true, nil
	}
	return []PragmaRow{{Values: []interface{}{e.pageSize}}}, true, nil
}

// pragmaIntegrityCheck performs a basic integrity check.
// Columns: integrity_check
func (e *Engine) pragmaIntegrityCheck() ([]PragmaRow, bool, error) {
	// Check each table's B-Tree
	var problems []string
	for name, tbl := range e.tables {
		var errs []string
		e.bt.IntegrityCheck(btree.PageNumber(tbl.RootPage), 0, &errs)
		if len(errs) > 0 {
			for _, e := range errs {
				problems = append(problems, fmt.Sprintf("table %s: %s", name, e))
			}
		}
	}

	if len(problems) == 0 {
		return []PragmaRow{{Values: []interface{}{"ok"}}}, true, nil
	}

	var rows []PragmaRow
	for _, p := range problems {
		rows = append(rows, PragmaRow{Values: []interface{}{p}})
	}
	return rows, true, nil
}

// pragmaCompileOptions returns the compile-time options.
// Columns: compile_option
func (e *Engine) pragmaCompileOptions() ([]PragmaRow, bool, error) {
	// Report the standard compile options for this Go implementation
	options := []string{
		"THREADSAFE=1",
		"MAX_PAGE_SIZE=65536",
		"DEFAULT_PAGE_SIZE=4096",
		"DEFAULT_CACHE_SIZE=2000",
		"DEFAULT_JOURNAL_MODE=memory",
		"DEFAULT_SYNCHRONOUS=2",
		"ENABLE_COLUMN_METADATA",
		"ENABLE_DBSTAT_VTAB",
		"ENABLE_FTS5",
		"ENABLE_RTREE",
		"ENABLE_SESSION",
		"OMIT_LOAD_EXTENSION",
	}
	var rows []PragmaRow
	for _, opt := range options {
		rows = append(rows, PragmaRow{Values: []interface{}{opt}})
	}
	return rows, true, nil
}

// toInt converts a pragma value to an integer.
func toInt(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case int64:
		return int(val)
	case float64:
		return int(val)
	case string:
		i, err := strconv.Atoi(val)
		if err == nil {
			return i
		}
		return 0
	default:
		return 0
	}
}

// pragmaForeignKeyList returns the list of FKs for a table.
func (e *Engine) pragmaForeignKeyList(tableName string) ([]PragmaRow, bool, error) {
	rows, err := e.getForeignKeyList(tableName)
	return rows, true, err
}

// pragmaForeignKeyCheck verifies FK integrity.
func (e *Engine) pragmaForeignKeyCheck() ([]PragmaRow, bool, error) {
	violations := e.fkIntegrityCheck()
	var rows []PragmaRow
	for _, v := range violations {
		rows = append(rows, PragmaRow{Values: []interface{}{v}})
	}
	if len(rows) == 0 {
		rows = append(rows, PragmaRow{Values: []interface{}{"ok"}})
	}
	return rows, true, nil
}
