// Package sql implements virtual table support for sqlite-go.
// Virtual tables allow external data sources to be accessed as if they were
// regular SQLite tables.
package sql

import (
	"fmt"
	"strings"

	"github.com/sqlite-go/sqlite-go/compile"
	"github.com/sqlite-go/sqlite-go/vdbe"
)

// ---------------------------------------------------------------------------
// Core virtual table interfaces
// ---------------------------------------------------------------------------

// VirtualTable is the interface that virtual table implementations must satisfy.
// It mirrors the sqlite3_vtab family of callbacks in SQLite's C API.
type VirtualTable interface {
	// BestIndex asks the virtual table to plan the best way to access
	// the data.  The implementation fills in idxInfo and returns nil on
	// success.
	BestIndex(idxInfo *IndexInfo) error

	// Open creates a new cursor for scanning the virtual table.
	Open() (VirtualCursor, error)

	// Disconnect is called when the database connection closes.
	Disconnect()

	// Destroy is called when the virtual table is dropped (DROP TABLE).
	Destroy()
}

// VirtualTableFactory creates a new VirtualTable instance.  It replaces
// SQLite's separate xCreate / xConnect callbacks with a single factory.
//
//   - isCreate is true for CREATE VIRTUAL TABLE, false for a reconnect.
//   - args are the module arguments from the CREATE VIRTUAL TABLE statement.
type VirtualTableFactory func(eng *Engine, isCreate bool, args []string) (VirtualTable, []ColumnInfo, error)

// VirtualCursor is the scanning cursor interface for virtual tables.
type VirtualCursor interface {
	// Filter begins a scan. idxNum and idxStr come from BestIndex.
	// args are the constraint values that BestIndex marked as usable.
	Filter(idxNum int, idxStr string, args []vdbe.Value) error

	// Next advances to the next row.
	Next() error

	// Column returns the value of column col.
	Column(col int) (vdbe.Value, error)

	// Rowid returns the 64-bit rowid of the current row.
	Rowid() (int64, error)

	// Eof returns true if the cursor is past the last row.
	Eof() bool

	// Close releases cursor resources.
	Close() error
}

// IndexInfo is passed to BestIndex so the virtual table can advertise its
// preferred access strategy.
type IndexInfo struct {
	// Constraints coming from the WHERE clause.
	Constraints []IndexConstraint

	// OrderBy terms from ORDER BY.
	OrderBy []IndexOrderBy

	// Outputs — filled in by BestInfo.

	// IdxNum is an arbitrary number passed back to Filter.
	IdxNum int

	// IdxStr is an arbitrary string passed back to Filter.
	IdxStr string

	// OrderByConsumed is true when the virtual table will deliver rows
	// already in ORDER BY order.
	OrderByConsumed bool

	// EstimatedCost is the estimated cost of the access strategy.
	EstimatedCost float64

	// EstimatedRows is the estimated row count (0 = unknown).
	EstimatedRows int64
}

// IndexConstraint represents a single WHERE-clause constraint.
type IndexConstraint struct {
	// Column index.  -1 means the rowid column.
	Column int

	// Operator: "=", ">", "<", "<=", ">=", "IN", "LIKE", "GLOB", "MATCH", "REGEXP", "NE".
	Op string

	// Usable is true when the constraint can be used by the virtual table.
	Usable bool

	// Omit is set to true by BestIndex to tell the core not to re-check
	// this constraint (the virtual table will enforce it).
	Omit bool
}

// IndexOrderBy represents one term of the ORDER BY clause.
type IndexOrderBy struct {
	Column int  // Column index
	Desc   bool // true for DESC
}

// ---------------------------------------------------------------------------
// Module registry (per-Engine)
// ---------------------------------------------------------------------------

// moduleEntry stores a registered virtual table module.
type moduleEntry struct {
	name    string
	factory VirtualTableFactory
	// eponymousOnly means the module can only be used as an eponymous table
	// (no explicit CREATE VIRTUAL TABLE needed).
	eponymousOnly bool
}

// vtabTableEntry stores an instantiated virtual table bound to a table name.
type vtabTableEntry struct {
	moduleName string
	vtab       VirtualTable
	columns    []ColumnInfo
}

// CreateModule registers a virtual table module so that
// CREATE VIRTUAL TABLE ... USING <module> ... works.
func (e *Engine) CreateModule(name string, factory VirtualTableFactory) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.modules == nil {
		e.modules = make(map[string]*moduleEntry)
	}
	e.modules[strings.ToLower(name)] = &moduleEntry{
		name:    strings.ToLower(name),
		factory: factory,
	}
}

// CreateEponymousModule registers an eponymous-only virtual table module.
// The virtual table can be queried directly by module name without an
// explicit CREATE VIRTUAL TABLE statement.
func (e *Engine) CreateEponymousModule(name string, factory VirtualTableFactory) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.modules == nil {
		e.modules = make(map[string]*moduleEntry)
	}
	nm := strings.ToLower(name)
	e.modules[nm] = &moduleEntry{
		name:          nm,
		factory:       factory,
		eponymousOnly: true,
	}
}

// ---------------------------------------------------------------------------
// Engine helpers for virtual tables
// ---------------------------------------------------------------------------

// execCreateVirtualTable handles CREATE VIRTUAL TABLE statements.
func (e *Engine) execCreateVirtualTable(tokens []compile.Token) error {
	// Expect: CREATE VIRTUAL TABLE [IF NOT EXISTS] name USING module(args...)
	pos := 0
	expectKeyword(tokens, &pos, "create")
	expectKeyword(tokens, &pos, "virtual")
	expectKeyword(tokens, &pos, "table")

	ifNotExists := false
	if pos < len(tokens) && isKeyword(tokens[pos], "if") {
		expectKeyword(tokens, &pos, "if")
		expectKeyword(tokens, &pos, "not")
		expectKeyword(tokens, &pos, "exists")
		ifNotExists = true
	}

	if pos >= len(tokens) {
		return fmt.Errorf("expected virtual table name")
	}
	tableName := tokens[pos].Value
	pos++

	if ifNotExists && e.vtabTables[vtabTableKey(tableName)] != nil {
		return nil
	}

	expectKeyword(tokens, &pos, "using")
	if pos >= len(tokens) {
		return fmt.Errorf("expected module name after USING")
	}
	moduleName := tokens[pos].Value
	pos++

	// Parse optional arguments inside parentheses
	var args []string
	if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
		pos++ // skip (
		// Collect everything up to matching )
		depth := 1
		var buf strings.Builder
		for pos < len(tokens) && depth > 0 {
			if tokens[pos].Type == compile.TokenLParen {
				depth++
				buf.WriteString(tokens[pos].Value)
			} else if tokens[pos].Type == compile.TokenRParen {
				depth--
				if depth == 0 {
					break
				}
				buf.WriteString(tokens[pos].Value)
			} else if tokens[pos].Type == compile.TokenComma && depth == 1 {
				args = append(args, strings.TrimSpace(buf.String()))
				buf.Reset()
			} else {
				buf.WriteString(tokens[pos].Value)
			}
			pos++
		}
		if buf.Len() > 0 {
			args = append(args, strings.TrimSpace(buf.String()))
		}
		if pos < len(tokens) {
			pos++ // skip )
		}
	}

	mod := e.modules[strings.ToLower(moduleName)]
	if mod == nil {
		return fmt.Errorf("no such module: %s", moduleName)
	}
	if mod.eponymousOnly {
		return fmt.Errorf("module %s is eponymous-only", moduleName)
	}

	vtab, cols, err := mod.factory(e, true, args)
	if err != nil {
		return fmt.Errorf("virtual table constructor: %w", err)
	}

	if e.vtabTables == nil {
		e.vtabTables = make(map[string]*vtabTableEntry)
	}
	e.vtabTables[vtabTableKey(tableName)] = &vtabTableEntry{
		moduleName: strings.ToLower(moduleName),
		vtab:       vtab,
		columns:    cols,
	}
	return nil
}

// lookupVTab resolves a table name to a virtual table entry, checking both
// explicitly created virtual tables and eponymous modules.
func (e *Engine) lookupVTab(name string) *vtabTableEntry {
	key := vtabTableKey(name)
	if e.vtabTables != nil {
		if vt, ok := e.vtabTables[key]; ok {
			return vt
		}
	}
	// Check eponymous modules
	if e.modules != nil {
		if mod, ok := e.modules[key]; ok && mod.eponymousOnly {
			vtab, cols, err := mod.factory(e, false, nil)
			if err != nil {
				return nil
			}
			entry := &vtabTableEntry{
				moduleName: mod.name,
				vtab:       vtab,
				columns:    cols,
			}
			// Cache for this connection
			if e.vtabTables == nil {
				e.vtabTables = make(map[string]*vtabTableEntry)
			}
			e.vtabTables[key] = entry
			return entry
		}
	}
	return nil
}

// QueryVTab runs a SELECT against a virtual table and returns results.
// This is used internally by the engine when a query targets a virtual table.
func (e *Engine) QueryVTab(tableName string, colNames []string, star bool) ([]VTabRow, error) {
	vt := e.lookupVTab(tableName)
	if vt == nil {
		return nil, fmt.Errorf("no such virtual table: %s", tableName)
	}

	// Call BestIndex with no constraints (full scan)
	idxInfo := &IndexInfo{}
	if err := vt.vtab.BestIndex(idxInfo); err != nil {
		return nil, fmt.Errorf("BestIndex: %w", err)
	}

	// Open cursor
	vcursor, err := vt.vtab.Open()
	if err != nil {
		return nil, fmt.Errorf("vtab open: %w", err)
	}
	defer vcursor.Close()

	// Start scan
	if err := vcursor.Filter(idxInfo.IdxNum, idxInfo.IdxStr, nil); err != nil {
		return nil, fmt.Errorf("vtab filter: %w", err)
	}

	var rows []VTabRow
	for !vcursor.Eof() {
		rowid, _ := vcursor.Rowid()

		var values []vdbe.Value
		if star || len(colNames) == 0 {
			// SELECT * — return all columns
			for i := range vt.columns {
				v, err := vcursor.Column(i)
				if err != nil {
					v = vdbe.Value{Type: "null"}
				}
				values = append(values, v)
			}
		} else {
			for _, cn := range colNames {
				idx := -1
				for j, col := range vt.columns {
					if strings.EqualFold(col.Name, cn) {
						idx = j
						break
					}
				}
				if idx >= 0 {
					v, err := vcursor.Column(idx)
					if err != nil {
						v = vdbe.Value{Type: "null"}
					}
					values = append(values, v)
				} else {
					values = append(values, vdbe.Value{Type: "null"})
				}
			}
		}

		rows = append(rows, VTabRow{Rowid: rowid, Values: values})

		if err := vcursor.Next(); err != nil {
			break
		}
	}

	return rows, nil
}

// VTabRow represents a single row from a virtual table query.
type VTabRow struct {
	Rowid  int64
	Values []vdbe.Value
}

// vtabTableKey normalises a table name for use as a map key.
func vtabTableKey(name string) string {
	return strings.ToLower(name)
}
