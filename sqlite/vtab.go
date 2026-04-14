package sqlite

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"
)

// ---------------------------------------------------------------------------
// Virtual table interfaces and registry (sqlite package)
// ---------------------------------------------------------------------------

// VTab is the interface that virtual table implementations must satisfy.
type VTab interface {
	VTabBestIndex(idxInfo *VTabIndexInfo) error
	VTabOpen() (VTabCursor, error)
	VTabDisconnect()
	VTabDestroy()
	VTabUpdate(args []interface{}) (int64, error)
}

// VTabCursor is the scanning cursor interface for virtual tables.
type VTabCursor interface {
	VTabFilter(idxNum int, idxStr string, args []interface{}) error
	VTabNext() error
	VTabColumn(col int) (interface{}, error)
	VTabRowid() (int64, error)
	VTabEof() bool
	VTabClose() error
}

// VTabIndexInfo is passed to BestIndex.
type VTabIndexInfo struct {
	Constraints     []VTabConstraint
	IdxNum          int
	IdxStr          string
	OrderByConsumed bool
	EstimatedCost   float64
	EstimatedRows   int64
}

// VTabConstraint represents a WHERE-clause constraint.
type VTabConstraint struct {
	Column   int
	Op       string
	Usable   bool
	Omit     bool
	ArgValue interface{}
}

// VTabColumn describes a column of a virtual table.
type VTabColumn struct {
	Name   string
	Type   string
	Hidden bool
}

// VTabModuleFactory creates a new VTab instance.
type VTabModuleFactory func(db *Database, isCreate bool, args []string) (VTab, []VTabColumn, error)

// vtabModule stores a registered virtual table module.
type vtabModule struct {
	name    string
	factory VTabModuleFactory
}

// vtabTableEntry stores an instantiated virtual table bound to a table name.
type vtabTableEntry struct {
	moduleName string
	vtab       VTab
	columns    []VTabColumn
}

// Global module registry
var (
	vtabModulesMu sync.Mutex
	vtabModules   = make(map[string]*vtabModule)
)

// RegisterVTabModule registers a virtual table module globally.
func RegisterVTabModule(name string, factory VTabModuleFactory) {
	vtabModulesMu.Lock()
	defer vtabModulesMu.Unlock()
	vtabModules[strings.ToLower(name)] = &vtabModule{
		name:    strings.ToLower(name),
		factory: factory,
	}
}

func init() {
	RegisterVTabModule("rtree", rtreeModuleFactory)
	RegisterVTabModule("rtree_i32", rtreeModuleFactory)
	RegisterVTabModule("fts5", fts5ModuleFactory)
	RegisterVTabModule("fts4", fts5ModuleFactory)
	RegisterVTabModule("fts3", fts5ModuleFactory)
}

// ---------------------------------------------------------------------------
// R*Tree virtual table implementation
// ---------------------------------------------------------------------------

type rtreeTable struct {
	db        *Database
	columns   []VTabColumn
	rows      []rtreeRow
	nextID    int64
}

type rtreeRow struct {
	rowid int64
	coord []float64 // coordinate values (x0, x1, y0, y1, ...)
}

func rtreeModuleFactory(db *Database, isCreate bool, args []string) (VTab, []VTabColumn, error) {
	if len(args) < 1 {
		return nil, nil, fmt.Errorf("rtree module requires at least an id column")
	}
	cols := []VTabColumn{{Name: args[0], Type: "INTEGER"}}
	for i := 1; i < len(args); i++ {
		cols = append(cols, VTabColumn{Name: args[i], Type: "REAL"})
	}
	return &rtreeTable{db: db, columns: cols, nextID: 1}, cols, nil
}

func (t *rtreeTable) VTabBestIndex(idxInfo *VTabIndexInfo) error {
	idxInfo.EstimatedCost = 10.0
	idxInfo.EstimatedRows = int64(len(t.rows))
	return nil
}

func (t *rtreeTable) VTabOpen() (VTabCursor, error) {
	return &rtreeCursor{table: t}, nil
}

func (t *rtreeTable) VTabDisconnect() {}

func (t *rtreeTable) VTabDestroy() { t.rows = nil }

func (t *rtreeTable) VTabUpdate(args []interface{}) (int64, error) {
	if len(args) == 0 {
		return 0, fmt.Errorf("rtree update: no arguments")
	}

	// args layout from the sqlite vtab interface:
	//   INSERT: args[0]=nil, args[1]=newRowid(nil=auto), args[2:]=colValues
	//   DELETE: args[0]=oldRowid, args[1]=nil
	//   UPDATE: args[0]=oldRowid, args[1]=newRowid, args[2:]=colValues
	//
	// For rtree(id,x0,x1): colValues = [id, x0, x1]
	// The id column value is the rowid; coordinates are x0, x1, ...

	switch {
	case args[0] == nil && len(args) >= 2: // INSERT
		var rowid int64
		if len(args) > 2 && args[2] != nil {
			rowid = toInt64(args[2]) // use the id column value
		}
		if rowid == 0 && args[1] != nil {
			rowid = toInt64(args[1])
		}
		if rowid == 0 {
			rowid = t.nextID
		}
		if rowid >= t.nextID {
			t.nextID = rowid + 1
		}
		// Coordinates start at args[3] (skip args[2] which is the id column)
		coord := make([]float64, 0, len(args)-3)
		for i := 3; i < len(args); i++ {
			coord = append(coord, toFloat64(args[i]))
		}
		t.rows = append(t.rows, rtreeRow{rowid: rowid, coord: coord})
		return rowid, nil

	case args[0] != nil && len(args) >= 2 && args[1] == nil: // DELETE
		delID := toInt64(args[0])
		for i, r := range t.rows {
			if r.rowid == delID {
				t.rows = append(t.rows[:i], t.rows[i+1:]...)
				return delID, nil
			}
		}
		return delID, nil

	case args[0] != nil && len(args) >= 2 && args[1] != nil: // UPDATE
		oldID := toInt64(args[0])
		newID := toInt64(args[1])
		coord := make([]float64, 0, len(args)-3)
		for i := 3; i < len(args); i++ {
			coord = append(coord, toFloat64(args[i]))
		}
		for i, r := range t.rows {
			if r.rowid == oldID {
				t.rows[i] = rtreeRow{rowid: newID, coord: coord}
				return newID, nil
			}
		}
		t.rows = append(t.rows, rtreeRow{rowid: newID, coord: coord})
		return newID, nil
	}

	return 0, fmt.Errorf("rtree update: invalid arguments")
}

type rtreeCursor struct {
	table *rtreeTable
	rows  []rtreeRow
	idx   int
}

func (c *rtreeCursor) VTabFilter(idxNum int, idxStr string, args []interface{}) error {
	c.rows = make([]rtreeRow, len(c.table.rows))
	copy(c.rows, c.table.rows)
	sort.Slice(c.rows, func(i, j int) bool {
		return c.rows[i].rowid < c.rows[j].rowid
	})
	c.idx = 0
	return nil
}

func (c *rtreeCursor) VTabNext() error { c.idx++; return nil }

func (c *rtreeCursor) VTabColumn(col int) (interface{}, error) {
	if c.idx < 0 || c.idx >= len(c.rows) {
		return nil, nil
	}
	r := c.rows[c.idx]
	if col == 0 {
		return r.rowid, nil
	}
	coordIdx := col - 1
	if coordIdx < len(r.coord) {
		return r.coord[coordIdx], nil
	}
	return nil, nil
}

func (c *rtreeCursor) VTabRowid() (int64, error) {
	if c.idx < 0 || c.idx >= len(c.rows) {
		return 0, nil
	}
	return c.rows[c.idx].rowid, nil
}

func (c *rtreeCursor) VTabEof() bool    { return c.idx >= len(c.rows) }
func (c *rtreeCursor) VTabClose() error { c.rows = nil; return nil }

// ---------------------------------------------------------------------------
// FTS5 virtual table implementation
// ---------------------------------------------------------------------------

type fts5Table struct {
	db        *Database
	columns   []VTabColumn
	rows      []fts5Row
	nextRowID int64
}

type fts5Row struct {
	rowid  int64
	values []string
}

func fts5ModuleFactory(db *Database, isCreate bool, args []string) (VTab, []VTabColumn, error) {
	if len(args) == 0 {
		return nil, nil, fmt.Errorf("fts5 module requires column names")
	}
	var colNames []string
	for _, arg := range args {
		lower := strings.ToLower(strings.TrimSpace(arg))
		if strings.HasPrefix(lower, "content=") ||
			strings.HasPrefix(lower, "content_table=") ||
			strings.HasPrefix(lower, "tokenize=") ||
			strings.HasPrefix(lower, "prefix=") ||
			strings.HasPrefix(lower, "rank=") ||
			strings.HasPrefix(lower, "columnsize=") ||
			strings.HasPrefix(lower, "detail=") ||
			strings.HasPrefix(lower, "notindexed=") ||
			strings.HasPrefix(lower, "order=") {
			continue
		}
		parts := strings.Fields(arg)
		colNames = append(colNames, parts[0])
	}
	if len(colNames) == 0 {
		return nil, nil, fmt.Errorf("fts5 module requires at least one column")
	}
	cols := make([]VTabColumn, len(colNames))
	for i, name := range colNames {
		cols[i] = VTabColumn{Name: name, Type: "TEXT"}
	}
	return &fts5Table{db: db, columns: cols, nextRowID: 1}, cols, nil
}

func (t *fts5Table) VTabBestIndex(idxInfo *VTabIndexInfo) error {
	for i, c := range idxInfo.Constraints {
		if c.Op == "MATCH" {
			idxInfo.Constraints[i].Omit = true
			idxInfo.EstimatedCost = 1.0
			return nil
		}
	}
	idxInfo.EstimatedCost = 100.0
	idxInfo.EstimatedRows = int64(len(t.rows))
	return nil
}

func (t *fts5Table) VTabOpen() (VTabCursor, error) {
	return &fts5Cursor{table: t}, nil
}

func (t *fts5Table) VTabDisconnect() {}
func (t *fts5Table) VTabDestroy()    { t.rows = nil }

func (t *fts5Table) VTabUpdate(args []interface{}) (int64, error) {
	if len(args) == 0 {
		return 0, fmt.Errorf("fts5 update: no arguments")
	}

	if args[0] == nil && len(args) >= 2 { // INSERT
		var rowid int64
		if args[1] != nil {
			rowid = toInt64(args[1])
		}
		if rowid == 0 {
			rowid = t.nextRowID
		}
		if rowid >= t.nextRowID {
			t.nextRowID = rowid + 1
		}
		values := make([]string, len(t.columns))
		for i := 2; i < len(args) && i-2 < len(values); i++ {
			values[i-2] = toString(args[i])
		}
		t.rows = append(t.rows, fts5Row{rowid: rowid, values: values})
		return rowid, nil
	}

	if args[0] != nil && len(args) >= 2 && args[1] == nil { // DELETE
		delID := toInt64(args[0])
		for i, r := range t.rows {
			if r.rowid == delID {
				t.rows = append(t.rows[:i], t.rows[i+1:]...)
				return delID, nil
			}
		}
		return delID, nil
	}

	if args[0] != nil && len(args) >= 2 && args[1] != nil { // UPDATE
		oldID := toInt64(args[0])
		newID := toInt64(args[1])
		values := make([]string, len(t.columns))
		for i := 2; i < len(args) && i-2 < len(values); i++ {
			values[i-2] = toString(args[i])
		}
		for i, r := range t.rows {
			if r.rowid == oldID {
				t.rows[i] = fts5Row{rowid: newID, values: values}
				return newID, nil
			}
		}
		t.rows = append(t.rows, fts5Row{rowid: newID, values: values})
		return newID, nil
	}

	return 0, fmt.Errorf("fts5 update: invalid arguments")
}

type fts5Cursor struct {
	table      *fts5Table
	rows       []fts5Row
	idx        int
	matchQuery string
}

func (c *fts5Cursor) VTabFilter(idxNum int, idxStr string, args []interface{}) error {
	c.rows = make([]fts5Row, len(c.table.rows))
	copy(c.rows, c.table.rows)
	sort.Slice(c.rows, func(i, j int) bool {
		return c.rows[i].rowid < c.rows[j].rowid
	})
	c.idx = 0
	if len(args) > 0 {
		query := strings.ToLower(toString(args[0]))
		var filtered []fts5Row
		for _, r := range c.rows {
			if fts5MatchRow(r, query) {
				filtered = append(filtered, r)
			}
		}
		c.rows = filtered
		c.idx = 0
	}
	return nil
}

func fts5MatchRow(r fts5Row, query string) bool {
	query = strings.TrimSpace(query)
	if query == "" {
		return true
	}
	tokens := fts5TokenizeQuery(query)
	if len(tokens) == 0 {
		return true
	}
	var parts []string
	for _, v := range r.values {
		parts = append(parts, v)
	}
	docText := strings.ToLower(strings.Join(parts, " "))
	for _, tok := range tokens {
		if tok == "and" || tok == "or" || tok == "not" || tok == "near" {
			continue
		}
		if strings.HasPrefix(tok, "\"") && strings.HasSuffix(tok, "\"") {
			phrase := tok[1 : len(tok)-1]
			if !strings.Contains(docText, phrase) {
				return false
			}
		} else {
			tok = strings.TrimSuffix(tok, "*")
			found := false
			for _, word := range strings.Fields(docText) {
				cleaned := strings.TrimFunc(word, func(r rune) bool {
					return !unicode.IsLetter(r) && !unicode.IsDigit(r)
				})
				if strings.HasPrefix(cleaned, tok) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

func fts5TokenizeQuery(query string) []string {
	var tokens []string
	var current strings.Builder
	inQuote := false
	for _, ch := range query {
		if ch == '"' {
			if inQuote {
				current.WriteRune(ch)
				tokens = append(tokens, current.String())
				current.Reset()
				inQuote = false
			} else {
				current.WriteRune(ch)
				inQuote = true
			}
		} else if ch == ' ' && !inQuote {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		} else {
			current.WriteRune(ch)
		}
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

func (c *fts5Cursor) VTabNext() error { c.idx++; return nil }

func (c *fts5Cursor) VTabColumn(col int) (interface{}, error) {
	if c.idx < 0 || c.idx >= len(c.rows) {
		return nil, nil
	}
	r := c.rows[c.idx]
	if col < len(r.values) {
		return r.values[col], nil
	}
	return nil, nil
}

func (c *fts5Cursor) VTabRowid() (int64, error) {
	if c.idx < 0 || c.idx >= len(c.rows) {
		return 0, nil
	}
	return c.rows[c.idx].rowid, nil
}

func (c *fts5Cursor) VTabEof() bool    { return c.idx >= len(c.rows) }
func (c *fts5Cursor) VTabClose() error { c.rows = nil; return nil }

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

func toFloat64(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0
		}
		return f
	default:
		return 0
	}
}

func toInt64(v interface{}) int64 {
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		if val > math.MaxInt64 {
			return math.MaxInt64
		}
		return int64(val)
	case string:
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return 0
		}
		return i
	default:
		return 0
	}
}

func toString(v interface{}) string {
	switch val := v.(type) {
	case string:
		return val
	case int64:
		return strconv.FormatInt(val, 10)
	case int:
		return strconv.Itoa(val)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case nil:
		return ""
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", v)
	}
}
