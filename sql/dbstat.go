// Package sql implements the dbstat virtual table for sqlite-go.
// dbstat provides information about btree page usage within the database,
// similar to SQLite's built-in dbstat extension.
package sql

import (
	"encoding/binary"
	"fmt"

	"github.com/sqlite-go/sqlite-go/pager"
	"github.com/sqlite-go/sqlite-go/vdbe"
)

// ---------------------------------------------------------------------------
// dbstat virtual table
// ---------------------------------------------------------------------------

// dbstatTable implements VirtualTable for the dbstat module.
type dbstatTable struct {
	eng     *Engine
	schema  string // database schema name (usually "main")
}

// dbstatColumnDef describes one column of the dbstat virtual table.
var dbstatColumns = []ColumnInfo{
	{Name: "name", Type: "TEXT"},
	{Name: "path", Type: "TEXT"},
	{Name: "pageno", Type: "INTEGER"},
	{Name: "pagetype", Type: "TEXT"},
	{Name: "ncell", Type: "INTEGER"},
	{Name: "payload", Type: "INTEGER"},
	{Name: "unused", Type: "INTEGER"},
	{Name: "mx_payload", Type: "INTEGER"},
	{Name: "pgoffset", Type: "INTEGER"},
	{Name: "pgsize", Type: "INTEGER"},
	{Name: "schema", Type: "TEXT"},     // HIDDEN
	{Name: "aggregate", Type: "INTEGER"}, // HIDDEN
}

// statCursor scans pages in the database.
type statCursor struct {
	vt      *dbstatTable
	pages   []statPage
	rowIdx  int
}

// statPage holds one row's worth of dbstat data.
type statPage struct {
	Name      string
	Path      string
	PageNo    int64
	PageType  string
	NCell     int64
	Payload   int64
	Unused    int64
	MxPayload int64
	PgOffset  int64
	PgSize    int64
}

func (t *dbstatTable) BestIndex(idxInfo *IndexInfo) error {
	idxInfo.EstimatedCost = 1000.0
	return nil
}

func (t *dbstatTable) Open() (VirtualCursor, error) {
	return &statCursor{vt: t}, nil
}

func (t *dbstatTable) Disconnect() {}

func (t *dbstatTable) Destroy() {}

func (c *statCursor) Filter(idxNum int, idxStr string, args []vdbe.Value) error {
	c.pages = nil
	c.rowIdx = 0

	eng := c.vt.eng
	if eng == nil || eng.pgr == nil {
		return nil
	}

	pageSize := eng.pgr.PageSize()
	if pageSize <= 0 {
		pageSize = 4096
	}

	// Iterate over all known tables and collect page information
	for _, tbl := range eng.tables {
		if tbl.RootPage <= 0 {
			continue
		}
		if err := c.collectPages(tbl.Name, tbl.RootPage, pageSize, ""); err != nil {
			// Skip tables that can't be read
			continue
		}
	}

	return nil
}

func (c *statCursor) collectPages(name string, rootPage int, pageSize int, parentPath string) error {
	eng := c.vt.eng

	// Build path for this page
	path := parentPath
	if path == "" {
		path = fmt.Sprintf("/%03x", rootPage)
	}

	pg, err := eng.pgr.GetPage(pager.PageNumber(rootPage))
	if err != nil {
		// Emit a placeholder row
		c.pages = append(c.pages, statPage{
			Name:     name,
			Path:     path,
			PageNo:   int64(rootPage),
			PageType: "?",
			PgSize:   int64(pageSize),
			PgOffset: int64((rootPage - 1) * pageSize),
		})
		return nil
	}
	defer eng.pgr.ReleasePage(pg)

	data := pg.Data
	if len(data) < 8 {
		return nil
	}

	pageType := data[0]
	nCells := int(binary.BigEndian.Uint16(data[3:5]))

	var typeStr string
	var payload, unused, mxPayload int64

	switch pageType {
	case 0x02: // Interior index b-tree
		typeStr = "interior_index"
		cellContent := 0
		headerSize := 12
		if rootPage == 1 {
			headerSize = 100 + 12
		}
		// Estimate payload from free space
		freeStart := int(binary.BigEndian.Uint16(data[1:3]))
		cellContent = int(binary.BigEndian.Uint16(data[5:7]))
		if cellContent == 0 {
			cellContent = pageSize
		}
		unused = int64(freeStart-headerSize) + int64(cellContent-pageSize)
		if unused < 0 {
			unused = 0
		}
	case 0x05: // Interior table b-tree
		typeStr = "interior_table"
		headerSize := 12
		if rootPage == 1 {
			headerSize = 100 + 12
		}
		freeStart := int(binary.BigEndian.Uint16(data[1:3]))
		cellContent := int(binary.BigEndian.Uint16(data[5:7]))
		if cellContent == 0 {
			cellContent = pageSize
		}
		unused = int64(freeStart-headerSize) + int64(cellContent-pageSize)
		if unused < 0 {
			unused = 0
		}

		// Recurse into child pages
		rightChild := int(binary.BigEndian.Uint32(data[8:12]))
		offset := headerSize
		for i := 0; i < nCells && offset+4 < len(data); i++ {
			cellPtr := int(binary.BigEndian.Uint16(data[offset : offset+2]))
			offset += 2
			if cellPtr+4 <= len(data) {
				childPage := int(binary.BigEndian.Uint32(data[cellPtr : cellPtr+4]))
				childPath := path + fmt.Sprintf("/%03x", childPage)
				_ = c.collectPages(name, childPage, pageSize, childPath)
			}
		}
		if rightChild > 0 {
			childPath := path + fmt.Sprintf("/%03x", rightChild)
			_ = c.collectPages(name, rightChild, pageSize, childPath)
		}

	case 0x0a: // Leaf index b-tree
		typeStr = "leaf_index"
		headerSize := 8
		if rootPage == 1 {
			headerSize = 100 + 8
		}
		freeStart := int(binary.BigEndian.Uint16(data[1:3]))
		cellContent := int(binary.BigEndian.Uint16(data[5:7]))
		if cellContent == 0 {
			cellContent = pageSize
		}
		// Estimate payload from cell data
		cellOff := headerSize
		for i := 0; i < nCells && cellOff+2 < len(data); i++ {
			cp := int(binary.BigEndian.Uint16(data[cellOff : cellOff+2]))
			cellOff += 2
			if cp+2 < len(data) {
				// Read varint for payload size
				_, varLen := readVarintDbstat(data[cp:])
				pl := int64(varLen)
				if pl > mxPayload {
					mxPayload = pl
				}
				payload += pl
			}
		}
		unused = int64(freeStart-headerSize) + int64(cellContent-pageSize)
		if unused < 0 {
			unused = 0
		}

	case 0x0d: // Leaf table b-tree
		typeStr = "leaf_table"
		headerSize := 8
		if rootPage == 1 {
			headerSize = 100 + 8
		}
		freeStart := int(binary.BigEndian.Uint16(data[1:3]))
		cellContent := int(binary.BigEndian.Uint16(data[5:7]))
		if cellContent == 0 {
			cellContent = pageSize
		}

		// Walk cells to compute payload
		cellOff := headerSize
		for i := 0; i < nCells && cellOff+2 < len(data); i++ {
			cp := int(binary.BigEndian.Uint16(data[cellOff : cellOff+2]))
			cellOff += 2
			if cp+2 < len(data) {
				// Varint: payload size
				pl, n := readVarintDbstat(data[cp:])
				if n > 0 && cp+n < len(data) {
					// Varint: rowid (skip)
					_, rn := readVarintDbstat(data[cp+n:])
					localPayload := computeLocalPayload(pageSize, pl)
					payload += localPayload
					if localPayload > mxPayload {
						mxPayload = localPayload
					}
					_ = rn
				}
			}
		}
		unused = int64(freeStart-headerSize) + int64(cellContent-pageSize)
		if unused < 0 {
			unused = 0
		}

	default:
		typeStr = "overflow"
		// Overflow page
		if len(data) >= 4 {
			unused = int64(pageSize - 4)
		}
	}

	c.pages = append(c.pages, statPage{
		Name:      name,
		Path:      path,
		PageNo:    int64(rootPage),
		PageType:  typeStr,
		NCell:     int64(nCells),
		Payload:   payload,
		Unused:    unused,
		MxPayload: mxPayload,
		PgOffset:  int64((rootPage - 1) * pageSize),
		PgSize:    int64(pageSize),
	})

	return nil
}

func (c *statCursor) Next() error {
	c.rowIdx++
	return nil
}

func (c *statCursor) Column(col int) (vdbe.Value, error) {
	if c.rowIdx < 0 || c.rowIdx >= len(c.pages) {
		return vdbe.Value{Type: "null"}, nil
	}
	p := c.pages[c.rowIdx]
	switch col {
	case 0:
		return vdbe.Value{Type: "text", Bytes: []byte(p.Name)}, nil
	case 1:
		return vdbe.Value{Type: "text", Bytes: []byte(p.Path)}, nil
	case 2:
		return vdbe.Value{Type: "int", IntVal: p.PageNo}, nil
	case 3:
		return vdbe.Value{Type: "text", Bytes: []byte(p.PageType)}, nil
	case 4:
		return vdbe.Value{Type: "int", IntVal: p.NCell}, nil
	case 5:
		return vdbe.Value{Type: "int", IntVal: p.Payload}, nil
	case 6:
		return vdbe.Value{Type: "int", IntVal: p.Unused}, nil
	case 7:
		return vdbe.Value{Type: "int", IntVal: p.MxPayload}, nil
	case 8:
		return vdbe.Value{Type: "int", IntVal: p.PgOffset}, nil
	case 9:
		return vdbe.Value{Type: "int", IntVal: p.PgSize}, nil
	case 10:
		return vdbe.Value{Type: "text", Bytes: []byte("main")}, nil
	case 11:
		return vdbe.Value{Type: "int", IntVal: 0}, nil
	}
	return vdbe.Value{Type: "null"}, nil
}

func (c *statCursor) Rowid() (int64, error) {
	return int64(c.rowIdx), nil
}

func (c *statCursor) Eof() bool {
	return c.rowIdx >= len(c.pages)
}

func (c *statCursor) Close() error {
	c.pages = nil
	return nil
}

// ---------------------------------------------------------------------------
// dbstat module factory
// ---------------------------------------------------------------------------

// DbstatModule is a VirtualTableFactory that creates dbstat virtual tables.
func DbstatModule(eng *Engine, isCreate bool, args []string) (VirtualTable, []ColumnInfo, error) {
	schema := "main"
	if len(args) > 0 && args[0] != "" {
		schema = args[0]
	}
	return &dbstatTable{eng: eng, schema: schema}, dbstatColumns, nil
}

// ---------------------------------------------------------------------------
// PRAGMA stats support
// ---------------------------------------------------------------------------

// Stats returns a snapshot of database page usage statistics.
// It is exposed via PRAGMA stats.
func (e *Engine) Stats() ([]StatRow, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil, fmt.Errorf("database is closed")
	}

	pageSize := e.pgr.PageSize()
	if pageSize <= 0 {
		pageSize = 4096
	}

	totalPages := e.pgr.PageCount()

	var rows []StatRow
	for _, tbl := range e.tables {
		if tbl.RootPage <= 0 {
			continue
		}

		pg, err := e.pgr.GetPage(pager.PageNumber(tbl.RootPage))
		if err != nil {
			continue
		}

		data := pg.Data
		nCells := 0
		pageType := byte(0)
		if len(data) > 0 {
			pageType = data[0]
			if len(data) >= 5 {
				nCells = int(binary.BigEndian.Uint16(data[3:5]))
			}
		}
		e.pgr.ReleasePage(pg)

		rows = append(rows, StatRow{
			TableName:  tbl.Name,
			RootPage:   tbl.RootPage,
			PageType:   pageTypeName(pageType),
			NumCells:   nCells,
			TotalPages: totalPages,
			PageSize:   pageSize,
		})
	}

	return rows, nil
}

// StatRow holds one row of PRAGMA stats output.
type StatRow struct {
	TableName  string
	RootPage   int
	PageType   string
	NumCells   int
	TotalPages int
	PageSize   int
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func pageTypeName(t byte) string {
	switch t {
	case 0x02:
		return "interior_index"
	case 0x05:
		return "interior_table"
	case 0x0a:
		return "leaf_index"
	case 0x0d:
		return "leaf_table"
	default:
		return "unknown"
	}
}

func readVarintDbstat(buf []byte) (int64, int) {
	if len(buf) == 0 {
		return 0, 0
	}
	var v uint64
	for i := 0; i < 9 && i < len(buf); i++ {
		v = (v << 7) | uint64(buf[i]&0x7f)
		if buf[i]&0x80 == 0 {
			return int64(v), i + 1
		}
	}
	if len(buf) >= 9 {
		v = (v << 8) | uint64(buf[8])
		return int64(v), 9
	}
	return int64(v), len(buf)
}

func computeLocalPayload(pageSize int, payloadLen int64) int64 {
	// Max local payload: U = pageSize - 35, X = ((U-12)*32/255)-23
	usable := pageSize - 0 // Reserved bytes = 0 for simplicity
	maxLocal := (usable - 12) * 32 / 255
	minLocal := (usable - 12) * 32 / 255 - 23

	if payloadLen <= int64(maxLocal) {
		return payloadLen
	}
	return int64(minLocal) + ((int64(payloadLen) - int64(minLocal)) % int64(usable-4))
}
