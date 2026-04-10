// Package sql implements incremental blob I/O for sqlite-go.
// Blob handles allow reading and writing large BLOB values without
// loading the entire value into memory at once.
package sql

import (
	"fmt"

	"github.com/sqlite-go/sqlite-go/btree"
	"github.com/sqlite-go/sqlite-go/vdbe"
)

// Blob provides incremental read/write access to a single BLOB value.
// It is modelled after SQLite's sqlite3_blob family of APIs.
type Blob struct {
	eng      *Engine
	cursor   btree.BTCursor
	rowid    int64
	colIdx   int
	offset   int
	size     int
	writable bool
	closed   bool
}

// OpenBlob opens a blob handle for incremental I/O on the given table/column/rowid.
//
// Parameters:
//   - table: table name
//   - column: column name
//   - rowid: the rowid of the target row
//   - writable: true to open for read-write, false for read-only
func (e *Engine) OpenBlob(table, column string, rowid int64, writable bool) (*Blob, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil, fmt.Errorf("database is closed")
	}

	tbl, ok := e.tables[table]
	if !ok {
		// Check virtual tables
		if vt := e.lookupVTab(table); vt != nil {
			return nil, fmt.Errorf("cannot open blob on virtual table: %s", table)
		}
		return nil, fmt.Errorf("no such table: %s", table)
	}

	// Find column index
	colIdx := -1
	for i, col := range tbl.Columns {
		if col.Name == column {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return nil, fmt.Errorf("no such column: %s", column)
	}

	// Open a cursor on the table's btree
	cursor, err := e.bt.Cursor(btree.PageNumber(tbl.RootPage), writable)
	if err != nil {
		return nil, fmt.Errorf("open cursor: %w", err)
	}

	// Seek to the row
	sr, err := cursor.SeekRowid(btree.RowID(rowid))
	if err != nil {
		cursor.Close()
		return nil, fmt.Errorf("seek rowid %d: %w", rowid, err)
	}
	if sr != btree.SeekFound {
		cursor.Close()
		return nil, fmt.Errorf("no such rowid: %d", rowid)
	}

	// Read the record to determine the blob size
	data, err := cursor.Data()
	if err != nil {
		cursor.Close()
		return nil, fmt.Errorf("read data: %w", err)
	}

	values, err := vdbe.ParseRecord(data)
	if err != nil {
		cursor.Close()
		return nil, fmt.Errorf("parse record: %w", err)
	}

	if colIdx >= len(values) {
		cursor.Close()
		return nil, fmt.Errorf("column index out of range")
	}

	val := values[colIdx]
	blobSize := 0
	if val.Type == "blob" {
		blobSize = len(val.Bytes)
	} else if val.Type == "text" {
		blobSize = len(val.Bytes)
	}

	return &Blob{
		eng:      e,
		cursor:   cursor,
		rowid:    rowid,
		colIdx:   colIdx,
		size:     blobSize,
		writable: writable,
	}, nil
}

// Size returns the total size of the blob in bytes.
func (b *Blob) Size() int {
	return b.size
}

// Read reads up to len(buf) bytes from the blob starting at the current offset.
// Returns the number of bytes read and advances the offset.
func (b *Blob) Read(buf []byte) (int, error) {
	if b.closed {
		return 0, fmt.Errorf("blob is closed")
	}

	// Re-read the current row data
	data, err := b.cursor.Data()
	if err != nil {
		return 0, err
	}
	values, err := vdbe.ParseRecord(data)
	if err != nil {
		return 0, err
	}
	if b.colIdx >= len(values) {
		return 0, fmt.Errorf("column index out of range")
	}

	val := values[b.colIdx]
	var blobData []byte
	if val.Type == "blob" || val.Type == "text" {
		blobData = val.Bytes
	} else {
		return 0, nil
	}

	if b.offset >= len(blobData) {
		return 0, nil // EOF
	}

	n := copy(buf, blobData[b.offset:])
	b.offset += n
	return n, nil
}

// ReadAt reads len(buf) bytes from the blob starting at offset off.
// It does not change the blob's internal offset.
func (b *Blob) ReadAt(buf []byte, off int) (int, error) {
	if b.closed {
		return 0, fmt.Errorf("blob is closed")
	}

	data, err := b.cursor.Data()
	if err != nil {
		return 0, err
	}
	values, err := vdbe.ParseRecord(data)
	if err != nil {
		return 0, err
	}
	if b.colIdx >= len(values) {
		return 0, fmt.Errorf("column index out of range")
	}

	val := values[b.colIdx]
	var blobData []byte
	if val.Type == "blob" || val.Type == "text" {
		blobData = val.Bytes
	} else {
		return 0, nil
	}

	if off >= len(blobData) {
		return 0, nil
	}

	n := copy(buf, blobData[off:])
	return n, nil
}

// Write writes buf to the blob starting at the current offset.
// The blob must have been opened for writing.
func (b *Blob) Write(buf []byte) (int, error) {
	if b.closed {
		return 0, fmt.Errorf("blob is closed")
	}
	if !b.writable {
		return 0, fmt.Errorf("blob not opened for writing")
	}

	// Read the current full row
	data, err := b.cursor.Data()
	if err != nil {
		return 0, err
	}
	values, err := vdbe.ParseRecord(data)
	if err != nil {
		return 0, err
	}
	if b.colIdx >= len(values) {
		return 0, fmt.Errorf("column index out of range")
	}

	// Modify the blob column
	val := values[b.colIdx]
	var blobData []byte
	if val.Type == "blob" || val.Type == "text" {
		blobData = make([]byte, len(val.Bytes))
		copy(blobData, val.Bytes)
	} else {
		// Convert to blob
		blobData = make([]byte, b.size)
		val.Type = "blob"
	}

	// Ensure blob is large enough
	if b.offset+len(buf) > len(blobData) {
		newData := make([]byte, b.offset+len(buf))
		copy(newData, blobData)
		blobData = newData
	}

	n := copy(blobData[b.offset:], buf)
	b.offset += n
	values[b.colIdx] = vdbe.Value{Type: val.Type, Bytes: blobData}

	// Rebuild the record
	rb := vdbe.NewRecordBuilder()
	for _, v := range values {
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
	newData := rb.Build()

	// Re-insert the row
	keyBuf := make([]byte, 9)
	keyLen := encodeVarintKeyBlob(keyBuf, b.rowid)

	if inserter, ok := b.eng.bt.(btreeInserter); ok {
		if err := inserter.Insert(b.cursor, keyBuf[:keyLen], newData, btree.RowID(b.rowid), btree.SeekFound); err != nil {
			return 0, fmt.Errorf("write blob: %w", err)
		}
	}

	b.size = len(blobData)
	return n, nil
}

// Seek sets the offset for the next Read or Write.
// whence: 0 = start, 1 = current, 2 = end.
func (b *Blob) Seek(offset int, whence int) int {
	switch whence {
	case 0: // start
		b.offset = offset
	case 1: // current
		b.offset += offset
	case 2: // end
		b.offset = b.size + offset
	}
	if b.offset < 0 {
		b.offset = 0
	}
	if b.offset > b.size {
		b.offset = b.size
	}
	return b.offset
}

// Close releases the blob handle.
func (b *Blob) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	if b.cursor != nil {
		return b.cursor.Close()
	}
	return nil
}

// btreeInserter is a private interface to access the Insert method on the BTree.
type btreeInserter interface {
	Insert(cursor btree.BTCursor, key []byte, data []byte, rowid btree.RowID, seekResult btree.SeekResult) error
}

func encodeVarintKeyBlob(buf []byte, rowid int64) int {
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
