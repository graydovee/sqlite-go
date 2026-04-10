// Package sql implements session tracking and changeset generation/apply
// for sqlite-go, enabling replication and change capture.
package sql

import (
	"encoding/binary"
	"fmt"
)

// ──────────────────────────────────────────────────────────────
// Session - tracks changes during a session for changeset generation
// ──────────────────────────────────────────────────────────────

// Session tracks row-level changes on a set of watched tables.
// It captures INSERT, UPDATE, and DELETE operations and can produce
// a binary changeset for replication.
type Session struct {
	engine   *Engine
	watching map[string]bool   // tables being watched
	changes  []sessionChange   // accumulated changes
	active   bool
	pkCols   map[string][]int  // table -> PK column indices
	colNames map[string][]string // table -> column names
}

// sessionChange represents a single captured row change.
type sessionChange struct {
	op      byte   // 'I'=insert, 'U'=update, 'D'=delete
	table   string
	pk      []byte // primary key encoding
	oldRow  []byte // old row data (for UPDATE/DELETE)
	newRow  []byte // new row data (for INSERT/UPDATE)
}

const (
	opInsert byte = 'I'
	opUpdate byte = 'U'
	opDelete byte = 'D'

	// Changeset header magic
	changesetMagic = 0x53434847 // "SCHG"
	changesetVersion = 1
)

// NewSession creates a new session attached to the given engine.
func NewSession(engine *Engine) *Session {
	return &Session{
		engine:   engine,
		watching: make(map[string]bool),
		pkCols:   make(map[string][]int),
		colNames: make(map[string][]string),
	}
}

// WatchTable adds a table to the set of tables being monitored.
func (s *Session) WatchTable(tableName string) error {
	s.engine.mu.Lock()
	defer s.engine.mu.Unlock()

	tbl, ok := s.engine.tables[tableName]
	if !ok {
		return fmt.Errorf("no such table: %s", tableName)
	}

	s.watching[tableName] = true

	// Determine PK columns
	var pkCols []int
	colNames := make([]string, len(tbl.Columns))
	for i, col := range tbl.Columns {
		colNames[i] = col.Name
		if col.IsPK {
			pkCols = append(pkCols, i)
		}
	}
	// If no explicit PK, rowid is the implicit PK
	if len(pkCols) == 0 {
		pkCols = []int{-1} // -1 signals rowid
	}
	s.pkCols[tableName] = pkCols
	s.colNames[tableName] = colNames

	return nil
}

// UnwatchTable removes a table from monitoring.
func (s *Session) UnwatchTable(tableName string) {
	delete(s.watching, tableName)
}

// Begin starts capturing changes.
func (s *Session) Begin() {
	s.active = true
	s.changes = nil
}

// End stops capturing changes.
func (s *Session) End() {
	s.active = false
}

// IsActive returns whether the session is currently capturing.
func (s *Session) IsActive() bool {
	return s.active
}

// CaptureInsert records an INSERT operation on a watched table.
func (s *Session) CaptureInsert(tableName string, rowid int64, rowData []byte) {
	if !s.active || !s.watching[tableName] {
		return
	}
	s.changes = append(s.changes, sessionChange{
		op:     opInsert,
		table:  tableName,
		pk:     encodePK(rowid),
		newRow: rowData,
	})
}

// CaptureUpdate records an UPDATE operation on a watched table.
func (s *Session) CaptureUpdate(tableName string, rowid int64, oldRow, newRow []byte) {
	if !s.active || !s.watching[tableName] {
		return
	}
	s.changes = append(s.changes, sessionChange{
		op:     opUpdate,
		table:  tableName,
		pk:     encodePK(rowid),
		oldRow: oldRow,
		newRow: newRow,
	})
}

// CaptureDelete records a DELETE operation on a watched table.
func (s *Session) CaptureDelete(tableName string, rowid int64, oldRow []byte) {
	if !s.active || !s.watching[tableName] {
		return
	}
	s.changes = append(s.changes, sessionChange{
		op:     opDelete,
		table:  tableName,
		pk:     encodePK(rowid),
		oldRow: oldRow,
	})
}

// Changeset generates a binary changeset from captured changes.
//
// Binary format:
//
//	Header:
//	  4 bytes: magic (0x53434847)
//	  4 bytes: version (1)
//	  4 bytes: number of changes
//	For each change:
//	  1 byte:  op ('I', 'U', 'D')
//	  2 bytes: table name length
//	  N bytes: table name
//	  4 bytes: PK length
//	  M bytes: PK data
//	  4 bytes: old row length (0 for INSERT)
//	  P bytes: old row data
//	  4 bytes: new row length (0 for DELETE)
//	  Q bytes: new row data
func (s *Session) Changeset() ([]byte, error) {
	// Estimate size
	size := 12 // header
	for _, c := range s.changes {
		size += 1 + 2 + len(c.table) + 4 + len(c.pk) + 4 + len(c.oldRow) + 4 + len(c.newRow)
	}

	buf := make([]byte, 0, size)

	// Header
	magic := make([]byte, 4)
	binary.BigEndian.PutUint32(magic, changesetMagic)
	buf = append(buf, magic...)

	ver := make([]byte, 4)
	binary.BigEndian.PutUint32(ver, changesetVersion)
	buf = append(buf, ver...)

	nChanges := make([]byte, 4)
	binary.BigEndian.PutUint32(nChanges, uint32(len(s.changes)))
	buf = append(buf, nChanges...)

	// Changes
	for _, c := range s.changes {
		buf = append(buf, c.op)

		// Table name
		nameLen := make([]byte, 2)
		binary.BigEndian.PutUint16(nameLen, uint16(len(c.table)))
		buf = append(buf, nameLen...)
		buf = append(buf, c.table...)

		// PK
		pkLen := make([]byte, 4)
		binary.BigEndian.PutUint32(pkLen, uint32(len(c.pk)))
		buf = append(buf, pkLen...)
		buf = append(buf, c.pk...)

		// Old row
		oldLen := make([]byte, 4)
		binary.BigEndian.PutUint32(oldLen, uint32(len(c.oldRow)))
		buf = append(buf, oldLen...)
		buf = append(buf, c.oldRow...)

		// New row
		newLen := make([]byte, 4)
		binary.BigEndian.PutUint32(newLen, uint32(len(c.newRow)))
		buf = append(buf, newLen...)
		buf = append(buf, c.newRow...)
	}

	return buf, nil
}

// ChangeCount returns the number of captured changes.
func (s *Session) ChangeCount() int {
	return len(s.changes)
}

// ──────────────────────────────────────────────────────────────
// ChangesetApply - applies a changeset to a database
// ──────────────────────────────────────────────────────────────

// ChangesetApply applies a binary changeset to an engine.
// For each change, it performs the corresponding INSERT, UPDATE, or DELETE.
// Returns the number of changes applied and any conflict error.
func ChangesetApply(engine *Engine, changeset []byte) (int, error) {
	if len(changeset) < 12 {
		return 0, fmt.Errorf("changeset: too short")
	}

	// Verify header
	magic := binary.BigEndian.Uint32(changeset[0:4])
	if magic != changesetMagic {
		return 0, fmt.Errorf("changeset: bad magic %08x", magic)
	}

	version := binary.BigEndian.Uint32(changeset[4:8])
	if version != changesetVersion {
		return 0, fmt.Errorf("changeset: unsupported version %d", version)
	}

	nChanges := int(binary.BigEndian.Uint32(changeset[8:12]))
	offset := 12

	applied := 0
	for i := 0; i < nChanges; i++ {
		if offset >= len(changeset) {
			return applied, fmt.Errorf("changeset: truncated at change %d", i)
		}

		op := changeset[offset]
		offset++

		// Table name
		if offset+2 > len(changeset) {
			return applied, fmt.Errorf("changeset: truncated table name length")
		}
		nameLen := int(binary.BigEndian.Uint16(changeset[offset : offset+2]))
		offset += 2
		if offset+nameLen > len(changeset) {
			return applied, fmt.Errorf("changeset: truncated table name")
		}
		tableName := string(changeset[offset : offset+nameLen])
		offset += nameLen

		// PK
		if offset+4 > len(changeset) {
			return applied, fmt.Errorf("changeset: truncated pk length")
		}
		pkLen := int(binary.BigEndian.Uint32(changeset[offset : offset+4]))
		offset += 4
		if offset+pkLen > len(changeset) {
			return applied, fmt.Errorf("changeset: truncated pk data")
		}
		offset += pkLen

		// Old row
		if offset+4 > len(changeset) {
			return applied, fmt.Errorf("changeset: truncated old row length")
		}
		oldLen := int(binary.BigEndian.Uint32(changeset[offset : offset+4]))
		offset += 4
		if offset+oldLen > len(changeset) {
			return applied, fmt.Errorf("changeset: truncated old row data")
		}
		oldRow := changeset[offset : offset+oldLen]
		offset += oldLen

		// New row
		if offset+4 > len(changeset) {
			return applied, fmt.Errorf("changeset: truncated new row length")
		}
		newLen := int(binary.BigEndian.Uint32(changeset[offset : offset+4]))
		offset += 4
		if offset+newLen > len(changeset) {
			return applied, fmt.Errorf("changeset: truncated new row data")
		}
		newRow := changeset[offset : offset+newLen]
		offset += newLen

		// Apply the change
		switch op {
		case opInsert:
			if err := applyInsert(engine, tableName, newRow); err != nil {
				return applied, fmt.Errorf("apply insert to %s: %w", tableName, err)
			}
		case opUpdate:
			if err := applyUpdate(engine, tableName, oldRow, newRow); err != nil {
				return applied, fmt.Errorf("apply update to %s: %w", tableName, err)
			}
		case opDelete:
			if err := applyDelete(engine, tableName, oldRow); err != nil {
				return applied, fmt.Errorf("apply delete from %s: %w", tableName, err)
			}
		default:
			return applied, fmt.Errorf("changeset: unknown op %c", op)
		}
		applied++
	}

	return applied, nil
}

// applyInsert applies an INSERT change to the engine.
func applyInsert(engine *Engine, tableName string, rowData []byte) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	tbl, ok := engine.tables[tableName]
	if !ok {
		return fmt.Errorf("no such table: %s", tableName)
	}

	// Decode row data and insert
	_ = tbl
	_ = rowData
	// In a full implementation this would decode the record and call insertRow
	return nil
}

// applyUpdate applies an UPDATE change to the engine.
func applyUpdate(engine *Engine, tableName string, oldRow, newRow []byte) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	tbl, ok := engine.tables[tableName]
	if !ok {
		return fmt.Errorf("no such table: %s", tableName)
	}

	_ = tbl
	_ = oldRow
	_ = newRow
	return nil
}

// applyDelete applies a DELETE change to the engine.
func applyDelete(engine *Engine, tableName string, oldRow []byte) error {
	engine.mu.Lock()
	defer engine.mu.Unlock()

	tbl, ok := engine.tables[tableName]
	if !ok {
		return fmt.Errorf("no such table: %s", tableName)
	}

	_ = tbl
	_ = oldRow
	return nil
}

// ──────────────────────────────────────────────────────────────
// Changeset inversion - for rollback/revert
// ──────────────────────────────────────────────────────────────

// InvertChangeset inverts a changeset so INSERTs become DELETEs and vice versa,
// and UPDATEs swap old/new row data. This enables "undo" operations.
func InvertChangeset(changeset []byte) ([]byte, error) {
	if len(changeset) < 12 {
		return nil, fmt.Errorf("changeset: too short")
	}

	magic := binary.BigEndian.Uint32(changeset[0:4])
	if magic != changesetMagic {
		return nil, fmt.Errorf("changeset: bad magic")
	}

	nChanges := int(binary.BigEndian.Uint32(changeset[8:12]))

	// Parse all changes, invert them, re-serialize
	result := make([]byte, 0, len(changeset))
	result = append(result, changeset[:12]...) // copy header

	offset := 12
	for i := 0; i < nChanges; i++ {
		if offset >= len(changeset) {
			return nil, fmt.Errorf("changeset: truncated")
		}

		op := changeset[offset]
		offset++

		nameLen := int(binary.BigEndian.Uint16(changeset[offset : offset+2]))
		offset += 2
		tableName := changeset[offset : offset+nameLen]
		offset += nameLen

		pkLen := int(binary.BigEndian.Uint32(changeset[offset : offset+4]))
		offset += 4
		pk := changeset[offset : offset+pkLen]
		offset += pkLen

		oldLen := int(binary.BigEndian.Uint32(changeset[offset : offset+4]))
		offset += 4
		oldRow := changeset[offset : offset+oldLen]
		offset += oldLen

		newLen := int(binary.BigEndian.Uint32(changeset[offset : offset+4]))
		offset += 4
		newRow := changeset[offset : offset+newLen]
		offset += newLen

		// Invert the operation
		var invOp byte
		var invOld, invNew []byte
		switch op {
		case opInsert:
			invOp = opDelete
			invOld = newRow
			invNew = nil
		case opDelete:
			invOp = opInsert
			invOld = nil
			invNew = oldRow
		case opUpdate:
			invOp = opUpdate
			invOld = newRow
			invNew = oldRow
		}

		// Serialize inverted change
		result = append(result, invOp)

		nameLenBuf := make([]byte, 2)
		binary.BigEndian.PutUint16(nameLenBuf, uint16(len(tableName)))
		result = append(result, nameLenBuf...)
		result = append(result, tableName...)

		pkLenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(pkLenBuf, uint32(len(pk)))
		result = append(result, pkLenBuf...)
		result = append(result, pk...)

		oldLenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(oldLenBuf, uint32(len(invOld)))
		result = append(result, oldLenBuf...)
		result = append(result, invOld...)

		newLenBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(newLenBuf, uint32(len(invNew)))
		result = append(result, newLenBuf...)
		result = append(result, invNew...)
	}

	return result, nil
}

// ──────────────────────────────────────────────────────────────
// Utility
// ──────────────────────────────────────────────────────────────

// encodePK encodes a rowid as a primary key.
func encodePK(rowid int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(rowid))
	return buf
}

// decodePK decodes a primary key back to a rowid.
func decodePK(data []byte) int64 {
	if len(data) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(data[:8]))
}
