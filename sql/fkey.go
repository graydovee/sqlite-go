// Package sql implements foreign key constraint enforcement for sqlite-go.
package sql

import (
	"fmt"
	"strings"

	"github.com/sqlite-go/sqlite-go/btree"
	"github.com/sqlite-go/sqlite-go/compile"
	"github.com/sqlite-go/sqlite-go/vdbe"
)

// ForeignKeyInfo stores metadata about a single foreign key constraint.
type ForeignKeyInfo struct {
	ID         int      // Foreign key ID (sequential per table)
	Table      string   // Table that has the FK
	Columns    []string // Local columns
	RefTable   string   // Referenced table
	RefColumns []string // Referenced columns (nil means use parent PK)
	OnDelete   string   // CASCADE, SET NULL, SET DEFAULT, RESTRICT, NO ACTION
	OnUpdate   string   // CASCADE, SET NULL, SET DEFAULT, RESTRICT, NO ACTION
}

// tableFKInfo holds all foreign keys for a table and all FKs that reference it.
type tableFKInfo struct {
	fks          []*ForeignKeyInfo // FKs defined on this table
	referencedBy []*ForeignKeyInfo // FKs from other tables referencing this one
}

// fkState tracks foreign key information across all tables.
type fkState struct {
	tables map[string]*tableFKInfo
}

func newFKState() *fkState {
	return &fkState{tables: make(map[string]*tableFKInfo)}
}

func (s *fkState) getOrCreate(table string) *tableFKInfo {
	info, ok := s.tables[table]
	if !ok {
		info = &tableFKInfo{}
		s.tables[table] = info
	}
	return info
}

func (s *fkState) addFK(fk *ForeignKeyInfo) {
	ti := s.getOrCreate(fk.Table)
	fk.ID = len(ti.fks)
	ti.fks = append(ti.fks, fk)
	ri := s.getOrCreate(fk.RefTable)
	ri.referencedBy = append(ri.referencedBy, fk)
}

func (s *fkState) getFKsForTable(table string) []*ForeignKeyInfo {
	ti, ok := s.tables[table]
	if !ok {
		return nil
	}
	return ti.fks
}

func (s *fkState) getReferencingFKs(table string) []*ForeignKeyInfo {
	ti, ok := s.tables[table]
	if !ok {
		return nil
	}
	return ti.referencedBy
}

// getFKState returns the FK state, lazily initialized.
func (e *Engine) getFKState() *fkState {
	if e.fkStateData == nil {
		e.fkStateData = newFKState()
	}
	return e.fkStateData
}

// parseFKFromCreate extracts foreign key constraints from CREATE TABLE tokens.
func (e *Engine) parseFKFromCreate(tokens []compile.Token, tableName string) {
	s := e.getFKState()

	pos := 0
	// Find opening paren
	for pos < len(tokens) && tokens[pos].Type != compile.TokenLParen {
		pos++
	}
	if pos >= len(tokens) {
		return
	}
	pos++ // skip (

	for pos < len(tokens) {
		if tokens[pos].Type == compile.TokenRParen {
			break
		}

		// Table-level FOREIGN KEY constraint
		if isKeyword(tokens[pos], "foreign") && pos+1 < len(tokens) && isKeyword(tokens[pos+1], "key") {
			pos += 2
			fk := &ForeignKeyInfo{Table: tableName}

			// Local columns
			if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
				pos++
				for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
					if tokens[pos].Type == compile.TokenID || tokens[pos].Type == compile.TokenKeyword {
						fk.Columns = append(fk.Columns, tokens[pos].Value)
					}
					pos++
				}
				if pos < len(tokens) {
					pos++
				}
			}

			// REFERENCES
			if pos < len(tokens) && isKeyword(tokens[pos], "references") {
				pos++
				if pos < len(tokens) {
					fk.RefTable = tokens[pos].Value
					pos++
				}
				if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
					pos++
					for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
						if tokens[pos].Type == compile.TokenID || tokens[pos].Type == compile.TokenKeyword {
							fk.RefColumns = append(fk.RefColumns, tokens[pos].Value)
						}
						pos++
					}
					if pos < len(tokens) {
						pos++
					}
				}
			}

			// ON DELETE / ON UPDATE
			for pos < len(tokens) &&
				tokens[pos].Type != compile.TokenComma &&
				tokens[pos].Type != compile.TokenRParen {
				if isKeyword(tokens[pos], "on") && pos+2 < len(tokens) {
					action := strings.ToUpper(tokens[pos+2].Value)
					if isKeyword(tokens[pos+1], "delete") {
						fk.OnDelete = action
					} else if isKeyword(tokens[pos+1], "update") {
						fk.OnUpdate = action
					}
					pos += 3
					if pos < len(tokens) && isKeyword(tokens[pos], "no") && pos+1 < len(tokens) && isKeyword(tokens[pos+1], "action") {
						if fk.OnDelete == "NO" {
							fk.OnDelete = "NO ACTION"
						}
						if fk.OnUpdate == "NO" {
							fk.OnUpdate = "NO ACTION"
						}
						pos += 2
					}
					continue
				}
				pos++
			}

			if fk.OnDelete == "" {
				fk.OnDelete = "NO ACTION"
			}
			if fk.OnUpdate == "" {
				fk.OnUpdate = "NO ACTION"
			}

			// Default ref columns to parent PK
			if len(fk.RefColumns) == 0 {
				if refTbl, ok := e.tables[fk.RefTable]; ok {
					for _, col := range refTbl.Columns {
						if col.IsPK {
							fk.RefColumns = append(fk.RefColumns, col.Name)
							break
						}
					}
				}
			}

			s.addFK(fk)

			if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
				pos++
			}
			continue
		}

		// Skip non-FK constraint
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
	}
}

// recordToMap decodes a record from cursor data into a column name -> value map.
func recordToMap(columns []ColumnInfo, data []byte) map[string]interface{} {
	vals, err := vdbe.ParseRecord(data)
	if err != nil {
		return nil
	}
	m := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		if i < len(vals) {
			m[col.Name] = valueToInterface(vals[i])
		}
	}
	return m
}

// enforceFKInsert checks FK constraints before inserting a row.
func (e *Engine) enforceFKInsert(tableName string, rowValues map[string]interface{}) error {
	if !e.foreignKeys {
		return nil
	}
	s := e.getFKState()
	for _, fk := range s.getFKsForTable(tableName) {
		if err := e.checkFKReference(fk, rowValues); err != nil {
			return err
		}
	}
	return nil
}

// checkFKReference verifies that referenced rows exist for a single FK.
func (e *Engine) checkFKReference(fk *ForeignKeyInfo, rowValues map[string]interface{}) error {
	// All-NULL FK columns satisfy the constraint
	allNull := true
	for _, col := range fk.Columns {
		if rowValues[col] != nil {
			allNull = false
			break
		}
	}
	if allNull {
		return nil
	}

	refTbl, ok := e.tables[fk.RefTable]
	if !ok {
		return fmt.Errorf("foreign key mismatch: table %q references nonexistent table %q", fk.Table, fk.RefTable)
	}

	refCols := fk.RefColumns
	if len(refCols) == 0 {
		for _, col := range refTbl.Columns {
			if col.IsPK {
				refCols = append(refCols, col.Name)
				break
			}
		}
	}

	cursor, err := e.bt.Cursor(btree.PageNumber(refTbl.RootPage), false)
	if err != nil {
		return fmt.Errorf("foreign key check: %w", err)
	}
	defer cursor.Close()

	found := false
	hasRow, _ := cursor.First()
	for hasRow {
		data, derr := cursor.Data()
		if derr != nil {
			break
		}
		rowVals := recordToMap(refTbl.Columns, data)

		match := true
		for i, refCol := range refCols {
			if !valuesEqual(rowVals[refCol], rowValues[fk.Columns[i]]) {
				match = false
				break
			}
		}
		if match {
			found = true
			break
		}
		hasRow, _ = cursor.Next()
	}

	if !found {
		return fmt.Errorf("foreign key constraint failed: INSERT into table %q violates FK to %q",
			fk.Table, fk.RefTable)
	}
	return nil
}

// enforceFKDelete checks and enforces FK constraints when deleting a row.
func (e *Engine) enforceFKDelete(tableName string, deletedRowID int64, deletedValues map[string]interface{}) error {
	if !e.foreignKeys {
		return nil
	}
	s := e.getFKState()
	for _, fk := range s.getReferencingFKs(tableName) {
		if err := e.handleFKDeleteAction(fk, tableName, deletedValues); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) handleFKDeleteAction(fk *ForeignKeyInfo, parentTable string, parentValues map[string]interface{}) error {
	childTbl, ok := e.tables[fk.Table]
	if !ok {
		return nil
	}

	refCols := resolveRefCols(fk, parentTable, e.tables)

	cursor, err := e.bt.Cursor(btree.PageNumber(childTbl.RootPage), true)
	if err != nil {
		return err
	}
	defer cursor.Close()

	type rowAction struct {
		rowID  int64
		values map[string]interface{}
		action string
	}
	var actions []rowAction

	hasRow, _ := cursor.First()
	for hasRow {
		data, derr := cursor.Data()
		if derr != nil {
			hasRow, _ = cursor.Next()
			continue
		}
		rowVals := recordToMap(childTbl.Columns, data)

		match := true
		for i, refCol := range refCols {
			if !valuesEqual(parentValues[refCol], rowVals[fk.Columns[i]]) {
				match = false
				break
			}
		}

		if match {
			switch strings.ToUpper(fk.OnDelete) {
			case "RESTRICT", "NO ACTION":
				return fmt.Errorf("foreign key constraint failed: DELETE from %q restricted by FK in %q",
					parentTable, fk.Table)
			case "CASCADE":
				actions = append(actions, rowAction{rowID: int64(cursor.RowID()), values: rowVals, action: "delete"})
			case "SET NULL":
				actions = append(actions, rowAction{rowID: int64(cursor.RowID()), values: rowVals, action: "set_null"})
			case "SET DEFAULT":
				actions = append(actions, rowAction{rowID: int64(cursor.RowID()), values: rowVals, action: "set_default"})
			}
		}
		hasRow, _ = cursor.Next()
	}

	for _, act := range actions {
		switch act.action {
		case "delete":
			if err := e.deleteRowByID(childTbl, act.rowID); err != nil {
				return err
			}
		case "set_null":
			newVals := copyMap(act.values)
			for _, col := range fk.Columns {
				newVals[col] = nil
			}
			if err := e.updateRowByID(childTbl, act.rowID, newVals); err != nil {
				return err
			}
		case "set_default":
			newVals := copyMap(act.values)
			for _, col := range fk.Columns {
				for _, c := range childTbl.Columns {
					if c.Name == col && c.DefaultValue != "" {
						newVals[col] = c.DefaultValue
						break
					}
				}
			}
			if err := e.updateRowByID(childTbl, act.rowID, newVals); err != nil {
				return err
			}
		}
	}
	return nil
}

// enforceFKUpdate handles ON UPDATE actions when a parent row is modified.
func (e *Engine) enforceFKUpdate(tableName string, rowID int64, oldValues, newValues map[string]interface{}) error {
	if !e.foreignKeys {
		return nil
	}
	s := e.getFKState()
	for _, fk := range s.getReferencingFKs(tableName) {
		if err := e.handleFKUpdateAction(fk, tableName, oldValues, newValues); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) handleFKUpdateAction(fk *ForeignKeyInfo, parentTable string, oldValues, newValues map[string]interface{}) error {
	childTbl, ok := e.tables[fk.Table]
	if !ok {
		return nil
	}

	refCols := resolveRefCols(fk, parentTable, e.tables)

	changed := false
	for _, refCol := range refCols {
		if !valuesEqual(oldValues[refCol], newValues[refCol]) {
			changed = true
			break
		}
	}
	if !changed {
		return nil
	}

	cursor, err := e.bt.Cursor(btree.PageNumber(childTbl.RootPage), true)
	if err != nil {
		return err
	}
	defer cursor.Close()

	hasRow, _ := cursor.First()
	for hasRow {
		data, derr := cursor.Data()
		if derr != nil {
			hasRow, _ = cursor.Next()
			continue
		}
		rowVals := recordToMap(childTbl.Columns, data)

		match := true
		for i, refCol := range refCols {
			if !valuesEqual(oldValues[refCol], rowVals[fk.Columns[i]]) {
				match = false
				break
			}
		}

		if match {
			switch strings.ToUpper(fk.OnUpdate) {
			case "RESTRICT", "NO ACTION":
				return fmt.Errorf("foreign key constraint failed: UPDATE of %q restricted by FK in %q",
					parentTable, fk.Table)
			case "CASCADE":
				newVals := copyMap(rowVals)
				for i, refCol := range refCols {
					newVals[fk.Columns[i]] = newValues[refCol]
				}
				if err := e.updateRowByID(childTbl, int64(cursor.RowID()), newVals); err != nil {
					return err
				}
			case "SET NULL":
				newVals := copyMap(rowVals)
				for _, col := range fk.Columns {
					newVals[col] = nil
				}
				if err := e.updateRowByID(childTbl, int64(cursor.RowID()), newVals); err != nil {
					return err
				}
			case "SET DEFAULT":
				newVals := copyMap(rowVals)
				for _, col := range fk.Columns {
					for _, c := range childTbl.Columns {
						if c.Name == col && c.DefaultValue != "" {
							newVals[col] = c.DefaultValue
							break
						}
					}
				}
				if err := e.updateRowByID(childTbl, int64(cursor.RowID()), newVals); err != nil {
					return err
				}
			}
		}
		hasRow, _ = cursor.Next()
	}
	return nil
}

// fkIntegrityCheck verifies all FK constraints; returns violations.
func (e *Engine) fkIntegrityCheck() []string {
	if !e.foreignKeys {
		return nil
	}
	s := e.getFKState()
	var violations []string

	for tblName, ti := range e.tables {
		fks := s.getFKsForTable(tblName)
		if len(fks) == 0 {
			continue
		}
		cursor, err := e.bt.Cursor(btree.PageNumber(ti.RootPage), false)
		if err != nil {
			continue
		}
		rowNum := 0
		hasRow, _ := cursor.First()
		for hasRow {
			rowNum++
			data, derr := cursor.Data()
			if derr != nil {
				hasRow, _ = cursor.Next()
				continue
			}
			rowVals := recordToMap(ti.Columns, data)
			for _, fk := range fks {
				allNull := true
				for _, col := range fk.Columns {
					if rowVals[col] != nil {
						allNull = false
						break
					}
				}
				if allNull {
					continue
				}
				if err := e.checkFKReference(fk, rowVals); err != nil {
					violations = append(violations, fmt.Sprintf("%s row %d: %s", tblName, rowNum, err.Error()))
				}
			}
			hasRow, _ = cursor.Next()
		}
		cursor.Close()
	}
	return violations
}

// getForeignKeyList returns FKs for a table as PragmaRows.
func (e *Engine) getForeignKeyList(tableName string) ([]PragmaRow, error) {
	s := e.getFKState()
	fks := s.getFKsForTable(tableName)
	var rows []PragmaRow
	for _, fk := range fks {
		for i, col := range fk.Columns {
			refCol := ""
			if i < len(fk.RefColumns) {
				refCol = fk.RefColumns[i]
			}
			rows = append(rows, PragmaRow{Values: []interface{}{
				fk.ID, i, col, fk.RefTable, refCol, fk.OnUpdate, fk.OnDelete, "",
			}})
		}
	}
	return rows, nil
}

// deleteRowByID deletes a row by rowid.
func (e *Engine) deleteRowByID(tbl *TableInfo, rowID int64) error {
	cursor, err := e.bt.Cursor(btree.PageNumber(tbl.RootPage), true)
	if err != nil {
		return err
	}
	defer cursor.Close()

	keyBuf := make([]byte, 9)
	keyLen := encodeVarintKey(keyBuf, rowID)
	if _, err := cursor.Seek(keyBuf[:keyLen]); err != nil {
		return fmt.Errorf("row not found for delete: %w", err)
	}
	return e.bt.Delete(cursor)
}

// updateRowByID replaces a row's data at the given rowid.
func (e *Engine) updateRowByID(tbl *TableInfo, rowID int64, values map[string]interface{}) error {
	cursor, err := e.bt.Cursor(btree.PageNumber(tbl.RootPage), true)
	if err != nil {
		return err
	}
	defer cursor.Close()

	keyBuf := make([]byte, 9)
	keyLen := encodeVarintKey(keyBuf, rowID)
	if _, err := cursor.Seek(keyBuf[:keyLen]); err != nil {
		return fmt.Errorf("row not found for update: %w", err)
	}

	// Read old values for columns not provided
	data, _ := cursor.Data()
	oldVals, _ := vdbe.ParseRecord(data)

	rb := vdbe.NewRecordBuilder()
	for i, col := range tbl.Columns {
		v, ok := values[col.Name]
		if !ok && i < len(oldVals) {
			v = valueToInterface(oldVals[i])
		}
		addValueToRecord(rb, v)
	}

	return e.bt.Insert(cursor, keyBuf[:keyLen], rb.Build(), btree.RowID(rowID), btree.SeekFound)
}

// valueToInterface converts a vdbe.Value to a Go interface.
func valueToInterface(v vdbe.Value) interface{} {
	switch v.Type {
	case "null":
		return nil
	case "int":
		return v.IntVal
	case "float":
		return v.FloatVal
	case "text":
		return string(v.Bytes)
	case "blob":
		return v.Bytes
	default:
		return nil
	}
}

// valuesEqual compares two values for FK checks.
func valuesEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	switch av := a.(type) {
	case int64:
		switch bv := b.(type) {
		case int64:
			return av == bv
		case float64:
			return float64(av) == bv
		}
	case float64:
		switch bv := b.(type) {
		case float64:
			return av == bv
		case int64:
			return av == float64(bv)
		}
	case string:
		if bs, ok := b.(string); ok {
			return av == bs
		}
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func resolveRefCols(fk *ForeignKeyInfo, parentTable string, tables map[string]*TableInfo) []string {
	if len(fk.RefColumns) > 0 {
		return fk.RefColumns
	}
	if pt, ok := tables[parentTable]; ok {
		for _, col := range pt.Columns {
			if col.IsPK {
				return []string{col.Name}
			}
		}
	}
	return nil
}

func copyMap(m map[string]interface{}) map[string]interface{} {
	cp := make(map[string]interface{}, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
