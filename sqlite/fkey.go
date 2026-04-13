package sqlite

import (
	"fmt"
	"strings"

	"github.com/sqlite-go/sqlite-go/btree"
	"github.com/sqlite-go/sqlite-go/compile"
	"github.com/sqlite-go/sqlite-go/vdbe"
)

// fkInfo stores metadata about a single foreign key constraint.
type fkInfo struct {
	id         int      // sequential per table
	table      string   // table that has the FK (child)
	columns    []string // local columns in child table
	refTable   string   // referenced (parent) table
	refColumns []string // referenced columns in parent (nil means use parent PK)
	onDelete   string   // CASCADE, SET NULL, SET DEFAULT, RESTRICT, NO ACTION
	onUpdate   string   // CASCADE, SET NULL, SET DEFAULT, RESTRICT, NO ACTION
}

// fkTableInfo holds all FKs for a table and FKs from other tables referencing it.
type fkTableInfo struct {
	fks          []*fkInfo // FKs defined on this table
	referencedBy []*fkInfo // FKs from other tables referencing this one
}

// fkState tracks FK constraints across all tables.
type fkState struct {
	tables map[string]*fkTableInfo
}

func newFKState() *fkState {
	return &fkState{tables: make(map[string]*fkTableInfo)}
}

func (s *fkState) getOrCreate(table string) *fkTableInfo {
	info, ok := s.tables[table]
	if !ok {
		info = &fkTableInfo{}
		s.tables[table] = info
	}
	return info
}

// getFKState returns the FK state, lazily initialized.
func (db *Database) getFKState() *fkState {
	if db.fkStateData == nil {
		db.fkStateData = newFKState()
	}
	return db.fkStateData
}

func (s *fkState) addFK(fk *fkInfo) {
	ti := s.getOrCreate(fk.table)
	fk.id = len(ti.fks)
	ti.fks = append(ti.fks, fk)
	ri := s.getOrCreate(fk.refTable)
	ri.referencedBy = append(ri.referencedBy, fk)
}

func (s *fkState) getFKs(table string) []*fkInfo {
	ti, ok := s.tables[table]
	if !ok {
		return nil
	}
	return ti.fks
}

func (s *fkState) getReferencingFKs(table string) []*fkInfo {
	ti, ok := s.tables[table]
	if !ok {
		return nil
	}
	return ti.referencedBy
}

// parseFKFromCreate extracts foreign key constraints from CREATE TABLE tokens.
// It handles both column-level REFERENCES and table-level FOREIGN KEY constraints.
func (db *Database) parseFKFromCreate(tokens []compile.Token, tableName string) {
	s := db.getFKState()

	pos := 0
	// Find opening paren
	for pos < len(tokens) && tokens[pos].Type != compile.TokenLParen {
		pos++
	}
	if pos >= len(tokens) {
		return
	}
	pos++ // skip (

	colIdx := 0
	for pos < len(tokens) {
		if tokens[pos].Type == compile.TokenRParen {
			break
		}

		// Table-level FOREIGN KEY constraint
		if isKeyword(tokens[pos], "foreign") && pos+1 < len(tokens) && isKeyword(tokens[pos+1], "key") {
			pos += 2
			fk := &fkInfo{table: tableName}

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
					pos++
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
						pos++
					}
				}
			}

			// ON DELETE / ON UPDATE actions
			fk.onDelete, fk.onUpdate = parseFKActions(tokens, &pos)

			// Default ref columns to parent PK
			if len(fk.refColumns) == 0 {
				if refTbl, ok := db.tables[fk.refTable]; ok {
					pk := findPKColumn(refTbl)
					if pk != "" {
						fk.refColumns = []string{pk}
					}
				}
			}

			if fk.onDelete == "" {
				fk.onDelete = "NO ACTION"
			}
			if fk.onUpdate == "" {
				fk.onUpdate = "NO ACTION"
			}

			s.addFK(fk)

			if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
				pos++
			}
			continue
		}

		// Column definition: parse column name and type, then look for REFERENCES
		colName := tokens[pos].Value
		_ = colIdx
		pos++

		// Skip type name(s)
		for pos < len(tokens) &&
			tokens[pos].Type != compile.TokenComma &&
			tokens[pos].Type != compile.TokenRParen {
			// Check for column-level REFERENCES
			if isKeyword(tokens[pos], "references") && pos+1 < len(tokens) {
				pos++ // skip REFERENCES
				fk := &fkInfo{table: tableName, columns: []string{colName}}
				if pos < len(tokens) {
					fk.refTable = tokens[pos].Value
					pos++
				}
				// Optional referenced columns
				if pos < len(tokens) && tokens[pos].Type == compile.TokenLParen {
					pos++
					for pos < len(tokens) && tokens[pos].Type != compile.TokenRParen {
						if tokens[pos].Type == compile.TokenID || tokens[pos].Type == compile.TokenKeyword {
							fk.refColumns = append(fk.refColumns, tokens[pos].Value)
						}
						pos++
					}
					if pos < len(tokens) {
						pos++
					}
				}

				// ON DELETE / ON UPDATE
				fk.onDelete, fk.onUpdate = parseFKActions(tokens, &pos)

				// Default ref columns to parent PK
				if len(fk.refColumns) == 0 {
					if refTbl, ok := db.tables[fk.refTable]; ok {
						pk := findPKColumn(refTbl)
						if pk != "" {
							fk.refColumns = []string{pk}
						}
					}
				}

				if fk.onDelete == "" {
					fk.onDelete = "NO ACTION"
				}
				if fk.onUpdate == "" {
					fk.onUpdate = "NO ACTION"
				}

				s.addFK(fk)
				continue
			}

			// Skip ON DELETE / ON UPDATE that belong to a REFERENCES we already parsed
			if isKeyword(tokens[pos], "on") && pos+2 < len(tokens) {
				if isKeyword(tokens[pos+1], "delete") || isKeyword(tokens[pos+1], "update") {
					action := strings.ToUpper(tokens[pos+2].Value)
					if action == "NO" && pos+3 < len(tokens) && isKeyword(tokens[pos+3], "action") {
						pos += 4
						continue
					}
					pos += 3
					continue
				}
			}

			// Skip DEFERRABLE INITIALLY ...
			if isKeyword(tokens[pos], "deferrable") {
				pos++
				if pos < len(tokens) && isKeyword(tokens[pos], "initially") {
					pos += 2
				}
				continue
			}

			pos++
		}

		colIdx++

		if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
			pos++
		}
	}
}

// parseFKActions parses ON DELETE/ON UPDATE actions after a REFERENCES clause.
func parseFKActions(tokens []compile.Token, pos *int) (onDelete, onUpdate string) {
	for *pos < len(tokens) &&
		tokens[*pos].Type != compile.TokenComma &&
		tokens[*pos].Type != compile.TokenRParen {
		if isKeyword(tokens[*pos], "on") && *pos+2 < len(tokens) {
			action := strings.ToUpper(tokens[*pos+2].Value)
			skip := 3
			// Check for two-keyword actions: SET NULL, SET DEFAULT, NO ACTION
			if *pos+3 < len(tokens) {
				next := strings.ToUpper(tokens[*pos+3].Value)
				if action == "SET" && (next == "NULL" || next == "DEFAULT") {
					action = action + " " + next
					skip = 4
				}
				if action == "NO" && next == "ACTION" {
					action = "NO ACTION"
					skip = 4
				}
			}
			if isKeyword(tokens[*pos+1], "delete") {
				onDelete = action
			} else if isKeyword(tokens[*pos+1], "update") {
				onUpdate = action
			}
			*pos += skip
			continue
		}
		// Skip DEFERRABLE INITIALLY ...
		if isKeyword(tokens[*pos], "deferrable") {
			*pos++
			if *pos < len(tokens) && isKeyword(tokens[*pos], "initially") {
				*pos += 2
			}
			continue
		}
		*pos++
	}
	return
}

// findPKColumn returns the name of the first PRIMARY KEY column in a table.
func findPKColumn(tbl *tableEntry) string {
	// Check if any column was marked as PRIMARY KEY via parsing
	// In the current execCreateTable, PK columns are detected and tracked via uniqueCols
	// but not stored on columnEntry. We need to check the table schema.
	// For now, we'll use "rowid" as implicit PK or look at the columns.
	// Since columnEntry doesn't have IsPK, we check the convention:
	// INTEGER PRIMARY KEY column is the rowid alias.
	for _, col := range tbl.columns {
		if strings.EqualFold(col.typeName, "integer") {
			// This might be the PK - but we can't tell for sure without
			// tracking it. Fall through to use implicit rowid.
			// Actually, for references without explicit columns, SQLite uses
			// the parent table's PK, which is the rowid.
			// We'll return the first column if it's INTEGER, else empty.
			return col.name
		}
	}
	if len(tbl.columns) > 0 {
		return tbl.columns[0].name
	}
	return ""
}

// recordToMap decodes a record from cursor data into a column name -> value map.
func recordToMap(columns []columnEntry, data []byte) map[string]interface{} {
	vals, err := vdbe.ParseRecord(data)
	if err != nil {
		return nil
	}
	m := make(map[string]interface{}, len(columns))
	for i, col := range columns {
		if i < len(vals) {
			m[col.name] = fkValueToInterface(vals[i])
		}
	}
	return m
}

// valueToInterface converts a vdbe.Value to a Go interface.
func fkValueToInterface(v vdbe.Value) interface{} {
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

// fkValuesEqual compares two values for FK checks.
func fkValuesEqual(a, b interface{}) bool {
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

// enforceFKInsert checks FK constraints before inserting a row.
func (db *Database) enforceFKInsert(tableName string, rowValues map[string]interface{}) error {
	if !db.foreignKeys {
		return nil
	}
	s := db.getFKState()
	fks := s.getFKs(tableName)
	for _, fk := range fks {
		if err := db.checkFKReference(fk, rowValues); err != nil {
			return err
		}
	}
	return nil
}

// checkFKReference verifies that referenced rows exist for a single FK.
func (db *Database) checkFKReference(fk *fkInfo, rowValues map[string]interface{}) error {
	// All-NULL FK columns satisfy the constraint
	allNull := true
	for _, col := range fk.columns {
		if rowValues[col] != nil {
			allNull = false
			break
		}
	}
	if allNull {
		return nil
	}

	refTbl, ok := db.tables[fk.refTable]
	if !ok {
		return fmt.Errorf("foreign key mismatch - \"%s\" references \"%s\" which does not exist", fk.table, fk.refTable)
	}

	refCols := fk.refColumns
	if len(refCols) == 0 {
		pk := findPKColumn(refTbl)
		if pk != "" {
			refCols = []string{pk}
		}
	}

	cursor, err := db.bt.Cursor(btree.PageNumber(refTbl.rootPage), false)
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
		rowVals := recordToMap(refTbl.columns, data)

		match := true
		for i, refCol := range refCols {
			if !fkValuesEqual(rowVals[refCol], rowValues[fk.columns[i]]) {
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
		return fmt.Errorf("FOREIGN KEY constraint failed")
	}
	return nil
}

// enforceFKDelete checks and enforces FK constraints when deleting a row.
func (db *Database) enforceFKDelete(tableName string, deletedValues map[string]interface{}) error {
	if !db.foreignKeys {
		return nil
	}
	s := db.getFKState()
	for _, fk := range s.getReferencingFKs(tableName) {
		if err := db.handleFKDeleteAction(fk, tableName, deletedValues); err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) handleFKDeleteAction(fk *fkInfo, parentTable string, parentValues map[string]interface{}) error {
	childTbl, ok := db.tables[fk.table]
	if !ok {
		return nil
	}

	refCols := db.resolveRefCols(fk, parentTable)

	cursor, err := db.bt.Cursor(btree.PageNumber(childTbl.rootPage), true)
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
		rowVals := recordToMap(childTbl.columns, data)

		match := true
		for i, refCol := range refCols {
			if !fkValuesEqual(parentValues[refCol], rowVals[fk.columns[i]]) {
				match = false
				break
			}
		}

		if match {
			switch strings.ToUpper(fk.onDelete) {
			case "RESTRICT", "NO ACTION", "":
				return fmt.Errorf("FOREIGN KEY constraint failed")
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
			if err := db.deleteRowByID(childTbl, act.rowID); err != nil {
				return err
			}
		case "set_null":
			newVals := fkCopyMap(act.values)
			for _, col := range fk.columns {
				newVals[col] = nil
			}
			if err := db.updateRowByID(childTbl, act.rowID, newVals); err != nil {
				return err
			}
		case "set_default":
			newVals := fkCopyMap(act.values)
			for _, col := range fk.columns {
				for _, c := range childTbl.columns {
					if c.name == col && c.defaultValue != nil {
						newVals[col] = c.defaultValue
						break
					}
				}
			}
			if err := db.updateRowByID(childTbl, act.rowID, newVals); err != nil {
				return err
			}
		}
	}
	return nil
}

// enforceFKUpdate handles ON UPDATE actions when a parent row is modified.
func (db *Database) enforceFKUpdate(tableName string, oldValues, newValues map[string]interface{}) error {
	if !db.foreignKeys {
		return nil
	}
	s := db.getFKState()
	for _, fk := range s.getReferencingFKs(tableName) {
		if err := db.handleFKUpdateAction(fk, tableName, oldValues, newValues); err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) handleFKUpdateAction(fk *fkInfo, parentTable string, oldValues, newValues map[string]interface{}) error {
	childTbl, ok := db.tables[fk.table]
	if !ok {
		return nil
	}

	refCols := db.resolveRefCols(fk, parentTable)

	// Check if the referenced key actually changed
	changed := false
	for _, refCol := range refCols {
		if !fkValuesEqual(oldValues[refCol], newValues[refCol]) {
			changed = true
			break
		}
	}
	if !changed {
		return nil
	}

	cursor, err := db.bt.Cursor(btree.PageNumber(childTbl.rootPage), true)
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
		rowVals := recordToMap(childTbl.columns, data)

		match := true
		for i, refCol := range refCols {
			if !fkValuesEqual(oldValues[refCol], rowVals[fk.columns[i]]) {
				match = false
				break
			}
		}

		if match {
			switch strings.ToUpper(fk.onUpdate) {
			case "RESTRICT", "NO ACTION", "":
				return fmt.Errorf("FOREIGN KEY constraint failed")
			case "CASCADE":
				newVals := fkCopyMap(rowVals)
				for i, refCol := range refCols {
					newVals[fk.columns[i]] = newValues[refCol]
				}
				if err := db.updateRowByID(childTbl, int64(cursor.RowID()), newVals); err != nil {
					return err
				}
			case "SET NULL":
				newVals := fkCopyMap(rowVals)
				for _, col := range fk.columns {
					newVals[col] = nil
				}
				if err := db.updateRowByID(childTbl, int64(cursor.RowID()), newVals); err != nil {
					return err
				}
			case "SET DEFAULT":
				newVals := fkCopyMap(rowVals)
				for _, col := range fk.columns {
					for _, c := range childTbl.columns {
						if c.name == col && c.defaultValue != nil {
							newVals[col] = c.defaultValue
							break
						}
					}
				}
				if err := db.updateRowByID(childTbl, int64(cursor.RowID()), newVals); err != nil {
					return err
				}
			}
		}
		hasRow, _ = cursor.Next()
	}
	return nil
}

// deleteRowByID deletes a row by rowid.
func (db *Database) deleteRowByID(tbl *tableEntry, rowID int64) error {
	cursor, err := db.bt.Cursor(btree.PageNumber(tbl.rootPage), true)
	if err != nil {
		return err
	}
	defer cursor.Close()

	keyBuf := make([]byte, 9)
	keyLen := encodeVarintKey(keyBuf, rowID)
	if _, err := cursor.Seek(keyBuf[:keyLen]); err != nil {
		return fmt.Errorf("row not found for delete: %w", err)
	}
	return db.bt.Delete(cursor)
}

// updateRowByID replaces a row's data at the given rowid.
func (db *Database) updateRowByID(tbl *tableEntry, rowID int64, values map[string]interface{}) error {
	cursor, err := db.bt.Cursor(btree.PageNumber(tbl.rootPage), true)
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
	for i, col := range tbl.columns {
		v, ok := values[col.name]
		if !ok && i < len(oldVals) {
			v = fkValueToInterface(oldVals[i])
		}
		addValueToRecord(rb, v)
	}

	return db.bt.Insert(cursor, keyBuf[:keyLen], rb.Build(), btree.RowID(rowID), btree.SeekFound)
}

func (db *Database) resolveRefCols(fk *fkInfo, parentTable string) []string {
	if len(fk.refColumns) > 0 {
		return fk.refColumns
	}
	if pt, ok := db.tables[parentTable]; ok {
		pk := findPKColumn(pt)
		if pk != "" {
			return []string{pk}
		}
	}
	return nil
}

func fkCopyMap(m map[string]interface{}) map[string]interface{} {
	cp := make(map[string]interface{}, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// execPragma handles PRAGMA statements related to foreign keys.
func (db *Database) execPragma(tokens []compile.Token) error {
	// PRAGMA foreign_keys = ON/OFF
	// PRAGMA foreign_keys  (query form handled in queryPragmaFK)
	if len(tokens) < 2 {
		return nil
	}
	tok := tokens[1]
	isFKPragma := isKeyword(tok, "foreign_keys") ||
		(tok.Type == compile.TokenID && strings.EqualFold(tok.Value, "foreign_keys"))
	if !isFKPragma {
		// Silently ignore unknown pragmas
		return nil
	}
	if len(tokens) >= 4 && tokens[2].Type == compile.TokenEq {
		val := strings.ToLower(tokens[3].Value)
		db.foreignKeys = val == "on" || val == "1" || val == "true" || val == "yes"
	}
	return nil
}

// queryPragmaFK handles PRAGMA foreign_keys and PRAGMA foreign_key_list queries.
func (db *Database) queryPragmaFK(tokens []compile.Token) (*ResultSet, bool, error) {
	if len(tokens) < 2 {
		return nil, false, nil
	}

	tok := tokens[1]
	isFKPragma := isKeyword(tok, "foreign_keys") ||
		(tok.Type == compile.TokenID && strings.EqualFold(tok.Value, "foreign_keys"))
	isFKListPragma := isKeyword(tok, "foreign_key_list") ||
		(tok.Type == compile.TokenID && strings.EqualFold(tok.Value, "foreign_key_list"))

	if isFKPragma && (len(tokens) == 2 || tokens[2].Type != compile.TokenEq) {
		// PRAGMA foreign_keys (read)
		cols := []ResultColumnInfo{
			{Name: "foreign_keys", Type: ColNull},
		}
		val := 0
		if db.foreignKeys {
			val = 1
		}
		row := Row{values: []*vdbe.Mem{vdbe.NewMemInt(int64(val))}}
		return newResultSet([]Row{row}, cols), true, nil
	}

	if isFKListPragma {
		// PRAGMA foreign_key_list(table)
		tableName := ""
		if len(tokens) >= 4 && tokens[2].Type == compile.TokenLParen {
			tableName = tokens[3].Value
		} else if len(tokens) >= 3 {
			tableName = tokens[2].Value
		}
		if tableName == "" {
			return nil, true, nil
		}
		rs, err := db.queryForeignKeyList(tableName)
		return rs, true, err
	}

	return nil, false, nil
}

// queryForeignKeyList returns FK info for a table as a ResultSet.
func (db *Database) queryForeignKeyList(tableName string) (*ResultSet, error) {
	s := db.getFKState()
	fks := s.getFKs(tableName)

	colNames := []string{"id", "seq", "table", "from", "to", "on_update", "on_delete", "match"}
	cols := make([]ResultColumnInfo, len(colNames))
	for i, n := range colNames {
		cols[i] = ResultColumnInfo{Name: n, Type: ColNull}
	}

	var rows []Row
	for _, fk := range fks {
		for i, col := range fk.columns {
			refCol := ""
			if i < len(fk.refColumns) {
				refCol = fk.refColumns[i]
			}
			rows = append(rows, Row{values: []*vdbe.Mem{
				vdbe.NewMemInt(int64(fk.id)),
				vdbe.NewMemInt(int64(i)),
				vdbe.NewMemStr(fk.refTable),
				vdbe.NewMemStr(col),
				vdbe.NewMemStr(refCol),
				vdbe.NewMemStr(fk.onUpdate),
				vdbe.NewMemStr(fk.onDelete),
				vdbe.NewMemStr(""),
			}})
		}
	}

	return newResultSet(rows, cols), nil
}
