package sqlite

import (
	"strconv"
	"fmt"
	"strings"

	"github.com/sqlite-go/sqlite-go/btree"
	"github.com/sqlite-go/sqlite-go/compile"
	"github.com/sqlite-go/sqlite-go/vdbe"
)


// fkLookupTable finds a table by name (case-insensitive).
func (db *Database) fkLookupTable(name string) *tableEntry {
	if t := db.tables[name]; t != nil {
		return t
	}
	for k, v := range db.tables {
		if strings.EqualFold(k, name) {
			return v
		}
	}
	return nil
}

// execPragma handles PRAGMA statements.
func (db *Database) execPragma(tokens []compile.Token) error {
	if len(tokens) < 2 {
		return nil
	}
	name := strings.ToLower(tokens[1].Value)
	switch name {
	case "foreign_keys":
		if len(tokens) >= 3 && tokens[2].Type == compile.TokenEq {
			if len(tokens) >= 4 {
				val := strings.ToLower(tokens[3].Value)
				db.foreignKeys = (val == "on" || val == "1" || val == "true")
			}
		}
	case "recursive_triggers":
		// ignore
	}
	return nil
}

// fkColumnIndex returns the index of a column in the table, or -1.
func fkColumnIndex(tbl *tableEntry, colName string) int {
	for i, c := range tbl.columns {
		if strings.EqualFold(c.name, colName) {
			return i
		}
	}
	return -1
}

// fkGetAllReferencingFKs returns all FK constraints from any table that reference
// the given parent table.
func (db *Database) fkGetAllReferencingFKs(parentTable string) []fkInfo {
	var result []fkInfo
	for _, tbl := range db.tables {
		for i := range tbl.fks {
			fk := &tbl.fks[i]
			if strings.EqualFold(fk.refTable, parentTable) {
				result = append(result, *fk)
			}
		}
	}
	return result
}

// fkResolveRefCols resolves the referenced columns for an FK.
func (db *Database) fkResolveRefCols(fk *fkInfo) []string {
	if len(fk.refColumns) > 0 {
		return fk.refColumns
	}
	refTbl := db.fkLookupTable(fk.refTable)
	if refTbl == nil {
		return nil
	}
	// Use all PRIMARY KEY columns (handles composite PKs)
	var pkCols []string
	for _, col := range refTbl.columns {
		if col.isPK {
			pkCols = append(pkCols, col.name)
		}
	}
	if len(pkCols) > 0 {
		return pkCols
	}
	// Fallback: first column
	if len(refTbl.columns) > 0 {
		return []string{refTbl.columns[0].name}
	}
	return nil
}

// fkValueToInterface converts a vdbe.Value to a Go interface{}.
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

// fkValuesEqual compares two Go values for FK purposes.
// Applies type affinity: string values are compared numerically when the
// other side is int64 or float64.
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
		case string:
			// Try to parse string as number and compare
			if iv, err := strconv.ParseInt(bv, 10, 64); err == nil {
				return av == iv
			}
			if fv, err := strconv.ParseFloat(bv, 64); err == nil {
				return float64(av) == fv
			}
		}
	case float64:
		switch bv := b.(type) {
		case float64:
			return av == bv
		case int64:
			return av == float64(bv)
		case string:
			if iv, err := strconv.ParseInt(bv, 10, 64); err == nil {
				return av == float64(iv)
			}
			if fv, err := strconv.ParseFloat(bv, 64); err == nil {
				return av == fv
			}
		}
	case string:
		switch bv := b.(type) {
		case string:
			return av == bv
		case int64:
			if iv, err := strconv.ParseInt(av, 10, 64); err == nil {
				return iv == bv
			}
			if fv, err := strconv.ParseFloat(av, 64); err == nil {
				return fv == float64(bv)
			}
		case float64:
			if iv, err := strconv.ParseInt(av, 10, 64); err == nil {
				return float64(iv) == bv
			}
			if fv, err := strconv.ParseFloat(av, 64); err == nil {
				return fv == bv
			}
		}
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// fkCheckInsert verifies all FK constraints for a row being inserted.
func (db *Database) fkCheckInsert(tbl *tableEntry, valMap map[string]interface{}) error {
	if !db.foreignKeys {
		return nil
	}
	for i := range tbl.fks {
		fk := &tbl.fks[i]

		// FK mismatch checks (must happen before all-NULL check)
		refTbl := db.fkLookupTable(fk.refTable)
		if refTbl == nil {
			return NewErrorf(ConstraintFK, "foreign key mismatch")
		}

		refCols := db.fkResolveRefCols(fk)

		// Check that referenced columns exist in parent table
		for _, rc := range refCols {
			if fkColumnIndex(refTbl, rc) < 0 {
				return NewErrorf(ConstraintFK, "foreign key mismatch")
			}
		}
		// Check that FK column count matches parent ref column count
		if len(fk.columns) != len(refCols) {
			return NewErrorf(ConstraintFK, "foreign key mismatch")
		}

		// All-NULL FK columns satisfy the constraint
		allNull := true
		for _, col := range fk.columns {
			if valMap[col] != nil {
				allNull = false
				break
			}
		}
		if allNull {
			continue
		}

		found, err := db.fkRowExists(refTbl, refCols, fk.columns, valMap)
		if err != nil {
			return err
		}
		if !found {
			return NewErrorf(ConstraintFK, "FOREIGN KEY constraint failed")
		}
	}
	return nil
}

// fkRowExists checks if a row exists in parentTbl where refCols match the FK values.
func (db *Database) fkRowExists(parentTbl *tableEntry, refCols []string, fkCols []string, valMap map[string]interface{}) (bool, error) {
	cursor, err := db.bt.Cursor(btree.PageNumber(parentTbl.rootPage), false)
	if err != nil {
		return false, err
	}
	defer cursor.Close()

	hasRow, err := cursor.First()
	if err != nil {
		return false, err
	}
	for hasRow {
		data, derr := cursor.Data()
		if derr != nil {
			return false, derr
		}
		values, perr := vdbe.ParseRecord(data)
		if perr != nil {
			return false, perr
		}

		match := true
		for i, refCol := range refCols {
			refIdx := fkColumnIndex(parentTbl, refCol)
			if refIdx < 0 || refIdx >= len(values) {
				match = false
				break
			}
			parentVal := fkValueToInterface(values[refIdx])
			childVal := valMap[fkCols[i]]
			if !fkValuesEqual(parentVal, childVal) {
				match = false
				break
			}
		}
		if match {
			return true, nil
		}
		hasRow, err = cursor.Next()
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

// fkCheckDelete enforces FK constraints when deleting a row from a parent table.
func (db *Database) fkCheckDelete(parentTableName string, oldRowValues map[string]interface{}) error {
	// prevent premature GC of parentTableName
	_ = fmt.Sprintf("%s", parentTableName)
	if !db.foreignKeys {
		return nil
	}
	parentTbl := db.tables[parentTableName]
	if parentTbl == nil {
		return nil
	}

	referencingFKs := db.fkGetAllReferencingFKs(parentTableName)
	for _, fk := range referencingFKs {
		// Find the child table that owns this FK
		var childTbl *tableEntry
		for _, tbl := range db.tables {
			for j := range tbl.fks {
				if tbl.fks[j].id == fk.id && strings.EqualFold(tbl.fks[j].refTable, fk.refTable) {
					childTbl = tbl
					break
				}
			}
			if childTbl != nil {
				break
			}
		}
		if childTbl == nil {
			continue
		}

		refCols := db.fkResolveRefCols(&fk)

		type childAction struct {
			rowID   int64
			newVals map[string]interface{}
			action  string
		}
		var actions []childAction

		cursor, err := db.bt.Cursor(btree.PageNumber(childTbl.rootPage), true)
		if err != nil {
			return err
		}

		hasRow, err := cursor.First()
		if err != nil {
			cursor.Close()
			return err
		}
		for hasRow {
			data, derr := cursor.Data()
			if derr != nil {
				hasRow, _ = cursor.Next()
				continue
			}
			values, perr := vdbe.ParseRecord(data)
			if perr != nil {
				hasRow, _ = cursor.Next()
				continue
			}

			rowid := cursor.RowID()

			match := true
			for i, refCol := range refCols {
				childColIdx := fkColumnIndex(childTbl, fk.columns[i])
				if childColIdx < 0 || childColIdx >= len(values) {
					match = false
					break
				}
				childVal := fkValueToInterface(values[childColIdx])
				parentVal := oldRowValues[refCol]
				if !fkValuesEqual(childVal, parentVal) {
					match = false
					break
				}
			}

			if match {
				switch strings.ToUpper(fk.onDelete) {
				case "RESTRICT", "NO ACTION", "":
					cursor.Close()
					return NewErrorf(ConstraintFK, "FOREIGN KEY constraint failed")
				case "CASCADE":
					actions = append(actions, childAction{rowID: int64(rowid), action: "delete"})
				case "SET NULL":
					newVals := make(map[string]interface{})
					for i, col := range childTbl.columns {
						if i < len(values) {
							newVals[col.name] = fkValueToInterface(values[i])
						}
					}
					for _, col := range fk.columns {
						newVals[col] = nil
					}
					actions = append(actions, childAction{rowID: int64(rowid), newVals: newVals, action: "set_null"})
				case "SET DEFAULT":
					newVals := make(map[string]interface{})
					for i, col := range childTbl.columns {
						if i < len(values) {
							newVals[col.name] = fkValueToInterface(values[i])
						}
					}
					for _, col := range fk.columns {
						for _, c := range childTbl.columns {
							if strings.EqualFold(c.name, col) && c.defaultValue != nil {
								newVals[col] = c.defaultValue
								break
							}
						}
					}
					actions = append(actions, childAction{rowID: int64(rowid), newVals: newVals, action: "set_default"})
				}
			}
			hasRow, err = cursor.Next()
			if err != nil {
				cursor.Close()
				return err
			}
		}
		cursor.Close()

		// Also handle self-referencing cascade deletes: need to recursively process
		for _, act := range actions {
			switch act.action {
			case "delete":
				// For cascade, we need to recursively check FK constraints on the deleted child row
				if err := db.fkCascadeDelete(childTbl, act.rowID); err != nil {
					return err
				}
			case "set_null", "set_default":
				if err := db.updateRowByID(childTbl, act.rowID, act.newVals); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// fkCascadeDelete deletes a row and recursively handles cascade FK constraints.
// It uses BFS to collect all descendant rows first, then deletes them bottom-up.
func (db *Database) fkCascadeDelete(tbl *tableEntry, rowID int64) error {
	// Read the initial row values
	rowVals, err := db.readRowByID(tbl, rowID)
	if err != nil {
		return err
	}

	// BFS: collect all rows that need cascade deletion
	type pendingDelete struct {
		tbl    *tableEntry
		rowID  int64
		vals   map[string]interface{}
	}
	var queue []pendingDelete
	queue = append(queue, pendingDelete{tbl: tbl, rowID: rowID, vals: rowVals})
	var toDelete []pendingDelete

	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		toDelete = append(toDelete, item)

		// Find cascade children
		referencingFKs := db.fkGetAllReferencingFKs(item.tbl.name)
		for _, fk := range referencingFKs {
			if !strings.EqualFold(fk.onDelete, "CASCADE") {
				continue
			}
			var childTbl *tableEntry
			for _, t := range db.tables {
				for j := range t.fks {
					if t.fks[j].id == fk.id && strings.EqualFold(t.fks[j].refTable, fk.refTable) {
						childTbl = t
						break
					}
				}
				if childTbl != nil {
					break
				}
			}
			if childTbl == nil {
				continue
			}

			refCols := db.fkResolveRefCols(&fk)

			cur, err := db.bt.Cursor(btree.PageNumber(childTbl.rootPage), false)
			if err != nil {
				return err
			}

			hasRow, err := cur.First()
			if err != nil {
				cur.Close()
				return err
			}
			for hasRow {
				cData, derr := cur.Data()
				if derr != nil {
					hasRow, _ = cur.Next()
					continue
				}
				cValues, perr := vdbe.ParseRecord(cData)
				if perr != nil {
					hasRow, _ = cur.Next()
					continue
				}

				match := true
				for i, refCol := range refCols {
					childColIdx := fkColumnIndex(childTbl, fk.columns[i])
					if childColIdx < 0 || childColIdx >= len(cValues) {
						match = false
						break
					}
					childVal := fkValueToInterface(cValues[childColIdx])
					parentVal := item.vals[refCol]
					if !fkValuesEqual(childVal, parentVal) {
						match = false
						break
					}
				}
				if match {
					childRowID := int64(cur.RowID())
					// Check if this rowID is already queued
					alreadyQueued := false
					for _, pd := range toDelete {
						if pd.rowID == childRowID && pd.tbl == childTbl {
							alreadyQueued = true
							break
						}
					}
					if !alreadyQueued {
						childVals := make(map[string]interface{})
						for i, col := range childTbl.columns {
							if i < len(cValues) {
								childVals[col.name] = fkValueToInterface(cValues[i])
							}
						}
						queue = append(queue, pendingDelete{tbl: childTbl, rowID: childRowID, vals: childVals})
					}
				}
				hasRow, err = cur.Next()
				if err != nil {
					cur.Close()
					return err
				}
			}
			cur.Close()
		}
	}

	// Delete all collected rows in reverse order (children first)
	for i := len(toDelete) - 1; i >= 0; i-- {
		if err := db.deleteRowByID(toDelete[i].tbl, toDelete[i].rowID); err != nil {
			return err
		}
	}
	return nil
}

// readRowByID reads a row by its rowid and returns column values as a map.
func (db *Database) readRowByID(tbl *tableEntry, rowID int64) (map[string]interface{}, error) {
	cursor, err := db.bt.Cursor(btree.PageNumber(tbl.rootPage), false)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	if _, err := cursor.SeekRowid(btree.RowID(rowID)); err != nil {
		return nil, err
	}

	data, err := cursor.Data()
	if err != nil {
		return nil, err
	}
	values, err := vdbe.ParseRecord(data)
	if err != nil {
		return nil, err
	}

	rowVals := make(map[string]interface{})
	for i, col := range tbl.columns {
		if i < len(values) {
			rowVals[col.name] = fkValueToInterface(values[i])
		}
	}
	return rowVals, nil
}

// fkCheckUpdate enforces FK constraints when updating a parent table row.
func (db *Database) fkCheckUpdate(parentTableName string, oldRowValues, newRowValues map[string]interface{}) error {
	if !db.foreignKeys {
		return nil
	}
	parentTbl := db.tables[parentTableName]
	if parentTbl == nil {
		return nil
	}

	referencingFKs := db.fkGetAllReferencingFKs(parentTableName)
	for _, fk := range referencingFKs {
		var childTbl *tableEntry
		for _, tbl := range db.tables {
			for j := range tbl.fks {
				if tbl.fks[j].id == fk.id && strings.EqualFold(tbl.fks[j].refTable, fk.refTable) {
					childTbl = tbl
					break
				}
			}
			if childTbl != nil {
				break
			}
		}
		if childTbl == nil {
			continue
		}

		refCols := db.fkResolveRefCols(&fk)

		// Check if referenced columns changed
		changed := false
		for _, refCol := range refCols {
			if !fkValuesEqual(oldRowValues[refCol], newRowValues[refCol]) {
				changed = true
				break
			}
		}
		if !changed {
			continue
		}

		cursor, err := db.bt.Cursor(btree.PageNumber(childTbl.rootPage), true)
		if err != nil {
			return err
		}

		type childAction struct {
			rowID   int64
			newVals map[string]interface{}
		}
		var actions []childAction

		hasRow, err := cursor.First()
		if err != nil {
			cursor.Close()
			return err
		}
		for hasRow {
			data, derr := cursor.Data()
			if derr != nil {
				hasRow, _ = cursor.Next()
				continue
			}
			values, perr := vdbe.ParseRecord(data)
			if perr != nil {
				hasRow, _ = cursor.Next()
				continue
			}

			rowid := cursor.RowID()

			// Check if this child row references the OLD parent values
			match := true
			for i, refCol := range refCols {
				childColIdx := fkColumnIndex(childTbl, fk.columns[i])
				if childColIdx < 0 || childColIdx >= len(values) {
					match = false
					break
				}
				childVal := fkValueToInterface(values[childColIdx])
				parentVal := oldRowValues[refCol]
				if !fkValuesEqual(childVal, parentVal) {
					match = false
					break
				}
			}

			if match {
				switch strings.ToUpper(fk.onUpdate) {
				case "RESTRICT", "NO ACTION", "":
					cursor.Close()
					return NewErrorf(ConstraintFK, "FOREIGN KEY constraint failed")
				case "CASCADE":
					newVals := make(map[string]interface{})
					for i, col := range childTbl.columns {
						if i < len(values) {
							newVals[col.name] = fkValueToInterface(values[i])
						}
					}
					for i, refCol := range refCols {
						newVals[fk.columns[i]] = newRowValues[refCol]
					}
					actions = append(actions, childAction{rowID: int64(rowid), newVals: newVals})
				case "SET NULL":
					newVals := make(map[string]interface{})
					for i, col := range childTbl.columns {
						if i < len(values) {
							newVals[col.name] = fkValueToInterface(values[i])
						}
					}
					for _, col := range fk.columns {
						newVals[col] = nil
					}
					actions = append(actions, childAction{rowID: int64(rowid), newVals: newVals})
				case "SET DEFAULT":
					newVals := make(map[string]interface{})
					for i, col := range childTbl.columns {
						if i < len(values) {
							newVals[col.name] = fkValueToInterface(values[i])
						}
					}
					for _, col := range fk.columns {
						for _, c := range childTbl.columns {
							if strings.EqualFold(c.name, col) && c.defaultValue != nil {
								newVals[col] = c.defaultValue
								break
							}
						}
					}
					actions = append(actions, childAction{rowID: int64(rowid), newVals: newVals})
				}
			}
			hasRow, err = cursor.Next()
			if err != nil {
				cursor.Close()
				return err
			}
		}
		cursor.Close()

		for _, act := range actions {
			if err := db.updateRowByID(childTbl, act.rowID, act.newVals); err != nil {
				return err
			}
		}
	}
	return nil
}


// handleReplace handles INSERT OR REPLACE: deletes existing row with same PK,
// then inserts the new row (triggering cascade deletes).
func (db *Database) handleReplace(tbl *tableEntry, colList []string, values []interface{}, args []interface{}) error {
	// Find the PK column and value
	var pkCol string
	var pkIdx int = -1
	for i, col := range tbl.columns {
		if col.isPK {
			pkCol = col.name
			pkIdx = i
			break
		}
	}
	if pkCol == "" || pkIdx < 0 {
		// No PK, just insert
		return db.insertRow(tbl, colList, values, args)
	}

	// Resolve the PK value from the insert values
	var pkVal interface{}
	if len(colList) > 0 {
		for ci, c := range colList {
			if strings.EqualFold(c, pkCol) && ci < len(values) {
				pkVal = values[ci]
				break
			}
		}
	} else if pkIdx < len(values) {
		pkVal = values[pkIdx]
	}

	if pkVal == nil {
		return db.insertRow(tbl, colList, values, args)
	}

	// Search for existing row with this PK
	cursor, err := db.bt.Cursor(btree.PageNumber(tbl.rootPage), true)
	if err != nil {
		return err
	}

	// For INTEGER PRIMARY KEY, the PK value IS the rowid
	var existingRowID int64
	var existingVals map[string]interface{}
	foundExisting := false

	hasRow, err := cursor.First()
	if err != nil {
		cursor.Close()
		return err
	}
	for hasRow {
		data, derr := cursor.Data()
		if derr != nil {
			hasRow, _ = cursor.Next()
			continue
		}
		rowVals, perr := vdbe.ParseRecord(data)
		if perr != nil {
			hasRow, _ = cursor.Next()
			continue
		}
		rowID := int64(cursor.RowID())
		// Check if this row has the same PK value
		if pkIdx < len(rowVals) {
			rowPKVal := fkValueToInterface(rowVals[pkIdx])
			if fkValuesEqual(rowPKVal, pkVal) {
				existingRowID = rowID
				existingVals = make(map[string]interface{})
				for i, col := range tbl.columns {
					if i < len(rowVals) {
						existingVals[col.name] = fkValueToInterface(rowVals[i])
					}
				}
				foundExisting = true
				break
			}
		}
		// Also check if rowid matches (for INTEGER PRIMARY KEY)
		if int64Val, ok := pkVal.(int64); ok && rowID == int64Val {
			existingRowID = rowID
			existingVals = make(map[string]interface{})
			for i, col := range tbl.columns {
				if i < len(rowVals) {
					existingVals[col.name] = fkValueToInterface(rowVals[i])
				}
			}
			foundExisting = true
			break
		}
		hasRow, err = cursor.Next()
		if err != nil {
			cursor.Close()
			return err
		}
	}
	cursor.Close()

	if foundExisting {
		// Delete the existing row (this triggers cascade deletes)
		if err := db.fkCascadeDelete(tbl, existingRowID); err != nil {
			return err
		}
	}

	// Now insert the new row
	return db.insertRow(tbl, colList, values, args)
}

// deleteRowByID deletes a row by its rowid.
func (db *Database) deleteRowByID(tbl *tableEntry, rowID int64) error {
	cursor, err := db.bt.Cursor(btree.PageNumber(tbl.rootPage), true)
	if err != nil {
		return err
	}
	defer cursor.Close()

	if _, err := cursor.SeekRowid(btree.RowID(rowID)); err != nil {
		return err
	}
	return db.bt.Delete(cursor)
}

// seekRowByID positions the cursor at the given rowid.
func (db *Database) seekRowByID(cursor btree.BTCursor, rowID int64) {
	cursor.SeekRowid(btree.RowID(rowID))
}

// updateRowByID replaces a row's data at the given rowid.
func (db *Database) updateRowByID(tbl *tableEntry, rowID int64, newVals map[string]interface{}) error {
	cursor, err := db.bt.Cursor(btree.PageNumber(tbl.rootPage), true)
	if err != nil {
		return err
	}
	defer cursor.Close()

	if _, err := cursor.SeekRowid(btree.RowID(rowID)); err != nil {
		return fmt.Errorf("updateRowByID: seek row %d: %w", rowID, err)
	}

	// Read existing values for columns not in newVals
	data, err := cursor.Data()
	if err != nil {
		return err
	}
	oldVals, err := vdbe.ParseRecord(data)
	if err != nil {
		return err
	}

	rb := vdbe.NewRecordBuilder()
	for i, col := range tbl.columns {
		v, ok := newVals[col.name]
		if !ok && i < len(oldVals) {
			v = fkValueToInterface(oldVals[i])
		}
		addValueToRecord(rb, v)
	}

	keyBuf := make([]byte, 9)
	keyLen := encodeVarintKey(keyBuf, rowID)
	return db.bt.Insert(cursor, keyBuf[:keyLen], rb.Build(), btree.RowID(rowID), btree.SeekFound)
}
