package sql

import (
	"fmt"

	"github.com/sqlite-go/sqlite-go/btree"
	"github.com/sqlite-go/sqlite-go/compile"
	"github.com/sqlite-go/sqlite-go/vdbe"
)

// AnalyzeStmt represents an ANALYZE statement.
type AnalyzeStmt struct {
	// Target is the object to analyze: "", "table", "schema.table", or "index"
	Target string
}

// execAnalyze handles ANALYZE statements.
// ANALYZE collects statistics about tables and indices and stores them in
// the sqlite_stat1 table with columns (tbl, idx, stat).
func (e *Engine) execAnalyze(tokens []compile.Token) error {
	pos := 0
	expectKeyword(tokens, &pos, "analyze")

	// Parse optional target
	target := ""
	if pos < len(tokens) && tokens[pos].Type != compile.TokenSemi {
		target = tokens[pos].Value
		pos++
		if pos < len(tokens) && tokens[pos].Type == compile.TokenDot {
			pos++
			if pos < len(tokens) {
				target = tokens[pos].Value
				pos++
			}
		}
	}

	// Ensure sqlite_stat1 table exists
	if err := e.ensureStat1Table(); err != nil {
		return err
	}

	// Clear existing statistics for the target (or all if no target)
	if err := e.clearEngineStat1(target); err != nil {
		return err
	}

	if target == "" {
		// Analyze all tables
		for name := range e.tables {
			if err := e.analyzeEngineTable(name); err != nil {
				return err
			}
		}
	} else {
		// Check if target is an index
		isIndex := false
		for _, idx := range e.indexes {
			if idx.Name == target {
				isIndex = true
				break
			}
		}

		if isIndex {
			if err := e.analyzeEngineIndex(target); err != nil {
				return err
			}
		} else {
			if _, ok := e.tables[target]; !ok {
				return fmt.Errorf("no such table: %s", target)
			}
			if err := e.analyzeEngineTable(target); err != nil {
				return err
			}
		}
	}

	return nil
}

// ensureStat1Table creates the sqlite_stat1 table if it doesn't exist.
func (e *Engine) ensureStat1Table() error {
	if _, ok := e.tables["sqlite_stat1"]; ok {
		return nil
	}

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

	e.tables["sqlite_stat1"] = &TableInfo{
		Name:     "sqlite_stat1",
		RootPage: int(rootPage),
		Columns: []ColumnInfo{
			{CID: 0, Name: "tbl", Type: "TEXT"},
			{CID: 1, Name: "idx", Type: "TEXT"},
			{CID: 2, Name: "stat", Type: "TEXT"},
		},
	}

	return nil
}

// clearEngineStat1 removes statistics rows from sqlite_stat1 for the given target.
func (e *Engine) clearEngineStat1(target string) error {
	tbl, ok := e.tables["sqlite_stat1"]
	if !ok {
		return nil
	}

	cursor, err := e.bt.Cursor(btree.PageNumber(tbl.RootPage), true)
	if err != nil {
		return err
	}
	defer cursor.Close()

	type statRow struct {
		rowID int64
		tbl   string
		idx   string
	}
	var rows []statRow

	hasRow, err := cursor.First()
	if err != nil {
		return err
	}
	for hasRow {
		data, _ := cursor.Data()
		rec, parseErr := vdbe.ParseRecord(data)
		if parseErr == nil && len(rec) >= 2 {
			tblName := ""
			idxName := ""
			if rec[0].Type == "text" {
				tblName = string(rec[0].Bytes)
			}
			if rec[1].Type == "text" {
				idxName = string(rec[1].Bytes)
			}
			rows = append(rows, statRow{rowID: int64(cursor.RowID()), tbl: tblName, idx: idxName})
		}
		hasRow, err = cursor.Next()
		if err != nil {
			break
		}
	}

	for _, r := range rows {
		if target == "" || r.tbl == target || r.idx == target {
			keyBuf := make([]byte, 9)
			keyLen := encodeVarintKey(keyBuf, r.rowID)
			if _, seekErr := cursor.Seek(keyBuf[:keyLen]); seekErr == nil {
				e.bt.Delete(cursor)
			}
		}
	}

	return nil
}

// analyzeEngineTable gathers statistics for a table and all its indexes.
func (e *Engine) analyzeEngineTable(tableName string) error {
	tbl, ok := e.tables[tableName]
	if !ok {
		return nil
	}

	// Skip the sqlite_stat1 table itself
	if tableName == "sqlite_stat1" {
		return nil
	}

	rowCount, err := e.bt.Count(btree.PageNumber(tbl.RootPage))
	if err != nil {
		return err
	}

	stat := fmt.Sprintf("%d", rowCount)
	if err := e.insertStat1Row(tableName, tableName, stat); err != nil {
		return err
	}

	// Gather stats for all indexes on this table
	for _, idx := range e.indexes {
		if idx.TableName == tableName {
			if err := e.analyzeEngineIndex(idx.Name); err != nil {
				return err
			}
		}
	}

	return nil
}

// analyzeEngineIndex gathers statistics for a specific index.
func (e *Engine) analyzeEngineIndex(indexName string) error {
	var targetIdx *IndexEntry
	for i := range e.indexes {
		if e.indexes[i].Name == indexName {
			targetIdx = e.indexes[i]
			break
		}
	}
	if targetIdx == nil {
		return nil
	}

	tbl, ok := e.tables[targetIdx.TableName]
	if !ok {
		return nil
	}

	tableCount, err := e.bt.Count(btree.PageNumber(tbl.RootPage))
	if err != nil {
		return err
	}

	indexCount, err := e.bt.Count(btree.PageNumber(targetIdx.RootPage))
	if err != nil {
		return err
	}

	stat := fmt.Sprintf("%d %d", tableCount, indexCount)
	return e.insertStat1Row(targetIdx.TableName, indexName, stat)
}

// insertStat1Row inserts a row into the sqlite_stat1 table.
func (e *Engine) insertStat1Row(tblName, idxName, stat string) error {
	statTbl, ok := e.tables["sqlite_stat1"]
	if !ok {
		return fmt.Errorf("sqlite_stat1 table not found")
	}

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

	rb := vdbe.NewRecordBuilder()
	rb.AddText(tblName)
	rb.AddText(idxName)
	rb.AddText(stat)
	data := rb.Build()

	cursor, err := e.bt.Cursor(btree.PageNumber(statTbl.RootPage), true)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer cursor.Close()

	var newRowID int64 = 1
	if hasRow, _ := cursor.Last(); hasRow {
		newRowID = int64(cursor.RowID()) + 1
	}

	keyBuf := make([]byte, 9)
	keyLen := encodeVarintKey(keyBuf, newRowID)

	err = e.bt.Insert(cursor, keyBuf[:keyLen], data, btree.RowID(newRowID), btree.SeekNotFound)
	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}

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

// ParseAnalyze parses an ANALYZE statement from tokens.
// This is exported for use by the compile/ package if needed.
func ParseAnalyze(tokens []compile.Token) (*AnalyzeStmt, error) {
	pos := 0
	if len(tokens) == 0 || !isKeyword(tokens[pos], "analyze") {
		return nil, fmt.Errorf("expected ANALYZE")
	}
	pos++

	target := ""
	if pos < len(tokens) && tokens[pos].Type != compile.TokenSemi {
		target = tokens[pos].Value
		pos++
		if pos < len(tokens) && tokens[pos].Type == compile.TokenDot {
			target += "."
			pos++
			if pos < len(tokens) {
				target += tokens[pos].Value
				pos++
			}
		}
	}

	return &AnalyzeStmt{Target: target}, nil
}
