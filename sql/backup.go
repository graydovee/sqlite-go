// Package sql implements the online backup API for sqlite-go.
package sql

import (
	"fmt"

	"github.com/sqlite-go/sqlite-go/btree"
	"github.com/sqlite-go/sqlite-go/pager"
	"github.com/sqlite-go/sqlite-go/vfs"
)

// Backup represents an ongoing online backup operation.
// It copies pages from a source database to a destination database.
type Backup struct {
	destEngine *Engine
	srcEngine  *Engine

	// Page tracking
	srcRootPage int // Source table root page being copied
	destRootPage int // Destination table root page

	// State
	totalPages  int // Total pages to copy
	copiedPages int // Pages copied so far
	completed   bool
}

// BackupInit initializes a new backup operation from src to dest.
// The srcName and destName specify the database names (e.g., "main").
func BackupInit(dest, src *Engine, destName, srcName string) (*Backup, error) {
	if dest == nil || src == nil {
		return nil, fmt.Errorf("backup: nil engine")
	}

	b := &Backup{
		destEngine: dest,
		srcEngine:  src,
	}

	return b, nil
}

// BackupAll copies all tables from source to destination.
// This is a simplified implementation that copies table schemas and all row data.
func (b *Backup) BackupAll() error {
	if b.completed {
		return fmt.Errorf("backup already completed")
	}

	b.srcEngine.mu.Lock()
	defer b.srcEngine.mu.Unlock()
	b.destEngine.mu.Lock()
	defer b.destEngine.mu.Unlock()

	// Calculate total pages
	b.totalPages = len(b.srcEngine.tables)

	// Copy each table
	for tblName, srcTbl := range b.srcEngine.tables {
		// Create the table in dest
		destCols := make([]ColumnInfo, len(srcTbl.Columns))
		copy(destCols, srcTbl.Columns)

		// Create B-Tree in destination
		if !b.destEngine.inTx {
			if err := b.destEngine.pgr.Begin(true); err != nil {
				return fmt.Errorf("backup: dest begin: %w", err)
			}
			if err := b.destEngine.bt.Begin(true); err != nil {
				b.destEngine.pgr.Rollback()
				return fmt.Errorf("backup: dest btree begin: %w", err)
			}
		}

		rootPage, err := b.destEngine.bt.CreateBTree(btree.CreateTable)
		if err != nil {
			if !b.destEngine.inTx {
				b.destEngine.bt.Rollback()
				b.destEngine.pgr.Rollback()
			}
			return fmt.Errorf("backup: create dest btree: %w", err)
		}

		b.destEngine.tables[tblName] = &TableInfo{
			Name:     srcTbl.Name,
			RootPage: int(rootPage),
			Columns:  destCols,
		}

		if !b.destEngine.inTx {
			if err := b.destEngine.bt.Commit(); err != nil {
				return fmt.Errorf("backup: dest commit: %w", err)
			}
			if err := b.destEngine.pgr.Commit(); err != nil {
				return fmt.Errorf("backup: dest pager commit: %w", err)
			}
		}

		// Copy all rows
		cursor, err := b.srcEngine.bt.Cursor(btree.PageNumber(srcTbl.RootPage), false)
		if err != nil {
			return fmt.Errorf("backup: open src cursor: %w", err)
		}

		destTbl := b.destEngine.tables[tblName]
		hasRow, _ := cursor.First()
		for hasRow {
			data, err := cursor.Data()
			if err != nil {
				cursor.Close()
				return fmt.Errorf("backup: read row: %w", err)
			}

			if err := b.insertRowToDest(destTbl, int64(cursor.RowID()), data); err != nil {
				cursor.Close()
				return fmt.Errorf("backup: write row: %w", err)
			}

			b.copiedPages++
			hasRow, _ = cursor.Next()
		}
		cursor.Close()
	}

	// Copy FK state
	if b.srcEngine.fkStateData != nil {
		b.destEngine.fkStateData = newFKState()
		for tblName, ti := range b.srcEngine.fkStateData.tables {
			destTi := b.destEngine.fkStateData.getOrCreate(tblName)
			for _, fk := range ti.fks {
				fkCopy := *fk
				destTi.fks = append(destTi.fks, &fkCopy)
			}
			for _, fk := range ti.referencedBy {
				fkCopy := *fk
				destTi.referencedBy = append(destTi.referencedBy, &fkCopy)
			}
		}
	}

	b.completed = true
	return nil
}

// Step copies up to nPage pages from source to destination.
// Returns true if there are more pages to copy, false if done.
func (b *Backup) Step(nPage int) (bool, error) {
	if b.completed {
		return false, nil
	}

	if nPage <= 0 {
		// Copy everything
		return false, b.BackupAll()
	}

	b.srcEngine.mu.Lock()
	defer b.srcEngine.mu.Unlock()
	b.destEngine.mu.Lock()
	defer b.destEngine.mu.Unlock()

	// Simplified: copy up to nPage rows across all tables
	copied := 0
	for tblName, srcTbl := range b.srcEngine.tables {
		if copied >= nPage {
			break
		}

		destTbl, ok := b.destEngine.tables[tblName]
		if !ok {
			// Create table in dest first
			destCols := make([]ColumnInfo, len(srcTbl.Columns))
			copy(destCols, srcTbl.Columns)

			if !b.destEngine.inTx {
				b.destEngine.pgr.Begin(true)
				b.destEngine.bt.Begin(true)
			}

			rootPage, err := b.destEngine.bt.CreateBTree(btree.CreateTable)
			if err != nil {
				return false, err
			}

			b.destEngine.tables[tblName] = &TableInfo{
				Name:     srcTbl.Name,
				RootPage: int(rootPage),
				Columns:  destCols,
			}
			destTbl = b.destEngine.tables[tblName]

			if !b.destEngine.inTx {
				b.destEngine.bt.Commit()
				b.destEngine.pgr.Commit()
			}
		}

		cursor, err := b.srcEngine.bt.Cursor(btree.PageNumber(srcTbl.RootPage), false)
		if err != nil {
			return false, err
		}

		hasRow, _ := cursor.First()
		for hasRow && copied < nPage {
			data, derr := cursor.Data()
			if derr != nil {
				cursor.Close()
				return false, derr
			}
			if err := b.insertRowToDest(destTbl, int64(cursor.RowID()), data); err != nil {
				cursor.Close()
				return false, err
			}
			copied++
			b.copiedPages++
			hasRow, _ = cursor.Next()
		}
		cursor.Close()
	}

	if copied == 0 {
		b.completed = true
		return false, nil
	}
	return true, nil
}

// Finish completes the backup operation.
func (b *Backup) Finish() error {
	b.completed = true
	return nil
}

// Remaining returns the number of pages still to be copied.
func (b *Backup) Remaining() int {
	if b.totalPages <= b.copiedPages {
		return 0
	}
	return b.totalPages - b.copiedPages
}

// PageCount returns the total number of pages in the source database.
func (b *Backup) PageCount() int {
	return b.totalPages
}

// insertRowToDest inserts a row into the destination table.
func (b *Backup) insertRowToDest(tbl *TableInfo, rowID int64, data []byte) error {
	if !b.destEngine.inTx {
		if err := b.destEngine.pgr.Begin(true); err != nil {
			return err
		}
		if err := b.destEngine.bt.Begin(true); err != nil {
			b.destEngine.pgr.Rollback()
			return err
		}
		defer func() {
			b.destEngine.bt.Commit()
			b.destEngine.pgr.Commit()
		}()
	}

	cursor, err := b.destEngine.bt.Cursor(btree.PageNumber(tbl.RootPage), true)
	if err != nil {
		return err
	}
	defer cursor.Close()

	keyBuf := make([]byte, 9)
	keyLen := encodeVarintKey(keyBuf, rowID)

	return b.destEngine.bt.Insert(cursor, keyBuf[:keyLen], data, btree.RowID(rowID), btree.SeekNotFound)
}

// BackupDatabase performs a full backup from src to dest databases.
// This is a convenience function that creates an in-memory dest if nil.
func BackupDatabase(dest, src *Engine) error {
	if dest == nil {
		var err error
		dest, err = OpenEngine()
		if err != nil {
			return err
		}
	}

	b, err := BackupInit(dest, src, "main", "main")
	if err != nil {
		return err
	}
	return b.BackupAll()
}

// OpenEngineFromBackup creates a new engine by backing up from a source.
func OpenEngineFromBackup(src *Engine) (*Engine, error) {
	memVFS := vfs.Find("memory")
	if memVFS == nil {
		return nil, fmt.Errorf("no memory VFS available")
	}

	cfg := pager.PagerConfig{
		VFS:         memVFS,
		Path:        "",
		PageSize:    4096,
		CacheSize:   2000,
		JournalMode: pager.JournalMemory,
	}

	pgr, err := pager.OpenPager(cfg)
	if err != nil {
		return nil, err
	}

	btConn := btree.OpenBTreeConn(pgr)
	bt, err := btConn.Open(pgr)
	if err != nil {
		pgr.Close()
		return nil, err
	}

	dest := &Engine{
		vfs:         memVFS,
		pgr:         pgr,
		btConn:      btConn,
		bt:          bt,
		tables:      make(map[string]*TableInfo),
		journalMode: pager.JournalMemory,
		synchronous: 2,
		cacheSize:   2000,
		pageSize:    4096,
		autoCommit:  true,
	}

	if err := BackupDatabase(dest, src); err != nil {
		dest.Close()
		return nil, err
	}

	return dest, nil
}
