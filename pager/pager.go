// Package pager implements the page cache and transaction management
// for sqlite-go, mirroring SQLite's pager.c/pcache.c architecture.
package pager

import (
	"github.com/sqlite-go/sqlite-go/vfs"
)

// PageNumber is the 1-based page number in the database file.
type PageNumber int64

// Page represents a cached database page.
type Page struct {
	// PageNum is the 1-based page number.
	PageNum PageNumber
	// Data holds the page content (pageSize bytes).
	Data []byte
	// Dirty indicates the page has been modified.
	Dirty bool
	// RefCount tracks how many references exist.
	RefCount int
}

// JournalMode controls the journaling mode for the pager.
type JournalMode int

const (
	JournalDelete    JournalMode = iota // Delete journal after commit
	JournalPersist                      // Persist journal
	JournalOff                          // No journaling
	JournalTruncate                     // Truncate journal after commit
	JournalMemory                       // In-memory journal
	JournalWAL                          // Write-ahead logging
)

// SyncMode controls how aggressively data is synced to disk.
type SyncMode int

const (
	SyncOff     SyncMode = iota // No syncing
	SyncNormal                   // Sync normally
	SyncFull                     // Full sync with barriers
)

// PagerConfig holds configuration for creating a new Pager.
type PagerConfig struct {
	// VFS to use for file I/O.
	VFS vfs.VFS
	// Path to the database file. Empty means in-memory.
	Path string
	// Page size in bytes. Must be a power of 2 between 512 and 65536.
	// 0 means use default (4096).
	PageSize int
	// Journal mode.
	JournalMode JournalMode
	// Sync mode.
	SyncMode SyncMode
	// Maximum page cache size.
	CacheSize int
	// Whether to use memory-mapped I/O.
	MmapSize int64
	// Temp database flag.
	IsTemp bool
	// Is the database read-only.
	ReadOnly bool
}

// Pager manages reading, writing, and caching database pages.
// It handles transactions, journaling, and crash recovery.
type Pager interface {
	// Open opens/creates the database file.
	Open() error

	// Close flushes and releases all resources.
	Close() error

	// PageCount returns the total number of pages in the database.
	PageCount() int

	// PageSize returns the page size in bytes.
	PageSize() int

	// GetPage fetches a page by number. Returns from cache or reads from disk.
	GetPage(pageNum PageNumber) (*Page, error)

	// GetNewPage allocates a new page at the end of the file.
	GetNewPage() (*Page, error)

	// MarkDirty marks a page as needing to be written to disk.
	MarkDirty(page *Page) error

	// ReleasePage decrements the reference count on a page.
	ReleasePage(page *Page) error

	// WritePage writes a single page to the database file.
	WritePage(page *Page) error

	// Begin starts a new transaction.
	// If write is true, this begins a write transaction.
	Begin(write bool) error

	// Commit commits the current transaction.
	Commit() error

	// Rollback rolls back the current transaction.
	Rollback() error

	// IsInTransaction returns whether a transaction is active.
	IsInTransaction() bool

	// Sync forces all dirty pages to disk.
	Sync() error

	// Truncate truncates the database to pageCount pages.
	Truncate(pageCount int) error

	// SetJournalMode changes the journal mode.
	SetJournalMode(mode JournalMode) (JournalMode, error)

	// JournalMode returns the current journal mode.
	GetJournalMode() JournalMode

	// SetCacheSize changes the page cache size.
	SetCacheSize(size int) error

	// Checkpoint checkpoints the WAL (if in WAL mode).
	Checkpoint(mode int) (int, int, error)

	// FileSize returns the database file size.
	FileSize() (int64, error)

	// BackupInit initializes a backup operation.
	BackupInit(dest Pager) (Backup, error)
}

// Backup represents an ongoing database backup operation.
type Backup interface {
	// Step copies nPage pages from source to destination.
	Step(nPage int) (bool, error)

	// Remaining returns the number of pages still to be copied.
	Remaining() int

	// PageCount returns the total number of pages in the source database.
	PageCount() int

	// Finish finalizes the backup operation.
	Finish() error
}
