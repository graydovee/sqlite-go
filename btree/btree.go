// Package btree implements the B-Tree engine for sqlite-go,
// mirroring SQLite's btree.c architecture.
// Tables use B+Trees (leaf cells contain data).
// Indexes use B-Trees (all cells contain keys only).
package btree

import (
	"github.com/sqlite-go/sqlite-go/pager"
)

// RowID is the 64-bit integer primary key.
type RowID int64

// PageNumber represents a B-Tree page number.
type PageNumber = pager.PageNumber

// CreateFlags controls B-Tree creation behavior.
type CreateFlags int

const (
	CreateTable     CreateFlags = 0
	CreateIndex     CreateFlags = 1
	CreateProtected CreateFlags = 2
)

// SeekResult indicates the result of a cursor seek.
type SeekResult int

const (
	SeekNotFound SeekResult = iota
	SeekFound
	SeekInvalid
)

// BTCursor is a cursor for navigating a B-Tree.
type BTCursor interface {
	Close() error
	First() (bool, error)
	Last() (bool, error)
	Next() (bool, error)
	Prev() (bool, error)
	Seek(key []byte) (SeekResult, error)
	SeekRowid(rowid RowID) (SeekResult, error)
	SeekNear(key []byte) (SeekResult, error)
	Key() []byte
	Data() ([]byte, error)
	RowID() RowID
	IsValid() bool
	SetRowID(rowid RowID) error
}

// BTree represents a B-Tree structure (table or index).
type BTree interface {
	// Close closes the B-Tree and releases resources.
	Close() error

	// Begin starts a transaction on this B-Tree.
	Begin(write bool) error

	// Commit commits the transaction.
	Commit() error

	// Rollback rolls back the transaction.
	Rollback() error

	// CreateBTree creates a new B-Tree and returns its root page number.
	CreateBTree(flags CreateFlags) (PageNumber, error)

	// Drop drops the B-Tree rooted at the given page.
	Drop(rootPage PageNumber) error

	// Clear removes all content from the B-Tree.
	Clear(rootPage PageNumber) error

	// Cursor creates a new cursor on the B-Tree.
	Cursor(rootPage PageNumber, write bool) (BTCursor, error)

	// Insert inserts a key/value pair into the B-Tree.
	Insert(cursor BTCursor, key []byte, data []byte, rowid RowID, seekResult SeekResult) error

	// Delete deletes the entry at the cursor position.
	Delete(cursor BTCursor) error

	// Count returns the number of entries in the B-Tree.
	Count(rootPage PageNumber) (int64, error)

	// IntegrityCheck performs an integrity check on the B-Tree.
	IntegrityCheck(rootPage PageNumber, depth int, errDest *[]string)

	// PageCount returns the number of pages used by this B-Tree.
	PageCount() int
}

// BTreeConn represents a connection to a B-Tree subsystem.
type BTreeConn interface {
	// Open opens/creates a database and returns the BTree interface.
	Open(pgr pager.Pager) (BTree, error)

	// GetPager returns the underlying pager.
	GetPager() pager.Pager

	// GetMeta reads a value from the database header meta array.
	GetMeta(idx int) (int32, error)

	// SetMeta writes a value to the database header meta array.
	SetMeta(idx int, value int32) error

	// SchemaVersion returns the database schema version.
	SchemaVersion() (int32, error)

	// SetSchemaVersion sets the database schema version.
	SetSchemaVersion(version int32) error
}
