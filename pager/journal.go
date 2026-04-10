package pager

import (
	"encoding/binary"
	"fmt"

	"github.com/sqlite-go/sqlite-go/vfs"
)

// Journal implements a rollback journal for transaction management.
type Journal struct {
	vfs      vfs.VFS
	file     vfs.File
	path     string
	pageSize int
	open     bool
	records  []journalRecord
}

type journalRecord struct {
	pageNum PageNumber
	data    []byte
}

// JournalHeader is the magic header for journal files.
var journalHeader = []byte{
	0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7,
}

// NewJournal creates a new rollback journal.
func NewJournal(path string, v vfs.VFS, pageSize int) *Journal {
	return &Journal{
		vfs:      v,
		path:     path,
		pageSize: pageSize,
	}
}

// Begin creates and opens the journal file, writing the header.
func (j *Journal) Begin() error {
	f, err := j.vfs.Open(j.path, vfs.OpenReadWrite|vfs.OpenCreate|vfs.OpenDeleteOnClose, vfs.FileJournal)
	if err != nil {
		return fmt.Errorf("journal open: %w", err)
	}
	j.file = f
	j.open = true
	j.records = j.records[:0]

	// Write journal header
	hdr := make([]byte, 28)
	copy(hdr[0:8], journalHeader)
	// Number of records (initially 0) - will be updated on commit
	binary.BigEndian.PutUint32(hdr[8:12], 0)
	// Nonce for random nonce
	binary.BigEndian.PutUint32(hdr[12:16], 0)
	// Initial page count placeholder
	binary.BigEndian.PutUint32(hdr[16:20], 0)
	// Sector size
	binary.BigEndian.PutUint32(hdr[20:24], uint32(4096))
	// Page size
	binary.BigEndian.PutUint32(hdr[24:28], uint32(j.pageSize))

	if err := j.file.Write(hdr, 0); err != nil {
		return fmt.Errorf("journal write header: %w", err)
	}

	return nil
}

// WritePage records a page in the journal before it gets modified.
func (j *Journal) WritePage(pageNum PageNumber, data []byte) error {
	if !j.open {
		return fmt.Errorf("journal not open")
	}

	// Store record for rollback
	rec := journalRecord{
		pageNum: pageNum,
		data:    make([]byte, len(data)),
	}
	copy(rec.data, data)
	j.records = append(j.records, rec)

	// Write to journal file: page number (4 bytes) + page data
	offset := int64(28 + (len(j.records)-1)*(4+j.pageSize))
	buf := make([]byte, 4+j.pageSize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(pageNum))
	copy(buf[4:], data)

	if err := j.file.Write(buf, offset); err != nil {
		return fmt.Errorf("journal write page %d: %w", pageNum, err)
	}

	return nil
}

// Commit syncs and updates the journal header with the record count.
func (j *Journal) Commit() error {
	if !j.open {
		return nil
	}

	// Update record count in header
	countBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(countBuf, uint32(len(j.records)))
	if err := j.file.Write(countBuf, 8); err != nil {
		return err
	}

	return j.file.Sync(vfs.SyncFull)
}

// Rollback plays back the journal in reverse order to restore original pages.
// Returns the page data that needs to be written back.
func (j *Journal) Rollback() ([]journalRecord, error) {
	if !j.open {
		return nil, nil
	}
	// Return records in reverse order
	result := make([]journalRecord, len(j.records))
	for i, r := range j.records {
		result[len(j.records)-1-i] = r
	}
	return result, nil
}

// Close closes and deletes the journal file.
func (j *Journal) Close() error {
	if !j.open || j.file == nil {
		return nil
	}
	err := j.file.Close()
	j.file = nil
	j.open = false
	return err
}

// IsOpen returns whether the journal is open.
func (j *Journal) IsOpen() bool {
	return j.open
}
