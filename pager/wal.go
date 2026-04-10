package pager

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/sqlite-go/sqlite-go/vfs"
)

// WAL implements Write-Ahead Logging.
// WAL file format: 32-byte header + frames (24-byte frame header + page data)
type WAL struct {
	mu        sync.Mutex
	vfs       vfs.VFS
	file      vfs.File
	path      string
	pageSize  int
	maxFrame  uint32
	frames    []walFrame
	open      bool
	writeTx   bool
	readTx    bool
}

const (
	walHeaderSize    = 32
	walFrameHdrSize  = 24
	walMagicBE       = 0x377f0682
	walMagicLE       = 0x377f0683
	walFileVersion   = 3007000
)

type walFrame struct {
	pageNum   PageNumber
	data      []byte
	commit    uint32 // If non-zero, this is the last frame of a transaction
}

// NewWAL creates a new WAL instance.
func NewWAL(path string, v vfs.VFS, pageSize int) *WAL {
	return &WAL{
		vfs:      v,
		path:     path,
		pageSize: pageSize,
	}
}

// Open opens or creates the WAL file.
func (w *WAL) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	f, err := w.vfs.Open(w.path, vfs.OpenReadWrite|vfs.OpenCreate, vfs.FileWAL)
	if err != nil {
		return fmt.Errorf("wal open: %w", err)
	}
	w.file = f
	w.open = true

	// Check if file has data
	size, _ := f.Size()
	if size >= walHeaderSize {
		// Read existing header
		hdr := make([]byte, walHeaderSize)
		if err := f.Read(hdr, 0); err != nil {
			return fmt.Errorf("wal read header: %w", err)
		}
		magic := binary.BigEndian.Uint32(hdr[0:4])
		if magic == walMagicBE || magic == walMagicLE {
			w.maxFrame = binary.BigEndian.Uint32(hdr[16:20])
		}
	} else {
		// Write new header
		hdr := make([]byte, walHeaderSize)
		binary.BigEndian.PutUint32(hdr[0:4], walMagicBE)
		binary.BigEndian.PutUint32(hdr[4:8], walFileVersion)
		binary.BigEndian.PutUint32(hdr[8:12], uint32(w.pageSize))
		binary.BigEndian.PutUint32(hdr[12:16], 0) // checkpoint sequence
		binary.BigEndian.PutUint32(hdr[16:20], 0) // salt-1
		binary.BigEndian.PutUint32(hdr[20:24], 0) // salt-2
		// Checksum at 24:32
		if err := f.Write(hdr, 0); err != nil {
			return fmt.Errorf("wal write header: %w", err)
		}
		f.Sync(vfs.SyncFull)
	}

	return nil
}

// BeginReadTx starts a read transaction. Returns the max committed frame.
func (w *WAL) BeginReadTx() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.readTx {
		return int(w.maxFrame), nil
	}
	w.readTx = true
	return int(w.maxFrame), nil
}

// EndReadTx ends a read transaction.
func (w *WAL) EndReadTx() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.readTx = false
}

// BeginWriteTx starts a write transaction.
func (w *WAL) BeginWriteTx() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writeTx {
		return fmt.Errorf("wal: write transaction already active")
	}
	w.writeTx = true
	return nil
}

// WriteFrame writes a frame to the WAL.
func (w *WAL) WriteFrame(pageNum PageNumber, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.open || !w.writeTx {
		return fmt.Errorf("wal not open or no write tx")
	}

	frame := walFrame{
		pageNum: pageNum,
		data:    make([]byte, len(data)),
	}
	copy(frame.data, data)
	w.frames = append(w.frames, frame)

	// Write frame to file
	offset := int64(walHeaderSize + (len(w.frames)-1)*(walFrameHdrSize+w.pageSize))
	frameHdr := make([]byte, walFrameHdrSize)
	binary.BigEndian.PutUint32(frameHdr[0:4], uint32(pageNum))
	// Size of database in pages after commit (0 = not a commit frame)
	binary.BigEndian.PutUint32(frameHdr[4:8], 0)
	// Salt
	binary.BigEndian.PutUint32(frameHdr[8:12], 0)
	binary.BigEndian.PutUint32(frameHdr[12:16], 0)
	// Checksum (simplified - use 0 for now)
	binary.BigEndian.PutUint32(frameHdr[16:20], 0)
	binary.BigEndian.PutUint32(frameHdr[20:24], 0)

	if err := w.file.Write(frameHdr, offset); err != nil {
		return err
	}
	if err := w.file.Write(data, offset+walFrameHdrSize); err != nil {
		return err
	}

	return nil
}

// Commit finalizes the current write transaction.
func (w *WAL) Commit(dbPageCount int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.writeTx || len(w.frames) == 0 {
		w.writeTx = false
		return nil
	}

	// Update the last frame's commit field
	lastIdx := len(w.frames) - 1
	offset := int64(walHeaderSize + lastIdx*(walFrameHdrSize+w.pageSize))
	commitBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(commitBuf, uint32(dbPageCount))
	if err := w.file.Write(commitBuf, offset+4); err != nil {
		return err
	}

	if err := w.file.Sync(vfs.SyncFull); err != nil {
		return err
	}

	w.maxFrame = uint32(len(w.frames))
	w.writeTx = false
	return nil
}

// Rollback discards uncommitted frames.
func (w *WAL) Rollback() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writeTx = false
	// Truncate back to last committed frame
	if w.open && w.file != nil {
		targetSize := int64(walHeaderSize + int(w.maxFrame)*(walFrameHdrSize+w.pageSize))
		w.file.Truncate(targetSize)
	}
	// Remove uncommitted frames from memory
	if int(w.maxFrame) < len(w.frames) {
		w.frames = w.frames[:w.maxFrame]
	}
	return nil
}

// Checkpoint copies frames from WAL to the main database file.
func (w *WAL) Checkpoint(dbFile vfs.File, mode int) (int, int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.frames) == 0 {
		return 0, 0, nil
	}

	// Write all frames to database
	for _, frame := range w.frames {
		offset := int64(frame.pageNum-1) * int64(w.pageSize)
		if err := dbFile.Write(frame.data, offset); err != nil {
			return 0, len(w.frames), err
		}
	}

	if err := dbFile.Sync(vfs.SyncFull); err != nil {
		return 0, len(w.frames), err
	}

	// Truncate WAL
	written := len(w.frames)
	w.frames = w.frames[:0]
	w.maxFrame = 0

	if w.file != nil {
		w.file.Truncate(walHeaderSize)
		// Rewrite header with 0 frames
		hdr := make([]byte, walHeaderSize)
		binary.BigEndian.PutUint32(hdr[0:4], walMagicBE)
		binary.BigEndian.PutUint32(hdr[4:8], walFileVersion)
		binary.BigEndian.PutUint32(hdr[8:12], uint32(w.pageSize))
		w.file.Write(hdr, 0)
		w.file.Sync(vfs.SyncFull)
	}

	return written, 0, nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.open || w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	w.open = false
	return err
}

// FrameCount returns the number of frames in the WAL.
func (w *WAL) FrameCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.frames)
}

// ReadFrame returns the latest committed version of a page from the WAL.
// Returns nil if the page is not in the WAL.
func (w *WAL) ReadFrame(pageNum PageNumber) []byte {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Search backwards for the most recent version
	for i := len(w.frames) - 1; i >= 0; i-- {
		if w.frames[i].pageNum == pageNum && w.frames[i].commit > 0 || i < int(w.maxFrame) {
			data := make([]byte, len(w.frames[i].data))
			copy(data, w.frames[i].data)
			return data
		}
	}
	return nil
}
