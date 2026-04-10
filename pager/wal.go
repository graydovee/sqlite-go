package pager

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/sqlite-go/sqlite-go/vfs"
)

// ──────────────────────────────────────────────────────────────
// WAL file format constants (matching SQLite WAL format)
// ──────────────────────────────────────────────────────────────

const (
	walHeaderSize   = 32 // WAL file header size in bytes
	walFrameHdrSize = 24 // Frame header size in bytes

	// Magic numbers: high bit indicates native byte-order checksums.
	// 0x377f0682 = big-endian checksum
	// 0x377f0683 = little-endian checksum
	walMagicBE = 0x377f0682
	walMagicLE = 0x377f0683

	walFileVersion = 3007000

	// WAL index (shm) constants
	walIndexHdrSize    = 136  // Size of wal-index header
	walIndexBlockSize  = 4096 // Size of one wal-index block (header+hash+page)
	hashslotNPage      = 4096 // Frames per non-first index block
	hashslotNPageOne   = 4062 // Frames in first index block
	hashslotNSlot      = 8192 // Hash slots per index block (2 * NPAGE)

	// Locking constants (matching SQLite)
	walWriteLock   = 0
	walCkptLock    = 1
	walRecoverLock = 2
	walNReader     = 5 // Number of reader slots

	// Checkpoint modes
	CheckpointPassive  = 0 // Do as much as possible without blocking
	CheckpointFull     = 1 // Wait for readers, then checkpoint
	CheckpointRestart  = 2 // Like FULL but also reconnect readers
	CheckpointTruncate = 3 // Like RESTART but also truncate WAL
)

// ──────────────────────────────────────────────────────────────
// WAL header (32 bytes, on-disk)
// ──────────────────────────────────────────────────────────────

// walHeader represents the 32-byte WAL file header.
//
//	 0: Magic number (0x377f0682 or 0x377f0683)
//	 4: File format version (3007000)
//	 8: Database page size
//	12: Checkpoint sequence number
//	16: Salt-1
//	20: Salt-2
//	24: Checksum-1
//	28: Checksum-2
type walHeader struct {
	magic      uint32
	version    uint32
	pageSize   uint32
	ckptSeq    uint32
	salt1      uint32
	salt2      uint32
	checksum1  uint32
	checksum2  uint32
	bigEndCksum bool // true if big-endian checksums
}

func (h *walHeader) encode() []byte {
	buf := make([]byte, walHeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], h.magic)
	binary.BigEndian.PutUint32(buf[4:8], h.version)
	binary.BigEndian.PutUint32(buf[8:12], h.pageSize)
	binary.BigEndian.PutUint32(buf[12:16], h.ckptSeq)
	binary.BigEndian.PutUint32(buf[16:20], h.salt1)
	binary.BigEndian.PutUint32(buf[20:24], h.salt2)
	binary.BigEndian.PutUint32(buf[24:28], h.checksum1)
	binary.BigEndian.PutUint32(buf[28:32], h.checksum2)
	return buf
}

func decodeWalHeader(buf []byte) walHeader {
	h := walHeader{
		magic:     binary.BigEndian.Uint32(buf[0:4]),
		version:   binary.BigEndian.Uint32(buf[4:8]),
		pageSize:  binary.BigEndian.Uint32(buf[8:12]),
		ckptSeq:   binary.BigEndian.Uint32(buf[12:16]),
		salt1:     binary.BigEndian.Uint32(buf[16:20]),
		salt2:     binary.BigEndian.Uint32(buf[20:24]),
		checksum1: binary.BigEndian.Uint32(buf[24:28]),
		checksum2: binary.BigEndian.Uint32(buf[28:32]),
	}
	h.bigEndCksum = (h.magic == walMagicBE)
	return h
}

// ──────────────────────────────────────────────────────────────
// Frame header (24 bytes, on-disk)
// ──────────────────────────────────────────────────────────────

// walFrameHdr represents the 24-byte frame header.
//
//	 0: Page number
//	 4: Database size in pages after commit (0 = not a commit frame)
//	 8: Salt-1 (must match WAL header)
//	12: Salt-2 (must match WAL header)
//	16: Checksum-1
//	20: Checksum-2
type walFrameHdr struct {
	pageNo    uint32
	dbSize    uint32 // 0 for non-commit frames
	salt1     uint32
	salt2     uint32
	checksum1 uint32
	checksum2 uint32
}

func (h *walFrameHdr) encode() []byte {
	buf := make([]byte, walFrameHdrSize)
	binary.BigEndian.PutUint32(buf[0:4], h.pageNo)
	binary.BigEndian.PutUint32(buf[4:8], h.dbSize)
	binary.BigEndian.PutUint32(buf[8:12], h.salt1)
	binary.BigEndian.PutUint32(buf[12:16], h.salt2)
	binary.BigEndian.PutUint32(buf[16:20], h.checksum1)
	binary.BigEndian.PutUint32(buf[20:24], h.checksum2)
	return buf
}

func decodeWalFrameHdr(buf []byte) walFrameHdr {
	return walFrameHdr{
		pageNo:    binary.BigEndian.Uint32(buf[0:4]),
		dbSize:    binary.BigEndian.Uint32(buf[4:8]),
		salt1:     binary.BigEndian.Uint32(buf[8:12]),
		salt2:     binary.BigEndian.Uint32(buf[12:16]),
		checksum1: binary.BigEndian.Uint32(buf[16:20]),
		checksum2: binary.BigEndian.Uint32(buf[20:24]),
	}
}

// ──────────────────────────────────────────────────────────────
// WAL index (shared memory / shm)
// ──────────────────────────────────────────────────────────────

// walIndex is the in-memory WAL index for fast page-to-frame lookup.
// In a real multi-process setup this would be a shared memory segment.
// For our single-process implementation, we use an in-memory data structure
// that provides the same fast lookup semantics.
type walIndex struct {
	mu sync.RWMutex

	// hdr holds the wal-index header fields
	hdr walIndexHdr

	// pageMap maps page number -> latest frame index (0-based).
	// Updated on every write and checkpoint.
	pageMap map[uint32]int

	// framePages holds the page number for each frame (0-based frame index).
	framePages []uint32
}

// walIndexHdr mirrors the in-memory part of the shm header.
type walIndexHdr struct {
	version    uint32
	pageSize   uint32
	ckptSeq    uint32
	salt1      uint32
	salt2      uint32
	checksum1  uint32
	checksum2  uint32
	bigEndCksum bool
	mxFrame    uint32 // max valid frame (1-based; 0 = none)
	pageCount  uint32 // database page count from last commit
}

func newWalIndex() *walIndex {
	return &walIndex{
		pageMap:     make(map[uint32]int),
		framePages:  nil,
	}
}

// lookup finds the latest frame index (0-based) for page pgno
// among frames 0..mxFrame-1. Returns -1 if not found.
func (idx *walIndex) lookup(pgno uint32, mxFrame int) int {
	// Scan backward from mxFrame-1 to find the latest frame for this page
	// that is within the reader's snapshot.
	for i := mxFrame - 1; i >= 0; i-- {
		if i < len(idx.framePages) && idx.framePages[i] == pgno {
			return i
		}
	}
	return -1
}

// append adds a frame for the given page number.
func (idx *walIndex) append(pgno uint32) {
	fi := len(idx.framePages)
	idx.framePages = append(idx.framePages, pgno)
	idx.pageMap[pgno] = fi // latest wins
}

// truncate removes all entries after frame n (0-based, exclusive).
func (idx *walIndex) truncate(n int) {
	if n < 0 {
		n = 0
	}
	for i := n; i < len(idx.framePages); i++ {
		pgno := idx.framePages[i]
		if idx.pageMap[pgno] >= n {
			delete(idx.pageMap, pgno)
		}
	}
	idx.framePages = idx.framePages[:n]
}

// rebuild reconstructs the index from frame data.
func (idx *walIndex) rebuild(frames []walFrameEntry) {
	idx.pageMap = make(map[uint32]int, len(frames))
	idx.framePages = make([]uint32, len(frames))
	for i, f := range frames {
		idx.framePages[i] = uint32(f.pageNum)
		idx.pageMap[uint32(f.pageNum)] = i
	}
}

// reset clears the index.
func (idx *walIndex) reset() {
	idx.pageMap = make(map[uint32]int)
	idx.framePages = nil
	idx.hdr.mxFrame = 0
}

// ──────────────────────────────────────────────────────────────
// In-memory frame representation
// ──────────────────────────────────────────────────────────────

type walFrameEntry struct {
	pageNum PageNumber
	data    []byte
	dbSize  uint32 // non-zero for commit frames
}

// ──────────────────────────────────────────────────────────────
// Checksum algorithm (matching SQLite's walChecksumBytes)
// ──────────────────────────────────────────────────────────────

// walChecksum computes the rolling checksum used by SQLite's WAL.
// bigEnd = true means interpret data as big-endian uint32s.
// s1, s2 are the running checksum state.
func walChecksum(bigEnd bool, data []byte, s1, s2 uint32) (uint32, uint32) {
	n := len(data)
	if n%8 != 0 {
		// Pad to multiple of 8
		pad := make([]byte, n+(8-n%8))
		copy(pad, data)
		data = pad
		n = len(data)
	}

	for i := 0; i < n; i += 8 {
		var v0, v1 uint32
		if bigEnd {
			v0 = binary.BigEndian.Uint32(data[i : i+4])
			v1 = binary.BigEndian.Uint32(data[i+4 : i+8])
		} else {
			v0 = binary.LittleEndian.Uint32(data[i : i+4])
			v1 = binary.LittleEndian.Uint32(data[i+4 : i+8])
		}
		s1 += v0 + s2
		s2 += v1 + s1
	}
	return s1, s2
}

// ──────────────────────────────────────────────────────────────
// WAL - the main structure
// ──────────────────────────────────────────────────────────────

// WAL implements Write-Ahead Logging for the pager.
// It stores the on-disk WAL file header, frame data, maintains an
// in-memory index for fast lookups, and supports checkpointing.
type WAL struct {
	mu sync.Mutex

	vfs      vfs.VFS
	file     vfs.File
	shmFile  vfs.File // shm file for shared memory (or nil)
	path     string   // WAL file path
	shmPath  string   // shm file path
	pageSize int

	hdr     walHeader   // current WAL header
	frames  []walFrameEntry // committed frames
	pending []walFrameEntry // uncommitted frames in current write tx

	index   *walIndex   // page-to-frame index

	open     bool
	writeTx  bool
	readers  [walNReader]int // reader slots: 0=unused, >0=maxFrame for that reader

	// running checksum state (carries across frames within a write)
	cksum1 uint32
	cksum2 uint32
}

// NewWAL creates a new WAL instance.
func NewWAL(path string, v vfs.VFS, pageSize int) *WAL {
	return &WAL{
		vfs:      v,
		path:     path,
		shmPath:  path + "-shm",
		pageSize: pageSize,
		index:    newWalIndex(),
	}
}

// ──────────────────────────────────────────────────────────────
// Open / Close
// ──────────────────────────────────────────────────────────────

// Open opens or creates the WAL file and rebuilds the index from disk.
func (w *WAL) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	flags := vfs.OpenReadWrite | vfs.OpenCreate
	f, err := w.vfs.Open(w.path, flags, vfs.FileWAL)
	if err != nil {
		return fmt.Errorf("wal open: %w", err)
	}
	w.file = f
	w.open = true

	// Try to open shm file (best-effort; not required for single-process)
	sf, err := w.vfs.Open(w.shmPath, flags, vfs.FileWAL)
	if err == nil {
		w.shmFile = sf
	}

	// Check if the file already has content
	size, err := f.Size()
	if err != nil {
		return fmt.Errorf("wal size: %w", err)
	}

	if size >= walHeaderSize {
		// Read and validate existing header
		if err := w.readAndValidateHeader(); err != nil {
			// Corrupt WAL: reinitialize
			return w.initHeader()
		}
		// Recover frames from disk
		if err := w.recoverFrames(); err != nil {
			return fmt.Errorf("wal recover: %w", err)
		}
	} else {
		// New WAL: write fresh header
		if err := w.initHeader(); err != nil {
			return err
		}
	}

	return nil
}

// initHeader writes a fresh WAL header to disk.
func (w *WAL) initHeader() error {
	// Use big-endian checksums (walMagicBE)
	w.hdr = walHeader{
		magic:       walMagicBE,
		version:     walFileVersion,
		pageSize:    uint32(w.pageSize),
		ckptSeq:     0,
		salt1:       generateSalt(),
		salt2:       generateSalt(),
		bigEndCksum: true,
	}
	// Compute header checksum over first 24 bytes
	s1, s2 := walChecksum(true, w.hdr.encode()[:24], 0, 0)
	w.hdr.checksum1 = s1
	w.hdr.checksum2 = s2

	if err := w.file.Write(w.hdr.encode(), 0); err != nil {
		return fmt.Errorf("wal write header: %w", err)
	}
	if err := w.file.Sync(vfs.SyncFull); err != nil {
		return err
	}

	w.frames = nil
	w.pending = nil
	w.cksum1 = s1
	w.cksum2 = s2
	w.index.reset()

	return nil
}

// readAndValidateHeader reads the WAL header and validates checksums.
func (w *WAL) readAndValidateHeader() error {
	buf := make([]byte, walHeaderSize)
	if err := w.file.Read(buf, 0); err != nil {
		return fmt.Errorf("wal read header: %w", err)
	}
	w.hdr = decodeWalHeader(buf)

	// Validate magic
	if w.hdr.magic != walMagicBE && w.hdr.magic != walMagicLE {
		return fmt.Errorf("wal: bad magic %08x", w.hdr.magic)
	}
	// Validate version
	if w.hdr.version != walFileVersion {
		return fmt.Errorf("wal: unsupported version %d", w.hdr.version)
	}
	// Validate header checksum
	s1, s2 := walChecksum(w.hdr.bigEndCksum, buf[:24], 0, 0)
	if s1 != w.hdr.checksum1 || s2 != w.hdr.checksum2 {
		return fmt.Errorf("wal: header checksum mismatch")
	}

	w.cksum1 = s1
	w.cksum2 = s2
	return nil
}

// recoverFrames reads all valid frames from the WAL file and rebuilds
// the in-memory frame list and index.
func (w *WAL) recoverFrames() error {
	// Compute expected file size for frame scanning
	frameSize := walFrameHdrSize + w.pageSize
	size, err := w.file.Size()
	if err != nil {
		return err
	}
	nFrames := int((size - walHeaderSize) / int64(frameSize))
	if nFrames < 0 {
		nFrames = 0
	}

	s1, s2 := w.cksum1, w.cksum2
	w.frames = make([]walFrameEntry, 0, nFrames)

	frameHdrBuf := make([]byte, walFrameHdrSize)
	pageBuf := make([]byte, w.pageSize)

	for i := 0; i < nFrames; i++ {
		offset := int64(walHeaderSize + i*frameSize)

		// Read frame header
		if err := w.file.Read(frameHdrBuf, offset); err != nil {
			break // stop at first read error
		}
		fhdr := decodeWalFrameHdr(frameHdrBuf)

		// Validate salt
		if fhdr.salt1 != w.hdr.salt1 || fhdr.salt2 != w.hdr.salt2 {
			break // salt mismatch = end of valid frames
		}

		// Compute and validate frame checksum
		// Checksum covers first 8 bytes of frame header (pageNo + dbSize)
		// and then the page data, chained from prior checksum
		fs1, fs2 := walChecksum(w.hdr.bigEndCksum, frameHdrBuf[:8], s1, s2)

		// Read page data
		if err := w.file.Read(pageBuf, offset+walFrameHdrSize); err != nil {
			break
		}
		fs1, fs2 = walChecksum(w.hdr.bigEndCksum, pageBuf, fs1, fs2)

		if fs1 != fhdr.checksum1 || fs2 != fhdr.checksum2 {
			break // checksum mismatch = end of valid frames
		}

		// Valid frame: add to list
		data := make([]byte, w.pageSize)
		copy(data, pageBuf)
		w.frames = append(w.frames, walFrameEntry{
			pageNum: PageNumber(fhdr.pageNo),
			data:    data,
			dbSize:  fhdr.dbSize,
		})

		s1, s2 = fs1, fs2
	}

	// Update running checksum state to match the last valid frame
	w.cksum1 = s1
	w.cksum2 = s2

	// Rebuild the index
	w.index.rebuild(w.frames)

	return nil
}

// Close closes the WAL file and any associated resources.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.open {
		return nil
	}

	var firstErr error
	if w.shmFile != nil {
		if err := w.shmFile.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		w.shmFile = nil
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		w.file = nil
	}
	w.open = false
	return firstErr
}

// ──────────────────────────────────────────────────────────────
// Read operations
// ──────────────────────────────────────────────────────────────

// BeginReadTx starts a read transaction. Returns the max frame count
// the reader should consider valid (frames with 0-based index < mxFrame).
func (w *WAL) BeginReadTx() (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	mx := len(w.frames)
	// Find a free reader slot
	for i := 0; i < walNReader; i++ {
		if w.readers[i] == 0 {
			w.readers[i] = mx
			return mx, nil
		}
	}
	// All reader slots occupied - still return current frame count
	return mx, nil
}

// EndReadTx ends a read transaction for the given reader.
func (w *WAL) EndReadTx(mxFrame int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i := 0; i < walNReader; i++ {
		if w.readers[i] == mxFrame {
			w.readers[i] = 0
			return
		}
	}
}

// ReadPage reads the latest committed version of a page from the WAL.
// Returns (data, true) if found, or (nil, false) if not in WAL.
func (w *WAL) ReadPage(pageNum PageNumber, mxFrame int) ([]byte, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if mxFrame <= 0 {
		return nil, false
	}

	// Use the index for fast lookup
	fi := w.index.lookup(uint32(pageNum), mxFrame)
	if fi < 0 {
		return nil, false
	}

	data := make([]byte, w.pageSize)
	copy(data, w.frames[fi].data)
	return data, true
}

// FrameCount returns the number of committed frames.
func (w *WAL) FrameCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.frames)
}

// MaxFrame returns the current maximum frame number (1-based).
func (w *WAL) MaxFrame() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.frames)
}

// ──────────────────────────────────────────────────────────────
// Write operations
// ──────────────────────────────────────────────────────────────

// BeginWriteTx acquires the write lock on the WAL.
func (w *WAL) BeginWriteTx() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writeTx {
		return fmt.Errorf("wal: write transaction already active")
	}
	if !w.open {
		return fmt.Errorf("wal: not open")
	}

	// Acquire write lock on the WAL file
	if err := w.file.Lock(vfs.LockExclusive); err != nil {
		return fmt.Errorf("wal: cannot acquire write lock: %w", err)
	}

	w.writeTx = true
	w.pending = nil
	// Reset checksum to the state after the last committed frame
	w.cksum1, w.cksum2 = w.computeLastChecksum()

	return nil
}

// computeLastChecksum returns the checksum state after the last committed frame.
func (w *WAL) computeLastChecksum() (uint32, uint32) {
	// Recompute from scratch. For correctness we need the rolling checksum.
	// Start from header checksum.
	buf := w.hdr.encode()
	s1, s2 := walChecksum(w.hdr.bigEndCksum, buf[:24], 0, 0)

	frameSize := walFrameHdrSize + w.pageSize
	for i, f := range w.frames {
		// Checksum first 8 bytes of frame header
		fhdr := make([]byte, 8)
		binary.BigEndian.PutUint32(fhdr[0:4], uint32(f.pageNum))
		binary.BigEndian.PutUint32(fhdr[4:8], f.dbSize)
		s1, s2 = walChecksum(w.hdr.bigEndCksum, fhdr, s1, s2)
		// Checksum page data
		s1, s2 = walChecksum(w.hdr.bigEndCksum, f.data, s1, s2)
		_ = frameSize
		_ = i
	}
	return s1, s2
}

// WriteFrame writes a frame to the WAL as part of the current write transaction.
// The frame is buffered in memory until Commit is called.
func (w *WAL) WriteFrame(pageNum PageNumber, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.open || !w.writeTx {
		return fmt.Errorf("wal: not open or no write tx")
	}

	frame := walFrameEntry{
		pageNum: pageNum,
		data:    make([]byte, w.pageSize),
	}
	copy(frame.data, data)
	w.pending = append(w.pending, frame)
	return nil
}

// Commit finalizes the current write transaction by flushing all pending
// frames to disk with proper checksums and a commit marker on the last frame.
func (w *WAL) Commit(dbPageCount int) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.writeTx {
		return nil
	}
	if len(w.pending) == 0 {
		w.writeTx = false
		w.file.Unlock(vfs.LockShared)
		return nil
	}

	frameSize := walFrameHdrSize + w.pageSize
	s1, s2 := w.cksum1, w.cksum2

	for i, f := range w.pending {
		// Frame offset in the WAL file
		frameIdx := len(w.frames) + i
		offset := int64(walHeaderSize + frameIdx*frameSize)

		// Build frame header: first 8 bytes
		fhdrBuf := make([]byte, walFrameHdrSize)
		binary.BigEndian.PutUint32(fhdrBuf[0:4], uint32(f.pageNum))
		isLast := i == len(w.pending)-1
		if isLast {
			binary.BigEndian.PutUint32(fhdrBuf[4:8], uint32(dbPageCount))
		} else {
			binary.BigEndian.PutUint32(fhdrBuf[4:8], 0)
		}

		// Copy salt from WAL header
		binary.BigEndian.PutUint32(fhdrBuf[8:12], w.hdr.salt1)
		binary.BigEndian.PutUint32(fhdrBuf[12:16], w.hdr.salt2)

		// Compute checksum over first 8 bytes of frame header + page data
		s1, s2 = walChecksum(w.hdr.bigEndCksum, fhdrBuf[:8], s1, s2)
		s1, s2 = walChecksum(w.hdr.bigEndCksum, f.data, s1, s2)

		// Store checksum in frame header
		binary.BigEndian.PutUint32(fhdrBuf[16:20], s1)
		binary.BigEndian.PutUint32(fhdrBuf[20:24], s2)

		// Write frame header
		if err := w.file.Write(fhdrBuf, offset); err != nil {
			return fmt.Errorf("wal write frame hdr %d: %w", frameIdx, err)
		}
		// Write page data
		if err := w.file.Write(f.data, offset+walFrameHdrSize); err != nil {
			return fmt.Errorf("wal write frame data %d: %w", frameIdx, err)
		}
	}

	// Sync WAL to disk
	if err := w.file.Sync(vfs.SyncFull); err != nil {
		return fmt.Errorf("wal sync: %w", err)
	}

	// Move pending frames to committed list and update index
	for i, f := range w.pending {
		isLast := i == len(w.pending)-1
		entry := walFrameEntry{
			pageNum: f.pageNum,
			data:    f.data,
		}
		if isLast {
			entry.dbSize = uint32(dbPageCount)
		}
		w.frames = append(w.frames, entry)
		w.index.append(uint32(f.pageNum))
	}

	// Update checksum state
	w.cksum1, w.cksum2 = s1, s2
	w.pending = nil
	w.writeTx = false
	w.file.Unlock(vfs.LockShared)

	return nil
}

// Rollback discards uncommitted frames and releases the write lock.
func (w *WAL) Rollback() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.writeTx {
		return nil
	}

	// Discard pending frames
	w.pending = nil
	w.writeTx = false

	// Truncate WAL file back to last committed frame
	if w.open && w.file != nil {
		frameSize := walFrameHdrSize + w.pageSize
		targetSize := int64(walHeaderSize + len(w.frames)*frameSize)
		_ = w.file.Truncate(targetSize)
	}

	w.file.Unlock(vfs.LockShared)
	return nil
}

// ──────────────────────────────────────────────────────────────
// Checkpoint
// ──────────────────────────────────────────────────────────────

// Checkpoint copies committed frames from WAL back to the database file.
// Returns (framesCheckpointed, framesRemaining, error).
//
// Modes:
//   - PASSIVE (0): checkpoint without blocking readers
//   - FULL (1): wait for all readers to finish, then checkpoint
//   - RESTART (2): like FULL, then restart the WAL
//   - TRUNCATE (3): like RESTART, then truncate the WAL file
func (w *WAL) Checkpoint(dbFile vfs.File, mode int) (int, int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.open {
		return 0, 0, fmt.Errorf("wal: not open")
	}
	if dbFile == nil {
		return 0, 0, fmt.Errorf("wal: nil db file")
	}

	nFrames := len(w.frames)
	if nFrames == 0 {
		return 0, 0, nil
	}

	// For FULL/RESTART/TRUNCATE, ensure no active readers block progress.
	if mode >= CheckpointFull {
		// Check for active readers that would prevent checkpointing
		minReader := math.MaxInt32
		for i := 0; i < walNReader; i++ {
			if w.readers[i] > 0 && w.readers[i] < minReader {
				minReader = w.readers[i]
			}
		}
		// Can only checkpoint up to the minimum reader's maxFrame
		maxSafe := nFrames
		if minReader < maxSafe {
			maxSafe = minReader
		}
		if maxSafe == 0 {
			return 0, nFrames, nil
		}
		// If we can't checkpoint everything in FULL+ mode, return partial
		if maxSafe < nFrames && mode >= CheckpointFull {
			// Checkpoint what we can
			nFrames = maxSafe
		}
	}

	// Build a map of page -> latest frame for pages to checkpoint
	// (only up to nFrames)
	pageData := make(map[uint32][]byte)
	for i := 0; i < nFrames; i++ {
		pgno := uint32(w.frames[i].pageNum)
		data := make([]byte, w.pageSize)
		copy(data, w.frames[i].data)
		pageData[pgno] = data
	}

	// Write pages to the main database file
	for pgno, data := range pageData {
		offset := int64(pgno-1) * int64(w.pageSize)
		if err := dbFile.Write(data, offset); err != nil {
			return 0, nFrames, fmt.Errorf("wal checkpoint write page %d: %w", pgno, err)
		}
	}

	// Sync the database file
	if err := dbFile.Sync(vfs.SyncFull); err != nil {
		return 0, nFrames, fmt.Errorf("wal checkpoint sync db: %w", err)
	}

	checkpointed := len(pageData)
	remaining := len(w.frames) - nFrames

	// Remove checkpointed frames
	if nFrames < len(w.frames) {
		// Partial checkpoint: keep later frames
		w.frames = w.frames[nFrames:]
		// Rebuild index for remaining frames
		w.index.rebuild(w.frames)
	} else {
		// Full checkpoint: clear everything
		w.frames = w.frames[:0]
		w.index.reset()
	}

	// Update WAL header with new checkpoint sequence and salt
	w.hdr.ckptSeq++
	w.hdr.salt1 = generateSalt()
	w.hdr.salt2 = generateSalt()
	// Recompute header checksum
	s1, s2 := walChecksum(w.hdr.bigEndCksum, w.hdr.encode()[:24], 0, 0)
	w.hdr.checksum1 = s1
	w.hdr.checksum2 = s2
	w.cksum1 = s1
	w.cksum2 = s2

	if mode >= CheckpointRestart {
		// Truncate the WAL to just the header
		if w.file != nil {
			w.file.Truncate(walHeaderSize)
			w.file.Write(w.hdr.encode(), 0)
			w.file.Sync(vfs.SyncFull)
		}
		// Reset frames and index
		w.frames = w.frames[:0]
		w.index.reset()
		remaining = 0

		if mode >= CheckpointTruncate {
			// Delete the WAL file entirely (or truncate to zero)
			// In practice, we truncate to header size (keeps it reusable)
			// The WAL header is rewritten above already.
		}
	} else if len(w.frames) == 0 && w.file != nil {
		// All frames checkpointed: truncate WAL to header
		w.file.Truncate(walHeaderSize)
		w.file.Write(w.hdr.encode(), 0)
		w.file.Sync(vfs.SyncFull)
	}

	return checkpointed, remaining, nil
}

// ──────────────────────────────────────────────────────────────
// Utility
// ──────────────────────────────────────────────────────────────

// generateSalt generates a pseudo-random salt value.
var saltCounter uint32

func generateSalt() uint32 {
	saltCounter++
	// Multiplicative hash with Knuth's constant
	return saltCounter * 2654435761
}

// DBPageCount returns the database page count from the last commit frame.
func (w *WAL) DBPageCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	// Find the last commit frame
	for i := len(w.frames) - 1; i >= 0; i-- {
		if w.frames[i].dbSize > 0 {
			return int(w.frames[i].dbSize)
		}
	}
	return 0
}

// IsOpen returns whether the WAL is open.
func (w *WAL) IsOpen() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.open
}

// PageSize returns the page size.
func (w *WAL) PageSize() int {
	return w.pageSize
}

// Salt returns the current salt values.
func (w *WAL) Salt() (uint32, uint32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.hdr.salt1, w.hdr.salt2
}

// CkptSeq returns the current checkpoint sequence number.
func (w *WAL) CkptSeq() uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.hdr.ckptSeq
}
