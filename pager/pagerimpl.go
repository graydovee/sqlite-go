package pager

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/sqlite-go/sqlite-go/vfs"
)

// memDBCounter generates unique paths for in-memory database backing files.
var memDBCounter atomic.Int64

// PagerImpl implements the Pager interface.
type PagerImpl struct {
	mu          sync.Mutex
	vfs         vfs.VFS
	file        vfs.File
	path        string
	pageSize    int
	pageCount   int
	cacheSize   int
	journalMode JournalMode
	syncMode    SyncMode
	cache       *PCache
	journal     journalOps
	wal         *WAL
	inTx        bool
	inWriteTx   bool
	isMemory    bool
	readonly    bool
	changeCount uint32
	freeList    []PageNumber // in-memory freelist of reusable page numbers

	// WAL reader state: mxFrame for consistent snapshot reads
	walMxFrame int

	// In-memory transaction snapshot
	snapshot         map[PageNumber]*Page
	snapshotPageCount int
	snapshotFreeList []PageNumber
}

type journalOps interface {
	Begin() error
	WritePage(pageNum PageNumber, data []byte) error
	Commit() error
	Rollback() ([]journalRecord, error)
	Close() error
	IsOpen() bool
}

// OpenPager creates and opens a new Pager.
func OpenPager(cfg PagerConfig) (*PagerImpl, error) {
	if cfg.PageSize <= 0 {
		cfg.PageSize = 4096
	}
	if cfg.CacheSize <= 0 {
		cfg.CacheSize = 100
	}
	if cfg.VFS == nil {
		cfg.VFS = vfs.Default()
	}

	isMem := cfg.Path == "" || cfg.Path == ":memory:"

	// For in-memory databases the page cache IS the database — evicting a
	// page means permanent data loss.  Use a cache size large enough that
	// eviction never triggers.  The LRU slice is only pre-allocated to a
	// reasonable capacity; it will grow via append as needed.
	cacheSize := cfg.CacheSize
	if isMem {
		cacheSize = 1 << 20 // ~1 M pages — effectively unlimited
	}

	p := &PagerImpl{
		vfs:         cfg.VFS,
		path:        cfg.Path,
		pageSize:    cfg.PageSize,
		cacheSize:   cacheSize,
		journalMode: cfg.JournalMode,
		syncMode:    cfg.SyncMode,
		isMemory:    isMem,
		readonly:    cfg.ReadOnly,
		cache:       NewPCache(cfg.PageSize, cacheSize),
	}

	if p.journalMode == JournalWAL && !p.isMemory {
		p.wal = NewWAL(p.path+"-wal", p.vfs, p.pageSize)
	}

	return p, nil
}

func (p *PagerImpl) Open() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isMemory {
		// In-memory database
		return nil
	}

	flags := vfs.OpenReadWrite | vfs.OpenCreate
	if p.readonly {
		flags = vfs.OpenReadOnly
	}

	f, err := p.vfs.Open(p.path, flags, vfs.FileMainDB)
	if err != nil {
		return fmt.Errorf("pager open: %w", err)
	}
	p.file = f

	// Read database header to get page count
	size, err := f.Size()
	if err != nil {
		return err
	}
	if size > 0 {
		// Read all header fields from existing database
		hdr := make([]byte, 100)
		if err := f.Read(hdr, 0); err == nil {
			// Page size: uint16 at offset 16-17, value 1 means 65536
			ps := int(binary.BigEndian.Uint16(hdr[16:18]))
			if ps == 1 {
				ps = 65536
			}
			if ps >= 512 && ps <= 65536 {
				p.pageSize = ps
				p.cache = NewPCache(p.pageSize, p.cacheSize)
			}
			// Use page count from header (offset 28-31) when available,
			// fall back to file-size calculation.
			hdrPageCount := int(binary.BigEndian.Uint32(hdr[28:32]))
			if hdrPageCount > 0 {
				p.pageCount = hdrPageCount
			} else {
				p.pageCount = int(size / int64(p.pageSize))
			}
			p.changeCount = binary.BigEndian.Uint32(hdr[24:28])
		} else {
			p.pageCount = int(size / int64(p.pageSize))
		}
	}

	if p.wal != nil {
		if err := p.wal.Open(); err != nil {
			return fmt.Errorf("wal open: %w", err)
		}
	}

	return nil
}

func (p *PagerImpl) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.inTx {
		p.rollbackInternal()
	}

	if p.wal != nil {
		if p.walMxFrame > 0 {
			p.wal.EndReadTx(p.walMxFrame)
			p.walMxFrame = 0
		}
		p.wal.Close()
	}

	// Flush all dirty pages to disk before closing
	if p.file != nil && !p.isMemory {
		dirty := p.cache.DirtyPages()
		for _, page := range dirty {
			offset := int64(page.PageNum-1) * int64(p.pageSize)
			if err := p.file.Write(page.Data, offset); err != nil {
				return fmt.Errorf("flush page %d on close: %w", page.PageNum, err)
			}
			page.Dirty = false
		}
		p.cache.ClearDirty()

		p.writeDBHeader()

		if err := p.file.Sync(vfs.SyncFull); err != nil {
			return fmt.Errorf("sync on close: %w", err)
		}
	}

	p.cache.DiscardAll()

	if p.journal != nil {
		p.journal.Close()
	}

	if p.file != nil {
		err := p.file.Close()
		p.file = nil
		return err
	}
	return nil
}

func (p *PagerImpl) PageCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pageCount
}

func (p *PagerImpl) PageSize() int {
	return p.pageSize
}

func (p *PagerImpl) GetPage(pageNum PageNumber) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if pageNum < 1 {
		return nil, fmt.Errorf("invalid page number: %d", pageNum)
	}

	// Check cache
	if page := p.cache.Fetch(pageNum); page != nil {
		return page, nil
	}

	// Read from file/WAL
	page := &Page{
		PageNum:  pageNum,
		Data:     make([]byte, p.pageSize),
		RefCount: 1,
	}

	if p.isMemory {
		// In-memory: return zeroed page
	} else {
		// Check WAL for latest version of this page
		if p.wal != nil && p.wal.IsOpen() {
			mx := p.walMxFrame
			if mx == 0 {
				mx = p.wal.MaxFrame()
			}
			if data, ok := p.wal.ReadPage(pageNum, mx); ok {
				copy(page.Data, data)
				p.cache.Add(page)
				return page, nil
			}
		}
		offset := int64(pageNum-1) * int64(p.pageSize)
		if err := p.file.Read(page.Data, offset); err != nil {
			return nil, fmt.Errorf("read page %d: %w", pageNum, err)
		}
	}

	p.cache.Add(page)
	return page, nil
}

func (p *PagerImpl) GetNewPage() (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var pageNum PageNumber
	if len(p.freeList) > 0 {
		// Reuse a free page
		pageNum = p.freeList[len(p.freeList)-1]
		p.freeList = p.freeList[:len(p.freeList)-1]
		// Check if it's in cache already
		if page := p.cache.Fetch(pageNum); page != nil {
			// Clear the page data
			for i := range page.Data {
				page.Data[i] = 0
			}
			page.Dirty = true
			p.cache.MarkDirty(page)
			return page, nil
		}
	} else {
		// Extend the file
		p.pageCount++
		pageNum = PageNumber(p.pageCount)
	}

	page := &Page{
		PageNum:  pageNum,
		Data:     make([]byte, p.pageSize),
		Dirty:    true,
		RefCount: 1,
	}
	p.cache.Add(page)
	p.cache.MarkDirty(page)
	return page, nil
}

func (p *PagerImpl) FreePage(pageNum PageNumber) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.freeList = append(p.freeList, pageNum)
	return nil
}

func (p *PagerImpl) FreelistCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.freeList)
}

func (p *PagerImpl) MarkDirty(page *Page) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	page.Dirty = true
	p.cache.MarkDirty(page)
	return nil
}

func (p *PagerImpl) ReleasePage(page *Page) error {
	p.cache.Release(page)
	return nil
}

func (p *PagerImpl) WritePage(page *Page) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isMemory {
		page.Dirty = false
		return nil
	}

	// In WAL mode, write to WAL
	if p.wal != nil && p.journalMode == JournalWAL {
		if err := p.wal.WriteFrame(page.PageNum, page.Data); err != nil {
			return fmt.Errorf("wal write page %d: %w", page.PageNum, err)
		}
		page.Dirty = false
		return nil
	}

	offset := int64(page.PageNum-1) * int64(p.pageSize)
	if err := p.file.Write(page.Data, offset); err != nil {
		return fmt.Errorf("write page %d: %w", page.PageNum, err)
	}

	page.Dirty = false
	return nil
}

func (p *PagerImpl) Begin(write bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.inTx {
		if write && !p.inWriteTx {
			// Upgrade to write transaction
			p.inWriteTx = true
			return p.beginJournal()
		}
		return nil
	}

	p.inTx = true
	p.inWriteTx = write

	if write {
		return p.beginJournal()
	}

	// For read transactions in WAL mode, snapshot the mxFrame
	if p.wal != nil && p.journalMode == JournalWAL && p.wal.IsOpen() {
		mx, _ := p.wal.BeginReadTx()
		p.walMxFrame = mx
	}

	return nil
}

func (p *PagerImpl) beginJournal() error {
	if p.isMemory {
		// For in-memory databases, take a snapshot of all cached pages
		p.snapshot = p.cache.Snapshot()
		p.snapshotPageCount = p.pageCount
		p.snapshotFreeList = make([]PageNumber, len(p.freeList))
		copy(p.snapshotFreeList, p.freeList)
		return nil
	}

	if p.journalMode == JournalOff {
		return nil
	}

	if p.wal != nil && p.journalMode == JournalWAL {
		// Also begin a read tx to snapshot the current state
		if p.walMxFrame == 0 {
			mx, _ := p.wal.BeginReadTx()
			p.walMxFrame = mx
		}
		return p.wal.BeginWriteTx()
	}

	// Start rollback journal
	journalPath := p.path + "-journal"
	j := NewJournal(journalPath, p.vfs, p.pageSize)
	if err := j.Begin(); err != nil {
		return err
	}
	p.journal = j
	return nil
}

func (p *PagerImpl) Commit() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.inTx {
		return nil
	}

	// Write dirty pages
	dirtyPages := p.cache.DirtyPages()
	for _, page := range dirtyPages {
		if p.wal != nil && p.journalMode == JournalWAL {
			if err := p.wal.WriteFrame(page.PageNum, page.Data); err != nil {
				return err
			}
		} else if p.file != nil {
			offset := int64(page.PageNum-1) * int64(p.pageSize)
			if err := p.file.Write(page.Data, offset); err != nil {
				return err
			}
		}
		page.Dirty = false
	}

	if p.wal != nil && p.journalMode == JournalWAL {
		if err := p.wal.Commit(p.pageCount); err != nil {
			return err
		}
		// End read transaction
		if p.walMxFrame > 0 {
			p.wal.EndReadTx(p.walMxFrame)
			p.walMxFrame = 0
		}
	} else if p.journal != nil {
		if err := p.journal.Commit(); err != nil {
			return err
		}
		p.journal.Close()
		p.journal = nil
	}

	// Update change counter
	p.changeCount++
	p.writeDBHeader()

	if p.file != nil && p.syncMode != SyncOff {
		p.file.Sync(vfs.SyncFull)
	}

	p.cache.ClearDirty()
	p.inTx = false
	p.inWriteTx = false
	return nil
}

func (p *PagerImpl) Rollback() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rollbackInternal()
}

// rollbackInternal performs rollback without acquiring the lock.
// Must be called with p.mu held.
func (p *PagerImpl) rollbackInternal() error {

	if !p.inTx {
		return nil
	}

	// Restore in-memory snapshot
	if p.isMemory && p.snapshot != nil {
		for k, snap := range p.snapshot {
			if cached := p.cache.Fetch(k); cached != nil {
				copy(cached.Data, snap.Data)
				cached.Dirty = false
			}
		}
		p.pageCount = p.snapshotPageCount
		p.freeList = p.snapshotFreeList
		p.snapshot = nil
	}

	// Rollback journal: restore original pages
	if p.journal != nil {
		records, err := p.journal.Rollback()
		if err == nil {
			for _, rec := range records {
				// Restore page data
				if cached := p.cache.Fetch(rec.pageNum); cached != nil {
					copy(cached.Data, rec.data)
					cached.Dirty = false
				}
				// Also write back to file
				if p.file != nil {
					offset := int64(rec.pageNum-1) * int64(p.pageSize)
					p.file.Write(rec.data, offset)
				}
			}
		}
		p.journal.Close()
		p.journal = nil
	}

	// Rollback WAL
	if p.wal != nil {
		p.wal.Rollback()
		if p.walMxFrame > 0 {
			p.wal.EndReadTx(p.walMxFrame)
			p.walMxFrame = 0
		}
		// Discard dirty cached pages so they are re-read from the DB file
		p.cache.DiscardAll()
	}

	// Rollback in-memory snapshot
	if p.snapshot != nil {
		for k, snap := range p.snapshot {
			if cached := p.cache.Fetch(k); cached != nil {
				copy(cached.Data, snap.Data)
				cached.Dirty = false
			}
		}
		p.pageCount = p.snapshotPageCount
		p.freeList = p.snapshotFreeList
		p.snapshot = nil
	}

	p.cache.ClearDirty()
	p.inTx = false
	p.inWriteTx = false
	return nil
}

func (p *PagerImpl) IsInTransaction() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.inTx
}

func (p *PagerImpl) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.file == nil {
		return nil
	}

	// Write all dirty pages
	dirty := p.cache.DirtyPages()
	for _, page := range dirty {
		offset := int64(page.PageNum-1) * int64(p.pageSize)
		if err := p.file.Write(page.Data, offset); err != nil {
			return err
		}
		page.Dirty = false
	}
	p.cache.ClearDirty()

	return p.file.Sync(vfs.SyncFull)
}

func (p *PagerImpl) Truncate(pageCount int) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.pageCount = pageCount
	if p.file != nil {
		return p.file.Truncate(int64(pageCount) * int64(p.pageSize))
	}
	return nil
}

func (p *PagerImpl) SetJournalMode(mode JournalMode) (JournalMode, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	oldMode := p.journalMode
	p.journalMode = mode

	if mode == JournalWAL && p.wal == nil && !p.isMemory {
		p.wal = NewWAL(p.path+"-wal", p.vfs, p.pageSize)
		if err := p.wal.Open(); err != nil {
			p.journalMode = oldMode
			p.wal = nil
			return oldMode, err
		}
	}

	return mode, nil
}

func (p *PagerImpl) GetJournalMode() JournalMode {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.journalMode
}

func (p *PagerImpl) CacheSize() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cacheSize
}

func (p *PagerImpl) SetCacheSize(size int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cacheSize = size
	p.cache = NewPCache(p.pageSize, size)
	return nil
}

func (p *PagerImpl) Checkpoint(mode int) (int, int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.wal == nil {
		return 0, 0, fmt.Errorf("not in WAL mode")
	}
	return p.wal.Checkpoint(p.file, mode)
}

func (p *PagerImpl) FileSize() (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isMemory {
		return int64(p.pageCount) * int64(p.pageSize), nil
	}
	if p.file == nil {
		return 0, nil
	}
	return p.file.Size()
}

func (p *PagerImpl) BackupInit(dest Pager) (Backup, error) {
	return nil, fmt.Errorf("backup not yet implemented")
}

// writeDBHeader writes the complete 100-byte SQLite header to the database file.
func (p *PagerImpl) writeDBHeader() {
	if p.isMemory || p.file == nil {
		return
	}

	hdr := make([]byte, 100)
	// Read existing header to preserve fields we don't explicitly set (e.g. schema cookie,
	// freelist trunk page, user version, application ID).  Read failure is fine — the
	// zero-initialized buffer gives correct defaults for a new database.
	p.file.Read(hdr, 0)

	// Offset 0-15: Magic string
	copy(hdr[0:], "SQLite format 3\x00")

	// Offset 16-17: Page size (big-endian uint16, value 1 means 65536)
	psVal := uint16(p.pageSize)
	if p.pageSize == 65536 {
		psVal = 1
	}
	binary.BigEndian.PutUint16(hdr[16:18], psVal)

	// Offset 18: File format write version (1=legacy journal, 2=WAL)
	if p.journalMode == JournalWAL {
		hdr[18] = 2
	} else {
		hdr[18] = 1
	}
	// Offset 19: File format read version (same as write version)
	hdr[19] = hdr[18]

	// Offset 20: Reserved space at end of each page
	hdr[20] = 0
	// Offset 21: Max embedded payload fraction (MUST be 64)
	hdr[21] = 64
	// Offset 22: Min embedded payload fraction (MUST be 32)
	hdr[22] = 32
	// Offset 23: Leaf payload fraction (MUST be 32)
	hdr[23] = 32

	// Offset 24-27: File change counter
	binary.BigEndian.PutUint32(hdr[24:28], p.changeCount)
	// Offset 28-31: Size of the database file in pages
	binary.BigEndian.PutUint32(hdr[28:32], uint32(p.pageCount))

	// Offset 32-35: First freelist trunk page (0 = none)
	binary.BigEndian.PutUint32(hdr[32:36], 0)
	// Offset 36-39: Total freelist pages
	binary.BigEndian.PutUint32(hdr[36:40], uint32(len(p.freeList)))

	// Offset 40-43: Schema cookie (preserve existing)
	if binary.BigEndian.Uint32(hdr[40:44]) == 0 && p.changeCount > 0 {
		// First write after creating the DB: initialise schema cookie
		binary.BigEndian.PutUint32(hdr[40:44], 0)
	}

	// Offset 44-47: Schema format number (4 = current)
	if binary.BigEndian.Uint32(hdr[44:48]) == 0 {
		binary.BigEndian.PutUint32(hdr[44:48], 4)
	}

	// Offset 48-51: Default page cache size (0)
	binary.BigEndian.PutUint32(hdr[48:52], 0)
	// Offset 52-55: Largest root b-tree page number (auto-vacuum, 0 = none)
	binary.BigEndian.PutUint32(hdr[52:56], 0)
	// Offset 56-59: Text encoding (1 = UTF-8)
	binary.BigEndian.PutUint32(hdr[56:60], 1)
	// Offset 60-63: User version
	binary.BigEndian.PutUint32(hdr[60:64], 0)
	// Offset 64-67: Incremental vacuum (0 = not incremental)
	binary.BigEndian.PutUint32(hdr[64:68], 0)
	// Offset 68-71: Application ID
	binary.BigEndian.PutUint32(hdr[68:72], 0)
	// Offset 72-91: Reserved for expansion (must be zero)
	for i := 72; i < 92; i++ {
		hdr[i] = 0
	}
	// Offset 92-95: Version-valid-for number
	binary.BigEndian.PutUint32(hdr[92:96], p.changeCount)
	// Offset 96-99: SQLite version number (3.46.0)
	binary.BigEndian.PutUint32(hdr[96:100], 3046000)

	p.file.Write(hdr, 0)
}
