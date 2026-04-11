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
		p.pageCount = int(size / int64(p.pageSize))
		// Read change counter from header (offset 24)
		hdr := make([]byte, 100)
		if err := f.Read(hdr, 0); err == nil {
			p.pageSize = int(binary.BigEndian.Uint32(hdr[16:20]))
			if p.pageSize < 512 || p.pageSize > 65536 {
				p.pageSize = 4096
			}
			p.changeCount = binary.BigEndian.Uint32(hdr[24:28])
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
		p.Rollback()
	}

	if p.wal != nil {
		if p.walMxFrame > 0 {
			p.wal.EndReadTx(p.walMxFrame)
			p.walMxFrame = 0
		}
		p.wal.Close()
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

// writeDBHeader writes key fields to the database file header.
func (p *PagerImpl) writeDBHeader() {
	if p.isMemory || p.file == nil {
		return
	}

	hdr := make([]byte, 100)
	// Read existing header
	p.file.Read(hdr, 0)

	// Update fields
	binary.BigEndian.PutUint32(hdr[16:20], uint32(p.pageSize))
	binary.BigEndian.PutUint32(hdr[24:28], p.changeCount)
	binary.BigEndian.PutUint32(hdr[28:32], uint32(p.pageCount))

	p.file.Write(hdr, 0)
}
