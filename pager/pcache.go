package pager

import (
	"sync"
)

// PCache implements a page cache with LRU eviction and dirty tracking.
type PCache struct {
	mu        sync.Mutex
	pageSize  int
	cacheSize int
	pages     map[PageNumber]*Page
	dirty     map[PageNumber]*Page
	lru       []*Page // Front = most recent, Back = least recent
}

// NewPCache creates a new page cache.
func NewPCache(pageSize, cacheSize int) *PCache {
	if pageSize <= 0 {
		pageSize = 4096
	}
	if cacheSize <= 0 {
		cacheSize = 100
	}
	return &PCache{
		pageSize:  pageSize,
		cacheSize: cacheSize,
		pages:     make(map[PageNumber]*Page),
		dirty:     make(map[PageNumber]*Page),
		lru:       make([]*Page, 0, cacheSize),
	}
}

// Fetch retrieves a page from cache. Returns nil if not cached.
func (pc *PCache) Fetch(pageNum PageNumber) *Page {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	p := pc.pages[pageNum]
	if p != nil {
		p.RefCount++
		pc.touchLRU(p)
	}
	return p
}

// Add adds a page to the cache.
func (pc *PCache) Add(page *Page) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	page.RefCount++
	pc.pages[page.PageNum] = page
	pc.touchLRU(page)

	// Evict if over capacity
	for len(pc.pages) > pc.cacheSize {
		pc.evictOne()
	}
	return nil
}

// Release decrements the reference count on a page.
func (pc *PCache) Release(page *Page) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if page.RefCount > 0 {
		page.RefCount--
	}
}

// MarkDirty marks a page as dirty.
func (pc *PCache) MarkDirty(page *Page) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	page.Dirty = true
	pc.dirty[page.PageNum] = page
}

// DirtyPages returns all dirty pages.
func (pc *PCache) DirtyPages() []*Page {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	result := make([]*Page, 0, len(pc.dirty))
	for _, p := range pc.dirty {
		result = append(result, p)
	}
	return result
}

// ClearDirty clears the dirty flag on all pages.
func (pc *PCache) ClearDirty() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	for _, p := range pc.dirty {
		p.Dirty = false
	}
	pc.dirty = make(map[PageNumber]*Page)
}

// DiscardAll removes all pages from cache.
func (pc *PCache) DiscardAll() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.pages = make(map[PageNumber]*Page)
	pc.dirty = make(map[PageNumber]*Page)
	pc.lru = pc.lru[:0]
}

// Size returns the number of cached pages.
func (pc *PCache) Size() int {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return len(pc.pages)
}

// Snapshot returns a deep copy of all cached pages for in-memory rollback.
func (pc *PCache) Snapshot() map[PageNumber]*Page {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	snap := make(map[PageNumber]*Page, len(pc.pages))
	for k, p := range pc.pages {
		cp := &Page{
			PageNum: p.PageNum,
			Data:    make([]byte, len(p.Data)),
			Dirty:   p.Dirty,
		}
		copy(cp.Data, p.Data)
		snap[k] = cp
	}
	return snap
}

// PageSize returns the page size.
func (pc *PCache) PageSize() int {
	return pc.pageSize
}

// touchLRU moves a page to the front of the LRU list.
func (pc *PCache) touchLRU(p *Page) {
	// Remove from current position
	for i, lp := range pc.lru {
		if lp == p {
			pc.lru = append(pc.lru[:i], pc.lru[i+1:]...)
			break
		}
	}
	// Add to front
	pc.lru = append([]*Page{p}, pc.lru...)
}

// evictOne evicts the least recently used unreferenced page.
func (pc *PCache) evictOne() {
	// Find LRU page with RefCount == 0 and not dirty
	for i := len(pc.lru) - 1; i >= 0; i-- {
		p := pc.lru[i]
		if p.RefCount == 0 && !p.Dirty {
			delete(pc.pages, p.PageNum)
			pc.lru = append(pc.lru[:i], pc.lru[i+1:]...)
			return
		}
	}
	// If all pages are referenced or dirty, can't evict
}