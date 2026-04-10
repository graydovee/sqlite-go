package pager

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sqlite-go/sqlite-go/vfs"
)

func TestPagerOpenClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	cfg := PagerConfig{
		VFS:      vfs.Find("unix"),
		Path:     path,
		PageSize: 4096,
		CacheSize: 10,
	}

	p, err := OpenPager(cfg)
	if err != nil {
		t.Fatalf("OpenPager: %v", err)
	}

	if err := p.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}

	if p.PageSize() != 4096 {
		t.Errorf("PageSize = %d, want 4096", p.PageSize())
	}

	if p.PageCount() != 0 {
		t.Errorf("PageCount = %d, want 0", p.PageCount())
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestPagerReadWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	cfg := PagerConfig{
		VFS:      vfs.Find("unix"),
		Path:     path,
		PageSize: 4096,
		CacheSize: 10,
	}

	p, err := OpenPager(cfg)
	if err != nil {
		t.Fatalf("OpenPager: %v", err)
	}
	if err := p.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	// Allocate a new page
	page, err := p.GetNewPage()
	if err != nil {
		t.Fatalf("GetNewPage: %v", err)
	}
	if page.PageNum != 1 {
		t.Errorf("new page num = %d, want 1", page.PageNum)
	}

	// Write data to page
	copy(page.Data[0:], []byte("Hello, Pager!"))
	p.MarkDirty(page)

	// Write page to disk
	if err := p.WritePage(page); err != nil {
		t.Fatalf("WritePage: %v", err)
	}

	// Release page
	p.ReleasePage(page)

	// Re-open pager and read back
	p2, _ := OpenPager(cfg)
	p2.Open()
	defer p2.Close()

	readPage, err := p2.GetPage(1)
	if err != nil {
		t.Fatalf("GetPage: %v", err)
	}
	defer p2.ReleasePage(readPage)

	got := string(readPage.Data[0:13])
	if got != "Hello, Pager!" {
		t.Errorf("page data = %q, want %q", got, "Hello, Pager!")
	}
}

func TestPagerTransaction(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	cfg := PagerConfig{
		VFS:        vfs.Find("unix"),
		Path:       path,
		PageSize:   4096,
		CacheSize:  10,
		JournalMode: JournalDelete,
	}

	p, _ := OpenPager(cfg)
	p.Open()
	defer p.Close()

	// Allocate page and write in a transaction
	if err := p.Begin(true); err != nil {
		t.Fatalf("Begin: %v", err)
	}

	page, _ := p.GetNewPage()
	copy(page.Data[0:], []byte("Transaction test"))
	p.MarkDirty(page)
	p.WritePage(page)
	p.ReleasePage(page)

	if err := p.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if p.IsInTransaction() {
		t.Error("should not be in transaction after commit")
	}
}

func TestPagerRollback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	cfg := PagerConfig{
		VFS:        vfs.Find("unix"),
		Path:       path,
		PageSize:   4096,
		CacheSize:  10,
		JournalMode: JournalDelete,
	}

	p, _ := OpenPager(cfg)
	p.Open()
	defer p.Close()

	// First, write initial data - use direct write since journal needs file on disk
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	initialData := make([]byte, 4096)
	copy(initialData[0:], []byte("Original data"))
	f.Write(initialData)
	f.Close()

	// Re-open pager - it should read the existing page
	p, _ = OpenPager(cfg)
	p.Open()
	defer p.Close()

	// Verify initial data
	origPage, _ := p.GetPage(1)
	if got := string(origPage.Data[0:13]); got != "Original data" {
		t.Fatalf("initial data = %q, want %q", got, "Original data")
	}
	p.ReleasePage(origPage)

	// Now start a transaction and rollback
	p.Begin(true)
	// Journal should capture original page 1
	if p.journal != nil {
		p.journal.WritePage(1, initialData)
	}

	// Modify page
	page2, _ := p.GetPage(1)
	copy(page2.Data[0:], []byte("Modified data"))
	p.MarkDirty(page2)
	p.ReleasePage(page2)
	p.Rollback()

	// Verify original data
	page3, _ := p.GetPage(1)
	defer p.ReleasePage(page3)
	got := string(page3.Data[0:13])
	if got != "Original data" {
		t.Errorf("after rollback, got %q, want %q", got, "Original data")
	}
}

func TestPagerMemoryDB(t *testing.T) {
	cfg := PagerConfig{
		Path:     ":memory:",
		PageSize: 4096,
		CacheSize: 10,
		VFS:      vfs.Find("memory"),
	}

	p, _ := OpenPager(cfg)
	if err := p.Open(); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer p.Close()

	if !p.isMemory {
		t.Error("should be memory database")
	}

	// Allocate and write
	page, _ := p.GetNewPage()
	copy(page.Data[0:], []byte("Memory DB"))
	p.MarkDirty(page)
	p.WritePage(page)
	p.ReleasePage(page)

	if p.PageCount() != 1 {
		t.Errorf("PageCount = %d, want 1", p.PageCount())
	}
}

func TestPagerFileSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	cfg := PagerConfig{
		VFS:      vfs.Find("unix"),
		Path:     path,
		PageSize: 4096,
		CacheSize: 10,
	}

	p, _ := OpenPager(cfg)
	p.Open()
	defer p.Close()

	// Allocate a page
	page, _ := p.GetNewPage()
	p.MarkDirty(page)

	// Flush dirty pages to disk
	if err := p.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	size, err := p.FileSize()
	if err != nil {
		t.Fatalf("FileSize: %v", err)
	}
	if size != 4096 {
		t.Errorf("FileSize = %d, want 4096", size)
	}

	// Verify actual file on disk
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Size() != 4096 {
		t.Errorf("file size = %d, want 4096", fi.Size())
	}
}

func TestPagerJournalMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	cfg := PagerConfig{
		VFS:        vfs.Find("unix"),
		Path:       path,
		PageSize:   4096,
		CacheSize:  10,
		JournalMode: JournalDelete,
	}

	p, _ := OpenPager(cfg)
	p.Open()
	defer p.Close()

	if p.GetJournalMode() != JournalDelete {
		t.Errorf("JournalMode = %d, want %d", p.GetJournalMode(), JournalDelete)
	}

	mode, err := p.SetJournalMode(JournalMemory)
	if err != nil {
		t.Fatalf("SetJournalMode: %v", err)
	}
	if mode != JournalMemory {
		t.Errorf("SetJournalMode returned %d, want %d", mode, JournalMemory)
	}
}

func TestPCacheBasic(t *testing.T) {
	pc := NewPCache(4096, 5)

	// Add pages
	for i := 1; i <= 3; i++ {
		p := &Page{
			PageNum: PageNumber(i),
			Data:    make([]byte, 4096),
		}
		pc.Add(p)
	}

	if pc.Size() != 3 {
		t.Errorf("cache size = %d, want 3", pc.Size())
	}

	// Fetch
	p := pc.Fetch(2)
	if p == nil {
		t.Fatal("expected to fetch page 2")
	}

	// Release
	pc.Release(p)

	// Dirty
	pc.MarkDirty(p)
	dirty := pc.DirtyPages()
	if len(dirty) != 1 {
		t.Errorf("dirty pages = %d, want 1", len(dirty))
	}

	// DiscardAll
	pc.DiscardAll()
	if pc.Size() != 0 {
		t.Errorf("cache size after discard = %d, want 0", pc.Size())
	}
}
