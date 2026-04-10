package tests

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/sqlite-go/sqlite-go/pager"
	"github.com/sqlite-go/sqlite-go/vfs"
)

// ============================================================================
// Pager tests - ported from C test2.c
// ============================================================================

// openTestPager opens a file-backed pager for testing.
func openTestPagerFile(t *testing.T) *pager.PagerImpl {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "pager_test.db")
	cfg := pager.PagerConfig{
		VFS:      vfs.Find("unix"),
		Path:     path,
		PageSize: 4096,
		CacheSize: 10,
	}
	p, err := pager.OpenPager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// openTestPagerMem opens an in-memory pager for testing.
func openTestPagerMem(t *testing.T) *pager.PagerImpl {
	t.Helper()
	cfg := pager.PagerConfig{
		Path:      ":memory:",
		PageSize:  4096,
		CacheSize: 10,
		VFS:       vfs.Find("memory"),
	}
	p, err := pager.OpenPager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// --- Test: Journal mode switching ---

func TestPagerJournalModeSwitch(t *testing.T) {
	p := openTestPagerFile(t)

	// Default should be JournalDelete
	mode := p.GetJournalMode()
	if mode != pager.JournalDelete {
		t.Errorf("default mode = %v, want JournalDelete", mode)
	}

	// Switch to JournalMemory
	newMode, err := p.SetJournalMode(pager.JournalMemory)
	if err != nil {
		t.Fatalf("SetJournalMode(Memory): %v", err)
	}
	if newMode != pager.JournalMemory {
		t.Errorf("SetJournalMode returned %v, want Memory", newMode)
	}

	// Switch to JournalPersist
	newMode, err = p.SetJournalMode(pager.JournalPersist)
	if err != nil {
		t.Fatalf("SetJournalMode(Persist): %v", err)
	}

	// Switch to JournalTruncate
	newMode, err = p.SetJournalMode(pager.JournalTruncate)
	if err != nil {
		t.Fatalf("SetJournalMode(Truncate): %v", err)
	}

	// Switch back to JournalDelete
	newMode, err = p.SetJournalMode(pager.JournalDelete)
	if err != nil {
		t.Fatalf("SetJournalMode(Delete): %v", err)
	}
	if newMode != pager.JournalDelete {
		t.Errorf("SetJournalMode returned %v, want Delete", newMode)
	}
}

// --- Test: Page cache behavior under pressure ---

func TestPagerPageCachePressure(t *testing.T) {
	cfg := pager.PagerConfig{
		Path:      ":memory:",
		PageSize:  4096,
		CacheSize: 5, // Very small cache
		VFS:       vfs.Find("memory"),
	}
	p, err := pager.OpenPager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	p.Open()
	defer p.Close()

	// Allocate more pages than the cache can hold
	const numPages = 20
	pages := make([]*pager.Page, numPages)

	p.Begin(true)

	for i := 0; i < numPages; i++ {
		page, err := p.GetNewPage()
		if err != nil {
			t.Fatalf("GetNewPage %d: %v", i, err)
		}
		pages[i] = page

		// Write unique data to each page
		header := fmt.Sprintf("page_%d_data", page.PageNum)
		copy(page.Data[0:], []byte(header))
		p.MarkDirty(page)
		p.WritePage(page)
		p.ReleasePage(page)
	}

	p.Commit()

	// Verify all pages can be read back
	for i := 1; i <= numPages; i++ {
		page, err := p.GetPage(pager.PageNumber(i))
		if err != nil {
			t.Fatalf("GetPage(%d): %v", i, err)
		}
		expected := fmt.Sprintf("page_%d_data", i)
		got := string(page.Data[0:len(expected)])
		if got != expected {
			t.Errorf("page %d: got %q, want %q", i, got, expected)
		}
		p.ReleasePage(page)
	}

	if p.PageCount() != numPages {
		t.Errorf("PageCount = %d, want %d", p.PageCount(), numPages)
	}
}

// --- Test: Transaction commit/rollback stress ---

func TestPagerTransactionStress(t *testing.T) {
	p := openTestPagerMem(t)

	const numIterations = 100

	for i := 0; i < numIterations; i++ {
		if err := p.Begin(true); err != nil {
			t.Fatalf("iter %d: Begin: %v", i, err)
		}

		page, err := p.GetNewPage()
		if err != nil {
			p.Rollback()
			t.Fatalf("iter %d: GetNewPage: %v", i, err)
		}

		header := fmt.Sprintf("iter_%d", i)
		copy(page.Data[0:], []byte(header))
		p.MarkDirty(page)
		p.WritePage(page)
		p.ReleasePage(page)

		if i%2 == 0 {
			// Commit on even iterations
			if err := p.Commit(); err != nil {
				t.Fatalf("iter %d: Commit: %v", i, err)
			}
		} else {
			// Rollback on odd iterations
			if err := p.Rollback(); err != nil {
				t.Fatalf("iter %d: Rollback: %v", i, err)
			}
		}
	}

	// After 100 iterations (50 commits, 50 rollbacks), should have 50 pages
	expectedPages := 50
	if p.PageCount() != expectedPages {
		t.Errorf("PageCount = %d, want %d", p.PageCount(), expectedPages)
	}
}

// --- Test: Transaction commit preserves data ---

func TestPagerCommitPreservesData(t *testing.T) {
	p := openTestPagerMem(t)

	// Write data in a transaction
	p.Begin(true)
	page, _ := p.GetNewPage()
	testData := []byte("persistent data that survives commit")
	copy(page.Data[0:], testData)
	p.MarkDirty(page)
	p.WritePage(page)
	p.ReleasePage(page)
	p.Commit()

	// Read back
	page2, err := p.GetPage(1)
	if err != nil {
		t.Fatalf("GetPage(1): %v", err)
	}
	got := string(page2.Data[0:len(testData)])
	if got != string(testData) {
		t.Errorf("got %q, want %q", got, string(testData))
	}
	p.ReleasePage(page2)
}

// --- Test: Transaction rollback discards data ---

func TestPagerRollbackDiscardsData(t *testing.T) {
	p := openTestPagerMem(t)

	// First, write initial data
	p.Begin(true)
	page, _ := p.GetNewPage()
	originalData := []byte("original data")
	copy(page.Data[0:], originalData)
	p.MarkDirty(page)
	p.WritePage(page)
	p.ReleasePage(page)
	p.Commit()

	// Now start a transaction and modify
	p.Begin(true)
	page2, _ := p.GetPage(1)
	modifiedData := []byte("modified data in transaction")
	copy(page2.Data[0:], modifiedData)
	p.MarkDirty(page2)
	p.WritePage(page2)
	p.ReleasePage(page2)

	// Rollback
	p.Rollback()

	// Verify original data is restored
	page3, _ := p.GetPage(1)
	got := string(page3.Data[0:len(originalData)])
	if got != string(originalData) {
		t.Errorf("after rollback: got %q, want %q", got, string(originalData))
	}
	p.ReleasePage(page3)
}

// --- Test: Page allocation and freeing ---

func TestPagerPageAllocation(t *testing.T) {
	p := openTestPagerMem(t)

	// Allocate pages
	p1, err := p.GetNewPage()
	if err != nil {
		t.Fatalf("GetNewPage: %v", err)
	}
	if p1.PageNum != 1 {
		t.Errorf("first page = %d, want 1", p1.PageNum)
	}
	p.MarkDirty(p1)
	p.WritePage(p1)
	p.ReleasePage(p1)

	p2, _ := p.GetNewPage()
	if p2.PageNum != 2 {
		t.Errorf("second page = %d, want 2", p2.PageNum)
	}
	p.MarkDirty(p2)
	p.WritePage(p2)
	p.ReleasePage(p2)

	p3, _ := p.GetNewPage()
	if p3.PageNum != 3 {
		t.Errorf("third page = %d, want 3", p3.PageNum)
	}
	p.MarkDirty(p3)
	p.WritePage(p3)
	p.ReleasePage(p3)

	if p.PageCount() != 3 {
		t.Errorf("PageCount = %d, want 3", p.PageCount())
	}

	// Free a page
	p.FreePage(2)
	if p.FreelistCount() != 1 {
		t.Errorf("FreelistCount = %d, want 1", p.FreelistCount())
	}

	// Next allocation should reuse freed page
	p4, _ := p.GetNewPage()
	if p4.PageNum != 2 {
		t.Errorf("reused page = %d, want 2 (from freelist)", p4.PageNum)
	}
	p.MarkDirty(p4)
	p.WritePage(p4)
	p.ReleasePage(p4)
}

// --- Test: File size tracking ---

func TestPagerFileSize(t *testing.T) {
	p := openTestPagerMem(t)

	// Initially empty
	if p.PageCount() != 0 {
		t.Errorf("initial PageCount = %d, want 0", p.PageCount())
	}

	// Allocate a page
	page, _ := p.GetNewPage()
	copy(page.Data[0:], []byte("test"))
	p.MarkDirty(page)
	p.WritePage(page)
	p.ReleasePage(page)

	if p.PageSize() != 4096 {
		t.Errorf("PageSize = %d, want 4096", p.PageSize())
	}
	if p.PageCount() != 1 {
		t.Errorf("PageCount = %d, want 1", p.PageCount())
	}
}

// --- Test: Read after write consistency ---

func TestPagerReadAfterWrite(t *testing.T) {
	p := openTestPagerMem(t)

	testData := make([]byte, 4096)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	// Write
	p.Begin(true)
	page, _ := p.GetNewPage()
	copy(page.Data[0:], testData)
	p.MarkDirty(page)
	p.WritePage(page)
	p.ReleasePage(page)
	p.Commit()

	// Read back
	page2, _ := p.GetPage(1)
	for i := range testData {
		if page2.Data[i] != testData[i] {
			t.Fatalf("byte %d mismatch: got %d, want %d", i, page2.Data[i], testData[i])
		}
	}
	p.ReleasePage(page2)
}

// --- Test: Multiple page operations ---

func TestPagerMultiplePages(t *testing.T) {
	p := openTestPagerMem(t)

	const numPages = 50
	p.Begin(true)

	// Write unique data to each page
	for i := 0; i < numPages; i++ {
		page, err := p.GetNewPage()
		if err != nil {
			t.Fatalf("GetNewPage %d: %v", i, err)
		}
		header := fmt.Sprintf("PAGE_%03d_HEADER_DATA", page.PageNum)
		copy(page.Data[0:], []byte(header))
		// Fill rest with pattern based on page number
		for j := len(header); j < p.PageSize(); j++ {
			page.Data[j] = byte(int(page.PageNum) ^ j)
		}
		p.MarkDirty(page)
		p.WritePage(page)
		p.ReleasePage(page)
	}
	p.Commit()

	// Verify all pages
	for i := pager.PageNumber(1); i <= pager.PageNumber(numPages); i++ {
		page, err := p.GetPage(i)
		if err != nil {
			t.Fatalf("GetPage(%d): %v", i, err)
		}
		expected := fmt.Sprintf("PAGE_%03d_HEADER_DATA", i)
		got := string(page.Data[0:len(expected)])
		if got != expected {
			t.Errorf("page %d header: got %q, want %q", i, got, expected)
		}
		// Verify pattern
		for j := len(expected); j < p.PageSize(); j++ {
			if page.Data[j] != byte(int(i)^j) {
				t.Errorf("page %d byte %d: got %d, want %d", i, j, page.Data[j], byte(int(i)^j))
				break
			}
		}
		p.ReleasePage(page)
	}
}

// --- Test: Sync operation ---

func TestPagerSync(t *testing.T) {
	p := openTestPagerFile(t)

	p.Begin(true)
	page, _ := p.GetNewPage()
	copy(page.Data[0:], []byte("sync test"))
	p.MarkDirty(page)
	p.WritePage(page)
	p.ReleasePage(page)

	if err := p.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	p.Commit()
}

// --- Test: Truncate operation ---

func TestPagerTruncate(t *testing.T) {
	p := openTestPagerMem(t)

	// Create 10 pages
	p.Begin(true)
	for i := 0; i < 10; i++ {
		page, _ := p.GetNewPage()
		copy(page.Data[0:], []byte(fmt.Sprintf("page_%d", i+1)))
		p.MarkDirty(page)
		p.WritePage(page)
		p.ReleasePage(page)
	}
	p.Commit()

	if p.PageCount() != 10 {
		t.Errorf("before truncate: PageCount = %d, want 10", p.PageCount())
	}

	// Truncate to 5 pages
	if err := p.Truncate(5); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	if p.PageCount() != 5 {
		t.Errorf("after truncate: PageCount = %d, want 5", p.PageCount())
	}
}

// --- Test: Memory database operations ---

func TestPagerMemoryDBOps(t *testing.T) {
	p := openTestPagerMem(t)

	// In-memory pager should support all basic operations
	p.Begin(true)
	page, _ := p.GetNewPage()
	copy(page.Data[0:], []byte("memory test"))
	p.MarkDirty(page)
	p.WritePage(page)
	p.ReleasePage(page)
	p.Commit()

	if p.PageCount() != 1 {
		t.Errorf("memory PageCount = %d, want 1", p.PageCount())
	}

	page2, err := p.GetPage(1)
	if err != nil {
		t.Fatalf("GetPage(1): %v", err)
	}
	got := string(page2.Data[0:11])
	if got != "memory test" {
		t.Errorf("memory data: got %q, want 'memory test'", got)
	}
	p.ReleasePage(page2)
}

// --- Test: Page cache basics ---

func TestPagerPCache(t *testing.T) {
	pc := pager.NewPCache(4096, 10)

	// Add pages
	for i := 1; i <= 5; i++ {
		p := &pager.Page{
			PageNum: pager.PageNumber(i),
			Data:    make([]byte, 4096),
		}
		copy(p.Data[0:], []byte(fmt.Sprintf("page_%d", i)))
		pc.Add(p)
	}

	if pc.Size() != 5 {
		t.Errorf("cache size = %d, want 5", pc.Size())
	}

	// Fetch existing page
	p := pc.Fetch(3)
	if p == nil {
		t.Fatal("Fetch(3) returned nil")
	}
	if p.PageNum != 3 {
		t.Errorf("Fetch(3) returned page %d", p.PageNum)
	}

	// Fetch non-existing page
	p = pc.Fetch(99)
	if p != nil {
		t.Error("Fetch(99) should return nil")
	}

	// Release
	pc.Release(pc.Fetch(3))

	// Mark dirty
	pc.MarkDirty(pc.Fetch(3))
	dirty := pc.DirtyPages()
	if len(dirty) != 1 {
		t.Errorf("dirty pages = %d, want 1", len(dirty))
	}

	// Discard
	pc.DiscardAll()
	if pc.Size() != 0 {
		t.Errorf("after discard: size = %d, want 0", pc.Size())
	}
}

// --- Test: Nested transactions (savepoint-like) via multiple begin/commit ---

func TestPagerTransactionState(t *testing.T) {
	p := openTestPagerMem(t)

	// Initially not in transaction
	if p.IsInTransaction() {
		t.Error("should not be in transaction initially")
	}

	// Begin transaction
	p.Begin(true)
	if !p.IsInTransaction() {
		t.Error("should be in transaction after Begin")
	}

	// Commit
	p.Commit()
	if p.IsInTransaction() {
		t.Error("should not be in transaction after Commit")
	}

	// Begin and rollback
	p.Begin(true)
	if !p.IsInTransaction() {
		t.Error("should be in transaction after Begin")
	}
	p.Rollback()
	if p.IsInTransaction() {
		t.Error("should not be in transaction after Rollback")
	}
}

// --- Test: Large number of pages ---

func TestPagerManyPages(t *testing.T) {
	p := openTestPagerMem(t)

	const numPages = 500
	p.Begin(true)

	for i := 0; i < numPages; i++ {
		page, err := p.GetNewPage()
		if err != nil {
			t.Fatalf("GetNewPage %d: %v", i, err)
		}
		// Write page number as identifier
		copy(page.Data[0:], []byte(fmt.Sprintf("P%04d", page.PageNum)))
		p.MarkDirty(page)
		p.WritePage(page)
		p.ReleasePage(page)
	}
	p.Commit()

	if p.PageCount() != numPages {
		t.Errorf("PageCount = %d, want %d", p.PageCount(), numPages)
	}

	// Verify random pages
	for _, idx := range []int{1, 50, 100, 250, 499, 500} {
		page, err := p.GetPage(pager.PageNumber(idx))
		if err != nil {
			t.Errorf("GetPage(%d): %v", idx, err)
			continue
		}
		expected := fmt.Sprintf("P%04d", idx)
		got := string(page.Data[0:len(expected)])
		if got != expected {
			t.Errorf("page %d: got %q, want %q", idx, got, expected)
		}
		p.ReleasePage(page)
	}
}

// --- Test: WAL basic operations ---

func TestPagerWALBasic(t *testing.T) {
	// Test WAL creation (doesn't panic)
	wal := pager.NewWAL("", nil, 4096)
	if wal == nil {
		t.Error("expected non-nil WAL")
	}

	// Closing unopened WAL should be safe
	wal.Close()
}
