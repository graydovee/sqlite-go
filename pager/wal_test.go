package pager

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sqlite-go/sqlite-go/vfs"
)

// helper: create a WAL in a temp dir
func newTestWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")
	w := NewWAL(walPath, vfs.Find("unix"), 4096)
	if err := w.Open(); err != nil {
		t.Fatalf("WAL Open: %v", err)
	}
	return w, dir
}

// helper: make page data with a known pattern
func makePageData(pageSize int, fill byte) []byte {
	data := make([]byte, pageSize)
	for i := range data {
		data[i] = fill
	}
	return data
}

// ──────────────────────────────────────────────────────────────
// WAL Header Tests
// ──────────────────────────────────────────────────────────────

func TestWALOpenClose(t *testing.T) {
	w, _ := newTestWAL(t)

	if !w.IsOpen() {
		t.Error("WAL should be open after Open()")
	}
	if w.PageSize() != 4096 {
		t.Errorf("PageSize = %d, want 4096", w.PageSize())
	}
	if w.FrameCount() != 0 {
		t.Errorf("FrameCount = %d, want 0", w.FrameCount())
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if w.IsOpen() {
		t.Error("WAL should not be open after Close()")
	}
}

func TestWALHeaderFormat(t *testing.T) {
	w, dir := newTestWAL(t)
	defer w.Close()

	// Read the raw WAL file and verify the header
	walPath := filepath.Join(dir, "test.db-wal")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < walHeaderSize {
		t.Fatalf("WAL file too small: %d bytes", len(data))
	}

	// Verify magic number
	magic := binary.BigEndian.Uint32(data[0:4])
	if magic != walMagicBE {
		t.Errorf("magic = %08x, want %08x", magic, walMagicBE)
	}

	// Verify version
	version := binary.BigEndian.Uint32(data[4:8])
	if version != walFileVersion {
		t.Errorf("version = %d, want %d", version, walFileVersion)
	}

	// Verify page size
	ps := binary.BigEndian.Uint32(data[8:12])
	if ps != 4096 {
		t.Errorf("page size = %d, want 4096", ps)
	}

	// Verify checkpoint sequence starts at 0
	ckptSeq := binary.BigEndian.Uint32(data[12:16])
	if ckptSeq != 0 {
		t.Errorf("ckpt seq = %d, want 0", ckptSeq)
	}

	// Verify salt values are non-zero
	salt1 := binary.BigEndian.Uint32(data[16:20])
	salt2 := binary.BigEndian.Uint32(data[20:24])
	if salt1 == 0 && salt2 == 0 {
		t.Error("salt values should not both be zero")
	}

	// Verify header checksum
	s1 := binary.BigEndian.Uint32(data[24:28])
	s2 := binary.BigEndian.Uint32(data[28:32])
	cs1, cs2 := walChecksum(true, data[:24], 0, 0)
	if s1 != cs1 || s2 != cs2 {
		t.Errorf("header checksum mismatch: got (%08x,%08x), computed (%08x,%08x)", s1, s2, cs1, cs2)
	}
}

func TestWALReopen(t *testing.T) {
	w, dir := newTestWAL(t)

	// Write a frame
	if err := w.BeginWriteTx(); err != nil {
		t.Fatal(err)
	}
	pageData := makePageData(4096, 0xAA)
	if err := w.WriteFrame(1, pageData); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(1); err != nil {
		t.Fatal(err)
	}

	salt1, salt2 := w.Salt()
	w.Close()

	// Reopen and verify recovery
	w2 := NewWAL(filepath.Join(dir, "test.db-wal"), vfs.Find("unix"), 4096)
	if err := w2.Open(); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	if w2.FrameCount() != 1 {
		t.Errorf("FrameCount after reopen = %d, want 1", w2.FrameCount())
	}

	// Verify salt is preserved
	s1, s2 := w2.Salt()
	if s1 != salt1 || s2 != salt2 {
		t.Errorf("salt mismatch after reopen: got (%08x,%08x), want (%08x,%08x)", s1, s2, salt1, salt2)
	}

	// Verify page data is recovered
	data, ok := w2.ReadPage(1, 1)
	if !ok {
		t.Fatal("expected to find page 1 in recovered WAL")
	}
	if data[0] != 0xAA {
		t.Errorf("page data[0] = %02x, want %02x", data[0], 0xAA)
	}
}

// ──────────────────────────────────────────────────────────────
// Frame Write/Read Tests
// ──────────────────────────────────────────────────────────────

func TestWALWriteReadFrame(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	// Start write transaction
	if err := w.BeginWriteTx(); err != nil {
		t.Fatal(err)
	}

	// Write a frame for page 1
	pageData := makePageData(4096, 0xBB)
	if err := w.WriteFrame(1, pageData); err != nil {
		t.Fatal(err)
	}

	// Page should NOT be readable before commit
	data, ok := w.ReadPage(1, w.MaxFrame())
	if ok {
		t.Error("page should not be readable before commit (uncommitted frame)")
	}

	// Commit
	if err := w.Commit(1); err != nil {
		t.Fatal(err)
	}

	// Now it should be readable
	data, ok = w.ReadPage(1, 1)
	if !ok {
		t.Fatal("expected page 1 to be readable after commit")
	}
	if data[0] != 0xBB {
		t.Errorf("data[0] = %02x, want %02x", data[0], 0xBB)
	}
}

func TestWALMultipleFrames(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	// Write 5 pages in one transaction
	if err := w.BeginWriteTx(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		pageData := makePageData(4096, byte(i))
		if err := w.WriteFrame(PageNumber(i), pageData); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Commit(5); err != nil {
		t.Fatal(err)
	}

	if w.FrameCount() != 5 {
		t.Errorf("FrameCount = %d, want 5", w.FrameCount())
	}

	// Verify all pages
	for i := 1; i <= 5; i++ {
		data, ok := w.ReadPage(PageNumber(i), 5)
		if !ok {
			t.Errorf("page %d not found in WAL", i)
			continue
		}
		if data[0] != byte(i) {
			t.Errorf("page %d data[0] = %02x, want %02x", i, data[0], byte(i))
		}
	}
}

func TestWALOverwritePage(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	// Write page 1 with value 0x11
	if err := w.BeginWriteTx(); err != nil {
		t.Fatal(err)
	}
	w.WriteFrame(1, makePageData(4096, 0x11))
	w.Commit(1)

	// Write page 1 again with value 0x22
	if err := w.BeginWriteTx(); err != nil {
		t.Fatal(err)
	}
	w.WriteFrame(1, makePageData(4096, 0x22))
	w.Commit(1)

	// Should get the latest version
	if w.FrameCount() != 2 {
		t.Errorf("FrameCount = %d, want 2", w.FrameCount())
	}

	data, ok := w.ReadPage(1, 2)
	if !ok {
		t.Fatal("expected page 1")
	}
	if data[0] != 0x22 {
		t.Errorf("data[0] = %02x, want 0x22 (latest version)", data[0])
	}

	// Reading with mxFrame=1 should get the old version
	data, ok = w.ReadPage(1, 1)
	if !ok {
		t.Fatal("expected page 1 with mxFrame=1")
	}
	if data[0] != 0x11 {
		t.Errorf("data[0] = %02x, want 0x11 (old version with mxFrame=1)", data[0])
	}
}

func TestWALReadNonExistentPage(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	if err := w.BeginWriteTx(); err != nil {
		t.Fatal(err)
	}
	w.WriteFrame(1, makePageData(4096, 0xCC))
	w.Commit(1)

	// Page 2 was never written
	_, ok := w.ReadPage(2, 1)
	if ok {
		t.Error("page 2 should not be in WAL")
	}
}

// ──────────────────────────────────────────────────────────────
// Transaction Tests
// ──────────────────────────────────────────────────────────────

func TestWALRollback(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	// Write and commit page 1
	if err := w.BeginWriteTx(); err != nil {
		t.Fatal(err)
	}
	w.WriteFrame(1, makePageData(4096, 0x11))
	w.Commit(1)

	// Start new transaction, write page 2, then rollback
	if err := w.BeginWriteTx(); err != nil {
		t.Fatal(err)
	}
	w.WriteFrame(2, makePageData(4096, 0x22))
	w.Rollback()

	// Only page 1 should be readable
	if w.FrameCount() != 1 {
		t.Errorf("FrameCount = %d, want 1 after rollback", w.FrameCount())
	}

	_, ok := w.ReadPage(2, 1)
	if ok {
		t.Error("page 2 should not be readable after rollback")
	}

	data, ok := w.ReadPage(1, 1)
	if !ok {
		t.Fatal("page 1 should still be readable after rollback")
	}
	if data[0] != 0x11 {
		t.Errorf("page 1 data[0] = %02x, want 0x11", data[0])
	}
}

func TestWALDoubleWriteTx(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	if err := w.BeginWriteTx(); err != nil {
		t.Fatal(err)
	}
	// Second BeginWriteTx should fail
	if err := w.BeginWriteTx(); err == nil {
		t.Error("expected error for double BeginWriteTx")
	}
	w.Rollback()
}

func TestWALWriteWithoutTx(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	err := w.WriteFrame(1, makePageData(4096, 0xFF))
	if err == nil {
		t.Error("expected error for WriteFrame without BeginWriteTx")
	}
}

func TestWALMultipleTransactions(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	// Transaction 1: write page 1
	w.BeginWriteTx()
	w.WriteFrame(1, makePageData(4096, 0x01))
	w.Commit(1)

	// Transaction 2: write page 2
	w.BeginWriteTx()
	w.WriteFrame(2, makePageData(4096, 0x02))
	w.Commit(2)

	// Transaction 3: write page 1 again
	w.BeginWriteTx()
	w.WriteFrame(1, makePageData(4096, 0x03))
	w.Commit(2)

	// Verify state
	if w.FrameCount() != 3 {
		t.Errorf("FrameCount = %d, want 3", w.FrameCount())
	}

	// Page 1 should be latest (0x03)
	data, ok := w.ReadPage(1, 3)
	if !ok || data[0] != 0x03 {
		t.Errorf("page 1 = %02x, want 0x03", data[0])
	}

	// Page 2 should be 0x02
	data, ok = w.ReadPage(2, 3)
	if !ok || data[0] != 0x02 {
		t.Errorf("page 2 = %02x, want 0x02", data[0])
	}

	// DB page count from last commit should be 2
	if pc := w.DBPageCount(); pc != 2 {
		t.Errorf("DBPageCount = %d, want 2", pc)
	}
}

// ──────────────────────────────────────────────────────────────
// Checksum Tests
// ──────────────────────────────────────────────────────────────

func TestWALChecksumBasic(t *testing.T) {
	// Test the checksum algorithm with known data
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

	s1, s2 := walChecksum(true, data, 0, 0)
	if s1 == 0 && s2 == 0 {
		t.Error("checksum should not be all zeros for non-zero input")
	}

	// Same data, same checksum
	s1b, s2b := walChecksum(true, data, 0, 0)
	if s1 != s1b || s2 != s2b {
		t.Error("checksum should be deterministic")
	}
}

func TestWALChecksumChaining(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

	// Compute chained
	s1a, s2a := walChecksum(true, data, 0, 0)
	s1a, s2a = walChecksum(true, data, s1a, s2a)

	// Compute concatenated
	combined := make([]byte, 16)
	copy(combined, data)
	copy(combined[8:], data)
	s1b, s2b := walChecksum(true, combined, 0, 0)

	if s1a != s1b || s2a != s2b {
		t.Errorf("chained (%08x,%08x) != concatenated (%08x,%08x)", s1a, s2a, s1b, s2b)
	}
}

func TestWALChecksumEndian(t *testing.T) {
	data := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}

	s1be, s2be := walChecksum(true, data, 0, 0)
	s1le, s2le := walChecksum(false, data, 0, 0)

	if s1be == s1le && s2be == s2le {
		t.Error("big-endian and little-endian checksums should differ for this data")
	}
}

func TestWALFrameChecksumIntegrity(t *testing.T) {
	w, dir := newTestWAL(t)
	defer w.Close()

	// Write a frame
	w.BeginWriteTx()
	w.WriteFrame(1, makePageData(4096, 0xDD))
	w.Commit(1)

	// Read the raw WAL file
	walPath := filepath.Join(dir, "test.db-wal")
	raw, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}

	// Frame starts at offset 32 (after WAL header)
	frameOff := walHeaderSize
	if len(raw) < frameOff+walFrameHdrSize {
		t.Fatal("WAL file too small for frame header")
	}

	// Read frame header
	fhdr := raw[frameOff : frameOff+walFrameHdrSize]
	pgno := binary.BigEndian.Uint32(fhdr[0:4])
	dbSize := binary.BigEndian.Uint32(fhdr[4:8])
	fsalt1 := binary.BigEndian.Uint32(fhdr[8:12])
	fsalt2 := binary.BigEndian.Uint32(fhdr[12:16])
	fcksum1 := binary.BigEndian.Uint32(fhdr[16:20])
	fcksum2 := binary.BigEndian.Uint32(fhdr[20:24])

	if pgno != 1 {
		t.Errorf("frame pgno = %d, want 1", pgno)
	}
	if dbSize != 1 {
		t.Errorf("frame dbSize = %d, want 1 (commit frame)", dbSize)
	}

	// Verify salt matches WAL header
	hsalt1 := binary.BigEndian.Uint32(raw[16:20])
	hsalt2 := binary.BigEndian.Uint32(raw[20:24])
	if fsalt1 != hsalt1 || fsalt2 != hsalt2 {
		t.Errorf("frame salt (%08x,%08x) != header salt (%08x,%08x)", fsalt1, fsalt2, hsalt1, hsalt2)
	}

	// Verify frame checksum
	hdrCs1 := binary.BigEndian.Uint32(raw[24:28])
	hdrCs2 := binary.BigEndian.Uint32(raw[28:32])
	s1, s2 := walChecksum(true, fhdr[:8], hdrCs1, hdrCs2)
	pageData := raw[frameOff+walFrameHdrSize : frameOff+walFrameHdrSize+4096]
	s1, s2 = walChecksum(true, pageData, s1, s2)

	if fcksum1 != s1 || fcksum2 != s2 {
		t.Errorf("frame checksum mismatch: stored (%08x,%08x), computed (%08x,%08x)", fcksum1, fcksum2, s1, s2)
	}
}

func TestWALCorruptFrame(t *testing.T) {
	w, dir := newTestWAL(t)
	defer w.Close()

	// Write valid frames
	w.BeginWriteTx()
	w.WriteFrame(1, makePageData(4096, 0x11))
	w.Commit(1)

	// Write another frame
	w.BeginWriteTx()
	w.WriteFrame(2, makePageData(4096, 0x22))
	w.Commit(2)

	w.Close()

	// Corrupt the second frame's page data
	walPath := filepath.Join(dir, "test.db-wal")
	raw, _ := os.ReadFile(walPath)
	corruptOff := walHeaderSize + walFrameHdrSize + 4096 + walFrameHdrSize + 100
	if corruptOff < len(raw) {
		raw[corruptOff] ^= 0xFF // flip a byte
		os.WriteFile(walPath, raw, 0644)
	}

	// Reopen: should recover only the first frame
	w2 := NewWAL(walPath, vfs.Find("unix"), 4096)
	if err := w2.Open(); err != nil {
		t.Fatalf("reopen after corruption: %v", err)
	}
	defer w2.Close()

	if w2.FrameCount() != 1 {
		t.Errorf("FrameCount after corruption = %d, want 1", w2.FrameCount())
	}

	data, ok := w2.ReadPage(1, 1)
	if !ok || data[0] != 0x11 {
		t.Errorf("page 1 should be recovered: ok=%v, data[0]=%02x", ok, data[0])
	}

	_, ok = w2.ReadPage(2, 1)
	if ok {
		t.Error("page 2 should not be recovered (corrupt frame)")
	}
}

// ──────────────────────────────────────────────────────────────
// Checkpoint Tests
// ──────────────────────────────────────────────────────────────

func TestWALCheckpointPassive(t *testing.T) {
	w, dir := newTestWAL(t)
	defer w.Close()

	// Create a fake DB file
	dbPath := filepath.Join(dir, "test.db")
	dbFile, err := vfs.Find("unix").Open(dbPath, vfs.OpenReadWrite|vfs.OpenCreate, vfs.FileMainDB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbFile.Close()

	// Write pages to WAL
	w.BeginWriteTx()
	w.WriteFrame(1, makePageData(4096, 0xAA))
	w.WriteFrame(2, makePageData(4096, 0xBB))
	w.Commit(2)

	// Checkpoint
	ckpt, remaining, err := w.Checkpoint(dbFile, CheckpointPassive)
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if ckpt != 2 {
		t.Errorf("checkpointed = %d, want 2", ckpt)
	}
	if remaining != 0 {
		t.Errorf("remaining = %d, want 0", remaining)
	}

	// WAL should be reset
	if w.FrameCount() != 0 {
		t.Errorf("FrameCount after checkpoint = %d, want 0", w.FrameCount())
	}

	// Verify data is now in the DB file
	page1 := make([]byte, 4096)
	if err := dbFile.Read(page1, 0); err != nil {
		t.Fatal(err)
	}
	if page1[0] != 0xAA {
		t.Errorf("db page 1 data[0] = %02x, want 0xAA", page1[0])
	}
}

func TestWALCheckpointIncremental(t *testing.T) {
	w, dir := newTestWAL(t)
	defer w.Close()

	dbPath := filepath.Join(dir, "test.db")
	dbFile, err := vfs.Find("unix").Open(dbPath, vfs.OpenReadWrite|vfs.OpenCreate, vfs.FileMainDB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbFile.Close()

	// Transaction 1
	w.BeginWriteTx()
	w.WriteFrame(1, makePageData(4096, 0x01))
	w.Commit(1)

	// Transaction 2
	w.BeginWriteTx()
	w.WriteFrame(2, makePageData(4096, 0x02))
	w.Commit(2)

	// Checkpoint should write both pages
	ckpt, remaining, err := w.Checkpoint(dbFile, CheckpointPassive)
	if err != nil {
		t.Fatal(err)
	}
	if ckpt != 2 {
		t.Errorf("checkpointed = %d, want 2", ckpt)
	}
	if remaining != 0 {
		t.Errorf("remaining = %d, want 0", remaining)
	}
}

func TestWALCheckpointTruncate(t *testing.T) {
	w, dir := newTestWAL(t)
	defer w.Close()

	dbPath := filepath.Join(dir, "test.db")
	dbFile, err := vfs.Find("unix").Open(dbPath, vfs.OpenReadWrite|vfs.OpenCreate, vfs.FileMainDB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbFile.Close()

	w.BeginWriteTx()
	w.WriteFrame(1, makePageData(4096, 0xEE))
	w.Commit(1)

	ckpt, _, err := w.Checkpoint(dbFile, CheckpointTruncate)
	if err != nil {
		t.Fatal(err)
	}
	if ckpt != 1 {
		t.Errorf("checkpointed = %d, want 1", ckpt)
	}

	// WAL should be truncated
	walPath := filepath.Join(dir, "test.db-wal")
	fi, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() > walHeaderSize+1024 {
		// Allow some slack for header + small amount
		t.Errorf("WAL file size = %d, should be close to %d (header)", fi.Size(), walHeaderSize)
	}

	// Verify checkpoint sequence incremented
	if w.CkptSeq() != 1 {
		t.Errorf("CkptSeq = %d, want 1", w.CkptSeq())
	}
}

func TestWALCheckpointEmpty(t *testing.T) {
	w, dir := newTestWAL(t)
	defer w.Close()

	dbPath := filepath.Join(dir, "test.db")
	dbFile, err := vfs.Find("unix").Open(dbPath, vfs.OpenReadWrite|vfs.OpenCreate, vfs.FileMainDB)
	if err != nil {
		t.Fatal(err)
	}
	defer dbFile.Close()

	ckpt, remaining, err := w.Checkpoint(dbFile, CheckpointPassive)
	if err != nil {
		t.Fatal(err)
	}
	if ckpt != 0 || remaining != 0 {
		t.Errorf("empty checkpoint: (%d, %d), want (0, 0)", ckpt, remaining)
	}
}

// ──────────────────────────────────────────────────────────────
// Reader Tests
// ──────────────────────────────────────────────────────────────

func TestWALReadSnapshot(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	// Transaction 1: page 1 = 0x01
	w.BeginWriteTx()
	w.WriteFrame(1, makePageData(4096, 0x01))
	w.Commit(1)

	// Reader starts here, sees mxFrame = 1
	mx, err := w.BeginReadTx()
	if err != nil {
		t.Fatal(err)
	}
	if mx != 1 {
		t.Errorf("mxFrame = %d, want 1", mx)
	}

	// Transaction 2: page 1 = 0x02
	w.BeginWriteTx()
	w.WriteFrame(1, makePageData(4096, 0x02))
	w.Commit(1)

	// Reader should still see page 1 = 0x01
	data, ok := w.ReadPage(1, mx)
	if !ok {
		t.Fatal("expected page 1")
	}
	if data[0] != 0x01 {
		t.Errorf("reader snapshot: data[0] = %02x, want 0x01 (snapshot)", data[0])
	}

	// New reader should see page 1 = 0x02
	mx2, _ := w.BeginReadTx()
	data2, ok := w.ReadPage(1, mx2)
	if !ok {
		t.Fatal("expected page 1 for new reader")
	}
	if data2[0] != 0x02 {
		t.Errorf("new reader: data[0] = %02x, want 0x02", data2[0])
	}

	w.EndReadTx(mx)
	w.EndReadTx(mx2)
}

func TestWALMultipleReaders(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	// Write initial data
	w.BeginWriteTx()
	w.WriteFrame(1, makePageData(4096, 0x01))
	w.Commit(1)

	// Start multiple readers
	readers := make([]int, 3)
	for i := range readers {
		mx, err := w.BeginReadTx()
		if err != nil {
			t.Fatal(err)
		}
		readers[i] = mx
	}

	// Write more data
	w.BeginWriteTx()
	w.WriteFrame(2, makePageData(4096, 0x02))
	w.Commit(2)

	// All readers should still see mxFrame=1
	for i, mx := range readers {
		_, ok := w.ReadPage(2, mx)
		if ok {
			t.Errorf("reader %d should not see page 2 (mxFrame=%d)", i, mx)
		}
	}

	// End all readers
	for _, mx := range readers {
		w.EndReadTx(mx)
	}
}

// ──────────────────────────────────────────────────────────────
// Pager + WAL Integration Tests
// ──────────────────────────────────────────────────────────────

func TestPagerWALMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	cfg := PagerConfig{
		VFS:         vfs.Find("unix"),
		Path:        path,
		PageSize:    4096,
		CacheSize:   10,
		JournalMode: JournalWAL,
	}

	p, err := OpenPager(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Open(); err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	if p.GetJournalMode() != JournalWAL {
		t.Error("expected WAL journal mode")
	}

	// Write a page
	if err := p.Begin(true); err != nil {
		t.Fatal(err)
	}
	page, err := p.GetNewPage()
	if err != nil {
		t.Fatal(err)
	}
	copy(page.Data[0:], []byte("WAL test data"))
	p.MarkDirty(page)
	p.WritePage(page)
	p.ReleasePage(page)
	if err := p.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify WAL file exists
	walPath := path + "-wal"
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Error("WAL file should exist after write in WAL mode")
	}

	// Verify data via pager read
	readPage, err := p.GetPage(1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.ReleasePage(readPage)
	if string(readPage.Data[0:13]) != "WAL test data" {
		t.Errorf("data = %q, want %q", string(readPage.Data[0:13]), "WAL test data")
	}
}

func TestPagerWALCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	cfg := PagerConfig{
		VFS:         vfs.Find("unix"),
		Path:        path,
		PageSize:    4096,
		CacheSize:   10,
		JournalMode: JournalWAL,
	}

	p, _ := OpenPager(cfg)
	p.Open()
	defer p.Close()

	// Write a page via WAL
	p.Begin(true)
	page, _ := p.GetNewPage()
	copy(page.Data[0:], []byte("Checkpoint test"))
	p.MarkDirty(page)
	p.WritePage(page)
	p.ReleasePage(page)
	p.Commit()

	// Checkpoint
	ckpt, remaining, err := p.Checkpoint(CheckpointPassive)
	if err != nil {
		t.Fatal(err)
	}
	if ckpt != 1 {
		t.Errorf("checkpointed = %d, want 1", ckpt)
	}
	if remaining != 0 {
		t.Errorf("remaining = %d, want 0", remaining)
	}

	// Data should still be readable
	readPage, err := p.GetPage(1)
	if err != nil {
		t.Fatal(err)
	}
	defer p.ReleasePage(readPage)
	if string(readPage.Data[0:15]) != "Checkpoint test" {
		t.Errorf("data after checkpoint = %q", string(readPage.Data[0:15]))
	}
}

func TestPagerWALRollback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	cfg := PagerConfig{
		VFS:         vfs.Find("unix"),
		Path:        path,
		PageSize:    4096,
		CacheSize:   10,
		JournalMode: JournalWAL,
	}

	p, _ := OpenPager(cfg)
	p.Open()
	defer p.Close()

	// Write initial data
	p.Begin(true)
	page, _ := p.GetNewPage()
	copy(page.Data[0:], []byte("Original"))
	p.MarkDirty(page)
	p.WritePage(page)
	p.ReleasePage(page)
	p.Commit()

	// Read and verify
	orig, _ := p.GetPage(1)
	origData := string(orig.Data[0:8])
	p.ReleasePage(orig)
	if origData != "Original" {
		t.Fatalf("original data = %q", origData)
	}

	// Start transaction, modify, and rollback
	p.Begin(true)
	modPage, _ := p.GetPage(1)
	copy(modPage.Data[0:], []byte("Modified"))
	p.MarkDirty(modPage)
	p.WritePage(modPage)
	p.ReleasePage(modPage)
	p.Rollback()

	// Verify rollback worked
	after, _ := p.GetPage(1)
	defer p.ReleasePage(after)
	if string(after.Data[0:8]) != "Original" {
		t.Errorf("after rollback = %q, want %q", string(after.Data[0:8]), "Original")
	}
}

func TestPagerWALReopenRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	cfg := PagerConfig{
		VFS:         vfs.Find("unix"),
		Path:        path,
		PageSize:    4096,
		CacheSize:   10,
		JournalMode: JournalWAL,
	}

	// Write data
	p1, _ := OpenPager(cfg)
	p1.Open()
	p1.Begin(true)
	page, _ := p1.GetNewPage()
	copy(page.Data[0:], []byte("Recovery test"))
	p1.MarkDirty(page)
	p1.WritePage(page)
	p1.ReleasePage(page)
	p1.Commit()
	p1.Close()

	// Reopen and verify data
	p2, _ := OpenPager(cfg)
	p2.Open()
	defer p2.Close()

	readPage, err := p2.GetPage(1)
	if err != nil {
		t.Fatal(err)
	}
	defer p2.ReleasePage(readPage)
	if string(readPage.Data[0:13]) != "Recovery test" {
		t.Errorf("recovered data = %q, want %q", string(readPage.Data[0:13]), "Recovery test")
	}
}

func TestPagerSetWALMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	cfg := PagerConfig{
		VFS:       vfs.Find("unix"),
		Path:      path,
		PageSize:  4096,
		CacheSize: 10,
	}

	p, _ := OpenPager(cfg)
	p.Open()
	defer p.Close()

	// Switch to WAL mode
	mode, err := p.SetJournalMode(JournalWAL)
	if err != nil {
		t.Fatal(err)
	}
	if mode != JournalWAL {
		t.Errorf("mode = %d, want WAL", mode)
	}

	// Verify WAL file was created
	walPath := path + "-wal"
	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		t.Error("WAL file should exist after switching to WAL mode")
	}
}

// ──────────────────────────────────────────────────────────────
// WAL Index Tests
// ──────────────────────────────────────────────────────────────

func TestWALIndexBasic(t *testing.T) {
	idx := newWalIndex()

	// Empty index
	if fi := idx.lookup(1, 10); fi != -1 {
		t.Errorf("lookup on empty index = %d, want -1", fi)
	}

	// Append frames
	idx.append(1) // frame 0
	idx.append(2) // frame 1
	idx.append(1) // frame 2 (overwrite page 1)

	// Lookup should return latest frame
	if fi := idx.lookup(1, 10); fi != 2 {
		t.Errorf("lookup page 1 = %d, want 2", fi)
	}
	if fi := idx.lookup(2, 10); fi != 1 {
		t.Errorf("lookup page 2 = %d, want 1", fi)
	}
	if fi := idx.lookup(3, 10); fi != -1 {
		t.Errorf("lookup page 3 = %d, want -1", fi)
	}

	// Lookup with limited mxFrame
	if fi := idx.lookup(1, 2); fi != 0 {
		t.Errorf("lookup page 1 mxFrame=2 = %d, want 0", fi)
	}
}

func TestWALIndexTruncate(t *testing.T) {
	idx := newWalIndex()
	idx.append(1) // frame 0
	idx.append(2) // frame 1
	idx.append(3) // frame 2

	idx.truncate(2)
	if fi := idx.lookup(3, 10); fi != -1 {
		t.Errorf("page 3 should be gone after truncate, got %d", fi)
	}
	if fi := idx.lookup(1, 10); fi != 0 {
		t.Errorf("page 1 should still exist, got %d", fi)
	}
}

func TestWALIndexRebuild(t *testing.T) {
	idx := newWalIndex()
	frames := []walFrameEntry{
		{pageNum: 1, data: make([]byte, 4096)},
		{pageNum: 2, data: make([]byte, 4096)},
		{pageNum: 1, data: make([]byte, 4096)}, // overwrite
	}
	idx.rebuild(frames)

	if fi := idx.lookup(1, 10); fi != 2 {
		t.Errorf("lookup page 1 after rebuild = %d, want 2", fi)
	}
	if fi := idx.lookup(2, 10); fi != 1 {
		t.Errorf("lookup page 2 after rebuild = %d, want 1", fi)
	}
}

// ──────────────────────────────────────────────────────────────
// WAL Header Encode/Decode Tests
// ──────────────────────────────────────────────────────────────

func TestWALHeaderEncodeDecode(t *testing.T) {
	orig := walHeader{
		magic:       walMagicBE,
		version:     walFileVersion,
		pageSize:    4096,
		ckptSeq:     42,
		salt1:       0xDEADBEEF,
		salt2:       0xCAFEBABE,
		checksum1:   0x11111111,
		checksum2:   0x22222222,
		bigEndCksum: true,
	}

	encoded := orig.encode()
	if len(encoded) != walHeaderSize {
		t.Fatalf("encoded header size = %d, want %d", len(encoded), walHeaderSize)
	}

	decoded := decodeWalHeader(encoded)
	if decoded.magic != orig.magic {
		t.Errorf("magic: got %08x, want %08x", decoded.magic, orig.magic)
	}
	if decoded.version != orig.version {
		t.Errorf("version: got %d, want %d", decoded.version, orig.version)
	}
	if decoded.pageSize != orig.pageSize {
		t.Errorf("pageSize: got %d, want %d", decoded.pageSize, orig.pageSize)
	}
	if decoded.ckptSeq != orig.ckptSeq {
		t.Errorf("ckptSeq: got %d, want %d", decoded.ckptSeq, orig.ckptSeq)
	}
	if decoded.salt1 != orig.salt1 {
		t.Errorf("salt1: got %08x, want %08x", decoded.salt1, orig.salt1)
	}
	if decoded.salt2 != orig.salt2 {
		t.Errorf("salt2: got %08x, want %08x", decoded.salt2, orig.salt2)
	}
	if decoded.checksum1 != orig.checksum1 {
		t.Errorf("checksum1: got %08x, want %08x", decoded.checksum1, orig.checksum1)
	}
	if decoded.checksum2 != orig.checksum2 {
		t.Errorf("checksum2: got %08x, want %08x", decoded.checksum2, orig.checksum2)
	}
}

func TestWALFrameHeaderEncodeDecode(t *testing.T) {
	orig := walFrameHdr{
		pageNo:    5,
		dbSize:    100,
		salt1:     0xAAAAAAAA,
		salt2:     0xBBBBBBBB,
		checksum1: 0xCCCCCCCC,
		checksum2: 0xDDDDDDDD,
	}

	encoded := orig.encode()
	if len(encoded) != walFrameHdrSize {
		t.Fatalf("encoded frame header size = %d, want %d", len(encoded), walFrameHdrSize)
	}

	decoded := decodeWalFrameHdr(encoded)
	if decoded.pageNo != orig.pageNo {
		t.Errorf("pageNo: got %d, want %d", decoded.pageNo, orig.pageNo)
	}
	if decoded.dbSize != orig.dbSize {
		t.Errorf("dbSize: got %d, want %d", decoded.dbSize, orig.dbSize)
	}
	if decoded.salt1 != orig.salt1 {
		t.Errorf("salt1: got %08x, want %08x", decoded.salt1, orig.salt1)
	}
	if decoded.salt2 != orig.salt2 {
		t.Errorf("salt2: got %08x, want %08x", decoded.salt2, orig.salt2)
	}
	if decoded.checksum1 != orig.checksum1 {
		t.Errorf("checksum1: got %08x, want %08x", decoded.checksum1, orig.checksum1)
	}
	if decoded.checksum2 != orig.checksum2 {
		t.Errorf("checksum2: got %08x, want %08x", decoded.checksum2, orig.checksum2)
	}
}

// ──────────────────────────────────────────────────────────────
// Stress / Edge Case Tests
// ──────────────────────────────────────────────────────────────

func TestWALLargePageCount(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	// Write 100 pages
	w.BeginWriteTx()
	for i := 1; i <= 100; i++ {
		data := makePageData(4096, byte(i%256))
		if err := w.WriteFrame(PageNumber(i), data); err != nil {
			t.Fatalf("WriteFrame %d: %v", i, err)
		}
	}
	w.Commit(100)

	if w.FrameCount() != 100 {
		t.Errorf("FrameCount = %d, want 100", w.FrameCount())
	}

	// Verify random pages
	for _, pgno := range []int{1, 50, 100} {
		data, ok := w.ReadPage(PageNumber(pgno), 100)
		if !ok {
			t.Errorf("page %d not found", pgno)
			continue
		}
		expected := byte(pgno % 256)
		if data[0] != expected {
			t.Errorf("page %d data[0] = %02x, want %02x", pgno, data[0], expected)
		}
	}
}

func TestWALPageOverwriteMultipleTx(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	// Write page 1 three times across transactions
	for v := byte(1); v <= 3; v++ {
		w.BeginWriteTx()
		w.WriteFrame(1, makePageData(4096, v))
		w.Commit(1)
	}

	// Latest read should be 3
	data, ok := w.ReadPage(1, 3)
	if !ok || data[0] != 3 {
		t.Errorf("page 1 = %02x, want 0x03", data[0])
	}

	// Historical reads
	data, ok = w.ReadPage(1, 1)
	if !ok || data[0] != 1 {
		t.Errorf("page 1 mxFrame=1 = %02x, want 0x01", data[0])
	}

	data, ok = w.ReadPage(1, 2)
	if !ok || data[0] != 2 {
		t.Errorf("page 1 mxFrame=2 = %02x, want 0x02", data[0])
	}
}

func TestWALCommitEmpty(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	w.BeginWriteTx()
	// Commit without writing any frames
	if err := w.Commit(0); err != nil {
		t.Fatalf("empty commit: %v", err)
	}
	if w.FrameCount() != 0 {
		t.Errorf("FrameCount = %d after empty commit, want 0", w.FrameCount())
	}
}

func TestWALRollbackEmpty(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	w.BeginWriteTx()
	w.Rollback()
	// Should be fine
}

func TestWALConcurrentTransactions(t *testing.T) {
	w, _ := newTestWAL(t)
	defer w.Close()

	// Simulate a series of committed and rolled-back transactions
	for i := 0; i < 10; i++ {
		w.BeginWriteTx()
		w.WriteFrame(1, makePageData(4096, byte(i)))

		if i%3 == 0 {
			// Rollback every third transaction
			w.Rollback()
		} else {
			w.Commit(1)
		}
	}

	// WAL should have frames from committed transactions
	if w.FrameCount() == 0 {
		t.Error("expected some frames in WAL")
	}
}

func TestWALPageSizeDifferent(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "test.db-wal")

	for _, ps := range []int{512, 1024, 2048, 4096, 8192, 16384} {
		t.Run(fmt.Sprintf("PageSize_%d", ps), func(t *testing.T) {
			w := NewWAL(walPath, vfs.Find("unix"), ps)
			if err := w.Open(); err != nil {
				t.Fatalf("Open: %v", err)
			}

			w.BeginWriteTx()
			w.WriteFrame(1, makePageData(ps, 0xAB))
			w.Commit(1)

			data, ok := w.ReadPage(1, 1)
			if !ok {
				t.Fatal("page 1 not found")
			}
			if data[0] != 0xAB {
				t.Errorf("data[0] = %02x, want 0xAB", data[0])
			}
			if len(data) != ps {
				t.Errorf("page size = %d, want %d", len(data), ps)
			}

			w.Close()
		})
	}
}
