package vfs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestVFSRegistration(t *testing.T) {
	unix := Find("unix")
	if unix == nil {
		t.Fatal("unix VFS should be registered")
	}
	if unix.Name() != "unix" {
		t.Errorf("expected name 'unix', got %s", unix.Name())
	}

	mem := Find("memory")
	if mem == nil {
		t.Fatal("memory VFS should be registered")
	}
	if mem.Name() != "memory" {
		t.Errorf("expected name 'memory', got %s", mem.Name())
	}

	if Default() == nil {
		t.Fatal("default VFS should exist")
	}
}

func TestUnixVFSFileOperations(t *testing.T) {
	vfs := Find("unix")
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Create file
	f, err := vfs.Open(path, OpenReadWrite|OpenCreate, FileMainDB)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write
	data := []byte("Hello, SQLite!")
	if err := f.Write(data, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read
	buf := make([]byte, len(data))
	if err := f.Read(buf, 0); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf) != string(data) {
		t.Errorf("Read: got %q, want %q", buf, data)
	}

	// Size
	size, err := f.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != int64(len(data)) {
		t.Errorf("Size: got %d, want %d", size, len(data))
	}

	// Sync
	if err := f.Sync(SyncNormal); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	// Truncate
	if err := f.Truncate(5); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	size, _ = f.Size()
	if size != 5 {
		t.Errorf("Truncate: size = %d, want 5", size)
	}

	// SectorSize
	if ss := f.SectorSize(); ss <= 0 {
		t.Errorf("SectorSize = %d", ss)
	}

	// Close
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestUnixVFSAccess(t *testing.T) {
	vfs := Find("unix")
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// File doesn't exist
	exists, _ := vfs.Access(path, AccessExists)
	if exists {
		t.Error("file should not exist yet")
	}

	// Create file
	os.WriteFile(path, []byte("test"), 0644)

	exists, _ = vfs.Access(path, AccessExists)
	if !exists {
		t.Error("file should exist")
	}

	readable, _ := vfs.Access(path, AccessRead)
	if !readable {
		t.Error("file should be readable")
	}

	// FullPathname
	full, err := vfs.FullPathname(path)
	if err != nil {
		t.Fatalf("FullPathname: %v", err)
	}
	if !filepath.IsAbs(full) {
		t.Errorf("FullPathname: %q is not absolute", full)
	}
}

func TestUnixVFSDelete(t *testing.T) {
	vfs := Find("unix")
	dir := t.TempDir()
	path := filepath.Join(dir, "todelete.db")

	os.WriteFile(path, []byte("test"), 0644)

	if err := vfs.Delete(path, false); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("file should be deleted")
	}
}

func TestUnixVFSRandomness(t *testing.T) {
	vfs := Find("unix")
	buf := make([]byte, 32)
	if err := vfs.Randomness(buf); err != nil {
		t.Fatalf("Randomness: %v", err)
	}
	allZero := true
	for _, b := range buf {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		t.Error("random buffer is all zeros")
	}
}

func TestMemVFS(t *testing.T) {
	vfs := Find("memory")

	f, err := vfs.Open("test.db", OpenReadWrite|OpenCreate, FileMainDB)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write
	data := []byte("In-memory data")
	if err := f.Write(data, 0); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read
	buf := make([]byte, len(data))
	if err := f.Read(buf, 0); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf) != string(data) {
		t.Errorf("Read: got %q, want %q", buf, data)
	}

	// Size
	size, err := f.Size()
	if err != nil {
		t.Fatalf("Size: %v", err)
	}
	if size != int64(len(data)) {
		t.Errorf("Size: got %d, want %d", size, len(data))
	}

	// Close
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// File should still exist in VFS
	exists, _ := vfs.Access("test.db", AccessExists)
	if !exists {
		t.Error("file should still exist in memory VFS")
	}

	// Delete
	vfs.Delete("test.db", false)
	exists, _ = vfs.Access("test.db", AccessExists)
	if exists {
		t.Error("file should be deleted from memory VFS")
	}
}

func TestFileLocking(t *testing.T) {
	vfs := Find("unix")
	dir := t.TempDir()
	path := filepath.Join(dir, "lock.db")

	f, err := vfs.Open(path, OpenReadWrite|OpenCreate, FileMainDB)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	// Lock shared
	if err := f.Lock(LockShared); err != nil {
		t.Fatalf("Lock shared: %v", err)
	}

	// Lock reserved
	if err := f.Lock(LockReserved); err != nil {
		t.Fatalf("Lock reserved: %v", err)
	}

	// Unlock to shared
	if err := f.Unlock(LockShared); err != nil {
		t.Fatalf("Unlock to shared: %v", err)
	}

	// Unlock to none
	if err := f.Unlock(LockNone); err != nil {
		t.Fatalf("Unlock to none: %v", err)
	}
}
