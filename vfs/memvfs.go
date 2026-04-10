package vfs

import (
	"fmt"
	"sync"
	"time"
)

// MemVFS implements an in-memory VFS for ":memory:" databases.
type MemVFS struct {
	mu      sync.RWMutex
	files   map[string]*MemFile
	lastErr string
}

// NewMemVFS creates a new in-memory VFS.
func NewMemVFS() *MemVFS {
	return &MemVFS{
		files: make(map[string]*MemFile),
	}
}

func (v *MemVFS) Name() string { return "memory" }

func (v *MemVFS) Open(path string, flags OpenFlag, fileType FileType) (File, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if path == "" {
		return nil, fmt.Errorf("empty path")
	}

	mf, exists := v.files[path]
	if !exists {
		if flags&OpenCreate == 0 && flags&OpenReadWrite != 0 {
			return nil, fmt.Errorf("file not found: %s", path)
		}
		mf = &MemFile{
			path:     path,
			fileType: fileType,
			data:     make([]byte, 0),
		}
		v.files[path] = mf
	}

	// Return a handle (cursor) into the shared file data
	return &MemFileHandle{
		file:    mf,
		lockLevel: LockNone,
		vfs:     v,
	}, nil
}

func (v *MemVFS) Delete(path string, sync bool) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	delete(v.files, path)
	return nil
}

func (v *MemVFS) Access(path string, mode FileAccessMode) (bool, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	_, exists := v.files[path]
	switch mode {
	case AccessExists:
		return exists, nil
	case AccessReadWrite, AccessRead:
		return exists, nil
	default:
		return false, nil
	}
}

func (v *MemVFS) FullPathname(path string) (string, error) {
	return path, nil
}

func (v *MemVFS) Randomness(buf []byte) error {
	// Use crypto/rand via the default VFS
	if dv := Default(); dv != nil && dv.Name() != "memory" {
		return dv.Randomness(buf)
	}
	// Fallback: timestamp-based
	t := time.Now().UnixNano()
	for i := range buf {
		buf[i] = byte(t >> (i * 8))
	}
	return nil
}

func (v *MemVFS) Sleep(microseconds int) error {
	time.Sleep(time.Duration(microseconds) * time.Microsecond)
	return nil
}

func (v *MemVFS) CurrentTime() (float64, error) {
	if dv := Default(); dv != nil && dv.Name() != "memory" {
		return dv.CurrentTime()
	}
	return 0, fmt.Errorf("no real-time source")
}

func (v *MemVFS) GetLastError() string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	return v.lastErr
}

// MemFile represents an in-memory file's data.
type MemFile struct {
	mu       sync.RWMutex
	path     string
	fileType FileType
	data     []byte
}

// MemFileHandle is a handle (cursor) to an in-memory file.
type MemFileHandle struct {
	mu        sync.Mutex
	file      *MemFile
	lockLevel FileLockLevel
	vfs       *MemVFS
}

func (h *MemFileHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lockLevel = LockNone
	return nil
}

func (h *MemFileHandle) Read(buf []byte, offset int64) error {
	h.file.mu.RLock()
	defer h.file.mu.RUnlock()

	if offset >= int64(len(h.file.data)) {
		return fmt.Errorf("read beyond end of file")
	}

	n := copy(buf, h.file.data[offset:])
	if n < len(buf) {
		// Zero-fill rest (short read)
		for i := n; i < len(buf); i++ {
			buf[i] = 0
		}
	}
	return nil
}

func (h *MemFileHandle) Write(buf []byte, offset int64) error {
	h.file.mu.Lock()
	defer h.file.mu.Unlock()

	end := offset + int64(len(buf))
	if end > int64(len(h.file.data)) {
		newData := make([]byte, end)
		copy(newData, h.file.data)
		h.file.data = newData
	}
	copy(h.file.data[offset:], buf)
	return nil
}

func (h *MemFileHandle) Truncate(size int64) error {
	h.file.mu.Lock()
	defer h.file.mu.Unlock()

	if size < int64(len(h.file.data)) {
		h.file.data = h.file.data[:size]
	} else if size > int64(len(h.file.data)) {
		newData := make([]byte, size)
		copy(newData, h.file.data)
		h.file.data = newData
	}
	return nil
}

func (h *MemFileHandle) Sync(flag SyncFlag) error {
	return nil // In-memory, no-op
}

func (h *MemFileHandle) Size() (int64, error) {
	h.file.mu.RLock()
	defer h.file.mu.RUnlock()
	return int64(len(h.file.data)), nil
}

func (h *MemFileHandle) Lock(level FileLockLevel) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	// In-memory files don't need real locking
	h.lockLevel = level
	return nil
}

func (h *MemFileHandle) Unlock(level FileLockLevel) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lockLevel = level
	return nil
}

func (h *MemFileHandle) CheckReservedLock() (bool, error) {
	return h.lockLevel >= LockReserved, nil
}

func (h *MemFileHandle) FileControl(op int, arg interface{}) error {
	return nil
}

func (h *MemFileHandle) SectorSize() int {
	return 4096
}

func (h *MemFileHandle) DeviceCharacteristics() DeviceCharacteristic {
	return 0
}

func init() {
	Register(NewMemVFS())
}
