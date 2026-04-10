package vfs

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// UnixVFS implements the VFS interface for Unix-like systems.
type UnixVFS struct {
	mu       sync.Mutex
	lastErr  string
	pageSize int
}

// NewUnixVFS creates a new Unix VFS instance.
func NewUnixVFS() *UnixVFS {
	return &UnixVFS{pageSize: 4096}
}

func (v *UnixVFS) Name() string { return "unix" }

func (v *UnixVFS) Open(path string, flags OpenFlag, fileType FileType) (File, error) {
	if path == "" {
		return nil, fmt.Errorf("empty path")
	}

	var sysFlags int
	if flags&OpenReadOnly != 0 && flags&OpenReadWrite == 0 {
		sysFlags = os.O_RDONLY
	} else if flags&OpenReadWrite != 0 {
		sysFlags = os.O_RDWR
		if flags&OpenCreate != 0 {
			sysFlags |= os.O_CREATE
		}
		if flags&OpenExclusive != 0 {
			sysFlags |= os.O_EXCL
		}
	} else {
		sysFlags = os.O_RDWR | os.O_CREATE
	}

	if flags&OpenDeleteOnClose != 0 {
		// Will delete on close
	}

	f, err := os.OpenFile(path, sysFlags, 0666)
	if err != nil {
		v.mu.Lock()
		v.lastErr = err.Error()
		v.mu.Unlock()
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("unable to open database file: %s: %w", path, err)
		}
		if os.IsPermission(err) {
			return nil, fmt.Errorf("access permission denied: %s: %w", path, err)
		}
		return nil, fmt.Errorf("failed to open %s: %w", path, err)
	}

	uf := &UnixFile{
		file:     f,
		path:     path,
		fileType: fileType,
		lockLevel: LockNone,
		pageSize: v.pageSize,
		deleteOnClose: flags&OpenDeleteOnClose != 0,
	}

	return uf, nil
}

func (v *UnixVFS) Delete(path string, sync bool) error {
	if sync {
		// Try to sync directory before delete
		dir := filepath.Dir(path)
		df, err := os.Open(dir)
		if err == nil {
			df.Sync()
			df.Close()
		}
	}
	err := os.Remove(path)
	if err != nil {
		v.mu.Lock()
		v.lastErr = err.Error()
		v.mu.Unlock()
		return fmt.Errorf("failed to delete %s: %w", path, err)
	}
	return nil
}

func (v *UnixVFS) Access(path string, mode FileAccessMode) (bool, error) {
	switch mode {
	case AccessExists:
		_, err := os.Stat(path)
		return err == nil, nil
	case AccessReadWrite:
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return false, nil
		}
		f.Close()
		return true, nil
	case AccessRead:
		f, err := os.Open(path)
		if err != nil {
			return false, nil
		}
		f.Close()
		return true, nil
	default:
		return false, fmt.Errorf("unknown access mode: %d", mode)
	}
}

func (v *UnixVFS) FullPathname(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return path, nil
	}
	return abs, nil
}

func (v *UnixVFS) Randomness(buf []byte) error {
	_, err := rand.Read(buf)
	return err
}

func (v *UnixVFS) Sleep(microseconds int) error {
	time.Sleep(time.Duration(microseconds) * time.Microsecond)
	return nil
}

func (v *UnixVFS) CurrentTime() (float64, error) {
	// Julian Day Number
	t := time.Now().UTC()
	y := t.Year()
	m := int(t.Month())
	d := t.Day()
	hour := t.Hour()
	min := t.Minute()
	sec := t.Second()

	// Adjust for January/February
	if m <= 2 {
		y--
		m += 12
	}

	A := y / 100
	B := 2 - A + A/4

	jd := float64(int(365.25*float64(y+4716))) + float64(int(30.6001*float64(m+1))) + float64(d) + float64(B) - 1524.5
	// Add fractional day
	fraction := float64(hour)/24.0 + float64(min)/1440.0 + float64(sec)/86400.0
	jd += fraction

	return jd, nil
}

func (v *UnixVFS) GetLastError() string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.lastErr
}

// UnixFile implements the File interface for Unix-like systems.
type UnixFile struct {
	mu           sync.Mutex
	file         *os.File
	path         string
	fileType     FileType
	lockLevel    FileLockLevel
	pageSize     int
	deleteOnClose bool
}

func (f *UnixFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return nil
	}

	// Release all locks
	if f.lockLevel != LockNone {
		f.unlockSys(LockNone)
	}

	path := f.path
	del := f.deleteOnClose
	err := f.file.Close()
	f.file = nil

	if del && err == nil {
		os.Remove(path)
	}

	return err
}

func (f *UnixFile) Read(buf []byte, offset int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return fmt.Errorf("file not open")
	}

	_, err := f.file.ReadAt(buf, offset)
	if err != nil {
		return fmt.Errorf("read error at offset %d: %w", offset, err)
	}
	return nil
}

func (f *UnixFile) Write(buf []byte, offset int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return fmt.Errorf("file not open")
	}

	_, err := f.file.WriteAt(buf, offset)
	if err != nil {
		return fmt.Errorf("write error at offset %d: %w", offset, err)
	}
	return nil
}

func (f *UnixFile) Truncate(size int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return fmt.Errorf("file not open")
	}
	return f.file.Truncate(size)
}

func (f *UnixFile) Sync(flag SyncFlag) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return fmt.Errorf("file not open")
	}
	return f.file.Sync()
}

func (f *UnixFile) Size() (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return 0, fmt.Errorf("file not open")
	}

	stat, err := f.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

func (f *UnixFile) Lock(level FileLockLevel) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return fmt.Errorf("file not open")
	}

	if level <= f.lockLevel {
		return nil // Already have this lock or better
	}

	fd := f.file.Fd()

	switch level {
	case LockShared:
		// If we already have shared or above, nothing to do
		if f.lockLevel >= LockShared {
			return nil
		}
		flock := syscall.Flock_t{
			Type:  syscall.F_RDLCK,
			Whence: 0,
			Start:  0,
			Len:    0, // Lock entire file
		}
		if err := syscall.FcntlFlock(fd, syscall.F_SETLK, &flock); err != nil {
			return fmt.Errorf("shared lock failed: %w", err)
		}
		f.lockLevel = LockShared

	case LockReserved:
		if f.lockLevel >= LockReserved {
			return nil
		}
		// Try to get a write lock on the reserved byte
		flock := syscall.Flock_t{
			Type:   syscall.F_WRLCK,
			Whence: 0,
			Start:  0,
			Len:    1,
		}
		if err := syscall.FcntlFlock(fd, syscall.F_SETLK, &flock); err != nil {
			return fmt.Errorf("reserved lock failed: %w", err)
		}
		f.lockLevel = LockReserved

	case LockPending:
		f.lockLevel = LockPending

	case LockExclusive:
		if f.lockLevel >= LockExclusive {
			return nil
		}
		flock := syscall.Flock_t{
			Type:   syscall.F_WRLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}
		if err := syscall.FcntlFlock(fd, syscall.F_SETLK, &flock); err != nil {
			return fmt.Errorf("exclusive lock failed: %w", err)
		}
		f.lockLevel = LockExclusive
	}

	return nil
}

func (f *UnixFile) Unlock(level FileLockLevel) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unlockSys(level)
}

func (f *UnixFile) unlockSys(level FileLockLevel) error {
	if f.file == nil {
		return nil
	}

	if level >= f.lockLevel {
		return nil // Nothing to unlock
	}

	fd := f.file.Fd()

	if level == LockNone {
		flock := syscall.Flock_t{
			Type:   syscall.F_UNLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}
		syscall.FcntlFlock(fd, syscall.F_SETLK, &flock)
	} else if level == LockShared {
		flock := syscall.Flock_t{
			Type:   syscall.F_RDLCK,
			Whence: 0,
			Start:  0,
			Len:    0,
		}
		syscall.FcntlFlock(fd, syscall.F_SETLK, &flock)
	}

	f.lockLevel = level
	return nil
}

func (f *UnixFile) CheckReservedLock() (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.file == nil {
		return false, nil
	}

	fd := f.file.Fd()
	flock := syscall.Flock_t{
		Type:   syscall.F_WRLCK,
		Whence: 0,
		Start:  0,
		Len:    1,
	}
	// Try to get info about the lock
	err := syscall.FcntlFlock(fd, syscall.F_GETLK, &flock)
	if err != nil {
		return false, err
	}
	// If Type is still F_WRLCK, the lock is not held by anyone
	// (GETLK returns the first blocking lock, or replaces type with F_UNLCK)
	return flock.Type != syscall.F_UNLCK, nil
}

func (f *UnixFile) FileControl(op int, arg interface{}) error {
	return nil // No-op for basic implementation
}

func (f *UnixFile) SectorSize() int {
	return 4096
}

func (f *UnixFile) DeviceCharacteristics() DeviceCharacteristic {
	return DevSafeAppend | DevSequential
}

func init() {
	Register(NewUnixVFS())
}
