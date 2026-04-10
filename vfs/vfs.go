// Package vfs implements the Virtual File System abstraction for sqlite-go.
// It provides a portable interface for file I/O operations, mirroring
// SQLite's os.h/os.c/os_unix.c architecture.
package vfs

// FileLockLevel represents the locking level for a file.
type FileLockLevel int

const (
	LockNone    FileLockLevel = iota // No lock
	LockShared                       // Shared lock
	LockReserved                     // Reserved lock
	LockPending                      // Pending lock
	LockExclusive                    // Exclusive lock
)

// SyncFlag controls the behavior of File.Sync().
type SyncFlag int

const (
	SyncNormal SyncFlag = iota // Normal sync
	SyncFull                   // Full sync (barrier)
)

// FileType identifies the type of a database file.
type FileType int

const (
	FileMainDB    FileType = iota // Main database file
	FileJournal                   // Journal file
	FileWAL                       // Write-ahead log
	FileMasterJournal             // Master journal
	FileTemp                      // Temporary file
)

// FileAccessMode for FileControl operations.
type FileAccessMode int

const (
	AccessExists    FileAccessMode = iota // Does file exist?
	AccessReadWrite                       // Is file readable/writable?
	AccessRead                            // Is file readable?
)

// VFS represents a virtual file system. Each VFS provides a way to
// interact with the operating system's file system.
type VFS interface {
	// Name returns the name of this VFS.
	Name() string

	// Open opens a file. path is the filename, flags controls open behavior.
	// The fileType indicates what kind of file is being opened.
	Open(path string, flags OpenFlag, fileType FileType) (File, error)

	// Delete removes a file from the file system.
	Delete(path string, sync bool) error

	// Access tests whether a file has the given access permissions.
	Access(path string, mode FileAccessMode) (bool, error)

	// FullPathname returns the fully qualified pathname for the given file.
	FullPathname(path string) (string, error)

	// Randomness fills buf with random bytes.
	Randomness(buf []byte) error

	// Sleep suspends execution for at least the given number of microseconds.
	Sleep(microseconds int) error

	// CurrentTime returns the current time as a Julian Day Number.
	CurrentTime() (float64, error)

	// GetLastError returns the last OS error.
	GetLastError() string
}

// OpenFlag controls file open behavior.
type OpenFlag int

const (
	OpenReadOnly    OpenFlag = 1 << iota // Read-only access
	OpenReadWrite                        // Read-write access
	OpenCreate                           // Create if not exists
	OpenDeleteOnClose                    // Delete on close
	OpenExclusive                        // Exclusive access
	OpenAutoProxy                        // Auto proxy
	OpenURI                              // URI filename
	OpenMemory                           // In-memory database
	OpenMainDB                           // Main database
	OpenTempDB                           // Temp database
	OpenTransientDB                      // Transient database
	OpenMainJournal                      // Main journal
	OpenTempJournal                      // Temp journal
	OpenSubJournal                       // Subjournal
	OpenMasterJournal                    // Master journal
	OpenNoFollow                         // No symlinks
	OpenWAL                              // WAL file
)

// File represents an open file in the VFS.
type File interface {
	// Close closes the file.
	Close() error

	// Read reads data from the file at the given offset.
	Read(buf []byte, offset int64) error

	// Write writes data to the file at the given offset.
	Write(buf []byte, offset int64) error

	// Truncate truncates the file to the given size.
	Truncate(size int64) error

	// Sync flushes pending writes to disk.
	Sync(flag SyncFlag) error

	// Size returns the current size of the file.
	Size() (int64, error)

	// Lock applies a file lock.
	Lock(level FileLockLevel) error

	// Unlock releases a file lock.
	Unlock(level FileLockLevel) error

	// CheckReservedLock checks if a reserved lock is held.
	CheckReservedLock() (bool, error)

	// FileControl performs file-specific control operations.
	FileControl(op int, arg interface{}) error

	// SectorSize returns the device sector size.
	SectorSize() int

	// DeviceCharacteristics returns device characteristic flags.
	DeviceCharacteristics() DeviceCharacteristic
}

// DeviceCharacteristic flags.
type DeviceCharacteristic int

const (
	DevAtomic     DeviceCharacteristic = 1 << iota // Atomic writes
	DevAtomic512                                    // 512-byte atomic
	DevAtomic1K                                     // 1K atomic
	DevAtomic2K                                     // 2K atomic
	DevAtomic4K                                     // 4K atomic
	DevAtomic8K                                     // 8K atomic
	DevAtomic16K                                    // 16K atomic
	DevAtomic32K                                    // 32K atomic
	DevAtomic64K                                    // 64K atomic
	DevSafeAppend                                   // Safe to append
	DevSequential                                   // Sequential I/O
	DevPowerLoss                                    // Power loss protected
)

// Registry maintains registered VFS implementations.
var registry = struct {
	vfsMap map[string]VFS
}{
	vfsMap: make(map[string]VFS),
}

// Register adds a VFS to the global registry.
func Register(vfs VFS) {
	registry.vfsMap[vfs.Name()] = vfs
}

// Find returns a VFS by name. Returns nil if not found.
func Find(name string) VFS {
	return registry.vfsMap[name]
}

// Default returns the default VFS.
func Default() VFS {
	if v, ok := registry.vfsMap["unix"]; ok {
		return v
	}
	// Return first registered VFS
	for _, v := range registry.vfsMap {
		return v
	}
	return nil
}
