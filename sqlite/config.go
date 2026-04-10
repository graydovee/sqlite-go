package sqlite

import "sync"

// config holds global library configuration state.
var config = struct {
	mu       sync.Mutex
	threads  ConfigOption // threading mode
	initialized bool
}{
	threads: ConfigSerialized,
}

// Configure sets a global configuration option.
// Returns any previous value or an error if the option is not supported.
func Configure(op ConfigOption, args ...interface{}) (interface{}, error) {
	config.mu.Lock()
	defer config.mu.Unlock()

	switch op {
	case ConfigSingleThread, ConfigMultiThread, ConfigSerialized:
		prev := config.threads
		config.threads = op
		return prev, nil
	case ConfigLog:
		// Log callback configuration - stub
		return nil, nil
	case ConfigURI:
		// URI filename support - stub
		return nil, nil
	case ConfigMemStatus:
		// Memory statistics - stub
		return nil, nil
	case ConfigMmapSize:
		// mmap size configuration - stub
		return nil, nil
	default:
		return nil, NewErrorf(Misuse, "unknown config option: %d", op)
	}
}

// Initialize initializes the SQLite library. Must be called before any
// database connections are opened. It is safe to call multiple times.
func Initialize() error {
	config.mu.Lock()
	defer config.mu.Unlock()
	if config.initialized {
		return nil
	}
	config.initialized = true
	return nil
}

// Shutdown shuts down the SQLite library. All database connections must
// be closed before calling this.
func Shutdown() error {
	config.mu.Lock()
	defer config.mu.Unlock()
	config.initialized = false
	return nil
}

// IsInitialized returns whether the library has been initialized.
func IsInitialized() bool {
	config.mu.Lock()
	defer config.mu.Unlock()
	return config.initialized
}
