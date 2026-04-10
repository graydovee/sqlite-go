// Package sql implements a Go-native extension system for sqlite-go.
// Since Go cannot load dynamic shared libraries the way C does, extensions
// are Go packages that implement the Extension interface and register
// themselves via RegisterExtension.
package sql

import (
	"fmt"
	"sync"
)

// ---------------------------------------------------------------------------
// Extension interface
// ---------------------------------------------------------------------------

// Extension is the interface that Go-native SQLite extensions must implement.
// The Init method is called when the extension is loaded into an Engine.
type Extension interface {
	// Name returns the extension's unique name.
	Name() string

	// Init initialises the extension within the given Engine.
	// The extension may register custom functions, virtual tables, etc.
	Init(eng *Engine) error
}

// ExtensionInitFunc is a functional alternative to the Extension interface.
// Extensions can be registered as a simple init function.
type ExtensionInitFunc func(eng *Engine) error

// ---------------------------------------------------------------------------
// Global extension registry
// ---------------------------------------------------------------------------

var (
	globalExtMu   sync.Mutex
	globalExts    []Extension
	autoLoadExts  []ExtensionInitFunc
)

// RegisterExtension registers an Extension so that it can be loaded into
// Engine instances.  Registered extensions are *not* auto-loaded; call
// LoadExtension explicitly, or use RegisterAutoExtension for auto-loading.
func RegisterExtension(ext Extension) {
	globalExtMu.Lock()
	defer globalExtMu.Unlock()
	globalExts = append(globalExts, ext)
}

// RegisterAutoExtension registers an init function that is called
// automatically for every new Engine created by OpenEngine.
// This mirrors sqlite3_auto_extension() in the C API.
func RegisterAutoExtension(fn ExtensionInitFunc) {
	globalExtMu.Lock()
	defer globalExtMu.Unlock()
	autoLoadExts = append(autoLoadExts, fn)
}

// CancelAutoExtension removes a previously registered auto-extension.
// It returns true if the function was found and removed.
func CancelAutoExtension(fn ExtensionInitFunc) bool {
	globalExtMu.Lock()
	defer globalExtMu.Unlock()
	for i, f := range autoLoadExts {
		// Compare function pointers via fmt.Sprintf
		if fmt.Sprintf("%p", f) == fmt.Sprintf("%p", fn) {
			autoLoadExts = append(autoLoadExts[:i], autoLoadExts[i+1:]...)
			return true
		}
	}
	return false
}

// ResetAutoExtensions removes all registered auto-extensions.
func ResetAutoExtensions() {
	globalExtMu.Lock()
	defer globalExtMu.Unlock()
	autoLoadExts = nil
}

// ---------------------------------------------------------------------------
// Engine methods
// ---------------------------------------------------------------------------

// LoadExtension loads a named extension into the Engine.
// The extension must have been previously registered via RegisterExtension.
func (e *Engine) LoadExtension(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return fmt.Errorf("database is closed")
	}

	globalExtMu.Lock()
	defer globalExtMu.Unlock()

	for _, ext := range globalExts {
		if ext.Name() == name {
			return ext.Init(e)
		}
	}
	return fmt.Errorf("extension not found: %s", name)
}

// LoadExtensionFunc loads an extension via an init function directly.
func (e *Engine) LoadExtensionFunc(name string, fn func(eng *Engine) error) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return fmt.Errorf("database is closed")
	}
	return fn(e)
}

// loadAutoExtensions loads all registered auto-extensions.
// Called from OpenEngine after the Engine is fully constructed.
func (e *Engine) loadAutoExtensions() error {
	globalExtMu.Lock()
	exts := make([]ExtensionInitFunc, len(autoLoadExts))
	copy(exts, autoLoadExts)
	globalExtMu.Unlock()

	for _, fn := range exts {
		if err := fn(e); err != nil {
			return fmt.Errorf("auto-extension init: %w", err)
		}
	}
	return nil
}

// ListExtensions returns the names of all globally registered extensions.
func ListExtensions() []string {
	globalExtMu.Lock()
	defer globalExtMu.Unlock()
	names := make([]string, len(globalExts))
	for i, ext := range globalExts {
		names[i] = ext.Name()
	}
	return names
}
