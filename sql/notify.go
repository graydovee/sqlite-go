// Package sql implements database connection notification for sqlite-go.
package sql

import "sync"

// NotifyPhase indicates when a notification is triggered.
type NotifyPhase int

const (
	NotifyPreCommit  NotifyPhase = 0 // Before commit
	NotifyPostCommit NotifyPhase = 1 // After successful commit
	NotifyRollback   NotifyPhase = 2 // On rollback
)

// NotifyFunc is the callback type for database notifications.
type NotifyFunc func(phase NotifyPhase)

// notifyRegistry manages notification callbacks.
type notifyRegistry struct {
	mu        sync.RWMutex
	callbacks []NotifyFunc
}

// newNotifyRegistry creates a new notification registry.
func newNotifyRegistry() *notifyRegistry {
	return &notifyRegistry{}
}

// Register adds a notification callback.
func (nr *notifyRegistry) Register(fn NotifyFunc) {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	nr.callbacks = append(nr.callbacks, fn)
}

// Unregister removes a notification callback.
func (nr *notifyRegistry) Unregister(fn NotifyFunc) {
	nr.mu.Lock()
	defer nr.mu.Unlock()
	for i, cb := range nr.callbacks {
		if &cb == &fn {
			nr.callbacks = append(nr.callbacks[:i], nr.callbacks[i+1:]...)
			break
		}
	}
}

// Notify sends a notification to all registered callbacks.
func (nr *notifyRegistry) Notify(phase NotifyPhase) {
	nr.mu.RLock()
	callbacks := make([]NotifyFunc, len(nr.callbacks))
	copy(callbacks, nr.callbacks)
	nr.mu.RUnlock()

	for _, fn := range callbacks {
		fn(phase)
	}
}

// UpdateNotify registers a notification callback on the engine.
// The callback is invoked on commit and rollback events.
func (e *Engine) UpdateNotify(fn NotifyFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.notifier == nil {
		e.notifier = newNotifyRegistry()
	}
	e.notifier.Register(fn)
}

// notifyCommit sends post-commit notifications.
func (e *Engine) notifyCommit() {
	if e.notifier != nil {
		e.notifier.Notify(NotifyPostCommit)
	}
}

// notifyRollback sends rollback notifications.
func (e *Engine) notifyRollback() {
	if e.notifier != nil {
		e.notifier.Notify(NotifyRollback)
	}
}
