// Package encoding provides utility data structures and functions
// for the sqlite-go database engine, including hash tables, bit vectors,
// printf-style formatting, random number generation, UTF-8 handling,
// and row sets.
package encoding

import (
	"sync"
)

// HashEntry represents a single entry in a HashTable.
type HashEntry struct {
	key   string
	value interface{}
	h     uint32
	next  *HashEntry
	prev  *HashEntry
}

// Key returns the key of this hash entry.
func (e *HashEntry) Key() string { return e.key }

// Value returns the value of this hash entry.
func (e *HashEntry) Value() interface{} { return e.value }

// Next returns the next entry in the hash table's linked list, or nil.
func (e *HashEntry) Next() *HashEntry { return e.next }

// hashBucket holds entries that hash to the same bucket.
type hashBucket struct {
	count int
	chain *HashEntry
}

// HashTable implements a chained hash table with string keys,
// inspired by SQLite's hash table implementation.
type HashTable struct {
	mu     sync.RWMutex
	htsize uint32
	count  uint32
	first  *HashEntry
	ht     []hashBucket
}

// strHash computes a hash for a string using Knuth's multiplicative hashing.
// This mirrors the strHash() function in SQLite's hash.c.
func strHash(s string) uint32 {
	var h uint32
	for i := 0; i < len(s); i++ {
		h += uint32(0xDF & s[i])
		h *= 0x9E3779B1
	}
	return h
}

// NewHashTable creates a new hash table with the given initial bucket size.
// If size is 0, a default size is used.
func NewHashTable(size int) *HashTable {
	if size <= 0 {
		size = 8
	}
	return &HashTable{
		htsize: uint32(size),
		ht:     make([]hashBucket, size),
	}
}

// rehash resizes the hash table to new_size buckets.
func (h *HashTable) rehash(newSize uint32) {
	newHT := make([]hashBucket, newSize)
	// Reinsert all entries from the doubly-linked list
	for elem := h.first; elem != nil; {
		next := elem.next
		idx := elem.h % newSize
		elem.next = nil
		elem.prev = nil
		h.insertIntoBucket(newHT, idx, elem)
		elem = next
	}
	h.ht = newHT
	h.htsize = newSize
}

func (h *HashTable) insertIntoBucket(ht []hashBucket, idx uint32, entry *HashEntry) {
	head := ht[idx].chain
	ht[idx].count++
	ht[idx].chain = entry
	if head != nil {
		entry.next = head
		entry.prev = head.prev
		if head.prev != nil {
			head.prev.next = entry
		} else {
			h.first = entry
		}
		head.prev = entry
	} else {
		entry.next = h.first
		if h.first != nil {
			h.first.prev = entry
		}
		entry.prev = nil
		h.first = entry
	}
}

// findElement locates an element by key, returning the element and its hash.
func (h *HashTable) findElement(key string) (*HashEntry, uint32) {
	hash := strHash(key)
	var elem *HashEntry
	var count int
	if len(h.ht) > 0 {
		idx := hash % h.htsize
		elem = h.ht[idx].chain
		count = h.ht[idx].count
	} else {
		elem = h.first
		count = int(h.count)
	}
	for i := 0; i < count && elem != nil; i++ {
		if elem.h == hash && elem.key == key {
			return elem, hash
		}
		elem = elem.next
	}
	return nil, hash
}

// Insert adds a key-value pair to the hash table. If the key already exists,
// the old value is replaced. Returns an error only if memory allocation fails
// (which cannot happen in Go's slice-based implementation).
func (h *HashTable) Insert(key string, value interface{}) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	elem, hash := h.findElement(key)
	if elem != nil {
		elem.value = value
		elem.key = key
		return nil
	}

	newEntry := &HashEntry{
		key:   key,
		value: value,
		h:     hash,
	}
	h.count++

	// Rehash if table is getting full (mirrors SQLite's heuristic)
	if h.count >= 5 && h.count > 2*h.htsize {
		h.rehash(h.count * 3)
	}

	if len(h.ht) > 0 {
		idx := newEntry.h % h.htsize
		h.insertIntoBucket(h.ht, idx, newEntry)
	} else {
		// No bucket array; just prepend to the global list
		newEntry.next = h.first
		if h.first != nil {
			h.first.prev = newEntry
		}
		h.first = newEntry
	}

	return nil
}

// Find looks up a key and returns its value, or nil if not found.
func (h *HashTable) Find(key string) interface{} {
	h.mu.RLock()
	defer h.mu.RUnlock()

	elem, _ := h.findElement(key)
	if elem != nil {
		return elem.value
	}
	return nil
}

// Delete removes a key from the hash table. Returns true if the key was found.
func (h *HashTable) Delete(key string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	elem, _ := h.findElement(key)
	if elem == nil {
		return false
	}

	// Remove from doubly-linked list
	if elem.prev != nil {
		elem.prev.next = elem.next
	} else {
		h.first = elem.next
	}
	if elem.next != nil {
		elem.next.prev = elem.prev
	}

	// Remove from bucket
	if len(h.ht) > 0 {
		idx := elem.h % h.htsize
		if h.ht[idx].chain == elem {
			h.ht[idx].chain = elem.next
		}
		h.ht[idx].count--
	}

	h.count--

	// Clear the table if empty
	if h.count == 0 {
		h.first = nil
		// Reset buckets but keep the array
		for i := range h.ht {
			h.ht[i] = hashBucket{}
		}
	}

	return true
}

// Count returns the number of entries in the hash table.
func (h *HashTable) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return int(h.count)
}

// First returns the first entry in the hash table for iteration, or nil.
func (h *HashTable) First() *HashEntry {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.first
}
