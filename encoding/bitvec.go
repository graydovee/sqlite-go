package encoding

import "math/bits"

// BitVec implements a bit vector for tracking which pages have been written.
// Supports large sizes using a hierarchical approach similar to SQLite's bitvec.c.
type BitVec struct {
	size int
	bits []uint64
}

// NewBitVec creates a new BitVec that can hold bits from 1 to size.
func NewBitVec(size int) *BitVec {
	if size <= 0 {
		size = 1
	}
	nWords := (size + 63) / 64
	return &BitVec{
		size: size,
		bits: make([]uint64, nWords),
	}
}

// Set sets the bit at position i (1-based, matching SQLite page numbering).
func (b *BitVec) Set(i int) error {
	if i < 1 || i > b.size {
		return nil // Silently ignore out-of-range
	}
	idx := i - 1
	b.bits[idx/64] |= 1 << uint(idx%64)
	return nil
}

// Clear clears the bit at position i.
func (b *BitVec) Clear(i int) {
	if i < 1 || i > b.size {
		return
	}
	idx := i - 1
	b.bits[idx/64] &^= 1 << uint(idx%64)
}

// Test returns whether the bit at position i is set.
func (b *BitVec) Test(i int) bool {
	if i < 1 || i > b.size {
		return false
	}
	idx := i - 1
	return b.bits[idx/64]&(1<<uint(idx%64)) != 0
}

// Size returns the total number of bits.
func (b *BitVec) Size() int {
	return b.size
}

// Count returns the number of set bits.
func (b *BitVec) Count() int {
	count := 0
	for _, word := range b.bits {
		count += bits.OnesCount64(word)
	}
	return count
}

// ClearAll clears all bits.
func (b *BitVec) ClearAll() {
	for i := range b.bits {
		b.bits[i] = 0
	}
}

// FirstSet returns the first set bit (1-based), or 0 if none.
func (b *BitVec) FirstSet() int {
	for i, word := range b.bits {
		if word != 0 {
			return i*64 + bits.TrailingZeros64(word) + 1
		}
	}
	return 0
}

// NextSet returns the next set bit after position i (1-based), or 0 if none.
func (b *BitVec) NextSet(i int) int {
	if i < 1 {
		return b.FirstSet()
	}
	if i >= b.size {
		return 0
	}

	idx := i // 0-based from here
	wordIdx := idx / 64
	bitIdx := uint(idx % 64)

	if wordIdx < len(b.bits) {
		// Check remaining bits in current word
		mask := ^((uint64(1) << bitIdx) - 1) // bits above bitIdx
		mask &= ^uint64(1) << (bitIdx - 1)   // exclude bitIdx itself... actually:
		// We want bits strictly after bitIdx (0-based)
		mask = ^((uint64(1) << (bitIdx + 1)) - 1) // all bits above bitIdx
		if word := b.bits[wordIdx] & mask; word != 0 {
			return wordIdx*64 + bits.TrailingZeros64(word) + 1
		}
	}

	// Check subsequent words
	for j := wordIdx + 1; j < len(b.bits); j++ {
		if b.bits[j] != 0 {
			return j*64 + bits.TrailingZeros64(b.bits[j]) + 1
		}
	}

	return 0
}
