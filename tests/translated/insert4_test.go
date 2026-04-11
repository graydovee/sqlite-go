package tests

import (
	"testing"
)

func TestInsert4(t *testing.T) {
	// =========================================================================
	// insert4-1.x: CHECK constraint with INSERT...SELECT - SKIP
	// =========================================================================
	t.Run("insert4-1.1", func(t *testing.T) {
		t.Skip("CHECK constraint / INSERT...SELECT tests skipped")
	})
	t.Run("insert4-1.2", func(t *testing.T) {
		t.Skip("CHECK constraint / INSERT...SELECT tests skipped")
	})
	t.Run("insert4-1.3", func(t *testing.T) {
		t.Skip("CHECK constraint / INSERT...SELECT tests skipped")
	})

	// =========================================================================
	// insert4-2.x: Views and SELECT optimization - SKIP
	// =========================================================================
	for i := 1; i <= 10; i++ {
		name := string(rune('0' + i))
		t.Run("insert4-2."+name, func(t *testing.T) {
			t.Skip("views / SELECT optimization tests skipped")
		})
	}

	// =========================================================================
	// insert4-3.x: xfer optimization checks - SKIP
	// =========================================================================
	for i := 1; i <= 10; i++ {
		name := string(rune('0' + i))
		t.Run("insert4-3."+name, func(t *testing.T) {
			t.Skip("xfer optimization tests skipped")
		})
	}

	// =========================================================================
	// insert4-4.x: UNIQUE with VACUUM - SKIP
	// =========================================================================
	for i := 1; i <= 5; i++ {
		name := string(rune('0' + i))
		t.Run("insert4-4."+name, func(t *testing.T) {
			t.Skip("UNIQUE / VACUUM tests skipped")
		})
	}

	// =========================================================================
	// insert4-5.1: Nonexistent table in INSERT...SELECT
	// =========================================================================
	t.Run("insert4-5.1", func(t *testing.T) {
		t.Skip("INSERT...SELECT error detection not yet supported")
	})

	// =========================================================================
	// insert4-5.2: Column count mismatch between INSERT and SELECT
	// =========================================================================
	t.Run("insert4-5.2", func(t *testing.T) {
		t.Skip("INSERT...SELECT error detection not yet supported")
	})

	// =========================================================================
	// insert4-6.x through 9.x: Index collation, foreign keys, ON CONFLICT - SKIP
	// =========================================================================
	t.Run("insert4-6.1", func(t *testing.T) {
		t.Skip("index collation tests skipped")
	})
	t.Run("insert4-7.1", func(t *testing.T) {
		t.Skip("foreign key tests skipped")
	})
	t.Run("insert4-8.1", func(t *testing.T) {
		t.Skip("ON CONFLICT tests skipped")
	})
	t.Run("insert4-9.1", func(t *testing.T) {
		t.Skip("ON CONFLICT tests skipped")
	})

	// =========================================================================
	// insert4-10.x: Integrity check with xfer - SKIP
	// =========================================================================
	t.Run("insert4-10.1", func(t *testing.T) {
		t.Skip("integrity check / xfer tests skipped")
	})

	// =========================================================================
	// insert4-11.x: Partial index - SKIP
	// =========================================================================
	t.Run("insert4-11.1", func(t *testing.T) {
		t.Skip("partial index tests skipped")
	})
}
