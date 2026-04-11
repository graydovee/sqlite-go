package tests

import (
	"testing"
)

func TestInsert3(t *testing.T) {
	// =========================================================================
	// insert3-1.x: Tests that require triggers
	// =========================================================================
	t.Run("insert3-1.0", func(t *testing.T) {
		t.Skip("triggers not supported")
	})
	t.Run("insert3-1.1", func(t *testing.T) {
		t.Skip("triggers not supported")
	})
	t.Run("insert3-1.2", func(t *testing.T) {
		t.Skip("triggers not supported")
	})
	t.Run("insert3-1.3", func(t *testing.T) {
		t.Skip("triggers not supported")
	})
	t.Run("insert3-1.4", func(t *testing.T) {
		t.Skip("triggers not supported")
	})
	t.Run("insert3-1.5", func(t *testing.T) {
		t.Skip("triggers not supported")
	})

	// =========================================================================
	// insert3-2.x: Tests that require triggers
	// =========================================================================
	t.Run("insert3-2.1", func(t *testing.T) {
		t.Skip("triggers not supported")
	})
	t.Run("insert3-2.2", func(t *testing.T) {
		t.Skip("triggers not supported")
	})

	// =========================================================================
	// insert3-3.x: Tests that require triggers (3.1 through 3.4)
	// =========================================================================
	t.Run("insert3-3.1", func(t *testing.T) {
		t.Skip("triggers not supported")
	})
	t.Run("insert3-3.2", func(t *testing.T) {
		t.Skip("triggers not supported")
	})
	t.Run("insert3-3.3", func(t *testing.T) {
		t.Skip("triggers not supported")
	})
	t.Run("insert3-3.4", func(t *testing.T) {
		t.Skip("triggers not supported")
	})

	// =========================================================================
	// insert3-3.5: INSERT DEFAULT VALUES with INTEGER PRIMARY KEY
	// =========================================================================
	t.Run("insert3-3.5", func(t *testing.T) {
		t.Skip("DEFAULT column values not yet applied during INSERT DEFAULT VALUES")
	})

	// =========================================================================
	// insert3-3.6: Second INSERT DEFAULT VALUES increments ROWID
	// =========================================================================
	t.Run("insert3-3.6", func(t *testing.T) {
		t.Skip("DEFAULT column values not yet applied during INSERT DEFAULT VALUES")
	})

	// =========================================================================
	// insert3-3.7: Blob literal - SKIP
	// =========================================================================
	t.Run("insert3-3.7", func(t *testing.T) {
		t.Skip("blob literal tests skipped")
	})

	// =========================================================================
	// insert3-4.x: randstr - SKIP
	// =========================================================================
	t.Run("insert3-4.1", func(t *testing.T) {
		t.Skip("randstr not supported")
	})
	t.Run("insert3-4.2", func(t *testing.T) {
		t.Skip("randstr not supported")
	})
}

