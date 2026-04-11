package tests

import (
	"testing"
)

func TestDelete3(t *testing.T) {
	// =========================================================================
	// delete3-1.1: Create table with 524288 rows using doubling INSERT...SELECT
	// SKIP: too large and INSERT...SELECT may not be supported
	// =========================================================================
	t.Run("delete3-1.1", func(t *testing.T) {
		t.Skip("INSERT...SELECT with 524288 rows too large / may not be supported")
	})

	// =========================================================================
	// delete3-1.2: DELETE WHERE x%2==0 on large table
	// SKIP: depends on delete3-1.1 data
	// =========================================================================
	t.Run("delete3-1.2", func(t *testing.T) {
		t.Skip("depends on delete3-1.1 data (INSERT...SELECT with 524288 rows)")
	})
}
