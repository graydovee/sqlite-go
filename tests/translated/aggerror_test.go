package tests

// Translated from SQLite aggerror.test
// Tests for error handling in aggregate functions.
//
// The original C test uses a custom x_count aggregate function registered
// via sqlite3_create_function. Since Go test code cannot register custom
// aggregate functions, we skip these tests. Standard aggregate error cases
// are covered by other test files (e.g., aggorderby_test.go).

import (
	"testing"
)

func TestAggError(t *testing.T) {
	t.Skip("aggerror tests require custom aggregate function registration (x_count)")
}
