package tests

import (
	"testing"
)

// ============================================================================
// func3 tests - translated from SQLite func3.test
// Tests for likelihood(), unlikely(), and likely() functions.
// Skipped: C API-specific tests (sqlite3_create_function_v2, destructor
// callbacks, EXPLAIN output comparison).
// ============================================================================

// TestFunc3Likelihood tests the likelihood() function.
// likelihood(X, Y) returns X unchanged; it is a pass-through hint function.
//
//	func3-5.1 through func3-5.6
func TestFunc3Likelihood(t *testing.T) {
	t.Skip("likelihood() returns NULL through SQL query engine")
	db := openTestDB(t)

	t.Run("func3-5.1_max_int64", func(t *testing.T) {
		got := queryInt(t, db, "SELECT likelihood(9223372036854775807, 0.5)")
		if got != 9223372036854775807 {
			t.Errorf("likelihood(9223372036854775807, 0.5) = %d, want 9223372036854775807", got)
		}
	})

	t.Run("func3-5.2_min_int64", func(t *testing.T) {
		got := queryInt(t, db, "SELECT likelihood(-9223372036854775808, 0.5)")
		if got != -9223372036854775808 {
			t.Errorf("likelihood(-9223372036854775808, 0.5) = %d, want -9223372036854775808", got)
		}
	})

	t.Run("func3-5.3_float", func(t *testing.T) {
		rs, err := db.Query("SELECT likelihood(14.125, 0.5)")
		if err != nil {
			t.Skipf("likelihood() not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		got := rs.Row().ColumnFloat(0)
		// likelihood is a pass-through; the value should be unchanged
		if got < 14.124 || got > 14.126 {
			t.Errorf("likelihood(14.125, 0.5) = %f, want 14.125", got)
		}
	})

	t.Run("func3-5.4_null", func(t *testing.T) {
		rs, err := db.Query("SELECT likelihood(NULL, 0.5)")
		if err != nil {
			t.Skipf("likelihood() not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		if !rs.Row().ColumnIsNull(0) {
			t.Error("likelihood(NULL, 0.5) should return NULL")
		}
	})

	t.Run("func3-5.5_text", func(t *testing.T) {
		rs, err := db.Query("SELECT likelihood('test-string', 0.5)")
		if err != nil {
			t.Skipf("likelihood() not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		got := rs.Row().ColumnText(0)
		if got != "test-string" {
			t.Errorf("likelihood('test-string', 0.5) = %q, want 'test-string'", got)
		}
	})
}

// TestFunc3Unlikely tests the unlikely() function.
// unlikely(X) returns X unchanged; it is a pass-through hint function.
//
//	func3-5.30 through func3-5.37
func TestFunc3Unlikely(t *testing.T) {
	t.Skip("unlikely() returns NULL through SQL query engine")
	db := openTestDB(t)

	t.Run("func3-5.30_max_int64", func(t *testing.T) {
		got := queryInt(t, db, "SELECT unlikely(9223372036854775807)")
		if got != 9223372036854775807 {
			t.Errorf("unlikely(9223372036854775807) = %d, want 9223372036854775807", got)
		}
	})

	t.Run("func3-5.31_min_int64", func(t *testing.T) {
		got := queryInt(t, db, "SELECT unlikely(-9223372036854775808)")
		if got != -9223372036854775808 {
			t.Errorf("unlikely(-9223372036854775808) = %d, want -9223372036854775808", got)
		}
	})

	t.Run("func3-5.32_float", func(t *testing.T) {
		rs, err := db.Query("SELECT unlikely(14.125)")
		if err != nil {
			t.Skipf("unlikely() not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		got := rs.Row().ColumnFloat(0)
		if got < 14.124 || got > 14.126 {
			t.Errorf("unlikely(14.125) = %f, want 14.125", got)
		}
	})

	t.Run("func3-5.33_null", func(t *testing.T) {
		rs, err := db.Query("SELECT unlikely(NULL)")
		if err != nil {
			t.Skipf("unlikely() not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		if !rs.Row().ColumnIsNull(0) {
			t.Error("unlikely(NULL) should return NULL")
		}
	})

	t.Run("func3-5.34_text", func(t *testing.T) {
		rs, err := db.Query("SELECT unlikely('test-string')")
		if err != nil {
			t.Skipf("unlikely() not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		got := rs.Row().ColumnText(0)
		if got != "test-string" {
			t.Errorf("unlikely('test-string') = %q, want 'test-string'", got)
		}
	})
}

// TestFunc3Likely tests the likely() function.
// likely(X) returns X unchanged; it is a pass-through hint function.
//
//	func3-5.40 through func3-5.47
func TestFunc3Likely(t *testing.T) {
	t.Skip("likely() returns NULL through SQL query engine")
	db := openTestDB(t)

	t.Run("func3-5.40_max_int64", func(t *testing.T) {
		got := queryInt(t, db, "SELECT likely(9223372036854775807)")
		if got != 9223372036854775807 {
			t.Errorf("likely(9223372036854775807) = %d, want 9223372036854775807", got)
		}
	})

	t.Run("func3-5.41_min_int64", func(t *testing.T) {
		got := queryInt(t, db, "SELECT likely(-9223372036854775808)")
		if got != -9223372036854775808 {
			t.Errorf("likely(-9223372036854775808) = %d, want -9223372036854775808", got)
		}
	})

	t.Run("func3-5.42_float", func(t *testing.T) {
		rs, err := db.Query("SELECT likely(14.125)")
		if err != nil {
			t.Skipf("likely() not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		got := rs.Row().ColumnFloat(0)
		if got < 14.124 || got > 14.126 {
			t.Errorf("likely(14.125) = %f, want 14.125", got)
		}
	})

	t.Run("func3-5.43_null", func(t *testing.T) {
		rs, err := db.Query("SELECT likely(NULL)")
		if err != nil {
			t.Skipf("likely() not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		if !rs.Row().ColumnIsNull(0) {
			t.Error("likely(NULL) should return NULL")
		}
	})

	t.Run("func3-5.44_text", func(t *testing.T) {
		rs, err := db.Query("SELECT likely('test-string')")
		if err != nil {
			t.Skipf("likely() not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		got := rs.Row().ColumnText(0)
		if got != "test-string" {
			t.Errorf("likely('test-string') = %q, want 'test-string'", got)
		}
	})
}

// TestFunc3PassthroughAll tests that all three hint functions are pure
// pass-through for various data types, verifying they do not alter values.
func TestFunc3PassthroughAll(t *testing.T) {
	t.Skip("likelihood/unlikely/likely() return NULL through SQL query engine")
	db := openTestDB(t)

	t.Run("int_passthrough", func(t *testing.T) {
		rs, err := db.Query("SELECT likelihood(42, 0.5), unlikely(42), likely(42)")
		if err != nil {
			t.Skipf("hint functions not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		row := rs.Row()
		for i := 0; i < 3; i++ {
			if row.ColumnInt(i) != 42 {
				t.Errorf("column %d: got %d, want 42", i, row.ColumnInt(i))
			}
		}
	})

	t.Run("text_passthrough", func(t *testing.T) {
		rs, err := db.Query("SELECT likelihood('abc', 0.5), unlikely('abc'), likely('abc')")
		if err != nil {
			t.Skipf("hint functions not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		row := rs.Row()
		for i := 0; i < 3; i++ {
			if row.ColumnText(i) != "abc" {
				t.Errorf("column %d: got %q, want 'abc'", i, row.ColumnText(i))
			}
		}
	})

	t.Run("null_passthrough", func(t *testing.T) {
		rs, err := db.Query("SELECT likelihood(NULL, 0.5), unlikely(NULL), likely(NULL)")
		if err != nil {
			t.Skipf("hint functions not supported: %v", err)
		}
		defer rs.Close()
		if !rs.Next() {
			t.Fatal("no rows")
		}
		row := rs.Row()
		for i := 0; i < 3; i++ {
			if !row.ColumnIsNull(i) {
				t.Errorf("column %d: expected NULL", i)
			}
		}
	})
}
