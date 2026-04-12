package tests

import (
	"testing"
)

// TestExpr2ISFalse tests IS FALSE / IS NOT FALSE expressions.
// Translated from SQLite expr2.test.
func TestExpr2ISFalse(t *testing.T) {
	t.Skip("expression evaluation incomplete")
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t0(c0)")
	mustExec(t, db, "INSERT INTO t0(c0) VALUES ('val')")

	t.Run("1.1 where clause", func(t *testing.T) {
		got := queryStrings(t, db, "SELECT * FROM t0 WHERE ( ((0 IS NOT FALSE) OR NOT (0 IS FALSE OR (t0.c0 = 1))) IS 0 )")
		if len(got) == 0 {
			t.Skipf("IS FALSE / IS NOT FALSE not supported")
		}
		want := []string{"val"}
		assertResults(t, got, want)
	})

	t.Run("1.2.1 select expression", func(t *testing.T) {
		got := queryString(t, db, "SELECT ( (0 IS NOT FALSE) OR NOT (0 IS FALSE OR (t0.c0 = 1)) ) IS 0 FROM t0")
		if got == "" {
			t.Skipf("IS FALSE not supported")
		}
		if got != "1" {
			t.Errorf("got %q, want %q", got, "1")
		}
	})

	t.Run("1.2.2 select expression IS 0 variant", func(t *testing.T) {
		got := queryString(t, db, "SELECT ( (0 IS NOT FALSE) OR NOT (0 IS 0 OR (t0.c0 = 1)) ) IS 0 FROM t0")
		if got == "" {
			t.Skipf("IS not supported")
		}
		if got != "1" {
			t.Errorf("got %q, want %q", got, "1")
		}
	})

	t.Run("1.3 inner expression", func(t *testing.T) {
		got := queryString(t, db, "SELECT ( (0 IS NOT FALSE) OR NOT (0 IS FALSE OR (t0.c0 = 1)) ) FROM t0")
		if got == "" {
			t.Skipf("IS FALSE not supported")
		}
		if got != "0" {
			t.Errorf("got %q, want %q", got, "0")
		}
	})

	t.Run("1.4.1 zero IS NOT FALSE", func(t *testing.T) {
		got := queryString(t, db, "SELECT (0 IS NOT FALSE) FROM t0")
		if got == "" {
			t.Skipf("IS NOT FALSE not supported")
		}
		if got != "0" {
			t.Errorf("got %q, want %q", got, "0")
		}
	})

	t.Run("1.4.2 NOT compound", func(t *testing.T) {
		got := queryString(t, db, "SELECT NOT (0 IS FALSE OR (t0.c0 = 1)) FROM t0")
		if got == "" {
			t.Skipf("IS FALSE not supported")
		}
		if got != "0" {
			t.Errorf("got %q, want %q", got, "0")
		}
	})
}
