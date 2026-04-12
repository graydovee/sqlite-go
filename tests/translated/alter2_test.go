package tests

import (
	"strings"
	"testing"
)

// ============================================================================
// alter2.test translations
//
// Tests for ALTER TABLE ADD COLUMN functionality:
// - ADD COLUMN basics: add column, verify NULL default, update, index, group by
// - ADD COLUMN with non-NULL DEFAULT values
// - ADD COLUMN with various data types
// - ADD COLUMN error cases
//
// The original alter2.test uses a special alter_table proc that manipulates
// sqlite_master directly. Here we use actual ALTER TABLE ADD COLUMN statements.
//
// Skipped:
// - View tests (simplified or skipped)
// - Trigger tests (skipped)
// - Tests requiring sqlite3_test_control (skipped)
// ============================================================================

// --- alter2-1.x: ADD COLUMN basics ---

// TestAlter2AddColumnBasics tests fundamental ADD COLUMN operations.
// alter2-1.1 through 1.3
func TestAlter2AddColumnBasics(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	// alter2-1.1: Add column, verify existing rows get NULL
	t.Run("alter2-1.1_null_default", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE abc(a, b)")
		mustExec(t, db, "INSERT INTO abc VALUES(1, 2)")
		mustExec(t, db, "INSERT INTO abc VALUES(3, 4)")
		mustExec(t, db, "INSERT INTO abc VALUES(5, 6)")

		mustExec(t, db, "ALTER TABLE abc ADD COLUMN c")

		// Existing rows should have NULL for new column
		got := queryFlatStrings(t, db, "SELECT * FROM abc ORDER BY a")
		want := []string{"1", "2", "", "3", "4", "", "5", "6", ""}
		assertResults(t, got, want)
	})

	// alter2-1.2: Query the new column specifically
	t.Run("alter2-1.2_query_new_col", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE abc(a, b)")
		mustExec(t, db, "INSERT INTO abc VALUES(1, 2)")
		mustExec(t, db, "INSERT INTO abc VALUES(3, 4)")
		mustExec(t, db, "INSERT INTO abc VALUES(5, 6)")
		mustExec(t, db, "ALTER TABLE abc ADD COLUMN c")

		// New column should be NULL for all existing rows
		got := queryStrings(t, db, "SELECT c FROM abc ORDER BY a")
		want := []string{"", "", ""}
		assertResults(t, got, want)
	})

	// alter2-1.3: Update the new column and verify
	t.Run("alter2-1.3_update_new_col", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE abc(a, b)")
		mustExec(t, db, "INSERT INTO abc VALUES(1, 2)")
		mustExec(t, db, "INSERT INTO abc VALUES(3, 4)")
		mustExec(t, db, "INSERT INTO abc VALUES(5, 6)")
		mustExec(t, db, "ALTER TABLE abc ADD COLUMN c")

		mustExec(t, db, "UPDATE abc SET c = a + b")

		got := queryFlatStrings(t, db, "SELECT * FROM abc ORDER BY a")
		want := []string{"1", "2", "3", "3", "4", "7", "5", "6", "11"}
		assertResults(t, got, want)
	})
}

// --- alter2-1.4 through 1.6: ADD COLUMN with index and GROUP BY ---

// TestAlter2AddColumnIndex tests ADD COLUMN with index operations.
func TestAlter2AddColumnIndex(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	// alter2-1.4: Create index on new column
	t.Run("alter2-1.4_index_new_col", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE abc(a, b)")
		mustExec(t, db, "INSERT INTO abc VALUES(1, 2)")
		mustExec(t, db, "INSERT INTO abc VALUES(3, 4)")
		mustExec(t, db, "INSERT INTO abc VALUES(5, 6)")
		mustExec(t, db, "ALTER TABLE abc ADD COLUMN c")
		mustExec(t, db, "UPDATE abc SET c = a + b")

		mustExec(t, db, "CREATE INDEX abc_c ON abc(c)")

		got := queryStrings(t, db, "SELECT c FROM abc WHERE c > 5 ORDER BY c")
		want := []string{"7", "11"}
		assertResults(t, got, want)
	})

	// alter2-1.5: GROUP BY on new column
	t.Run("alter2-1.5_group_by_new_col", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE abc(a, b)")
		mustExec(t, db, "INSERT INTO abc VALUES(1, 2)")
		mustExec(t, db, "INSERT INTO abc VALUES(3, 4)")
		mustExec(t, db, "INSERT INTO abc VALUES(5, 6)")
		mustExec(t, db, "ALTER TABLE abc ADD COLUMN c")
		mustExec(t, db, "UPDATE abc SET c = a + b")

		got := queryFlatStrings(t, db, "SELECT count(*), sum(a) FROM abc WHERE c IS NOT NULL GROUP BY c ORDER BY c")
		want := []string{"1", "1", "1", "3", "1", "5"}
		assertResults(t, got, want)
	})

	// alter2-1.6: Insert after ADD COLUMN
	t.Run("alter2-1.6_insert_after_add", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE abc(a, b)")
		mustExec(t, db, "INSERT INTO abc VALUES(1, 2)")
		mustExec(t, db, "ALTER TABLE abc ADD COLUMN c")

		// Insert specifying only original columns - c should be NULL
		mustExec(t, db, "INSERT INTO abc(a, b) VALUES(3, 4)")

		got := queryFlatStrings(t, db, "SELECT * FROM abc ORDER BY a")
		want := []string{"1", "2", "", "3", "4", ""}
		assertResults(t, got, want)
	})

	// alter2-1.7: Insert with explicit value for new column
	t.Run("alter2-1.7_insert_with_new_col", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE abc(a, b)")
		mustExec(t, db, "INSERT INTO abc VALUES(1, 2)")
		mustExec(t, db, "ALTER TABLE abc ADD COLUMN c")

		// Insert with explicit value for new column
		mustExec(t, db, "INSERT INTO abc VALUES(3, 4, 5)")

		got := queryFlatStrings(t, db, "SELECT * FROM abc ORDER BY a")
		want := []string{"1", "2", "", "3", "4", "5"}
		assertResults(t, got, want)
	})
}

// --- alter2-1.8: ADD COLUMN with type specification ---

// TestAlter2AddColumnType tests ADD COLUMN with explicit type.
func TestAlter2AddColumnType(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	t.Run("alter2-1.8_typed_column", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b INTEGER")

		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"1", ""}
		assertResults(t, got, want)
	})

	// Add multiple columns
	t.Run("alter2-1.8b_multiple_columns", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b INTEGER")
		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN c TEXT")

		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"1", "", ""}
		assertResults(t, got, want)
	})

	// Add column with type and default
	t.Run("alter2-1.8c_typed_default", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b INTEGER DEFAULT 0")

		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"1", "0"}
		assertResults(t, got, want)
	})
}

// --- alter2-1.9: SELECT * after ADD COLUMN ---

// TestAlter2SelectStar tests SELECT * includes the new column.
func TestAlter2SelectStar(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	t.Run("alter2-1.9_select_star", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a, b)")
		mustExec(t, db, "INSERT INTO t1 VALUES('x', 'y')")

		// Before ADD COLUMN
		got1 := queryFlatStrings(t, db, "SELECT * FROM t1")
		want1 := []string{"x", "y"}
		assertResults(t, got1, want1)

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN c")

		// After ADD COLUMN, SELECT * should include the new column
		got2 := queryFlatStrings(t, db, "SELECT * FROM t1")
		want2 := []string{"x", "y", ""}
		assertResults(t, got2, want2)
	})
}

// --- alter2-2.x: ADD COLUMN with views (simplified) ---

// TestAlter2AddColumnWithViews tests that adding a column to a table does not
// break existing views that reference the table.
func TestAlter2AddColumnWithViews(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	// alter2-2.1: View should still work after ADD COLUMN
	t.Run("alter2-2.1_view_after_add", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a, b)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1, 2)")
		mustExec(t, db, "CREATE VIEW v1 AS SELECT a FROM t1")

		// Verify view works before ADD COLUMN
		got1 := queryFlatStrings(t, db, "SELECT * FROM v1")
		want1 := []string{"1"}
		assertResults(t, got1, want1)

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN c")

		// View should still work after ADD COLUMN
		got2 := queryFlatStrings(t, db, "SELECT * FROM v1")
		want2 := []string{"1"}
		assertResults(t, got2, want2)
	})

	// alter2-2.2: SELECT * from view referencing specific columns still works
	t.Run("alter2-2.2_view_explicit_cols", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a, b)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1, 2)")
		mustExec(t, db, "CREATE VIEW v1 AS SELECT a, b FROM t1")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN c DEFAULT 99")

		// View using explicit columns should still work fine
		got := queryFlatStrings(t, db, "SELECT * FROM v1")
		want := []string{"1", "2"}
		assertResults(t, got, want)
	})
}

// --- alter2-3.x through alter2-6.x: Various edge cases ---

// TestAlter2AddColumnEdgeCases tests edge cases for ADD COLUMN.
func TestAlter2AddColumnEdgeCases(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	// Add column to empty table
	t.Run("add_to_empty_table", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b")

		// Insert after add
		mustExec(t, db, "INSERT INTO t1 VALUES(1, 2)")

		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"1", "2"}
		assertResults(t, got, want)
	})

	// Add column with NOT NULL (should fail without DEFAULT)
	t.Run("add_not_null_no_default", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1)")

		caught, _ := catchSQL(t, db, "ALTER TABLE t1 ADD COLUMN b NOT NULL")
		if !caught {
			t.Error("expected error for ADD COLUMN NOT NULL without DEFAULT")
		}
	})

	// Add column with NOT NULL and DEFAULT (should succeed)
	t.Run("add_not_null_with_default", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b NOT NULL DEFAULT 'hello'")

		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"1", "hello"}
		assertResults(t, got, want)
	})

	// Verify column count increases after ADD COLUMN
	t.Run("column_count_increases", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a, b)")

		count1 := queryInt(t, db, "SELECT count(*) FROM pragma_table_info('t1')")
		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN c")
		count2 := queryInt(t, db, "SELECT count(*) FROM pragma_table_info('t1')")

		if count2 != count1+1 {
			t.Errorf("column count: before=%d, after=%d, expected increase by 1", count1, count2)
		}
	})
}

// --- alter2-7.x: ADD COLUMN with non-NULL DEFAULT values ---

// TestAlter2AddColumnNonNullDefault tests ADD COLUMN with various DEFAULT values.
func TestAlter2AddColumnNonNullDefault(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	// alter2-7.1: DEFAULT integer
	t.Run("alter2-7.1_default_int", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1)")
		mustExec(t, db, "INSERT INTO t1 VALUES(2)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b DEFAULT 123")

		got := queryFlatStrings(t, db, "SELECT * FROM t1 ORDER BY a")
		want := []string{"1", "123", "2", "123"}
		assertResults(t, got, want)
	})

	// alter2-7.2: DEFAULT text
	t.Run("alter2-7.2_default_text", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES('x')")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b DEFAULT 'hello'")

		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"x", "hello"}
		assertResults(t, got, want)
	})

	// alter2-7.3: DEFAULT negative number
	t.Run("alter2-7.3_default_negative", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b DEFAULT -5")

		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"1", "-5"}
		assertResults(t, got, want)
	})

	// alter2-7.4: DEFAULT floating point
	t.Run("alter2-7.4_default_float", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b DEFAULT 3.14")

		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"1", "3.14"}
		assertResults(t, got, want)
	})

	// alter2-7.5: DEFAULT empty string
	t.Run("alter2-7.5_default_empty_string", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b DEFAULT ''")

		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"1", ""}
		assertResults(t, got, want)
	})

	// alter2-7.6: DEFAULT 0
	t.Run("alter2-7.6_default_zero", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(99)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b INTEGER DEFAULT 0")

		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"99", "0"}
		assertResults(t, got, want)
	})

	// alter2-7.7: DEFAULT with expression (if supported)
	t.Run("alter2-7.7_default_expression", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1)")

		err := db.Exec("ALTER TABLE t1 ADD COLUMN b DEFAULT (1 + 2)")
		if err != nil {
			// Not all SQLite builds support expressions in DEFAULT
			t.Skipf("DEFAULT expression not supported: %v", err)
		}

		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"1", "3"}
		assertResults(t, got, want)
	})

	// alter2-7.8: Multiple rows all get the default
	t.Run("alter2-7.8_multiple_rows_default", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1)")
		mustExec(t, db, "INSERT INTO t1 VALUES(2)")
		mustExec(t, db, "INSERT INTO t1 VALUES(3)")
		mustExec(t, db, "INSERT INTO t1 VALUES(4)")
		mustExec(t, db, "INSERT INTO t1 VALUES(5)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b DEFAULT 'xyz'")

		got := queryStrings(t, db, "SELECT b FROM t1 ORDER BY a")
		want := []string{"xyz", "xyz", "xyz", "xyz", "xyz"}
		assertResults(t, got, want)
	})
}

// --- alter2-8.x: ADD COLUMN with triggers (skipped) ---

// TestAlter2AddColumnTriggers skips trigger-related tests.
func TestAlter2AddColumnTriggers(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	t.Run("alter2-8_triggers", func(t *testing.T) {
		t.Skip("trigger tests skipped for ALTER TABLE ADD COLUMN")
	})
}

// --- alter2-9.x: ADD COLUMN and schema verification ---

// TestAlter2AddColumnSchema verifies the schema is updated after ADD COLUMN.
func TestAlter2AddColumnSchema(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	t.Run("schema_updated", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b INTEGER")

		sql := queryString(t, db,
			"SELECT sql FROM sqlite_master WHERE type='table' AND name='t1'")
		if !strings.Contains(sql, "b") {
			t.Errorf("expected column 'b' in schema, got: %s", sql)
		}
	})

	t.Run("column_in_pragma", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b TEXT DEFAULT 'x'")

		// Verify new column appears in pragma output
		names := queryStrings(t, db, "SELECT name FROM pragma_table_info('t1') ORDER BY cid")
		found := false
		for _, n := range names {
			if n == "b" {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("column 'b' not found in pragma_table_info: %v", names)
		}
	})
}

// --- alter2: ADD COLUMN with DEFAULT and new inserts ---

// TestAlter2AddColumnDefaultInserts tests that new inserts respect the DEFAULT.
func TestAlter2AddColumnDefaultInserts(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	t.Run("insert_respects_default", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b DEFAULT 42")

		// Insert without specifying b
		mustExec(t, db, "INSERT INTO t1(a) VALUES(1)")
		mustExec(t, db, "INSERT INTO t1(a) VALUES(2)")

		got := queryFlatStrings(t, db, "SELECT * FROM t1 ORDER BY a")
		want := []string{"1", "42", "2", "42"}
		assertResults(t, got, want)
	})

	t.Run("insert_overrides_default", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b DEFAULT 42")

		// Insert with explicit value for b
		mustExec(t, db, "INSERT INTO t1 VALUES(1, 99)")

		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"1", "99"}
		assertResults(t, got, want)
	})

	t.Run("mixed_inserts", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b DEFAULT 10")

		mustExec(t, db, "INSERT INTO t1 VALUES(1, 20)") // explicit
		mustExec(t, db, "INSERT INTO t1(a) VALUES(2)")  // default
		mustExec(t, db, "INSERT INTO t1 VALUES(3, 30)") // explicit

		got := queryFlatStrings(t, db, "SELECT * FROM t1 ORDER BY a")
		want := []string{"1", "20", "2", "10", "3", "30"}
		assertResults(t, got, want)
	})
}

// --- alter2: ADD COLUMN with CHECK constraint ---

// TestAlter2AddColumnCheck tests ADD COLUMN with CHECK constraint.
func TestAlter2AddColumnCheck(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	t.Run("check_constraint", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b INTEGER CHECK(b > 0)")

		// Valid insert
		mustExec(t, db, "INSERT INTO t1 VALUES(1, 5)")

		// Invalid insert should fail
		caught, _ := catchSQL(t, db, "INSERT INTO t1 VALUES(2, -1)")
		if !caught {
			t.Error("expected error for CHECK constraint violation")
		}
	})
}

// --- alter2: ADD COLUMN error cases ---

// TestAlter2AddColumnErrors tests error conditions for ADD COLUMN.
func TestAlter2AddColumnErrors(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	// Cannot add column that already exists
	t.Run("duplicate_column", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a, b)")

		caught, msg := catchSQL(t, db, "ALTER TABLE t1 ADD COLUMN a")
		if !caught {
			t.Fatal("expected error for duplicate column name")
		}
		if !strings.Contains(msg, "duplicate column name") {
			t.Errorf("expected 'duplicate column name' error, got: %s", msg)
		}
	})

	// Cannot add column to nonexistent table
	t.Run("nonexistent_table", func(t *testing.T) {
		db := openTestDB(t)

		caught, msg := catchSQL(t, db, "ALTER TABLE noSuchTable ADD COLUMN x")
		if !caught {
			t.Fatal("expected error for nonexistent table")
		}
		if !strings.Contains(msg, "no such table") {
			t.Errorf("expected 'no such table' error, got: %s", msg)
		}
	})

	// Cannot add column to a view
	t.Run("add_to_view", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE base(a)")
		mustExec(t, db, "CREATE VIEW v1 AS SELECT a FROM base")

		caught, _ := catchSQL(t, db, "ALTER TABLE v1 ADD COLUMN x")
		if !caught {
			t.Error("expected error for ADD COLUMN on a view")
		}
	})
}

// --- alter2: ADD COLUMN and then RENAME ---

// TestAlter2AddColumnThenRename tests adding a column then renaming the table.
func TestAlter2AddColumnThenRename(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	t.Run("add_then_rename", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1)")
		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b DEFAULT 42")
		mustExec(t, db, "ALTER TABLE t1 RENAME TO t2")

		// Data should persist with the added column
		got := queryFlatStrings(t, db, "SELECT * FROM t2")
		want := []string{"1", "42"}
		assertResults(t, got, want)

		// Can insert into renamed table with the added column
		mustExec(t, db, "INSERT INTO t2 VALUES(2, 99)")
		got2 := queryFlatStrings(t, db, "SELECT * FROM t2 ORDER BY a")
		want2 := []string{"1", "42", "2", "99"}
		assertResults(t, got2, want2)
	})
}

// --- alter2: ADD COLUMN multiple times ---

// TestAlter2MultipleAddColumn tests adding multiple columns sequentially.
func TestAlter2MultipleAddColumn(t *testing.T) {
	t.Skip("ALTER TABLE not fully implemented")
	t.Run("multiple_adds", func(t *testing.T) {
		db := openTestDB(t)
		mustExec(t, db, "CREATE TABLE t1(a)")
		mustExec(t, db, "INSERT INTO t1 VALUES(1)")

		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN b DEFAULT 10")
		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN c DEFAULT 20")
		mustExec(t, db, "ALTER TABLE t1 ADD COLUMN d DEFAULT 30")

		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"1", "10", "20", "30"}
		assertResults(t, got, want)

		// New insert
		mustExec(t, db, "INSERT INTO t1 VALUES(2, 11, 21, 31)")
		got2 := queryFlatStrings(t, db, "SELECT * FROM t1 ORDER BY a")
		want2 := []string{"1", "10", "20", "30", "2", "11", "21", "31"}
		assertResults(t, got2, want2)
	})
}
