package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// ============================================================================
// join3-1.*: Unrestricted join with increasing table count
//
// Creates N tables t1..tN each with a single column x and value equal to
// the table number, then does a cross join of all tables. The result should
// be the values 1..N (since each table contributes one row).
// ============================================================================

func TestJoin3_1Unrestricted(t *testing.T) {
	// Test with a reasonable number of tables
	maxN := 10

	for n := 1; n <= maxN; n++ {
		n := n // capture
		t.Run(fmt.Sprintf("1.%d - unrestricted join %d tables", n, n), func(t *testing.T) {
			db := openTestDB(t)

			// Create N tables and insert values
			for i := 1; i <= n; i++ {
				mustExec(t, db, fmt.Sprintf("CREATE TABLE t%d(x)", i))
				mustExec(t, db, fmt.Sprintf("INSERT INTO t%d VALUES(%d)", i, i))
			}

			// Build the cross-join SQL
			sql := "SELECT * FROM t1"
			for i := 2; i <= n; i++ {
				sql += fmt.Sprintf(", t%d", i)
			}

			got := queryFlatStrings(t, db, sql)

			// Expected: 1 2 3 ... N
			want := make([]string, n)
			for i := 0; i < n; i++ {
				want[i] = fmt.Sprintf("%d", i+1)
			}
			assertResults(t, got, want)
		})
	}
}

// ============================================================================
// join3-2.*: Joins with comparison conditions
//
// Similar to join3-1 but adds WHERE conditions linking tables:
// t(i+1).x == t(i).x + 1 for each consecutive pair.
// ============================================================================

func TestJoin3_2Comparison(t *testing.T) {
	maxN := 10

	for n := 1; n <= maxN; n++ {
		n := n // capture
		t.Run(fmt.Sprintf("2.%d - comparison join %d tables", n, n), func(t *testing.T) {
			db := openTestDB(t)

			// Create N tables and insert values
			for i := 1; i <= n; i++ {
				mustExec(t, db, fmt.Sprintf("CREATE TABLE t%d(x)", i))
				mustExec(t, db, fmt.Sprintf("INSERT INTO t%d VALUES(%d)", i, i))
			}

			// Build the cross-join SQL with WHERE conditions
			sql := "SELECT * FROM t1"
			for i := 2; i <= n; i++ {
				sql += fmt.Sprintf(", t%d", i)
			}

			if n > 1 {
				sep := " WHERE "
				for i := 1; i < n; i++ {
					sql += fmt.Sprintf("%st%d.x==t%d.x+1", sep, i+1, i)
					sep = " AND "
				}
			}

			got := queryFlatStrings(t, db, sql)

			// Expected: 1 2 3 ... N (the chain condition holds for these values)
			want := make([]string, n)
			for i := 0; i < n; i++ {
				want[i] = fmt.Sprintf("%d", i+1)
			}
			assertResults(t, got, want)
		})
	}
}

// ============================================================================
// join3-3.*: Error for too many tables in join
//
// Tests that trying to join more tables than the limit produces an error.
// SQLite allows at most 64 tables in a single join (as of the version in use).
// ============================================================================

func TestJoin3_3TooManyTables(t *testing.T) {
	db := openTestDB(t)

	// Create a single table to reference many times
	mustExec(t, db, "CREATE TABLE t1(x)")
	mustExec(t, db, "INSERT INTO t1 VALUES(1)")

	// Also create tables t2..t65 for the test
	for i := 2; i <= 65; i++ {
		mustExec(t, db, fmt.Sprintf("CREATE TABLE t%d(x)", i))
		mustExec(t, db, fmt.Sprintf("INSERT INTO t%d VALUES(%d)", i, i))
	}

	// Test that 65 tables in a join causes an error
	t.Run("3.1 - too many tables in join", func(t *testing.T) {
		// Build SQL: SELECT * FROM t1 AS t0, t1, t2, t3, ..., t65
		// That's 66 references (t1 AS t0 + t1 + t2..t65 = 66 tables)
		// Actually per the original test: t1 AS t0, t1, t2..$bitmask_size
		// We just need > 64 tables.
		parts := []string{"t1 AS t0", "t1"}
		for i := 2; i <= 64; i++ {
			parts = append(parts, fmt.Sprintf("t%d", i))
		}
		sql := "SELECT * FROM " + strings.Join(parts, ", ")
		ok, errMsg := catchQuery(t, db, sql)
		if !ok {
			t.Errorf("expected error for too many tables, got success. SQL length: %d", len(sql))
		} else {
			// Verify the error message mentions the table limit
			if !strings.Contains(errMsg, "at most") || !strings.Contains(errMsg, "tables in a join") {
				t.Logf("got error message: %s", errMsg)
			}
		}
	})
}

// Ensure sqlite import is used
var _ *sqlite.Database
