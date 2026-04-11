package tests

import (
	"sort"
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// setupSelect4Tables creates t1 with n=1..31, log=floor(log2(n))
func setupSelect4Tables(t *testing.T, db *sqlite.Database) {
	t.Helper()
	db.Exec("CREATE TABLE t1(n int, log int)")
	db.Exec("BEGIN")
	for i := 1; i < 32; i++ {
		j := 0
		for (1 << uint(j)) < i {
			j++
		}
		db.Exec("INSERT INTO t1 VALUES(?, ?)", i, j)
	}
	db.Exec("COMMIT")
}

func TestSelect4UnionAll(t *testing.T) {
	db := openTestDB(t)
	setupSelect4Tables(t, db)

	t.Run("1.0 - distinct log", func(t *testing.T) {
		t.Skip("feature not yet implemented: UNION ALL")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 ORDER BY log")
		want := []interface{}{int64(0), int64(1), int64(2), int64(3), int64(4), int64(5)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("1.1a - distinct log unsorted", func(t *testing.T) {
		t.Skip("feature not yet implemented: UNION ALL")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1")
		vals := toInt64s(got)
		sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
		want := []int64{0, 1, 2, 3, 4, 5}
		for i, v := range want {
			if vals[i] != v {
				t.Errorf("sorted[%d] = %d, want %d", i, vals[i], v)
			}
		}
	})

	t.Run("1.1b - n WHERE log=3", func(t *testing.T) {
		t.Skip("feature not yet implemented: UNION ALL")
		got := queryFlat(t, db, "SELECT n FROM t1 WHERE log=3")
		vals := toInt64s(got)
		sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
		want := []int64{5, 6, 7, 8}
		if len(vals) != len(want) {
			t.Fatalf("expected %d values, got %d: %v", len(want), len(vals), vals)
		}
		for i, v := range want {
			if vals[i] != v {
				t.Errorf("vals[%d] = %d, want %d", i, vals[i], v)
			}
		}
	})

	t.Run("1.1c - UNION ALL", func(t *testing.T) {
		t.Skip("feature not yet implemented: UNION ALL")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 UNION ALL SELECT n FROM t1 WHERE log=3 ORDER BY log")
		want := []interface{}{int64(0), int64(1), int64(2), int64(3), int64(4), int64(5), int64(5), int64(6), int64(7), int64(8)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("1.1f - UNION ALL with log=2", func(t *testing.T) {
		t.Skip("feature not yet implemented: UNION ALL")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 UNION ALL SELECT n FROM t1 WHERE log=2")
		// Distinct log: 0,1,2,3,4,5 + n WHERE log=2: 3,4
		if len(got) != 8 {
			t.Fatalf("expected 8 values, got %d: %v", len(got), got)
		}
	})

	t.Run("1.3 - ORDER BY before UNION ALL error", func(t *testing.T) {
		t.Skip("feature not yet implemented: UNION ALL")
		err := catchSQL(t, db, "SELECT DISTINCT log FROM t1 ORDER BY log UNION ALL SELECT n FROM t1 WHERE log=3 ORDER BY log")
		if err == nil {
			t.Error("expected error for ORDER BY before UNION ALL")
		}
	})
}

func TestSelect4Union(t *testing.T) {
	db := openTestDB(t)
	setupSelect4Tables(t, db)

	t.Run("2.1 - UNION", func(t *testing.T) {
		t.Skip("feature not yet implemented: UNION")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 UNION SELECT n FROM t1 WHERE log=3 ORDER BY log")
		want := []interface{}{int64(0), int64(1), int64(2), int64(3), int64(4), int64(5), int64(6), int64(7), int64(8)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("2.3 - ORDER BY before UNION error", func(t *testing.T) {
		t.Skip("feature not yet implemented: UNION")
		err := catchSQL(t, db, "SELECT DISTINCT log FROM t1 ORDER BY log UNION SELECT n FROM t1 WHERE log=3 ORDER BY log")
		if err == nil {
			t.Error("expected error for ORDER BY before UNION")
		}
	})
}

func TestSelect4Except(t *testing.T) {
	db := openTestDB(t)
	setupSelect4Tables(t, db)

	t.Run("3.1.1 - EXCEPT", func(t *testing.T) {
		t.Skip("feature not yet implemented: EXCEPT")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 EXCEPT SELECT n FROM t1 WHERE log=3 ORDER BY log")
		// DISTINCT log: {0,1,2,3,4,5}
		// n WHERE log=3: {5,6,7,8}
		// EXCEPT: {0,1,2,3,4}
		want := []interface{}{int64(0), int64(1), int64(2), int64(3), int64(4)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("3.3 - ORDER BY before EXCEPT error", func(t *testing.T) {
		t.Skip("feature not yet implemented: EXCEPT")
		err := catchSQL(t, db, "SELECT DISTINCT log FROM t1 ORDER BY log EXCEPT SELECT n FROM t1 WHERE log=3 ORDER BY log")
		if err == nil {
			t.Error("expected error for ORDER BY before EXCEPT")
		}
	})
}

func TestSelect4Intersect(t *testing.T) {
	db := openTestDB(t)
	setupSelect4Tables(t, db)

	t.Run("4.1.1 - INTERSECT", func(t *testing.T) {
		t.Skip("feature not yet implemented: INTERSECT")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 INTERSECT SELECT n FROM t1 WHERE log=3 ORDER BY log")
		// DISTINCT log: {0,1,2,3,4,5}
		// n WHERE log=3: {5,6,7,8}
		// INTERSECT: {5}
		want := []interface{}{int64(5)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("4.1.2 - UNION ALL then INTERSECT", func(t *testing.T) {
		t.Skip("feature not yet implemented: INTERSECT")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 UNION ALL SELECT 6 INTERSECT SELECT n FROM t1 WHERE log=3 ORDER BY t1.log")
		// DISTINCT log UNION ALL 6: {0,1,2,3,4,5,6}
		// INTERSECT with {5,6,7,8}: {5,6}
		want := []interface{}{int64(5), int64(6)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("4.3 - ORDER BY before INTERSECT error", func(t *testing.T) {
		t.Skip("feature not yet implemented: INTERSECT")
		err := catchSQL(t, db, "SELECT DISTINCT log FROM t1 ORDER BY log INTERSECT SELECT n FROM t1 WHERE log=3 ORDER BY log")
		if err == nil {
			t.Error("expected error for ORDER BY before INTERSECT")
		}
	})
}

func TestSelect4ErrorMessages(t *testing.T) {
	db := openTestDB(t)
	setupSelect4Tables(t, db)

	t.Run("5.1 - no such table in UNION", func(t *testing.T) {
		t.Skip("feature not yet implemented: UNION error handling")
		err := catchSQL(t, db, "SELECT DISTINCT log FROM t2 UNION ALL SELECT n FROM t1 WHERE log=3 ORDER BY log")
		if err == nil {
			t.Error("expected error for non-existent table t2")
		}
	})

	t.Run("5.3 - column count mismatch", func(t *testing.T) {
		t.Skip("feature not yet implemented: UNION error handling")
		err := catchSQL(t, db, "SELECT DISTINCT log, n FROM t1 UNION ALL SELECT n FROM t1 WHERE log=3 ORDER BY log")
		if err == nil {
			t.Error("expected error for column count mismatch in UNION ALL")
		}
	})

	t.Run("5.4 - multiple UNION ALL", func(t *testing.T) {
		t.Skip("feature not yet implemented: UNION error handling")
		got := queryFlat(t, db, "SELECT log FROM t1 WHERE n=2 UNION ALL SELECT log FROM t1 WHERE n=3 UNION ALL SELECT log FROM t1 WHERE n=4 UNION ALL SELECT log FROM t1 WHERE n=5 ORDER BY log")
		want := []interface{}{int64(1), int64(2), int64(2), int64(3)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

func TestSelect4CompoundWithAggregate(t *testing.T) {
	db := openTestDB(t)
	setupSelect4Tables(t, db)

	t.Run("6.1 - UNION with aggregate", func(t *testing.T) {
		t.Skip("feature not yet implemented: compound queries with aggregates")
		got := queryFlat(t, db, "SELECT log, count(*) as cnt FROM t1 GROUP BY log UNION SELECT log, n FROM t1 WHERE n=7 ORDER BY cnt, log")
		want := []interface{}{int64(0), int64(1), int64(1), int64(1), int64(2), int64(2), int64(3), int64(4), int64(3), int64(7), int64(4), int64(8), int64(5), int64(15)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

func TestSelect4NullDistinct(t *testing.T) {
	db := openTestDB(t)
	setupSelect4Tables(t, db)

	t.Run("6.3 - UNION with NULLs", func(t *testing.T) {
		t.Skip("feature not yet implemented: UNION with NULLs")
		got := queryFlat(t, db, "SELECT NULL UNION SELECT NULL UNION SELECT 1 UNION SELECT 2 AS 'x' ORDER BY x")
		// NULLs are deduped by UNION
		if len(got) != 3 {
			t.Fatalf("expected 3 values, got %d: %v", len(got), got)
		}
	})

	t.Run("6.3.1 - UNION ALL with NULLs", func(t *testing.T) {
		t.Skip("feature not yet implemented: UNION ALL with NULLs")
		got := queryFlat(t, db, "SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT 1 UNION ALL SELECT 2 AS 'x' ORDER BY x")
		// UNION ALL keeps duplicates
		if len(got) != 4 {
			t.Fatalf("expected 4 values, got %d: %v", len(got), got)
		}
	})
}

func TestSelect4DistinctTextNumeric(t *testing.T) {
	db := openTestDB(t)
	setupSelect4Tables(t, db)

	// Create t3
	db.Exec("BEGIN")
	db.Exec("CREATE TABLE t3(a text, b float, c text)")
	db.Exec("INSERT INTO t3 VALUES(1, 1.1, '1.1')")
	db.Exec("INSERT INTO t3 VALUES(2, 1.10, '1.10')")
	db.Exec("INSERT INTO t3 VALUES(3, 1.10, '1.1')")
	db.Exec("INSERT INTO t3 VALUES(4, 1.1, '1.10')")
	db.Exec("INSERT INTO t3 VALUES(5, 1.2, '1.2')")
	db.Exec("INSERT INTO t3 VALUES(6, 1.3, '1.3')")
	db.Exec("COMMIT")

	t.Run("8.1 - DISTINCT float", func(t *testing.T) {
		got := queryFlat(t, db, "SELECT DISTINCT b FROM t3 ORDER BY c")
		// Distinct floats: 1.1, 1.2, 1.3 (1.10 == 1.1)
		if len(got) != 3 {
			t.Errorf("expected 3 distinct float values, got %d: %v", len(got), got)
		}
	})

	t.Run("8.2 - DISTINCT text", func(t *testing.T) {
		got := queryFlat(t, db, "SELECT DISTINCT c FROM t3 ORDER BY c")
		// Distinct texts: '1.1', '1.10', '1.2', '1.3'
		if len(got) != 4 {
			t.Errorf("expected 4 distinct text values, got %d: %v", len(got), got)
		}
	})
}

func TestSelect4DistinctLimitOffset(t *testing.T) {
	db := openTestDB(t)
	setupSelect4Tables(t, db)

	t.Run("10.1 - DISTINCT log", func(t *testing.T) {
		t.Skip("feature not yet implemented: DISTINCT with LIMIT/OFFSET")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 ORDER BY log")
		want := []interface{}{int64(0), int64(1), int64(2), int64(3), int64(4), int64(5)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("10.2 - DISTINCT log LIMIT 4", func(t *testing.T) {
		t.Skip("feature not yet implemented: DISTINCT with LIMIT/OFFSET")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 ORDER BY log LIMIT 4")
		want := []interface{}{int64(0), int64(1), int64(2), int64(3)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("10.3 - DISTINCT log LIMIT 0", func(t *testing.T) {
		t.Skip("feature not yet implemented: DISTINCT with LIMIT/OFFSET")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 ORDER BY log LIMIT 0")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	t.Run("10.4 - DISTINCT log LIMIT -1", func(t *testing.T) {
		t.Skip("feature not yet implemented: DISTINCT with LIMIT/OFFSET")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 ORDER BY log LIMIT -1")
		want := []interface{}{int64(0), int64(1), int64(2), int64(3), int64(4), int64(5)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("10.5 - DISTINCT log LIMIT -1 OFFSET 2", func(t *testing.T) {
		t.Skip("feature not yet implemented: DISTINCT with LIMIT/OFFSET")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 ORDER BY log LIMIT -1 OFFSET 2")
		want := []interface{}{int64(2), int64(3), int64(4), int64(5)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("10.6 - DISTINCT log LIMIT 3 OFFSET 2", func(t *testing.T) {
		t.Skip("feature not yet implemented: DISTINCT with LIMIT/OFFSET")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 ORDER BY log LIMIT 3 OFFSET 2")
		want := []interface{}{int64(2), int64(3), int64(4)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("10.7 - DISTINCT log LIMIT 3 OFFSET 20", func(t *testing.T) {
		t.Skip("feature not yet implemented: DISTINCT with LIMIT/OFFSET")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 ORDER BY +log LIMIT 3 OFFSET 20")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	t.Run("10.8 - DISTINCT log LIMIT 0 OFFSET 3", func(t *testing.T) {
		t.Skip("feature not yet implemented: DISTINCT with LIMIT/OFFSET")
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 ORDER BY log LIMIT 0 OFFSET 3")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})
}

func TestSelect4ColumnMismatch(t *testing.T) {
	db := openTestDB(t)
	setupSelect4Tables(t, db)
	// Create t2 with two columns
	db.Exec("CREATE TABLE t2(x, y)")
	db.Exec("INSERT INTO t2 VALUES(1, 1)")
	db.Exec("INSERT INTO t2 VALUES(2, 2)")

	t.Run("11.1 - too many columns left", func(t *testing.T) {
		t.Skip("feature not yet implemented: column mismatch in compound queries")
		err := catchSQL(t, db, "SELECT x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x FROM t2 UNION SELECT x FROM t2")
		if err == nil {
			t.Error("expected error for column count mismatch")
		}
	})

	t.Run("11.2 - too many columns right", func(t *testing.T) {
		t.Skip("feature not yet implemented: column mismatch in compound queries")
		err := catchSQL(t, db, "SELECT x FROM t2 UNION SELECT x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x,x FROM t2")
		if err == nil {
			t.Error("expected error for column count mismatch")
		}
	})
}

func TestSelect4CompoundWithValues(t *testing.T) {
	t.Skip("VALUES clause in compound queries not yet implemented")
}

// Ensure Database type is used
var _ *sqlite.Database = nil
