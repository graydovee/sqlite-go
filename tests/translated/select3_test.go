package tests

import (
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// setupSelect3Tables creates t1 with n=1..31, log=floor(log2(n))
func setupSelect3Tables(t *testing.T, db *sqlite.Database) {
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

func TestSelect3BasicAggregate(t *testing.T) {
	db := openTestDB(t)
	setupSelect3Tables(t, db)

	t.Run("1.0 - distinct log", func(t *testing.T) {
		got := queryFlat(t, db, "SELECT DISTINCT log FROM t1 ORDER BY log")
		want := []interface{}{int64(0), int64(1), int64(2), int64(3), int64(4), int64(5)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("1.1 - count(*)", func(t *testing.T) {
		got := queryFlat(t, db, "SELECT count(*) FROM t1")
		if len(got) != 1 || !isNumericEqual(got[0], 31) {
			t.Errorf("got %v, want 31", got)
		}
	})

	t.Run("1.2 - basic aggregate functions", func(t *testing.T) {
		got := queryFlat(t, db, "SELECT min(n),min(log),max(n),max(log),sum(n),sum(log),avg(n),avg(log) FROM t1")
		if len(got) != 8 {
			t.Fatalf("expected 8 values, got %d: %v", len(got), got)
		}
		// min(n)=1, min(log)=0, max(n)=31, max(log)=5, sum(n)=496, sum(log)=124
		// avg(n)=16.0, avg(log)=4.0
		if !isNumericEqual(got[0], 1) {
			t.Errorf("min(n): got %v, want 1", got[0])
		}
		if !isNumericEqual(got[1], 0) {
			t.Errorf("min(log): got %v, want 0", got[1])
		}
		if !isNumericEqual(got[2], 31) {
			t.Errorf("max(n): got %v, want 31", got[2])
		}
		if !isNumericEqual(got[3], 5) {
			t.Errorf("max(log): got %v, want 5", got[3])
		}
		if !isNumericEqual(got[4], 496) {
			t.Errorf("sum(n): got %v, want 496", got[4])
		}
		if !isNumericEqual(got[5], 124) {
			t.Errorf("sum(log): got %v, want 124", got[5])
		}
		// avg(n) should be 16.0
		if !isNumericEqual(got[6], 16.0) {
			t.Errorf("avg(n): got %v, want 16.0", got[6])
		}
		// avg(log) should be 4.0
		if !isNumericEqual(got[7], 4.0) {
			t.Errorf("avg(log): got %v, want 4.0", got[7])
		}
	})

	t.Run("1.3 - max(n)/avg(n), max(log)/avg(log)", func(t *testing.T) {
		got := queryFlat(t, db, "SELECT max(n)/avg(n), max(log)/avg(log) FROM t1")
		if len(got) != 2 {
			t.Fatalf("expected 2 values, got %d: %v", len(got), got)
		}
		// max(n)/avg(n) = 31/16.0 = 1.9375
		// max(log)/avg(log) = 5/4.0 = 1.25
		if !isNumericEqual(got[0], 1.9375) {
			t.Errorf("max(n)/avg(n): got %v, want 1.9375", got[0])
		}
		if !isNumericEqual(got[1], 1.25) {
			t.Errorf("max(log)/avg(log): got %v, want 1.25", got[1])
		}
	})
}

func TestSelect3GroupBy(t *testing.T) {
	db := openTestDB(t)
	setupSelect3Tables(t, db)

	t.Run("2.1 - GROUP BY log ORDER BY log", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY")
		got := queryFlat(t, db, "SELECT log, count(*) FROM t1 GROUP BY log ORDER BY log")
		want := []interface{}{int64(0), int64(1), int64(1), int64(1), int64(2), int64(2), int64(3), int64(4), int64(4), int64(8), int64(5), int64(15)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("2.2 - log, min(n) GROUP BY log", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY")
		got := queryFlat(t, db, "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log")
		want := []interface{}{int64(0), int64(1), int64(1), int64(2), int64(2), int64(3), int64(3), int64(5), int64(4), int64(9), int64(5), int64(17)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("2.3.1 - log, avg(n) GROUP BY log", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY")
		got := queryFlat(t, db, "SELECT log, avg(n) FROM t1 GROUP BY log ORDER BY log")
		if len(got) != 12 {
			t.Fatalf("expected 12 values, got %d: %v", len(got), got)
		}
		// avg values: 1.0, 2.0, 3.5, 6.5, 12.5, 24.0
		expectedAvgs := []float64{1.0, 2.0, 3.5, 6.5, 12.5, 24.0}
		for i, avg := range expectedAvgs {
			if !isNumericEqual(got[i*2+1], avg) {
				t.Errorf("avg for log=%d: got %v, want %v", i, got[i*2+1], avg)
			}
		}
	})

	t.Run("2.3.2 - log, avg(n)+1 GROUP BY log", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY")
		got := queryFlat(t, db, "SELECT log, avg(n)+1 FROM t1 GROUP BY log ORDER BY log")
		if len(got) != 12 {
			t.Fatalf("expected 12 values, got %d: %v", len(got), got)
		}
		expectedAvgs := []float64{2.0, 3.0, 4.5, 7.5, 13.5, 25.0}
		for i, avg := range expectedAvgs {
			if !isNumericEqual(got[i*2+1], avg) {
				t.Errorf("avg+1 for log=%d: got %v, want %v", i, got[i*2+1], avg)
			}
		}
	})

	t.Run("2.4 - log, avg(n)-min(n) GROUP BY log", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY")
		got := queryFlat(t, db, "SELECT log, avg(n)-min(n) FROM t1 GROUP BY log ORDER BY log")
		if len(got) != 12 {
			t.Fatalf("expected 12 values, got %d: %v", len(got), got)
		}
		expectedDiffs := []float64{0.0, 0.0, 0.5, 1.5, 3.5, 7.0}
		for i, diff := range expectedDiffs {
			if !isNumericEqual(got[i*2+1], diff) {
				t.Errorf("avg-min for log=%d: got %v, want %v", i, got[i*2+1], diff)
			}
		}
	})

	t.Run("2.5 - log*2+1, avg(n)-min(n) GROUP BY log", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY")
		got := queryFlat(t, db, "SELECT log*2+1, avg(n)-min(n) FROM t1 GROUP BY log ORDER BY log")
		if len(got) != 12 {
			t.Fatalf("expected 12 values, got %d: %v", len(got), got)
		}
		expectedFirst := []float64{1, 3, 5, 7, 9, 11}
		for i, v := range expectedFirst {
			if !isNumericEqual(got[i*2], v) {
				t.Errorf("log*2+1 for log=%d: got %v, want %v", i, got[i*2], v)
			}
		}
	})

	t.Run("2.6 - GROUP BY alias x", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY")
		got := queryFlat(t, db, "SELECT log*2+1 as x, count(*) FROM t1 GROUP BY x ORDER BY x")
		want := []interface{}{int64(1), int64(1), int64(3), int64(1), int64(5), int64(2), int64(7), int64(4), int64(9), int64(8), int64(11), int64(15)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("2.10 - GROUP BY 0 out of range", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY")
		err := catchSQLErr(t, db, "SELECT log, count(*) FROM t1 GROUP BY 0 ORDER BY log")
		if err == nil {
			t.Error("expected error for GROUP BY 0")
		}
	})

	t.Run("2.11 - GROUP BY 3 out of range", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY")
		err := catchSQLErr(t, db, "SELECT log, count(*) FROM t1 GROUP BY 3 ORDER BY log")
		if err == nil {
			t.Error("expected error for GROUP BY 3")
		}
	})

	t.Run("2.12 - GROUP BY 1", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY")
		got := queryFlat(t, db, "SELECT log, count(*) FROM t1 GROUP BY 1 ORDER BY log")
		want := []interface{}{int64(0), int64(1), int64(1), int64(1), int64(2), int64(2), int64(3), int64(4), int64(4), int64(8), int64(5), int64(15)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("2.13 - empty GROUP BY syntax error", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY")
		err := catchSQLErr(t, db, "SELECT log, count(*) FROM t1 GROUP BY ORDER BY log")
		if err == nil {
			t.Error("expected syntax error for empty GROUP BY")
		}
	})

	t.Run("2.14 - GROUP BY; syntax error", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY")
		err := catchSQLErr(t, db, "SELECT log, count(*) FROM t1 GROUP BY;")
		if err == nil {
			t.Error("expected syntax error for GROUP BY;")
		}
	})
}

func TestSelect3Having(t *testing.T) {
	db := openTestDB(t)
	setupSelect3Tables(t, db)

	t.Run("4.1 - HAVING log>=4", func(t *testing.T) {
		// t.Skip("feature not yet implemented: HAVING")
		got := queryFlat(t, db, "SELECT log, count(*) FROM t1 GROUP BY log HAVING log>=4 ORDER BY log")
		want := []interface{}{int64(4), int64(8), int64(5), int64(15)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("4.2 - HAVING count(*)>=4", func(t *testing.T) {
		// t.Skip("feature not yet implemented: HAVING")
		got := queryFlat(t, db, "SELECT log, count(*) FROM t1 GROUP BY log HAVING count(*)>=4 ORDER BY log")
		want := []interface{}{int64(3), int64(4), int64(4), int64(8), int64(5), int64(15)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("4.3 - HAVING count(*)>=4 ORDER BY max(n)+0", func(t *testing.T) {
		// t.Skip("feature not yet implemented: HAVING")
		got := queryFlat(t, db, "SELECT log, count(*) FROM t1 GROUP BY log HAVING count(*)>=4 ORDER BY max(n)+0")
		want := []interface{}{int64(3), int64(4), int64(4), int64(8), int64(5), int64(15)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("4.4 - aliases in GROUP BY and HAVING", func(t *testing.T) {
		// t.Skip("feature not yet implemented: HAVING")
		got := queryFlat(t, db, "SELECT log AS x, count(*) AS y FROM t1 GROUP BY x HAVING y>=4 ORDER BY max(n)+0")
		want := []interface{}{int64(3), int64(4), int64(4), int64(8), int64(5), int64(15)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("4.5 - select only x from GROUP BY", func(t *testing.T) {
		// t.Skip("feature not yet implemented: HAVING")
		got := queryFlat(t, db, "SELECT log AS x FROM t1 GROUP BY x HAVING count(*)>=4 ORDER BY max(n)+0")
		want := []interface{}{int64(3), int64(4), int64(5)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

func TestSelect3OrderByWithIndex(t *testing.T) {
	db := openTestDB(t)
	setupSelect3Tables(t, db)

	t.Run("6.1 - GROUP BY log ORDER BY log", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY with ORDER BY")
		got := queryFlat(t, db, "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log")
		want := []interface{}{int64(0), int64(1), int64(1), int64(2), int64(2), int64(3), int64(3), int64(5), int64(4), int64(9), int64(5), int64(17)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("6.2 - GROUP BY log ORDER BY log DESC", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY with ORDER BY")
		got := queryFlat(t, db, "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log DESC")
		want := []interface{}{int64(5), int64(17), int64(4), int64(9), int64(3), int64(5), int64(2), int64(3), int64(1), int64(2), int64(0), int64(1)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("6.3 - GROUP BY log ORDER BY 1", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY with ORDER BY")
		got := queryFlat(t, db, "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY 1")
		want := []interface{}{int64(0), int64(1), int64(1), int64(2), int64(2), int64(3), int64(3), int64(5), int64(4), int64(9), int64(5), int64(17)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("6.4 - GROUP BY log ORDER BY 1 DESC", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY with ORDER BY")
		got := queryFlat(t, db, "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY 1 DESC")
		want := []interface{}{int64(5), int64(17), int64(4), int64(9), int64(3), int64(5), int64(2), int64(3), int64(1), int64(2), int64(0), int64(1)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	// Create index and re-test
	db.Exec("CREATE INDEX i1 ON t1(log)")

	t.Run("6.5 - GROUP BY log with index ORDER BY log", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY with ORDER BY")
		got := queryFlat(t, db, "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log")
		want := []interface{}{int64(0), int64(1), int64(1), int64(2), int64(2), int64(3), int64(3), int64(5), int64(4), int64(9), int64(5), int64(17)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("6.6 - GROUP BY log with index ORDER BY log DESC", func(t *testing.T) {
		// t.Skip("feature not yet implemented: GROUP BY with ORDER BY")
		got := queryFlat(t, db, "SELECT log, min(n) FROM t1 GROUP BY log ORDER BY log DESC")
		want := []interface{}{int64(5), int64(17), int64(4), int64(9), int64(3), int64(5), int64(2), int64(3), int64(1), int64(2), int64(0), int64(1)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

func TestSelect3EmptyAggregate(t *testing.T) {
	db := openTestDB(t)
	setupSelect3Tables(t, db)

	t.Run("7.1 - aggregate with no matching rows returns empty", func(t *testing.T) {
		db.Exec("CREATE TABLE t2(a,b)")
		db.Exec("INSERT INTO t2 VALUES(1,2)")
		got := queryFlat(t, db, "SELECT a, sum(b) FROM t2 WHERE b=5 GROUP BY a")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	t.Run("7.2 - aggregate without GROUP BY returns NULL", func(t *testing.T) {
		db.Exec("CREATE TABLE t2b(a,b)")
		db.Exec("INSERT INTO t2b VALUES(1,2)")
		got := queryFlat(t, db, "SELECT a, sum(b) FROM t2b WHERE b=5")
		// No rows match, but aggregate without GROUP BY returns one row with NULLs
		if len(got) != 2 {
			t.Fatalf("expected 2 values (NULL NULL), got %d: %v", len(got), got)
		}
		// Both should be null or nil
		if got[0] != nil && !isNullLike(got[0]) {
			t.Errorf("first value: got %v, want NULL", got[0])
		}
		if got[1] != nil && !isNullLike(got[1]) {
			t.Errorf("second value: got %v, want NULL", got[1])
		}
	})
}

func TestSelect3ComplexAggregate(t *testing.T) {
	db := openTestDB(t)
	setupSelect3Tables(t, db)

	t.Run("5.1 - multiple aggregates with complex ORDER BY", func(t *testing.T) {
		got := queryFlat(t, db, "SELECT log, count(*), avg(n), max(n+log*2) FROM t1 GROUP BY log ORDER BY max(n+log*2)+0, avg(n)+0")
		if len(got) != 24 {
			t.Fatalf("expected 24 values, got %d: %v", len(got), got)
		}
		// Just verify we got 6 groups of 4 values each
		// log values should be 0,1,2,3,4,5 in order
		expectedLogs := []float64{0, 1, 2, 3, 4, 5}
		for i, log := range expectedLogs {
			if !isNumericEqual(got[i*4], log) {
				t.Errorf("group %d log: got %v, want %v", i, got[i*4], log)
			}
		}
	})
}

// Helper to check if a value is null-like
func isNullLike(v interface{}) bool {
	if v == nil {
		return true
	}
	switch v := v.(type) {
	case string:
		return v == "" || v == "NULL" || v == "<nil>"
	default:
		return false
	}
}

// Ensure Database type is used in this file
var _ *sqlite.Database = nil
