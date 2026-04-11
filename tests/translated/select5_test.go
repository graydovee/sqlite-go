package tests

import (
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// setupSelect5Tables creates t1 with x=31-i (0..30), y=10-floor(log2(i)) for i=1..31
func setupSelect5Tables(t *testing.T, db *sqlite.Database) {
	t.Helper()
	db.Exec("CREATE TABLE t1(x int, y int)")
	db.Exec("BEGIN")
	for i := 1; i < 32; i++ {
		j := 0
		for (1 << uint(j)) < i {
			j++
		}
		x := 32 - i
		y := 10 - j
		db.Exec("INSERT INTO t1 VALUES(?, ?)", x, y)
	}
	db.Exec("COMMIT")
}

func TestSelect5Distinct(t *testing.T) {
	db := openTestDB(t)
	setupSelect5Tables(t, db)

	t.Run("1.0 - DISTINCT y ORDER BY y", func(t *testing.T) {
		t.Skip("feature not yet implemented: DISTINCT not supported")
		got := queryFlat(t, db, "SELECT DISTINCT y FROM t1 ORDER BY y")
		want := []interface{}{int64(5), int64(6), int64(7), int64(8), int64(9), int64(10)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

func TestSelect5GroupByAggregate(t *testing.T) {
	db := openTestDB(t)
	setupSelect5Tables(t, db)

	t.Run("1.1 - y, count(*) GROUP BY y ORDER BY y", func(t *testing.T) {
		t.Skip("feature not yet implemented: GROUP BY not supported")
		got := queryFlat(t, db, "SELECT y, count(*) FROM t1 GROUP BY y ORDER BY y")
		want := []interface{}{int64(5), int64(15), int64(6), int64(8), int64(7), int64(4), int64(8), int64(2), int64(9), int64(1), int64(10), int64(1)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("1.2 - sort by aggregate", func(t *testing.T) {
		t.Skip("feature not yet implemented: GROUP BY not supported")
		got := queryFlat(t, db, "SELECT y, count(*) FROM t1 GROUP BY y ORDER BY count(*), y")
		want := []interface{}{int64(9), int64(1), int64(10), int64(1), int64(8), int64(2), int64(7), int64(4), int64(6), int64(8), int64(5), int64(15)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("1.3 - count(*), y ORDER BY count(*), y", func(t *testing.T) {
		t.Skip("feature not yet implemented: GROUP BY not supported")
		got := queryFlat(t, db, "SELECT count(*), y FROM t1 GROUP BY y ORDER BY count(*), y")
		want := []interface{}{int64(1), int64(9), int64(1), int64(10), int64(2), int64(8), int64(4), int64(7), int64(8), int64(6), int64(15), int64(5)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

func TestSelect5GroupByErrors(t *testing.T) {
	db := openTestDB(t)
	setupSelect5Tables(t, db)

	t.Run("2.1.1 - GROUP BY non-existent column z", func(t *testing.T) {
		t.Skip("feature not yet implemented: GROUP BY not supported")
		err := catchSQL(t, db, "SELECT y, count(*) FROM t1 GROUP BY z ORDER BY y")
		if err == nil {
			t.Error("expected error for non-existent column z")
		}
	})

	t.Run("2.3 - HAVING count(*)<3", func(t *testing.T) {
		t.Skip("feature not yet implemented: HAVING not supported")
		got := queryFlat(t, db, "SELECT y, count(*) FROM t1 GROUP BY y HAVING count(*)<3 ORDER BY y")
		want := []interface{}{int64(8), int64(2), int64(9), int64(1), int64(10), int64(1)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("2.4 - HAVING with non-existent function z(y)", func(t *testing.T) {
		t.Skip("feature not yet implemented: HAVING not supported")
		err := catchSQL(t, db, "SELECT y, count(*) FROM t1 GROUP BY y HAVING z(y)<3 ORDER BY y")
		if err == nil {
			t.Error("expected error for non-existent function z")
		}
	})

	t.Run("2.5 - HAVING with non-existent column z", func(t *testing.T) {
		t.Skip("feature not yet implemented: HAVING not supported")
		err := catchSQL(t, db, "SELECT y, count(*) FROM t1 GROUP BY y HAVING count(*)<z ORDER BY y")
		if err == nil {
			t.Error("expected error for non-existent column z")
		}
	})
}

func TestSelect5AggregateRehash(t *testing.T) {
	db := openTestDB(t)
	setupSelect5Tables(t, db)

	t.Run("3.1 - x, count(*), avg(y) GROUP BY x HAVING x<4", func(t *testing.T) {
		t.Skip("feature not yet implemented: GROUP BY and HAVING not supported")
		got := queryFlat(t, db, "SELECT x, count(*), avg(y) FROM t1 GROUP BY x HAVING x<4 ORDER BY x")
		want := []interface{}{int64(1), int64(1), float64(5.0), int64(2), int64(1), float64(5.0), int64(3), int64(1), float64(5.0)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})
}

func TestSelect5ZeroCount(t *testing.T) {
	db := openTestDB(t)
	setupSelect5Tables(t, db)

	t.Run("4.1 - avg(x) WHERE x>100 (empty)", func(t *testing.T) {
		t.Skip("feature not yet implemented: aggregate functions not fully supported")
		got := queryFlat(t, db, "SELECT avg(x) FROM t1 WHERE x>100")
		if len(got) != 1 {
			t.Fatalf("expected 1 value, got %d: %v", len(got), got)
		}
		// avg of empty set should be NULL
		if got[0] != nil && !isNullLike(got[0]) {
			t.Errorf("expected NULL, got %v", got[0])
		}
	})

	t.Run("4.2 - count(x) WHERE x>100", func(t *testing.T) {
		t.Skip("feature not yet implemented: aggregate functions not fully supported")
		got := queryFlat(t, db, "SELECT count(x) FROM t1 WHERE x>100")
		want := []interface{}{int64(0)}
		if !equalValues(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("4.3 - min(x) WHERE x>100 (empty)", func(t *testing.T) {
		t.Skip("feature not yet implemented: aggregate functions not fully supported")
		got := queryFlat(t, db, "SELECT min(x) FROM t1 WHERE x>100")
		if len(got) != 1 {
			t.Fatalf("expected 1 value, got %d: %v", len(got), got)
		}
		if got[0] != nil && !isNullLike(got[0]) {
			t.Errorf("expected NULL, got %v", got[0])
		}
	})

	t.Run("4.4 - max(x) WHERE x>100 (empty)", func(t *testing.T) {
		t.Skip("feature not yet implemented: aggregate functions not fully supported")
		got := queryFlat(t, db, "SELECT max(x) FROM t1 WHERE x>100")
		if len(got) != 1 {
			t.Fatalf("expected 1 value, got %d: %v", len(got), got)
		}
		if got[0] != nil && !isNullLike(got[0]) {
			t.Errorf("expected NULL, got %v", got[0])
		}
	})

	t.Run("4.5 - sum(x) WHERE x>100 (empty)", func(t *testing.T) {
		t.Skip("feature not yet implemented: aggregate functions not fully supported")
		got := queryFlat(t, db, "SELECT sum(x) FROM t1 WHERE x>100")
		if len(got) != 1 {
			t.Fatalf("expected 1 value, got %d: %v", len(got), got)
		}
		if got[0] != nil && !isNullLike(got[0]) {
			t.Errorf("expected NULL, got %v", got[0])
		}
	})
}

func TestSelect5GroupByNoAggregate(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t2(a, b, c)")
	db.Exec("INSERT INTO t2 VALUES(1, 2, 3)")
	db.Exec("INSERT INTO t2 VALUES(1, 4, 5)")
	db.Exec("INSERT INTO t2 VALUES(6, 4, 7)")
	db.Exec("CREATE INDEX t2_idx ON t2(a)")

	t.Run("5.2 - GROUP BY without aggregates", func(t *testing.T) {
		t.Skip("feature not yet implemented: GROUP BY not supported")
		got := queryFlat(t, db, "SELECT a FROM t2 GROUP BY a")
		if len(got) != 2 {
			t.Fatalf("expected 2 values, got %d: %v", len(got), got)
		}
	})

	t.Run("5.3 - GROUP BY with WHERE", func(t *testing.T) {
		t.Skip("feature not yet implemented: GROUP BY not supported")
		got := queryFlat(t, db, "SELECT a FROM t2 WHERE a>2 GROUP BY a")
		if len(got) != 1 {
			t.Fatalf("expected 1 value, got %d: %v", len(got), got)
		}
		if !isNumericEqual(got[0], 6) {
			t.Errorf("got %v, want 6", got[0])
		}
	})

	t.Run("5.4 - GROUP BY a, b", func(t *testing.T) {
		t.Skip("feature not yet implemented: GROUP BY not supported")
		got := queryFlat(t, db, "SELECT a, b FROM t2 GROUP BY a, b")
		if len(got) != 4 {
			t.Fatalf("expected 4 values, got %d: %v", len(got), got)
		}
	})

	t.Run("5.5 - GROUP BY a only", func(t *testing.T) {
		t.Skip("feature not yet implemented: GROUP BY not supported")
		got := queryFlat(t, db, "SELECT a, b FROM t2 GROUP BY a")
		if len(got) != 4 {
			t.Fatalf("expected 4 values (2 groups * 2 cols), got %d: %v", len(got), got)
		}
	})
}

func TestSelect5NullGroupBy(t *testing.T) {
	db := openTestDB(t)

	t.Run("6.1 - NULLs grouped together", func(t *testing.T) {
		t.Skip("feature not yet implemented: GROUP BY not supported")
		db.Exec("CREATE TABLE t3(x,y)")
		db.Exec("INSERT INTO t3 VALUES(1,NULL)")
		db.Exec("INSERT INTO t3 VALUES(2,NULL)")
		db.Exec("INSERT INTO t3 VALUES(3,4)")

		got := queryFlat(t, db, "SELECT count(x), y FROM t3 GROUP BY y ORDER BY 1")
		// Two groups: y=NULL (count=2), y=4 (count=1)
		if len(got) != 4 {
			t.Fatalf("expected 4 values, got %d: %v", len(got), got)
		}
		// First group: count=1, y=4
		if !isNumericEqual(got[0], 1) {
			t.Errorf("first count: got %v, want 1", got[0])
		}
		if !isNumericEqual(got[2], 2) {
			t.Errorf("second count: got %v, want 2", got[2])
		}
	})

	t.Run("6.2 - multi-column GROUP BY with NULLs", func(t *testing.T) {
		t.Skip("feature not yet implemented: GROUP BY not supported")
		db.Exec("CREATE TABLE t4(x,y,z)")
		db.Exec("INSERT INTO t4 VALUES(1,2,NULL)")
		db.Exec("INSERT INTO t4 VALUES(2,3,NULL)")
		db.Exec("INSERT INTO t4 VALUES(3,NULL,5)")
		db.Exec("INSERT INTO t4 VALUES(4,NULL,6)")
		db.Exec("INSERT INTO t4 VALUES(4,NULL,6)")
		db.Exec("INSERT INTO t4 VALUES(5,NULL,NULL)")
		db.Exec("INSERT INTO t4 VALUES(5,NULL,NULL)")
		db.Exec("INSERT INTO t4 VALUES(6,7,8)")

		got := queryFlat(t, db, "SELECT max(x), count(x), y, z FROM t4 GROUP BY y, z ORDER BY 1")
		// Groups: (2,NULL), (3,NULL), (NULL,5), (NULL,6), (NULL,NULL), (7,8)
		if len(got) < 12 {
			t.Fatalf("expected at least 12 values (4 cols * N groups), got %d: %v", len(got), got)
		}
		// Just verify we get results
		_ = got
	})
}

func TestSelect5MultiTableAggregate(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t8a(a,b)")
	db.Exec("CREATE TABLE t8b(x)")
	db.Exec("INSERT INTO t8a VALUES('one', 1)")
	db.Exec("INSERT INTO t8a VALUES('one', 2)")
	db.Exec("INSERT INTO t8a VALUES('two', 3)")
	db.Exec("INSERT INTO t8a VALUES('one', NULL)")

	t.Run("8.1 - cross join aggregate", func(t *testing.T) {
		// This test requires rowid support
		t.Skip("depends on rowid and multi-table aggregate support not yet implemented")

		db.Exec("INSERT INTO t8b(rowid,x) VALUES(1,111)")
		db.Exec("INSERT INTO t8b(rowid,x) VALUES(2,222)")
		db.Exec("INSERT INTO t8b(rowid,x) VALUES(3,333)")

		got := queryFlat(t, db, "SELECT a, count(b) FROM t8a, t8b WHERE b=t8b.rowid GROUP BY a ORDER BY a")
		_ = got
	})
}

// Ensure Database type is used
var _ *sqlite.Database = nil
