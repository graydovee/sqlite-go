package tests

import (
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// ============================================================================
// GROUP BY Tests
// ============================================================================

// setupGroupByTable creates a table with test data for GROUP BY tests:
//
//	log | n
//	----+---
//	 1  | 1
//	 1  | 2
//	 1  | 3
//	 2  | 4
//	 2  | 5
//	 3  | 6
//	 3  | 7
//	 3  | 8
//	 3  | 9
func setupGroupByTable(t *testing.T) *sqlite.Database {
	t.Helper()
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t1 (log INT, n INT)")
	rows := []struct{ log, n int }{
		{1, 1}, {1, 2}, {1, 3},
		{2, 4}, {2, 5},
		{3, 6}, {3, 7}, {3, 8}, {3, 9},
	}
	for _, r := range rows {
		execOrFail(t, db, "INSERT INTO t1 VALUES (?, ?)", r.log, r.n)
	}
	return db
}

func TestGroupByCount(t *testing.T) {
	db := setupGroupByTable(t)

	rs, err := db.Query("SELECT log, count(*) FROM t1 GROUP BY log")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	var results [][2]int64
	for rs.Next() {
		row := rs.Row()
		log := row.ColumnInt(0)
		cnt := row.ColumnInt(1)
		results = append(results, [2]int64{log, cnt})
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 groups, got %d: %v", len(results), results)
	}

	expected := map[int64]int64{1: 3, 2: 2, 3: 4}
	for _, r := range results {
		if exp, ok := expected[r[0]]; !ok {
			t.Errorf("unexpected group key %d", r[0])
		} else if r[1] != exp {
			t.Errorf("log=%d: count=%d, want %d", r[0], r[1], exp)
		}
	}
}

func TestGroupBySum(t *testing.T) {
	db := setupGroupByTable(t)

	rs, err := db.Query("SELECT log, sum(n) FROM t1 GROUP BY log")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	expected := map[int64]int64{1: 6, 2: 9, 3: 30}
	for rs.Next() {
		row := rs.Row()
		log := row.ColumnInt(0)
		sum := row.ColumnInt(1)
		if exp, ok := expected[log]; !ok {
			t.Errorf("unexpected group key %d", log)
		} else if sum != exp {
			t.Errorf("log=%d: sum=%d, want %d", log, sum, exp)
		}
	}
}

func TestGroupByAvg(t *testing.T) {
	db := setupGroupByTable(t)

	rs, err := db.Query("SELECT log, avg(n) FROM t1 GROUP BY log")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	expected := map[int64]float64{1: 2.0, 2: 4.5, 3: 7.5}
	for rs.Next() {
		row := rs.Row()
		log := row.ColumnInt(0)
		avg := row.ColumnFloat(1)
		if exp, ok := expected[log]; !ok {
			t.Errorf("unexpected group key %d", log)
		} else if avg != exp {
			t.Errorf("log=%d: avg=%f, want %f", log, avg, exp)
		}
	}
}

func TestGroupByMin(t *testing.T) {
	db := setupGroupByTable(t)

	rs, err := db.Query("SELECT log, min(n) FROM t1 GROUP BY log")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	expected := map[int64]int64{1: 1, 2: 4, 3: 6}
	for rs.Next() {
		row := rs.Row()
		log := row.ColumnInt(0)
		min := row.ColumnInt(1)
		if exp, ok := expected[log]; !ok {
			t.Errorf("unexpected group key %d", log)
		} else if min != exp {
			t.Errorf("log=%d: min=%d, want %d", log, min, exp)
		}
	}
}

func TestGroupByMax(t *testing.T) {
	db := setupGroupByTable(t)

	rs, err := db.Query("SELECT log, max(n) FROM t1 GROUP BY log")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	expected := map[int64]int64{1: 3, 2: 5, 3: 9}
	for rs.Next() {
		row := rs.Row()
		log := row.ColumnInt(0)
		max := row.ColumnInt(1)
		if exp, ok := expected[log]; !ok {
			t.Errorf("unexpected group key %d", log)
		} else if max != exp {
			t.Errorf("log=%d: max=%d, want %d", log, max, exp)
		}
	}
}

func TestGroupByMultipleAggregates(t *testing.T) {
	db := setupGroupByTable(t)

	rs, err := db.Query("SELECT log, count(*), sum(n), min(n), max(n) FROM t1 GROUP BY log")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	type result struct {
		log, cnt, sum, min, max int64
	}
	expected := map[int64]result{
		1: {1, 3, 6, 1, 3},
		2: {2, 2, 9, 4, 5},
		3: {3, 4, 30, 6, 9},
	}

	for rs.Next() {
		row := rs.Row()
		log := row.ColumnInt(0)
		cnt := row.ColumnInt(1)
		sum := row.ColumnInt(2)
		min := row.ColumnInt(3)
		max := row.ColumnInt(4)
		if exp, ok := expected[log]; !ok {
			t.Errorf("unexpected group key %d", log)
		} else {
			if cnt != exp.cnt {
				t.Errorf("log=%d: count=%d, want %d", log, cnt, exp.cnt)
			}
			if sum != exp.sum {
				t.Errorf("log=%d: sum=%d, want %d", log, sum, exp.sum)
			}
			if min != exp.min {
				t.Errorf("log=%d: min=%d, want %d", log, min, exp.min)
			}
			if max != exp.max {
				t.Errorf("log=%d: max=%d, want %d", log, max, exp.max)
			}
		}
	}
}

func TestGroupByWithOrderBy(t *testing.T) {
	db := setupGroupByTable(t)

	rs, err := db.Query("SELECT log, count(*) FROM t1 GROUP BY log ORDER BY log")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	expected := [][2]int64{{1, 3}, {2, 2}, {3, 4}}
	var results [][2]int64
	for rs.Next() {
		row := rs.Row()
		log := row.ColumnInt(0)
		cnt := row.ColumnInt(1)
		results = append(results, [2]int64{log, cnt})
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(results))
	}
	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("row %d: got %v, want %v", i, results[i], exp)
		}
	}
}

func TestGroupByWithHaving(t *testing.T) {
	db := setupGroupByTable(t)

	rs, err := db.Query("SELECT log, count(*) FROM t1 GROUP BY log HAVING count(*) > 2")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	var results [][2]int64
	for rs.Next() {
		row := rs.Row()
		log := row.ColumnInt(0)
		cnt := row.ColumnInt(1)
		results = append(results, [2]int64{log, cnt})
	}

	// Only log=1 (count=3) and log=3 (count=4) have count > 2
	if len(results) != 2 {
		t.Fatalf("expected 2 groups after HAVING, got %d: %v", len(results), results)
	}
	for _, r := range results {
		if r[1] <= 2 {
			t.Errorf("group log=%d has count=%d, should be filtered by HAVING", r[0], r[1])
		}
	}
}

func TestGroupByWithWhere(t *testing.T) {
	db := setupGroupByTable(t)

	rs, err := db.Query("SELECT log, count(*) FROM t1 WHERE n > 3 GROUP BY log")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	// After WHERE n > 3: log=2 has {4,5}, log=3 has {6,7,8,9}
	expected := map[int64]int64{2: 2, 3: 4}
	for rs.Next() {
		row := rs.Row()
		log := row.ColumnInt(0)
		cnt := row.ColumnInt(1)
		if exp, ok := expected[log]; !ok {
			t.Errorf("unexpected group key %d", log)
		} else if cnt != exp {
			t.Errorf("log=%d: count=%d, want %d", log, cnt, exp)
		}
	}
}

func TestAggregateWithoutGroupBy(t *testing.T) {
	db := setupGroupByTable(t)

	// COUNT without GROUP BY should still work (returns one row)
	rs, err := db.Query("SELECT count(*) FROM t1")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one row")
	}
	row := rs.Row()
	cnt := row.ColumnInt(0)
	if cnt != 9 {
		t.Errorf("count(*)=%d, want 9", cnt)
	}
	if rs.Next() {
		t.Error("expected exactly one row")
	}
}

func TestAggregateSumWithoutGroupBy(t *testing.T) {
	db := setupGroupByTable(t)

	rs, err := db.Query("SELECT sum(n) FROM t1")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one row")
	}
	row := rs.Row()
	sum := row.ColumnInt(0)
	// 1+2+3+4+5+6+7+8+9 = 45
	if sum != 45 {
		t.Errorf("sum(n)=%d, want 45", sum)
	}
}

func TestGroupByCountColumn(t *testing.T) {
	db := setupGroupByTable(t)

	// COUNT(column) counts non-NULL values
	rs, err := db.Query("SELECT log, count(n) FROM t1 GROUP BY log")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	expected := map[int64]int64{1: 3, 2: 2, 3: 4}
	for rs.Next() {
		row := rs.Row()
		log := row.ColumnInt(0)
		cnt := row.ColumnInt(1)
		if exp, ok := expected[log]; !ok {
			t.Errorf("unexpected group key %d", log)
		} else if cnt != exp {
			t.Errorf("log=%d: count=%d, want %d", log, cnt, exp)
		}
	}
}

func TestGroupByEmptyTable(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE empty (a INT, b INT)")

	// GROUP BY on empty table should return empty result set
	rs, err := db.Query("SELECT a, count(*) FROM empty GROUP BY a")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	if rs.Next() {
		t.Error("expected no rows from GROUP BY on empty table")
	}
}

func TestGroupBySingleGroup(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t2 (cat TEXT, val INT)")
	execOrFail(t, db, "INSERT INTO t2 VALUES ('A', 10)")
	execOrFail(t, db, "INSERT INTO t2 VALUES ('A', 20)")

	rs, err := db.Query("SELECT cat, sum(val) FROM t2 GROUP BY cat")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one row")
	}
	row := rs.Row()
	cat := row.ColumnText(0)
	sum := row.ColumnInt(1)
	if cat != "A" {
		t.Errorf("cat=%s, want A", cat)
	}
	if sum != 30 {
		t.Errorf("sum=%d, want 30", sum)
	}
	if rs.Next() {
		t.Error("expected exactly one row")
	}
}

func TestGroupByWithTextColumn(t *testing.T) {
	db := openTestDB(t)
	execOrFail(t, db, "CREATE TABLE t3 (dept TEXT, salary INT)")
	execOrFail(t, db, "INSERT INTO t3 VALUES ('eng', 100)")
	execOrFail(t, db, "INSERT INTO t3 VALUES ('eng', 120)")
	execOrFail(t, db, "INSERT INTO t3 VALUES ('sales', 80)")
	execOrFail(t, db, "INSERT INTO t3 VALUES ('sales', 90)")
	execOrFail(t, db, "INSERT INTO t3 VALUES ('hr', 70)")

	rs, err := db.Query("SELECT dept, count(*), avg(salary) FROM t3 GROUP BY dept")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	expected := map[string][2]float64{
		"eng":   {2, 110.0},
		"sales": {2, 85.0},
		"hr":    {1, 70.0},
	}

	for rs.Next() {
		row := rs.Row()
		dept := row.ColumnText(0)
		cnt := row.ColumnInt(1)
		avg := row.ColumnFloat(2)
		if exp, ok := expected[dept]; !ok {
			t.Errorf("unexpected dept %s", dept)
		} else {
			if int64(exp[0]) != cnt {
				t.Errorf("dept=%s: count=%d, want %d", dept, cnt, int64(exp[0]))
			}
			if avg != exp[1] {
				t.Errorf("dept=%s: avg=%f, want %f", dept, avg, exp[1])
			}
		}
	}
}

func TestGroupByWithOrderByDesc(t *testing.T) {
	db := setupGroupByTable(t)

	rs, err := db.Query("SELECT log, sum(n) FROM t1 GROUP BY log ORDER BY log DESC")
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	defer rs.Close()

	expected := [][2]int64{{3, 30}, {2, 9}, {1, 6}}
	var results [][2]int64
	for rs.Next() {
		row := rs.Row()
		log := row.ColumnInt(0)
		sum := row.ColumnInt(1)
		results = append(results, [2]int64{log, sum})
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(results))
	}
	for i, exp := range expected {
		if results[i] != exp {
			t.Errorf("row %d: got %v, want %v", i, results[i], exp)
		}
	}
}
