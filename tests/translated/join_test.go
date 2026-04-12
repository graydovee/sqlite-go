package tests

import (
	"strings"
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// ============================================================================
// join-1.*: NATURAL JOIN, INNER JOIN, USING clause tests
// ============================================================================

// setupJoinTables creates tables t1(a,b,c), t2(b,c,d), t3(c,d,e), t4(d,e,f)
// used throughout the join tests.
func setupJoinTables(t *testing.T, db *sqlite.Database) {
	t.Helper()
	mustExec(t, db, "CREATE TABLE t1(a,b,c)")
	mustExec(t, db, "INSERT INTO t1 VALUES(1,2,3)")
	mustExec(t, db, "INSERT INTO t1 VALUES(2,3,4)")
	mustExec(t, db, "INSERT INTO t1 VALUES(3,4,5)")

	mustExec(t, db, "CREATE TABLE t2(b,c,d)")
	mustExec(t, db, "INSERT INTO t2 VALUES(1,2,3)")
	mustExec(t, db, "INSERT INTO t2 VALUES(2,3,4)")
	mustExec(t, db, "INSERT INTO t2 VALUES(3,4,5)")
}

func TestJoin1Basic(t *testing.T) {
	
	db := openTestDB(t)
	setupJoinTables(t, db)

	// join-1.1: verify t1
	t.Run("1.1 - verify t1", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"1", "2", "3", "2", "3", "4", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.2: verify t2
	t.Run("1.2 - verify t2", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t2")
		want := []string{"1", "2", "3", "2", "3", "4", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.2.1: implicit join with ON (comma syntax)
	t.Run("1.2.1 - comma join with ON", func(t *testing.T) {
		t.Skip("comma join with ON not supported")
		got := queryFlatStrings(t, db, "SELECT t1.rowid, t2.rowid, '|' FROM t1, t2 ON t1.a=t2.b")
		want := []string{"1", "1", "|", "2", "2", "|", "3", "3", "|"}
		assertResults(t, got, want)
	})

	// join-1.3: NATURAL JOIN
	t.Run("1.3 - natural join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL JOIN t2")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.3.1: NATURAL JOIN reversed
	t.Run("1.3.1 - natural join reversed", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t2 NATURAL JOIN t1")
		want := []string{"2", "3", "4", "1", "3", "4", "5", "2"}
		assertResults(t, got, want)
	})

	// join-1.3.2: NATURAL JOIN with alias
	t.Run("1.3.2 - natural join with alias", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t2 AS x NATURAL JOIN t1")
		want := []string{"2", "3", "4", "1", "3", "4", "5", "2"}
		assertResults(t, got, want)
	})

	// join-1.3.3: NATURAL JOIN with alias on right
	t.Run("1.3.3 - natural join alias right", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t2 NATURAL JOIN t1 AS y")
		want := []string{"2", "3", "4", "1", "3", "4", "5", "2"}
		assertResults(t, got, want)
	})

	// join-1.3.4: select single column from natural join
	t.Run("1.3.4 - select b from natural join", func(t *testing.T) {
		got := queryStrings(t, db, "SELECT b FROM t1 NATURAL JOIN t2")
		want := []string{"2", "3"}
		assertResults(t, got, want)
	})

	// join-1.3.5: select t2.* from natural join
	t.Run("1.3.5 - select t2.* from natural join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT t2.* FROM t2 NATURAL JOIN t1")
		want := []string{"2", "3", "4", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.3.6: select alias.* from natural join
	t.Run("1.3.6 - select alias.* from natural join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT xyzzy.* FROM t2 AS xyzzy NATURAL JOIN t1")
		want := []string{"2", "3", "4", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.3.7: select t1.* from reversed natural join
	t.Run("1.3.7 - select t1.* from reversed natural join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT t1.* FROM t2 NATURAL JOIN t1")
		want := []string{"1", "2", "3", "2", "3", "4"}
		assertResults(t, got, want)
	})

	// join-1.3.8: select alias.* for right table alias
	t.Run("1.3.8 - select alias.* right table", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT xyzzy.* FROM t2 NATURAL JOIN t1 AS xyzzy")
		want := []string{"1", "2", "3", "2", "3", "4"}
		assertResults(t, got, want)
	})

	// join-1.3.9: select both aliases from natural join
	t.Run("1.3.9 - both aliases", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT aaa.*, bbb.* FROM t2 AS aaa NATURAL JOIN t1 AS bbb")
		want := []string{"2", "3", "4", "1", "2", "3", "3", "4", "5", "2", "3", "4"}
		assertResults(t, got, want)
	})

	// join-1.3.10: select t1.*, t2.* from natural join
	t.Run("1.3.10 - select both t1.* t2.*", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT t1.*, t2.* FROM t2 NATURAL JOIN t1")
		want := []string{"1", "2", "3", "2", "3", "4", "2", "3", "4", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.4.1: INNER JOIN USING(b,c)
	t.Run("1.4.1 - inner join using(b,c)", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 INNER JOIN t2 USING(b,c)")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.4.2: INNER JOIN with left alias
	t.Run("1.4.2 - inner join left alias", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 AS x INNER JOIN t2 USING(b,c)")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.4.3: INNER JOIN with right alias
	t.Run("1.4.3 - inner join right alias", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 INNER JOIN t2 AS y USING(b,c)")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.4.4: INNER JOIN both aliases
	t.Run("1.4.4 - inner join both aliases", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 AS x INNER JOIN t2 AS y USING(b,c)")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.4.5: select b from JOIN USING(b)
	t.Run("1.4.5 - select b from join using(b)", func(t *testing.T) {
		got := queryStrings(t, db, "SELECT b FROM t1 JOIN t2 USING(b)")
		want := []string{"2", "3"}
		assertResults(t, got, want)
	})

	// join-1.4.6: select t1.* from join USING(b)
	t.Run("1.4.6 - select t1.* from join using(b)", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT t1.* FROM t1 JOIN t2 USING(b)")
		want := []string{"1", "2", "3", "2", "3", "4"}
		assertResults(t, got, want)
	})

	// join-1.4.7: select t2.* from join USING(b)
	t.Run("1.4.7 - select t2.* from join using(b)", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT t2.* FROM t1 JOIN t2 USING(b)")
		want := []string{"2", "3", "4", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.5: INNER JOIN USING(b) -- both c columns
	t.Run("1.5 - inner join using(b) both c cols", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 INNER JOIN t2 USING(b)")
		want := []string{"1", "2", "3", "3", "4", "2", "3", "4", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.6: INNER JOIN USING(c) -- both b columns
	t.Run("1.6 - inner join using(c) both b cols", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 INNER JOIN t2 USING(c)")
		want := []string{"1", "2", "3", "2", "4", "2", "3", "4", "3", "5"}
		assertResults(t, got, want)
	})

	// join-1.7: INNER JOIN USING(c,b)
	t.Run("1.7 - inner join using(c,b)", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 INNER JOIN t2 USING(c,b)")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.8: NATURAL CROSS JOIN
	t.Run("1.8 - natural cross join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL CROSS JOIN t2")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.9: CROSS JOIN USING(b,c)
	t.Run("1.9 - cross join using(b,c)", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 CROSS JOIN t2 USING(b,c)")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.10: NATURAL INNER JOIN
	t.Run("1.10 - natural inner join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL INNER JOIN t2")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.11: INNER JOIN USING(b,c)
	t.Run("1.11 - inner join using(b,c)", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 INNER JOIN t2 USING(b,c)")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.12: lowercase natural inner join
	t.Run("1.12 - lowercase natural inner join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 natural inner join t2")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.13: NATURAL JOIN with subquery (skip if subquery not supported)
	t.Run("1.13 - natural join subquery", func(t *testing.T) {
		t.Skip("subquery in FROM clause")
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL JOIN (SELECT b as 'c', c as 'd', d as 'e' FROM t2) as t3")
		want := []string{"1", "2", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-1.14: NATURAL JOIN with subquery reversed
	t.Run("1.14 - natural join subquery reversed", func(t *testing.T) {
		t.Skip("subquery in FROM clause")
		got := queryFlatStrings(t, db, "SELECT * FROM (SELECT b as 'c', c as 'd', d as 'e' FROM t2) as 'tx' NATURAL JOIN t1")
		want := []string{"3", "4", "5", "1", "2"}
		assertResults(t, got, want)
	})

	// join-1.15: create t3
	t.Run("1.15 - create t3", func(t *testing.T) {
		mustExec(t, db, "CREATE TABLE t3(c,d,e)")
		mustExec(t, db, "INSERT INTO t3 VALUES(2,3,4)")
		mustExec(t, db, "INSERT INTO t3 VALUES(3,4,5)")
		mustExec(t, db, "INSERT INTO t3 VALUES(4,5,6)")
		got := queryFlatStrings(t, db, "SELECT * FROM t3")
		want := []string{"2", "3", "4", "3", "4", "5", "4", "5", "6"}
		assertResults(t, got, want)
	})

	// join-1.16: triple natural join
	t.Run("1.16 - triple natural join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 natural join t2 natural join t3")
		want := []string{"1", "2", "3", "4", "5", "2", "3", "4", "5", "6"}
		assertResults(t, got, want)
	})

	// join-1.17: triple natural join with column names
	t.Run("1.17 - triple natural join columns", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 natural join t2 natural join t3")
		want := []string{"1", "2", "3", "4", "5", "2", "3", "4", "5", "6"}
		assertResults(t, got, want)
	})

	// join-1.18: create t4
	t.Run("1.18 - create t4", func(t *testing.T) {
		mustExec(t, db, "CREATE TABLE t4(d,e,f)")
		mustExec(t, db, "INSERT INTO t4 VALUES(2,3,4)")
		mustExec(t, db, "INSERT INTO t4 VALUES(3,4,5)")
		mustExec(t, db, "INSERT INTO t4 VALUES(4,5,6)")
		got := queryFlatStrings(t, db, "SELECT * FROM t4")
		want := []string{"2", "3", "4", "3", "4", "5", "4", "5", "6"}
		assertResults(t, got, want)
	})

	// join-1.19.1: triple natural join with t4
	t.Run("1.19.1 - triple natural join t4", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 natural join t2 natural join t4")
		want := []string{"1", "2", "3", "4", "5", "6"}
		assertResults(t, got, want)
	})

	// join-1.19.2: triple natural join t4 with column names
	t.Run("1.19.2 - triple natural join t4 columns", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 natural join t2 natural join t4")
		want := []string{"1", "2", "3", "4", "5", "6"}
		assertResults(t, got, want)
	})

	// join-1.20: natural join with WHERE
	t.Run("1.20 - natural join with WHERE", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 natural join t2 natural join t3 WHERE t1.a=1")
		want := []string{"1", "2", "3", "4", "5"}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join-2.*: LEFT JOIN tests
// ============================================================================

func TestJoin2LeftJoin(t *testing.T) {
	// t.Skip("LEFT JOIN execution not fully working")
	
	db := openTestDB(t)
	setupJoinTables(t, db)

	// join-2.1: NATURAL LEFT JOIN
	t.Run("2.1 - natural left join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL LEFT JOIN t2")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5", "3", "4", "5", ""}
		assertResults(t, got, want)
	})

	// join-2.1b: OUTER LEFT NATURAL JOIN
	t.Run("2.1b - outer left natural join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 OUTER LEFT NATURAL JOIN t2")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5", "3", "4", "5", ""}
		assertResults(t, got, want)
	})

	// join-2.1c: NATURAL LEFT OUTER JOIN
	t.Run("2.1c - natural left outer join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL LEFT OUTER JOIN t2")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5", "3", "4", "5", ""}
		assertResults(t, got, want)
	})

	// join-2.1.1: NATURAL LEFT JOIN all columns
	t.Run("2.1.1 - natural left join all columns", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL LEFT JOIN t2")
		want := []string{"1", "2", "3", "4", "2", "3", "4", "5", "3", "4", "5", ""}
		assertResults(t, got, want)
	})

	// join-2.1.2: select t1.* from natural left join
	t.Run("2.1.2 - select t1.* from natural left join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT t1.* FROM t1 NATURAL LEFT JOIN t2")
		want := []string{"1", "2", "3", "2", "3", "4", "3", "4", "5"}
		assertResults(t, got, want)
	})

	// join-2.1.3: select t2.* from natural left join
	t.Run("2.1.3 - select t2.* from natural left join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT t2.* FROM t1 NATURAL LEFT JOIN t2")
		want := []string{"2", "3", "4", "3", "4", "5", "", "", ""}
		assertResults(t, got, want)
	})

	// join-2.2: reversed NATURAL LEFT OUTER JOIN
	t.Run("2.2 - reversed natural left outer join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t2 NATURAL LEFT OUTER JOIN t1")
		want := []string{"1", "2", "3", "", "2", "3", "4", "1", "3", "4", "5", "2"}
		assertResults(t, got, want)
	})

	// join-2.4: LEFT JOIN with ON condition
	t.Run("2.4 - left join ON condition", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d")
		want := []string{"1", "2", "3", "", "", "", "2", "3", "4", "", "", "", "3", "4", "5", "1", "2", "3"}
		assertResults(t, got, want)
	})

	// join-2.5: LEFT JOIN with ON and WHERE
	t.Run("2.5 - left join with WHERE", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d WHERE t1.a>1")
		want := []string{"2", "3", "4", "", "", "", "3", "4", "5", "1", "2", "3"}
		assertResults(t, got, want)
	})

	// join-2.6: LEFT JOIN with IS NULL
	t.Run("2.6 - left join IS NULL", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.d WHERE t2.b IS NULL OR t2.b>1")
		want := []string{"1", "2", "3", "", "", "", "2", "3", "4", "", "", ""}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join-3.*: Error cases for joins
// ============================================================================

func TestJoin3Errors(t *testing.T) {
	t.Skip("JOIN error detection not implemented")
	
	db := openTestDB(t)
	setupJoinTables(t, db)

	// join-3.1: NATURAL JOIN with ON clause
	t.Run("3.1 - natural join with ON clause", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t1 NATURAL JOIN t2 ON t1.a=t2.b")
		if !ok {
			t.Error("expected error for NATURAL JOIN with ON clause")
		}
	})

	// join-3.2: NATURAL JOIN with USING clause
	t.Run("3.2 - natural join with USING clause", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t1 NATURAL JOIN t2 USING(b)")
		if !ok {
			t.Error("expected error for NATURAL JOIN with USING clause")
		}
	})

	// join-3.3: JOIN with both ON and USING
	t.Run("3.3 - join with ON and USING", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t1 JOIN t2 ON t1.a=t2.b USING(b)")
		if !ok {
			t.Error("expected error for JOIN with both ON and USING")
		}
	})

	// join-3.4.1: USING column not in both tables (a)
	t.Run("3.4.1 - using column not in both tables a", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t1 JOIN t2 USING(a)")
		if !ok {
			t.Error("expected error for USING(a) - column not in both tables")
		}
	})

	// join-3.4.2: USING column not in both tables (d)
	t.Run("3.4.2 - using column not in both tables d", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t1 JOIN t2 USING(d)")
		if !ok {
			t.Error("expected error for USING(d) - column not in both tables")
		}
	})

	// join-3.5: USING without preceding JOIN
	t.Run("3.5 - using without join", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t1 USING(a)")
		if !ok {
			t.Error("expected error for USING without preceding JOIN")
		}
	})

	// join-3.6: reference to non-existent table in ON clause
	t.Run("3.6 - reference non-existent table", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t1 JOIN t2 ON t3.a=t2.b")
		if !ok {
			t.Error("expected error for reference to non-existent table t3")
		}
	})

	// join-3.7: INNER OUTER JOIN
	t.Run("3.7 - inner outer join", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t1 INNER OUTER JOIN t2")
		if !ok {
			t.Error("expected error for INNER OUTER JOIN")
		}
	})

	// join-3.8: INNER OUTER CROSS JOIN
	t.Run("3.8 - inner outer cross join", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t1 INNER OUTER CROSS JOIN t2")
		if !ok {
			t.Error("expected error for INNER OUTER CROSS JOIN")
		}
	})

	// join-3.9: OUTER NATURAL INNER JOIN
	t.Run("3.9 - outer natural inner join", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t1 OUTER NATURAL INNER JOIN t2")
		if !ok {
			t.Error("expected error for OUTER NATURAL INNER JOIN")
		}
	})

	// join-3.10: LEFT BOGUS JOIN
	t.Run("3.10 - left bogus join", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t1 LEFT BOGUS JOIN t2")
		if !ok {
			t.Error("expected error for LEFT BOGUS JOIN")
		}
	})

	// join-3.11: INNER BOGUS CROSS JOIN
	t.Run("3.11 - inner bogus cross join", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t1 INNER BOGUS CROSS JOIN t2")
		if !ok {
			t.Error("expected error for INNER BOGUS CROSS JOIN")
		}
	})

	// join-3.12: NATURAL AWK SED JOIN
	t.Run("3.12 - natural awk sed join", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t1 NATURAL AWK SED JOIN t2")
		if !ok {
			t.Error("expected error for NATURAL AWK SED JOIN")
		}
	})
}

// ============================================================================
// join-4.*: NULL handling in joins
// ============================================================================

func TestJoin4Nulls(t *testing.T) {
	// t.Skip("LEFT JOIN execution not fully working")
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t5(a INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE t6(a INTEGER)")
	mustExec(t, db, "INSERT INTO t6 VALUES(NULL)")
	mustExec(t, db, "INSERT INTO t6 VALUES(NULL)")

	// Double the rows 6 times: 2 -> 4 -> 8 -> 16 -> 32 -> 64 -> 128
	for i := 0; i < 6; i++ {
		mustExec(t, db, "INSERT INTO t6 SELECT * FROM t6")
	}

	// join-4.1: NATURAL JOIN with NULLs
	t.Run("4.1 - natural join with nulls", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t6 NATURAL JOIN t5")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	// join-4.2: cross join with NULL < comparison
	t.Run("4.2 - cross join null less than", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t6, t5 WHERE t6.a<t5.a")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	// join-4.3: cross join with NULL > comparison
	t.Run("4.3 - cross join null greater than", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t6, t5 WHERE t6.a>t5.a")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	// join-4.4: update NULLs to 'xyz' and natural join
	t.Run("4.4 - update nulls to text and natural join", func(t *testing.T) {
		mustExec(t, db, "UPDATE t6 SET a='xyz'")
		got := queryFlatStrings(t, db, "SELECT * FROM t6 NATURAL JOIN t5")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	// join-4.6: text values with < comparison
	t.Run("4.6 - text values less than", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t6, t5 WHERE t6.a<t5.a")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	// join-4.7: text values with > comparison
	t.Run("4.7 - text values greater than", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t6, t5 WHERE t6.a>t5.a")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	// join-4.8: update to integer 1 and natural join
	t.Run("4.8 - update to 1 and natural join", func(t *testing.T) {
		mustExec(t, db, "UPDATE t6 SET a=1")
		got := queryFlatStrings(t, db, "SELECT * FROM t6 NATURAL JOIN t5")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	// join-4.9: integer 1 with < comparison
	t.Run("4.9 - integer 1 less than", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t6, t5 WHERE t6.a<t5.a")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	// join-4.10: integer 1 with > comparison
	t.Run("4.10 - integer 1 greater than", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t6, t5 WHERE t6.a>t5.a")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})
}

// ============================================================================
// join-5.*: LEFT JOIN with index
// ============================================================================

func TestJoin5LeftJoinIndex(t *testing.T) {
	// t.Skip("LEFT JOIN execution not fully working")
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE centros(id integer primary key, centro)")
	mustExec(t, db, "INSERT INTO centros VALUES(1,'xxx')")
	mustExec(t, db, "CREATE TABLE usuarios(id integer primary key, nombre, apellidos, idcentro integer)")
	mustExec(t, db, "INSERT INTO usuarios VALUES(1,'a','aa',1)")
	mustExec(t, db, "INSERT INTO usuarios VALUES(2,'b','bb',1)")
	mustExec(t, db, "INSERT INTO usuarios VALUES(3,'c','cc',NULL)")
	mustExec(t, db, "CREATE INDEX idcentro ON usuarios(idcentro)")

	// join-5.1: LEFT OUTER JOIN with index
	t.Run("5.1 - left outer join with index", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT usuarios.id, usuarios.nombre, centros.centro FROM usuarios LEFT OUTER JOIN centros ON usuarios.idcentro = centros.id")
		want := []string{"1", "a", "xxx", "2", "b", "xxx", "3", "c", ""}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join-7.*: LEFT JOIN with coalesce
// ============================================================================

func TestJoin7Coalesce(t *testing.T) {
	
	db := openTestDB(t)

	// Enable double-quoted string literals for backward compatibility
	db.Exec("PRAGMA double_quote_string_literals = ON")

	mustExec(t, db, "CREATE TABLE t7(x, y)")
	mustExec(t, db, `INSERT INTO t7 VALUES("pa1", 1)`)
	mustExec(t, db, `INSERT INTO t7 VALUES("pa2", NULL)`)
	mustExec(t, db, `INSERT INTO t7 VALUES("pa3", NULL)`)
	mustExec(t, db, `INSERT INTO t7 VALUES("pa4", 2)`)
	mustExec(t, db, `INSERT INTO t7 VALUES("pa30", 131)`)
	mustExec(t, db, `INSERT INTO t7 VALUES("pa31", 130)`)
	mustExec(t, db, `INSERT INTO t7 VALUES("pa28", NULL)`)
	mustExec(t, db, "CREATE TABLE t8(a integer primary key, b)")
	mustExec(t, db, `INSERT INTO t8 VALUES(1, "pa1")`)
	mustExec(t, db, `INSERT INTO t8 VALUES(2, "pa4")`)
	mustExec(t, db, `INSERT INTO t8 VALUES(3, NULL)`)
	mustExec(t, db, `INSERT INTO t8 VALUES(4, NULL)`)
	mustExec(t, db, `INSERT INTO t8 VALUES(130, "pa31")`)
	mustExec(t, db, `INSERT INTO t8 VALUES(131, "pa30")`)

	// join-7.1: coalesce in LEFT JOIN
	t.Run("7.1 - coalesce in left join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT coalesce(t8.a,999) FROM t7 LEFT JOIN t8 ON y=a")
		want := []string{"1", "999", "999", "2", "131", "130", "999"}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join-8.*: LEFT JOIN with views
// ============================================================================

func TestJoin8Views(t *testing.T) {
	
	t.Run("left join with view", func(t *testing.T) {
		t.Skip("view support required")
	})
}

// ============================================================================
// join-9.*: LEFT JOIN with subqueries (EXCEPT)
// ============================================================================

func TestJoin9Subqueries(t *testing.T) {
	
	t.Run("left join subquery except", func(t *testing.T) {
		t.Skip("compound/subquery support required")
	})
}

// ============================================================================
// join-10.*: LEFT JOIN with empty subquery
// ============================================================================

func TestJoin10EmptySubquery(t *testing.T) {
	
	t.Run("left join empty subquery", func(t *testing.T) {
		t.Skip("subquery in FROM clause required")
	})
}

// ============================================================================
// join-11.*: Self-join tests
// ============================================================================

func TestJoin11SelfJoin(t *testing.T) {
	t.Skip("LEFT JOIN / advanced JOIN not fully working")
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b TEXT)")
	mustExec(t, db, "CREATE TABLE t2(a INTEGER PRIMARY KEY, b TEXT)")
	mustExec(t, db, "INSERT INTO t1 VALUES(1,'abc')")
	mustExec(t, db, "INSERT INTO t1 VALUES(2,'def')")
	mustExec(t, db, "INSERT INTO t2 VALUES(1,'abc')")
	mustExec(t, db, "INSERT INTO t2 VALUES(2,'def')")

	// join-11.1: NATURAL JOIN with identical tables
	t.Run("11.1 - natural join identical tables", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL JOIN t2")
		want := []string{"1", "abc", "2", "def"}
		assertResults(t, got, want)
	})

	// join-11.2: self-join with USING
	t.Run("11.2 - self join using(a)", func(t *testing.T) {
		got := queryStrings(t, db, "SELECT a FROM t1 JOIN t1 USING(a)")
		want := []string{"1", "2"}
		assertResults(t, got, want)
	})

	// join-11.3: self-join with alias USING
	t.Run("11.3 - self join alias using(a)", func(t *testing.T) {
		got := queryStrings(t, db, "SELECT a FROM t1 JOIN t1 AS t2 USING(a)")
		want := []string{"1", "2"}
		assertResults(t, got, want)
	})

	// join-11.3: self-join NATURAL with alias
	t.Run("11.3b - self natural join alias", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL JOIN t1 AS t2")
		want := []string{"1", "abc", "2", "def"}
		assertResults(t, got, want)
	})

	// join-11.4: self-join NATURAL without alias
	t.Run("11.4 - self natural join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL JOIN t1")
		want := []string{"1", "abc", "2", "def"}
		assertResults(t, got, want)
	})

	// join-11.5 through 11.10: COLLATE and type affinity tests
	t.Run("11.5 - collate nocase join setup", func(t *testing.T) {
		db.Exec("DROP TABLE IF EXISTS t1")
		db.Exec("DROP TABLE IF EXISTS t2")
		mustExec(t, db, "CREATE TABLE t1(a COLLATE nocase, b)")
		mustExec(t, db, "CREATE TABLE t2(a, b)")
		mustExec(t, db, "INSERT INTO t1 VALUES('ONE', 1)")
		mustExec(t, db, "INSERT INTO t1 VALUES('two', 2)")
		mustExec(t, db, "INSERT INTO t2 VALUES('one', 1)")
		mustExec(t, db, "INSERT INTO t2 VALUES('two', 2)")
	})

	// join-11.6: NATURAL JOIN with NOCASE collation
	t.Run("11.6 - natural join nocase t1->t2", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL JOIN t2")
		want := []string{"ONE", "1", "two", "2"}
		assertResults(t, got, want)
	})

	// join-11.7: reversed NATURAL JOIN with NOCASE
	t.Run("11.7 - natural join nocase t2->t1", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t2 NATURAL JOIN t1")
		// With NOCASE on t1.a, 'two' matches 'two' but 'one' doesn't match 'ONE' from t2 side
		// The expected result from the original test is just {two 2}
		want := []string{"two", "2"}
		assertResults(t, got, want)
	})

	// join-11.8: type affinity join setup
	t.Run("11.8 - type affinity join setup", func(t *testing.T) {
		db.Exec("DROP TABLE IF EXISTS t1")
		db.Exec("DROP TABLE IF EXISTS t2")
		mustExec(t, db, "CREATE TABLE t1(a, b TEXT)")
		mustExec(t, db, "CREATE TABLE t2(b INTEGER, a)")
		mustExec(t, db, "INSERT INTO t1 VALUES('one', '1.0')")
		mustExec(t, db, "INSERT INTO t1 VALUES('two', '2')")
		mustExec(t, db, "INSERT INTO t2 VALUES(1, 'one')")
		mustExec(t, db, "INSERT INTO t2 VALUES(2, 'two')")
	})

	// join-11.9: natural join with type affinity
	t.Run("11.9 - natural join type affinity t1->t2", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL JOIN t2")
		want := []string{"one", "1.0", "two", "2"}
		assertResults(t, got, want)
	})

	// join-11.10: reversed natural join with type affinity
	t.Run("11.10 - natural join type affinity t2->t1", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t2 NATURAL JOIN t1")
		want := []string{"1", "one", "2", "two"}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join-12.*: Max tables in join (error case)
// ============================================================================

func TestJoin12MaxTables(t *testing.T) {
	t.Skip("LEFT JOIN / advanced JOIN not fully working")
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t14(x)")
	mustExec(t, db, "INSERT INTO t14 VALUES('abcdefghij')")

	// join-12.2: 30 tables should work
	t.Run("12.2 - 30 tables in join", func(t *testing.T) {
		sql := "SELECT 1 FROM " + strings.Repeat("t14,", 29) + "t14"
		got := queryFlatStrings(t, db, sql)
		want := []string{"1"}
		assertResults(t, got, want)
	})

	// join-12.3: 63 tables should work
	t.Run("12.3 - 63 tables in join", func(t *testing.T) {
		sql := "SELECT 1 FROM " + strings.Repeat("t14,", 62) + "t14"
		got := queryFlatStrings(t, db, sql)
		want := []string{"1"}
		assertResults(t, got, want)
	})

	// join-12.4: 64 tables should work
	t.Run("12.4 - 64 tables in join", func(t *testing.T) {
		sql := "SELECT 1 FROM " + strings.Repeat("t14,", 63) + "t14"
		got := queryFlatStrings(t, db, sql)
		want := []string{"1"}
		assertResults(t, got, want)
	})

	// join-12.5: 65 tables should fail
	t.Run("12.5 - 65 tables too many", func(t *testing.T) {
		sql := "SELECT 1 FROM " + strings.Repeat("t14,", 64) + "t14"
		ok, _ := catchQuery(t, db, sql)
		if !ok {
			t.Error("expected error for 65 tables in join")
		}
	})

	// join-12.6: 66 tables should fail
	t.Run("12.6 - 66 tables too many", func(t *testing.T) {
		sql := "SELECT 1 FROM " + strings.Repeat("t14,", 65) + "t14"
		ok, _ := catchQuery(t, db, sql)
		if !ok {
			t.Error("expected error for 66 tables in join")
		}
	})

	// join-12.7: 127 tables should fail
	t.Run("12.7 - 127 tables too many", func(t *testing.T) {
		sql := "SELECT 1 FROM " + strings.Repeat("t14,", 126) + "t14"
		ok, _ := catchQuery(t, db, sql)
		if !ok {
			t.Error("expected error for 127 tables in join")
		}
	})

	// join-12.8: 128 tables should fail
	t.Run("12.8 - 128 tables too many", func(t *testing.T) {
		sql := "SELECT 1 FROM " + strings.Repeat("t14,", 127) + "t14"
		ok, _ := catchQuery(t, db, sql)
		if !ok {
			t.Error("expected error for 128 tables in join")
		}
	})
}

// ============================================================================
// join-13.*: LEFT JOIN with WHERE clause reordering
// ============================================================================

func TestJoin13WhereReorder(t *testing.T) {
	t.Skip("LEFT JOIN / advanced JOIN not fully working")
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE aa(a)")
	mustExec(t, db, "CREATE TABLE bb(b)")
	mustExec(t, db, "CREATE TABLE cc(c)")
	mustExec(t, db, "INSERT INTO aa VALUES(45)")
	mustExec(t, db, "INSERT INTO cc VALUES(45)")
	mustExec(t, db, "INSERT INTO cc VALUES(45)")

	// join-13.1: LEFT JOIN with WHERE reordering
	t.Run("13.1 - left join where reorder", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM aa LEFT JOIN bb, cc WHERE cc.c=aa.a")
		want := []string{"45", "", "45", "45", "", "45"}
		assertResults(t, got, want)
	})

	// join-13.2: LEFT JOIN with index on cc
	t.Run("13.2 - left join with index reorder", func(t *testing.T) {
		mustExec(t, db, "CREATE INDEX ccc ON cc(c)")
		got := queryFlatStrings(t, db, "SELECT * FROM aa LEFT JOIN bb, cc WHERE cc.c=aa.a")
		want := []string{"45", "", "45", "45", "", "45"}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join-14.*: Nested subquery LEFT JOINs
// ============================================================================

func TestJoin14NestedSubquery(t *testing.T) {
	t.Skip("LEFT JOIN / advanced JOIN not fully working")
	
	db := openTestDB(t)

	// join-14.1: simple nested subquery left join
	t.Run("14.1 - nested subquery left join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM (SELECT 1 a) AS x LEFT JOIN (SELECT 1, * FROM (SELECT * FROM (SELECT 1)))")
		want := []string{"1", "1", "1"}
		assertResults(t, got, want)
	})

	// join-14.2: deeper nesting
	t.Run("14.2 - deeper nested subquery", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM (SELECT 1 a) AS x LEFT JOIN (SELECT 1, * FROM (SELECT * FROM (SELECT * FROM (SELECT 1)))) AS y JOIN (SELECT * FROM (SELECT 9)) AS z")
		want := []string{"1", "1", "1", "9"}
		assertResults(t, got, want)
	})

	// join-14.3: expression in subquery
	t.Run("14.3 - expression in subquery", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM (SELECT 111) LEFT JOIN (SELECT cc+222, * FROM (SELECT * FROM (SELECT 333 cc)))")
		want := []string{"111", "555", "333"}
		assertResults(t, got, want)
	})

	// join-14.4 through 14.9: tests with actual tables and GROUP BY
	t.Run("14.4 - left join empty table group by", func(t *testing.T) {
		db.Exec("DROP TABLE IF EXISTS t1")
		mustExec(t, db, "CREATE TABLE t1(c PRIMARY KEY, a TEXT(10000), b TEXT(10000))")
		got := queryFlatStrings(t, db, "SELECT * FROM (SELECT 111) LEFT JOIN (SELECT c+222 FROM t1) GROUP BY 1")
		want := []string{"111", ""}
		assertResults(t, got, want)
	})

	t.Run("14.4b - left join empty table", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM (SELECT 111) LEFT JOIN (SELECT c+222 FROM t1)")
		want := []string{"111", ""}
		assertResults(t, got, want)
	})

	// join-14.10: view-based test (skip if views not supported)
	t.Run("14.10 - view left join where", func(t *testing.T) {
		t.Skip("view support required")
	})

	// join-14.20: subquery left join chain
	t.Run("14.20 - subquery left join chain", func(t *testing.T) {
		t.Skip("complex subquery left join chain")
	})
}

// ============================================================================
// join-15.*: LEFT JOIN WHERE clause with CASE
// ============================================================================

func TestJoin15Case(t *testing.T) {
	t.Skip("LEFT JOIN / advanced JOIN not fully working")
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t1(a INT, b INT)")
	mustExec(t, db, "INSERT INTO t1 VALUES(1,2)")
	mustExec(t, db, "INSERT INTO t1 VALUES(3,4)")
	mustExec(t, db, "CREATE TABLE t2(x INT, y INT)")

	// join-15.100: LEFT JOIN with CASE WHEN FALSE
	t.Run("15.100 - left join case when false", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT *, 'x' FROM t1 LEFT JOIN t2 WHERE CASE WHEN 0 THEN a=x ELSE 1 END")
		want := []string{"1", "2", "", "", "x", "3", "4", "", "", "x"}
		assertResults(t, got, want)
	})

	// join-15.105: LEFT JOIN with IN
	t.Run("15.105 - left join with IN", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT *, 'x' FROM t1 LEFT JOIN t2 WHERE a IN (1,3,x,y)")
		want := []string{"1", "2", "", "", "x", "3", "4", "", "", "x"}
		assertResults(t, got, want)
	})

	// join-15.106a: LEFT JOIN with NOT (AND)
	t.Run("15.106a - left join not and", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT *, 'x' FROM t1 LEFT JOIN t2 WHERE NOT ('x'='y' AND t2.y=1)")
		want := []string{"1", "2", "", "", "x", "3", "4", "", "", "x"}
		assertResults(t, got, want)
	})

	// join-15.106b: LEFT JOIN with ~ (AND)
	t.Run("15.106b - left join tilde and", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT *, 'x' FROM t1 LEFT JOIN t2 WHERE ~ ('x'='y' AND t2.y=1)")
		want := []string{"1", "2", "", "", "x", "3", "4", "", "", "x"}
		assertResults(t, got, want)
	})

	// join-15.107: LEFT JOIN with IS NOT
	t.Run("15.107 - left join is not", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT *, 'x' FROM t1 LEFT JOIN t2 WHERE t2.y IS NOT 'abc'")
		want := []string{"1", "2", "", "", "x", "3", "4", "", "", "x"}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join-16.*: LEFT JOIN ON 0
// ============================================================================

func TestJoin16OnZero(t *testing.T) {
	t.Skip("LEFT JOIN / advanced JOIN not fully working")
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t1(a INT)")
	mustExec(t, db, "INSERT INTO t1(a) VALUES(1)")
	mustExec(t, db, "CREATE TABLE t2(b INT)")

	// join-16.100: LEFT JOIN ON 0 with IS NOT NULL check
	t.Run("16.100 - left join on 0", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT a, b FROM t1 LEFT JOIN t2 ON 0 WHERE (b IS NOT NULL)=0")
		want := []string{"1", ""}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join-17.*: LEFT JOIN with constants
// ============================================================================

func TestJoin17Constants(t *testing.T) {
	t.Skip("LEFT JOIN / advanced JOIN not fully working")
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t1(x)")
	mustExec(t, db, "INSERT INTO t1(x) VALUES(0)")
	mustExec(t, db, "INSERT INTO t1(x) VALUES(1)")

	// join-17.100: LEFT JOIN with abs(1)
	t.Run("17.100 - left join with abs(1)", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 LEFT JOIN (SELECT abs(1) AS y FROM t1) ON x WHERE NOT(y='a')")
		want := []string{"1", "1", "1", "1"}
		assertResults(t, got, want)
	})

	// join-17.110: LEFT JOIN with abs(1)+2
	t.Run("17.110 - left join with abs(1)+2", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 LEFT JOIN (SELECT abs(1)+2 AS y FROM t1) ON x WHERE NOT(y='a')")
		want := []string{"1", "3", "1", "3"}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join-18.*: LEFT JOIN with views and NULL handling
// ============================================================================

func TestJoin18NullHandling(t *testing.T) {
	
	t.Run("left join view null handling", func(t *testing.T) {
		t.Skip("view support required")
	})
}

// ============================================================================
// join-19.*: LEFT JOIN with NULL checks
// ============================================================================

func TestJoin19NullChecks(t *testing.T) {
	t.Skip("LEFT JOIN / advanced JOIN not fully working")
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t1(a)")
	mustExec(t, db, "CREATE TABLE t2(b)")
	mustExec(t, db, "INSERT INTO t1(a) VALUES(0)")

	// join-19.2: basic LEFT JOIN
	t.Run("19.2 - basic left join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 LEFT JOIN t2")
		want := []string{"0", ""}
		assertResults(t, got, want)
	})

	// join-19.3: LEFT JOIN with IS NOT NULL check
	t.Run("19.3 - left join is not null check", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 LEFT JOIN t2 WHERE (b IS NOT NULL) IS NOT NULL")
		want := []string{"0", ""}
		assertResults(t, got, want)
	})

	// join-19.4: IS NOT NULL expression from left join
	t.Run("19.4 - is not null expression", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT (b IS NOT NULL) IS NOT NULL FROM t1 LEFT JOIN t2")
		want := []string{"1"}
		assertResults(t, got, want)
	})

	// join-19.5: compound IS NOT NULL check
	t.Run("19.5 - compound is not null", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 LEFT JOIN t2 WHERE (b IS NOT NULL AND b IS NOT NULL) IS NOT NULL")
		want := []string{"0", ""}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join-20.*: LEFT JOIN with partial index
// ============================================================================

func TestJoin20PartialIndex(t *testing.T) {
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t1(c1)")
	mustExec(t, db, "CREATE TABLE t0(c0)")
	mustExec(t, db, "INSERT INTO t0(c0) VALUES(0)")

	// join-20.1: LEFT JOIN with NULL IN
	t.Run("20.1 - left join null in", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t0 LEFT JOIN t1 WHERE NULL IN (c1)")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	// join-20.2: LEFT JOIN with partial index
	t.Run("20.2 - left join partial index", func(t *testing.T) {
		mustExec(t, db, "CREATE INDEX t1x ON t1(0) WHERE NULL IN (c1)")
		got := queryFlatStrings(t, db, "SELECT * FROM t0 LEFT JOIN t1 WHERE NULL IN (c1)")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})
}

// ============================================================================
// join-21.*: LEFT JOIN partial index with ISNULL
// ============================================================================

func TestJoin21PartialIndexIsNull(t *testing.T) {
	t.Skip("LEFT JOIN / advanced JOIN not fully working")
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t0(aa)")
	mustExec(t, db, "CREATE TABLE t1(bb)")
	mustExec(t, db, "INSERT INTO t0(aa) VALUES(1)")
	mustExec(t, db, "INSERT INTO t1(bb) VALUES(1)")

	// join-21.10: Various LEFT JOIN with ISNULL checks
	t.Run("21.10 - left join isnull checks", func(t *testing.T) {
		// Test the expected results: ON clause ISNULL matches, WHERE clause does not
		got := queryFlatStrings(t, db, "SELECT 13, * FROM t1 LEFT JOIN t0 WHERE aa ISNULL")
		want := []string{"13", "1", ""}
		assertResults(t, got, want)

		got = queryFlatStrings(t, db, "SELECT 14, * FROM t1 LEFT JOIN t0 WHERE +aa ISNULL")
		want = []string{"14", "1", ""}
		assertResults(t, got, want)

		got = queryFlatStrings(t, db, "SELECT 23, * FROM t1 LEFT JOIN t0 ON aa ISNULL")
		want = []string{"23", "1", ""}
		assertResults(t, got, want)

		got = queryFlatStrings(t, db, "SELECT 24, * FROM t1 LEFT JOIN t0 ON +aa ISNULL")
		want = []string{"24", "1", ""}
		assertResults(t, got, want)

		// With index
		mustExec(t, db, "CREATE INDEX i0 ON t0(aa) WHERE aa ISNULL")

		got = queryFlatStrings(t, db, "SELECT 23, * FROM t1 LEFT JOIN t0 ON aa ISNULL")
		want = []string{"23", "1", ""}
		assertResults(t, got, want)

		got = queryFlatStrings(t, db, "SELECT 24, * FROM t1 LEFT JOIN t0 ON +aa ISNULL")
		want = []string{"24", "1", ""}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join-22.*: LEFT JOIN with distinct
// ============================================================================

func TestJoin22Distinct(t *testing.T) {
	t.Skip("LEFT JOIN / advanced JOIN not fully working")
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t0(a, b)")
	mustExec(t, db, "CREATE INDEX t0a ON t0(a)")
	mustExec(t, db, "INSERT INTO t0 VALUES(10,10)")
	mustExec(t, db, "INSERT INTO t0 VALUES(10,11)")
	mustExec(t, db, "INSERT INTO t0 VALUES(10,12)")

	// join-22.10: DISTINCT with LEFT JOIN subquery
	t.Run("22.10 - distinct left join subquery", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT DISTINCT c FROM t0 LEFT JOIN (SELECT a+1 AS c FROM t0) ORDER BY c")
		want := []string{"11"}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join-24.*: JOIN with index
// ============================================================================

func TestJoin24Index(t *testing.T) {
	t.Skip("LEFT JOIN / advanced JOIN not fully working")
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t1(a PRIMARY KEY, x)")
	mustExec(t, db, "CREATE TABLE t2(b INT)")
	mustExec(t, db, "CREATE INDEX t1aa ON t1(a, a)")
	mustExec(t, db, "INSERT INTO t1 VALUES('abc', 'def')")
	mustExec(t, db, "INSERT INTO t2 VALUES(1)")

	// join-24.2: JOIN with WHERE
	t.Run("24.2 - join where match", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t2 JOIN t1 WHERE a='abc' AND x='def'")
		want := []string{"1", "abc", "def"}
		assertResults(t, got, want)
	})

	// join-24.3: JOIN with non-matching WHERE
	t.Run("24.3 - join where no match", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t2 JOIN t1 WHERE a='abc' AND x='abc'")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	// join-24.2b: LEFT JOIN ON 0 with OR
	t.Run("24.2b - left join on 0 or null", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t2 LEFT JOIN t1 ON a=0 WHERE (x='x' OR x IS NULL)")
		want := []string{"1", "", ""}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join-25.*: LEFT JOIN with view column and AND expression
// ============================================================================

func TestJoin25ViewAnd(t *testing.T) {
	
	t.Run("left join view and expression", func(t *testing.T) {
		t.Skip("view support required")
	})
}

// ============================================================================
// join-26.*: Parser issue with nested joins
// ============================================================================

func TestJoin26Parser(t *testing.T) {
	
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t4(a,b)")
	mustExec(t, db, "CREATE TABLE t5(a,c)")
	mustExec(t, db, "CREATE TABLE t6(a,d)")

	// join-26.1: nested join syntax error
	t.Run("26.1 - nested join parser", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM t5 JOIN ((t4 JOIN (t5 JOIN t6)) t7)")
		if !ok {
			t.Error("expected error for nested join syntax")
		}
	})
}

// ============================================================================
// join-27.*: Subquery flattening with LEFT JOIN
// ============================================================================

func TestJoin27Flattening(t *testing.T) {
	
	t.Run("subquery flattening left join", func(t *testing.T) {
		t.Skip("complex CTE/subquery flattening tests")
	})
}

// ============================================================================
// join-28.*: LEFT JOIN view performance regression
// ============================================================================

func TestJoin28ViewPerf(t *testing.T) {
	
	t.Run("left join view performance", func(t *testing.T) {
		t.Skip("view support required")
	})
}

// ============================================================================
// join-29.*: FULL OUTER JOIN (skip)
// ============================================================================

func TestJoin29FullOuter(t *testing.T) {
	
	t.Run("full outer join", func(t *testing.T) {
		t.Skip("FULL OUTER JOIN not supported")
	})
}

// ============================================================================
// join-30.*: RIGHT JOIN with omit-noop-join (skip)
// ============================================================================

func TestJoin30RightJoin(t *testing.T) {
	
	t.Run("right join omit noop", func(t *testing.T) {
		t.Skip("RIGHT JOIN not supported")
	})
}

// ============================================================================
// join-31.*: RIGHT JOIN with USING/NATURAL (skip)
// ============================================================================

func TestJoin31RightUsing(t *testing.T) {
	
	t.Run("right join using natural", func(t *testing.T) {
		t.Skip("RIGHT JOIN not supported")
	})
}

// ============================================================================
// join-32.*: RIGHT JOIN transitive constraint (skip)
// ============================================================================

func TestJoin32RightTransitive(t *testing.T) {
	
	t.Run("right join transitive constraint", func(t *testing.T) {
		t.Skip("RIGHT JOIN not supported")
	})
}

// ============================================================================
// join-33.*: Chained omit-noop-join (skip - uses views)
// ============================================================================

func TestJoin33ChainedOmit(t *testing.T) {
	
	t.Run("chained omit noop join", func(t *testing.T) {
		t.Skip("view support required")
	})
}

// Ensure sqlite import is used
var _ *sqlite.Database
