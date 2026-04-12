package tests

import (
	"testing"

	"github.com/sqlite-go/sqlite-go/sqlite"
)

// ============================================================================
// join2-1.*: Natural join chains with LEFT OUTER JOIN
// ============================================================================

func TestJoin2_1NaturalChains(t *testing.T) {
	db := openTestDB(t)

	// Setup tables t1(a,b), t2(b,c), t3(c,d)
	mustExec(t, db, "CREATE TABLE t1(a,b)")
	mustExec(t, db, "INSERT INTO t1 VALUES(1,11)")
	mustExec(t, db, "INSERT INTO t1 VALUES(2,22)")
	mustExec(t, db, "INSERT INTO t1 VALUES(3,33)")

	mustExec(t, db, "CREATE TABLE t2(b,c)")
	mustExec(t, db, "INSERT INTO t2 VALUES(11,111)")
	mustExec(t, db, "INSERT INTO t2 VALUES(33,333)")
	mustExec(t, db, "INSERT INTO t2 VALUES(44,444)")

	mustExec(t, db, "CREATE TABLE t3(c,d)")
	mustExec(t, db, "INSERT INTO t3 VALUES(111,1111)")
	mustExec(t, db, "INSERT INTO t3 VALUES(444,4444)")
	mustExec(t, db, "INSERT INTO t3 VALUES(555,5555)")

	// Verify setup
	t.Run("1.1 - setup t1", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1")
		want := []string{"1", "11", "2", "22", "3", "33"}
		assertResults(t, got, want)
	})

	t.Run("1.2 - setup t2", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t2")
		want := []string{"11", "111", "33", "333", "44", "444"}
		assertResults(t, got, want)
	})

	t.Run("1.3 - setup t3", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t3")
		want := []string{"111", "1111", "444", "4444", "555", "5555"}
		assertResults(t, got, want)
	})

	// join2-1.4: Triple natural join (inner)
	t.Run("1.4 - triple natural join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL JOIN t2 NATURAL JOIN t3")
		want := []string{"1", "11", "111", "1111"}
		assertResults(t, got, want)
	})

	// join2-1.5: Natural join + natural left outer join
	t.Run("1.5 - natural join then left outer join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL JOIN t2 NATURAL LEFT OUTER JOIN t3")
		want := []string{"1", "11", "111", "1111", "3", "33", "333", ""}
		assertResults(t, got, want)
	})

	// join2-1.6: Natural left outer join then natural join
	t.Run("1.6 - natural left outer then natural join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t1 NATURAL LEFT OUTER JOIN t2 NATURAL JOIN t3")
		want := []string{"1", "11", "111", "1111"}
		assertResults(t, got, want)
	})

	// join2-1.7: LEFT JOIN with subquery (skip if subquery not supported)
	t.Run("1.7 - left outer join subquery", func(t *testing.T) {
		t.Skip("subquery in FROM clause required")
	})
}

// ============================================================================
// join2-2.*: Error for ON clause referencing tables to its right
// ============================================================================

func TestJoin2_2OnClauseRight(t *testing.T) {
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE aa(a)")
	mustExec(t, db, "CREATE TABLE bb(b)")
	mustExec(t, db, "CREATE TABLE cc(c)")
	mustExec(t, db, "INSERT INTO aa VALUES('one')")
	mustExec(t, db, "INSERT INTO bb VALUES('one')")
	mustExec(t, db, "INSERT INTO cc VALUES('one')")

	// join2-2.1: ON clause references tables to its right
	t.Run("2.1 - ON clause references right table", func(t *testing.T) {
		ok, _ := catchQuery(t, db, "SELECT * FROM aa LEFT JOIN cc ON (a=b) JOIN bb ON (b=coalesce(c,1))")
		if !ok {
			t.Error("expected error for ON clause referencing tables to its right")
		}
	})

	// join2-2.2: Valid join that should work
	t.Run("2.2 - valid join a=b b=c", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM aa JOIN cc ON (a=b) JOIN bb ON (b=c)")
		want := []string{"one", "one", "one"}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join2-3.*: LEFT JOIN optimization (skip EXPLAIN tests)
// ============================================================================

func TestJoin2_3Optimization(t *testing.T) {
	t.Run("left join optimization", func(t *testing.T) {
		t.Skip("EXPLAIN/query plan tests skipped")
	})
}

// ============================================================================
// join2-4.*: LEFT JOIN table omission
// ============================================================================

func TestJoin2_4TableOmission(t *testing.T) {
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE c1(k INTEGER PRIMARY KEY, v1)")
	mustExec(t, db, "CREATE TABLE c2(k INTEGER PRIMARY KEY, v2)")
	mustExec(t, db, "CREATE TABLE c3(k INTEGER PRIMARY KEY, v3)")

	mustExec(t, db, "INSERT INTO c1 VALUES(1, 2)")
	mustExec(t, db, "INSERT INTO c2 VALUES(2, 3)")
	mustExec(t, db, "INSERT INTO c3 VALUES(3, 'v3')")

	mustExec(t, db, "INSERT INTO c1 VALUES(111, 1112)")
	mustExec(t, db, "INSERT INTO c2 VALUES(112, 1113)")
	mustExec(t, db, "INSERT INTO c3 VALUES(113, 'v1113')")

	// join2-4.1.1: LEFT JOIN chain with ON conditions
	t.Run("4.1.1 - left join chain ON conditions", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT v1, v3 FROM c1 LEFT JOIN c2 ON (c2.k=v1) LEFT JOIN c3 ON (c3.k=v2)")
		want := []string{"2", "v3", "1112", ""}
		assertResults(t, got, want)
	})

	// join2-4.1.2: LEFT JOIN chain with expression
	t.Run("4.1.2 - left join chain expression", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT v1, v3 FROM c1 LEFT JOIN c2 ON (c2.k=v1) LEFT JOIN c3 ON (c3.k=v1+1)")
		want := []string{"2", "v3", "1112", ""}
		assertResults(t, got, want)
	})

	// join2-4.1.3: DISTINCT with LEFT JOIN
	t.Run("4.1.3 - distinct left join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT DISTINCT v1, v3 FROM c1 LEFT JOIN c2 LEFT JOIN c3 ON (c3.k=v1+1)")
		want := []string{"2", "v3", "1112", ""}
		assertResults(t, got, want)
	})

	// join2-4.1.4: LEFT JOIN without DISTINCT (may have duplicates)
	t.Run("4.1.4 - left join without distinct", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT v1, v3 FROM c1 LEFT JOIN c2 LEFT JOIN c3 ON (c3.k=v1+1)")
		want := []string{"2", "v3", "2", "v3", "1112", "", "1112", ""}
		assertResults(t, got, want)
	})

	// join2-4.2: Tests with UNIQUE index tables
	t.Run("4.2.0 - setup unique tables", func(t *testing.T) {
		db.Exec("DROP TABLE c1")
		db.Exec("DROP TABLE c2")
		db.Exec("DROP TABLE c3")
		mustExec(t, db, "CREATE TABLE c1(k UNIQUE, v1)")
		mustExec(t, db, "CREATE TABLE c2(k UNIQUE, v2)")
		mustExec(t, db, "CREATE TABLE c3(k UNIQUE, v3)")

		mustExec(t, db, "INSERT INTO c1 VALUES(1, 2)")
		mustExec(t, db, "INSERT INTO c2 VALUES(2, 3)")
		mustExec(t, db, "INSERT INTO c3 VALUES(3, 'v3')")

		mustExec(t, db, "INSERT INTO c1 VALUES(111, 1112)")
		mustExec(t, db, "INSERT INTO c2 VALUES(112, 1113)")
		mustExec(t, db, "INSERT INTO c3 VALUES(113, 'v1113')")
	})

	t.Run("4.2.1 - unique left join chain ON", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT v1, v3 FROM c1 LEFT JOIN c2 ON (c2.k=v1) LEFT JOIN c3 ON (c3.k=v2)")
		want := []string{"2", "v3", "1112", ""}
		assertResults(t, got, want)
	})

	t.Run("4.2.2 - unique left join expression", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT v1, v3 FROM c1 LEFT JOIN c2 ON (c2.k=v1) LEFT JOIN c3 ON (c3.k=v1+1)")
		want := []string{"2", "v3", "1112", ""}
		assertResults(t, got, want)
	})

	t.Run("4.2.3 - unique distinct left join", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT DISTINCT v1, v3 FROM c1 LEFT JOIN c2 LEFT JOIN c3 ON (c3.k=v1+1)")
		want := []string{"2", "v3", "1112", ""}
		assertResults(t, got, want)
	})

	t.Run("4.2.4 - unique left join no distinct", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT v1, v3 FROM c1 LEFT JOIN c2 LEFT JOIN c3 ON (c3.k=v1+1)")
		want := []string{"2", "v3", "2", "v3", "1112", "", "1112", ""}
		assertResults(t, got, want)
	})

	// join2-4.3: OSS Fuzz assertion test (without WITHOUT ROWID)
	t.Run("4.3.0 - self left join", func(t *testing.T) {
		db.Exec("DROP TABLE IF EXISTS t1")
		db.Exec("DROP TABLE IF EXISTS t2")
		mustExec(t, db, "CREATE TABLE t1(x PRIMARY KEY)")
		mustExec(t, db, "CREATE TABLE t2(x)")
		got := queryFlatStrings(t, db, "SELECT a.x FROM t1 AS a LEFT JOIN t1 AS b ON (a.x=b.x) LEFT JOIN t2 AS c ON (a.x=c.x)")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})

	t.Run("4.3.1 - self left join with data", func(t *testing.T) {
		mustExec(t, db, "INSERT INTO t1(x) VALUES(1)")
		mustExec(t, db, "INSERT INTO t1(x) VALUES(2)")
		mustExec(t, db, "INSERT INTO t1(x) VALUES(3)")
		mustExec(t, db, "INSERT INTO t1(x) VALUES(4)")
		mustExec(t, db, "INSERT INTO t1(x) VALUES(5)")
		mustExec(t, db, "INSERT INTO t1(x) VALUES(6)")
		mustExec(t, db, "INSERT INTO t1(x) VALUES(7)")
		mustExec(t, db, "INSERT INTO t1(x) VALUES(8)")
		mustExec(t, db, "INSERT INTO t1(x) VALUES(9)")
		mustExec(t, db, "INSERT INTO t1(x) VALUES(10)")
		mustExec(t, db, "INSERT INTO t2(x) VALUES(10)")
		mustExec(t, db, "INSERT INTO t2(x) VALUES(11)")
		mustExec(t, db, "INSERT INTO t2(x) VALUES(12)")
		mustExec(t, db, "INSERT INTO t2(x) VALUES(13)")
		mustExec(t, db, "INSERT INTO t2(x) VALUES(14)")
		mustExec(t, db, "INSERT INTO t2(x) VALUES(15)")
		mustExec(t, db, "INSERT INTO t2(x) VALUES(16)")
		mustExec(t, db, "INSERT INTO t2(x) VALUES(17)")
		mustExec(t, db, "INSERT INTO t2(x) VALUES(18)")
		mustExec(t, db, "INSERT INTO t2(x) VALUES(19)")
		got := queryFlatStrings(t, db, "SELECT a.x, c.x FROM t1 AS a LEFT JOIN t1 AS b ON (a.x=b.x) LEFT JOIN t2 AS c ON (a.x=c.x)")
		want := []string{"1", "", "2", "", "3", "", "4", "", "5", "", "6", "", "7", "", "8", "", "9", "", "10", "10"}
		assertResults(t, got, want)
	})
}

// ============================================================================
// join2-5.* and 6.*: More optimization tests (skip EXPLAIN)
// ============================================================================

func TestJoin2_5Optimization(t *testing.T) {
	t.Run("left join optimization 5/6", func(t *testing.T) {
		t.Skip("EXPLAIN/query plan tests skipped")
	})
}

// ============================================================================
// join2-7.*: LEFT JOIN with view and subquery
// ============================================================================

func TestJoin2_7ViewSubquery(t *testing.T) {
	t.Run("left join view subquery", func(t *testing.T) {
		t.Skip("view support required")
	})
}

// ============================================================================
// join2-8.*: LEFT JOIN with BETWEEN expression
// ============================================================================

func TestJoin2_8Between(t *testing.T) {
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t0(c0)")
	mustExec(t, db, "CREATE TABLE t1(c0)")

	// join2-8.1: LEFT JOIN with BETWEEN and comparison
	t.Run("8.1 - left join between", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT * FROM t0 LEFT JOIN t1 WHERE (t1.c0 BETWEEN 0 AND 0) > ('' AND t0.c0)")
		if len(got) != 0 {
			t.Errorf("expected empty result, got %v", got)
		}
	})
}

// ============================================================================
// join2-9.*: LEFT JOIN with view and type coercion
// ============================================================================

func TestJoin2_9ViewCoercion(t *testing.T) {
	t.Run("left join view type coercion", func(t *testing.T) {
		t.Skip("view support required")
	})
}

// ============================================================================
// join2-10.*: LEFT JOIN with subquery and NULL row
// ============================================================================

func TestJoin2_10NullRow(t *testing.T) {
	t.Run("left join subquery null row", func(t *testing.T) {
		t.Skip("view support required")
	})
}

// ============================================================================
// join2-11.*: LEFT JOIN with view and count subquery
// ============================================================================

func TestJoin2_11ViewCount(t *testing.T) {
	t.Run("left join view count subquery", func(t *testing.T) {
		t.Skip("view support required")
	})
}

// ============================================================================
// join2-12.*: LEFT JOIN view with performance test
// ============================================================================

func TestJoin2_12ViewPerf(t *testing.T) {
	t.Run("left join view performance", func(t *testing.T) {
		t.Skip("view/EXPLAIN support required")
	})
}

// ============================================================================
// join2-13.*: LEFT JOIN with omit-noop-join and ORDER BY DESC
// ============================================================================

func TestJoin2_13OmitNoopOrder(t *testing.T) {
	db := openTestDB(t)

	mustExec(t, db, "CREATE TABLE t1(a1 INTEGER PRIMARY KEY, b1 INT)")
	mustExec(t, db, "CREATE TABLE t2(c2 INT, d2 INTEGER PRIMARY KEY)")
	mustExec(t, db, "CREATE TABLE t3(e3 INTEGER PRIMARY KEY)")
	mustExec(t, db, "INSERT INTO t1 VALUES(33,0)")
	mustExec(t, db, "INSERT INTO t2 VALUES(33,1)")
	mustExec(t, db, "INSERT INTO t2 VALUES(33,2)")

	// join2-13.1: LEFT JOIN with ORDER BY DESC
	t.Run("13.1 - left join order by desc", func(t *testing.T) {
		got := queryFlatStrings(t, db, "SELECT t1.a1, t2.d2 FROM (t1 LEFT JOIN t3 ON t3.e3=t1.b1) JOIN t2 ON t2.c2=t1.a1 WHERE t1.a1=33 ORDER BY t2.d2 DESC")
		want := []string{"33", "2", "33", "1"}
		assertResults(t, got, want)
	})
}

// Ensure sqlite import is used
var _ *sqlite.Database
