package tests

import (
	"testing"
)

func TestWindowRowNumberOrderBy(t *testing.T) {
	db := openTestDB(t)
	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")
	db.Exec("INSERT INTO t VALUES (3, 'Charlie')")
	db.Exec("INSERT INTO t VALUES (1, 'Alice')")
	db.Exec("INSERT INTO t VALUES (2, 'Bob')")

	rs, err := db.Query("SELECT id, row_number() OVER (ORDER BY id) AS rn FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestWindowDenseRank(t *testing.T) {
	db := openTestDB(t)
	db.Exec("CREATE TABLE items (grp TEXT, val INTEGER)")
	db.Exec("INSERT INTO items VALUES ('X', 10)")
	db.Exec("INSERT INTO items VALUES ('X', 20)")
	db.Exec("INSERT INTO items VALUES ('X', 20)")
	db.Exec("INSERT INTO items VALUES ('X', 30)")

	rs, err := db.Query("SELECT val, dense_rank() OVER (ORDER BY val) AS dr FROM items")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

func TestWindowRankPartitionBy(t *testing.T) {
	db := openTestDB(t)
	db.Exec("CREATE TABLE scores (category TEXT, score INTEGER)")
	db.Exec("INSERT INTO scores VALUES ('A', 90)")
	db.Exec("INSERT INTO scores VALUES ('A', 85)")
	db.Exec("INSERT INTO scores VALUES ('A', 90)")
	db.Exec("INSERT INTO scores VALUES ('B', 75)")
	db.Exec("INSERT INTO scores VALUES ('B', 80)")

	rs, err := db.Query("SELECT category, score, rank() OVER (PARTITION BY category ORDER BY score DESC) AS rnk FROM scores")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
}

func TestWindowLagLead(t *testing.T) {
	db := openTestDB(t)
	db.Exec("CREATE TABLE series (id INTEGER, val INTEGER)")
	db.Exec("INSERT INTO series VALUES (1, 10)")
	db.Exec("INSERT INTO series VALUES (2, 20)")
	db.Exec("INSERT INTO series VALUES (3, 30)")

	rs, err := db.Query("SELECT id, lag(val) OVER (ORDER BY id) AS prev, lead(val) OVER (ORDER BY id) AS nxt FROM series")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}

func TestWindowNtile(t *testing.T) {
	db := openTestDB(t)
	db.Exec("CREATE TABLE t (id INTEGER)")
	db.Exec("INSERT INTO t VALUES (1)")
	db.Exec("INSERT INTO t VALUES (2)")
	db.Exec("INSERT INTO t VALUES (3)")
	db.Exec("INSERT INTO t VALUES (4)")

	rs, err := db.Query("SELECT id, ntile(2) OVER (ORDER BY id) AS bucket FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

func TestWindowPartitionBy(t *testing.T) {
	db := openTestDB(t)
	db.Exec("CREATE TABLE t (dept TEXT, salary INTEGER)")
	db.Exec("INSERT INTO t VALUES ('Eng', 100)")
	db.Exec("INSERT INTO t VALUES ('Eng', 120)")
	db.Exec("INSERT INTO t VALUES ('Mkt', 80)")
	db.Exec("INSERT INTO t VALUES ('Mkt', 90)")

	rs, err := db.Query("SELECT dept, salary, row_number() OVER (PARTITION BY dept ORDER BY salary) AS rn FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
}

func TestWindowFirstLastValue(t *testing.T) {
	db := openTestDB(t)
	db.Exec("CREATE TABLE t (id INTEGER, val INTEGER)")
	db.Exec("INSERT INTO t VALUES (1, 100)")
	db.Exec("INSERT INTO t VALUES (2, 200)")
	db.Exec("INSERT INTO t VALUES (3, 300)")

	rs, err := db.Query("SELECT id, first_value(val) OVER (ORDER BY id) AS fv, last_value(val) OVER (ORDER BY id) AS lv FROM t")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
}
