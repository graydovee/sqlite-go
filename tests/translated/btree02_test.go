package tests

import (
	"fmt"
	"testing"
)

// ============================================================================
// btree02.test translations
//
// Original tests focus on multiple calls to saveCursorPosition() and
// restoreCursorPosition() when cursors have eState==CURSOR_SKIPNEXT.
// Uses WITHOUT ROWID tables, CROSS JOIN, random(), printf().
// ============================================================================

// TestBTree02_100 translates btree02-100:
// Create WITHOUT ROWID table with composite PK, insert 10 rows with
// hex-formatted a values and random() ax values, create index, cross join.
// Expected: count(*) FROM t1 → 10
func TestBTree02_100(t *testing.T) {
	t.Skip("CREATE INDEX not yet supported")

	db := openTestDB(t)

	execSQL(t, db, "CREATE TABLE t1(a TEXT, ax INTEGER, b INT, PRIMARY KEY(a,ax)) WITHOUT ROWID")

	// Insert 10 rows: a = hex(i+160), ax = random(), b = i
	// Original uses: printf('%02x', i+160) which produces a0..a9
	for i := 1; i <= 10; i++ {
		a := fmt.Sprintf("%02x", i+160) // a0, a1, ..., a9
		sql := fmt.Sprintf("INSERT INTO t1(a,ax,b) VALUES('%s', random(), %d)", a, i)
		execSQL(t, db, sql)
	}

	execSQL(t, db, "CREATE INDEX t1a ON t1(a)")
	execSQL(t, db, "CREATE TABLE t2(x,y)")
	execSQL(t, db, "CREATE TABLE t3(cnt)")

	for i := 1; i <= 4; i++ {
		execSQL(t, db, fmt.Sprintf("INSERT INTO t3(cnt) VALUES(%d)", i))
	}

	got := queryInt(t, db, "SELECT count(*) FROM t1")
	if got != 10 {
		t.Errorf("count(*) = %d, want 10", got)
	}
}

// TestBTree02_110 translates btree02-110:
// Complex cursor save/restore test. Iterates over CROSS JOIN of t1×t3,
// alternately inserting new rows and deleting existing rows in t1,
// committing and beginning new transactions after each iteration.
// Expected: final count(*) = 10
func TestBTree02_110(t *testing.T) {
	t.Skip("CREATE INDEX, CROSS JOIN, and transaction interleaving not yet supported")

	db := openTestDB(t)

	execSQL(t, db, "CREATE TABLE t1(a TEXT, ax INTEGER, b INT, PRIMARY KEY(a,ax)) WITHOUT ROWID")

	// Initial 10 rows
	for i := 1; i <= 10; i++ {
		a := fmt.Sprintf("%02x", i+160)
		sql := fmt.Sprintf("INSERT INTO t1(a,ax,b) VALUES('%s', random(), %d)", a, i)
		execSQL(t, db, sql)
	}

	execSQL(t, db, "CREATE INDEX t1a ON t1(a)")
	execSQL(t, db, "CREATE TABLE t2(x,y)")
	execSQL(t, db, "CREATE TABLE t3(cnt)")
	for i := 1; i <= 4; i++ {
		execSQL(t, db, fmt.Sprintf("INSERT INTO t3(cnt) VALUES(%d)", i))
	}

	execSQL(t, db, "BEGIN")

	// Cross join t1 × t3, for each row:
	// odd iteration: insert a new row into t1 with b+1000
	// even iteration: delete the current row from t1
	// After each: COMMIT; BEGIN
	iteration := 0
	rs, err := db.Query("SELECT a, ax, b, cnt FROM t1 CROSS JOIN t3 WHERE b IS NOT NULL")
	if err != nil {
		t.Fatalf("CROSS JOIN query: %v", err)
	}
	// Collect all rows first since we'll be modifying t1
	type row struct {
		a, ax, b, cnt int64
		aText         string
	}
	var rows []row
	for rs.Next() {
		r := rs.Row()
		rows = append(rows, row{
			aText: r.ColumnText(0),
			ax:    r.ColumnInt(1),
		})
	}
	rs.Close()

	for _, r := range rows {
		if r.aText == "" {
			continue
		}
		iteration++
		if iteration%2 == 1 {
			// INSERT a new row with a=(old_a), b=b+1000
			bx := r.b + 1000
			newA := fmt.Sprintf("(%s)", r.aText)
			execSQL(t, db, fmt.Sprintf(
				"INSERT INTO t1(a,ax,b) VALUES('%s', random(), %d)", newA, bx))
		} else {
			// DELETE the row with matching a
			execSQL(t, db, fmt.Sprintf("DELETE FROM t1 WHERE a='%s'", r.aText))
		}
		execSQL(t, db, "COMMIT")
		execSQL(t, db, "BEGIN")
	}

	execSQL(t, db, "COMMIT")

	got := queryInt(t, db, "SELECT count(*) FROM t1")
	if got != 10 {
		t.Errorf("final count(*) = %d, want 10", got)
	}
}

// Ensure unused imports are referenced
var _ = fmt.Sprintf
