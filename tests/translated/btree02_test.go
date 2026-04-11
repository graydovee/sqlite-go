package tests

import (
	"testing"
)

// ============================================================================
// btree02.test translations
//
// Original tests focus on multiple calls to saveCursorPosition() and
// restoreCursorPosition() when cursors have eState==CURSOR_SKIPNEXT.
// Uses WITHOUT ROWID tables, CROSS JOIN, random(), printf().
// ============================================================================

func TestBTree02(t *testing.T) {
	db := openTestDB(t)

	// btree02-100: Create WITHOUT ROWID table with composite PK,
	// insert data with random(), create index, cross join
	t.Run("btree02-100", func(t *testing.T) {
		t.Skip("WITHOUT ROWID tables not yet supported")

		// Original SQL:
		// CREATE TABLE t1(a TEXT, ax INTEGER, b INT, PRIMARY KEY(a,ax)) WITHOUT ROWID;
		// WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<10)
		//   INSERT INTO t1(a,ax,b) SELECT printf('%02x',i+160), random(), i FROM c;
		// CREATE INDEX t1a ON t1(a);
		// CREATE TABLE t2(x,y);
		// CREATE TABLE t3(cnt);
		// WITH RECURSIVE c(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM c WHERE i<4)
		//   INSERT INTO t3(cnt) SELECT i FROM c;
		// SELECT count(*) FROM t1;
		// Expected: 10
		_ = db
	})

	// btree02-110: Complex cursor save/restore test with interleaved
	// inserts and deletes in a CROSS JOIN loop
	t.Run("btree02-110", func(t *testing.T) {
		t.Skip("WITHOUT ROWID tables, CROSS JOIN not yet supported")

		// Original test performs a CROSS JOIN of t1 and t3, then for each row:
		// - If odd iteration: INSERT a new row into t1
		// - If even iteration: DELETE the current row from t1
		// Commits and begins a new transaction after each iteration.
		// Expected final count: 10
	})
}
