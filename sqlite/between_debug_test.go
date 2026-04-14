package sqlite

import (
	"fmt"
	"testing"
)

func TestDeleteWhereBetweenLarge(t *testing.T) {
	for _, n := range []int{600, 700, 800, 900, 1000} {
		db, _ := Open(":memory:", OpenReadWrite|OpenCreate|OpenMemory)
		db.Exec("CREATE TABLE t1(f1 int, f2 int)")
		for i := 0; i < n; i++ {
			db.Exec("INSERT INTO t1 VALUES(?, ?)", i, i*2)
		}

		lo := n / 3
		hi := 2 * n / 3
		expectRemaining := n - (hi - lo + 1)

		// Use inline SQL instead of parameters
		sql := fmt.Sprintf("DELETE FROM t1 WHERE f1 BETWEEN %d AND %d", lo, hi)
		err := db.Exec(sql)
		if err != nil {
			t.Fatalf("n=%d: DELETE failed: %v", n, err)
		}

		rs, _ := db.Query("SELECT count(*) FROM t1")
		if !rs.Next() {
			t.Fatalf("n=%d: no rows", n)
		}
		count := rs.Row().ColumnInt(0)
		rs.Close()
		if count != int64(expectRemaining) {
			t.Errorf("n=%d: expected %d remaining, got %d", n, expectRemaining, count)
		} else {
			t.Logf("n=%d: OK (%d remaining)", n, count)
		}
		db.Close()
	}
}
