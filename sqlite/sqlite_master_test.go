package sqlite

import (
	"strings"
	"testing"
)

// --- sqlite_master query tests ---

func TestSqliteMasterSelectAll(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t1 (a INTEGER, b TEXT)")

	rs, err := db.Query("SELECT * FROM sqlite_master")
	if err != nil {
		t.Fatalf("SELECT from sqlite_master failed: %v", err)
	}
	defer rs.Close()

	count := 0
	for rs.Next() {
		count++
		row := rs.Row()
		if row.ColumnCount() != 5 {
			t.Errorf("expected 5 columns, got %d", row.ColumnCount())
		}
		tp := row.ColumnText(0)
		if tp != "table" {
			t.Errorf("expected type='table', got %q", tp)
		}
		name := row.ColumnText(1)
		if name != "t1" {
			t.Errorf("expected name='t1', got %q", name)
		}
		tblName := row.ColumnText(2)
		if tblName != "t1" {
			t.Errorf("expected tbl_name='t1', got %q", tblName)
		}
		sql := row.ColumnText(4)
		if !strings.Contains(strings.ToUpper(sql), "CREATE TABLE") {
			t.Errorf("expected sql to contain 'CREATE TABLE', got %q", sql)
		}
	}
	if count != 1 {
		t.Errorf("expected 1 row, got %d", count)
	}
}

func TestSqliteMasterSelectWhereType(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE users (id INTEGER, name TEXT)")
	db.Exec("CREATE TABLE orders (id INTEGER, user_id INTEGER)")
	db.Exec("CREATE VIEW v1 AS SELECT * FROM users")

	rs, err := db.Query("SELECT name, sql FROM sqlite_master WHERE type='table'")
	if err != nil {
		t.Fatalf("SELECT with WHERE failed: %v", err)
	}
	defer rs.Close()

	var names []string
	for rs.Next() {
		row := rs.Row()
		name := row.ColumnText(0)
		names = append(names, name)
		sql := row.ColumnText(1)
		if !strings.Contains(strings.ToUpper(sql), "CREATE TABLE") {
			t.Errorf("expected sql to contain 'CREATE TABLE', got %q", sql)
		}
	}
	if len(names) != 2 {
		t.Errorf("expected 2 tables, got %d: %v", len(names), names)
	}
}

func TestSqliteMasterCount(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t1 (a INTEGER)")
	db.Exec("CREATE TABLE t2 (b TEXT)")

	rs, err := db.Query("SELECT count(*) FROM sqlite_master")
	if err != nil {
		t.Fatalf("SELECT count(*) failed: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one row")
	}
	row := rs.Row()
	count := row.ColumnInt(0)
	if count != 2 {
		t.Errorf("expected count=2, got %d", count)
	}
}

func TestSqliteSchemaAlias(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t1 (a INTEGER)")

	rs, err := db.Query("SELECT * FROM sqlite_schema")
	if err != nil {
		t.Fatalf("SELECT from sqlite_schema failed: %v", err)
	}
	defer rs.Close()

	count := 0
	for rs.Next() {
		count++
	}
	if count != 1 {
		t.Errorf("expected 1 row from sqlite_schema, got %d", count)
	}
}

func TestSqliteMasterEmpty(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	rs, err := db.Query("SELECT * FROM sqlite_master")
	if err != nil {
		t.Fatalf("SELECT from empty sqlite_master failed: %v", err)
	}
	defer rs.Close()

	count := 0
	for rs.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 rows from empty sqlite_master, got %d", count)
	}
}

func TestSqliteMasterAfterDropTable(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t1 (a INTEGER)")
	db.Exec("CREATE TABLE t2 (b TEXT)")
	db.Exec("DROP TABLE t1")

	rs, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		t.Fatalf("SELECT after DROP TABLE failed: %v", err)
	}
	defer rs.Close()

	var names []string
	for rs.Next() {
		row := rs.Row()
		names = append(names, row.ColumnText(0))
	}
	if len(names) != 1 || names[0] != "t2" {
		t.Errorf("expected ['t2'], got %v", names)
	}
}

func TestDropTableIfExists(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	// DROP TABLE IF EXISTS on non-existent table should not error
	err = db.Exec("DROP TABLE IF EXISTS nonexistent")
	if err != nil {
		t.Errorf("DROP TABLE IF EXISTS on nonexistent table should not error: %v", err)
	}

	// DROP TABLE on non-existent table should error
	err = db.Exec("DROP TABLE nonexistent")
	if err == nil {
		t.Error("DROP TABLE on nonexistent table should error")
	}
}

func TestDropViewIfExists(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	// DROP VIEW IF EXISTS on non-existent view should not error
	err = db.Exec("DROP VIEW IF EXISTS nonexistent")
	if err != nil {
		t.Errorf("DROP VIEW IF EXISTS on nonexistent view should not error: %v", err)
	}
}

func TestCreateIndexSqliteMaster(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t1 (a INTEGER, b TEXT)")
	db.Exec("CREATE INDEX idx_b ON t1(b)")

	rs, err := db.Query("SELECT type, name, tbl_name FROM sqlite_master WHERE type='index'")
	if err != nil {
		t.Fatalf("SELECT indexes failed: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one index row")
	}
	row := rs.Row()
	tp := row.ColumnText(0)
	name := row.ColumnText(1)
	tblName := row.ColumnText(2)
	if tp != "index" {
		t.Errorf("expected type='index', got %q", tp)
	}
	if name != "idx_b" {
		t.Errorf("expected name='idx_b', got %q", name)
	}
	if tblName != "t1" {
		t.Errorf("expected tbl_name='t1', got %q", tblName)
	}
}

func TestCreateIndexIfNotExists(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t1 (a INTEGER)")
	db.Exec("CREATE INDEX idx_a ON t1(a)")

	// Should succeed without error
	err = db.Exec("CREATE INDEX IF NOT EXISTS idx_a ON t1(a)")
	if err != nil {
		t.Errorf("CREATE INDEX IF NOT EXISTS should not error: %v", err)
	}
}

func TestDropIndex(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t1 (a INTEGER, b TEXT)")
	db.Exec("CREATE INDEX idx_a ON t1(a)")
	db.Exec("DROP INDEX idx_a")

	rs, err := db.Query("SELECT count(*) FROM sqlite_master WHERE type='index'")
	if err != nil {
		t.Fatalf("SELECT count failed: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one row")
	}
	count := rs.Row().ColumnInt(0)
	if count != 0 {
		t.Errorf("expected 0 indexes after DROP INDEX, got %d", count)
	}
}

func TestDropIndexIfExists(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	err = db.Exec("DROP INDEX IF EXISTS nonexistent")
	if err != nil {
		t.Errorf("DROP INDEX IF EXISTS should not error: %v", err)
	}

	err = db.Exec("DROP INDEX nonexistent")
	if err == nil {
		t.Error("DROP INDEX on nonexistent should error")
	}
}

func TestSqliteMasterViewEntry(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t1 (a INTEGER)")
	db.Exec("CREATE VIEW v1 AS SELECT a FROM t1")

	rs, err := db.Query("SELECT type, name, tbl_name FROM sqlite_master WHERE type='view'")
	if err != nil {
		t.Fatalf("SELECT views failed: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one view row")
	}
	row := rs.Row()
	tp := row.ColumnText(0)
	name := row.ColumnText(1)
	tblName := row.ColumnText(2)
	if tp != "view" {
		t.Errorf("expected type='view', got %q", tp)
	}
	if name != "v1" {
		t.Errorf("expected name='v1', got %q", name)
	}
	if tblName != "v1" {
		t.Errorf("expected tbl_name='v1', got %q", tblName)
	}
}

func TestSqliteMasterDropViewRemoves(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t1 (a INTEGER)")
	db.Exec("CREATE VIEW v1 AS SELECT a FROM t1")
	db.Exec("DROP VIEW v1")

	rs, err := db.Query("SELECT count(*) FROM sqlite_master WHERE type='view'")
	if err != nil {
		t.Fatalf("SELECT count failed: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one row")
	}
	count := rs.Row().ColumnInt(0)
	if count != 0 {
		t.Errorf("expected 0 views after DROP VIEW, got %d", count)
	}
}

func TestSqliteMasterDropTableRemovesIndexes(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t1 (a INTEGER, b TEXT)")
	db.Exec("CREATE INDEX idx_a ON t1(a)")
	db.Exec("DROP TABLE t1")

	// Both the table entry and its index should be gone
	rs, err := db.Query("SELECT count(*) FROM sqlite_master")
	if err != nil {
		t.Fatalf("SELECT count failed: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one row")
	}
	count := rs.Row().ColumnInt(0)
	if count != 0 {
		t.Errorf("expected 0 entries after DROP TABLE, got %d", count)
	}
}

func TestSqliteMasterSelectNameWhereType(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	db.Exec("INSERT INTO users VALUES (1, 'alice')")

	// Verify the SELECT name FROM sqlite_master WHERE type='table' works correctly
	rs, err := db.Query("SELECT name FROM sqlite_master WHERE type='table'")
	if err != nil {
		t.Fatalf("SELECT name failed: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected at least one row")
	}
	name := rs.Row().ColumnText(0)
	if name != "users" {
		t.Errorf("expected name='users', got %q", name)
	}
	if rs.Next() {
		t.Error("expected only one row")
	}
}
