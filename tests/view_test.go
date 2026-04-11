package tests

import (
	"testing"

	"github.com/sqlite-go/sqlite-go/compile"
)

// ============================================================================
// Parser tests for CREATE VIEW / DROP VIEW
// ============================================================================

func TestParseCreateView(t *testing.T) {
	stmts, err := compile.Parse("CREATE VIEW v1 AS SELECT id, name FROM users")
	if err != nil {
		t.Fatalf("parse CREATE VIEW: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	s := stmts[0]
	if s.Type != compile.StmtCreateView {
		t.Fatalf("expected StmtCreateView, got %v", s.Type)
	}
	if s.CreateView == nil {
		t.Fatal("CreateView is nil")
	}
	if s.CreateView.Name != "v1" {
		t.Errorf("view name: got %q, want %q", s.CreateView.Name, "v1")
	}
	if s.CreateView.Select == nil {
		t.Fatal("view select is nil")
	}
	if len(s.CreateView.Select.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(s.CreateView.Select.Columns))
	}
}

func TestParseCreateViewIfNotExists(t *testing.T) {
	stmts, err := compile.Parse("CREATE VIEW IF NOT EXISTS v1 AS SELECT * FROM t")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	s := stmts[0]
	if !s.CreateView.IfNotExists {
		t.Error("expected IfNotExists=true")
	}
}

func TestParseCreateViewWithWhere(t *testing.T) {
	stmts, err := compile.Parse("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	s := stmts[0]
	if s.CreateView.Name != "active_users" {
		t.Errorf("name: got %q", s.CreateView.Name)
	}
	if s.CreateView.Select.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestParseDropView(t *testing.T) {
	stmts, err := compile.Parse("DROP VIEW v1")
	if err != nil {
		t.Fatalf("parse DROP VIEW: %v", err)
	}
	s := stmts[0]
	if s.Type != compile.StmtDropView {
		t.Fatalf("expected StmtDropView, got %v", s.Type)
	}
	if s.DropView == nil {
		t.Fatal("DropView is nil")
	}
	if s.DropView.Name != "v1" {
		t.Errorf("view name: got %q, want %q", s.DropView.Name, "v1")
	}
}

func TestParseDropViewIfExists(t *testing.T) {
	stmts, err := compile.Parse("DROP VIEW IF EXISTS v1")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	s := stmts[0]
	if !s.DropView.IfExists {
		t.Error("expected IfExists=true")
	}
}

// ============================================================================
// Integration tests for VIEW execution
// ============================================================================

func TestCreateViewBasic(t *testing.T) {
	db := openTestDB(t)

	// Create a table with data
	db.Exec("CREATE TABLE users (id INTEGER, name TEXT, active INTEGER)")
	db.Exec("INSERT INTO users VALUES (1, 'Alice', 1)")
	db.Exec("INSERT INTO users VALUES (2, 'Bob', 0)")
	db.Exec("INSERT INTO users VALUES (3, 'Carol', 1)")

	// Create a view
	err := db.Exec("CREATE VIEW active_users AS SELECT id, name FROM users WHERE active = 1")
	if err != nil {
		t.Fatalf("CREATE VIEW: %v", err)
	}

	// Query the view
	rs, err := db.Query("SELECT id, name FROM active_users")
	if err != nil {
		t.Fatalf("SELECT from view: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0]["id"] != int64(1) {
		t.Errorf("row 0 id: got %v, want 1", rows[0]["id"])
	}
	if rows[1]["id"] != int64(3) {
		t.Errorf("row 1 id: got %v, want 3", rows[1]["id"])
	}
}

func TestCreateViewSelectAll(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (a INTEGER, b TEXT)")
	db.Exec("INSERT INTO t VALUES (10, 'hello')")
	db.Exec("INSERT INTO t VALUES (20, 'world')")

	err := db.Exec("CREATE VIEW v AS SELECT * FROM t")
	if err != nil {
		t.Fatalf("CREATE VIEW: %v", err)
	}

	rs, err := db.Query("SELECT a, b FROM v")
	if err != nil {
		t.Fatalf("SELECT from view: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}

func TestCreateViewIfNotExists(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (x INTEGER)")
	db.Exec("INSERT INTO t VALUES (1)")

	err := db.Exec("CREATE VIEW v AS SELECT * FROM t")
	if err != nil {
		t.Fatalf("CREATE VIEW: %v", err)
	}

	// Creating again should fail
	err = db.Exec("CREATE VIEW v AS SELECT * FROM t")
	if err == nil {
		t.Error("expected error for duplicate view")
	}

	// IF NOT EXISTS should succeed silently
	err = db.Exec("CREATE VIEW IF NOT EXISTS v AS SELECT * FROM t")
	if err != nil {
		t.Errorf("CREATE VIEW IF NOT EXISTS: %v", err)
	}
}

func TestDropView(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (x INTEGER)")
	db.Exec("INSERT INTO t VALUES (42)")

	db.Exec("CREATE VIEW v AS SELECT * FROM t")

	// Drop the view
	err := db.Exec("DROP VIEW v")
	if err != nil {
		t.Fatalf("DROP VIEW: %v", err)
	}

	// Querying should now fail
	_, err = db.Query("SELECT x FROM v")
	if err == nil {
		t.Error("expected error querying dropped view")
	}
}

func TestDropViewIfExists(t *testing.T) {
	db := openTestDB(t)

	// DROP VIEW IF EXISTS on nonexistent view should succeed
	err := db.Exec("DROP VIEW IF EXISTS nonexistent")
	if err != nil {
		t.Errorf("DROP VIEW IF EXISTS: %v", err)
	}

	// DROP VIEW without IF EXISTS on nonexistent view should fail
	err = db.Exec("DROP VIEW nonexistent")
	if err == nil {
		t.Error("expected error dropping nonexistent view")
	}
}

func TestViewWithAggregate(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE scores (name TEXT, score INTEGER)")
	db.Exec("INSERT INTO scores VALUES ('Alice', 90)")
	db.Exec("INSERT INTO scores VALUES ('Alice', 85)")
	db.Exec("INSERT INTO scores VALUES ('Bob', 95)")
	db.Exec("INSERT INTO scores VALUES ('Bob', 80)")

	err := db.Exec("CREATE VIEW total_score AS SELECT sum(score) AS total FROM scores")
	if err != nil {
		t.Fatalf("CREATE VIEW: %v", err)
	}

	rs, err := db.Query("SELECT total FROM total_score")
	if err != nil {
		t.Fatalf("SELECT from view: %v", err)
	}
	defer rs.Close()

	rows := collectRows(t, rs)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (aggregate), got %d", len(rows))
	}
	total, ok := rows[0]["total"].(int64)
	if !ok {
		t.Fatalf("total is not int64: %v", rows[0]["total"])
	}
	if total != 350 {
		t.Errorf("total: got %d, want 350", total)
	}
}

func TestCreateViewBadSelect(t *testing.T) {
	db := openTestDB(t)

	// Referencing a nonexistent table should fail
	err := db.Exec("CREATE VIEW v AS SELECT * FROM nonexistent")
	if err == nil {
		t.Error("expected error referencing nonexistent table in view")
	}
}

func TestCreateViewChainedDrop(t *testing.T) {
	db := openTestDB(t)

	db.Exec("CREATE TABLE t (id INTEGER, val TEXT)")
	db.Exec("INSERT INTO t VALUES (1, 'a')")
	db.Exec("INSERT INTO t VALUES (2, 'b')")

	// Create view, query it, drop it, recreate it
	db.Exec("CREATE VIEW v1 AS SELECT id FROM t")

	rs, _ := db.Query("SELECT id FROM v1")
	rows := collectRows(t, rs)
	rs.Close()
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows from v1, got %d", len(rows))
	}

	db.Exec("DROP VIEW v1")

	db.Exec("CREATE VIEW v1 AS SELECT val FROM t")

	rs, _ = db.Query("SELECT val FROM v1")
	rows = collectRows(t, rs)
	rs.Close()
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows from recreated v1, got %d", len(rows))
	}
	if rows[0]["val"] != "a" {
		t.Errorf("row 0 val: got %v, want 'a'", rows[0]["val"])
	}
}
