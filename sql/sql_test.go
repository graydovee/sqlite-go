package sql

import (
	"strings"
	"testing"

	"github.com/sqlite-go/sqlite-go/compile"
)

// helper to create a fresh engine for each test
func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := OpenEngine()
	if err != nil {
		t.Fatalf("OpenEngine: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

// helper to create a table with given columns
func createTestTable(t *testing.T, e *Engine, name, colDefs string) {
	t.Helper()
	sql := "CREATE TABLE " + name + " (" + colDefs + ")"
	if err := e.ExecSQL(sql); err != nil {
		t.Fatalf("create table %q: %v", name, err)
	}
}

// =============================================================================
// Engine basics
// =============================================================================

func TestOpenEngine(t *testing.T) {
	e := newTestEngine(t)
	if e == nil {
		t.Fatal("expected non-nil engine")
	}
	if e.PageSize() != 4096 {
		t.Errorf("PageSize() = %d, want 4096", e.PageSize())
	}
}

func TestCloseEngine(t *testing.T) {
	e, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	// Double close should be ok
	if err := e.Close(); err != nil {
		t.Errorf("double Close: %v", err)
	}
}

func TestExecSQLClosed(t *testing.T) {
	e, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	if err := e.ExecSQL("PRAGMA user_version"); !strings.Contains(err.Error(), "closed") {
		t.Errorf("expected closed error, got: %v", err)
	}
}

// =============================================================================
// CREATE TABLE + INSERT basics (needed for other tests)
// =============================================================================

func TestCreateTable(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "id INTEGER PRIMARY KEY, name TEXT, age INTEGER")

	ti, ok := e.GetTableInfo("t1")
	if !ok {
		t.Fatal("table t1 not found")
	}
	if len(ti.Columns) != 3 {
		t.Errorf("len(Columns) = %d, want 3", len(ti.Columns))
	}
	if ti.Columns[0].Name != "id" {
		t.Errorf("Columns[0].Name = %q, want id", ti.Columns[0].Name)
	}
	if ti.Columns[1].Name != "name" {
		t.Errorf("Columns[1].Name = %q, want name", ti.Columns[1].Name)
	}
}

func TestCreateTableIfNotExists(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "x INTEGER")
	// IF NOT EXISTS should succeed silently
	if err := e.ExecSQL("CREATE TABLE IF NOT EXISTS t1 (x INTEGER)"); err != nil {
		t.Errorf("CREATE TABLE IF NOT EXISTS: %v", err)
	}
}

func TestCreateTableDuplicate(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "x INTEGER")
	if err := e.ExecSQL("CREATE TABLE t1 (x INTEGER)"); err == nil {
		t.Error("expected error for duplicate table")
	}
}

func TestInsertBasic(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "id INTEGER, name TEXT")
	if err := e.ExecSQL("INSERT INTO t1 VALUES (1, 'alice')"); err != nil {
		t.Errorf("INSERT: %v", err)
	}
	if err := e.ExecSQL("INSERT INTO t1 (id, name) VALUES (2, 'bob')"); err != nil {
		t.Errorf("INSERT with columns: %v", err)
	}
}

// =============================================================================
// PRAGMA tests
// =============================================================================

func TestPragmaTableInfo(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "users", "id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT, age INTEGER DEFAULT 25")

	rows, err := e.ExecPragmaAndQuery("PRAGMA table_info(users)")
	if err != nil {
		t.Fatalf("PRAGMA table_info: %v", err)
	}

	if len(rows) != 4 {
		t.Fatalf("len(rows) = %d, want 4", len(rows))
	}

	// Check first column (id)
	checkRow(t, rows[0], 0, "id", "INTEGER", 0, nil, 1)

	// Check second column (name)
	checkRow(t, rows[1], 1, "name", "TEXT", 1, nil, 0)

	// Check fourth column (age) - should have default
	checkRow(t, rows[3], 3, "age", "INTEGER", 0, "25", 0)
}

func checkRow(t *testing.T, row PragmaRow, cid int, name, typ string, notnull int, dflt interface{}, pk int) {
	t.Helper()
	if row.Values[0] != cid {
		t.Errorf("cid = %v, want %d", row.Values[0], cid)
	}
	if row.Values[1] != name {
		t.Errorf("name = %v, want %q", row.Values[1], name)
	}
	if row.Values[2] != typ {
		t.Errorf("type = %v, want %q", row.Values[2], typ)
	}
	if row.Values[3] != notnull {
		t.Errorf("notnull = %v, want %d", row.Values[3], notnull)
	}
	if dflt == nil && row.Values[4] != nil {
		t.Errorf("dflt = %v, want nil", row.Values[4])
	}
	if row.Values[5] != pk {
		t.Errorf("pk = %v, want %d", row.Values[5], pk)
	}
}

func TestPragmaTableInfoNotFound(t *testing.T) {
	e := newTestEngine(t)
	_, err := e.ExecPragmaAndQuery("PRAGMA table_info(nonexistent)")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestPragmaDatabaseList(t *testing.T) {
	e := newTestEngine(t)
	rows, err := e.ExecPragmaAndQuery("PRAGMA database_list")
	if err != nil {
		t.Fatalf("PRAGMA database_list: %v", err)
	}
	if len(rows) < 1 {
		t.Fatal("expected at least one database")
	}
	if rows[0].Values[1] != "main" {
		t.Errorf("database name = %v, want main", rows[0].Values[1])
	}
}

func TestPragmaUserVersion(t *testing.T) {
	e := newTestEngine(t)

	// Read default
	rows, err := e.ExecPragmaAndQuery("PRAGMA user_version")
	if err != nil {
		t.Fatalf("PRAGMA user_version (get): %v", err)
	}
	if rows[0].Values[0] != 0 {
		t.Errorf("user_version = %v, want 0", rows[0].Values[0])
	}

	// Set
	if err := e.ExecSQL("PRAGMA user_version = 42"); err != nil {
		t.Fatalf("PRAGMA user_version = 42: %v", err)
	}
	if v := e.UserVersion(); v != 42 {
		t.Errorf("UserVersion() = %d, want 42", v)
	}

	// Read back
	rows, err = e.ExecPragmaAndQuery("PRAGMA user_version")
	if err != nil {
		t.Fatalf("PRAGMA user_version (get): %v", err)
	}
	if rows[0].Values[0] != 42 {
		t.Errorf("user_version = %v, want 42", rows[0].Values[0])
	}
}

func TestPragmaJournalMode(t *testing.T) {
	e := newTestEngine(t)

	// Read current
	rows, err := e.ExecPragmaAndQuery("PRAGMA journal_mode")
	if err != nil {
		t.Fatalf("PRAGMA journal_mode (get): %v", err)
	}
	mode := rows[0].Values[0].(string)
	if mode != "memory" {
		t.Errorf("journal_mode = %q, want memory", mode)
	}

	// Set to off
	if err := e.ExecSQL("PRAGMA journal_mode = off"); err != nil {
		t.Fatalf("PRAGMA journal_mode = off: %v", err)
	}
	if jm := e.JournalMode(); jm != "off" {
		t.Errorf("JournalMode() = %q, want off", jm)
	}

	// Set to wal
	if err := e.ExecSQL("PRAGMA journal_mode = wal"); err != nil {
		t.Fatalf("PRAGMA journal_mode = wal: %v", err)
	}
}

func TestPragmaSynchronous(t *testing.T) {
	e := newTestEngine(t)

	// Read default (should be 2 = FULL)
	rows, err := e.ExecPragmaAndQuery("PRAGMA synchronous")
	if err != nil {
		t.Fatalf("PRAGMA synchronous (get): %v", err)
	}
	if rows[0].Values[0] != 2 {
		t.Errorf("synchronous = %v, want 2", rows[0].Values[0])
	}

	// Set to OFF (0)
	if err := e.ExecSQL("PRAGMA synchronous = 0"); err != nil {
		t.Fatalf("PRAGMA synchronous = 0: %v", err)
	}
	if s := e.Synchronous(); s != 0 {
		t.Errorf("Synchronous() = %d, want 0", s)
	}

	// Set to NORMAL (1)
	if err := e.ExecSQL("PRAGMA synchronous = 1"); err != nil {
		t.Fatalf("PRAGMA synchronous = 1: %v", err)
	}
	rows, err = e.ExecPragmaAndQuery("PRAGMA synchronous")
	if err != nil {
		t.Fatalf("PRAGMA synchronous (get): %v", err)
	}
	if rows[0].Values[0] != 1 {
		t.Errorf("synchronous = %v, want 1", rows[0].Values[0])
	}

	// Invalid value
	if err := e.ExecSQL("PRAGMA synchronous = 5"); err == nil {
		t.Error("expected error for synchronous > 3")
	}
}

func TestPragmaForeignKeys(t *testing.T) {
	e := newTestEngine(t)

	// Read default (off)
	rows, err := e.ExecPragmaAndQuery("PRAGMA foreign_keys")
	if err != nil {
		t.Fatalf("PRAGMA foreign_keys (get): %v", err)
	}
	if rows[0].Values[0] != 0 {
		t.Errorf("foreign_keys = %v, want 0", rows[0].Values[0])
	}

	// Enable
	if err := e.ExecSQL("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("PRAGMA foreign_keys = ON: %v", err)
	}
	if !e.ForeignKeys() {
		t.Error("ForeignKeys() = false, want true")
	}

	// Disable
	if err := e.ExecSQL("PRAGMA foreign_keys = OFF"); err != nil {
		t.Fatalf("PRAGMA foreign_keys = OFF: %v", err)
	}
	if e.ForeignKeys() {
		t.Error("ForeignKeys() = true, want false")
	}

	rows, err = e.ExecPragmaAndQuery("PRAGMA foreign_keys")
	if err != nil {
		t.Fatalf("PRAGMA foreign_keys (get): %v", err)
	}
	if rows[0].Values[0] != 0 {
		t.Errorf("foreign_keys = %v, want 0", rows[0].Values[0])
	}
}

func TestPragmaCacheSize(t *testing.T) {
	e := newTestEngine(t)

	// Read default
	rows, err := e.ExecPragmaAndQuery("PRAGMA cache_size")
	if err != nil {
		t.Fatalf("PRAGMA cache_size (get): %v", err)
	}
	if rows[0].Values[0] != 2000 {
		t.Errorf("cache_size = %v, want 2000", rows[0].Values[0])
	}

	// Set
	if err := e.ExecSQL("PRAGMA cache_size = 5000"); err != nil {
		t.Fatalf("PRAGMA cache_size = 5000: %v", err)
	}
	if cs := e.CacheSize(); cs != 5000 {
		t.Errorf("CacheSize() = %d, want 5000", cs)
	}
}

func TestPragmaPageSize(t *testing.T) {
	e := newTestEngine(t)
	rows, err := e.ExecPragmaAndQuery("PRAGMA page_size")
	if err != nil {
		t.Fatalf("PRAGMA page_size: %v", err)
	}
	if rows[0].Values[0] != 4096 {
		t.Errorf("page_size = %v, want 4096", rows[0].Values[0])
	}
}

func TestPragmaIntegrityCheck(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "x INTEGER")

	rows, err := e.ExecPragmaAndQuery("PRAGMA integrity_check")
	if err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("expected at least one row")
	}
	if rows[0].Values[0] != "ok" {
		t.Errorf("integrity_check = %v, want ok", rows[0].Values[0])
	}
}

func TestPragmaCompileOptions(t *testing.T) {
	e := newTestEngine(t)
	rows, err := e.ExecPragmaAndQuery("PRAGMA compile_options")
	if err != nil {
		t.Fatalf("PRAGMA compile_options: %v", err)
	}
	if len(rows) == 0 {
		t.Fatal("expected compile options")
	}

	// Check that we get some expected options
	found := false
	for _, r := range rows {
		if r.Values[0] == "THREADSAFE=1" {
			found = true
		}
	}
	if !found {
		t.Error("expected THREADSAFE=1 in compile options")
	}
}

func TestPragmaUnknown(t *testing.T) {
	e := newTestEngine(t)
	err := e.ExecSQL("PRAGMA nonexistent_pragma")
	if err == nil {
		t.Error("expected error for unknown pragma")
	}
}

// =============================================================================
// ALTER TABLE tests
// =============================================================================

func TestAlterTableRenameTo(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "old_name", "id INTEGER, val TEXT")

	if err := e.ExecSQL("ALTER TABLE old_name RENAME TO new_name"); err != nil {
		t.Fatalf("ALTER TABLE RENAME TO: %v", err)
	}

	if _, ok := e.GetTableInfo("old_name"); ok {
		t.Error("old_name should not exist")
	}
	ti, ok := e.GetTableInfo("new_name")
	if !ok {
		t.Fatal("new_name should exist")
	}
	if ti.Name != "new_name" {
		t.Errorf("table name = %q, want new_name", ti.Name)
	}
}

func TestAlterTableRenameToDuplicate(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "x INTEGER")
	createTestTable(t, e, "t2", "y INTEGER")

	err := e.ExecSQL("ALTER TABLE t1 RENAME TO t2")
	if err == nil {
		t.Error("expected error renaming to existing table")
	}
}

func TestAlterTableAddColumn(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "id INTEGER, name TEXT")

	if err := e.ExecSQL("ALTER TABLE t1 ADD COLUMN email TEXT"); err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN: %v", err)
	}

	ti, ok := e.GetTableInfo("t1")
	if !ok {
		t.Fatal("table t1 not found")
	}
	if len(ti.Columns) != 3 {
		t.Fatalf("len(Columns) = %d, want 3", len(ti.Columns))
	}
	if ti.Columns[2].Name != "email" {
		t.Errorf("Columns[2].Name = %q, want email", ti.Columns[2].Name)
	}
	if ti.Columns[2].Type != "TEXT" {
		t.Errorf("Columns[2].Type = %q, want TEXT", ti.Columns[2].Type)
	}
}

func TestAlterTableAddColumnWithConstraints(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "id INTEGER")

	if err := e.ExecSQL("ALTER TABLE t1 ADD COLUMN status TEXT NOT NULL DEFAULT 'active'"); err != nil {
		t.Fatalf("ALTER TABLE ADD COLUMN with constraints: %v", err)
	}

	ti, _ := e.GetTableInfo("t1")
	col := ti.Columns[1]
	if col.Name != "status" {
		t.Errorf("Name = %q, want status", col.Name)
	}
	if !col.NotNull {
		t.Error("NotNull should be true")
	}
	if col.DefaultValue != "active" {
		t.Errorf("DefaultValue = %q, want active", col.DefaultValue)
	}
}

func TestAlterTableNotFound(t *testing.T) {
	e := newTestEngine(t)
	err := e.ExecSQL("ALTER TABLE nonexistent RENAME TO foo")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

// =============================================================================
// ANALYZE tests
// =============================================================================

func TestAnalyzeNoTarget(t *testing.T) {
	e := newTestEngine(t)
	if err := e.ExecSQL("ANALYZE"); err != nil {
		t.Errorf("ANALYZE: %v", err)
	}
}

func TestAnalyzeTable(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "x INTEGER")
	if err := e.ExecSQL("ANALYZE t1"); err != nil {
		t.Errorf("ANALYZE t1: %v", err)
	}
}

func TestAnalyzeSchemaTable(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "x INTEGER")
	if err := e.ExecSQL("ANALYZE main.t1"); err != nil {
		t.Errorf("ANALYZE main.t1: %v", err)
	}
}

func TestAnalyzeStat1TableCreated(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "x INTEGER")
	if err := e.ExecSQL("ANALYZE"); err != nil {
		t.Fatalf("ANALYZE: %v", err)
	}
	ti, ok := e.GetTableInfo("sqlite_stat1")
	if !ok {
		t.Fatal("sqlite_stat1 table should exist after ANALYZE")
	}
	if len(ti.Columns) != 3 {
		t.Errorf("sqlite_stat1 has %d columns, want 3", len(ti.Columns))
	}
	if ti.Columns[0].Name != "tbl" {
		t.Errorf("column 0 = %q, want tbl", ti.Columns[0].Name)
	}
}

func TestAnalyzeTableNotFound(t *testing.T) {
	e := newTestEngine(t)
	if err := e.ExecSQL("ANALYZE nonexistent"); err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestAnalyzeIdempotent(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "x INTEGER")
	if err := e.ExecSQL("ANALYZE"); err != nil {
		t.Fatalf("first ANALYZE: %v", err)
	}
	// Running ANALYZE again should succeed (clears and re-populates)
	if err := e.ExecSQL("ANALYZE"); err != nil {
		t.Fatalf("second ANALYZE: %v", err)
	}
}

func TestParseAnalyze(t *testing.T) {
	tests := []struct {
		sql    string
		target string
	}{
		{"ANALYZE", ""},
		{"ANALYZE t1", "t1"},
		{"ANALYZE main.t1", "main.t1"},
	}

	for _, tc := range tests {
		tokens := filterTokens(tokenize(tc.sql))
		stmt, err := ParseAnalyze(tokens)
		if err != nil {
			t.Errorf("ParseAnalyze(%q): %v", tc.sql, err)
			continue
		}
		if stmt.Target != tc.target {
			t.Errorf("ParseAnalyze(%q).Target = %q, want %q", tc.sql, stmt.Target, tc.target)
		}
	}
}

// =============================================================================
// ATTACH / DETACH tests
// =============================================================================

func TestAttachDatabase(t *testing.T) {
	e := newTestEngine(t)
	if err := e.ExecSQL("ATTACH DATABASE 'test.db' AS aux"); err != nil {
		t.Errorf("ATTACH: %v", err)
	}
}

func TestAttachReservedName(t *testing.T) {
	e := newTestEngine(t)
	if err := e.ExecSQL("ATTACH DATABASE 'test.db' AS main"); err == nil {
		t.Error("expected error attaching as 'main'")
	}
	if err := e.ExecSQL("ATTACH DATABASE 'test.db' AS temp"); err == nil {
		t.Error("expected error attaching as 'temp'")
	}
}

func TestDetachDatabase(t *testing.T) {
	e := newTestEngine(t)
	if err := e.ExecSQL("ATTACH DATABASE 'test.db' AS aux"); err != nil {
		t.Fatalf("ATTACH: %v", err)
	}
	if err := e.ExecSQL("DETACH DATABASE aux"); err != nil {
		t.Errorf("DETACH: %v", err)
	}
}

func TestDetachReservedName(t *testing.T) {
	e := newTestEngine(t)
	if err := e.ExecSQL("DETACH DATABASE main"); err == nil {
		t.Error("expected error detaching 'main'")
	}
}

func TestParseAttach(t *testing.T) {
	tokens := filterTokens(tokenize("ATTACH DATABASE 'test.db' AS mydb"))
	stmt, err := ParseAttach(tokens)
	if err != nil {
		t.Fatalf("ParseAttach: %v", err)
	}
	if stmt.Filename != "test.db" {
		t.Errorf("Filename = %q, want test.db", stmt.Filename)
	}
	if stmt.Schema != "mydb" {
		t.Errorf("Schema = %q, want mydb", stmt.Schema)
	}
}

func TestParseDetach(t *testing.T) {
	tokens := filterTokens(tokenize("DETACH DATABASE mydb"))
	stmt, err := ParseDetach(tokens)
	if err != nil {
		t.Fatalf("ParseDetach: %v", err)
	}
	if stmt.Schema != "mydb" {
		t.Errorf("Schema = %q, want mydb", stmt.Schema)
	}
}

// =============================================================================
// VACUUM tests
// =============================================================================

func TestVacuum(t *testing.T) {
	e := newTestEngine(t)
	if err := e.ExecSQL("VACUUM"); err != nil {
		t.Errorf("VACUUM: %v", err)
	}
}

func TestVacuumSchema(t *testing.T) {
	e := newTestEngine(t)
	if err := e.ExecSQL("VACUUM main"); err != nil {
		t.Errorf("VACUUM main: %v", err)
	}
}

func TestVacuumInto(t *testing.T) {
	e := newTestEngine(t)
	if err := e.ExecSQL("VACUUM INTO 'backup.db'"); err != nil {
		t.Errorf("VACUUM INTO: %v", err)
	}
}

func TestParseVacuum(t *testing.T) {
	tests := []struct {
		sql    string
		schema string
		into   string
	}{
		{"VACUUM", "", ""},
		{"VACUUM main", "main", ""},
		{"VACUUM INTO 'backup.db'", "", "backup.db"},
	}

	for _, tc := range tests {
		tokens := filterTokens(tokenize(tc.sql))
		stmt, err := ParseVacuum(tokens)
		if err != nil {
			t.Errorf("ParseVacuum(%q): %v", tc.sql, err)
			continue
		}
		if stmt.Schema != tc.schema {
			t.Errorf("ParseVacuum(%q).Schema = %q, want %q", tc.sql, stmt.Schema, tc.schema)
		}
		if stmt.Into != tc.into {
			t.Errorf("ParseVacuum(%q).Into = %q, want %q", tc.sql, stmt.Into, tc.into)
		}
	}
}

// =============================================================================
// TRIGGER tests
// =============================================================================

func TestCreateTriggerBasic(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "id INTEGER, val TEXT")

	sql := `CREATE TRIGGER trg1 AFTER INSERT ON t1 BEGIN SELECT 1; END`
	if err := e.ExecSQL(sql); err != nil {
		t.Errorf("CREATE TRIGGER: %v", err)
	}
}

func TestCreateTriggerBefore(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "id INTEGER")

	sql := `CREATE TRIGGER trg2 BEFORE DELETE ON t1 BEGIN SELECT 1; END`
	if err := e.ExecSQL(sql); err != nil {
		t.Errorf("CREATE TRIGGER BEFORE DELETE: %v", err)
	}
}

func TestCreateTriggerInsteadOf(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "id INTEGER")

	sql := `CREATE TRIGGER trg3 INSTEAD OF UPDATE ON t1 BEGIN SELECT 1; END`
	if err := e.ExecSQL(sql); err != nil {
		t.Errorf("CREATE TRIGGER INSTEAD OF UPDATE: %v", err)
	}
}

func TestCreateTriggerUpdateOf(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "id INTEGER, name TEXT, age INTEGER")

	sql := `CREATE TRIGGER trg4 AFTER UPDATE OF name, age ON t1 BEGIN SELECT 1; END`
	if err := e.ExecSQL(sql); err != nil {
		t.Errorf("CREATE TRIGGER UPDATE OF: %v", err)
	}
}

func TestCreateTriggerWhen(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "id INTEGER, val TEXT")

	sql := `CREATE TRIGGER trg5 AFTER INSERT ON t1 WHEN val > 10 BEGIN SELECT 1; END`
	if err := e.ExecSQL(sql); err != nil {
		t.Errorf("CREATE TRIGGER WHEN: %v", err)
	}
}

func TestCreateTriggerIfNotExists(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "id INTEGER")

	sql := `CREATE TRIGGER IF NOT EXISTS trg AFTER INSERT ON t1 BEGIN SELECT 1; END`
	if err := e.ExecSQL(sql); err != nil {
		t.Errorf("CREATE TRIGGER IF NOT EXISTS: %v", err)
	}
	// Second time should also succeed
	if err := e.ExecSQL(sql); err != nil {
		t.Errorf("CREATE TRIGGER IF NOT EXISTS (duplicate): %v", err)
	}
}

func TestCreateTriggerNoTable(t *testing.T) {
	e := newTestEngine(t)
	sql := `CREATE TRIGGER trg AFTER INSERT ON nonexistent BEGIN SELECT 1; END`
	if err := e.ExecSQL(sql); err == nil {
		t.Error("expected error for trigger on nonexistent table")
	}
}

func TestDropTrigger(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "t1", "id INTEGER")

	if err := e.ExecSQL(`CREATE TRIGGER trg AFTER INSERT ON t1 BEGIN SELECT 1; END`); err != nil {
		t.Fatalf("CREATE TRIGGER: %v", err)
	}
	if err := e.ExecSQL("DROP TRIGGER trg"); err != nil {
		t.Errorf("DROP TRIGGER: %v", err)
	}
}

func TestDropTriggerIfExists(t *testing.T) {
	e := newTestEngine(t)
	if err := e.ExecSQL("DROP TRIGGER IF EXISTS nonexistent"); err != nil {
		t.Errorf("DROP TRIGGER IF EXISTS: %v", err)
	}
}

func TestParseCreateTrigger(t *testing.T) {
	tokens := filterTokens(tokenize("CREATE TRIGGER my_trig AFTER INSERT ON t1 BEGIN SELECT 1; END"))
	stmt, err := ParseCreateTrigger(tokens)
	if err != nil {
		t.Fatalf("ParseCreateTrigger: %v", err)
	}
	if stmt.Name != "my_trig" {
		t.Errorf("Name = %q, want my_trig", stmt.Name)
	}
}

func TestParseDropTrigger(t *testing.T) {
	tokens := filterTokens(tokenize("DROP TRIGGER IF EXISTS my_trig"))
	stmt, err := ParseDropTrigger(tokens)
	if err != nil {
		t.Fatalf("ParseDropTrigger: %v", err)
	}
	if stmt.Name != "my_trig" {
		t.Errorf("Name = %q, want my_trig", stmt.Name)
	}
	if !stmt.IfExists {
		t.Error("IfExists should be true")
	}
}

// =============================================================================
// UPSERT tests
// =============================================================================

func TestParseUpsertDoNothing(t *testing.T) {
	tokens := filterTokens(tokenize("ON CONFLICT DO NOTHING"))
	upsert, pos, err := ParseUpsert(tokens, 0)
	if err != nil {
		t.Fatalf("ParseUpsert DO NOTHING: %v", err)
	}
	if upsert.DoUpdate {
		t.Error("DoUpdate should be false")
	}
	if pos != len(tokens) {
		t.Errorf("pos = %d, want %d", pos, len(tokens))
	}
}

func TestParseUpsertDoUpdate(t *testing.T) {
	tokens := filterTokens(tokenize("ON CONFLICT (id) DO UPDATE SET name = excluded.name"))
	upsert, pos, err := ParseUpsert(tokens, 0)
	if err != nil {
		t.Fatalf("ParseUpsert DO UPDATE: %v", err)
	}
	if !upsert.DoUpdate {
		t.Error("DoUpdate should be true")
	}
	if len(upsert.ConflictColumns) != 1 || upsert.ConflictColumns[0] != "id" {
		t.Errorf("ConflictColumns = %v, want [id]", upsert.ConflictColumns)
	}
	if len(upsert.SetClauses) != 1 {
		t.Fatalf("len(SetClauses) = %d, want 1", len(upsert.SetClauses))
	}
	if upsert.SetClauses[0].Column != "name" {
		t.Errorf("SetClauses[0].Column = %q, want name", upsert.SetClauses[0].Column)
	}
	if pos != len(tokens) {
		t.Errorf("pos = %d, want %d", pos, len(tokens))
	}
}

func TestParseUpsertOnConstraint(t *testing.T) {
	tokens := filterTokens(tokenize("ON CONFLICT ON CONSTRAINT idx_name DO NOTHING"))
	upsert, _, err := ParseUpsert(tokens, 0)
	if err != nil {
		t.Fatalf("ParseUpsert ON CONSTRAINT: %v", err)
	}
	if upsert.ConflictIndex != "idx_name" {
		t.Errorf("ConflictIndex = %q, want idx_name", upsert.ConflictIndex)
	}
}

func TestParseUpsertWithWhere(t *testing.T) {
	tokens := filterTokens(tokenize("ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE name IS NOT NULL"))
	upsert, _, err := ParseUpsert(tokens, 0)
	if err != nil {
		t.Fatalf("ParseUpsert with WHERE: %v", err)
	}
	if upsert.UpdateWhere == "" {
		t.Error("expected UpdateWhere to be set")
	}
}

func TestParseUpsertConflictWhere(t *testing.T) {
	tokens := filterTokens(tokenize("ON CONFLICT (id) WHERE active = 1 DO NOTHING"))
	upsert, _, err := ParseUpsert(tokens, 0)
	if err != nil {
		t.Fatalf("ParseUpsert with conflict WHERE: %v", err)
	}
	if upsert.ConflictWhere == "" {
		t.Error("expected ConflictWhere to be set")
	}
}

func TestParseUpsertMultipleSets(t *testing.T) {
	tokens := filterTokens(tokenize("ON CONFLICT (id) DO UPDATE SET name = excluded.name, age = excluded.age"))
	upsert, _, err := ParseUpsert(tokens, 0)
	if err != nil {
		t.Fatalf("ParseUpsert multiple SETs: %v", err)
	}
	if len(upsert.SetClauses) != 2 {
		t.Errorf("len(SetClauses) = %d, want 2", len(upsert.SetClauses))
	}
}

func TestUpsertSQLRendering(t *testing.T) {
	upsert := &UpsertClause{
		ConflictColumns: []string{"id"},
		DoUpdate:        true,
		SetClauses: []UpsertSet{
			{Column: "name", ValueExpr: "excluded.name"},
			{Column: "age", ValueExpr: "excluded.age"},
		},
	}
	sql := upsert.UpsertSQL()
	expected := "ON CONFLICT (id) DO UPDATE SET name = excluded.name, age = excluded.age"
	if sql != expected {
		t.Errorf("UpsertSQL() = %q, want %q", sql, expected)
	}
}

func TestUpsertSQLDoNothing(t *testing.T) {
	upsert := &UpsertClause{
		DoUpdate: false,
	}
	sql := upsert.UpsertSQL()
	expected := "ON CONFLICT DO NOTHING"
	if sql != expected {
		t.Errorf("UpsertSQL() = %q, want %q", sql, expected)
	}
}

// =============================================================================
// ALTER TABLE parse tests
// =============================================================================

func TestParseAlterRename(t *testing.T) {
	tokens := filterTokens(tokenize("ALTER TABLE old RENAME TO new"))
	stmt, err := ParseAlter(tokens)
	if err != nil {
		t.Fatalf("ParseAlter: %v", err)
	}
	if stmt.Type != AlterRename {
		t.Errorf("Type = %d, want AlterRename", stmt.Type)
	}
	if stmt.TableName != "old" {
		t.Errorf("TableName = %q, want old", stmt.TableName)
	}
	if stmt.NewName != "new" {
		t.Errorf("NewName = %q, want new", stmt.NewName)
	}
}

func TestParseAlterAddColumn(t *testing.T) {
	tokens := filterTokens(tokenize("ALTER TABLE t1 ADD COLUMN email TEXT"))
	stmt, err := ParseAlter(tokens)
	if err != nil {
		t.Fatalf("ParseAlter: %v", err)
	}
	if stmt.Type != AlterAddColumn {
		t.Errorf("Type = %d, want AlterAddColumn", stmt.Type)
	}
	if stmt.NewColumn.Name != "email" {
		t.Errorf("NewColumn.Name = %q, want email", stmt.NewColumn.Name)
	}
	if stmt.NewColumn.Type != "TEXT" {
		t.Errorf("NewColumn.Type = %q, want TEXT", stmt.NewColumn.Type)
	}
}

// =============================================================================
// Trigger helper function tests
// =============================================================================

func TestTriggerTimeString(t *testing.T) {
	tests := []struct {
		time TriggerTime
		want string
	}{
		{TriggerBefore, "BEFORE"},
		{TriggerAfter, "AFTER"},
		{TriggerInstead, "INSTEAD OF"},
	}
	for _, tc := range tests {
		if got := triggerTimeString(tc.time); got != tc.want {
			t.Errorf("triggerTimeString(%d) = %q, want %q", tc.time, got, tc.want)
		}
	}
}

func TestTriggerEventString(t *testing.T) {
	tests := []struct {
		event TriggerEvent
		want  string
	}{
		{TriggerDelete, "DELETE"},
		{TriggerInsert, "INSERT"},
		{TriggerUpdate, "UPDATE"},
		{TriggerUpdateOf, "UPDATE OF"},
	}
	for _, tc := range tests {
		if got := triggerEventString(tc.event); got != tc.want {
			t.Errorf("triggerEventString(%d) = %q, want %q", tc.event, got, tc.want)
		}
	}
}

// =============================================================================
// Table listing
// =============================================================================

func TestTableNames(t *testing.T) {
	e := newTestEngine(t)
	createTestTable(t, e, "alpha", "x INTEGER")
	createTestTable(t, e, "beta", "y TEXT")

	names := e.TableNames()
	if len(names) != 2 {
		t.Errorf("len(TableNames) = %d, want 2", len(names))
	}
}

// =============================================================================
// Journal mode helpers
// =============================================================================

func TestJournalModeConversion(t *testing.T) {
	tests := []struct {
		str  string
		want string
	}{
		{"delete", "delete"},
		{"memory", "memory"},
		{"wal", "wal"},
		{"off", "off"},
		{"persist", "persist"},
		{"truncate", "truncate"},
	}

	for _, tc := range tests {
		mode, err := journalModeFromString(tc.str)
		if err != nil {
			t.Errorf("journalModeFromString(%q): %v", tc.str, err)
			continue
		}
		got := journalModeString(mode)
		if got != tc.want {
			t.Errorf("roundtrip(%q) = %q, want %q", tc.str, got, tc.want)
		}
	}
}

func TestJournalModeInvalid(t *testing.T) {
	_, err := journalModeFromString("invalid_mode")
	if err == nil {
		t.Error("expected error for invalid journal mode")
	}
}

// =============================================================================
// Pragma via parenthesized form
// =============================================================================

func TestPragmaParenForm(t *testing.T) {
	e := newTestEngine(t)

	// Set via PRAGMA name(value) form
	if err := e.ExecSQL("PRAGMA user_version(99)"); err != nil {
		t.Fatalf("PRAGMA user_version(99): %v", err)
	}
	if v := e.UserVersion(); v != 99 {
		t.Errorf("UserVersion() = %d, want 99", v)
	}
}

// =============================================================================
// Tokenizer helper for tests
// =============================================================================

func tokenize(sql string) []compile.Token {
	return compile.Tokenize(sql)
}
