package sqlite

import (
	"testing"
)

// --- Error handling tests ---

func TestErrorCodeString(t *testing.T) {
	tests := []struct {
		code ErrorCode
		want string
	}{
		{OK, "SQLITE_OK"},
		{Error, "SQLITE_ERROR"},
		{Busy, "SQLITE_BUSY"},
		{IOError, "SQLITE_IOERR"},
		{Corrupt, "SQLITE_CORRUPT"},
		{Constraint, "SQLITE_CONSTRAINT"},
		{ErrorCode(9999), "SQLITE_UNKNOWN(9999)"},
	}
	for _, tt := range tests {
		got := ErrorCodeString(tt.code)
		if got != tt.want {
			t.Errorf("ErrorCodeString(%v) = %q, want %q", tt.code, got, tt.want)
		}
	}
}

func TestIsError(t *testing.T) {
	if IsError(OK) {
		t.Error("IsError(OK) should be false")
	}
	if !IsError(Error) {
		t.Error("IsError(Error) should be true")
	}
	if !IsError(Busy) {
		t.Error("IsError(Busy) should be true")
	}
}

func TestIsBusy(t *testing.T) {
	if IsBusy(OK) {
		t.Error("IsBusy(OK) should be false")
	}
	if !IsBusy(Busy) {
		t.Error("IsBusy(Busy) should be true")
	}
	if !IsBusy(BusyRecovery) {
		t.Error("IsBusy(BusyRecovery) should be true")
	}
	if !IsBusy(Locked) {
		t.Error("IsBusy(Locked) should be true")
	}
}

func TestIsIOError(t *testing.T) {
	if IsIOError(OK) {
		t.Error("IsIOError(OK) should be false")
	}
	if !IsIOError(IOError) {
		t.Error("IsIOError(IOError) should be true")
	}
	if !IsIOError(IOErrorRead) {
		t.Error("IsIOError(IOErrorRead) should be true")
	}
	if !IsIOError(IOErrorWrite) {
		t.Error("IsIOError(IOErrorWrite) should be true")
	}
}

func TestIsConstraint(t *testing.T) {
	if IsConstraint(OK) {
		t.Error("IsConstraint(OK) should be false")
	}
	if !IsConstraint(Constraint) {
		t.Error("IsConstraint(Constraint) should be true")
	}
	if !IsConstraint(ConstraintPrimaryKey) {
		t.Error("IsConstraint(ConstraintPrimaryKey) should be true")
	}
	if !IsConstraint(ConstraintUnique) {
		t.Error("IsConstraint(ConstraintUnique) should be true")
	}
}

func TestNewError(t *testing.T) {
	err := NewError(Error, "test error")
	if err.Code != Error {
		t.Errorf("expected code %v, got %v", Error, err.Code)
	}
	if err.ExtCode != Error {
		t.Errorf("expected extcode %v, got %v", Error, err.ExtCode)
	}
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
}

func TestNewErrorf(t *testing.T) {
	err := NewErrorf(Busy, "table %s is locked", "users")
	if err.Code != Busy {
		t.Errorf("expected code %v, got %v", Busy, err.Code)
	}
	wantMsg := "table users is locked"
	if err.Error() == "" {
		t.Error("expected non-empty error message")
	}
	// Message should contain our formatted text
	if !contains(err.Error(), wantMsg) {
		t.Errorf("expected error message to contain %q, got %q", wantMsg, err.Error())
	}
}

// --- Configuration tests ---

func TestConfigInit(t *testing.T) {
	if err := Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if !IsInitialized() {
		t.Error("expected initialized after Initialize()")
	}
}

func TestConfigure(t *testing.T) {
	prev, err := Configure(ConfigSerialized)
	if err != nil {
		t.Fatalf("Configure failed: %v", err)
	}
	_ = prev

	_, err = Configure(ConfigOption(9999))
	if err == nil {
		t.Error("expected error for unknown config option")
	}
}

// --- Open/Close tests ---

func TestOpenInMemory(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	if db == nil {
		t.Fatal("expected non-nil database")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestOpenMemory(t *testing.T) {
	db, err := Open(":memory:", OpenReadWrite|OpenCreate|OpenMemory)
	if err != nil {
		t.Fatalf("Open :memory: failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestCloseTwice(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}

func TestExecAfterClose(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	db.Close()
	if err := db.Exec("CREATE TABLE t (x int)"); err == nil {
		t.Error("expected error executing on closed database")
	}
}

func TestQueryAfterClose(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	db.Close()
	if _, err := db.Query("SELECT 1"); err == nil {
		t.Error("expected error querying closed database")
	}
}

// --- Exec tests ---

func TestExecCreateTable(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	err = db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
}

func TestExecInsert(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	err = db.Exec("CREATE TABLE t (id INTEGER, val TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	err = db.Exec("INSERT INTO t VALUES (1, 'hello')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	if db.LastInsertRowID() <= 0 {
		t.Error("expected LastInsertRowID > 0 after INSERT")
	}
	if db.Changes() != 1 {
		t.Errorf("expected Changes()=1, got %d", db.Changes())
	}
	if db.TotalChanges() != 1 {
		t.Errorf("expected TotalChanges()=1, got %d", db.TotalChanges())
	}
}

func TestExecMultipleStatements(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	err = db.Exec(`
		CREATE TABLE t (id INTEGER, val TEXT);
		INSERT INTO t VALUES (1, 'a');
		INSERT INTO t VALUES (2, 'b');
	`)
	if err != nil {
		t.Fatalf("multiple statements failed: %v", err)
	}

	if db.TotalChanges() != 2 {
		t.Errorf("expected TotalChanges()=2, got %d", db.TotalChanges())
	}
}

// --- Query tests ---

func TestQuerySelect(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	err = db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}

	err = db.Exec("INSERT INTO t VALUES (1, 'alice')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}
	err = db.Exec("INSERT INTO t VALUES (2, 'bob')")
	if err != nil {
		t.Fatalf("INSERT failed: %v", err)
	}

	rs, err := db.Query("SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()

	count := 0
	for rs.Next() {
		count++
		row := rs.Row()
		if row == nil {
			t.Fatal("expected non-nil row")
		}
		if row.ColumnCount() != 2 {
			t.Errorf("expected 2 columns, got %d", row.ColumnCount())
		}
	}
	if count != 2 {
		t.Errorf("expected 2 rows, got %d", count)
	}
}

func TestQuerySelectStar(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (a INTEGER, b TEXT)")
	db.Exec("INSERT INTO t VALUES (1, 'x')")

	rs, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT * failed: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected at least one row")
	}
	row := rs.Row()
	if row.ColumnCount() != 2 {
		t.Errorf("expected 2 columns, got %d", row.ColumnCount())
	}
}

func TestQuerySelectValues(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")
	db.Exec("INSERT INTO t VALUES (42, 'hello')")

	rs, err := db.Query("SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one row")
	}
	row := rs.Row()

	id := row.ColumnInt(0)
	if id != 42 {
		t.Errorf("expected id=42, got %d", id)
	}

	name := row.ColumnText(1)
	if name != "hello" {
		t.Errorf("expected name='hello', got %q", name)
	}
}

func TestQuerySelectWithoutFrom(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	rs, err := db.Query("SELECT 1 + 2")
	if err != nil {
		t.Fatalf("SELECT without FROM failed: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one row")
	}
	row := rs.Row()
	val := row.ColumnInt(0)
	if val != 3 {
		t.Errorf("expected 3, got %d", val)
	}
}

func TestQueryColumnNames(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	rs, err := db.Query("SELECT 1 AS one, 2 AS two")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()

	names := rs.ColumnNames()
	if len(names) != 2 {
		t.Fatalf("expected 2 column names, got %d", len(names))
	}
	if names[0] != "one" {
		t.Errorf("expected column name 'one', got %q", names[0])
	}
	if names[1] != "two" {
		t.Errorf("expected column name 'two', got %q", names[1])
	}
}

func TestQueryEmptyTable(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER)")
	rs, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT on empty table failed: %v", err)
	}
	defer rs.Close()

	if rs.Next() {
		t.Error("expected no rows from empty table")
	}
}

// --- Row scan tests ---

func TestRowScan(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (a INTEGER, b TEXT, c REAL)")
	db.Exec("INSERT INTO t VALUES (1, 'test', 3.14)")

	rs, err := db.Query("SELECT a, b, c FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one row")
	}
	row := rs.Row()

	var a int64
	var b string
	var c float64
	if err := row.Scan(&a, &b, &c); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if a != 1 {
		t.Errorf("expected a=1, got %d", a)
	}
	if b != "test" {
		t.Errorf("expected b='test', got %q", b)
	}
	// Float comparison with tolerance
	if c < 3.13 || c > 3.15 {
		t.Errorf("expected c~=3.14, got %f", c)
	}
}

func TestRowColumnValue(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (i INTEGER, t TEXT, r REAL)")
	db.Exec("INSERT INTO t VALUES (10, 'hi', 2.5)")

	rs, err := db.Query("SELECT i, t, r FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one row")
	}
	row := rs.Row()

	if v := row.ColumnValue(0); v != int64(10) {
		t.Errorf("expected int64(10), got %v (%T)", v, v)
	}
	if v := row.ColumnValue(1); v != "hi" {
		t.Errorf("expected 'hi', got %v (%T)", v, v)
	}
	if v := row.ColumnValue(2); v != float64(2.5) {
		t.Errorf("expected float64(2.5), got %v (%T)", v, v)
	}
}

func TestRowColumnIsNull(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (a INTEGER)")
	db.Exec("INSERT INTO t VALUES (1)")

	rs, err := db.Query("SELECT a FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()

	if !rs.Next() {
		t.Fatal("expected one row")
	}
	row := rs.Row()

	if row.ColumnIsNull(0) {
		t.Error("expected column 0 to not be null")
	}
	if !row.ColumnIsNull(99) {
		t.Error("expected out-of-range column to be null")
	}
}

// --- Transaction tests ---

func TestBeginCommit(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	if err := db.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := db.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
}

func TestBeginRollback(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	if err := db.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := db.Rollback(); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
}

func TestDoubleBegin(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	if err := db.Begin(); err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := db.Begin(); err == nil {
		t.Error("expected error on double Begin")
	}
	db.Rollback()
}

func TestCommitWithoutBegin(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	if err := db.Commit(); err == nil {
		t.Error("expected error on Commit without Begin")
	}
}

func TestRollbackWithoutBegin(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	if err := db.Rollback(); err == nil {
		t.Error("expected error on Rollback without Begin")
	}
}

func TestTransactionViaExec(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	err = db.Exec("BEGIN")
	if err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	err = db.Exec("COMMIT")
	if err != nil {
		t.Fatalf("COMMIT failed: %v", err)
	}
}

// --- Statement tests ---

func TestPrepare(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")

	stmt, err := db.Prepare("INSERT INTO t VALUES (?, ?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Finalize()

	if stmt.ColumnCount() != 0 {
		t.Errorf("expected 0 columns for INSERT, got %d", stmt.ColumnCount())
	}
}

func TestStatementBind(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER, name TEXT)")

	stmt, err := db.Prepare("INSERT INTO t VALUES (?, ?)")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Finalize()

	if err := stmt.BindInt(1, 42); err != nil {
		t.Fatalf("BindInt failed: %v", err)
	}
	if err := stmt.BindText(2, "hello"); err != nil {
		t.Fatalf("BindText failed: %v", err)
	}
}

func TestStatementBindFloat(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Finalize()

	if err := stmt.BindFloat(1, 3.14); err != nil {
		t.Fatalf("BindFloat failed: %v", err)
	}
}

func TestStatementBindBlob(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Finalize()

	if err := stmt.BindBlob(1, []byte{1, 2, 3}); err != nil {
		t.Fatalf("BindBlob failed: %v", err)
	}
}

func TestStatementBindNull(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Finalize()

	if err := stmt.BindNull(1); err != nil {
		t.Fatalf("BindNull failed: %v", err)
	}
}

func TestStatementBindGeneric(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Finalize()

	tests := []interface{}{
		int(42),
		int64(100),
		float64(3.14),
		"hello",
		[]byte{1, 2, 3},
		true,
		nil,
	}
	for _, val := range tests {
		if err := stmt.Bind(1, val); err != nil {
			t.Errorf("Bind(%v) failed: %v", val, err)
		}
	}
}

func TestStatementBindOutOfRange(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Finalize()

	if err := stmt.BindInt(0, 1); err == nil {
		t.Error("expected error for index 0")
	}
}

func TestStatementReset(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	defer stmt.Finalize()

	stmt.BindInt(1, 42)
	if err := stmt.Reset(); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}
}

func TestStatementFinalizeTwice(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT 1")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	if err := stmt.Finalize(); err != nil {
		t.Fatalf("Finalize failed: %v", err)
	}
	if err := stmt.Finalize(); err != nil {
		t.Fatalf("second Finalize failed: %v", err)
	}
	if !stmt.IsFinalized() {
		t.Error("expected finalized to be true")
	}
}

func TestStatementBindAfterFinalize(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	stmt.Finalize()

	if err := stmt.BindInt(1, 1); err == nil {
		t.Error("expected error binding after finalize")
	}
}

// --- BusyTimeout test ---

func TestBusyTimeout(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.BusyTimeout(5000)
	// No way to verify directly, just ensure no panic
}

// --- Changes tracking tests ---

func TestChangesTracking(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (id INTEGER)")

	if db.Changes() != 0 {
		t.Errorf("expected initial Changes()=0, got %d", db.Changes())
	}

	db.Exec("INSERT INTO t VALUES (1)")
	if db.Changes() != 1 {
		t.Errorf("expected Changes()=1, got %d", db.Changes())
	}

	db.Exec("INSERT INTO t VALUES (2)")
	db.Exec("INSERT INTO t VALUES (3)")
	if db.TotalChanges() != 3 {
		t.Errorf("expected TotalChanges()=3, got %d", db.TotalChanges())
	}
}

// --- ResultSet tests ---

func TestResultSetRows(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (a INTEGER)")
	db.Exec("INSERT INTO t VALUES (1)")
	db.Exec("INSERT INTO t VALUES (2)")
	db.Exec("INSERT INTO t VALUES (3)")

	rs, err := db.Query("SELECT a FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()

	rows := rs.Rows()
	if len(rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(rows))
	}
}

func TestResultSetColumnInfo(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	rs, err := db.Query("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()

	if rs.ColumnCount() != 1 {
		t.Errorf("expected 1 column, got %d", rs.ColumnCount())
	}

	info := rs.ColumnInfo(0)
	if info == nil {
		t.Fatal("expected non-nil ColumnInfo")
	}
	if info.Name != "one" {
		t.Errorf("expected column name 'one', got %q", info.Name)
	}

	if rs.ColumnInfo(99) != nil {
		t.Error("expected nil for out-of-range ColumnInfo")
	}
}

// --- IfNotExists test ---

func TestCreateTableIfNotExists(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	err = db.Exec("CREATE TABLE t (id INTEGER)")
	if err != nil {
		t.Fatalf("first CREATE TABLE failed: %v", err)
	}

	// Should succeed without error
	err = db.Exec("CREATE TABLE IF NOT EXISTS t (id INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE IF NOT EXISTS failed: %v", err)
	}

	// Without IF NOT EXISTS should fail
	err = db.Exec("CREATE TABLE t (id INTEGER)")
	if err == nil {
		t.Error("expected error creating duplicate table")
	}
}

// --- Insert multiple rows ---

func TestInsertMultipleValueRows(t *testing.T) {
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	defer db.Close()

	db.Exec("CREATE TABLE t (a INTEGER, b TEXT)")

	err = db.Exec("INSERT INTO t VALUES (1, 'x'), (2, 'y'), (3, 'z')")
	if err != nil {
		t.Fatalf("multi-row INSERT failed: %v", err)
	}

	rs, err := db.Query("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()

	count := 0
	for rs.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 rows, got %d", count)
	}
}

// --- DELETE / UPDATE WHERE tests ---

func setupTableForWhere(t *testing.T) *Database {
	t.Helper()
	db, err := OpenInMemory()
	if err != nil {
		t.Fatalf("OpenInMemory failed: %v", err)
	}
	err = db.Exec("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)")
	if err != nil {
		t.Fatalf("CREATE TABLE failed: %v", err)
	}
	db.Exec("INSERT INTO t VALUES (1, 'alice', 30)")
	db.Exec("INSERT INTO t VALUES (2, 'bob', 25)")
	db.Exec("INSERT INTO t VALUES (3, 'charlie', 30)")
	db.Exec("INSERT INTO t VALUES (4, 'dave', 40)")
	return db
}

func countRows(t *testing.T, db *Database, query string) int {
	t.Helper()
	rs, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query %q failed: %v", query, err)
	}
	defer rs.Close()
	count := 0
	for rs.Next() {
		count++
	}
	return count
}

func TestDeleteAllRows(t *testing.T) {
	db := setupTableForWhere(t)
	defer db.Close()

	err := db.Exec("DELETE FROM t")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	if db.Changes() != 4 {
		t.Errorf("expected Changes()=4, got %d", db.Changes())
	}
	if countRows(t, db, "SELECT * FROM t") != 0 {
		t.Error("expected 0 rows after DELETE FROM t")
	}
}

func TestDeleteWhereEquals(t *testing.T) {
	db := setupTableForWhere(t)
	defer db.Close()

	err := db.Exec("DELETE FROM t WHERE name = 'bob'")
	if err != nil {
		t.Fatalf("DELETE WHERE failed: %v", err)
	}
	if db.Changes() != 1 {
		t.Errorf("expected Changes()=1, got %d", db.Changes())
	}
	n := countRows(t, db, "SELECT * FROM t")
	if n != 3 {
		t.Errorf("expected 3 rows after deleting bob, got %d", n)
	}
	// Verify bob is gone
	rs, err := db.Query("SELECT name FROM t WHERE name = 'bob'")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()
	if rs.Next() {
		t.Error("expected bob to be deleted")
	}
}

func TestDeleteWhereNumericComparison(t *testing.T) {
	db := setupTableForWhere(t)
	defer db.Close()

	err := db.Exec("DELETE FROM t WHERE age > 30")
	if err != nil {
		t.Fatalf("DELETE WHERE failed: %v", err)
	}
	if db.Changes() != 1 {
		t.Errorf("expected Changes()=1, got %d", db.Changes())
	}
	n := countRows(t, db, "SELECT * FROM t")
	if n != 3 {
		t.Errorf("expected 3 rows after deleting age>30, got %d", n)
	}
}

func TestDeleteWhereMultipleMatch(t *testing.T) {
	db := setupTableForWhere(t)
	defer db.Close()

	err := db.Exec("DELETE FROM t WHERE age = 30")
	if err != nil {
		t.Fatalf("DELETE WHERE failed: %v", err)
	}
	if db.Changes() != 2 {
		t.Errorf("expected Changes()=2, got %d", db.Changes())
	}
	n := countRows(t, db, "SELECT * FROM t")
	if n != 2 {
		t.Errorf("expected 2 rows after deleting age=30, got %d", n)
	}
}

func TestDeleteWhereNoMatch(t *testing.T) {
	db := setupTableForWhere(t)
	defer db.Close()

	err := db.Exec("DELETE FROM t WHERE id = 999")
	if err != nil {
		t.Fatalf("DELETE WHERE failed: %v", err)
	}
	if db.Changes() != 0 {
		t.Errorf("expected Changes()=0, got %d", db.Changes())
	}
	if countRows(t, db, "SELECT * FROM t") != 4 {
		t.Error("expected all 4 rows to remain")
	}
}

func TestDeleteWhereAndCondition(t *testing.T) {
	db := setupTableForWhere(t)
	defer db.Close()

	err := db.Exec("DELETE FROM t WHERE age = 30 AND name = 'alice'")
	if err != nil {
		t.Fatalf("DELETE WHERE AND failed: %v", err)
	}
	if db.Changes() != 1 {
		t.Errorf("expected Changes()=1, got %d", db.Changes())
	}
	n := countRows(t, db, "SELECT * FROM t")
	if n != 3 {
		t.Errorf("expected 3 rows, got %d", n)
	}
}

func TestUpdateWhereEquals(t *testing.T) {
	db := setupTableForWhere(t)
	defer db.Close()

	err := db.Exec("UPDATE t SET age = 26 WHERE name = 'bob'")
	if err != nil {
		t.Fatalf("UPDATE WHERE failed: %v", err)
	}
	if db.Changes() != 1 {
		t.Errorf("expected Changes()=1, got %d", db.Changes())
	}

	// Verify bob's age changed
	rs, err := db.Query("SELECT age FROM t WHERE name = 'bob'")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatal("expected bob row")
	}
	row := rs.Row()
	var age int64
	if err := row.Scan(&age); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if age != 26 {
		t.Errorf("expected bob age=26, got %d", age)
	}
}

func TestUpdateWhereMultipleMatch(t *testing.T) {
	db := setupTableForWhere(t)
	defer db.Close()

	err := db.Exec("UPDATE t SET age = 99 WHERE age = 30")
	if err != nil {
		t.Fatalf("UPDATE WHERE failed: %v", err)
	}
	if db.Changes() != 2 {
		t.Errorf("expected Changes()=2, got %d", db.Changes())
	}

	// Both alice and charlie should now have age 99
	rs, err := db.Query("SELECT name FROM t WHERE age = 99 ORDER BY name")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()
	count := 0
	for rs.Next() {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 rows with age=99, got %d", count)
	}
}

func TestUpdateWhereNoMatch(t *testing.T) {
	db := setupTableForWhere(t)
	defer db.Close()

	err := db.Exec("UPDATE t SET age = 99 WHERE id = 999")
	if err != nil {
		t.Fatalf("UPDATE WHERE failed: %v", err)
	}
	if db.Changes() != 0 {
		t.Errorf("expected Changes()=0, got %d", db.Changes())
	}
}

func TestUpdateAllRowsNoWhere(t *testing.T) {
	db := setupTableForWhere(t)
	defer db.Close()

	err := db.Exec("UPDATE t SET age = 0")
	if err != nil {
		t.Fatalf("UPDATE failed: %v", err)
	}
	if db.Changes() != 4 {
		t.Errorf("expected Changes()=4, got %d", db.Changes())
	}

	rs, err := db.Query("SELECT age FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()
	for rs.Next() {
		var age int64
		if err := rs.Row().Scan(&age); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		if age != 0 {
			t.Errorf("expected age=0, got %d", age)
		}
	}
}

func TestUpdateMultipleColumnsWhere(t *testing.T) {
	db := setupTableForWhere(t)
	defer db.Close()

	err := db.Exec("UPDATE t SET name = 'robert', age = 26 WHERE name = 'bob'")
	if err != nil {
		t.Fatalf("UPDATE SET multiple columns WHERE failed: %v", err)
	}
	if db.Changes() != 1 {
		t.Errorf("expected Changes()=1, got %d", db.Changes())
	}

	rs, err := db.Query("SELECT name, age FROM t WHERE name = 'robert'")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()
	if !rs.Next() {
		t.Fatal("expected robert row")
	}
	var name string
	var age int64
	if err := rs.Row().Scan(&name, &age); err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if name != "robert" {
		t.Errorf("expected name='robert', got %q", name)
	}
	if age != 26 {
		t.Errorf("expected age=26, got %d", age)
	}
}

func TestDeleteWhereOrCondition(t *testing.T) {
	db := setupTableForWhere(t)
	defer db.Close()

	err := db.Exec("DELETE FROM t WHERE name = 'alice' OR name = 'bob'")
	if err != nil {
		t.Fatalf("DELETE WHERE OR failed: %v", err)
	}
	if db.Changes() != 2 {
		t.Errorf("expected Changes()=2, got %d", db.Changes())
	}
	n := countRows(t, db, "SELECT * FROM t")
	if n != 2 {
		t.Errorf("expected 2 rows remaining, got %d", n)
	}
}

func TestUpdateWhereGreaterThan(t *testing.T) {
	db := setupTableForWhere(t)
	defer db.Close()

	err := db.Exec("UPDATE t SET age = 100 WHERE age >= 30")
	if err != nil {
		t.Fatalf("UPDATE WHERE >= failed: %v", err)
	}
	if db.Changes() != 3 {
		t.Errorf("expected Changes()=3, got %d", db.Changes())
	}

	rs, err := db.Query("SELECT name FROM t WHERE age = 100 ORDER BY name")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	defer rs.Close()
	count := 0
	for rs.Next() {
		count++
	}
	if count != 3 {
		t.Errorf("expected 3 rows with age=100, got %d", count)
	}
}

// --- helper ---

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
