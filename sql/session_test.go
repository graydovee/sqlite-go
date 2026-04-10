package sql

import (
	"testing"
)

// ──────────────────────────────────────────────────────────────
// PRAGMA Tests
// ──────────────────────────────────────────────────────────────

func TestPragmaTableXInfo(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.ExecSQL("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT NOT NULL, val REAL DEFAULT 0.0)")

	rows, err := eng.ExecPragmaAndQuery("PRAGMA table_xinfo(t1)")
	if err != nil {
		t.Fatalf("table_xinfo: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Check first column (id)
	if rows[0].Values[1] != "id" {
		t.Errorf("col 0 name = %v, want id", rows[0].Values[1])
	}
	if rows[0].Values[5] != 1 { // pk
		t.Errorf("col 0 pk = %v, want 1", rows[0].Values[5])
	}
	// hidden column (index 6)
	if rows[0].Values[6] != 0 {
		t.Errorf("col 0 hidden = %v, want 0", rows[0].Values[6])
	}

	// Check name column
	if rows[1].Values[1] != "name" {
		t.Errorf("col 1 name = %v, want name", rows[1].Values[1])
	}
	if rows[1].Values[3] != 1 { // notnull
		t.Errorf("col 1 notnull = %v, want 1", rows[1].Values[3])
	}
}

func TestPragmaTableXInfoNotFound(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	_, err = eng.ExecPragmaAndQuery("PRAGMA table_xinfo(nonexistent)")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestPragmaIndexList(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.ExecSQL("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")

	// Table with PK should show autoindex
	rows, err := eng.ExecPragmaAndQuery("PRAGMA index_list(t1)")
	if err != nil {
		t.Fatalf("index_list: %v", err)
	}
	if len(rows) < 1 {
		t.Fatalf("expected at least 1 row for PK autoindex, got %d", len(rows))
	}

	// Should have an autoindex entry for PRIMARY KEY
	found := false
	for _, row := range rows {
		name, _ := row.Values[1].(string)
		if name == "sqlite_autoindex_t1_1" {
			found = true
			if row.Values[2] != 1 { // unique
				t.Errorf("autoindex unique = %v, want 1", row.Values[2])
			}
		}
	}
	if !found {
		t.Errorf("expected autoindex entry, got rows: %v", rows)
	}
}

func TestPragmaIndexListNotFound(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	rows, err := eng.ExecPragmaAndQuery("PRAGMA index_list(nonexistent)")
	if err != nil {
		t.Fatalf("index_list on nonexistent table: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows for nonexistent table, got %d", len(rows))
	}
}

func TestPragmaStats(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.ExecSQL("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	eng.ExecSQL("INSERT INTO t1 VALUES (1, 'alice')")
	eng.ExecSQL("INSERT INTO t1 VALUES (2, 'bob')")

	rows, err := eng.ExecPragmaAndQuery("PRAGMA stats")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if len(rows) < 1 {
		t.Fatalf("expected at least 1 row, got %d", len(rows))
	}

	// Find t1 row
	found := false
	for _, row := range rows {
		name, _ := row.Values[0].(string)
		if name == "t1" {
			found = true
			rowCount := row.Values[4]
			if rowCount == nil || rowCount.(int64) < 2 {
				t.Errorf("t1 row count = %v, want >= 2", rowCount)
			}
		}
	}
	if !found {
		t.Errorf("expected stats for t1, got rows: %v", rows)
	}
}

func TestPragmaExistingOnes(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	// Test integrity_check
	rows, err := eng.ExecPragmaAndQuery("PRAGMA integrity_check")
	if err != nil {
		t.Fatalf("integrity_check: %v", err)
	}
	if len(rows) == 0 || rows[0].Values[0] != "ok" {
		t.Errorf("integrity_check = %v", rows)
	}

	// Test compile_options
	rows, err = eng.ExecPragmaAndQuery("PRAGMA compile_options")
	if err != nil {
		t.Fatalf("compile_options: %v", err)
	}
	if len(rows) == 0 {
		t.Error("expected at least one compile option")
	}

	// Test foreign_key_list (empty for table without FKs)
	eng.ExecSQL("CREATE TABLE nofk (id INTEGER PRIMARY KEY)")
	rows, err = eng.ExecPragmaAndQuery("PRAGMA foreign_key_list(nofk)")
	if err != nil {
		t.Fatalf("foreign_key_list: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 FK rows for table without FKs, got %d", len(rows))
	}
}

// ──────────────────────────────────────────────────────────────
// Session / Changeset Tests
// ──────────────────────────────────────────────────────────────

func TestSessionBasic(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.ExecSQL("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")

	sess := NewSession(eng)
	if err := sess.WatchTable("t1"); err != nil {
		t.Fatal(err)
	}

	sess.Begin()
	sess.CaptureInsert("t1", 1, []byte("test-data"))
	sess.End()

	if sess.ChangeCount() != 1 {
		t.Errorf("ChangeCount = %d, want 1", sess.ChangeCount())
	}
}

func TestSessionChangesetGeneration(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.ExecSQL("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")

	sess := NewSession(eng)
	sess.WatchTable("t1")
	sess.Begin()

	sess.CaptureInsert("t1", 1, []byte("insert-data"))
	sess.CaptureUpdate("t1", 2, []byte("old-data"), []byte("new-data"))
	sess.CaptureDelete("t1", 3, []byte("delete-data"))
	sess.End()

	cs, err := sess.Changeset()
	if err != nil {
		t.Fatalf("Changeset: %v", err)
	}
	if len(cs) < 12 {
		t.Fatalf("changeset too small: %d bytes", len(cs))
	}

	// Verify header magic
	if cs[0] != 0x53 || cs[1] != 0x43 || cs[2] != 0x48 || cs[3] != 0x47 {
		t.Errorf("bad magic: %02x %02x %02x %02x", cs[0], cs[1], cs[2], cs[3])
	}

	// Verify 3 changes in header
	nChanges := uint32(cs[8])<<24 | uint32(cs[9])<<16 | uint32(cs[10])<<8 | uint32(cs[11])
	if nChanges != 3 {
		t.Errorf("nChanges = %d, want 3", nChanges)
	}
}

func TestSessionWatchNonExistentTable(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	sess := NewSession(eng)
	err = sess.WatchTable("nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent table")
	}
}

func TestSessionUnwatch(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.ExecSQL("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")

	sess := NewSession(eng)
	sess.WatchTable("t1")
	sess.Begin()

	sess.CaptureInsert("t1", 1, []byte("data"))
	sess.UnwatchTable("t1")
	sess.CaptureInsert("t1", 2, []byte("data2"))
	sess.End()

	// Only the first insert should be captured (before unwatch)
	if sess.ChangeCount() != 1 {
		t.Errorf("ChangeCount = %d, want 1", sess.ChangeCount())
	}
}

func TestSessionNotActive(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.ExecSQL("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")

	sess := NewSession(eng)
	sess.WatchTable("t1")
	// Not calling Begin()
	sess.CaptureInsert("t1", 1, []byte("data"))

	if sess.ChangeCount() != 0 {
		t.Errorf("ChangeCount = %d, want 0 (not active)", sess.ChangeCount())
	}
}

func TestChangesetApply(t *testing.T) {
	// Create a changeset manually and apply it
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.ExecSQL("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")

	sess := NewSession(eng)
	sess.WatchTable("t1")
	sess.Begin()
	sess.CaptureInsert("t1", 1, []byte("test-insert"))
	sess.End()

	cs, err := sess.Changeset()
	if err != nil {
		t.Fatal(err)
	}

	// Apply to the same engine (should work)
	applied, err := ChangesetApply(eng, cs)
	if err != nil {
		t.Fatalf("ChangesetApply: %v", err)
	}
	if applied != 1 {
		t.Errorf("applied = %d, want 1", applied)
	}
}

func TestChangesetApplyBadMagic(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	cs := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}
	_, err = ChangesetApply(eng, cs)
	if err == nil {
		t.Error("expected error for bad magic")
	}
}

func TestChangesetApplyTooShort(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	_, err = ChangesetApply(eng, []byte{0x01, 0x02})
	if err == nil {
		t.Error("expected error for short changeset")
	}
}

func TestInvertChangeset(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.ExecSQL("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")

	sess := NewSession(eng)
	sess.WatchTable("t1")
	sess.Begin()
	sess.CaptureInsert("t1", 1, []byte("row1"))
	sess.CaptureDelete("t1", 2, []byte("row2"))
	sess.CaptureUpdate("t1", 3, []byte("old3"), []byte("new3"))
	sess.End()

	cs, err := sess.Changeset()
	if err != nil {
		t.Fatal(err)
	}

	inverted, err := InvertChangeset(cs)
	if err != nil {
		t.Fatalf("InvertChangeset: %v", err)
	}

	// Inverted changeset should be same length
	if len(inverted) != len(cs) {
		t.Errorf("inverted length = %d, want %d", len(inverted), len(cs))
	}

	// Double inversion should produce the original
	doubleInverted, err := InvertChangeset(inverted)
	if err != nil {
		t.Fatalf("second InvertChangeset: %v", err)
	}
	if len(doubleInverted) != len(cs) {
		t.Errorf("double inverted length = %d, want %d", len(doubleInverted), len(cs))
	}

	// Verify the double-inverted matches the original
	for i := range cs {
		if doubleInverted[i] != cs[i] {
			t.Errorf("double inverted differs at byte %d: got %02x, want %02x", i, doubleInverted[i], cs[i])
			break
		}
	}
}

func TestSessionMultipleTables(t *testing.T) {
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.ExecSQL("CREATE TABLE t1 (id INTEGER PRIMARY KEY)")
	eng.ExecSQL("CREATE TABLE t2 (id INTEGER PRIMARY KEY)")

	sess := NewSession(eng)
	sess.WatchTable("t1")
	sess.WatchTable("t2")
	sess.Begin()

	sess.CaptureInsert("t1", 1, []byte("t1-data"))
	sess.CaptureInsert("t2", 1, []byte("t2-data"))
	sess.CaptureDelete("t1", 2, []byte("t1-del"))
	sess.End()

	if sess.ChangeCount() != 3 {
		t.Errorf("ChangeCount = %d, want 3", sess.ChangeCount())
	}

	cs, err := sess.Changeset()
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) == 0 {
		t.Error("changeset should not be empty")
	}
}

func TestChangesetRoundTrip(t *testing.T) {
	// Create a changeset, invert it, and verify the round trip
	eng, err := OpenEngine()
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	eng.ExecSQL("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")

	sess := NewSession(eng)
	sess.WatchTable("t1")
	sess.Begin()
	sess.CaptureInsert("t1", 1, []byte("insert"))
	sess.CaptureUpdate("t1", 2, []byte("old"), []byte("new"))
	sess.CaptureDelete("t1", 3, []byte("delete"))
	sess.End()

	cs, _ := sess.Changeset()
	inv, err := InvertChangeset(cs)
	if err != nil {
		t.Fatal(err)
	}

	// Apply inverted should work
	applied, err := ChangesetApply(eng, inv)
	if err != nil {
		t.Fatalf("apply inverted: %v", err)
	}
	if applied != 3 {
		t.Errorf("applied = %d, want 3", applied)
	}
}
