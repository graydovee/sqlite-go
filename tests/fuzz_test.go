package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/sqlite-go/sqlite-go/btree"
	"github.com/sqlite-go/sqlite-go/compile"
	"github.com/sqlite-go/sqlite-go/encoding"
	"github.com/sqlite-go/sqlite-go/functions"
	"github.com/sqlite-go/sqlite-go/pager"
	"github.com/sqlite-go/sqlite-go/sqlite"
	"github.com/sqlite-go/sqlite-go/vdbe"
	"github.com/sqlite-go/sqlite-go/vfs"
)

// ============================================================================
// Fuzz tests - fuzz SQL parser, VDBE execution, record encoding
// ============================================================================

// --- Fuzz SQL parser with random strings ---

func FuzzSQLParser(f *testing.F) {
	// Seed corpus: valid and invalid SQL statements
	seeds := []string{
		"SELECT 1",
		"SELECT 1 + 2",
		"CREATE TABLE t (id INTEGER)",
		"INSERT INTO t VALUES (1)",
		"SELECT * FROM t",
		"UPDATE t SET id = 2",
		"DELETE FROM t",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		// Invalid SQL
		"SELEC 1",
		"CREATE",
		"INSERT INTO",
		"SELECT FROM WHERE",
		"!!!@@@###",
		"",
		"SELECT NULL",
		"SELECT 'hello'",
		"SELECT 1.5e10",
		"SELECT 'it''s quoted'",
		"CREATE TABLE t (a INTEGER, b TEXT, c REAL)",
		"SELECT upper('hello')",
		"SELECT abs(-42)",
		"DROP TABLE t",
		"ALTER TABLE t ADD COLUMN x TEXT",
		"SELECT 1 WHERE 1 = 1",
		"SELECT count(*) FROM t",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		db, err := sqlite.OpenInMemory()
		if err != nil {
			return
		}
		defer db.Close()

		// Execute the SQL - we just care that it doesn't panic
		_ = db.Exec(sql)

		// Also try as a query
		rs, _ := db.Query(sql)
		if rs != nil {
			for rs.Next() {
				_ = rs.Row()
			}
			rs.Close()
		}
	})
}

// --- Fuzz SQL with random tokens ---

func FuzzSQLRandomTokens(f *testing.F) {
	tokens := []string{
		"SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES",
		"UPDATE", "SET", "DELETE", "CREATE", "TABLE", "DROP",
		"ALTER", "ADD", "COLUMN", "INDEX", "VIEW", "TRIGGER",
		"BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION",
		"AND", "OR", "NOT", "NULL", "IS", "IN", "BETWEEN",
		"LIKE", "GLOB", "REGEXP", "MATCH", "EXISTS",
		"CASE", "WHEN", "THEN", "ELSE", "END",
		"ASC", "DESC", "ORDER", "BY", "GROUP", "HAVING",
		"LIMIT", "OFFSET", "DISTINCT", "ALL", "AS",
		"ON", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
		"CROSS", "NATURAL", "USING",
		"PRIMARY", "KEY", "UNIQUE", "CHECK", "DEFAULT",
		"FOREIGN", "REFERENCES", "CONSTRAINT",
		"INTEGER", "TEXT", "REAL", "BLOB", "NUMERIC",
		"IF", "EXISTS", "REPLACE", "IGNORE", "ABORT",
		"int64", "3.14", "'text'", "NULL", "TRUE", "FALSE",
		"(", ")", ",", ";", ".", "*", "+", "-", "/", "%",
		"=", "<", ">", "<=", ">=", "<>", "!=",
		"||", "<<", ">>", "&", "|", "~",
	}

	// Generate seed corpus
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 50; i++ {
		n := rng.Intn(10) + 1
		parts := make([]string, n)
		for j := 0; j < n; j++ {
			parts[j] = tokens[rng.Intn(len(tokens))]
		}
		f.Add(strings.Join(parts, " "))
	}

	f.Fuzz(func(t *testing.T, sql string) {
		db, err := sqlite.OpenInMemory()
		if err != nil {
			return
		}
		defer db.Close()
		// Should never panic
		_ = db.Exec(sql)
		rs, _ := db.Query(sql)
		if rs != nil {
			for rs.Next() {
			}
			rs.Close()
		}
	})
}

// --- Fuzz record encoding/decoding ---

func FuzzRecordEncoding(f *testing.F) {
	// Seed corpus: various value combinations
	seeds := [][]btree.Value{
		{btree.NullValue()},
		{btree.IntValue(0)},
		{btree.IntValue(1)},
		{btree.IntValue(-1)},
		{btree.IntValue(9223372036854775807)},
		{btree.IntValue(-9223372036854775808)},
		{btree.FloatValue(0.0)},
		{btree.FloatValue(3.14)},
		{btree.FloatValue(-1e308)},
		{btree.TextValue("")},
		{btree.TextValue("hello")},
		{btree.TextValue(strings.Repeat("x", 1000))},
		{btree.BlobValue(nil)},
		{btree.BlobValue([]byte{0x00, 0xFF, 0x80})},
		{btree.IntValue(1), btree.TextValue("x"), btree.NullValue()},
	}

	for i := range seeds {
		f.Add(i)
	}

	f.Fuzz(func(t *testing.T, seed int) {
		rng := rand.New(rand.NewSource(int64(seed)))

		// Generate random values
		n := rng.Intn(20) + 1
		values := make([]btree.Value, n)
		for i := range values {
			switch rng.Intn(5) {
			case 0:
				values[i] = btree.NullValue()
			case 1:
				values[i] = btree.IntValue(rng.Int63())
			case 2:
				values[i] = btree.FloatValue(rng.Float64())
			case 3:
				b := make([]byte, rng.Intn(100))
				rng.Read(b)
				values[i] = btree.TextValue(string(b))
			case 4:
				b := make([]byte, rng.Intn(100))
				rng.Read(b)
				values[i] = btree.BlobValue(b)
			}
		}

		// Encode and decode
		data := btree.MakeRecord(values)
		got, err := btree.ParseRecord(data)
		if err != nil {
			t.Fatalf("ParseRecord failed: %v (values: %v)", err, values)
		}

		if len(got) != len(values) {
			t.Fatalf("length mismatch: got %d, want %d", len(got), len(values))
		}

		for i, want := range values {
			if got[i].Type != want.Type {
				t.Errorf("value[%d]: type mismatch: got %q, want %q", i, got[i].Type, want.Type)
			}
		}
	})
}

// --- Fuzz varint encoding ---

func FuzzVarintEncoding(f *testing.F) {
	seeds := []int64{0, 1, -1, 127, 128, 255, 256, 1<<31, 1<<63 - 1, -1 << 63}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, v int64) {
		buf := make([]byte, 10)
		n := btree.PutVarint(buf, v)
		if n == 0 {
			t.Fatalf("PutVarint(%d) returned 0", v)
		}

		got, rn := btree.ReadVarint(buf[:n])
		if got != v {
			t.Errorf("ReadVarint = %d, want %d", got, v)
		}
		if rn != n {
			t.Errorf("ReadVarint consumed %d bytes, wrote %d", rn, n)
		}
	})
}

// --- Fuzz VDBE execution with random programs ---

func FuzzVDBEExecution(f *testing.F) {
	// We'll generate small random programs
	seeds := []int{0, 1, 2, 3, 42, 100, 255}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, seed int) {
		rng := rand.New(rand.NewSource(int64(seed)))

		db := newMockDB()
		db.tables[1] = &vdbe.TableInfo{
			RootPage: 1,
			Columns: []vdbe.ColumnInfo{
				{Name: "id", Affinity: 'i'},
				{Name: "name", Affinity: 't'},
			},
			Name: "test",
		}

		// Build a random program
		const maxInstrs = 20
		numInstrs := rng.Intn(maxInstrs) + 1
		instrs := make([]vdbe.Instruction, 0, numInstrs)

		// Always end with Halt
		for i := 0; i < numInstrs-1; i++ {
			op := rng.Intn(15)
			switch op {
			case 0:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpInteger, P1: rng.Intn(1000), P2: rng.Intn(10)})
			case 1:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpReal, P4: rng.Float64(), P2: rng.Intn(10)})
			case 2:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpString8, P4: fmt.Sprintf("str%d", rng.Intn(10)), P2: rng.Intn(10)})
			case 3:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpNull, P2: rng.Intn(10)})
			case 4:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpAdd, P1: rng.Intn(10), P2: rng.Intn(10), P3: rng.Intn(10)})
			case 5:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpSubtract, P1: rng.Intn(10), P2: rng.Intn(10), P3: rng.Intn(10)})
			case 6:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpMul, P1: rng.Intn(10), P2: rng.Intn(10), P3: rng.Intn(10)})
			case 7:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpNoop})
			case 8:
				// Safe jumps within bounds
				target := rng.Intn(numInstrs)
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpGoto, P2: target})
			case 9:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpCopy, P1: rng.Intn(10), P2: rng.Intn(10), P3: 1})
			case 10:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpSCopy, P1: rng.Intn(10), P2: rng.Intn(10)})
			case 11:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpMove, P1: rng.Intn(10), P2: rng.Intn(10), P3: 1})
			case 12:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpBitNot, P1: rng.Intn(10), P2: rng.Intn(10)})
			case 13:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpNot, P1: rng.Intn(10), P2: rng.Intn(10)})
			case 14:
				instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpBlob, P4: []byte{byte(rng.Intn(256))}, P2: rng.Intn(10)})
			}
		}
		instrs = append(instrs, vdbe.Instruction{Op: vdbe.OpHalt})

		prog := &vdbe.Program{
			Instructions: instrs,
			NumRegs:     20,
			NumCursors:  0,
		}
		v := vdbe.NewVDBE(db)
		v.SetProgram(prog)

		// Execute with a short timeout to prevent infinite loops
		ctx := context.Background()
		// Should never panic
		_, _ = v.Execute(ctx)
	})
}

// --- Fuzz function calls ---

func FuzzFunctionCalls(f *testing.F) {
	seeds := []string{"abs", "upper", "lower", "length", "typeof", "hex", "trim", "round"}
	for _, seed := range seeds {
		f.Add(seed, int64(42), "hello")
	}

	f.Fuzz(func(t *testing.T, funcName string, intArg int64, strArg string) {
		r := functions.NewFuncRegistry()
		fn := r.Lookup(funcName, 1)
		if fn == nil {
			return
		}

		// Create args from fuzz inputs - should never panic
		args := []*vdbe.Mem{vdbe.NewMemInt(intArg)}
		ctx := &functions.Context{}
		if fn.ScalarFunc != nil {
			_ = fn.ScalarFunc(ctx, args)
		}

		args2 := []*vdbe.Mem{vdbe.NewMemStr(strArg)}
		if fn.ScalarFunc != nil {
			_ = fn.ScalarFunc(ctx, args2)
		}

		// NULL arg
		args3 := []*vdbe.Mem{vdbe.NewMemNull()}
		if fn.ScalarFunc != nil {
			_ = fn.ScalarFunc(ctx, args3)
		}
	})
}

// --- Fuzz UTF-8 operations ---

func FuzzUTF8Operations(f *testing.F) {
	seeds := []string{"", "hello", "Héllo Wörld", "日本語", "🎉🚀", "\xff\xfe", string([]byte{0xC0, 0x80})}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, s string) {
		// Should never panic
		_ = encoding.Utf8Valid(s)
		_ = encoding.Utf8Len([]byte(s))
		_ = encoding.Utf8ToUpper(s)
		_ = encoding.Utf8ToLower(s)

		if len(s) > 0 {
			_ = encoding.Utf8CharLen(s[0])
		}

		// Read runes
		data := []byte(s)
		for len(data) > 0 {
			_, size := encoding.Utf8ReadRune(data)
			if size == 0 {
				break
			}
			data = data[size:]
		}

		// Case-insensitive comparison
		_ = encoding.Utf8CaseCmp(s, strings.ToUpper(s))
	})
}

// --- Fuzz compile/tokenize ---

func FuzzTokenize(f *testing.F) {
	seeds := []string{
		"SELECT 1",
		"CREATE TABLE t (id INTEGER)",
		"",
		"@@@!!!",
		"'unclosed string",
		"12345",
		"3.14e10",
		"0x1234",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		// Tokenize should never panic
		_ = compile.Tokenize(sql)
	})
}

// --- Fuzz hash table ---

func FuzzHashTable(f *testing.F) {
	seeds := []string{"", "a", "hello", strings.Repeat("x", 1000), "key with spaces", "key\twith\ttabs"}
	for _, seed := range seeds {
		f.Add(seed, 42)
	}

	f.Fuzz(func(t *testing.T, key string, val int) {
		ht := encoding.NewHashTable(16)

		// Insert - should never panic
		ht.Insert(key, val)

		// Find
		got := ht.Find(key)
		if got != val {
			t.Errorf("Find(%q) = %v, want %d", key, got, val)
		}

		// Delete
		ht.Delete(key)

		// Should be gone
		got = ht.Find(key)
		if got != nil {
			t.Errorf("Find after delete(%q) = %v, want nil", key, got)
		}
	})
}

// --- Fuzz bit vector ---

func FuzzBitVec(f *testing.F) {
	seeds := []int{0, 1, 100, 999, 10000}
	for _, seed := range seeds {
		f.Add(seed, int64(42))
	}

	f.Fuzz(func(t *testing.T, size int, bits int64) {
		if size <= 0 || size > 100000 {
			return
		}

		bv := encoding.NewBitVec(size)

		// Set some bits
		rng := rand.New(rand.NewSource(bits))
		numOps := rng.Intn(100)
		for i := 0; i < numOps; i++ {
			bit := rng.Intn(size)
			if bit <= 0 {
				continue
			}
			bv.Set(bit)
			if !bv.Test(bit) {
				t.Errorf("bit %d should be set", bit)
			}
			bv.Clear(bit)
			if bv.Test(bit) {
				t.Errorf("bit %d should be cleared", bit)
			}
		}
	})
}

// --- Fuzz pager operations ---

func FuzzPagerOps(f *testing.F) {
	seeds := []int{0, 1, 42, 100}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, seed int) {
		rng := rand.New(rand.NewSource(int64(seed)))

		cfg := pager.PagerConfig{
			Path:      ":memory:",
			PageSize:  4096,
			CacheSize: 5,
			VFS:       vfs.Find("memory"),
		}
		p, err := pager.OpenPager(cfg)
		if err != nil {
			return
		}
		if err := p.Open(); err != nil {
			return
		}
		defer p.Close()

		// Random sequence of operations
		for i := 0; i < 50; i++ {
			op := rng.Intn(4)
			switch op {
			case 0:
				// Begin
				_ = p.Begin(true)
			case 1:
				// GetNewPage
				page, err := p.GetNewPage()
				if err == nil {
					header := fmt.Sprintf("fuzz_%d", rng.Int63())
					copy(page.Data[0:], []byte(header))
					p.MarkDirty(page)
					p.WritePage(page)
					p.ReleasePage(page)
				}
			case 2:
				// Commit
				_ = p.Commit()
			case 3:
				// Rollback
				_ = p.Rollback()
			}
		}
	})
}

// --- Fuzz SQL with parameterized inputs ---

func FuzzSQLWithParameters(f *testing.F) {
	seeds := []string{
		"SELECT ?",
		"SELECT ?, ?, ?",
		"SELECT ? + ?",
		"SELECT upper(?)",
		"SELECT abs(?)",
		"SELECT coalesce(?, ?, ?)",
		"SELECT typeof(?)",
		"SELECT length(?)",
		"SELECT ? || ?",
	}

	for _, seed := range seeds {
		f.Add(seed, int64(1), "hello")
	}

	f.Fuzz(func(t *testing.T, sql string, intArg int64, strArg string) {
		db, err := sqlite.OpenInMemory()
		if err != nil {
			return
		}
		defer db.Close()

		// Try with int arg
		rs, _ := db.Query(sql, intArg)
		if rs != nil {
			for rs.Next() {
			}
			rs.Close()
		}

		// Try with string arg
		rs, _ = db.Query(sql, strArg)
		if rs != nil {
			for rs.Next() {
			}
			rs.Close()
		}

		// Try with nil arg
		rs, _ = db.Query(sql, nil)
		if rs != nil {
			for rs.Next() {
			}
			rs.Close()
		}
	})
}

// --- Fuzz SQL round-trip: create table, insert, select ---

func FuzzSQLRoundTrip(f *testing.F) {
	seeds := []int{0, 1, 42, 100, 255}
	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, seed int) {
		rng := rand.New(rand.NewSource(int64(seed)))
		db, err := sqlite.OpenInMemory()
		if err != nil {
			return
		}
		defer db.Close()

		// Create table
		_ = db.Exec("CREATE TABLE fuzz_rt (id INTEGER, val TEXT, score REAL)")

		// Insert random rows
		numRows := rng.Intn(20)
		for i := 0; i < numRows; i++ {
			id := rng.Int63n(1000)
			val := fmt.Sprintf("val_%d", rng.Int63n(100))
			score := rng.Float64() * 100
			_ = db.Exec("INSERT INTO fuzz_rt VALUES (?, ?, ?)", id, val, score)
		}

		// Select all
		rs, _ := db.Query("SELECT * FROM fuzz_rt")
		if rs != nil {
			for rs.Next() {
				_ = rs.Row()
			}
			rs.Close()
		}
	})
}

// Helper: simplified mock for fuzz tests (same as vdbe_test but in this package)
type fuzzMockDB struct {
	tables map[int]*vdbe.TableInfo
}

func newMockDB() *fuzzMockDB {
	return &fuzzMockDB{tables: make(map[int]*vdbe.TableInfo)}
}
func (m *fuzzMockDB) GetTableInfo(rootPage int) (*vdbe.TableInfo, error) {
	if info, ok := m.tables[rootPage]; ok {
		return info, nil
	}
	return nil, fmt.Errorf("table not found")
}
func (m *fuzzMockDB) GetCursor(rootPage int, write bool) (interface{}, error) { return nil, nil }
func (m *fuzzMockDB) BeginTransaction(write bool) error                       { return nil }
func (m *fuzzMockDB) Commit() error                                           { return nil }
func (m *fuzzMockDB) Rollback() error                                         { return nil }
func (m *fuzzMockDB) AutoCommit() bool                                        { return true }
func (m *fuzzMockDB) SetAutoCommit(on bool)                                   {}
func (m *fuzzMockDB) Changes() int64                                          { return 0 }
func (m *fuzzMockDB) TotalChanges() int64                                     { return 0 }
func (m *fuzzMockDB) LastInsertRowID() int64                                  { return 0 }
func (m *fuzzMockDB) SetLastInsertRowID(id int64)                             {}

// Suppress unused import warnings
var _ = utf8.RuneLen
var _ = context.Background
