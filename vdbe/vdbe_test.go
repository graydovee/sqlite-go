package vdbe

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
)

// ─── Mock Database ────────────────────────────────────────────────────────

type mockDB struct {
	autoCommit    bool
	changes       int64
	totalChanges  int64
	lastInsertID  int64
	tables        map[int]*TableInfo
	beganTx       bool
	beganWriteTx  bool
	committed     bool
	rolledBack    bool
}

type mockCursor struct {
	rows    []mockRow
	current int
	valid   bool
	closed  bool
	root    int
}

type mockRow struct {
	rowid int64
	data  []byte
}

func newMockDB() *mockDB {
	return &mockDB{
		autoCommit: true,
		tables:     make(map[int]*TableInfo),
	}
}

func (m *mockDB) GetTableInfo(rootPage int) (*TableInfo, error) {
	if info, ok := m.tables[rootPage]; ok {
		return info, nil
	}
	return nil, fmt.Errorf("table not found for root page %d", rootPage)
}

func (m *mockDB) GetCursor(rootPage int, write bool) (interface{}, error) {
	info, ok := m.tables[rootPage]
	if !ok {
		return nil, fmt.Errorf("no table at root page %d", rootPage)
	}
	// Create a mock cursor with some test rows
	rows := []mockRow{}
	rb := NewRecordBuilder()
	rb.AddInt(1).AddText("alice")
	data, _ := buildTestData(rb)
	rows = append(rows, mockRow{rowid: 1, data: data})

	rb = NewRecordBuilder()
	rb.AddInt(2).AddText("bob")
	data, _ = buildTestData(rb)
	rows = append(rows, mockRow{rowid: 2, data: data})

	rb = NewRecordBuilder()
	rb.AddInt(3).AddText("charlie")
	data, _ = buildTestData(rb)
	rows = append(rows, mockRow{rowid: 3, data: data})

	mc := &mockCursor{
		rows:    rows,
		current: -1,
		valid:   false,
		root:    rootPage,
	}
	_ = info // silence unused warning
	return mc, nil
}

func buildTestData(rb *RecordBuilder) ([]byte, error) {
	return rb.Build(), nil
}

func (m *mockDB) BeginTransaction(write bool) error {
	m.beganTx = true
	m.beganWriteTx = write
	return nil
}

func (m *mockDB) Commit() error {
	m.committed = true
	return nil
}

func (m *mockDB) Rollback() error {
	m.rolledBack = true
	return nil
}

func (m *mockDB) AutoCommit() bool          { return m.autoCommit }
func (m *mockDB) SetAutoCommit(on bool)     { m.autoCommit = on }
func (m *mockDB) Changes() int64            { return m.changes }
func (m *mockDB) TotalChanges() int64       { return m.totalChanges }
func (m *mockDB) LastInsertRowID() int64    { return m.lastInsertID }
func (m *mockDB) SetLastInsertRowID(id int64) { m.lastInsertID = id }

// Mock cursor implementing Cursor interface
func (c *mockCursor) Close() error       { c.closed = true; c.valid = false; return nil }
func (c *mockCursor) First() (bool, error) {
	if len(c.rows) == 0 {
		c.valid = false
		return false, nil
	}
	c.current = 0
	c.valid = true
	return true, nil
}
func (c *mockCursor) Last() (bool, error) {
	if len(c.rows) == 0 {
		c.valid = false
		return false, nil
	}
	c.current = len(c.rows) - 1
	c.valid = true
	return true, nil
}
func (c *mockCursor) Next() (bool, error) {
	if !c.valid || c.current >= len(c.rows)-1 {
		c.valid = false
		return false, nil
	}
	c.current++
	return true, nil
}
func (c *mockCursor) Prev() (bool, error) {
	if !c.valid || c.current <= 0 {
		c.valid = false
		return false, nil
	}
	c.current--
	return true, nil
}
func (c *mockCursor) Key() []byte          { return nil }
func (c *mockCursor) Data() ([]byte, error) {
	if !c.valid || c.current < 0 || c.current >= len(c.rows) {
		return nil, nil
	}
	return c.rows[c.current].data, nil
}
func (c *mockCursor) RowID() int64 {
	if !c.valid || c.current < 0 || c.current >= len(c.rows) {
		return 0
	}
	return c.rows[c.current].rowid
}
func (c *mockCursor) IsValid() bool       { return c.valid }

// ─── Helper ──────────────────────────────────────────────────────────────

// exec runs a program on a fresh VDBE and returns the result.
func exec(instrs []Instruction, numRegs, numCursors int) (*VDBE, ResultCode, error) {
	db := newMockDB()
	db.tables[1] = &TableInfo{
		RootPage: 1,
		Columns:  []ColumnInfo{{Name: "id", Affinity: 'i'}, {Name: "name", Affinity: 't'}},
		Name:     "test",
	}
	prog := &Program{
		Instructions: instrs,
		NumRegs:     numRegs,
		NumCursors:  numCursors,
	}
	v := NewVDBE(db)
	v.SetProgram(prog)
	rc, err := v.Execute(context.Background())
	return v, rc, err
}

// ─── Tests ───────────────────────────────────────────────────────────────

func TestVDBENilProgram(t *testing.T) {
	v := NewVDBE(nil)
	_, err := v.Execute(context.Background())
	if err == nil {
		t.Error("expected error for nil program")
	}
}

func TestVDBEEmptyProgram(t *testing.T) {
	prog := &Program{Instructions: []Instruction{}, NumRegs: 1}
	v := NewVDBE(nil)
	v.SetProgram(prog)
	rc, err := v.Execute(context.Background())
	if rc != ResultDone {
		t.Errorf("expected ResultDone, got %v", rc)
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// ─── Arithmetic Opcodes ──────────────────────────────────────────────────

func TestOpAdd(t *testing.T) {
	_, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 3, P2: 1},
		{Op: OpInteger, P1: 4, P2: 2},
		{Op: OpAdd, P1: 1, P2: 2, P3: 3},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 4, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
}

func TestArithmeticInt(t *testing.T) {
	tests := []struct {
		op      Opcode
		a, b    int64
		want    int64
	}{
		{OpAdd, 3, 4, 7},
		{OpSubtract, 10, 3, 7},
		{OpMul, 5, 6, 30},
		{OpDivide, 20, 4, 5},
		{OpRemainder, 17, 5, 2},
		{OpBitAnd, 0xFF, 0x0F, 0x0F},
		{OpBitOr, 0xF0, 0x0F, 0xFF},
		{OpShiftLeft, 1, 4, 16},
		{OpShiftRight, 16, 4, 1},
	}
	for _, tt := range tests {
		t.Run(OpcodeName(tt.op), func(t *testing.T) {
			v, rc, _ := exec([]Instruction{
				{Op: OpInteger, P1: int(tt.a), P2: 1},
				{Op: OpInteger, P1: int(tt.b), P2: 2},
				{Op: tt.op, P1: 1, P2: 2, P3: 3},
				{Op: OpHalt, P1: 0, P2: 0},
			}, 4, 0)
			if rc != ResultDone {
				t.Fatalf("unexpected rc: %v", rc)
			}
			got := v.regs[3].IntVal
			if got != tt.want {
				t.Errorf("%v(%d, %d) = %d, want %d", OpcodeName(tt.op), tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestArithmeticFloat(t *testing.T) {
	tests := []struct {
		op      Opcode
		a, b    float64
		want    float64
	}{
		{OpAdd, 3.5, 4.5, 8.0},
		{OpSubtract, 10.0, 3.5, 6.5},
		{OpMul, 2.5, 4.0, 10.0},
		{OpDivide, 20.0, 4.0, 5.0},
	}
	for _, tt := range tests {
		t.Run(OpcodeName(tt.op), func(t *testing.T) {
			v, rc, _ := exec([]Instruction{
				{Op: OpReal, P4: tt.a, P2: 1},
				{Op: OpReal, P4: tt.b, P2: 2},
				{Op: tt.op, P1: 1, P2: 2, P3: 3},
				{Op: OpHalt, P1: 0, P2: 0},
			}, 4, 0)
			if rc != ResultDone {
				t.Fatalf("unexpected rc: %v", rc)
			}
			got := v.regs[3].FloatVal
			if got != tt.want {
				t.Errorf("%v(%v, %v) = %v, want %v", OpcodeName(tt.op), tt.a, tt.b, got, tt.want)
			}
		})
	}
}

func TestOpBitNot(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 0, P2: 1},
		{Op: OpBitNot, P1: 1, P2: 2},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 3, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	got := v.regs[2].IntVal
	if got != -1 { // ^0 = -1 in two's complement
		t.Errorf("BitNot(0) = %d, want -1", got)
	}
}

func TestOpBitNotNonzero(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 0x0F, P2: 1},
		{Op: OpBitNot, P1: 1, P2: 2},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 3, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	got := v.regs[2].IntVal
	if got != ^int64(0x0F) {
		t.Errorf("BitNot(0x0F) = %d, want %d", got, ^int64(0x0F))
	}
}

// ─── Data Loading Opcodes ────────────────────────────────────────────────

func TestOpInteger(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 42, P2: 1},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 2, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[1].Type != MemInt {
		t.Errorf("expected MemInt, got %v", v.regs[1].Type)
	}
	if v.regs[1].IntVal != 42 {
		t.Errorf("expected 42, got %d", v.regs[1].IntVal)
	}
}

func TestOpReal(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpReal, P4: 3.14, P2: 1},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 2, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[1].Type != MemFloat {
		t.Errorf("expected MemFloat, got %v", v.regs[1].Type)
	}
	if v.regs[1].FloatVal != 3.14 {
		t.Errorf("expected 3.14, got %v", v.regs[1].FloatVal)
	}
}

func TestOpString8(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpString8, P4: "hello", P2: 1},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 2, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[1].Type != MemStr {
		t.Errorf("expected MemStr, got %v", v.regs[1].Type)
	}
	if string(v.regs[1].Bytes) != "hello" {
		t.Errorf("expected 'hello', got '%s'", string(v.regs[1].Bytes))
	}
}

func TestOpBlob(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpBlob, P4: []byte{1, 2, 3}, P2: 1},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 2, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[1].Type != MemBlob {
		t.Errorf("expected MemBlob, got %v", v.regs[1].Type)
	}
	if !bytes.Equal(v.regs[1].Bytes, []byte{1, 2, 3}) {
		t.Errorf("expected [1,2,3], got %v", v.regs[1].Bytes)
	}
}

func TestOpNull(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpNull, P1: 0, P2: 1, P3: 0},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 2, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[1].Type != MemNull {
		t.Errorf("expected MemNull, got %v", v.regs[1].Type)
	}
}

func TestOpNullWithP3(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpNull, P1: 0, P2: 1, P3: 2},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 3, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[1].Type != MemNull {
		t.Errorf("reg[1] expected MemNull, got %v", v.regs[1].Type)
	}
	if v.regs[2].Type != MemNull {
		t.Errorf("reg[2] expected MemNull, got %v", v.regs[2].Type)
	}
}

func TestOpCopy(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 99, P2: 1},
		{Op: OpCopy, P1: 1, P2: 2, P3: 1},
		{Op: OpInteger, P1: 0, P2: 1}, // overwrite source
		{Op: OpHalt, P1: 0, P2: 0},
	}, 3, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[2].IntVal != 99 {
		t.Errorf("Copy: expected 99, got %d", v.regs[2].IntVal)
	}
	if v.regs[1].IntVal != 0 {
		t.Errorf("source should be overwritten to 0, got %d", v.regs[1].IntVal)
	}
}

func TestOpCopyMultiple(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 10, P2: 1},
		{Op: OpInteger, P1: 20, P2: 2},
		{Op: OpCopy, P1: 1, P2: 5, P3: 2}, // copy regs 1-2 to 5-6
		{Op: OpHalt, P1: 0, P2: 0},
	}, 7, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[5].IntVal != 10 {
		t.Errorf("Copy multiple: regs[5] expected 10, got %d", v.regs[5].IntVal)
	}
	if v.regs[6].IntVal != 20 {
		t.Errorf("Copy multiple: regs[6] expected 20, got %d", v.regs[6].IntVal)
	}
}

func TestOpSCopy(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 77, P2: 1},
		{Op: OpSCopy, P1: 1, P2: 2},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 3, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[2].IntVal != 77 {
		t.Errorf("SCopy: expected 77, got %d", v.regs[2].IntVal)
	}
}

func TestOpMove(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 55, P2: 1},
		{Op: OpMove, P1: 1, P2: 2, P3: 1},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 3, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[2].IntVal != 55 {
		t.Errorf("Move: dest expected 55, got %d", v.regs[2].IntVal)
	}
	if v.regs[1].Type != MemNull {
		t.Errorf("Move: source should be null after move, got %v", v.regs[1].Type)
	}
}

// ─── Compare/Jump Opcodes ────────────────────────────────────────────────

func TestOpGoto(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpGoto, P1: 0, P2: 3},   // jump to halt
		{Op: OpInteger, P1: 99, P2: 1}, // should be skipped
		{Op: OpInteger, P1: 99, P2: 1}, // should be skipped
		{Op: OpHalt, P1: 0, P2: 0},
	}, 2, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[1].Type != MemNull {
		t.Errorf("Goto should have skipped the Integer instruction, got %v", v.regs[1])
	}
}

func TestOpGosubReturn(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpGoSub, P1: 5, P2: 4},   // call subroutine at 4, return addr in reg 5
		{Op: OpHalt, P1: 0, P2: 0},   // halt
		{Op: OpInteger, P1: 0, P2: 0}, // dead code
		{Op: OpInteger, P1: 0, P2: 0}, // dead code
		{Op: OpInteger, P1: 42, P2: 1}, // subroutine: set reg 1 = 42
		{Op: OpReturn, P1: 5},          // return
	}, 6, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[1].IntVal != 42 {
		t.Errorf("Gosub/Return: expected 42, got %d", v.regs[1].IntVal)
	}
}

func TestOpIf(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 1, P2: 1},  // r1 = 1 (true)
		{Op: OpIf, P1: 1, P2: 4},       // if r1, jump to 4
		{Op: OpInteger, P1: 0, P2: 2},  // skipped
		{Op: OpGoto, P1: 0, P2: 5},     // jump to halt
		{Op: OpInteger, P1: 1, P2: 2},  // taken
		{Op: OpHalt, P1: 0, P2: 0},
	}, 3, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[2].IntVal != 1 {
		t.Errorf("If: expected reg[2]=1, got %d", v.regs[2].IntVal)
	}
}

func TestOpIfNot(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 0, P2: 1},  // r1 = 0 (false)
		{Op: OpIfNot, P1: 1, P2: 4},    // if !r1, jump to 4
		{Op: OpInteger, P1: 0, P2: 2},  // skipped
		{Op: OpGoto, P1: 0, P2: 5},     // jump to halt
		{Op: OpInteger, P1: 1, P2: 2},  // taken
		{Op: OpHalt, P1: 0, P2: 0},
	}, 3, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[2].IntVal != 1 {
		t.Errorf("IfNot: expected reg[2]=1, got %d", v.regs[2].IntVal)
	}
}

func TestOpNot(t *testing.T) {
	tests := []struct {
		input int64
		want  int64
	}{
		{0, 1},
		{1, 0},
		{42, 0},
		{-1, 0},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("Not(%d)", tt.input), func(t *testing.T) {
			v, rc, _ := exec([]Instruction{
				{Op: OpInteger, P1: int(tt.input), P2: 1},
				{Op: OpNot, P1: 1, P2: 2},
				{Op: OpHalt, P1: 0, P2: 0},
			}, 3, 0)
			if rc != ResultDone {
				t.Fatalf("unexpected rc: %v", rc)
			}
			if v.regs[2].IntVal != tt.want {
				t.Errorf("Not(%d) = %d, want %d", tt.input, v.regs[2].IntVal, tt.want)
			}
		})
	}
}

func TestComparisons(t *testing.T) {
	tests := []struct {
		op   Opcode
		a, b int64
		jump bool
	}{
		{OpEq, 5, 5, true},
		{OpEq, 5, 6, false},
		{OpNe, 5, 6, true},
		{OpNe, 5, 5, false},
		{OpLt, 3, 5, true},
		{OpLt, 5, 3, false},
		{OpLt, 5, 5, false},
		{OpLe, 3, 5, true},
		{OpLe, 5, 5, true},
		{OpLe, 6, 5, false},
		{OpGt, 5, 3, true},
		{OpGt, 3, 5, false},
		{OpGt, 5, 5, false},
		{OpGe, 5, 3, true},
		{OpGe, 5, 5, true},
		{OpGe, 3, 5, false},
	}
	for _, tt := range tests {
		name := fmt.Sprintf("%s(%d,%d)=%v", OpcodeName(tt.op), tt.a, tt.b, tt.jump)
		t.Run(name, func(t *testing.T) {
			instrs := []Instruction{
				{Op: OpInteger, P1: int(tt.a), P2: 1},
				{Op: OpInteger, P1: int(tt.b), P2: 2},
				{Op: tt.op, P1: 1, P2: 5, P3: 2}, // if true, jump to instr 5
				{Op: OpInteger, P1: 0, P2: 3},     // not taken
				{Op: OpGoto, P1: 0, P2: 6},
				{Op: OpInteger, P1: 1, P2: 3},     // taken
				{Op: OpHalt, P1: 0, P2: 0},
			}
			v, rc, _ := exec(instrs, 4, 0)
			if rc != ResultDone {
				t.Fatalf("unexpected rc: %v", rc)
			}
			got := v.regs[3].IntVal == 1
			if got != tt.jump {
				t.Errorf("%s(%d,%d): jump=%v, want %v", OpcodeName(tt.op), tt.a, tt.b, got, tt.jump)
			}
		})
	}
}

func TestComparisonWithNull(t *testing.T) {
	// NULL comparisons should never jump (per SQLite semantics)
	ops := []Opcode{OpEq, OpNe, OpLt, OpLe, OpGt, OpGe}
	for _, op := range ops {
		t.Run(OpcodeName(op)+"_null", func(t *testing.T) {
			instrs := []Instruction{
				{Op: OpNull, P1: 0, P2: 1},
				{Op: OpInteger, P1: 5, P2: 2},
				{Op: op, P1: 1, P2: 5, P3: 2},
				{Op: OpInteger, P1: 0, P2: 3}, // not taken path
				{Op: OpGoto, P1: 0, P2: 6},
				{Op: OpInteger, P1: 1, P2: 3}, // taken path
				{Op: OpHalt, P1: 0, P2: 0},
			}
			v, rc, _ := exec(instrs, 4, 0)
			if rc != ResultDone {
				t.Fatalf("unexpected rc: %v", rc)
			}
			if v.regs[3].IntVal == 1 {
				t.Errorf("%s with NULL should not jump", OpcodeName(op))
			}
		})
	}
}

func TestComparisonFloatInt(t *testing.T) {
	// Float 5.0 should equal Int 5
	instrs := []Instruction{
		{Op: OpReal, P4: 5.0, P2: 1},
		{Op: OpInteger, P1: 5, P2: 2},
		{Op: OpEq, P1: 1, P2: 5, P3: 2},
		{Op: OpInteger, P1: 0, P2: 3},
		{Op: OpGoto, P1: 0, P2: 6},
		{Op: OpInteger, P1: 1, P2: 3},
		{Op: OpHalt, P1: 0, P2: 0},
	}
	v, rc, _ := exec(instrs, 4, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[3].IntVal != 1 {
		t.Errorf("Eq(5.0, 5) should be true")
	}
}

// ─── Control Flow ─────────────────────────────────────────────────────────

func TestOpInit(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInit, P1: 0, P2: 2},       // jump to instr 2
		{Op: OpInteger, P1: 99, P2: 1},   // should be skipped
		{Op: OpHalt, P1: 0, P2: 0},
	}, 2, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[1].Type != MemNull {
		t.Errorf("Init should have skipped instr 1")
	}
}

func TestOpHalt(t *testing.T) {
	_, rc, _ := exec([]Instruction{
		{Op: OpHalt, P1: 0, P2: 0},
	}, 1, 0)
	if rc != ResultDone {
		t.Errorf("expected ResultDone, got %v", rc)
	}
}

func TestOpHaltWithError(t *testing.T) {
	_, rc, err := exec([]Instruction{
		{Op: OpHalt, P1: 1, P2: 0},
	}, 1, 0)
	if rc != ResultError {
		t.Errorf("expected ResultError, got %v", rc)
	}
	if err == nil {
		t.Error("expected error")
	}
}

func TestOpNoop(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 42, P2: 1},
		{Op: OpNoop},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 2, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[1].IntVal != 42 {
		t.Errorf("Noop shouldn't affect registers")
	}
}

// ─── Transaction Opcodes ─────────────────────────────────────────────────

func TestOpTransaction(t *testing.T) {
	db := newMockDB()
	prog := &Program{
		Instructions: []Instruction{
			{Op: OpTransaction, P1: 0, P2: 0}, // read txn
			{Op: OpHalt, P1: 0, P2: 0},
		},
		NumRegs:    1,
		NumCursors: 0,
	}
	v := NewVDBE(db)
	v.SetProgram(prog)
	rc, _ := v.Execute(context.Background())
	if rc != ResultDone {
		t.Errorf("expected ResultDone, got %v", rc)
	}
	if !db.beganTx {
		t.Error("expected transaction to begin")
	}
	if db.beganWriteTx {
		t.Error("expected read transaction, got write")
	}
}

func TestOpTransactionWrite(t *testing.T) {
	db := newMockDB()
	prog := &Program{
		Instructions: []Instruction{
			{Op: OpTransaction, P1: 0, P2: 1}, // write txn
			{Op: OpHalt, P1: 0, P2: 0},
		},
		NumRegs:    1,
		NumCursors: 0,
	}
	v := NewVDBE(db)
	v.SetProgram(prog)
	rc, _ := v.Execute(context.Background())
	if rc != ResultDone {
		t.Errorf("expected ResultDone, got %v", rc)
	}
	if !db.beganWriteTx {
		t.Error("expected write transaction")
	}
}

func TestOpAutoCommit(t *testing.T) {
	db := newMockDB()
	db.autoCommit = false
	prog := &Program{
		Instructions: []Instruction{
			{Op: OpAutoCommit, P1: 1, P2: 0},
			{Op: OpHalt, P1: 0, P2: 0},
		},
		NumRegs:    1,
		NumCursors: 0,
	}
	v := NewVDBE(db)
	v.SetProgram(prog)
	rc, _ := v.Execute(context.Background())
	if rc != ResultDone {
		t.Errorf("expected ResultDone, got %v", rc)
	}
	if !db.autoCommit {
		t.Error("expected autocommit to be on")
	}
}

// ─── Record Operations ───────────────────────────────────────────────────

func TestOpMakeRecord(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 42, P2: 1},
		{Op: OpString8, P4: "hello", P2: 2},
		{Op: OpMakeRecord, P1: 1, P2: 2, P3: 3}, // make record from 2 regs starting at 1
		{Op: OpHalt, P1: 0, P2: 0},
	}, 4, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[3].Type != MemBlob {
		t.Fatalf("expected MemBlob (record), got %v", v.regs[3].Type)
	}
	// Verify we can parse the record back
	values, err := ParseRecord(v.regs[3].Bytes)
	if err != nil {
		t.Fatalf("failed to parse record: %v", err)
	}
	if len(values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(values))
	}
	if values[0].IntVal != 42 {
		t.Errorf("value[0] expected 42, got %d", values[0].IntVal)
	}
	if string(values[1].Bytes) != "hello" {
		t.Errorf("value[1] expected 'hello', got '%s'", string(values[1].Bytes))
	}
}

func TestOpMakeRecordWithNull(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpNull, P1: 0, P2: 1},
		{Op: OpInteger, P1: 10, P2: 2},
		{Op: OpMakeRecord, P1: 1, P2: 2, P3: 3},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 4, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	values, err := ParseRecord(v.regs[3].Bytes)
	if err != nil {
		t.Fatalf("failed to parse record: %v", err)
	}
	if values[0].Type != "null" {
		t.Errorf("expected null, got %s", values[0].Type)
	}
	if values[1].IntVal != 10 {
		t.Errorf("expected 10, got %d", values[1].IntVal)
	}
}

// ─── Result Row ───────────────────────────────────────────────────────────

func TestOpResultRow(t *testing.T) {
	var capturedRegs []Mem
	var capturedStart, capturedCount int

	db := newMockDB()
	prog := &Program{
		Instructions: []Instruction{
			{Op: OpInteger, P1: 42, P2: 1},
			{Op: OpString8, P4: "test", P2: 2},
			{Op: OpResultRow, P1: 1, P2: 2},
			{Op: OpHalt, P1: 0, P2: 0},
		},
		NumRegs: 3,
	}
	v := NewVDBE(db)
	v.SetProgram(prog)
	v.SetResultRowCallback(func(regs []Mem, startIdx, count int) {
		capturedStart = startIdx
		capturedCount = count
		capturedRegs = make([]Mem, count)
		for i := 0; i < count; i++ {
			capturedRegs[i] = *regs[startIdx+i].Copy()
		}
	})

	rc, _ := v.Execute(context.Background())
	if rc != ResultDone {
		t.Errorf("expected ResultDone, got %v", rc)
	}
	if capturedStart != 1 || capturedCount != 2 {
		t.Errorf("ResultRow callback: start=%d count=%d, want 1,2", capturedStart, capturedCount)
	}
	if capturedRegs[0].IntVal != 42 {
		t.Errorf("result[0] = %d, want 42", capturedRegs[0].IntVal)
	}
	if string(capturedRegs[1].Bytes) != "test" {
		t.Errorf("result[1] = %s, want 'test'", string(capturedRegs[1].Bytes))
	}
}

// ─── VdbeBuilder ──────────────────────────────────────────────────────────

func TestVdbeBuilder(t *testing.T) {
	vb := NewVdbeBuilder()
	vb.SetNumRegs(5)
	vb.AddOp(OpInteger, 42, 0, 1)
	vb.AddOp(OpHalt, 0, 0, 0)

	prog := vb.Finalize()
	if len(prog.Instructions) != 2 {
		t.Errorf("expected 2 instructions, got %d", len(prog.Instructions))
	}
	if prog.NumRegs != 5 {
		t.Errorf("expected 5 registers, got %d", prog.NumRegs)
	}
}

func TestVdbeBuilderWithJump(t *testing.T) {
	vb := NewVdbeBuilder()
	vb.SetNumRegs(3)

	// Build: if r1 then goto L1 else halt
	gotoPC := vb.AddOpJump(OpIf, 1, 0) // P2 will be resolved later
	vb.AddOp(OpHalt, 0, 0, 0)
	vb.SetLabel("L1") // this is instruction 2
	vb.AddOp(OpInteger, 1, 2, 0)
	vb.AddOp(OpHalt, 0, 0, 0)
	vb.ResolveLabel(gotoPC, "L1")

	prog := vb.Finalize()
	if prog.Instructions[0].P2 != 2 {
		t.Errorf("jump not resolved: P2=%d, want 2", prog.Instructions[0].P2)
	}
}

func TestVdbeBuilderAddOp4(t *testing.T) {
	vb := NewVdbeBuilder()
	vb.AddOp4(OpString8, 0, 1, 0, "hello", "load string")
	prog := vb.Finalize()

	if prog.Instructions[0].P4.(string) != "hello" {
		t.Errorf("P4 not stored correctly")
	}
	if prog.Instructions[0].Comment != "load string" {
		t.Errorf("comment not stored correctly")
	}
}

func TestVdbeBuilderAddOpList(t *testing.T) {
	vb := NewVdbeBuilder()
	instrs := []Instruction{
		{Op: OpInteger, P1: 1, P2: 1},
		{Op: OpInteger, P1: 2, P2: 2},
	}
	vb.AddOpList(instrs)
	prog := vb.Finalize()
	if len(prog.Instructions) != 2 {
		t.Errorf("expected 2 instructions, got %d", len(prog.Instructions))
	}
}

// ─── VdbeExplain / VdbeList ──────────────────────────────────────────────

func TestOpcodeName(t *testing.T) {
	tests := []struct {
		op   Opcode
		name string
	}{
		{OpAdd, "Add"},
		{OpHalt, "Halt"},
		{OpGoto, "Goto"},
		{OpColumn, "Column"},
		{OpInsert, "Insert"},
	}
	for _, tt := range tests {
		if got := OpcodeName(tt.op); got != tt.name {
			t.Errorf("OpcodeName(%d) = %q, want %q", tt.op, got, tt.name)
		}
	}
}

func TestFormatInstruction(t *testing.T) {
	instr := Instruction{Op: OpAdd, P1: 1, P2: 2, P3: 3, Comment: "test add"}
	s := FormatInstruction(0, instr)
	if !strings.Contains(s, "Add") {
		t.Errorf("format missing opcode name: %s", s)
	}
	if !strings.Contains(s, "test add") {
		t.Errorf("format missing comment: %s", s)
	}
}

func TestFormatInstructionWithP4String(t *testing.T) {
	instr := Instruction{Op: OpString8, P4: "hello", P2: 1}
	s := FormatInstruction(5, instr)
	if !strings.Contains(s, `P4="hello"`) {
		t.Errorf("format missing P4 string: %s", s)
	}
}

func TestVdbeList(t *testing.T) {
	prog := &Program{
		Instructions: []Instruction{
			{Op: OpInteger, P1: 42, P2: 1},
			{Op: OpHalt, P1: 0, P2: 0},
		},
	}
	listing := VdbeList(prog)
	if !strings.Contains(listing, "Integer") {
		t.Errorf("listing missing Integer: %s", listing)
	}
	if !strings.Contains(listing, "Halt") {
		t.Errorf("listing missing Halt: %s", listing)
	}
}

func TestVdbeExplain(t *testing.T) {
	prog := &Program{
		Instructions: []Instruction{
			{Op: OpInit, P1: 0, P2: 1},
			{Op: OpHalt, P1: 0, P2: 0},
		},
	}
	var buf bytes.Buffer
	VdbeExplain(&buf, prog)
	output := buf.String()
	if !strings.Contains(output, "Init") {
		t.Errorf("explain missing Init: %s", output)
	}
	if !strings.Contains(output, "Halt") {
		t.Errorf("explain missing Halt: %s", output)
	}
}

// ─── VDBE Resource Management ─────────────────────────────────────────────

func TestVDBECleanupCursors(t *testing.T) {
	mc := &mockCursor{rows: []mockRow{{rowid: 1, data: []byte("test")}}}
	v := &VDBE{
		cursors: []*VdbeCursor{
			{Cursor: mc, RootPage: 1},
		},
	}
	v.CleanupCursors()
	if !mc.closed {
		t.Error("cursor not closed")
	}
}

func TestVDBEClose(t *testing.T) {
	mc := &mockCursor{rows: []mockRow{{rowid: 1, data: []byte("test")}}}
	v := &VDBE{
		cursors: []*VdbeCursor{
			{Cursor: mc, RootPage: 1},
		},
		regs: make([]Mem, 5),
		prog: &Program{},
	}
	v.Close()
	if v.regs != nil {
		t.Error("regs should be nil after close")
	}
	if v.cursors != nil {
		t.Error("cursors should be nil after close")
	}
	if v.prog != nil {
		t.Error("prog should be nil after close")
	}
}

func TestVDBEGetSetRegister(t *testing.T) {
	v := &VDBE{
		regs: make([]Mem, 5),
	}
	for i := range v.regs {
		v.regs[i] = Mem{Type: MemNull, IsNull: true}
	}

	// Set a register
	v.SetRegister(2, NewMemInt(42))
	m := v.GetRegister(2)
	if m.IntVal != 42 {
		t.Errorf("expected 42, got %d", m.IntVal)
	}

	// Out of bounds
	m = v.GetRegister(100)
	if m.Type != MemNull {
		t.Errorf("expected null for out of bounds, got %v", m.Type)
	}
}

// ─── Complex Programs ─────────────────────────────────────────────────────

func TestComplexArithmeticExpression(t *testing.T) {
	// Compute (3 + 4) * (10 - 2) = 56
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 3, P2: 1},   // r1 = 3
		{Op: OpInteger, P1: 4, P2: 2},   // r2 = 4
		{Op: OpAdd, P1: 1, P2: 2, P3: 3}, // r3 = 3 + 4 = 7
		{Op: OpInteger, P1: 10, P2: 4},  // r4 = 10
		{Op: OpInteger, P1: 2, P2: 5},   // r5 = 2
		{Op: OpSubtract, P1: 4, P2: 5, P3: 6}, // r6 = 10 - 2 = 8
		{Op: OpMul, P1: 3, P2: 6, P3: 7}, // r7 = 7 * 8 = 56
		{Op: OpHalt, P1: 0, P2: 0},
	}, 8, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[7].IntVal != 56 {
		t.Errorf("(3+4)*(10-2) = %d, want 56", v.regs[7].IntVal)
	}
}

func TestLoop(t *testing.T) {
	// Sum 1..5 = 15
	// r1 = sum, r2 = counter, r3 = limit, r4 = constant 1
	instrs := []Instruction{
		{Op: OpInteger, P1: 0, P2: 1},  // 0: r1 = 0
		{Op: OpInteger, P1: 1, P2: 2},  // 1: r2 = 1
		{Op: OpInteger, P1: 5, P2: 3},  // 2: r3 = 5
		{Op: OpInteger, P1: 1, P2: 4},  // 3: r4 = 1
		// loop (PC=4):
		{Op: OpAdd, P1: 1, P2: 2, P3: 1},   // 4: r1 += r2
		{Op: OpAdd, P1: 2, P2: 4, P3: 2},   // 5: r2 += r4
		{Op: OpGt, P1: 2, P2: 9, P3: 3},    // 6: if r2 > r3 goto 9 (out of range = exit)
		{Op: OpGoto, P1: 0, P2: 4},          // 7: loop
		{Op: OpHalt, P1: 0, P2: 0},          // 8: halt
	}
	v, rc, _ := exec(instrs, 5, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	got := v.regs[1].IntValue()
	if got != 15 {
		t.Errorf("sum(1..5) = %d, want 15", got)
	}
}

func TestConditionalSum(t *testing.T) {
	// Sum only even numbers from 1 to 4: 2 + 4 = 6
	// r1 = sum, r2 = counter, r3 = limit, r4 = mod temp, r5 = 2, r6 = 1, r7 = 0
	instrs := []Instruction{
		{Op: OpInteger, P1: 0, P2: 1},  // 0: r1 = 0
		{Op: OpInteger, P1: 1, P2: 2},  // 1: r2 = 1
		{Op: OpInteger, P1: 4, P2: 3},  // 2: r3 = 4
		{Op: OpInteger, P1: 2, P2: 5},  // 3: r5 = 2
		{Op: OpInteger, P1: 1, P2: 6},  // 4: r6 = 1
		{Op: OpInteger, P1: 0, P2: 7},  // 5: r7 = 0
		// loop (PC=6):
		{Op: OpRemainder, P1: 2, P2: 5, P3: 4}, // 6: r4 = r2 % 2
		{Op: OpNe, P1: 4, P2: 9, P3: 7},        // 7: if r4 != 0 (odd), skip add → goto 9
		{Op: OpAdd, P1: 1, P2: 2, P3: 1},       // 8: r1 += r2 (even: add)
		{Op: OpAdd, P1: 2, P2: 6, P3: 2},       // 9: r2++ (always executed)
		{Op: OpGt, P1: 2, P2: 12, P3: 3},       // 10: if r2 > r3, exit (goto 12 = halt)
		{Op: OpGoto, P1: 0, P2: 6},              // 11: loop
		{Op: OpHalt, P1: 0, P2: 0},              // 12: halt
	}
	v, rc, _ := exec(instrs, 8, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	got := v.regs[1].IntValue()
	if got != 6 {
		t.Errorf("sum of even(1..4) = %d, want 6", got)
	}
}

func TestStringOperations(t *testing.T) {
	// Load a string and check it
	v, rc, _ := exec([]Instruction{
		{Op: OpString8, P4: "hello world", P2: 1},
		{Op: OpString8, P4: "hello world", P2: 2},
		{Op: OpEq, P1: 1, P2: 5, P3: 2},    // if equal, jump
		{Op: OpInteger, P1: 0, P2: 3},       // not equal path
		{Op: OpGoto, P1: 0, P2: 6},
		{Op: OpInteger, P1: 1, P2: 3},       // equal path
		{Op: OpHalt, P1: 0, P2: 0},
	}, 4, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[3].IntVal != 1 {
		t.Errorf("string comparison failed")
	}
}

// ─── NewVDBEFromBuilder ───────────────────────────────────────────────────

func TestNewVDBEFromBuilder(t *testing.T) {
	vb := NewVdbeBuilder()
	vb.SetNumRegs(2)
	vb.AddOp(OpInteger, 10, 0, 1)
	vb.AddOp(OpHalt, 0, 0, 0)

	db := newMockDB()
	v := NewVDBEFromBuilder(db, vb)
	if v == nil {
		t.Fatal("expected non-nil VDBE")
	}
	if v.prog == nil {
		t.Fatal("expected program to be set")
	}
	if len(v.prog.Instructions) != 2 {
		t.Errorf("expected 2 instructions, got %d", len(v.prog.Instructions))
	}
}

// ─── Mixed Type Arithmetic ────────────────────────────────────────────────

func TestMixedIntFloatArithmetic(t *testing.T) {
	// int + float should produce float
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 3, P2: 1},
		{Op: OpReal, P4: 4.5, P2: 2},
		{Op: OpAdd, P1: 1, P2: 2, P3: 3},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 4, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[3].Type != MemFloat {
		t.Errorf("int + float should be float, got %v", v.regs[3].Type)
	}
	if v.regs[3].FloatVal != 7.5 {
		t.Errorf("3 + 4.5 = %v, want 7.5", v.regs[3].FloatVal)
	}
}

// ─── Context Cancellation ─────────────────────────────────────────────────

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	db := newMockDB()
	prog := &Program{
		Instructions: []Instruction{
			{Op: OpInteger, P1: 1, P2: 1},
			{Op: OpHalt, P1: 0, P2: 0},
		},
		NumRegs: 2,
	}
	v := NewVDBE(db)
	v.SetProgram(prog)
	_, err := v.Execute(ctx)
	if err == nil {
		t.Error("expected error from cancelled context")
	}
}

// ─── PC and Result ────────────────────────────────────────────────────────

func TestVDBEPC(t *testing.T) {
	v := &VDBE{pc: 42}
	if v.PC() != 42 {
		t.Errorf("expected PC 42, got %d", v.PC())
	}
}

func TestVDBEResult(t *testing.T) {
	v := &VDBE{rc: ResultRow}
	if v.Result() != ResultRow {
		t.Errorf("expected ResultRow, got %v", v.Result())
	}
}

func TestVDBEError(t *testing.T) {
	v := &VDBE{err: fmt.Errorf("test error")}
	if v.Error() == nil {
		t.Error("expected error")
	}
}

func TestVDBEProgram(t *testing.T) {
	prog := &Program{Instructions: []Instruction{{Op: OpHalt}}}
	v := &VDBE{prog: prog}
	if v.Program() != prog {
		t.Error("expected same program")
	}
}

// ─── Edge Cases ───────────────────────────────────────────────────────────

func TestShiftLeftLarge(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 1, P2: 1},
		{Op: OpInteger, P1: 10, P2: 2},
		{Op: OpShiftLeft, P1: 1, P2: 2, P3: 3},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 4, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[3].IntVal != 1024 {
		t.Errorf("1 << 10 = %d, want 1024", v.regs[3].IntVal)
	}
}

func TestDivideByZero(t *testing.T) {
	// Float division by zero produces Inf, not panic
	v, rc, _ := exec([]Instruction{
		{Op: OpReal, P4: 10.0, P2: 1},
		{Op: OpInteger, P1: 0, P2: 2},
		{Op: OpDivide, P1: 1, P2: 2, P3: 3},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 4, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[3].Type != MemFloat {
		t.Errorf("expected float result, got %v", v.regs[3].Type)
	}
}

func TestNegativeRemainder(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: -17, P2: 1},
		{Op: OpInteger, P1: 5, P2: 2},
		{Op: OpRemainder, P1: 1, P2: 2, P3: 3},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 4, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[3].IntVal != -2 {
		t.Errorf("-17 %% 5 = %d, want -2", v.regs[3].IntVal)
	}
}

func TestComparisonStringInt(t *testing.T) {
	// String "5" should equal int 5 via coercion
	v, rc, _ := exec([]Instruction{
		{Op: OpString8, P4: "5", P2: 1},
		{Op: OpInteger, P1: 5, P2: 2},
		{Op: OpEq, P1: 1, P2: 5, P3: 2},    // if equal, jump to set r3=1
		{Op: OpInteger, P1: 0, P2: 3},       // not equal path
		{Op: OpGoto, P1: 0, P2: 6},          // goto halt
		{Op: OpInteger, P1: 1, P2: 3},       // equal path: r3=1
		{Op: OpHalt, P1: 0, P2: 0},
	}, 4, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[3].IntVal != 1 {
		t.Errorf("Eq('5', 5) should be true via coercion")
	}
}

func TestComparisonStrings(t *testing.T) {
	// String comparison: "abc" < "def"
	v, rc, _ := exec([]Instruction{
		{Op: OpString8, P4: "abc", P2: 1},
		{Op: OpString8, P4: "def", P2: 2},
		{Op: OpLt, P1: 1, P2: 5, P3: 2},
		{Op: OpInteger, P1: 0, P2: 3},
		{Op: OpGoto, P1: 0, P2: 6},
		{Op: OpInteger, P1: 1, P2: 3},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 4, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[3].IntVal != 1 {
		t.Errorf("Lt('abc', 'def') should be true")
	}
}

func TestComparisonFloats(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpReal, P4: 3.14, P2: 1},
		{Op: OpReal, P4: 2.71, P2: 2},
		{Op: OpGt, P1: 1, P2: 5, P3: 2},
		{Op: OpInteger, P1: 0, P2: 3},
		{Op: OpGoto, P1: 0, P2: 6},
		{Op: OpInteger, P1: 1, P2: 3},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 4, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[3].IntVal != 1 {
		t.Errorf("Gt(3.14, 2.71) should be true")
	}
}

func TestMoveMultiple(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpInteger, P1: 10, P2: 1},
		{Op: OpInteger, P1: 20, P2: 2},
		{Op: OpMove, P1: 1, P2: 5, P3: 2},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 7, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	if v.regs[5].IntVal != 10 {
		t.Errorf("Move: regs[5] expected 10, got %d", v.regs[5].IntVal)
	}
	if v.regs[6].IntVal != 20 {
		t.Errorf("Move: regs[6] expected 20, got %d", v.regs[6].IntVal)
	}
	if v.regs[1].Type != MemNull {
		t.Errorf("Move: regs[1] should be null")
	}
	if v.regs[2].Type != MemNull {
		t.Errorf("Move: regs[2] should be null")
	}
}

func TestMakeRecordFloat(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpReal, P4: 3.14, P2: 1},
		{Op: OpMakeRecord, P1: 1, P2: 1, P3: 2},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 3, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	values, err := ParseRecord(v.regs[2].Bytes)
	if err != nil {
		t.Fatalf("failed to parse record: %v", err)
	}
	if values[0].FloatVal != 3.14 {
		t.Errorf("expected 3.14, got %v", values[0].FloatVal)
	}
}

func TestMakeRecordBlob(t *testing.T) {
	v, rc, _ := exec([]Instruction{
		{Op: OpBlob, P4: []byte{0xDE, 0xAD}, P2: 1},
		{Op: OpMakeRecord, P1: 1, P2: 1, P3: 2},
		{Op: OpHalt, P1: 0, P2: 0},
	}, 3, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	values, err := ParseRecord(v.regs[2].Bytes)
	if err != nil {
		t.Fatalf("failed to parse record: %v", err)
	}
	if !bytes.Equal(values[0].Bytes, []byte{0xDE, 0xAD}) {
		t.Errorf("expected [DE AD], got %X", values[0].Bytes)
	}
}

// ─── Integration: Fibonacci ───────────────────────────────────────────────

func TestFibonacci(t *testing.T) {
	// Compute Fibonacci: after 9 iterations, r2 = fib(10) = 55
	// r1 = a (prev), r2 = b (curr), r3 = counter, r4 = limit, r5 = temp
	instrs := []Instruction{
		{Op: OpInteger, P1: 0, P2: 1},   // r1 = 0 (a)
		{Op: OpInteger, P1: 1, P2: 2},   // r2 = 1 (b)
		{Op: OpInteger, P1: 0, P2: 3},   // r3 = 0 (counter)
		{Op: OpInteger, P1: 9, P2: 4},   // r4 = 9 (limit: 9 iterations → fib(10))
		{Op: OpInteger, P1: 1, P2: 6},   // r6 = 1
		// loop (PC=5):
		{Op: OpAdd, P1: 1, P2: 2, P3: 5},    // r5 = a + b
		{Op: OpSCopy, P1: 2, P2: 1},          // r1 = b
		{Op: OpSCopy, P1: 5, P2: 2},          // r2 = r5
		{Op: OpAdd, P1: 3, P2: 6, P3: 3},    // counter++
		{Op: OpGe, P1: 3, P2: 11, P3: 4},    // if counter >= limit, halt
		{Op: OpGoto, P1: 0, P2: 5},          // loop
		{Op: OpHalt, P1: 0, P2: 0},
	}
	v, rc, _ := exec(instrs, 7, 0)
	if rc != ResultDone {
		t.Fatalf("unexpected rc: %v", rc)
	}
	got := v.regs[2].IntValue()
	if got != 55 {
		t.Errorf("fib(10) = %d, want 55", got)
	}
}

// ─── VdbeBuilder ResolveJump ──────────────────────────────────────────────

func TestVdbeBuilderResolveJump(t *testing.T) {
	vb := NewVdbeBuilder()
	vb.SetNumRegs(3)

	// Build: r1 = 1; if r1 then r2 = 1 else r2 = 0; halt
	vb.AddOp(OpInteger, 1, 1, 0)           // 0: r1 = 1 (P1=1 value, P2=1 reg)
	ifPC := vb.AddOpJump(OpIf, 1, 0)        // 1: if r1, goto ??? (resolve later)
	vb.AddOp(OpInteger, 0, 2, 0)           // 2: r2 = 0 (else branch)
	haltPC := vb.CurrentPC()
	vb.AddOp(OpGoto, 0, haltPC+2, 0)       // 3: goto end (5)
	vb.AddOp(OpInteger, 1, 2, 0)           // 4: r2 = 1 (if branch)
	// end (PC=5):
	vb.AddOp(OpHalt, 0, 0, 0)              // 5: halt

	vb.ResolveJump(ifPC, 4) // resolve if to goto 4 (set r2=1)

	prog := vb.Finalize()
	db := newMockDB()
	v := NewVDBE(db)
	v.SetProgram(prog)
	rc, _ := v.Execute(context.Background())

	if rc != ResultDone {
		t.Errorf("expected ResultDone, got %v", rc)
	}
	if v.regs[2].IntVal != 1 {
		t.Errorf("if-else: expected r2=1, got %d", v.regs[2].IntVal)
	}
}

// ─── Comprehensive integration test ───────────────────────────────────────

func TestIntegrationComplexProgram(t *testing.T) {
	// A program that exercises many opcodes:
	// 1. Load constants
	// 2. Arithmetic
	// 3. Comparisons and branching
	// 4. Gosub/Return
	// 5. MakeRecord
	// 6. ResultRow
	var results [][]Mem
	db := newMockDB()

	prog := &Program{
		Instructions: []Instruction{
			{Op: OpInit, P1: 0, P2: 1},          // 0: init -> jump to 1
			{Op: OpInteger, P1: 10, P2: 1},      // 1: r1 = 10
			{Op: OpInteger, P1: 20, P2: 2},      // 2: r2 = 20
			{Op: OpAdd, P1: 1, P2: 2, P3: 3},    // 3: r3 = 30
			{Op: OpSubtract, P1: 2, P2: 1, P3: 4}, // 4: r4 = 10
			{Op: OpMul, P1: 3, P2: 4, P3: 5},    // 5: r5 = 300
			{Op: OpDivide, P1: 5, P2: 4, P3: 6}, // 6: r6 = 30
			{Op: OpRemainder, P1: 5, P2: 1, P3: 7}, // 7: r7 = 0
			{Op: OpBitAnd, P1: 5, P2: 2, P3: 8}, // 8: r8 = 300 & 20 = 4
			{Op: OpBitOr, P1: 8, P2: 1, P3: 9},  // 9: r9 = 4 | 10 = 14
			{Op: OpString8, P4: "result", P2: 10}, // 10: r10 = "result"
			{Op: OpGoSub, P1: 20, P2: 14},        // 11: call subroutine
			{Op: OpResultRow, P1: 10, P2: 2},      // 12: result r10..r11
			{Op: OpHalt, P1: 0, P2: 0},            // 13: halt
			// subroutine: double r6
			{Op: OpAdd, P1: 6, P2: 6, P3: 11},    // 14: r11 = r6 + r6 = 60
			{Op: OpReturn, P1: 20},                // 15: return
		},
		NumRegs:    21,
		NumCursors: 0,
	}

	v := NewVDBE(db)
	v.SetProgram(prog)
	v.SetResultRowCallback(func(regs []Mem, startIdx, count int) {
		row := make([]Mem, count)
		for i := 0; i < count; i++ {
			row[i] = *regs[startIdx+i].Copy()
		}
		results = append(results, row)
	})

	rc, err := v.Execute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rc != ResultDone {
		t.Errorf("expected ResultDone, got %v", rc)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result row, got %d", len(results))
	}
	if string(results[0][0].Bytes) != "result" {
		t.Errorf("result[0] = %s, want 'result'", string(results[0][0].Bytes))
	}
	if results[0][1].IntVal != 60 {
		t.Errorf("result[1] = %d, want 60", results[0][1].IntVal)
	}

	// Verify intermediate computations
	if v.regs[3].IntVal != 30 {
		t.Errorf("r3 = %d, want 30", v.regs[3].IntVal)
	}
	if v.regs[5].IntVal != 300 {
		t.Errorf("r5 = %d, want 300", v.regs[5].IntVal)
	}
	if v.regs[6].IntVal != 30 {
		t.Errorf("r6 = %d, want 30", v.regs[6].IntVal)
	}
	if v.regs[8].IntVal != 4 {
		t.Errorf("r8 = %d, want 4", v.regs[8].IntVal)
	}
	if v.regs[9].IntVal != 14 {
		t.Errorf("r9 = %d, want 14", v.regs[9].IntVal)
	}
}
