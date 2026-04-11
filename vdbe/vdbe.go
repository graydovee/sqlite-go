package vdbe

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
)

// ResultCode represents the result of VDBE execution.
type ResultCode int

const (
	ResultOK      ResultCode = 0
	ResultDone    ResultCode = 101
	ResultRow     ResultCode = 100
	ResultError   ResultCode = 1
	ResultBusy    ResultCode = 5
	ResultNoMem   ResultCode = 7
	ResultIOErr   ResultCode = 10
	ResultCorrupt ResultCode = 11
)

// VdbeCursor wraps a BTree cursor for use by the VDBE.
type VdbeCursor struct {
	Cursor   interface{} // Will be btree.BTCursor at runtime
	RootPage int
	Writeable bool
	NullRow  bool
	Ordered  bool
}

// ColumnInfo describes column metadata for a table.
type ColumnInfo struct {
	Name     string
	Affinity byte // 't'=text, 'n'=numeric, 'i'=integer, 'b'=blob, 'u'=unspecified
}

// TableInfo describes a table's metadata.
type TableInfo struct {
	RootPage int
	Columns  []ColumnInfo
	Name     string
}

// Database is the interface the VDBE uses to access database operations.
type Database interface {
	// GetTableInfo returns table metadata for the given root page.
	GetTableInfo(rootPage int) (*TableInfo, error)
	// GetCursor opens a cursor on the given root page.
	GetCursor(rootPage int, write bool) (interface{}, error)
	// BeginTransaction starts a transaction.
	BeginTransaction(write bool) error
	// Commit commits the current transaction.
	Commit() error
	// Rollback rolls back the current transaction.
	Rollback() error
	// AutoCommit returns whether autocommit is enabled.
	AutoCommit() bool
	// SetAutoCommit enables or disables autocommit.
	SetAutoCommit(on bool)
	// Changes returns the number of rows changed by the last statement.
	Changes() int64
	// TotalChanges returns the total number of rows changed.
	TotalChanges() int64
	// LastInsertRowID returns the last inserted rowid.
	LastInsertRowID() int64
	// SetLastInsertRowID sets the last inserted rowid.
	SetLastInsertRowID(rowid int64)
}

// Cursor is the interface for B-Tree cursor operations used by VDBE opcodes.
type Cursor interface {
	Close() error
	First() (bool, error)
	Last() (bool, error)
	Next() (bool, error)
	Prev() (bool, error)
	Key() []byte
	Data() ([]byte, error)
	RowID() int64
	IsValid() bool
}

// Inserter is the interface for cursor insert operations.
type Inserter interface {
	Insert(cursor interface{}, key []byte, data []byte, rowid int64, seekResult int) error
}

// Deleter is the interface for cursor delete operations.
type Deleter interface {
	Delete(cursor interface{}) error
}

// Seeker is the interface for cursor seek operations.
type Seeker interface {
	Seek(key []byte, op int) (bool, error)
}

// CookieStore is the interface for database cookie (schema version) operations.
type CookieStore interface {
	GetCookie(id int) (int64, error)
	SetCookie(id int, value int64) error
}

// BTreeCreator is the interface for creating new B-Tree structures.
type BTreeCreator interface {
	CreateBTree(flags int) (int, error)
}

// BTreeDestroyer is the interface for destroying B-Tree structures.
type BTreeDestroyer interface {
	DestroyBTree(rootPage int) error
}

// Program represents a compiled VDBE program.
type Program struct {
	Instructions []Instruction
	NumRegs     int     // Number of registers needed
	NumCursors  int     // Number of cursors needed
	Constants   []Mem   // Constant pool
	SQL         string  // Original SQL text
	Comment     string  // Program-level comment
}

// VDBE is the virtual database engine — a register-based VM.
type VDBE struct {
	prog     *Program
	pc       int         // Program counter
	regs     []Mem       // Registers
	cursors  []*VdbeCursor
	nMem     int
	nCursor  int
	rc       ResultCode  // Current result code
	db       Database
	err      error
	halt     bool
	initialized bool
	// Callbacks
	resultRowFunc func(regs []Mem, startIdx, count int)
}

// NewVDBE creates a new VDBE instance.
func NewVDBE(db Database) *VDBE {
	return &VDBE{
		db: db,
	}
}

// SetProgram loads a program into the VDBE for execution.
func (v *VDBE) SetProgram(prog *Program) {
	v.prog = prog
	v.nMem = prog.NumRegs
	v.nCursor = prog.NumCursors
}

// SetResultRowCallback sets the callback invoked for OpResultRow.
func (v *VDBE) SetResultRowCallback(fn func(regs []Mem, startIdx, count int)) {
	v.resultRowFunc = fn
}

// Registers returns the current register array (for binding variables).
func (v *VDBE) Registers() []Mem {
	return v.regs
}

// Step executes the VDBE until it produces a row (ResultRow) or finishes.
// Returns (true, nil) when a row is available, (false, nil) when done,
// or (false, err) on error.
func (v *VDBE) Step(ctx context.Context) (bool, error) {
	if v.prog == nil {
		return false, fmt.Errorf("no program loaded")
	}

	// On first call, initialize registers
	if !v.initialized && len(v.regs) == 0 {
		if v.nMem > 0 {
			v.regs = make([]Mem, v.nMem+1)
			for i := range v.regs {
				v.regs[i] = Mem{Type: MemNull, IsNull: true}
			}
		}
		if v.nCursor > 0 {
			v.cursors = make([]*VdbeCursor, v.nCursor)
		}
	}

	aOp := v.prog.Instructions
	nOp := len(aOp)
	if nOp == 0 {
		return false, nil
	}

	for v.pc >= 0 && v.pc < nOp {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		default:
		}

		pOp := &aOp[v.pc]
		nextPC := v.pc + 1

		switch pOp.Op {
		case OpGoto:
			nextPC = pOp.P2

		case OpGoSub:
			v.regs[pOp.P1].SetInt(int64(v.pc + 1))
			nextPC = pOp.P2

		case OpReturn:
			retPC := v.regs[pOp.P1].IntValue()
			nextPC = int(retPC)

		case OpIf:
			pIn := &v.regs[pOp.P1]
			if pIn.Bool() {
				nextPC = pOp.P2
			}

		case OpIfNot:
			pIn := &v.regs[pOp.P1]
			if !pIn.Bool() {
				nextPC = pOp.P2
			}

		case OpInit:
			v.initialized = true
			nextPC = pOp.P2

		case OpHalt:
			v.halt = true
			if pOp.P1 != 0 {
				v.rc = ResultCode(pOp.P1)
				if pOp.P1 == 1 {
					v.err = fmt.Errorf("constraint failed")
				}
				return false, v.err
			}
			return false, nil

		case OpNoConflict:

		case OpAutoCommit:
			if v.db != nil {
				turnOn := pOp.P1
				rollback := pOp.P2
				if rollback != 0 {
					v.db.Rollback()
				}
				if turnOn != 0 {
					v.db.SetAutoCommit(true)
				}
			}

		case OpTransaction:
			if v.db != nil {
				write := pOp.P2 != 0
				if err := v.db.BeginTransaction(write); err != nil {
					v.rc = ResultError
					v.err = err
					return false, v.err
				}
			}
			nextPC = v.pc + 1

		case OpInteger:
			if p4int, ok := pOp.P4.(int64); ok {
				v.regs[pOp.P2].SetInt(p4int)
			} else {
				v.regs[pOp.P2].SetInt(int64(pOp.P1))
			}

		case OpReal:
			if p4float, ok := pOp.P4.(float64); ok {
				v.regs[pOp.P2].SetFloat(p4float)
			}

		case OpString8:
			if s, ok := pOp.P4.(string); ok {
				v.regs[pOp.P2].SetText(s)
			}

		case OpBlob:
			if b, ok := pOp.P4.([]byte); ok {
				v.regs[pOp.P2].SetBlob(b)
			}

		case OpNull:
			v.regs[pOp.P2].SetNull()
			if pOp.P3 > 0 {
				v.regs[pOp.P3].SetNull()
			}

		case OpCopy:
			n := pOp.P3
			if n <= 0 {
				n = 1
			}
			for i := 0; i < n; i++ {
				dstIdx := pOp.P2 + i
				srcIdx := pOp.P1 + i
				if srcIdx < len(v.regs) && dstIdx < len(v.regs) {
					v.regs[dstIdx] = *v.regs[srcIdx].Copy()
				}
			}

		case OpSCopy:
			src := &v.regs[pOp.P1]
			dst := &v.regs[pOp.P2]
			dst.Type = src.Type
			dst.IntVal = src.IntVal
			dst.FloatVal = src.FloatVal
			dst.IsNull = src.IsNull
			dst.IsRowID = src.IsRowID
			dst.IsZero = src.IsZero
			dst.Bytes = src.Bytes

		case OpMove:
			n := pOp.P3
			if n <= 0 {
				n = 1
			}
			for i := 0; i < n; i++ {
				srcIdx := pOp.P1 + i
				dstIdx := pOp.P2 + i
				if srcIdx < len(v.regs) && dstIdx < len(v.regs) {
					v.regs[dstIdx] = v.regs[srcIdx]
					v.regs[srcIdx] = Mem{Type: MemNull, IsNull: true}
				}
			}

		case OpAdd:
			v.arith(pOp, func(a, b int64) int64 { return a + b },
				func(a, b float64) float64 { return a + b })

		case OpSubtract:
			v.arith(pOp, func(a, b int64) int64 { return a - b },
				func(a, b float64) float64 { return a - b })

		case OpMul:
			v.arith(pOp, func(a, b int64) int64 { return a * b },
				func(a, b float64) float64 { return a * b })

		case OpDivide:
			v.arith(pOp, func(a, b int64) int64 { return a / b },
				func(a, b float64) float64 { return a / b })

		case OpRemainder:
			v.arith(pOp, func(a, b int64) int64 { return a % b },
				func(a, b float64) float64 { return math.Mod(a, b) })

		case OpBitAnd:
			v.arith(pOp, func(a, b int64) int64 { return a & b }, nil)

		case OpBitOr:
			v.arith(pOp, func(a, b int64) int64 { return a | b }, nil)

		case OpBitNot:
			pIn := &v.regs[pOp.P1]
			pOut := &v.regs[pOp.P2]
			if pIn.Type == MemInt {
				pOut.SetInt(^pIn.IntVal)
			} else if pIn.IsNumeric() || pIn.Type == MemStr {
				pOut.SetInt(^pIn.IntValue())
			}

		case OpShiftLeft:
			v.arith(pOp, func(a, b int64) int64 { return a << uint(b) }, nil)

		case OpShiftRight:
			v.arith(pOp, func(a, b int64) int64 { return a >> uint(b) }, nil)

		case OpEq:
			if v.compare(pOp, 0) {
				nextPC = pOp.P2
			}

		case OpNe:
			if v.compare(pOp, 1) {
				nextPC = pOp.P2
			}

		case OpLt:
			if v.compare(pOp, 2) {
				nextPC = pOp.P2
			}

		case OpLe:
			if v.compare(pOp, 3) {
				nextPC = pOp.P2
			}

		case OpGt:
			if v.compare(pOp, 4) {
				nextPC = pOp.P2
			}

		case OpGe:
			if v.compare(pOp, 5) {
				nextPC = pOp.P2
			}

		case OpNot:
			pIn := &v.regs[pOp.P1]
			pOut := &v.regs[pOp.P2]
			if pIn.Type == MemNull {
				pOut.SetNull()
			} else if pIn.Bool() {
				pOut.SetInt(0)
			} else {
				pOut.SetInt(1)
			}

		case OpVariable:
			// OpVariable loads bind parameter P1 into register P2.
			// The actual value is set via applyBindings before execution,
			// or we leave it as null (it should have been pre-set).

		case OpOpenRead:
			v.openCursor(pOp.P1, pOp.P2, false)

		case OpOpenWrite:
			v.openCursor(pOp.P1, pOp.P2, true)

		case OpClose:
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				if cursor, ok := v.cursors[pOp.P1].Cursor.(Cursor); ok {
					cursor.Close()
				}
				v.cursors[pOp.P1] = nil
			}

		case OpFirst:
			hasRow := false
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				v.cursors[pOp.P1].NullRow = false
				if cursor, ok := v.cursors[pOp.P1].Cursor.(Cursor); ok {
					var err error
					hasRow, err = cursor.First()
					if err != nil {
						return false, err
					}
				}
			}
			if !hasRow {
				nextPC = pOp.P2
			}

		case OpLast:
			hasRow := false
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				v.cursors[pOp.P1].NullRow = false
				if cursor, ok := v.cursors[pOp.P1].Cursor.(Cursor); ok {
					var err error
					hasRow, err = cursor.Last()
					if err != nil {
						return false, err
					}
				}
			}
			if !hasRow {
				nextPC = pOp.P2
			}

		case OpNext:
			hasRow := false
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				if cursor, ok := v.cursors[pOp.P1].Cursor.(Cursor); ok {
					var err error
					hasRow, err = cursor.Next()
					if err != nil {
						return false, err
					}
				}
			}
			if !hasRow {
				nextPC = pOp.P2
			}

		case OpPrev:
			hasRow := false
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				if cursor, ok := v.cursors[pOp.P1].Cursor.(Cursor); ok {
					var err error
					hasRow, err = cursor.Prev()
					if err != nil {
						return false, err
					}
				}
			}
			if !hasRow {
				nextPC = pOp.P2
			}

		case OpRewind:
			hasRow := false
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				v.cursors[pOp.P1].NullRow = false
				if cursor, ok := v.cursors[pOp.P1].Cursor.(Cursor); ok {
					var err error
					hasRow, err = cursor.First()
					if err != nil {
						return false, err
					}
				}
			}
			if !hasRow {
				nextPC = pOp.P2
			}

		case OpColumn:
			v.execColumn(pOp)

		case OpMakeRecord:
			v.execMakeRecord(pOp)

		case OpNewRowid:
			v.execNewRowId(pOp)

		case OpInsert:
			v.execInsert(pOp)

		case OpDelete:
			v.execDelete(pOp)

		case OpResultRow:
			if v.resultRowFunc != nil {
				v.resultRowFunc(v.regs, pOp.P1, pOp.P2)
			}
			v.pc = nextPC
			return true, nil

		default:
			// Unknown opcode — skip
		}

		v.pc = nextPC
	}

	return false, v.err
}

// reset prepares the VDBE for execution.
func (v *VDBE) reset() {
	v.pc = 0
	v.rc = ResultOK
	v.err = nil
	v.halt = false
	v.initialized = false

	if v.nMem > 0 {
		v.regs = make([]Mem, v.nMem+1) // 1-indexed for clarity
		for i := range v.regs {
			v.regs[i] = Mem{Type: MemNull, IsNull: true}
		}
	}
	if v.nCursor > 0 {
		v.cursors = make([]*VdbeCursor, v.nCursor)
	}
}

// Execute runs the VDBE program and returns the result code.
func (v *VDBE) Execute(ctx context.Context) (ResultCode, error) {
	v.reset()
	for {
		hasRow, err := v.Step(ctx)
		if err != nil {
			return v.rc, err
		}
		if !hasRow {
			break
		}
	}
	if v.err != nil {
		return v.rc, v.err
	}
	return ResultDone, nil
}

// arith performs a binary arithmetic operation.
func (v *VDBE) arith(pOp *Instruction, intFn func(a, b int64) int64, floatFn func(a, b float64) float64) {
	pIn1 := &v.regs[pOp.P1]
	pIn2 := &v.regs[pOp.P2]
	pOut := &v.regs[pOp.P3]

	// If both operands are integers, use integer arithmetic
	if pIn1.Type == MemInt && pIn2.Type == MemInt {
		pOut.SetInt(intFn(pIn1.IntVal, pIn2.IntVal))
		return
	}

	// Otherwise use float arithmetic
	if floatFn != nil {
		pOut.SetFloat(floatFn(pIn1.FloatValue(), pIn2.FloatValue()))
	} else {
		// Bitwise operations on non-integers: coerce to int
		pOut.SetInt(intFn(pIn1.IntValue(), pIn2.IntValue()))
	}
}

// compare performs a comparison and returns true if the condition is met.
// cmpOp: 0=Eq, 1=Ne, 2=Lt, 3=Le, 4=Gt, 5=Ge
func (v *VDBE) compare(pOp *Instruction, cmpOp int) bool {
	pIn1 := &v.regs[pOp.P1]
	pIn3 := &v.regs[pOp.P3]

	// NULL handling: per SQLite semantics, NULL comparisons never jump
	if pIn1.Type == MemNull || pIn3.Type == MemNull {
		// SQLite: if either operand is NULL, the comparison never succeeds
		// unless OP_Ne with NULL-equal flag in P5
		if cmpOp == 1 && pOp.P5 != 0 {
			// Ne with NULL-equal flag: NULL == NULL
			if pIn1.Type == MemNull && pIn3.Type == MemNull {
				return true
			}
		}
		return false
	}

	cmp := MemCompare(pIn1, pIn3)

	switch cmpOp {
	case 0: // Eq
		return cmp == 0
	case 1: // Ne
		return cmp != 0
	case 2: // Lt
		return cmp < 0
	case 3: // Le
		return cmp <= 0
	case 4: // Gt
		return cmp > 0
	case 5: // Ge
		return cmp >= 0
	}
	return false
}

// openCursor opens a cursor on the given root page.
func (v *VDBE) openCursor(cursorIdx, rootPage int, write bool) {
	if cursorIdx >= len(v.cursors) {
		newCursors := make([]*VdbeCursor, cursorIdx+1)
		copy(newCursors, v.cursors)
		v.cursors = newCursors
	}

	vc := &VdbeCursor{
		RootPage:  rootPage,
		Writeable: write,
		NullRow:   true,
	}

	if v.db != nil {
		if cur, err := v.db.GetCursor(rootPage, write); err == nil {
			vc.Cursor = cur
		}
	}

	v.cursors[cursorIdx] = vc
}

// execColumn reads a column from the current cursor row.
func (v *VDBE) execColumn(pOp *Instruction) {
	cursorIdx := pOp.P1
	colIdx := pOp.P2
	destIdx := pOp.P3

	if cursorIdx >= len(v.cursors) || v.cursors[cursorIdx] == nil {
		v.regs[destIdx].SetNull()
		return
	}

	vc := v.cursors[cursorIdx]
	if vc.NullRow {
		v.regs[destIdx].SetNull()
		return
	}

	cursor, ok := vc.Cursor.(Cursor)
	if !ok || !cursor.IsValid() {
		v.regs[destIdx].SetNull()
		return
	}

	data, err := cursor.Data()
	if err != nil || data == nil {
		v.regs[destIdx].SetNull()
		return
	}

	// Parse the record to extract the column
	values, err := ParseRecord(data)
	if err != nil || colIdx >= len(values) {
		v.regs[destIdx].SetNull()
		return
	}

	col := values[colIdx]
	result := MemFromValue(col)
	v.regs[destIdx] = *result
}

// execMakeRecord constructs a record from registers.
func (v *VDBE) execMakeRecord(pOp *Instruction) {
	startReg := pOp.P1
	count := pOp.P2
	destReg := pOp.P3

	rb := NewRecordBuilder()
	for i := 0; i < count; i++ {
		reg := &v.regs[startReg+i]
		switch reg.Type {
		case MemNull:
			rb.AddNull()
		case MemInt:
			rb.AddInt(reg.IntVal)
		case MemFloat:
			rb.AddFloat(reg.FloatVal)
		case MemStr:
			rb.AddText(string(reg.Bytes))
		case MemBlob:
			rb.AddBlob(reg.Bytes)
		}
	}

	v.regs[destReg].SetBlob(rb.Build())
}

// execNewRowId generates a new row ID for the cursor's table.
func (v *VDBE) execNewRowId(pOp *Instruction) {
	cursorIdx := pOp.P1
	destReg := pOp.P2

	var newRowID int64 = 1

	if cursorIdx < len(v.cursors) && v.cursors[cursorIdx] != nil {
		cursor, ok := v.cursors[cursorIdx].Cursor.(Cursor)
		if ok {
			// Try to find a new row ID by going to last and incrementing
			if hasRow, _ := cursor.Last(); hasRow {
				lastID := cursor.RowID()
				newRowID = lastID + 1
			}
		}
	}

	v.regs[destReg].SetInt(newRowID)
	if v.db != nil {
		// Use the larger of generated ID and db's last insert rowid
		last := v.db.LastInsertRowID()
		if newRowID <= last {
			newRowID = last + 1
		}
		v.regs[destReg].SetInt(newRowID)
	}
}

// execInsert inserts a row using the cursor.
func (v *VDBE) execInsert(pOp *Instruction) {
	cursorIdx := pOp.P1
	keyReg := pOp.P2
	dataReg := pOp.P3

	if cursorIdx >= len(v.cursors) || v.cursors[cursorIdx] == nil {
		return
	}

	vc := v.cursors[cursorIdx]
	cursor := vc.Cursor

	// Get the key (rowid)
	var rowid int64
	if keyReg < len(v.regs) {
		rowid = v.regs[keyReg].IntValue()
	}

	// Get the data (record)
	var data []byte
	if dataReg < len(v.regs) && v.regs[dataReg].Bytes != nil {
		data = v.regs[dataReg].Bytes
	}

	// Encode key as varint
	keyBuf := make([]byte, 9)
	keyLen := putVarint(keyBuf, rowid)

	// Try using Inserter interface
	if inserter, ok := v.db.(Inserter); ok && cursor != nil {
		if err := inserter.Insert(cursor, keyBuf[:keyLen], data, rowid, 0); err != nil {
			v.err = err
			v.rc = ResultError
			return
		}
	}

	if v.db != nil {
		v.db.SetLastInsertRowID(rowid)
	}
}

// execDelete deletes the row at the current cursor position.
func (v *VDBE) execDelete(pOp *Instruction) {
	cursorIdx := pOp.P1

	if cursorIdx >= len(v.cursors) || v.cursors[cursorIdx] == nil {
		return
	}

	vc := v.cursors[cursorIdx]
	cursor := vc.Cursor

	if deleter, ok := v.db.(Deleter); ok && cursor != nil {
		if err := deleter.Delete(cursor); err != nil {
			v.err = err
			v.rc = ResultError
		}
	}
}

// encodeVarint is a local helper for varint encoding.
func encodeVarint(buf []byte, v int64) int {
	uv := uint64(v)
	if uv <= 127 {
		buf[0] = byte(uv)
		return 1
	}
	var tmp [9]byte
	n := 0
	for i := 8; i >= 0; i-- {
		tmp[i] = byte((uv & 0x7f) | 0x80)
		uv >>= 7
		n++
		if uv == 0 {
			tmp[8] &= 0x7f
			break
		}
	}
	copy(buf, tmp[9-n:])
	return n
}

// decodeVarint is a local helper for varint decoding.
func decodeVarint(buf []byte) (int64, int) {
	if len(buf) == 0 {
		return 0, 0
	}
	var v uint64
	for i := 0; i < 9 && i < len(buf); i++ {
		v = (v << 7) | uint64(buf[i]&0x7f)
		if buf[i]&0x80 == 0 {
			return int64(v), i + 1
		}
	}
	if len(buf) >= 9 {
		v = (v << 8) | uint64(buf[8])
		return int64(v), 9
	}
	return int64(v), len(buf)
}

// Ensure binary is available for record operations.
var _ = binary.BigEndian
