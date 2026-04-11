package vdbe

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/sqlite-go/sqlite-go/btree"
	"github.com/sqlite-go/sqlite-go/encoding"
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
	Cursor    interface{} // Will be btree.BTCursor at runtime
	RootPage  int
	Writeable bool
	NullRow   bool
	Ordered   bool
	SeekHit   int   // tracks seek hit count for IN-early-out optimization
	SeqCount  int64 // Sequence counter for OpSequence
	Pseudo    bool  // True for pseudo-cursors (OpOpenPseudo)
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
	nChange  int64        // Change counter for OpResetCount
	// Callbacks
	resultRowFunc func(regs []Mem, startIdx, count int)
}

// AggFuncInfo carries aggregate function callbacks for VDBE opcodes.
// P4 on OpAggStep/OpAggFinal/OpAggInverse holds a *AggFuncInfo.
type AggFuncInfo struct {
	Name     string
	Step     func(state interface{}, args []*Mem)
	Finalize func(state interface{}) *Mem
	Inverse  func(state interface{}, args []*Mem)
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

		// ─── Index Opcodes ──────────────────────────────────────────────────

		case OpIdxInsert:
			v.execIdxInsert(pOp)

		case OpIdxDelete:
			v.execIdxDelete(pOp)

		case OpIdxRowid:
			v.execIdxRowid(pOp)

		// ─── Seek Opcodes ───────────────────────────────────────────────────

		case OpSeek:
			nextPC = v.execSeek(pOp, nextPC)

		case OpSeekGE:
			nextPC = v.execSeekCmp(pOp, nextPC, false, false) // >=

		case OpSeekGT:
			nextPC = v.execSeekCmp(pOp, nextPC, true, false) // >

		case OpSeekLE:
			nextPC = v.execSeekCmp(pOp, nextPC, false, true) // <=

		case OpSeekLT:
			nextPC = v.execSeekCmp(pOp, nextPC, true, true) // <

		case OpSeekScan:
			// SeekScan is an optimization hint for IN loops.
			// Scan forward at most P1 entries; if found, jump to P2.
			nextPC = v.execSeekScan(pOp, nextPC)

		case OpSeekHit:
			// Update the seekHit counter for cursor P1 within bounds [P2, P3].
			v.execSeekHit(pOp)

		// ─── Index Comparison Opcodes ───────────────────────────────────────

		case OpIdxLT:
			if v.execIdxCompare(pOp, -1) { // index_key < reg_key
				nextPC = pOp.P2
			}

		case OpIdxLE:
			if v.execIdxCompare(pOp, 0) { // index_key <= reg_key
				nextPC = pOp.P2
			}

		case OpIdxGT:
			if v.execIdxCompare(pOp, 1) { // index_key > reg_key
				nextPC = pOp.P2
			}

		case OpIdxGE:
			if v.execIdxCompare(pOp, 2) { // index_key >= reg_key
				nextPC = pOp.P2
			}

		// ─── Found / NotFound Opcodes ───────────────────────────────────────

		case OpFound:
			if v.execFound(pOp) {
				nextPC = pOp.P2
			}

		case OpNotFound:
			if !v.execFound(pOp) {
				nextPC = pOp.P2
			}

		case OpNoConflict:
			if !v.execConflict(pOp) {
				nextPC = pOp.P2
			}

		case OpNotExists:
			nextPC = v.execNotExists(pOp, nextPC)

		case OpUnique:
			nextPC = v.execUnique(pOp, nextPC)

		// ─── Row Data Opcodes ──────────────────────────────────────────────

		case OpRowid:
			v.execRowid(pOp)

		case OpRowData:
			v.execRowData(pOp)

		case OpRowCell:
			v.execRowCell(pOp)

		case OpOffset:
			v.execOffset(pOp)
		// --- Aggregate opcodes ---

		case OpAggStep:
			// P1 = register holding aggregate state
			// P2 = number of arguments
			// P3 = start register for arguments
			// P4 = *AggFuncInfo
			v.execAggStep(pOp)

		case OpAggFinal:
			// P1 = register holding aggregate state
			// P2 = destination register for result
			// P4 = *AggFuncInfo
			v.execAggFinal(pOp)

		case OpAggInverse:
			// P1 = register holding aggregate state
			// P2 = number of arguments
			// P3 = start register for arguments
			// P4 = *AggFuncInfo
			v.execAggInverse(pOp)

		case OpAggValue:
			// P1 = register holding aggregate state
			// P2 = destination register
			if pOp.P1 < len(v.regs) && pOp.P2 < len(v.regs) && pOp.P1 != pOp.P2 {
				src := &v.regs[pOp.P1]
				dst := &v.regs[pOp.P2]
				dst.Type = src.Type
				dst.IntVal = src.IntVal
				dst.FloatVal = src.FloatVal
				dst.IsNull = src.IsNull
				dst.IsRowID = src.IsRowID
				dst.IsZero = src.IsZero
				if src.Bytes != nil {
					dst.Bytes = make([]byte, len(src.Bytes))
					copy(dst.Bytes, src.Bytes)
				} else {
					dst.Bytes = nil
				}
			}

		// --- Sort opcodes ---

		case OpSorterOpen:
			// P1 = cursor number
			v.execSorterOpen(pOp)

		case OpSorterInsert:
			// P1 = cursor number
			// P2 = register with sort key
			// P3 = register with data
			v.execSorterInsert(pOp)

		case OpSorterSort:
			// P1 = cursor number
			// P2 = jump target if empty
			v.execSorterSort(pOp, &nextPC)

		case OpSorterNext:
			// P1 = cursor number
			// P2 = jump target if no more
			v.execSorterNext(pOp, &nextPC)

		case OpSorterData:
			// P1 = cursor number
			// P2 = destination register
			v.execSorterData(pOp)

		case OpSorterCompare:
			// P1 = cursor number
			// P2 = jump target if different
			// P3 = register with comparison key
			v.execSorterCompare(pOp, &nextPC)

		case OpSort:
			// P1 = cursor number - sort the sorter
			v.execSort(pOp)

		// --- Cursor opcodes ---

		case OpNullRow:
			// P1 = cursor number - set to null row
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				v.cursors[pOp.P1].NullRow = true
			}

		case OpOpenEphemeral:
			// P1 = cursor number
			// P2 = number of columns
			v.execOpenEphemeral(pOp)

		case OpOpenPseudo:
			// P1 = cursor number
			// P2 = content register
			// P3 = number of fields
			v.execOpenPseudo(pOp)

		case OpOpenDup:
			// P1 = source cursor
			// P2 = destination cursor
			v.execOpenDup(pOp)

		case OpOpenAutoindex:
			// P1 = cursor number
			// P2 = number of columns
			v.execOpenAutoindex(pOp)

		case OpResetSorter:
			// P1 = cursor number - reset the sorter
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				if sorter, ok := v.cursors[pOp.P1].Cursor.(*Sorter); ok {
					sorter.Reset()
				}
			}

		case OpResetCount:
			// Reset the change counter
			v.nChange = 0

		case OpSequence:
			// P1 = cursor number
			// P2 = destination register
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				v.regs[pOp.P2].SetInt(v.cursors[pOp.P1].SeqCount)
				v.cursors[pOp.P1].SeqCount++
			} else {
				v.regs[pOp.P2].SetInt(0)
			}

		case OpSequenceTest:
			// P1 = cursor number
			// P2 = jump target
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				if v.cursors[pOp.P1].SeqCount == 0 {
					nextPC = pOp.P2
				}
			}

		// --- RowSet opcodes ---

		case OpRowSetAdd:
			// P1 = register holding RowSet
			// P2 = register with rowid value
			v.execRowSetAdd(pOp)

		case OpRowSetRead:
			// P1 = register holding RowSet
			// P2 = jump target if empty
			// P3 = destination register for rowid
			v.execRowSetRead(pOp, &nextPC)

		case OpRowSetTest:
			// P1 = register holding RowSet
			// P2 = jump target
			// P3 = register with rowid
			v.execRowSetTest(pOp, &nextPC)

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

// ─── Index Opcode Helpers ────────────────────────────────────────────────────

// getCursor retrieves the VdbeCursor and btree.BTCursor for the given index.
func (v *VDBE) getCursor(idx int) (*VdbeCursor, btree.BTCursor, bool) {
	if idx >= len(v.cursors) || v.cursors[idx] == nil {
		return nil, nil, false
	}
	vc := v.cursors[idx]
	bc, ok := vc.Cursor.(btree.BTCursor)
	if !ok {
		return vc, nil, false
	}
	return vc, bc, true
}

// execIdxInsert inserts a record into an index B-Tree.
// P1 = cursor, P2 = key register (blob), P3 = rowid register (for append hint).
func (v *VDBE) execIdxInsert(pOp *Instruction) {
	vc, bc, ok := v.getCursor(pOp.P1)
	if !ok || bc == nil {
		return
	}

	// The key is a blob (record) from register P2
	keyReg := &v.regs[pOp.P2]
	var key []byte
	if keyReg.Type == MemBlob || keyReg.Type == MemStr {
		key = keyReg.Bytes
	} else {
		key = keyReg.BlobValue()
	}
	if key == nil {
		key = []byte{}
	}

	// For index inserts, we use the Inserter interface on db
	if inserter, ok := v.db.(Inserter); ok {
		if err := inserter.Insert(vc.Cursor, key, nil, 0, 0); err != nil {
			v.err = err
			v.rc = ResultError
		}
	}
}

// execIdxDelete deletes an entry from an index B-Tree.
// P1 = cursor, P2 = key register base, P3 = number of key fields.
func (v *VDBE) execIdxDelete(pOp *Instruction) {
	vc, bc, ok := v.getCursor(pOp.P1)
	if !ok || bc == nil {
		return
	}

	// Build the key from registers P2..P2+P3-1, or use a blob key
	var key []byte
	if pOp.P3 > 0 {
		rb := NewRecordBuilder()
		for i := 0; i < pOp.P3; i++ {
			reg := &v.regs[pOp.P2+i]
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
		key = rb.Build()
	} else {
		keyReg := &v.regs[pOp.P2]
		key = keyReg.BlobValue()
	}

	if key == nil {
		return
	}

	// Seek to the key
	_, err := bc.Seek(key)
	if err != nil {
		v.err = err
		v.rc = ResultError
		return
	}

	// Delete at current position
	if deleter, ok := v.db.(Deleter); ok && bc.IsValid() {
		if err := deleter.Delete(vc.Cursor); err != nil {
			v.err = err
			v.rc = ResultError
		}
	}
}

// execIdxRowid extracts the rowid from an index cursor's current entry.
// P1 = cursor, P2 = destination register.
// The rowid is the last varint in the index key.
func (v *VDBE) execIdxRowid(pOp *Instruction) {
	destReg := pOp.P2

	vc, bc, ok := v.getCursor(pOp.P1)
	if !ok || bc == nil {
		v.regs[destReg].SetNull()
		return
	}

	if vc.NullRow || !bc.IsValid() {
		v.regs[destReg].SetNull()
		return
	}

	// For index cursors, the key contains the index record.
	// The rowid is typically the last field in the index key record.
	key := bc.Key()
	if key == nil || len(key) == 0 {
		v.regs[destReg].SetNull()
		return
	}

	// Parse the record to get the last field (which should be the rowid)
	values, err := ParseRecord(key)
	if err != nil || len(values) == 0 {
		// Fallback: try to read as raw varint at end of key
		// The last varint in the key is often the rowid
		rowid := extractRowidFromIndexKey(key)
		if rowid != 0 {
			v.regs[destReg].SetInt(rowid)
		} else {
			v.regs[destReg].SetNull()
		}
		return
	}

	lastVal := values[len(values)-1]
	switch lastVal.Type {
	case "int":
		v.regs[destReg].SetInt(lastVal.IntVal)
	default:
		v.regs[destReg].SetNull()
	}
}

// extractRowidFromIndexKey tries to extract a rowid from the end of an index key.
func extractRowidFromIndexKey(key []byte) int64 {
	if len(key) == 0 {
		return 0
	}
	// Parse the record header to find the last field
	headerSize, n := readVarint(key)
	if n == 0 || int(headerSize) > len(key) {
		return 0
	}
	// Parse serial types to find the last field's position
	pos := n
	var lastSerialType int64
	lastSerialTypePos := n
	for pos < int(headerSize) {
		lastSerialTypePos = pos
		st, sn := readVarint(key[pos:])
		if sn == 0 {
			break
		}
		lastSerialType = st
		pos += sn
	}
	// Calculate the body position of the last field by summing sizes of all fields
	bodyPos := int(headerSize)
	serialPos := n
	for serialPos < lastSerialTypePos {
		st, sn := readVarint(key[serialPos:])
		if sn == 0 {
			break
		}
		bodyPos += serialTypeSize(st)
		serialPos += sn
	}
	// Decode the last field based on its serial type
	if bodyPos < len(key) {
		sz := serialTypeSize(lastSerialType)
		if bodyPos+sz <= len(key) {
			val := decodeValueFrom(key[bodyPos:bodyPos+sz], lastSerialType)
			if val.Type == "int" {
				return val.IntVal
			}
		}
	}
	return 0
}

// ─── Seek Opcode Helpers ─────────────────────────────────────────────────────

// execSeek positions a cursor at an exact key match.
// P1 = cursor, P2 = jump target, P3 = key register.
// For table cursors, the key is a rowid.
func (v *VDBE) execSeek(pOp *Instruction, nextPC int) int {
	_, bc, ok := v.getCursor(pOp.P1)
	if !ok || bc == nil {
		if pOp.P2 > 0 {
			return pOp.P2
		}
		return nextPC
	}

	keyReg := &v.regs[pOp.P3]

	// For table cursors, use SeekRowid with integer key
	rowid := keyReg.IntValue()
	result, err := bc.SeekRowid(btree.RowID(rowid))
	if err != nil {
		v.err = err
		v.rc = ResultError
		return nextPC
	}

	if result != btree.SeekFound {
		if pOp.P2 > 0 {
			return pOp.P2
		}
	}
	return nextPC
}

// execSeekCmp handles SeekGE, SeekGT, SeekLE, SeekLT.
// strict=true means > or < (not >= or <=).
// reverse=true means LE or LT (seeking backwards).
func (v *VDBE) execSeekCmp(pOp *Instruction, nextPC int, strict, reverse bool) int {
	vc, bc, ok := v.getCursor(pOp.P1)
	if !ok || bc == nil {
		if pOp.P2 > 0 {
			return pOp.P2
		}
		return nextPC
	}
	_ = vc

	keyReg := &v.regs[pOp.P3]

	// Try as a record/blob key first (for index cursors)
	var key []byte
	if keyReg.Type == MemBlob || keyReg.Type == MemStr {
		key = keyReg.Bytes
	} else {
		// Build a key from the register value
		rb := NewRecordBuilder()
		switch keyReg.Type {
		case MemNull:
			rb.AddNull()
		case MemInt:
			rb.AddInt(keyReg.IntVal)
		case MemFloat:
			rb.AddFloat(keyReg.FloatVal)
		case MemStr:
			rb.AddText(string(keyReg.Bytes))
		default:
			rb.AddInt(keyReg.IntValue())
		}
		key = rb.Build()
	}

	result, err := bc.Seek(key)
	if err != nil {
		v.err = err
		v.rc = ResultError
		return nextPC
	}

	// Adjust based on the seek comparison type
	switch {
	case !strict && !reverse: // SeekGE: >=
		// Cursor is positioned at or after the key. If not found exactly,
		// we may need to check if cursor is valid.
		if result == btree.SeekNotFound && !bc.IsValid() {
			if pOp.P2 > 0 {
				return pOp.P2
			}
		}
	case strict && !reverse: // SeekGT: >
		// If we found the exact key, advance past it
		if result == btree.SeekFound {
			hasNext, err := bc.Next()
			if err != nil {
				v.err = err
				v.rc = ResultError
				return nextPC
			}
			if !hasNext {
				if pOp.P2 > 0 {
					return pOp.P2
				}
			}
		}
	case !strict && reverse: // SeekLE: <=
		// If cursor is past the key, go back one
		if result == btree.SeekNotFound {
			// Cursor is at first entry > key, so go back to get <=
			hasPrev, err := bc.Prev()
			if err != nil {
				v.err = err
				v.rc = ResultError
				return nextPC
			}
			if !hasPrev {
				if pOp.P2 > 0 {
					return pOp.P2
				}
			}
		}
	case strict && reverse: // SeekLT: <
		// Cursor is at first entry >= key, so always go back one
		if result == btree.SeekFound {
			hasPrev, err := bc.Prev()
			if err != nil {
				v.err = err
				v.rc = ResultError
				return nextPC
			}
			if !hasPrev {
				if pOp.P2 > 0 {
					return pOp.P2
				}
			}
		} else {
			// NotFound means cursor is at first > key
			hasPrev, err := bc.Prev()
			if err != nil {
				v.err = err
				v.rc = ResultError
				return nextPC
			}
			if !hasPrev {
				if pOp.P2 > 0 {
					return pOp.P2
				}
			}
		}
	}

	return nextPC
}

// execSeekScan implements the SeekScan optimization for IN loops.
// P1 = max scan steps, P2 = jump target if found.
func (v *VDBE) execSeekScan(pOp *Instruction, nextPC int) int {
	// Look at the next instruction to get cursor info
	// This is a simplified implementation: scan forward at most P1 steps
	// from the current cursor position
	maxSteps := pOp.P1
	if maxSteps <= 0 {
		maxSteps = 1
	}

	// Use the cursor from the next instruction (the SeekGE/SeekGT that follows)
	// For now, just continue to next instruction (the real SeekGE/SeekGT)
	return nextPC
}

// execSeekHit updates the seekHit counter for a cursor.
// P1 = cursor, P2 = lower bound, P3 = upper bound.
func (v *VDBE) execSeekHit(pOp *Instruction) {
	if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
		vc := v.cursors[pOp.P1]
		if pOp.P3 > 0 {
			vc.SeekHit = pOp.P3
		} else if pOp.P2 > 0 {
			vc.SeekHit = pOp.P2
		}
	}
}

// ─── Index Comparison Helper ─────────────────────────────────────────────────

// execIdxCompare compares the current index entry with registers.
// mode: -1=LT, 0=LE, 1=GT, 2=GE
// Returns true if the comparison condition is satisfied.
func (v *VDBE) execIdxCompare(pOp *Instruction, mode int) bool {
	_, bc, ok := v.getCursor(pOp.P1)
	if !ok || bc == nil || !bc.IsValid() {
		return false
	}

	// Get the current index key
	idxKey := bc.Key()
	if idxKey == nil {
		return false
	}

	// Build comparison key from registers P3..P3+numFields-1
	numFields := pOp.P4
	if numFields == nil {
		if pOp.P5 > 0 {
			numFields = pOp.P5
		} else {
			numFields = 1
		}
	}

	var nFields int
	switch nf := numFields.(type) {
	case int:
		nFields = nf
	case int64:
		nFields = int(nf)
	default:
		nFields = 1
	}

	rb := NewRecordBuilder()
	for i := 0; i < nFields; i++ {
		regIdx := pOp.P3 + i
		if regIdx >= len(v.regs) {
			rb.AddNull()
			continue
		}
		reg := &v.regs[regIdx]
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
	cmpKey := rb.Build()

	// Compare index key with comparison key
	cmp := compareRecordKeys(idxKey, cmpKey)

	switch mode {
	case -1: // LT
		return cmp < 0
	case 0: // LE
		return cmp <= 0
	case 1: // GT
		return cmp > 0
	case 2: // GE
		return cmp >= 0
	}
	return false
}

// compareRecordKeys compares two record-encoded keys field by field.
func compareRecordKeys(a, b []byte) int {
	valsA, errA := ParseRecord(a)
	valsB, errB := ParseRecord(b)
	if errA != nil || errB != nil {
		// Fallback to byte comparison
		return bytesCompare(a, b)
	}

	minLen := len(valsA)
	if len(valsB) < minLen {
		minLen = len(valsB)
	}

	for i := 0; i < minLen; i++ {
		cmp := compareValues(valsA[i], valsB[i])
		if cmp != 0 {
			return cmp
		}
	}

	if len(valsA) < len(valsB) {
		return -1
	}
	if len(valsA) > len(valsB) {
		return 1
	}
	return 0
}

// bytesCompare provides simple byte-level comparison.
func bytesCompare(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// ─── Found / NotFound / Conflict Helpers ─────────────────────────────────────

// execFound checks if a key exists in the cursor's B-Tree.
// P1 = cursor, P2 = jump target, P3 = key register, P4 = num fields.
// Returns true if found.
func (v *VDBE) execFound(pOp *Instruction) bool {
	_, bc, ok := v.getCursor(pOp.P1)
	if !ok || bc == nil {
		return false
	}

	// If P4 is 0 or nil, use blob key from P3
	if pOp.P4 == nil {
		keyReg := &v.regs[pOp.P3]
		var key []byte
		if keyReg.Type == MemBlob || keyReg.Type == MemStr {
			key = keyReg.Bytes
		} else {
			key = keyReg.BlobValue()
		}
		if key == nil {
			return false
		}
		result, err := bc.Seek(key)
		if err != nil {
			return false
		}
		return result == btree.SeekFound
	}

	// Build record key from registers
	var nFields int
	switch nf := pOp.P4.(type) {
	case int:
		nFields = nf
	case int64:
		nFields = int(nf)
	default:
		nFields = 1
	}

	rb := NewRecordBuilder()
	for i := 0; i < nFields; i++ {
		regIdx := pOp.P3 + i
		if regIdx >= len(v.regs) {
			rb.AddNull()
			continue
		}
		reg := &v.regs[regIdx]
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
	key := rb.Build()

	result, err := bc.Seek(key)
	if err != nil {
		return false
	}
	return result == btree.SeekFound
}

// execConflict checks for a unique constraint conflict on an index.
// P1 = cursor, P2 = jump target, P3 = key register, P4 = num fields.
// Returns true if there IS a conflict (key exists and no NULLs in key).
// Returns false if no conflict (key not found or NULL in key).
func (v *VDBE) execConflict(pOp *Instruction) bool {
	// Check if any key field is NULL — NULLs never conflict
	var nFields int
	switch nf := pOp.P4.(type) {
	case int:
		nFields = nf
	case int64:
		nFields = int(nf)
	default:
		nFields = 1
	}

	for i := 0; i < nFields; i++ {
		regIdx := pOp.P3 + i
		if regIdx < len(v.regs) && v.regs[regIdx].Type == MemNull {
			return false // NULL fields means no conflict
		}
	}

	// Check if key exists
	return v.execFound(pOp)
}

// execNotExists checks if a row with a given rowid does NOT exist.
// P1 = cursor (table), P2 = jump target, P3 = rowid register.
// Returns P2 if the row does not exist.
func (v *VDBE) execNotExists(pOp *Instruction, nextPC int) int {
	_, bc, ok := v.getCursor(pOp.P1)
	if !ok || bc == nil {
		if pOp.P2 > 0 {
			return pOp.P2
		}
		return nextPC
	}

	rowid := v.regs[pOp.P3].IntValue()
	result, err := bc.SeekRowid(btree.RowID(rowid))
	if err != nil {
		v.err = err
		v.rc = ResultError
		return nextPC
	}

	if result != btree.SeekFound {
		return pOp.P2
	}
	return nextPC
}

// execUnique checks for a unique constraint violation on an index.
// P1 = cursor, P2 = jump target (constraint error handler).
// Similar to NotFound but used specifically for UNIQUE constraints.
func (v *VDBE) execUnique(pOp *Instruction, nextPC int) int {
	_, bc, ok := v.getCursor(pOp.P1)
	if !ok || bc == nil {
		return nextPC
	}

	// Build key from P3 registers with P4 field count
	keyReg := &v.regs[pOp.P3]
	var key []byte

	var nFields int
	switch nf := pOp.P4.(type) {
	case int:
		nFields = nf
	case int64:
		nFields = int(nf)
	default:
		nFields = 0
	}

	if nFields > 0 {
		rb := NewRecordBuilder()
		for i := 0; i < nFields; i++ {
			regIdx := pOp.P3 + i
			if regIdx >= len(v.regs) {
				rb.AddNull()
				continue
			}
			reg := &v.regs[regIdx]
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
		key = rb.Build()
	} else {
		if keyReg.Type == MemBlob || keyReg.Type == MemStr {
			key = keyReg.Bytes
		} else {
			key = keyReg.BlobValue()
		}
	}

	if key == nil {
		return nextPC
	}

	result, err := bc.Seek(key)
	if err != nil {
		v.err = err
		v.rc = ResultError
		return nextPC
	}

	// If found, there's a unique constraint violation
	if result == btree.SeekFound {
		if pOp.P2 > 0 {
			return pOp.P2
		}
		v.err = fmt.Errorf("UNIQUE constraint failed")
		v.rc = ResultError
	}
	return nextPC
}

// ─── Row Data Helpers ────────────────────────────────────────────────────────

// execRowid gets the rowid from the cursor at P1 and stores it in register P2.
func (v *VDBE) execRowid(pOp *Instruction) {
	destReg := pOp.P2

	vc, bc, ok := v.getCursor(pOp.P1)
	if !ok || bc == nil {
		v.regs[destReg].SetNull()
		return
	}

	if vc.NullRow {
		v.regs[destReg].SetNull()
		return
	}

	if !bc.IsValid() {
		v.regs[destReg].SetNull()
		return
	}

	rowid := bc.RowID()
	v.regs[destReg].SetInt(int64(rowid))
	v.regs[destReg].IsRowID = true
}

// execRowData reads the complete row data from the cursor at P1 into register P2.
func (v *VDBE) execRowData(pOp *Instruction) {
	destReg := pOp.P2

	vc, bc, ok := v.getCursor(pOp.P1)
	if !ok || bc == nil {
		v.regs[destReg].SetNull()
		return
	}

	if vc.NullRow {
		v.regs[destReg].SetNull()
		return
	}

	if !bc.IsValid() {
		v.regs[destReg].SetNull()
		return
	}

	data, err := bc.Data()
	if err != nil {
		v.err = err
		v.rc = ResultError
		v.regs[destReg].SetNull()
		return
	}

	if data == nil {
		// For index cursors, use the key instead
		key := bc.Key()
		if key != nil {
			v.regs[destReg].SetBlob(key)
		} else {
			v.regs[destReg].SetNull()
		}
		return
	}

	v.regs[destReg].SetBlob(data)
}

// execRowCell transfers a row from source cursor (P2) to prepare for insert
// on destination cursor (P1). P3 provides the rowid for intkey tables.
func (v *VDBE) execRowCell(pOp *Instruction) {
	// This opcode reads the current row from the source cursor (P2)
	// and prepares it for insertion via the next OpInsert/OpIdxInsert
	// with OPFLAG_PREFORMAT.
	dstIdx := pOp.P1
	srcIdx := pOp.P2
	rowidReg := pOp.P3

	_, srcBc, ok := v.getCursor(srcIdx)
	if !ok || srcBc == nil || !srcBc.IsValid() {
		return
	}

	// Read source data
	data, err := srcBc.Data()
	if err != nil {
		v.err = err
		v.rc = ResultError
		return
	}

	key := srcBc.Key()

	// Get rowid
	var rowid int64
	if rowidReg > 0 && rowidReg < len(v.regs) {
		rowid = v.regs[rowidReg].IntValue()
	} else {
		rowid = int64(srcBc.RowID())
	}

	// Store in destination cursor's context using registers
	// The next OpInsert will read from the same registers
	if dstIdx < len(v.cursors) {
		// Find a free register to store data
		// Convention: store key in rowidReg and data in rowidReg+1
		if rowidReg > 0 && rowidReg+1 < len(v.regs) {
			if data != nil {
				v.regs[rowidReg+1].SetBlob(data)
			} else if key != nil {
				v.regs[rowidReg+1].SetBlob(key)
			}
			v.regs[rowidReg].SetInt(rowid)
		}
	}
}

// execOffset reads a column value at a specific offset.
// P1 = cursor, P3 = destination register.
func (v *VDBE) execOffset(pOp *Instruction) {
	// Simplified: return the byte offset of the current row.
	// This is rarely used (SQLITE_ENABLE_OFFSET_SQL_FUNC).
	v.regs[pOp.P3].SetInt(0)
}

var _ = binary.BigEndian // ensure binary import used
// --- Aggregate opcode implementations ---

// execAggStep executes an aggregate step function.
// P1 = register holding aggregate state (created on first call)
// P2 = number of arguments
// P3 = start register of arguments
// P4 = *AggFuncInfo
func (v *VDBE) execAggStep(pOp *Instruction) {
	afi, ok := pOp.P4.(*AggFuncInfo)
	if !ok || afi.Step == nil {
		return
	}

	reg := &v.regs[pOp.P1]
	if reg.Pointer == nil {
		reg.Pointer = new(interface{})
	}

	// Collect arguments from registers P3..P3+P2-1
	nArgs := pOp.P2
	args := make([]*Mem, nArgs)
	for i := 0; i < nArgs; i++ {
		idx := pOp.P3 + i
		if idx < len(v.regs) {
			args[i] = &v.regs[idx]
		} else {
			args[i] = &Mem{Type: MemNull, IsNull: true}
		}
	}

	afi.Step(reg.Pointer, args)
}

// execAggFinal finalizes an aggregate function.
// P1 = register holding aggregate state
// P2 = destination register for result
// P4 = *AggFuncInfo
func (v *VDBE) execAggFinal(pOp *Instruction) {
	afi, ok := pOp.P4.(*AggFuncInfo)
	if !ok {
		return
	}

	reg := &v.regs[pOp.P1]
	destIdx := pOp.P2
	if destIdx == 0 {
		destIdx = pOp.P1
	}

	if afi.Finalize != nil && reg.Pointer != nil {
		result := afi.Finalize(reg.Pointer)
		if result != nil {
			v.regs[destIdx] = *result
		} else {
			v.regs[destIdx].SetNull()
		}
	} else {
		v.regs[destIdx].SetNull()
	}
}

// execAggInverse executes an aggregate inverse step (for window functions).
// P1 = register holding aggregate state
// P2 = number of arguments
// P3 = start register of arguments
// P4 = *AggFuncInfo
func (v *VDBE) execAggInverse(pOp *Instruction) {
	afi, ok := pOp.P4.(*AggFuncInfo)
	if !ok || afi.Inverse == nil {
		// If no inverse function, fall back to step
		v.execAggStep(pOp)
		return
	}

	reg := &v.regs[pOp.P1]
	if reg.Pointer == nil {
		reg.Pointer = new(interface{})
	}

	nArgs := pOp.P2
	args := make([]*Mem, nArgs)
	for i := 0; i < nArgs; i++ {
		idx := pOp.P3 + i
		if idx < len(v.regs) {
			args[i] = &v.regs[idx]
		} else {
			args[i] = &Mem{Type: MemNull, IsNull: true}
		}
	}

	afi.Inverse(reg.Pointer, args)
}

// --- Sort opcode implementations ---

// execSorterOpen opens a sorter and stores it in cursor P1.
func (v *VDBE) execSorterOpen(pOp *Instruction) {
	cursorIdx := pOp.P1
	if cursorIdx >= len(v.cursors) {
		newCursors := make([]*VdbeCursor, cursorIdx+1)
		copy(newCursors, v.cursors)
		v.cursors = newCursors
	}

	var sorter *Sorter
	if cmpFn, ok := pOp.P4.(func(a, b []byte) int); ok {
		sorter = NewSorterWithCompare(cmpFn)
	} else {
		sorter = NewSorter()
	}

	v.cursors[cursorIdx] = &VdbeCursor{
		Cursor:  sorter,
		NullRow: true,
		Ordered: true,
	}
}

// execSorterInsert inserts a record into the sorter at cursor P1.
func (v *VDBE) execSorterInsert(pOp *Instruction) {
	cursorIdx := pOp.P1
	if cursorIdx >= len(v.cursors) || v.cursors[cursorIdx] == nil {
		return
	}

	sorter, ok := v.cursors[cursorIdx].Cursor.(*Sorter)
	if !ok {
		return
	}

	var key []byte
	if pOp.P2 < len(v.regs) {
		key = v.regs[pOp.P2].BlobValue()
		if key == nil {
			key = v.regs[pOp.P2].Bytes
		}
	}

	var data []byte
	if pOp.P3 < len(v.regs) {
		data = v.regs[pOp.P3].BlobValue()
		if data == nil {
			data = v.regs[pOp.P3].Bytes
		}
	}

	if key == nil {
		key = []byte{}
	}
	if data == nil {
		data = []byte{}
	}

	sorter.Insert(key, data)
}

// execSorterSort sorts the sorter at cursor P1.
// Jumps to P2 if the sorter is empty.
func (v *VDBE) execSorterSort(pOp *Instruction, nextPC *int) {
	cursorIdx := pOp.P1
	if cursorIdx >= len(v.cursors) || v.cursors[cursorIdx] == nil {
		*nextPC = pOp.P2
		return
	}

	sorter, ok := v.cursors[cursorIdx].Cursor.(*Sorter)
	if !ok {
		*nextPC = pOp.P2
		return
	}

	if sorter.Count() == 0 {
		*nextPC = pOp.P2
		return
	}

	sorter.Sort()
	sorter.Rewind()
	v.cursors[cursorIdx].NullRow = false
}

// execSorterNext advances to the next sorted record.
// Jumps to P2 if no more records.
func (v *VDBE) execSorterNext(pOp *Instruction, nextPC *int) {
	cursorIdx := pOp.P1
	if cursorIdx >= len(v.cursors) || v.cursors[cursorIdx] == nil {
		*nextPC = pOp.P2
		return
	}

	sorter, ok := v.cursors[cursorIdx].Cursor.(*Sorter)
	if !ok || !sorter.Next() {
		*nextPC = pOp.P2
		return
	}

	v.cursors[cursorIdx].NullRow = false
}

// execSorterData reads data from the current sorter entry.
func (v *VDBE) execSorterData(pOp *Instruction) {
	cursorIdx := pOp.P1
	destIdx := pOp.P2

	if cursorIdx >= len(v.cursors) || v.cursors[cursorIdx] == nil {
		v.regs[destIdx].SetNull()
		return
	}

	sorter, ok := v.cursors[cursorIdx].Cursor.(*Sorter)
	if !ok {
		v.regs[destIdx].SetNull()
		return
	}

	data := sorter.Data()
	if data == nil {
		v.regs[destIdx].SetNull()
		return
	}
	v.regs[destIdx].SetBlob(data)
}

// execSorterCompare compares the current sorter key with a register value.
// Jumps to P2 if they differ.
func (v *VDBE) execSorterCompare(pOp *Instruction, nextPC *int) {
	cursorIdx := pOp.P1
	cmpRegIdx := pOp.P3

	if cursorIdx >= len(v.cursors) || v.cursors[cursorIdx] == nil {
		*nextPC = pOp.P2
		return
	}

	sorter, ok := v.cursors[cursorIdx].Cursor.(*Sorter)
	if !ok {
		*nextPC = pOp.P2
		return
	}

	sorterKey := sorter.Key()
	if sorterKey == nil || cmpRegIdx >= len(v.regs) {
		*nextPC = pOp.P2
		return
	}

	cmpVal := v.regs[cmpRegIdx].BlobValue()
	if cmpVal == nil {
		cmpVal = v.regs[cmpRegIdx].Bytes
	}

	if cmpVal == nil || CompareRecord(sorterKey, cmpVal) != 0 {
		*nextPC = pOp.P2
	}
}

// execSort sorts the sorter at cursor P1 (legacy sort opcode).
func (v *VDBE) execSort(pOp *Instruction) {
	cursorIdx := pOp.P1
	if cursorIdx >= len(v.cursors) || v.cursors[cursorIdx] == nil {
		return
	}

	sorter, ok := v.cursors[cursorIdx].Cursor.(*Sorter)
	if !ok {
		return
	}

	sorter.Sort()
	sorter.Rewind()
	v.cursors[cursorIdx].NullRow = false
}

// --- Cursor opcode implementations ---

// execOpenEphemeral opens an ephemeral table cursor.
func (v *VDBE) execOpenEphemeral(pOp *Instruction) {
	cursorIdx := pOp.P1
	if cursorIdx >= len(v.cursors) {
		newCursors := make([]*VdbeCursor, cursorIdx+1)
		copy(newCursors, v.cursors)
		v.cursors = newCursors
	}

	sorter := NewSorter()
	v.cursors[cursorIdx] = &VdbeCursor{
		Cursor:  sorter,
		NullRow: true,
		Ordered: true,
	}
}

// execOpenPseudo opens a pseudo-cursor that reads from a register.
func (v *VDBE) execOpenPseudo(pOp *Instruction) {
	cursorIdx := pOp.P1
	if cursorIdx >= len(v.cursors) {
		newCursors := make([]*VdbeCursor, cursorIdx+1)
		copy(newCursors, v.cursors)
		v.cursors = newCursors
	}

	v.cursors[cursorIdx] = &VdbeCursor{
		Cursor:   nil,
		NullRow:  true,
		Pseudo:   true,
		RootPage: pOp.P2, // reuse RootPage to store content register index
	}
}

// execOpenDup duplicates cursor P1 into cursor P2.
func (v *VDBE) execOpenDup(pOp *Instruction) {
	srcIdx := pOp.P1
	dstIdx := pOp.P2

	if srcIdx >= len(v.cursors) || v.cursors[srcIdx] == nil {
		return
	}

	if dstIdx >= len(v.cursors) {
		newCursors := make([]*VdbeCursor, dstIdx+1)
		copy(newCursors, v.cursors)
		v.cursors = newCursors
	}

	src := v.cursors[srcIdx]
	v.cursors[dstIdx] = &VdbeCursor{
		RootPage:  src.RootPage,
		Writeable: src.Writeable,
		NullRow:   src.NullRow,
		Ordered:   src.Ordered,
		Pseudo:    src.Pseudo,
		SeqCount:  src.SeqCount,
	}
	v.cursors[dstIdx].Cursor = src.Cursor
}

// execOpenAutoindex opens an automatic index cursor.
func (v *VDBE) execOpenAutoindex(pOp *Instruction) {
	cursorIdx := pOp.P1
	if cursorIdx >= len(v.cursors) {
		newCursors := make([]*VdbeCursor, cursorIdx+1)
		copy(newCursors, v.cursors)
		v.cursors = newCursors
	}

	sorter := NewSorter()
	v.cursors[cursorIdx] = &VdbeCursor{
		Cursor:  sorter,
		NullRow: true,
		Ordered: true,
	}
}

// --- RowSet opcode implementations ---

// execRowSetAdd adds a rowid to a RowSet stored in register P1.
func (v *VDBE) execRowSetAdd(pOp *Instruction) {
	reg := &v.regs[pOp.P1]
	if reg.Pointer == nil {
		reg.Pointer = encoding.NewRowSet()
	}

	rs, ok := reg.Pointer.(*encoding.RowSet)
	if !ok {
		rs = encoding.NewRowSet()
		reg.Pointer = rs
	}

	rowid := v.regs[pOp.P2].IntValue()
	rs.Insert(rowid)
}

// execRowSetRead reads the next rowid from a RowSet in register P1.
// Jumps to P2 if the RowSet is empty.
func (v *VDBE) execRowSetRead(pOp *Instruction, nextPC *int) {
	reg := &v.regs[pOp.P1]
	if reg.Pointer == nil {
		*nextPC = pOp.P2
		return
	}

	rs, ok := reg.Pointer.(*encoding.RowSet)
	if !ok {
		*nextPC = pOp.P2
		return
	}

	rowid, found := rs.Next()
	if !found {
		*nextPC = pOp.P2
		return
	}

	v.regs[pOp.P3].SetInt(rowid)
}

// execRowSetTest tests if a rowid is in the RowSet stored in register P1.
// Jumps to P2 if the rowid is already present.
func (v *VDBE) execRowSetTest(pOp *Instruction, nextPC *int) {
	reg := &v.regs[pOp.P1]
	if reg.Pointer == nil {
		return
	}

	rs, ok := reg.Pointer.(*encoding.RowSet)
	if !ok {
		return
	}

	rowid := v.regs[pOp.P3].IntValue()
	if rs.Test(rowid) {
		*nextPC = pOp.P2
	}
}
