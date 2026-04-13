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
	// Insert inserts a row into the database.
	Insert(cursor interface{}, key []byte, data []byte, rowid int64, seekResult int) error
	// Delete deletes the row at the cursor position.
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

// SchemaChanger handles in-memory schema modifications (drop table/index/trigger).
type SchemaChanger interface {
	DropTable(dbID int, name string) error
	DropIndex(dbID int, name string) error
	DropTrigger(dbID int, name string) error
}

// TableClearer clears all rows from a B-Tree table.
type TableClearer interface {
	ClearTable(rootPage int) (int64, error)
}

// SchemaParser parses schema from internal tables.
type SchemaParser interface {
	ParseSchema(dbID int, sql string) error
}

// TableLocker handles table locking operations.
type TableLocker interface {
	LockTable(dbID int, rootPage int, write bool) error
	UnlockTable(dbID int, rootPage int)
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
	lastCompare int           // Result of last comparison (-1, 0, +1)
	onceFlags   map[int]bool  // Once opcode: maps instruction PC to executed flag
	// Callbacks
	resultRowFunc func(regs []Mem, startIdx, count int)
	// Foreign key constraint counters
	fkConstraint int // immediate FK constraint counter
	deferredCons int // deferred FK constraint counter
	// Statement expiration flag
	expired bool
	// Permutation array set by OpPermutation, used by OpCompare
	permReg []int
}

// AggFuncInfo carries aggregate function callbacks for VDBE opcodes.
// P4 on OpAggStep/OpAggFinal/OpAggInverse holds a *AggFuncInfo.
type AggFuncInfo struct {
	Name     string
	Step     func(state interface{}, args []*Mem)
	Finalize func(state interface{}) *Mem
	Inverse  func(state interface{}, args []*Mem)
}

// FunctionCaller is the interface for calling registered SQL functions.
type FunctionCaller interface {
	CallFunction(name string, args []*Mem) *Mem
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
				if cursor, ok := v.cursors[pOp.P1].Cursor.(btree.BTCursor); ok {
					cursor.Close()
				}
				v.cursors[pOp.P1] = nil
			}

		case OpFirst:
			hasRow := false
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				v.cursors[pOp.P1].NullRow = false
				if cursor, ok := v.cursors[pOp.P1].Cursor.(btree.BTCursor); ok {
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
				if cursor, ok := v.cursors[pOp.P1].Cursor.(btree.BTCursor); ok {
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
				if cursor, ok := v.cursors[pOp.P1].Cursor.(btree.BTCursor); ok {
					var err error
					hasRow, err = cursor.Next()
					if err != nil {
						return false, err
					}
				}
			}
			if hasRow {
				nextPC = pOp.P2
			}

		case OpPrev:
			hasRow := false
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				if cursor, ok := v.cursors[pOp.P1].Cursor.(btree.BTCursor); ok {
					var err error
					hasRow, err = cursor.Prev()
					if err != nil {
						return false, err
					}
				}
			}
			if hasRow {
				nextPC = pOp.P2
			}

		case OpRewind:
			hasRow := false
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				v.cursors[pOp.P1].NullRow = false
				if cursor, ok := v.cursors[pOp.P1].Cursor.(btree.BTCursor); ok {
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

		// ─── Flow Control Opcodes ──────────────────────────────────────

		case OpJump:
			// Three-way jump based on last comparison result
			if v.lastCompare < 0 {
				nextPC = pOp.P1
			} else if v.lastCompare == 0 {
				nextPC = pOp.P2
			} else {
				nextPC = pOp.P3
			}

		case OpYield:
			// Swap PC with value in register P1 (coroutine yield/resume)
			pIn1 := &v.regs[pOp.P1]
			pcDest := int(pIn1.IntVal)
			pIn1.SetInt(int64(v.pc))
			nextPC = pcDest + 1

		case OpInitCoroutine:
			// Initialize coroutine register with entry point
			v.regs[pOp.P1].SetInt(int64(pOp.P3 - 1))
			if pOp.P2 != 0 {
				nextPC = pOp.P2
			}

		case OpEndCoroutine:
			// End a coroutine — jump to caller's Yield P2 target
			pIn1 := &v.regs[pOp.P1]
			callerPC := int(pIn1.IntVal)
			if callerPC >= 0 && callerPC < nOp {
				callerOp := &aOp[callerPC]
				pIn1.SetInt(int64(v.pc - 1))
				nextPC = callerOp.P2
			}

		case OpElseEq:
			// Jump if last comparison was equal (else-equal optimization)
			if v.lastCompare == 0 {
				nextPC = pOp.P2
			}

		case OpOnce:
			// Execute only once — jump to P2 on subsequent visits
			if v.onceFlags == nil {
				v.onceFlags = make(map[int]bool)
			}
			if v.onceFlags[v.pc] {
				nextPC = pOp.P2
			} else {
				v.onceFlags[v.pc] = true
			}

		case OpBeginSubrtn:
			// Begin subroutine — set output register to NULL
			v.regs[pOp.P2].SetNull()

		case OpIntCmp:
			// Compare two integer registers, store result for OpJump
			a := v.regs[pOp.P1].IntValue()
			b := v.regs[pOp.P3].IntValue()
			if a < b {
				v.lastCompare = -1
			} else if a > b {
				v.lastCompare = 1
			} else {
				v.lastCompare = 0
			}

		// ─── Type/Value Opcodes ───────────────────────────────────

		case OpMustBeInt:
			// Ensure register P1 is an integer; jump to P2 or error if not
			pIn1 := &v.regs[pOp.P1]
			if pIn1.Type == MemInt {
				break
			}
			coerced := false
			switch pIn1.Type {
			case MemFloat:
				iv := int64(pIn1.FloatVal)
				if float64(iv) == pIn1.FloatVal {
					pIn1.SetInt(iv)
					coerced = true
				}
			case MemStr:
				if f, ok := tryParseFloat(pIn1); ok {
					iv := int64(f)
					if f == float64(iv) {
						pIn1.SetInt(iv)
						coerced = true
					}
				}
			}
			if !coerced {
				if pOp.P2 == 0 {
					v.rc = ResultError
					v.err = fmt.Errorf("datatype mismatch")
					return false, v.err
				}
				nextPC = pOp.P2
			}

		case OpCast:
			// Cast register P1 to the affinity specified by P2
			pIn1 := &v.regs[pOp.P1]
			switch pOp.P2 {
			case 0x41: // 'A' - BLOB
				if pIn1.Type == MemStr {
					pIn1.Type = MemBlob
				} else if pIn1.Type == MemInt || pIn1.Type == MemFloat {
					pIn1.SetBlob(nil)
				}
			case 0x42: // 'B' - TEXT
				switch pIn1.Type {
				case MemInt:
					pIn1.SetText(pIn1.StringValue())
				case MemFloat:
					pIn1.SetText(pIn1.StringValue())
				case MemBlob:
					pIn1.Type = MemStr
				}
			case 0x43: // 'C' - NUMERIC
				if pIn1.Type == MemStr {
					if f, ok := tryParseFloat(pIn1); ok {
						if f == float64(int64(f)) {
							pIn1.SetInt(int64(f))
						} else {
							pIn1.SetFloat(f)
						}
					}
				}
			case 0x44: // 'D' - INTEGER
				switch pIn1.Type {
				case MemFloat:
					pIn1.SetInt(int64(pIn1.FloatVal))
				case MemStr:
					if f, ok := tryParseFloat(pIn1); ok {
						pIn1.SetInt(int64(f))
					}
				}
			case 0x45: // 'E' - REAL
				switch pIn1.Type {
				case MemInt:
					pIn1.SetFloat(float64(pIn1.IntVal))
				case MemStr:
					if f, ok := tryParseFloat(pIn1); ok {
						pIn1.SetFloat(f)
					}
				}
			}

		case OpAddImm:
			// Add immediate value P2 to register P1 (as integer)
			pIn1 := &v.regs[pOp.P1]
			pIn1.SetInt(pIn1.IntValue() + int64(pOp.P2))

		case OpDecrJumpZero:
			// Decrement register P1 and jump to P2 if it becomes zero
			pIn1 := &v.regs[pOp.P1]
			if pIn1.IntVal > math.MinInt64 {
				pIn1.SetInt(pIn1.IntVal - 1)
			}
			if pIn1.IntVal == 0 {
				nextPC = pOp.P2
			}

		case OpIfPos:
			// Jump to P2 if register P1 > 0; subtract P3 from P1
			pIn1 := &v.regs[pOp.P1]
			if pIn1.IntVal > 0 {
				pIn1.SetInt(pIn1.IntVal - int64(pOp.P3))
				nextPC = pOp.P2
			}

		case OpIfNullRow:
			// Jump to P2 if cursor P1 is on a null row; set reg[P3] to NULL
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				if v.cursors[pOp.P1].NullRow {
					if pOp.P3 > 0 && pOp.P3 < len(v.regs) {
						v.regs[pOp.P3].SetNull()
					}
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
		case OpIfNotOpen:
			// Jump to P2 if cursor P1 is not open
			if pOp.P1 >= len(v.cursors) || v.cursors[pOp.P1] == nil {
				nextPC = pOp.P2
			}

		case OpIsNull:
			// Jump to P2 if register P1 is NULL
			if v.regs[pOp.P1].Type == MemNull {
				nextPC = pOp.P2
			}

		case OpNotNull:
			// Jump to P2 if register P1 is not NULL
			if v.regs[pOp.P1].Type != MemNull {
				nextPC = pOp.P2
			}

		// ─── DDL Opcodes ───────────────────────────────────────

		case OpCreateBTree:
			// Create a new B-Tree and store root page in reg[P2]
			if creator, ok := v.db.(BTreeCreator); ok {
				rootPage, err := creator.CreateBTree(pOp.P3)
				if err != nil {
					v.rc = ResultError
					v.err = err
					return false, v.err
				}
				v.regs[pOp.P2].SetInt(int64(rootPage))
			}

		case OpCreateTable:
			// Create a new table B-Tree (integer key)
			if creator, ok := v.db.(BTreeCreator); ok {
				rootPage, err := creator.CreateBTree(1) // BTREE_INTKEY
				if err != nil {
					v.rc = ResultError
					v.err = err
					return false, v.err
				}
				v.regs[pOp.P2].SetInt(int64(rootPage))
			}

		case OpCreateIndex:
			// Create a new index B-Tree (blob key)
			if creator, ok := v.db.(BTreeCreator); ok {
				rootPage, err := creator.CreateBTree(2) // BTREE_BLOBKEY
				if err != nil {
					v.rc = ResultError
					v.err = err
					return false, v.err
				}
				v.regs[pOp.P2].SetInt(int64(rootPage))
			}

		case OpDestroy:
			// Destroy a B-Tree rooted at page P1
			if destroyer, ok := v.db.(BTreeDestroyer); ok {
				if err := destroyer.DestroyBTree(pOp.P1); err != nil {
					v.rc = ResultError
					v.err = err
					return false, v.err
				}
				v.regs[pOp.P2].SetInt(0) // iMoved (auto-vacuum, simplified)
			}

		case OpDropTable:
			// Drop table named P4 from database P1
			if sc, ok := v.db.(SchemaChanger); ok {
				name, _ := pOp.P4.(string)
				if err := sc.DropTable(pOp.P1, name); err != nil {
					v.rc = ResultError
					v.err = err
					return false, v.err
				}
			}

		case OpDropIndex:
			// Drop index named P4 from database P1
			if sc, ok := v.db.(SchemaChanger); ok {
				name, _ := pOp.P4.(string)
				if err := sc.DropIndex(pOp.P1, name); err != nil {
					v.rc = ResultError
					v.err = err
					return false, v.err
				}
			}

		case OpDropTrigger:
			// Drop trigger named P4 from database P1
			if sc, ok := v.db.(SchemaChanger); ok {
				name, _ := pOp.P4.(string)
				if err := sc.DropTrigger(pOp.P1, name); err != nil {
					v.rc = ResultError
					v.err = err
					return false, v.err
				}
			}

		case OpClear:
			// Clear all rows from table at root page P1
			if clearer, ok := v.db.(TableClearer); ok {
				nChange, err := clearer.ClearTable(pOp.P1)
				if err != nil {
					v.rc = ResultError
					v.err = err
					return false, v.err
				}
				if pOp.P3 > 0 && pOp.P3 < len(v.regs) {
					v.regs[pOp.P3].SetInt(v.regs[pOp.P3].IntValue() + nChange)
				}
			}

		case OpParseSchema:
			// Parse schema from internal table
			if parser, ok := v.db.(SchemaParser); ok {
				sql := ""
				if s, ok := pOp.P4.(string); ok {
					sql = s
				}
				if err := parser.ParseSchema(pOp.P1, sql); err != nil {
					v.rc = ResultError
					v.err = err
					return false, v.err
				}
			}

		case OpTableLock:
			// Lock table at root page P2 in database P1
			if locker, ok := v.db.(TableLocker); ok {
				write := pOp.P3 != 0
				if err := locker.LockTable(pOp.P1, pOp.P2, write); err != nil {
					v.rc = ResultError
					v.err = err
					return false, v.err
				}
			}

		case OpTableUnlock:
			// Unlock table in database P1
			if locker, ok := v.db.(TableLocker); ok {
				locker.UnlockTable(pOp.P1, pOp.P2)
			}

		case OpFunction:
			v.execFunction(pOp)

		// OpCompare compares two vectors of registers.
		// P1/P2 are starting registers, P3 is the count.
		// The comparison result is stored in v.lastCompare for use by OpJump.
		case OpCompare:
			v.execCompare(pOp)

		// OpConcat concatenates strings from registers P1 and P2 into P3.
		// Result is NULL if either input is NULL. P3 = P2 || P1.
		case OpConcat:
			pIn1 := &v.regs[pOp.P1]
			pIn2 := &v.regs[pOp.P2]
			pOut := &v.regs[pOp.P3]
			if pIn1.Type == MemNull || pIn2.Type == MemNull {
				pOut.SetNull()
			} else {
				s2 := pIn2.TextValue()
				s1 := pIn1.TextValue()
				pOut.SetText(s2 + s1)
			}

		// OpAnd performs three-valued logical AND.
		// P1 and P2 are input registers, P3 is output.
		// 0=false, 1=true, 2=null. AND truth table:
		// {0,0,0, 0,1,2, 0,2,2}
		case OpAnd:
			v1 := boolValue(&v.regs[pOp.P1])
			v2 := boolValue(&v.regs[pOp.P2])
			andLogic := [9]int{0, 0, 0, 0, 1, 2, 0, 2, 2}
			result := andLogic[v1*3+v2]
			pOut := &v.regs[pOp.P3]
			if result == 2 {
				pOut.SetNull()
			} else {
				pOut.SetInt(int64(result))
			}

		// OpOr performs three-valued logical OR.
		// P1 and P2 are input registers, P3 is output.
		// OR truth table: {0,1,2, 1,1,1, 2,1,2}
		case OpOr:
			v1 := boolValue(&v.regs[pOp.P1])
			v2 := boolValue(&v.regs[pOp.P2])
			orLogic := [9]int{0, 1, 2, 1, 1, 1, 2, 1, 2}
			result := orLogic[v1*3+v2]
			pOut := &v.regs[pOp.P3]
			if result == 2 {
				pOut.SetNull()
			} else {
				pOut.SetInt(int64(result))
			}

		// OpCollSeq sets the collation sequence. P4 holds collation info.
		// If P1 is non-zero, set register P1 to 0.
		// Mostly a no-op in this implementation.
		case OpCollSeq:
			if pOp.P1 != 0 && pOp.P1 < len(v.regs) {
				v.regs[pOp.P1].SetInt(0)
			}

		// OpCommit commits the current transaction.
		case OpCommit:
			if v.db != nil {
				if err := v.db.Commit(); err != nil {
					v.rc = ResultError
					v.err = err
					return false, v.err
				}
			}

		// OpRollback rolls back the current transaction.
		case OpRollback:
			if v.db != nil {
				if err := v.db.Rollback(); err != nil {
					v.rc = ResultError
					v.err = err
					return false, v.err
				}
			}

		// OpSavepoint manages savepoints.
		// P1: 0=BEGIN, 1=RELEASE, 2=ROLLBACK. P4 is the savepoint name.
		case OpSavepoint:
			if v.db != nil {
				switch pOp.P1 {
				case 0: // BEGIN
					v.db.BeginTransaction(false)
				case 1: // RELEASE
					v.db.Commit()
				case 2: // ROLLBACK
					v.db.Rollback()
				}
			}

		// OpRelease releases a savepoint.
		case OpRelease:
			if v.db != nil {
				v.db.Commit()
			}

		// OpCheckpoint checkpoints the database.
		// P1 is db index, P2 is mode, P3 is output register.
		case OpCheckpoint:
			if pOp.P3 > 0 && pOp.P3 < len(v.regs) {
				v.regs[pOp.P3].SetInt(0)
			}

		// OpVacuum vacuums the database.
		case OpVacuum:
			// Vacuum is a no-op in this implementation

		// OpIncrVacuum performs incremental vacuum.
		// P1 is db index, P2 is jump target if vacuum is done.
		case OpIncrVacuum:
			nextPC = pOp.P2

		// OpJournalMode changes or queries the journal mode.
		// P1 is db, P2 is output register, P3 is new mode.
		case OpJournalMode:
			if pOp.P2 > 0 && pOp.P2 < len(v.regs) {
				v.regs[pOp.P2].SetText("memory")
			}

		// OpMaxPgcnt sets/queries the maximum page count.
		// P1 is db, P2 is output register, P3 is new max (0 to query).
		case OpMaxPgcnt:
			if pOp.P2 > 0 && pOp.P2 < len(v.regs) {
				v.regs[pOp.P2].SetInt(0)
			}

		// OpPagecount returns the current page count.
		// P1 is db, P2 is output register.
		case OpPagecount:
			if pOp.P2 > 0 && pOp.P2 < len(v.regs) {
				v.regs[pOp.P2].SetInt(0)
			}

		// OpReadCookie reads a schema cookie value.
		// P1 is db index, P2 is output register, P3 is cookie id.
		case OpReadCookie:
			if pOp.P2 > 0 && pOp.P2 < len(v.regs) {
				var cookieVal int64
				if cs, ok := v.db.(CookieStore); ok && v.db != nil {
					val, err := cs.GetCookie(pOp.P3)
					if err == nil {
						cookieVal = val
					}
				}
				v.regs[pOp.P2].SetInt(cookieVal)
			}

		// OpSetCookie writes a cookie value.
		// P1 is db, P2 is cookie id, P3 is value.
		case OpSetCookie:
			if cs, ok := v.db.(CookieStore); ok && v.db != nil {
				cs.SetCookie(pOp.P2, int64(pOp.P3))
			}

		// OpWriteCookie writes a cookie value (alias for SetCookie).
		case OpWriteCookie:
			if cs, ok := v.db.(CookieStore); ok && v.db != nil {
				cs.SetCookie(pOp.P2, int64(pOp.P3))
			}

		// OpLoadAnalysis loads analysis data for the database.
		case OpLoadAnalysis:
			// Analysis loading is a no-op in this implementation

		// OpExpire expires prepared statements.
		// P1=0 expires all; P1=1 expires current. P2 controls immediate vs deferred.
		case OpExpire:
			if pOp.P1 != 0 {
				v.expired = true
			}

		// OpExplain outputs the explain text. No-op in execution.
		case OpExplain:
			// No-op during execution

		// OpTrace outputs trace information. No-op.
		case OpTrace:
			// No-op

		// OpNoop does nothing.
		case OpNoop:
			// Intentionally empty

		// OpInterrupt checks for an interrupt condition.
		case OpInterrupt:
			// No-op in this implementation

		// OpProgram executes a sub-program (trigger).
		// P4 is the sub-program, P3 is the register for frame data.
		case OpProgram:
			// Sub-program execution (triggers) not yet fully implemented.

		// OpParam copies a parameter from the parent frame.
		// P1 is offset into parent frame, P2 is output register.
		case OpParam:
			if pOp.P2 > 0 && pOp.P2 < len(v.regs) {
				v.regs[pOp.P2].SetNull()
			}

		// OpPermutation sets a column permutation for the next OpCompare.
		// P4 is an int array describing the permutation.
		case OpPermutation:
			if perm, ok := pOp.P4.([]int); ok {
				v.permReg = perm
			}

		// --- Virtual Table Opcodes (stubs) ---

		// OpVBegin begins a virtual table transaction.
		case OpVBegin:
			// Stub: virtual table transactions not yet implemented

		// OpVCreate creates a virtual table.
		case OpVCreate:
			// Stub: virtual table creation not yet implemented

		// OpVDestroy destroys a virtual table.
		case OpVDestroy:
			// Stub: virtual table destruction not yet implemented

		// OpVOpen opens a virtual table cursor.
		case OpVOpen:
			// Stub: virtual table cursors not yet implemented

		// OpVFilter filters a virtual table result set.
		// P1 is cursor, P2 is jump target if empty.
		case OpVFilter:
			nextPC = pOp.P2

		// OpVColumn reads a column from a virtual table.
		// P1 is cursor, P2 is column index, P3 is output register.
		case OpVColumn:
			if pOp.P3 > 0 && pOp.P3 < len(v.regs) {
				v.regs[pOp.P3].SetNull()
			}

		// OpVNext advances to the next row in a virtual table.
		// P1 is cursor, P2 is jump target if more rows.
		case OpVNext:
			nextPC = pOp.P2

		// OpVRename renames a virtual table.
		case OpVRename:
			// Stub: virtual table rename not yet implemented

		// OpVUpdate updates a row in a virtual table.
		case OpVUpdate:
			// Stub: virtual table update not yet implemented

		// OpVCheck checks virtual table integrity.
		case OpVCheck:
			if pOp.P2 > 0 && pOp.P2 < len(v.regs) {
				v.regs[pOp.P2].SetNull()
			}

		// OpVIn handles virtual table IN constraint.
		case OpVIn:
			if pOp.P2 > 0 && pOp.P2 < len(v.regs) {
				v.regs[pOp.P2].SetNull()
			}

		// --- Type Check and Maintenance Opcodes ---

		// OpTypeCheck validates register types against a schema.
		case OpTypeCheck:
			// Type checking is a no-op in this implementation

		// OpTrim trims the database file size.
		case OpTrim:
			// Database trimming is a no-op in this implementation

		// OpAbortable checks if the current operation can be aborted.
		case OpAbortable:
			// Debug assertion opcode, no-op in this implementation

		// OpFkCheck checks for immediate foreign key constraints.
		case OpFkCheck:
			// FK checking is a no-op in this implementation

		// OpFkCounter increments a foreign key constraint counter.
		// P1: 0=statement counter, 1=deferred counter. P2 is increment.
		case OpFkCounter:
			if pOp.P1 != 0 {
				v.deferredCons += pOp.P2
			} else {
				v.fkConstraint += pOp.P2
			}

		// OpFkIfZero jumps to P2 if the FK counter is zero.
		// P1: 0=statement counter, 1=deferred counter.
		case OpFkIfZero:
			var counter int
			if pOp.P1 != 0 {
				counter = v.deferredCons
			} else {
				counter = v.fkConstraint
			}
			if counter == 0 {
				nextPC = pOp.P2
			}

		// ─── Part 1: Already-defined but unimplemented opcodes ────────

		case OpInt64:
			// Load a 64-bit integer from P4 into register P2.
			if p4val, ok := pOp.P4.(int64); ok {
				v.regs[pOp.P2].SetInt(p4val)
			}

		case OpString:
			// Load a string from P4 into register P2. P1 = length.
			if s, ok := pOp.P4.(string); ok {
				if pOp.P1 > 0 && pOp.P1 < len(s) {
					v.regs[pOp.P2].SetText(s[:pOp.P1])
				} else {
					v.regs[pOp.P2].SetText(s)
				}
			}

		case OpHaltIfNull:
			// If register P3 is NULL, fall through into OpHalt.
			if v.regs[pOp.P3].Type == MemNull {
				v.halt = true
				if pOp.P1 != 0 {
					v.rc = ResultCode(pOp.P1)
					errMsg := "constraint failed"
					if pOp.P4 != nil {
						if s, ok := pOp.P4.(string); ok {
							errMsg = s
						}
					}
					v.err = fmt.Errorf("%s", errMsg)
					return false, v.err
				}
				return false, nil
			}

		case OpCrossjoin:
			// Set crossjoin flag on cursor P1.
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				v.cursors[pOp.P1].Ordered = false
			}

		case OpFilter:
			// Bloom filter check. P1 = filter blob register,
			// P3 = start of key regs, P4 = count. Jump to P2 if not present.
			filterReg := &v.regs[pOp.P1]
			if filterReg.Type != MemBlob || len(filterReg.Bytes) == 0 {
				nextPC = pOp.P2
				break
			}
			filterBlob := filterReg.Bytes
			var nFields int
			switch nf := pOp.P4.(type) {
			case int:
				nFields = nf
			case int64:
				nFields = int(nf)
			default:
				nFields = 1
			}
			var h uint64
			for i := 0; i < nFields; i++ {
				idx := pOp.P3 + i
				if idx >= len(v.regs) {
					h += 4093
					continue
				}
				reg := &v.regs[idx]
				switch reg.Type {
				case MemInt:
					h += uint64(reg.IntVal)
				case MemFloat:
					h += uint64(int64(reg.FloatVal))
				case MemStr, MemBlob:
					h += 4093 + uint64(reg.Type)
				}
			}
			if len(filterBlob) > 0 {
				bitIdx := h % uint64(len(filterBlob)*8)
				byteIdx := bitIdx / 8
				bitOff := bitIdx % 8
				if int(byteIdx) < len(filterBlob) {
					if filterBlob[byteIdx]&(1<<bitOff) == 0 {
						nextPC = pOp.P2
					}
				}
			}

		case OpUpdate:
			// Update current row at cursor P1 with data from register P3.
			cursorIdx := pOp.P1
			if cursorIdx >= len(v.cursors) || v.cursors[cursorIdx] == nil {
				break
			}
			vc := v.cursors[cursorIdx]
			cursor := vc.Cursor
			var data []byte
			if pOp.P3 > 0 && pOp.P3 < len(v.regs) && v.regs[pOp.P3].Bytes != nil {
				data = v.regs[pOp.P3].Bytes
			}
			var rowid int64
			if pOp.P2 > 0 && pOp.P2 < len(v.regs) {
				rowid = v.regs[pOp.P2].IntValue()
			}
			keyBuf := make([]byte, 9)
			keyLen := putVarint(keyBuf, rowid)
			if cursor != nil {
				if err := v.db.Insert(cursor, keyBuf[:keyLen], data, rowid, 0); err != nil {
					v.err = err
					v.rc = ResultError
				}
			}

		// ─── Part 2: New opcodes from C version ─────────────────────

		case OpAffinity:
			// Apply column affinity to a range of registers.
			// P4 = affinity string, P1 = start reg, P2 = count.
			affStr, ok := pOp.P4.(string)
			if !ok || len(affStr) == 0 {
				break
			}
			for i := 0; i < len(affStr) && i < pOp.P2; i++ {
				reg := &v.regs[pOp.P1+i]
				applyAffinity(reg, affStr[i])
			}

		case OpIntCopy:
			// Copy integer value from register P1 to register P2.
			pIn1 := &v.regs[pOp.P1]
			v.regs[pOp.P2].SetInt(pIn1.IntValue())

		case OpSoftNull:
			// Set register P1 to NULL but preserve type info hint.
			v.regs[pOp.P1].IsNull = true
			v.regs[pOp.P1].Type = MemNull

		case OpSeekRowid:
			// Seek cursor P1 to rowid from register P3. Jump to P2 if not found.
			_, bc, ok := v.getCursor(pOp.P1)
			if !ok || bc == nil {
				if pOp.P2 > 0 {
					nextPC = pOp.P2
				}
				break
			}
			rowid := v.regs[pOp.P3].IntValue()
			result, err := bc.SeekRowid(btree.RowID(rowid))
			if err != nil {
				v.err = err
				v.rc = ResultError
				break
			}
			if result != btree.SeekFound {
				if pOp.P2 > 0 {
					nextPC = pOp.P2
				}
			}

		case OpSeekEnd:
			// Position cursor P1 at end. If already valid, no-op.
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				vc := v.cursors[pOp.P1]
				if cursor, ok := vc.Cursor.(btree.BTCursor); ok && cursor.IsValid() {
					break
				}
				if cursor, ok := vc.Cursor.(btree.BTCursor); ok {
					cursor.Last()
					vc.NullRow = false
				}
			}

		case OpReopenIdx:
			// Reopen index cursor P1 on root page P2 if already open on same page.
			if pOp.P1 < len(v.cursors) && v.cursors[pOp.P1] != nil {
				vc := v.cursors[pOp.P1]
				if vc.RootPage == pOp.P2 {
					vc.NullRow = true
					if cursor, ok := vc.Cursor.(btree.BTCursor); ok {
						cursor.First()
					}
					break
				}
			}
			v.openCursor(pOp.P1, pOp.P2, false)

		case OpCount:
			// Count entries in cursor P1's B-Tree, store in register P2.
			if pOp.P1 >= len(v.cursors) || v.cursors[pOp.P1] == nil {
				v.regs[pOp.P2].SetInt(0)
				break
			}
			vc := v.cursors[pOp.P1]
			var count int64
			if bc, ok := vc.Cursor.(btree.BTCursor); ok {
				count = 0
				if hasFirst, _ := bc.First(); hasFirst {
					count = 1
					for {
						hasNext, err := bc.Next()
						if err != nil || !hasNext {
							break
						}
						count++
					}
					bc.First()
				}
			if pOp.P3 != 0 && count > 0 {
			}
				est := int64(1)
				for est < count {
					est *= 2
				}
				v.regs[pOp.P2].SetInt(est)
			} else {
				v.regs[pOp.P2].SetInt(count)
			}

		case OpOffsetLimit:
			// Compute OFFSET+LIMIT: r[P2] = r[P1] + max(r[P3], 0).
			limit := v.regs[pOp.P1].IntValue()
			offset := v.regs[pOp.P3].IntValue()
			if offset < 0 {
				offset = 0
			}
			if limit <= 0 {
				v.regs[pOp.P2].SetInt(-1)
			} else {
				result := limit + offset
				if result < limit {
					v.regs[pOp.P2].SetInt(-1)
				} else {
					v.regs[pOp.P2].SetInt(result)
				}
			}

		case OpIsTrue:
			// Check truthiness with NULL handling. P3=NULL semantics, P4=invert.
			pIn1 := &v.regs[pOp.P1]
			nullSemantics := pOp.P3
			invertFlag := 0
			if pOp.P4 != nil {
				switch v := pOp.P4.(type) {
				case int:
					invertFlag = v
				case int64:
					invertFlag = int(v)
				}
			}
			if pIn1.Type == MemNull {
				if nullSemantics != 0 {
					v.regs[pOp.P2].SetInt(int64(1 ^ invertFlag))
				} else {
					v.regs[pOp.P2].SetInt(int64(0 ^ invertFlag))
				}
			} else {
				var boolVal int64
				if pIn1.Bool() {
					boolVal = 1
				}
				v.regs[pOp.P2].SetInt(boolVal ^ int64(invertFlag))
			}

		case OpIsType:
			// Check column serial type against mask in P5. Jump to P2 on match.
			var typeMask int
			if pOp.P1 >= 0 {
				if pOp.P1 >= len(v.cursors) || v.cursors[pOp.P1] == nil {
					break
				}
				vc := v.cursors[pOp.P1]
				if vc.NullRow {
					typeMask = 0x10
				} else {
					cursor, ok := vc.Cursor.(btree.BTCursor)
					if !ok || !cursor.IsValid() {
						typeMask = 0x10
					} else {
						typeMask = typeMaskFromCursor(cursor, pOp.P3)
					}
				}
			} else {
				if pOp.P3 < len(v.regs) {
					typeMask = typeMaskFromMem(&v.regs[pOp.P3])
				}
			}
			if typeMask&pOp.P5 != 0 {
				nextPC = pOp.P2
			}

		case OpZeroOrNull:
			// If either P1 or P3 is NULL, store NULL in P2. Otherwise store 0.
			if v.regs[pOp.P1].Type == MemNull || v.regs[pOp.P3].Type == MemNull {
				v.regs[pOp.P2].SetNull()
			} else {
				v.regs[pOp.P2].SetInt(0)
			}

		case OpRealAffinity:
			// If register P1 is an integer, coerce it to float.
			pIn1 := &v.regs[pOp.P1]
			if pIn1.Type == MemInt {
				pIn1.SetFloat(float64(pIn1.IntVal))
			}

		case OpMemMax:
			// Set register P1 to max(current value, value from P2).
			pIn1 := &v.regs[pOp.P1]
			pIn2 := &v.regs[pOp.P2]
			val1 := pIn1.IntValue()
			val2 := pIn2.IntValue()
			if val1 < val2 {
				pIn1.SetInt(val2)
			}

		case OpIntegrityCk:
			// Run integrity check on B-Trees.
			destReg := pOp.P1 + 1
			if destReg < len(v.regs) {
				v.regs[destReg].SetNull()
			}

		case OpAggStep1:
			// Aggregate step variant. P1=inverse flag, P2=arg start, P3=accum reg.
			afi, ok := pOp.P4.(*AggFuncInfo)
			if !ok {
				break
			}
			reg := &v.regs[pOp.P3]
			if reg.Pointer == nil {
				reg.Pointer = new(interface{})
			}
			nArgs := pOp.P5
			if nArgs <= 0 {
				nArgs = 1
			}
			args := make([]*Mem, nArgs)
			for i := 0; i < nArgs; i++ {
				idx := pOp.P2 + i
				if idx < len(v.regs) {
					args[i] = &v.regs[idx]
				} else {
					args[i] = &Mem{Type: MemNull, IsNull: true}
				}
			}
			if pOp.P1 != 0 && afi.Inverse != nil {
				afi.Inverse(reg.Pointer, args)
			} else if afi.Step != nil {
				afi.Step(reg.Pointer, args)
			}

		case OpPureFunc:
			// Pure (deterministic) function call - same as OpFunction.
			v.execFunction(pOp)

		case OpSqlExec:
			// Execute SQL from P4 string. No-op in current implementation.

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
	v.lastCompare = 0
	v.onceFlags = nil

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
				v.lastCompare = 0
				return true
			}
		}
		return false
	}

	cmp := MemCompare(pIn1, pIn3)
	v.lastCompare = cmp

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

	// Special handling for sorter cursors
	if sorter, ok := vc.Cursor.(*Sorter); ok {
		data := sorter.Data()
		if data == nil {
			v.regs[destIdx].SetNull()
			return
		}
		values, err := ParseRecord(data)
		if err != nil || colIdx >= len(values) {
			v.regs[destIdx].SetNull()
			return
		}
		result := MemFromValue(values[colIdx])
		v.regs[destIdx] = *result
		return
	}

	cursor, ok := vc.Cursor.(btree.BTCursor)
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
		cursor, ok := v.cursors[cursorIdx].Cursor.(btree.BTCursor)
		if ok {
			// Try to find a new row ID by going to last and incrementing
			if hasRow, _ := cursor.Last(); hasRow {
				lastID := int64(cursor.RowID())
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

	// Insert the row
	if cursor != nil {
		if err := v.db.Insert(cursor, keyBuf[:keyLen], data, rowid, 0); err != nil {
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

	if cursor != nil {
		if err := v.db.Delete(cursor); err != nil {
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

	// For index inserts, use Insert method on db
	if err := v.db.Insert(vc.Cursor, key, nil, 0, 0); err != nil {
		v.err = err
		v.rc = ResultError
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
	if bc.IsValid() {
		if err := v.db.Delete(vc.Cursor); err != nil {
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

func boolValue(m *Mem) int {
	if m.Type == MemNull {
		return 2
	}
	if m.Bool() {
		return 1
	}
	return 0
}

// execFunction implements OpFunction: invokes a registered SQL function.
// P4 is a FuncInfo, P2 is the start of argument registers, P3 is the output register.
func (v *VDBE) execFunction(pOp *Instruction) {
	fi, ok := pOp.P4.(*FuncInfo)
	if !ok || fi == nil {
		v.regs[pOp.P3].SetNull()
		return
	}

	// Collect arguments from registers starting at P2
	argCount := fi.ArgCount
	if argCount < 0 {
		// Variable args: P5 contains the actual count
		argCount = pOp.P5
	}
	if argCount < 0 {
		argCount = 0
	}

	args := make([]*Mem, argCount)
	for i := 0; i < argCount; i++ {
		idx := pOp.P2 + i
		if idx < len(v.regs) {
			args[i] = &v.regs[idx]
		} else {
			args[i] = &Mem{Type: MemNull, IsNull: true}
		}
	}

	// Try to look up and invoke the function via FunctionCaller interface
	if fc, ok := v.db.(FunctionCaller); ok {
		result := fc.CallFunction(fi.Name, args)
		if result != nil {
			v.regs[pOp.P3] = *result
		} else {
			v.regs[pOp.P3].SetNull()
		}
	} else {
		// No function registry available; result is null
		v.regs[pOp.P3].SetNull()
	}
}

// execCompare implements OpCompare: compares two vectors of registers
// and stores the result in v.lastCompare for subsequent OpJump.
// P1 and P2 are starting register indices, P3 is the vector length.
func (v *VDBE) execCompare(pOp *Instruction) {
	n := pOp.P3
	if n <= 0 {
		v.lastCompare = 0
		return
	}

	p1 := pOp.P1
	p2 := pOp.P2

	for i := 0; i < n; i++ {
		var idx1, idx2 int
		if v.permReg != nil && i < len(v.permReg) {
			idx1 = p1 + v.permReg[i]
			idx2 = p2 + v.permReg[i]
		} else {
			idx1 = p1 + i
			idx2 = p2 + i
		}

		if idx1 >= len(v.regs) || idx2 >= len(v.regs) {
			continue
		}

		v.lastCompare = MemCompare(&v.regs[idx1], &v.regs[idx2])
		if v.lastCompare != 0 {
			// Clear permutation after use
			v.permReg = nil
			return
		}
	}

	v.lastCompare = 0
	v.permReg = nil
}

// Ensure binary is available for record operations.

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

// --- Affinity and type helpers for new opcodes ---

// applyAffinity applies a column affinity to a register value.
// Affinity characters: 't'=text, 'n'=numeric, 'i'=integer, 'b'=blob, 'u'=unspecified/none.
func applyAffinity(reg *Mem, aff byte) {
	switch aff {
	case 'i': // INTEGER affinity
		switch reg.Type {
		case MemStr:
			// Try to parse as integer
			s := string(reg.Bytes)
			var v int64
			if _, err := fmt.Sscanf(s, "%d", &v); err == nil {
				reg.SetInt(v)
			}
		case MemFloat:
			reg.SetInt(int64(reg.FloatVal))
		}
	case 'n': // NUMERIC affinity
		switch reg.Type {
		case MemStr:
			s := string(reg.Bytes)
			var v int64
			if _, err := fmt.Sscanf(s, "%d", &v); err == nil {
				reg.SetInt(v)
			} else {
				var f float64
				if _, err := fmt.Sscanf(s, "%f", &f); err == nil {
					reg.SetFloat(f)
				}
			}
		}
	case 't': // TEXT affinity
		switch reg.Type {
		case MemInt:
			reg.SetText(fmt.Sprintf("%d", reg.IntVal))
		case MemFloat:
			reg.SetText(fmt.Sprintf("%g", reg.FloatVal))
		case MemBlob:
			reg.Type = MemStr
		}
	case 'b': // BLOB affinity - no conversion needed
		// blob affinity is a no-op
	default:
		// 'u' or unspecified - no conversion
	}
}

// typeMaskFromMem returns a type mask bit for the given Mem value.
// Bits: 0x01=INTEGER, 0x02=FLOAT, 0x04=TEXT, 0x08=BLOB, 0x10=NULL
func typeMaskFromMem(m *Mem) int {
	switch m.Type {
	case MemInt:
		return 0x01
	case MemFloat:
		return 0x02
	case MemStr:
		return 0x04
	case MemBlob:
		return 0x08
	case MemNull:
		return 0x10
	}
	return 0x10
}

// typeMaskFromCursor reads column colIdx from the cursor's current row
// and returns the type mask. Falls back to NULL if column cannot be read.
func typeMaskFromCursor(cursor btree.BTCursor, colIdx int) int {
	data, err := cursor.Data()
	if err != nil || data == nil {
		return 0x10
	}
	values, err := ParseRecord(data)
	if err != nil || colIdx >= len(values) {
		return 0x10
	}
	v := values[colIdx]
	switch v.Type {
	case "int":
		return 0x01
	case "float":
		return 0x02
	case "text":
		return 0x04
	case "blob":
		return 0x08
	default:
		return 0x10
	}
}
