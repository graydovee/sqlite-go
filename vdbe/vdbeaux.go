package vdbe

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sqlite-go/sqlite-go/btree"
)

// OpcodeName returns the human-readable name for an opcode.
func OpcodeName(op Opcode) string {
	switch op {
	case OpAbortable:
		return "Abortable"
	case OpAdd:
		return "Add"
	case OpAddImm:
		return "AddImm"
	case OpAggFinal:
		return "AggFinal"
	case OpAggInverse:
		return "AggInverse"
	case OpAggStep:
		return "AggStep"
	case OpAggValue:
		return "AggValue"
	case OpAnd:
		return "And"
	case OpAutoCommit:
		return "AutoCommit"
	case OpBeginSubrtn:
		return "BeginSubrtn"
	case OpBitAnd:
		return "BitAnd"
	case OpBitNot:
		return "BitNot"
	case OpBitOr:
		return "BitOr"
	case OpBlob:
		return "Blob"
	case OpCast:
		return "Cast"
	case OpCheckpoint:
		return "Checkpoint"
	case OpClear:
		return "Clear"
	case OpClose:
		return "Close"
	case OpCollSeq:
		return "CollSeq"
	case OpColumn:
		return "Column"
	case OpCompare:
		return "Compare"
	case OpConcat:
		return "Concat"
	case OpCopy:
		return "Copy"
	case OpCreateBTree:
		return "CreateBTree"
	case OpCrossjoin:
		return "Crossjoin"
	case OpDecrJumpZero:
		return "DecrJumpZero"
	case OpDelete:
		return "Delete"
	case OpDestroy:
		return "Destroy"
	case OpDivide:
		return "Divide"
	case OpDropIndex:
		return "DropIndex"
	case OpDropTable:
		return "DropTable"
	case OpDropTrigger:
		return "DropTrigger"
	case OpElseEq:
		return "ElseEq"
	case OpEndCoroutine:
		return "EndCoroutine"
	case OpEq:
		return "Eq"
	case OpExpire:
		return "Expire"
	case OpExplain:
		return "Explain"
	case OpFilter:
		return "Filter"
	case OpFirst:
		return "First"
	case OpFkCheck:
		return "FkCheck"
	case OpFkCounter:
		return "FkCounter"
	case OpFkIfZero:
		return "FkIfZero"
	case OpFound:
		return "Found"
	case OpFunction:
		return "Function"
	case OpGe:
		return "Ge"
	case OpGoSub:
		return "Gosub"
	case OpGoto:
		return "Goto"
	case OpGt:
		return "Gt"
	case OpHalt:
		return "Halt"
	case OpHaltIfNull:
		return "HaltIfNull"
	case OpIdxDelete:
		return "IdxDelete"
	case OpIdxGE:
		return "IdxGE"
	case OpIdxGT:
		return "IdxGT"
	case OpIdxInsert:
		return "IdxInsert"
	case OpIdxLE:
		return "IdxLE"
	case OpIdxLT:
		return "IdxLT"
	case OpIdxRowid:
		return "IdxRowid"
	case OpIf:
		return "If"
	case OpIfNot:
		return "IfNot"
	case OpIfNotOpen:
		return "IfNotOpen"
	case OpIfNullRow:
		return "IfNullRow"
	case OpIfPos:
		return "IfPos"
	case OpIncrVacuum:
		return "IncrVacuum"
	case OpInit:
		return "Init"
	case OpInitCoroutine:
		return "InitCoroutine"
	case OpInteger:
		return "Integer"
		return "InitCoroutine"
	case OpInsert:
		return "Insert"
	case OpInt64:
		return "Int64"
	case OpIntCmp:
		return "IntCmp"
	case OpInterrupt:
		return "Interrupt"
	case OpIsNull:
		return "IsNull"
	case OpJournalMode:
		return "JournalMode"
	case OpJump:
		return "Jump"
	case OpLast:
		return "Last"
	case OpLe:
		return "Le"
	case OpLoadAnalysis:
		return "LoadAnalysis"
	case OpLt:
		return "Lt"
	case OpMakeRecord:
		return "MakeRecord"
	case OpMaxPgcnt:
		return "MaxPgcnt"
	case OpMove:
		return "Move"
	case OpMul:
		return "Mul"
	case OpMustBeInt:
		return "MustBeInt"
	case OpNe:
		return "Ne"
	case OpNewRowid:
		return "NewRowid"
	case OpNext:
		return "Next"
	case OpNoConflict:
		return "NoConflict"
	case OpNoop:
		return "Noop"
	case OpNot:
		return "Not"
	case OpNotNull:
		return "NotNull"
	case OpNotExists:
		return "NotExists"
	case OpNotFound:
		return "NotFound"
	case OpNull:
		return "Null"
	case OpNullRow:
		return "NullRow"
	case OpOffset:
		return "Offset"
	case OpOnce:
		return "Once"
	case OpOpenAutoindex:
		return "OpenAutoindex"
	case OpOpenDup:
		return "OpenDup"
	case OpOpenEphemeral:
		return "OpenEphemeral"
	case OpOpenPseudo:
		return "OpenPseudo"
	case OpOpenRead:
		return "OpenRead"
	case OpOpenWrite:
		return "OpenWrite"
	case OpOr:
		return "Or"
	case OpPagecount:
		return "Pagecount"
	case OpParam:
		return "Param"
	case OpParseSchema:
		return "ParseSchema"
	case OpPermutation:
		return "Permutation"
	case OpPrev:
		return "Prev"
	case OpProgram:
		return "Program"
	case OpReadCookie:
		return "ReadCookie"
	case OpReal:
		return "Real"
	case OpRelease:
		return "Release"
	case OpRemainder:
		return "Remainder"
	case OpResetCount:
		return "ResetCount"
	case OpResetSorter:
		return "ResetSorter"
	case OpResultRow:
		return "ResultRow"
	case OpReturn:
		return "Return"
	case OpRewind:
		return "Rewind"
	case OpRowCell:
		return "RowCell"
	case OpRowData:
		return "RowData"
	case OpRowid:
		return "Rowid"
	case OpRowSetAdd:
		return "RowSetAdd"
	case OpRowSetRead:
		return "RowSetRead"
	case OpRowSetTest:
		return "RowSetTest"
	case OpSavepoint:
		return "Savepoint"
	case OpSCopy:
		return "SCopy"
	case OpSeek:
		return "Seek"
	case OpSeekGE:
		return "SeekGE"
	case OpSeekGT:
		return "SeekGT"
	case OpSeekHit:
		return "SeekHit"
	case OpSeekLE:
		return "SeekLE"
	case OpSeekLT:
		return "SeekLT"
	case OpSeekScan:
		return "SeekScan"
	case OpSequence:
		return "Sequence"
	case OpSequenceTest:
		return "SequenceTest"
	case OpSetCookie:
		return "SetCookie"
	case OpShiftLeft:
		return "ShiftLeft"
	case OpShiftRight:
		return "ShiftRight"
	case OpSort:
		return "Sort"
	case OpSorterCompare:
		return "SorterCompare"
	case OpSorterData:
		return "SorterData"
	case OpSorterInsert:
		return "SorterInsert"
	case OpSorterNext:
		return "SorterNext"
	case OpSorterOpen:
		return "SorterOpen"
	case OpSorterSort:
		return "SorterSort"
	case OpString:
		return "String"
	case OpString8:
		return "String8"
	case OpSubtract:
		return "Subtract"
	case OpTableLock:
		return "TableLock"
	case OpTableUnlock:
		return "TableUnlock"
	case OpTrace:
		return "Trace"
	case OpTransaction:
		return "Transaction"
	case OpTrim:
		return "Trim"
	case OpTypeCheck:
		return "TypeCheck"
	case OpUnique:
		return "Unique"
	case OpUpdate:
		return "Update"
	case OpVacuum:
		return "Vacuum"
	case OpVariable:
		return "Variable"
	case OpVBegin:
		return "VBegin"
	case OpVCheck:
		return "VCheck"
	case OpVColumn:
		return "VColumn"
	case OpVCreate:
		return "VCreate"
	case OpVDestroy:
		return "VDestroy"
	case OpVFilter:
		return "VFilter"
	case OpVIn:
		return "VIn"
	case OpVNext:
		return "VNext"
	case OpVOpen:
		return "VOpen"
	case OpVRename:
		return "VRename"
	case OpVUpdate:
		return "VUpdate"
	case OpYield:
		return "Yield"
	default:
		return fmt.Sprintf("Unknown(%d)", op)
	}
}

// FormatInstruction formats a single instruction for display.
func FormatInstruction(addr int, instr Instruction) string {
	p4str := ""
	switch v := instr.P4.(type) {
	case string:
		p4str = fmt.Sprintf(" P4=%q", v)
	case int64:
		p4str = fmt.Sprintf(" P4=%d", v)
	case float64:
		p4str = fmt.Sprintf(" P4=%g", v)
	case []byte:
		p4str = fmt.Sprintf(" P4=blob(%d)", len(v))
	case nil:
		// no P4
	}

	comment := ""
	if instr.Comment != "" {
		comment = fmt.Sprintf("  ; %s", instr.Comment)
	}

	return fmt.Sprintf("%4d  %-12s P1=%-3d P2=%-3d P3=%-3d%s%s",
		addr, OpcodeName(instr.Op), instr.P1, instr.P2, instr.P3, p4str, comment)
}

// VdbeExplain prints EXPLAIN output for a program to the writer.
// This mirrors SQLite's EXPLAIN functionality.
func VdbeExplain(w io.Writer, prog *Program) {
	fmt.Fprintf(w, "addr  opcode       P1   P2   P3\n")
	fmt.Fprintf(w, "----  ----------- ---- ---- ----\n")
	for i, instr := range prog.Instructions {
		fmt.Fprintln(w, FormatInstruction(i, instr))
	}
}

// VdbeExplainStdout prints EXPLAIN output to stdout.
func VdbeExplainStdout(prog *Program) {
	VdbeExplain(os.Stdout, prog)
}

// VdbeList returns a human-readable listing of the program as a string.
func VdbeList(prog *Program) string {
	var sb strings.Builder
	for i, instr := range prog.Instructions {
		sb.WriteString(FormatInstruction(i, instr))
		sb.WriteByte('\n')
	}
	return sb.String()
}

// CleanupCursors closes all open cursors.
func (v *VDBE) CleanupCursors() {
	for i, vc := range v.cursors {
		if vc != nil {
			if cursor, ok := vc.Cursor.(btree.BTCursor); ok {
				cursor.Close()
			}
			v.cursors[i] = nil
		}
	}
}

// Close releases all resources held by the VDBE.
func (v *VDBE) Close() {
	v.CleanupCursors()
	v.regs = nil
	v.cursors = nil
	v.prog = nil
}

// GetRegister returns a copy of the register value at the given index.
func (v *VDBE) GetRegister(idx int) *Mem {
	if idx < 0 || idx >= len(v.regs) {
		return NewMemNull()
	}
	return v.regs[idx].Copy()
}

// SetRegister sets the register at the given index.
func (v *VDBE) SetRegister(idx int, m *Mem) {
	if idx >= 0 && idx < len(v.regs) {
		v.regs[idx] = *m.Copy()
	}
}

// PC returns the current program counter.
func (v *VDBE) PC() int {
	return v.pc
}

// ResultCode returns the current result code.
func (v *VDBE) Result() ResultCode {
	return v.rc
}

// Error returns the last error.
func (v *VDBE) Error() error {
	return v.err
}

// Program returns the loaded program.
func (v *VDBE) Program() *Program {
	return v.prog
}
