package compile

import (
	"fmt"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// emitOptimizedScan emits VDBE code for an index-based table scan.
// Returns (loopBodyLabel, error).
// The caller emits the loop body after this, then calls emitOptimizedScanEnd.
func (b *Build) emitOptimizedScan(
	plan *indexScanPlan,
	emptyLabel int,
	loopEndLabel int,
) (int, error) {
	switch plan.ScanType {
	case scanTableFull:
		return b.emitFullScan(plan, emptyLabel)
	case scanIndexEq:
		return b.emitIndexEqScan(plan, emptyLabel, loopEndLabel)
	case scanIndexRange:
		return b.emitIndexRangeScan(plan, emptyLabel, loopEndLabel)
	default:
		return b.emitFullScan(plan, emptyLabel)
	}
}

// emitFullScan emits a standard Rewind/Next table scan.
func (b *Build) emitFullScan(plan *indexScanPlan, emptyLabel int) (int, error) {
	cursor := plan.TblCursor
	loopBody := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpRewind, cursor, emptyLabel, 0)
	b.b.DefineLabel(loopBody)
	return loopBody, nil
}

// emitIndexEqScan emits code for an index point query.
func (b *Build) emitIndexEqScan(plan *indexScanPlan, emptyLabel, loopEndLabel int) (int, error) {
	if plan.Index == nil {
		// PK/rowid lookup
		return b.emitRowidSeek(plan, emptyLabel)
	}

	// Open index cursor
	idxCursor := b.b.AllocCursor()
	plan.IdxCursor = idxCursor
	b.emitOpenRead(idxCursor, plan.Index.RootPage)

	// Evaluate equality values
	valBase := b.b.AllocReg(len(plan.EqExprs))
	for i, eqExpr := range plan.EqExprs {
		if err := b.compileExpr(eqExpr, valBase+i); err != nil {
			return 0, fmt.Errorf("index eq constraint: %w", err)
		}
	}

	// Make record for the seek key
	recReg := b.b.AllocReg(1)
	b.emitMakeRecord(valBase, len(plan.EqExprs), recReg)

	// SeekGE positions at first entry >= key
	// If no match, jump to emptyLabel
	b.b.EmitJump(vdbe.OpSeekGE, idxCursor, emptyLabel, valBase)

	// Verify the entry is actually == (not just >=)
	// IdxGT jumps if current entry > key; if it jumps, no match
	b.b.EmitJump(vdbe.OpIdxGT, idxCursor, emptyLabel, valBase)

	// Match found - get the rowid and position the table cursor
	rowidReg := b.b.AllocReg(1)
	b.b.Emit(vdbe.OpIdxRowid, idxCursor, rowidReg, 0)
	b.b.EmitJump(vdbe.OpSeek, plan.TblCursor, emptyLabel, rowidReg)

	// Define the loop body (for a point query, runs exactly once)
	loopBody := b.b.NewLabel()
	b.b.DefineLabel(loopBody)
	return loopBody, nil
}

// emitRowidSeek emits a PK/rowid equality seek.
func (b *Build) emitRowidSeek(plan *indexScanPlan, emptyLabel int) (int, error) {
	// Evaluate the PK value
	pkReg := b.b.AllocReg(1)
	if len(plan.EqExprs) > 0 {
		if err := b.compileExpr(plan.EqExprs[0], pkReg); err != nil {
			return 0, fmt.Errorf("PK constraint: %w", err)
		}
	}

	// Seek on the table cursor using rowid
	loopBody := b.b.NewLabel()
	b.b.EmitJump(vdbe.OpSeek, plan.TblCursor, emptyLabel, pkReg)
	b.b.DefineLabel(loopBody)
	return loopBody, nil
}

// emitIndexRangeScan emits code for an index range scan.
func (b *Build) emitIndexRangeScan(plan *indexScanPlan, emptyLabel, loopEndLabel int) (int, error) {
	// Open index cursor if using an index
	if plan.Index != nil {
		idxCursor := b.b.AllocCursor()
		plan.IdxCursor = idxCursor
		b.emitOpenRead(idxCursor, plan.Index.RootPage)
	}

	cursor := plan.TblCursor
	if plan.IdxCursor >= 0 {
		cursor = plan.IdxCursor
	}

	// Evaluate equality constraint values
	valBase := b.b.AllocReg(len(plan.EqExprs))
	for i, eqExpr := range plan.EqExprs {
		if err := b.compileExpr(eqExpr, valBase+i); err != nil {
			return 0, fmt.Errorf("index eq constraint: %w", err)
		}
	}

	// Evaluate range bounds
	var lowerReg, upperReg int
	if plan.RangeLower != nil {
		lowerReg = b.b.AllocReg(1)
		if err := b.compileExpr(plan.RangeLower, lowerReg); err != nil {
			return 0, fmt.Errorf("index lower bound: %w", err)
		}
	}
	if plan.RangeUpper != nil {
		upperReg = b.b.AllocReg(1)
		if err := b.compileExpr(plan.RangeUpper, upperReg); err != nil {
			return 0, fmt.Errorf("index upper bound: %w", err)
		}
	}

	loopBody := b.b.NewLabel()

	// Position the cursor at the start of the range
	if plan.RangeLower != nil {
		seekOp := vdbe.OpSeekGE
		if !plan.LowerInclusive {
			seekOp = vdbe.OpSeekGT
		}
		b.b.EmitJump(seekOp, cursor, emptyLabel, lowerReg)
	} else if len(plan.EqExprs) > 0 {
		b.b.EmitJump(vdbe.OpSeekGE, cursor, emptyLabel, valBase)
	} else {
		b.b.EmitJump(vdbe.OpRewind, cursor, emptyLabel, 0)
	}

	b.b.DefineLabel(loopBody)

	// Check upper bound
	if plan.RangeUpper != nil && plan.IdxCursor >= 0 {
		if plan.UpperInclusive {
			// Exit if entry > upper bound
			b.b.EmitJump(vdbe.OpIdxGT, plan.IdxCursor, loopEndLabel, upperReg)
		} else {
			// Exit if entry >= upper bound
			b.b.EmitJump(vdbe.OpIdxGE, plan.IdxCursor, loopEndLabel, upperReg)
		}
	}

	// If using an index, fetch the rowid and seek the table
	if plan.IdxCursor >= 0 && plan.Index != nil {
		rowidReg := b.b.AllocReg(1)
		b.b.Emit(vdbe.OpIdxRowid, plan.IdxCursor, rowidReg, 0)
		b.b.EmitJump(vdbe.OpSeek, plan.TblCursor, loopEndLabel, rowidReg)
	}

	return loopBody, nil
}

// emitOptimizedScanEnd emits the loop-back Next instruction for an optimized scan.
func (b *Build) emitOptimizedScanEnd(plan *indexScanPlan, loopBodyLabel int) {
	switch plan.ScanType {
	case scanTableFull:
		b.emitNext(plan.TblCursor, loopBodyLabel)

	case scanIndexEq:
		// Point query: no Next needed
		if plan.IdxCursor >= 0 {
			b.emitClose(plan.IdxCursor)
		}

	case scanIndexRange:
		if plan.IdxCursor >= 0 {
			b.b.EmitJump(vdbe.OpNext, plan.IdxCursor, loopBodyLabel, 0)
			b.emitClose(plan.IdxCursor)
		} else {
			b.emitNext(plan.TblCursor, loopBodyLabel)
		}
	}
}

// emitRemainingWhere evaluates WHERE terms not consumed by index plans.
func (b *Build) emitRemainingWhere(remaining []*whereTerm, skipLabel, loopEndLabel int) error {
	for _, t := range remaining {
		if t.Expr == nil {
			continue
		}
		if err := b.compileCondition(t.Expr, skipLabel, loopEndLabel, true); err != nil {
			return err
		}
	}
	return nil
}

// buildRemainingExpr reconstructs an expression from remaining terms.
// Returns nil if no remaining terms.
func buildRemainingExpr(terms []*whereTerm) *Expr {
	var result *Expr
	for _, t := range terms {
		if t.Expr == nil {
			continue
		}
		if result == nil {
			result = t.Expr
		} else {
			result = &Expr{
				Kind:  ExprBinaryOp,
				Op:    "AND",
				Left:  result,
				Right: t.Expr,
			}
		}
	}
	return result
}
