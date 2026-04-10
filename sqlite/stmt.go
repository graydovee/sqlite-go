package sqlite

import (
	"fmt"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// Statement represents a prepared SQL statement.
type Statement struct {
	db       *Database
	prog     *vdbe.Program
	sql      string
	colNames []string
	finalized bool

	// Bind parameter values (1-indexed)
	bindVars []bindVar
}

// bindVar holds a bound parameter value.
type bindVar struct {
	set   bool
	mem   vdbe.Mem
}

// newStatement creates a new prepared statement.
func newStatement(db *Database, prog *vdbe.Program, sql string, colNames []string) *Statement {
	return &Statement{
		db:       db,
		prog:     prog,
		sql:      sql,
		colNames: colNames,
	}
}

// BindInt binds an integer value to the given parameter index (1-based).
func (s *Statement) BindInt(idx int, val int64) error {
	if s.finalized {
		return NewError(Misuse, "statement is finalized")
	}
	if idx < 1 {
		return NewErrorf(Range, "bind index out of range: %d", idx)
	}
	s.growBindVars(idx)
	s.bindVars[idx].set = true
	s.bindVars[idx].mem.SetInt(val)
	return nil
}

// BindFloat binds a float value to the given parameter index (1-based).
func (s *Statement) BindFloat(idx int, val float64) error {
	if s.finalized {
		return NewError(Misuse, "statement is finalized")
	}
	if idx < 1 {
		return NewErrorf(Range, "bind index out of range: %d", idx)
	}
	s.growBindVars(idx)
	s.bindVars[idx].set = true
	s.bindVars[idx].mem.SetFloat(val)
	return nil
}

// BindText binds a text value to the given parameter index (1-based).
func (s *Statement) BindText(idx int, val string) error {
	if s.finalized {
		return NewError(Misuse, "statement is finalized")
	}
	if idx < 1 {
		return NewErrorf(Range, "bind index out of range: %d", idx)
	}
	s.growBindVars(idx)
	s.bindVars[idx].set = true
	s.bindVars[idx].mem.SetText(val)
	return nil
}

// BindBlob binds a blob value to the given parameter index (1-based).
func (s *Statement) BindBlob(idx int, val []byte) error {
	if s.finalized {
		return NewError(Misuse, "statement is finalized")
	}
	if idx < 1 {
		return NewErrorf(Range, "bind index out of range: %d", idx)
	}
	s.growBindVars(idx)
	s.bindVars[idx].set = true
	s.bindVars[idx].mem.SetBlob(val)
	return nil
}

// BindNull binds a NULL value to the given parameter index (1-based).
func (s *Statement) BindNull(idx int) error {
	if s.finalized {
		return NewError(Misuse, "statement is finalized")
	}
	if idx < 1 {
		return NewErrorf(Range, "bind index out of range: %d", idx)
	}
	s.growBindVars(idx)
	s.bindVars[idx].set = true
	s.bindVars[idx].mem.SetNull()
	return nil
}

// Bind binds a Go value to the given parameter index (1-based).
// Supports int, int64, float64, string, []byte, bool, and nil.
func (s *Statement) Bind(idx int, val interface{}) error {
	if s.finalized {
		return NewError(Misuse, "statement is finalized")
	}
	if idx < 1 {
		return NewErrorf(Range, "bind index out of range: %d", idx)
	}
	s.growBindVars(idx)
	s.bindVars[idx].set = true
	m := vdbe.MakeMem(val)
	s.bindVars[idx].mem = *m
	return nil
}

// Step advances the statement to the next row. Returns true if a row
// is available, false if execution is complete.
func (s *Statement) Step() (bool, error) {
	if s.finalized {
		return false, NewError(Misuse, "statement is finalized")
	}
	return false, fmt.Errorf("statement stepping requires VDBE execution; use Exec/Query on Database")
}

// Reset resets the prepared statement so it can be re-executed.
func (s *Statement) Reset() error {
	if s.finalized {
		return NewError(Misuse, "statement is finalized")
	}
	for i := range s.bindVars {
		s.bindVars[i].set = false
		s.bindVars[i].mem = vdbe.Mem{Type: vdbe.MemNull, IsNull: true}
	}
	return nil
}

// Finalize releases all resources associated with the prepared statement.
func (s *Statement) Finalize() error {
	if s.finalized {
		return nil
	}
	s.finalized = true
	s.bindVars = nil
	s.prog = nil
	return nil
}

// ColumnInt returns the integer value of the given column (0-based) from
// the current row. Call only after Step() returns true.
func (s *Statement) ColumnInt(idx int) int64 {
	return 0
}

// ColumnFloat returns the float value of the given column (0-based).
func (s *Statement) ColumnFloat(idx int) float64 {
	return 0
}

// ColumnText returns the text value of the given column (0-based).
func (s *Statement) ColumnText(idx int) string {
	return ""
}

// ColumnBlob returns the blob value of the given column (0-based).
func (s *Statement) ColumnBlob(idx int) []byte {
	return nil
}

// ColumnType returns the type of the given column (0-based).
func (s *Statement) ColumnType(idx int) ColumnType {
	return ColNull
}

// ColumnName returns the declared name of the given column (0-based).
func (s *Statement) ColumnName(idx int) string {
	if idx < 0 || idx >= len(s.colNames) {
		return ""
	}
	return s.colNames[idx]
}

// ColumnCount returns the number of columns in the result set.
func (s *Statement) ColumnCount() int {
	return len(s.colNames)
}

// SQL returns the original SQL text of the statement.
func (s *Statement) SQL() string {
	return s.sql
}

// IsFinalized returns whether the statement has been finalized.
func (s *Statement) IsFinalized() bool {
	return s.finalized
}

// growBindVars ensures bindVars is large enough for index idx (1-based).
func (s *Statement) growBindVars(idx int) {
	needed := idx + 1 // 1-indexed, so allocate idx+1
	if len(s.bindVars) < needed {
		newVars := make([]bindVar, needed)
		copy(newVars, s.bindVars)
		s.bindVars = newVars
	}
}

// applyBindings sets OpVariable register values on a VDBE before execution.
func (s *Statement) applyBindings(v *vdbe.VDBE) {
	for i, bv := range s.bindVars {
		if bv.set && i > 0 {
			// Find OpVariable instructions that reference this parameter index
			// and set the corresponding register
			for _, instr := range s.prog.Instructions {
				if instr.Op == vdbe.OpVariable && instr.P1 == i {
					regs := v.Registers()
					if instr.P2 < len(regs) {
						regs[instr.P2] = bv.mem
					}
				}
			}
		}
	}
}
