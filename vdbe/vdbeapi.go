package vdbe

// VdbeBuilder constructs a VDBE program (list of instructions).
// It mirrors SQLite's sqlite3VdbeAddOp series of APIs.
type VdbeBuilder struct {
	instrs    []Instruction
	numRegs   int
	numCursors int
	constants []Mem
	sql       string
	labels    map[string]int // named jump targets
}

// NewVdbeBuilder creates a new program builder.
func NewVdbeBuilder() *VdbeBuilder {
	return &VdbeBuilder{
		instrs: make([]Instruction, 0, 64),
		labels: make(map[string]int),
	}
}

// SetSQL records the original SQL text.
func (vb *VdbeBuilder) SetSQL(sql string) {
	vb.sql = sql
}

// SetNumRegs ensures the program has at least n registers.
func (vb *VdbeBuilder) SetNumRegs(n int) {
	if n > vb.numRegs {
		vb.numRegs = n
	}
}

// SetNumCursors ensures the program has at least n cursors.
func (vb *VdbeBuilder) SetNumCursors(n int) {
	if n > vb.numCursors {
		vb.numCursors = n
	}
}

// CurrentPC returns the current program counter (index of the next instruction).
func (vb *VdbeBuilder) CurrentPC() int {
	return len(vb.instrs)
}

// AddOp adds a basic instruction with P1, P2, P3.
func (vb *VdbeBuilder) AddOp(op Opcode, p1, p2, p3 int) *VdbeBuilder {
	vb.instrs = append(vb.instrs, Instruction{
		Op: op,
		P1: p1,
		P2: p2,
		P3: p3,
	})
	return vb
}

// AddOpJump adds a jump instruction and returns the PC of the instruction
// so that jump targets can be resolved later.
func (vb *VdbeBuilder) AddOpJump(op Opcode, p1, p2 int) int {
	pc := len(vb.instrs)
	vb.instrs = append(vb.instrs, Instruction{
		Op: op,
		P1: p1,
		P2: p2,
	})
	return pc
}

// AddOpInt adds an instruction that loads an integer constant into a register.
// This is a convenience wrapper around AddOp for OpInteger.
func (vb *VdbeBuilder) AddOpInt(destReg int, value int64) *VdbeBuilder {
	vb.instrs = append(vb.instrs, Instruction{
		Op: OpInteger,
		P1: int(value),
		P2: destReg,
	})
	return vb
}

// AddOp4 adds an instruction with a P4 value.
func (vb *VdbeBuilder) AddOp4(op Opcode, p1, p2, p3 int, p4 interface{}, comment string) *VdbeBuilder {
	vb.instrs = append(vb.instrs, Instruction{
		Op:      op,
		P1:      p1,
		P2:      p2,
		P3:      p3,
		P4:      p4,
		Comment: comment,
	})
	return vb
}

// AddOpList adds multiple instructions at once.
func (vb *VdbeBuilder) AddOpList(instrs []Instruction) *VdbeBuilder {
	vb.instrs = append(vb.instrs, instrs...)
	return vb
}

// ResolveJump fixes up the P2 jump target of a previously added instruction.
func (vb *VdbeBuilder) ResolveJump(instrPC int, targetPC int) {
	if instrPC >= 0 && instrPC < len(vb.instrs) {
		vb.instrs[instrPC].P2 = targetPC
	}
}

// SetLabel records a named label at the current PC.
func (vb *VdbeBuilder) SetLabel(name string) {
	vb.labels[name] = len(vb.instrs)
}

// ResolveLabel sets the P2 of the instruction at instrPC to the named label.
func (vb *VdbeBuilder) ResolveLabel(instrPC int, label string) {
	if target, ok := vb.labels[label]; ok {
		vb.ResolveJump(instrPC, target)
	}
}

// AddComment adds a comment to the most recently added instruction.
func (vb *VdbeBuilder) AddComment(comment string) *VdbeBuilder {
	if len(vb.instrs) > 0 {
		vb.instrs[len(vb.instrs)-1].Comment = comment
	}
	return vb
}

// Finalize builds and returns the completed Program.
// After calling Finalize, the builder should not be reused.
func (vb *VdbeBuilder) Finalize() *Program {
	return &Program{
		Instructions: vb.instrs,
		NumRegs:     vb.numRegs,
		NumCursors:  vb.numCursors,
		Constants:   vb.constants,
		SQL:         vb.sql,
	}
}

// NewVDBEFromBuilder is a convenience: build the program and create a VDBE.
func NewVDBEFromBuilder(db Database, vb *VdbeBuilder) *VDBE {
	v := NewVDBE(db)
	v.SetProgram(vb.Finalize())
	return v
}
