package compile

import (
	"fmt"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// Program represents a compiled VDBE program ready for execution.
type Program struct {
	Instructions []vdbe.Instruction
	NumRegs      int
	NumCursors   int
	Comment      string
}

// Builder constructs VDBE programs instruction by instruction.
// It manages label resolution, register allocation, and cursor allocation.
type Builder struct {
	instrs     []vdbe.Instruction
	labels     map[int]*labelInfo
	nextLabel  int
	regCounter int
	curCounter int
}

type labelInfo struct {
	defined bool
	addr    int
	refs    []int // instruction indices whose P2 references this label
}

// NewBuilder creates a new VDBE program builder.
func NewBuilder() *Builder {
	return &Builder{
		instrs: make([]vdbe.Instruction, 0, 64),
		labels: make(map[int]*labelInfo),
	}
}

// CurrentAddr returns the index of the next instruction to be emitted.
func (b *Builder) CurrentAddr() int {
	return len(b.instrs)
}

// AllocReg allocates n consecutive registers and returns the first register number.
func (b *Builder) AllocReg(n int) int {
	r := b.regCounter
	b.regCounter += n
	return r
}

// AllocCursor allocates a cursor number and returns it.
func (b *Builder) AllocCursor() int {
	c := b.curCounter
	b.curCounter++
	return c
}

// NewLabel creates a new unresolved label and returns its ID.
func (b *Builder) NewLabel() int {
	id := b.nextLabel
	b.nextLabel++
	b.labels[id] = &labelInfo{}
	return id
}

// DefineLabel defines label id at the current instruction address.
func (b *Builder) DefineLabel(id int) {
	info, ok := b.labels[id]
	if !ok {
		info = &labelInfo{}
		b.labels[id] = info
	}
	info.defined = true
	info.addr = b.CurrentAddr()
}

// Emit adds an instruction and returns its address.
func (b *Builder) Emit(op vdbe.Opcode, p1, p2, p3 int) int {
	addr := len(b.instrs)
	b.instrs = append(b.instrs, vdbe.Instruction{
		Op: op,
		P1: p1,
		P2: p2,
		P3: p3,
	})
	return addr
}

// EmitP4 adds an instruction with a P4 value and comment, returns its address.
func (b *Builder) EmitP4(op vdbe.Opcode, p1, p2, p3 int, p4 interface{}, comment string) int {
	addr := len(b.instrs)
	b.instrs = append(b.instrs, vdbe.Instruction{
		Op:      op,
		P1:      p1,
		P2:      p2,
		P3:      p3,
		P4:      p4,
		Comment: comment,
	})
	return addr
}

// EmitComment adds an instruction with just a comment string (for P4).
func (b *Builder) EmitComment(op vdbe.Opcode, p1, p2, p3 int, comment string) int {
	return b.EmitP4(op, p1, p2, p3, comment, comment)
}

// EmitJump emits a jump instruction. p2 is treated as a label ID
// that will be resolved later by ResolveLabels.
func (b *Builder) EmitJump(op vdbe.Opcode, p1, label, p3 int) int {
	addr := b.Emit(op, p1, label, p3)
	info, ok := b.labels[label]
	if !ok {
		info = &labelInfo{}
		b.labels[label] = info
	}
	info.refs = append(info.refs, addr)
	return addr
}

// SetP2 sets the P2 field of the instruction at the given address.
func (b *Builder) SetP2(addr, p2 int) {
	if addr >= 0 && addr < len(b.instrs) {
		b.instrs[addr].P2 = p2
	}
}

// SetP3 sets the P3 field of the instruction at the given address.
func (b *Builder) SetP3(addr, p3 int) {
	if addr >= 0 && addr < len(b.instrs) {
		b.instrs[addr].P3 = p3
	}
}

// SetP5 sets the P5 field of the instruction at the given address.
func (b *Builder) SetP5(addr, p5 int) {
	if addr >= 0 && addr < len(b.instrs) {
		b.instrs[addr].P5 = p5
	}
}

// LabelAddr returns the address of a label, or -1 if not defined.
func (b *Builder) LabelAddr(labelID int) int {
	if info, ok := b.labels[labelID]; ok && info.defined {
		return info.addr
	}
	return -1
}

// SetP4 sets the P4 field of the instruction at the given address.
func (b *Builder) SetP4(addr int, p4 interface{}) {
	if addr >= 0 && addr < len(b.instrs) {
		b.instrs[addr].P4 = p4
	}
}

// ResolveLabels resolves all label references to actual instruction addresses.
// Must be called after all instructions are emitted.
func (b *Builder) ResolveLabels() error {
	for id, info := range b.labels {
		if !info.defined {
			return fmt.Errorf("label %d referenced but never defined (refs: %v)", id, info.refs)
		}
		for _, addr := range info.refs {
			b.instrs[addr].P2 = info.addr
		}
	}
	return nil
}

// BuildProgram constructs the final Program, resolving all labels.
func (b *Builder) BuildProgram() (*Program, error) {
	if err := b.ResolveLabels(); err != nil {
		return nil, err
	}
	instrs := make([]vdbe.Instruction, len(b.instrs))
	copy(instrs, b.instrs)
	return &Program{
		Instructions: instrs,
		NumRegs:      b.regCounter,
		NumCursors:   b.curCounter,
	}, nil
}

// NumRegs returns the total number of registers allocated.
func (b *Builder) NumRegs() int {
	return b.regCounter
}
