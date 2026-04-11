package vdbe

// FuncInfo describes a SQL function for the VDBE Function opcode.
type FuncInfo struct {
	Name        string
	ArgCount    int  // -1 for variable args
	Distinct    bool
	IsAggregate bool
}
