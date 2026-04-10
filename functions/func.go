// Package funcs implements SQLite built-in SQL functions.
package functions

import (
	"fmt"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// FuncDef describes a registered SQL function.
type FuncDef struct {
	Name            string // SQL function name (lowercase)
	NumArgs         int    // Number of arguments; -1 means variable
	IsAggregate     bool   // True for aggregate functions
	IsWindow        bool   // True for window functions
	IsDeterministic bool   // True if result depends only on inputs

	// Scalar implementation. Args are the function arguments; return one result.
	ScalarFunc func(ctx *Context, args []*vdbe.Mem) *vdbe.Mem

	// Aggregate implementations.
	StepFunc     func(ctx *Context, aggCtx *AggregateContext, args []*vdbe.Mem)
	FinalizeFunc func(ctx *Context, aggCtx *AggregateContext) *vdbe.Mem
}

// Context provides execution context for function implementations.
type Context struct {
	// Error can be set by function implementations to signal an error.
	Error error
}

// AggregateContext holds state across Step calls for aggregate functions.
type AggregateContext struct {
	Data interface{}
}

// FuncRegistry stores registered functions and supports lookup by name and arg count.
type FuncRegistry struct {
	funcs map[string][]*FuncDef // keyed by lowercase name, slice for overloads
}

// NewFuncRegistry creates a registry pre-loaded with all built-in SQLite functions.
func NewFuncRegistry() *FuncRegistry {
	r := &FuncRegistry{
		funcs: make(map[string][]*FuncDef),
	}
	r.registerBuiltins()
	return r
}

// Register adds a function definition to the registry.
func (r *FuncRegistry) Register(fn *FuncDef) {
	name := fn.Name
	r.funcs[name] = append(r.funcs[name], fn)
}

// Lookup finds a function by name and argument count.
// numArgs=-1 matches variable-argument functions.
// Returns nil if no matching function is found.
// Exact arg count matches are preferred over variable-arg (-1) matches.
func (r *FuncRegistry) Lookup(name string, numArgs int) *FuncDef {
	candidates := r.funcs[name]
	// First pass: exact match on argument count
	for _, fn := range candidates {
		if fn.NumArgs == numArgs {
			return fn
		}
	}
	// Second pass: variable-arg functions
	for _, fn := range candidates {
		if fn.NumArgs == -1 && numArgs >= 0 {
			return fn
		}
	}
	return nil
}

// registerBuiltins registers all built-in scalar, aggregate, date/time, and math functions.
func (r *FuncRegistry) registerBuiltins() {
	registerScalarFunctions(r)
	registerAggregateFunctions(r)
	registerDateFunctions(r)
	registerMathFunctions(r)
	registerJSONFunctions(r)
}

// --- Helper functions shared across implementations ---

// toInt converts a Mem to int64, returning (value, true) or (0, false) if NULL.
func toInt(m *vdbe.Mem) (int64, bool) {
	if m.Type == vdbe.MemNull {
		return 0, false
	}
	return m.IntValue(), true
}

// toFloat converts a Mem to float64, returning (value, true) or (0, false) if NULL.
func toFloat(m *vdbe.Mem) (float64, bool) {
	if m.Type == vdbe.MemNull {
		return 0, false
	}
	return m.FloatValue(), true
}

// toText converts a Mem to string, returning (value, true) or ("", false) if NULL.
func toText(m *vdbe.Mem) (string, bool) {
	if m.Type == vdbe.MemNull {
		return "", false
	}
	return m.TextValue(), true
}

// isNull returns true if the Mem is NULL.
func isNull(m *vdbe.Mem) bool {
	return m.Type == vdbe.MemNull
}

// memError creates an error result Mem with the given message.
func memError(msg string) *vdbe.Mem {
	return vdbe.NewMemStr(fmt.Sprintf("ERROR: %s", msg))
}

// utf8Len counts the number of UTF-8 code points in a byte slice.
func utf8Len(b []byte) int {
	count := 0
	for i := 0; i < len(b); {
		if b[i] < 0x80 {
			i++
		} else if b[i] < 0xE0 {
			i += 2
		} else if b[i] < 0xF0 {
			i += 3
		} else {
			i += 4
		}
		count++
	}
	return count
}
