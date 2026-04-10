package functions

import (
	"github.com/sqlite-go/sqlite-go/vdbe"
)

func registerMathFunctions(r *FuncRegistry) {
	r.Register(&FuncDef{Name: "round", NumArgs: 1, IsDeterministic: true, ScalarFunc: roundFunc1})
	r.Register(&FuncDef{Name: "round", NumArgs: 2, IsDeterministic: true, ScalarFunc: roundFunc2})
}

func roundFunc1(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return roundImpl(args[0], 0)
}

func roundFunc2(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if args[1].Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	n := args[1].IntValue()
	return roundImpl(args[0], n)
}

func roundImpl(m *vdbe.Mem, n int64) *vdbe.Mem {
	if m.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	if n < 0 {
		n = 0
	}
	if n > 30 {
		n = 30
	}

	r := m.FloatValue()

	// For very large values, no rounding needed (no fractional part)
	if r < -4503599627370496.0 || r > 4503599627370496.0 {
		return vdbe.NewMemFloat(r)
	}

	if n == 0 {
		// Round to integer
		if r < 0 {
			r = float64(int64(r - 0.5))
		} else {
			r = float64(int64(r + 0.5))
		}
	} else {
		// Round to n decimal places using multiply-round-divide
		scale := 1.0
		for i := int64(0); i < n; i++ {
			scale *= 10.0
		}
		r = r * scale
		if r < 0 {
			r = float64(int64(r - 0.5))
		} else {
			r = float64(int64(r + 0.5))
		}
		r = r / scale
	}

	return vdbe.NewMemFloat(r)
}
