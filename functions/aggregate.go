package functions

import (
	"strings"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

func registerAggregateFunctions(r *FuncRegistry) {
	r.Register(&FuncDef{
		Name: "count", NumArgs: 0, IsAggregate: true,
		StepFunc:    countStepStar,
		FinalizeFunc: countFinalize,
	})
	r.Register(&FuncDef{
		Name: "count", NumArgs: 1, IsAggregate: true,
		StepFunc:    countStep,
		FinalizeFunc: countFinalize,
	})
	r.Register(&FuncDef{
		Name: "sum", NumArgs: 1, IsAggregate: true,
		StepFunc:    sumStep,
		FinalizeFunc: sumFinalize,
	})
	r.Register(&FuncDef{
		Name: "total", NumArgs: 1, IsAggregate: true,
		StepFunc:    totalStep,
		FinalizeFunc: totalFinalize,
	})
	r.Register(&FuncDef{
		Name: "avg", NumArgs: 1, IsAggregate: true,
		StepFunc:    avgStep,
		FinalizeFunc: avgFinalize,
	})
	r.Register(&FuncDef{
		Name: "min", NumArgs: 1, IsAggregate: true,
		StepFunc:    minAggStep,
		FinalizeFunc: minMaxAggFinalize,
	})
	r.Register(&FuncDef{
		Name: "max", NumArgs: 1, IsAggregate: true,
		StepFunc:    maxAggStep,
		FinalizeFunc: minMaxAggFinalize,
	})
	r.Register(&FuncDef{
		Name: "group_concat", NumArgs: 1, IsAggregate: true,
		StepFunc:    groupConcatStep1,
		FinalizeFunc: groupConcatFinalize,
	})
	r.Register(&FuncDef{
		Name: "group_concat", NumArgs: 2, IsAggregate: true,
		StepFunc:    groupConcatStep2,
		FinalizeFunc: groupConcatFinalize,
	})
}

// --- count ---

type countAgg struct {
	count int64
}

func countStepStar(_ *Context, aggCtx *AggregateContext, args []*vdbe.Mem) {
	st, ok := aggCtx.Data.(*countAgg)
	if !ok {
		st = &countAgg{}
		aggCtx.Data = st
	}
	st.count++
}

func countStep(_ *Context, aggCtx *AggregateContext, args []*vdbe.Mem) {
	st, ok := aggCtx.Data.(*countAgg)
	if !ok {
		st = &countAgg{}
		aggCtx.Data = st
	}
	if args[0].Type != vdbe.MemNull {
		st.count++
	}
}

func countFinalize(_ *Context, aggCtx *AggregateContext) *vdbe.Mem {
	st, ok := aggCtx.Data.(*countAgg)
	if !ok {
		return vdbe.NewMemInt(0)
	}
	return vdbe.NewMemInt(st.count)
}

// --- sum ---

type sumAgg struct {
	sum      float64
	useFloat bool
	intSum   int64
	hasValue bool
}

func sumStep(_ *Context, aggCtx *AggregateContext, args []*vdbe.Mem) {
	st, ok := aggCtx.Data.(*sumAgg)
	if !ok {
		st = &sumAgg{}
		aggCtx.Data = st
	}
	if args[0].Type == vdbe.MemNull {
		return
	}
	st.hasValue = true

	if st.useFloat {
		// Already in float mode
		st.sum += args[0].FloatValue()
		return
	}

	if args[0].Type == vdbe.MemInt {
		// Try to accumulate as int
		newSum := st.intSum + args[0].IntVal
		// Check for overflow: if signs differ between the result and both inputs having same sign
		if (newSum > 0) != (st.intSum > 0 || args[0].IntVal > 0) &&
			st.intSum != 0 && args[0].IntVal != 0 {
			// Overflow detected, switch to float
			st.sum = float64(st.intSum) + float64(args[0].IntVal)
			st.useFloat = true
		} else {
			st.intSum = newSum
		}
	} else {
		// Non-int input, switch to float
		st.sum = float64(st.intSum) + args[0].FloatValue()
		st.useFloat = true
	}
}

func sumFinalize(_ *Context, aggCtx *AggregateContext) *vdbe.Mem {
	st, ok := aggCtx.Data.(*sumAgg)
	if !ok || !st.hasValue {
		return vdbe.NewMemNull()
	}
	if st.useFloat {
		return vdbe.NewMemFloat(st.sum)
	}
	return vdbe.NewMemInt(st.intSum)
}

// --- total ---

type totalAgg struct {
	sum    float64
	hasVal bool
}

func totalStep(_ *Context, aggCtx *AggregateContext, args []*vdbe.Mem) {
	st, ok := aggCtx.Data.(*totalAgg)
	if !ok {
		st = &totalAgg{}
		aggCtx.Data = st
	}
	if args[0].Type == vdbe.MemNull {
		return
	}
	st.hasVal = true
	st.sum += args[0].FloatValue()
}

func totalFinalize(_ *Context, aggCtx *AggregateContext) *vdbe.Mem {
	st, ok := aggCtx.Data.(*totalAgg)
	if !ok {
		return vdbe.NewMemFloat(0.0)
	}
	return vdbe.NewMemFloat(st.sum)
}

// --- avg ---

type avgAgg struct {
	sum   float64
	count int64
}

func avgStep(_ *Context, aggCtx *AggregateContext, args []*vdbe.Mem) {
	st, ok := aggCtx.Data.(*avgAgg)
	if !ok {
		st = &avgAgg{}
		aggCtx.Data = st
	}
	if args[0].Type == vdbe.MemNull {
		return
	}
	st.sum += args[0].FloatValue()
	st.count++
}

func avgFinalize(_ *Context, aggCtx *AggregateContext) *vdbe.Mem {
	st, ok := aggCtx.Data.(*avgAgg)
	if !ok || st.count == 0 {
		return vdbe.NewMemNull()
	}
	return vdbe.NewMemFloat(st.sum / float64(st.count))
}

// --- min/max aggregate ---

type minMaxAgg struct {
	value *vdbe.Mem
}

func minAggStep(_ *Context, aggCtx *AggregateContext, args []*vdbe.Mem) {
	minMaxStep(aggCtx, args, false)
}

func maxAggStep(_ *Context, aggCtx *AggregateContext, args []*vdbe.Mem) {
	minMaxStep(aggCtx, args, true)
}

func minMaxStep(aggCtx *AggregateContext, args []*vdbe.Mem, isMax bool) {
	if args[0].Type == vdbe.MemNull {
		return
	}
	st, ok := aggCtx.Data.(*minMaxAgg)
	if !ok {
		st = &minMaxAgg{value: args[0].Copy()}
		aggCtx.Data = st
		return
	}
	if st.value == nil || st.value.Type == vdbe.MemNull {
		st.value = args[0].Copy()
		return
	}
	cmp := vdbe.MemCompare(st.value, args[0])
	if isMax && cmp < 0 {
		st.value = args[0].Copy()
	} else if !isMax && cmp > 0 {
		st.value = args[0].Copy()
	}
}

func minMaxAggFinalize(_ *Context, aggCtx *AggregateContext) *vdbe.Mem {
	st, ok := aggCtx.Data.(*minMaxAgg)
	if !ok || st.value == nil {
		return vdbe.NewMemNull()
	}
	return st.value.Copy()
}

// --- group_concat ---

type groupConcatAgg struct {
	parts     []string
	separator string
}

func groupConcatStep1(_ *Context, aggCtx *AggregateContext, args []*vdbe.Mem) {
	groupConcatStep(aggCtx, args, ",")
}

func groupConcatStep2(_ *Context, aggCtx *AggregateContext, args []*vdbe.Mem) {
	sep := ","
	if args[1].Type != vdbe.MemNull {
		sep = args[1].TextValue()
	}
	groupConcatStep(aggCtx, args[:1], sep)
}

func groupConcatStep(aggCtx *AggregateContext, args []*vdbe.Mem, sep string) {
	if args[0].Type == vdbe.MemNull {
		return
	}
	st, ok := aggCtx.Data.(*groupConcatAgg)
	if !ok {
		st = &groupConcatAgg{separator: sep}
		aggCtx.Data = st
	}
	st.parts = append(st.parts, args[0].TextValue())
}

func groupConcatFinalize(_ *Context, aggCtx *AggregateContext) *vdbe.Mem {
	st, ok := aggCtx.Data.(*groupConcatAgg)
	if !ok || len(st.parts) == 0 {
		return vdbe.NewMemNull()
	}
	return vdbe.NewMemStr(strings.Join(st.parts, st.separator))
}
