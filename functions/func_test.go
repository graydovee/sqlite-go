package functions

import (
	"math"
	"strings"
	"testing"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// --- Helpers ---

func newRegistry() *FuncRegistry {
	return NewFuncRegistry()
}

func callScalar(r *FuncRegistry, name string, args ...*vdbe.Mem) *vdbe.Mem {
	fn := r.Lookup(name, len(args))
	if fn == nil {
		return vdbe.NewMemNull()
	}
	ctx := &Context{}
	return fn.ScalarFunc(ctx, args)
}

func callAggStep(r *FuncRegistry, name string, numArgs int, aggCtx *AggregateContext, args ...*vdbe.Mem) {
	fn := r.Lookup(name, numArgs)
	if fn == nil {
		return
	}
	ctx := &Context{}
	fn.StepFunc(ctx, aggCtx, args)
}

func callAggFinalize(r *FuncRegistry, name string, numArgs int, aggCtx *AggregateContext) *vdbe.Mem {
	fn := r.Lookup(name, numArgs)
	if fn == nil {
		return vdbe.NewMemNull()
	}
	ctx := &Context{}
	return fn.FinalizeFunc(ctx, aggCtx)
}

// --- Scalar function tests ---

func TestAbs(t *testing.T) {
	r := newRegistry()

	tests := []struct {
		input    int64
		expected int64
	}{
		{5, 5},
		{-5, 5},
		{0, 0},
		{-9223372036854775807, 9223372036854775807},
	}
	for _, tt := range tests {
		result := callScalar(r, "abs", vdbe.NewMemInt(tt.input))
		if result.IntVal != tt.expected {
			t.Errorf("abs(%d) = %d, want %d", tt.input, result.IntVal, tt.expected)
		}
	}

	// abs(NULL)
	result := callScalar(r, "abs", vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("abs(NULL) should be NULL, got %v", result)
	}

	// abs(float)
	result = callScalar(r, "abs", vdbe.NewMemFloat(-3.14))
	if result.FloatVal != 3.14 {
		t.Errorf("abs(-3.14) = %f, want 3.14", result.FloatVal)
	}

	// abs(string) - returns 0.0
	result = callScalar(r, "abs", vdbe.NewMemStr("hello"))
	if result.FloatVal != 0.0 {
		t.Errorf("abs('hello') = %f, want 0.0", result.FloatVal)
	}
}

func TestTypeof(t *testing.T) {
	r := newRegistry()

	tests := []struct {
		input    *vdbe.Mem
		expected string
	}{
		{vdbe.NewMemInt(42), "integer"},
		{vdbe.NewMemFloat(3.14), "real"},
		{vdbe.NewMemStr("hello"), "text"},
		{vdbe.NewMemBlob([]byte{1, 2, 3}), "blob"},
		{vdbe.NewMemNull(), "null"},
	}
	for _, tt := range tests {
		result := callScalar(r, "typeof", tt.input)
		if result.TextValue() != tt.expected {
			t.Errorf("typeof(%v) = %q, want %q", tt.input, result.TextValue(), tt.expected)
		}
	}
}

func TestLength(t *testing.T) {
	r := newRegistry()

	// String length (UTF-8 character count)
	result := callScalar(r, "length", vdbe.NewMemStr("hello"))
	if result.IntVal != 5 {
		t.Errorf("length('hello') = %d, want 5", result.IntVal)
	}

	// Unicode
	result = callScalar(r, "length", vdbe.NewMemStr("héllo"))
	if result.IntVal != 5 {
		t.Errorf("length('héllo') = %d, want 5", result.IntVal)
	}

	// Blob length (bytes)
	result = callScalar(r, "length", vdbe.NewMemBlob([]byte{1, 2, 3}))
	if result.IntVal != 3 {
		t.Errorf("length(blob(3)) = %d, want 3", result.IntVal)
	}

	// Integer length (digit count)
	result = callScalar(r, "length", vdbe.NewMemInt(12345))
	if result.IntVal != 5 {
		t.Errorf("length(12345) = %d, want 5", result.IntVal)
	}

	// NULL
	result = callScalar(r, "length", vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("length(NULL) should be NULL")
	}
}

func TestLower(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "lower", vdbe.NewMemStr("HELLO World"))
	if result.TextValue() != "hello world" {
		t.Errorf("lower('HELLO World') = %q, want %q", result.TextValue(), "hello world")
	}

	result = callScalar(r, "lower", vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("lower(NULL) should be NULL")
	}
}

func TestUpper(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "upper", vdbe.NewMemStr("hello World"))
	if result.TextValue() != "HELLO WORLD" {
		t.Errorf("upper('hello World') = %q, want %q", result.TextValue(), "HELLO WORLD")
	}

	result = callScalar(r, "upper", vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("upper(NULL) should be NULL")
	}
}

func TestCoalesce(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "coalesce", vdbe.NewMemNull(), vdbe.NewMemNull(), vdbe.NewMemInt(3))
	if result.IntVal != 3 {
		t.Errorf("coalesce(NULL, NULL, 3) = %d, want 3", result.IntVal)
	}

	result = callScalar(r, "coalesce", vdbe.NewMemStr("x"))
	if result.TextValue() != "x" {
		t.Errorf("coalesce('x') = %q, want 'x'", result.TextValue())
	}

	result = callScalar(r, "coalesce", vdbe.NewMemNull(), vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("coalesce(NULL, NULL) should be NULL")
	}
}

func TestIfnull(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "ifnull", vdbe.NewMemInt(1), vdbe.NewMemInt(2))
	if result.IntVal != 1 {
		t.Errorf("ifnull(1, 2) = %d, want 1", result.IntVal)
	}

	result = callScalar(r, "ifnull", vdbe.NewMemNull(), vdbe.NewMemInt(2))
	if result.IntVal != 2 {
		t.Errorf("ifnull(NULL, 2) = %d, want 2", result.IntVal)
	}
}

func TestIif(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "iif", vdbe.NewMemInt(1), vdbe.NewMemStr("yes"), vdbe.NewMemStr("no"))
	if result.TextValue() != "yes" {
		t.Errorf("iif(1, 'yes', 'no') = %q, want 'yes'", result.TextValue())
	}

	result = callScalar(r, "iif", vdbe.NewMemInt(0), vdbe.NewMemStr("yes"), vdbe.NewMemStr("no"))
	if result.TextValue() != "no" {
		t.Errorf("iif(0, 'yes', 'no') = %q, want 'no'", result.TextValue())
	}
}

func TestNullif(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "nullif", vdbe.NewMemInt(1), vdbe.NewMemInt(1))
	if result.Type != vdbe.MemNull {
		t.Errorf("nullif(1, 1) should be NULL")
	}

	result = callScalar(r, "nullif", vdbe.NewMemInt(1), vdbe.NewMemInt(2))
	if result.IntVal != 1 {
		t.Errorf("nullif(1, 2) = %d, want 1", result.IntVal)
	}
}

func TestSubstr(t *testing.T) {
	r := newRegistry()

	// substr(x, start, length)
	result := callScalar(r, "substr", vdbe.NewMemStr("Hello"), vdbe.NewMemInt(2), vdbe.NewMemInt(3))
	if result.TextValue() != "ell" {
		t.Errorf("substr('Hello', 2, 3) = %q, want 'ell'", result.TextValue())
	}

	// substr(x, start) - rest of string
	result = callScalar(r, "substr", vdbe.NewMemStr("Hello"), vdbe.NewMemInt(3))
	if result.TextValue() != "llo" {
		t.Errorf("substr('Hello', 3) = %q, want 'llo'", result.TextValue())
	}

	// Negative start (from end)
	result = callScalar(r, "substr", vdbe.NewMemStr("Hello"), vdbe.NewMemInt(-3), vdbe.NewMemInt(2))
	if result.TextValue() != "ll" {
		t.Errorf("substr('Hello', -3, 2) = %q, want 'll'", result.TextValue())
	}

	// NULL
	result = callScalar(r, "substr", vdbe.NewMemNull(), vdbe.NewMemInt(1), vdbe.NewMemInt(1))
	if result.Type != vdbe.MemNull {
		t.Errorf("substr(NULL, 1, 1) should be NULL")
	}
}

func TestTrim(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "trim", vdbe.NewMemStr("  hello  "))
	if result.TextValue() != "hello" {
		t.Errorf("trim('  hello  ') = %q, want 'hello'", result.TextValue())
	}

	result = callScalar(r, "trim", vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("trim(NULL) should be NULL")
	}
}

func TestLtrim(t *testing.T) {
	r := newRegistry()

	// ltrim with default whitespace
	result := callScalar(r, "ltrim", vdbe.NewMemStr("  hello  "))
	if result.TextValue() != "hello  " {
		t.Errorf("ltrim('  hello  ') = %q, want 'hello  '", result.TextValue())
	}

	// ltrim with custom chars
	result = callScalar(r, "ltrim", vdbe.NewMemStr("xxxhello"), vdbe.NewMemStr("x"))
	if result.TextValue() != "hello" {
		t.Errorf("ltrim('xxxhello', 'x') = %q, want 'hello'", result.TextValue())
	}
}

func TestRtrim(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "rtrim", vdbe.NewMemStr("  hello  "))
	if result.TextValue() != "  hello" {
		t.Errorf("rtrim('  hello  ') = %q, want '  hello'", result.TextValue())
	}

	result = callScalar(r, "rtrim", vdbe.NewMemStr("helloxxx"), vdbe.NewMemStr("x"))
	if result.TextValue() != "hello" {
		t.Errorf("rtrim('helloxxx', 'x') = %q, want 'hello'", result.TextValue())
	}
}

func TestReplace(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "replace", vdbe.NewMemStr("hello world"), vdbe.NewMemStr("world"), vdbe.NewMemStr("Go"))
	if result.TextValue() != "hello Go" {
		t.Errorf("replace('hello world', 'world', 'Go') = %q, want 'hello Go'", result.TextValue())
	}

	// NULL propagation
	result = callScalar(r, "replace", vdbe.NewMemNull(), vdbe.NewMemStr("a"), vdbe.NewMemStr("b"))
	if result.Type != vdbe.MemNull {
		t.Errorf("replace(NULL, 'a', 'b') should be NULL")
	}
}

func TestInstr(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "instr", vdbe.NewMemStr("hello world"), vdbe.NewMemStr("world"))
	if result.IntVal != 7 {
		t.Errorf("instr('hello world', 'world') = %d, want 7", result.IntVal)
	}

	result = callScalar(r, "instr", vdbe.NewMemStr("hello"), vdbe.NewMemStr("xyz"))
	if result.IntVal != 0 {
		t.Errorf("instr('hello', 'xyz') = %d, want 0", result.IntVal)
	}

	result = callScalar(r, "instr", vdbe.NewMemNull(), vdbe.NewMemStr("a"))
	if result.Type != vdbe.MemNull {
		t.Errorf("instr(NULL, 'a') should be NULL")
	}
}

func TestHex(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "hex", vdbe.NewMemStr("abc"))
	if result.TextValue() != "616263" {
		t.Errorf("hex('abc') = %q, want '616263'", result.TextValue())
	}

	result = callScalar(r, "hex", vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("hex(NULL) should be NULL")
	}
}

func TestUnhex(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "unhex", vdbe.NewMemStr("616263"))
	if result.Type != vdbe.MemBlob || string(result.Bytes) != "abc" {
		t.Errorf("unhex('616263') = %v, want blob 'abc'", result)
	}

	// Invalid hex returns NULL
	result = callScalar(r, "unhex", vdbe.NewMemStr("xyz"))
	if result.Type != vdbe.MemNull {
		t.Errorf("unhex('xyz') should be NULL")
	}

	// NULL
	result = callScalar(r, "unhex", vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("unhex(NULL) should be NULL")
	}
}

func TestUnicode(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "unicode", vdbe.NewMemStr("A"))
	if result.IntVal != 65 {
		t.Errorf("unicode('A') = %d, want 65", result.IntVal)
	}

	result = callScalar(r, "unicode", vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("unicode(NULL) should be NULL")
	}
}

func TestChar(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "char", vdbe.NewMemInt(72), vdbe.NewMemInt(105))
	if result.TextValue() != "Hi" {
		t.Errorf("char(72, 105) = %q, want 'Hi'", result.TextValue())
	}

	// NULL args are skipped
	result = callScalar(r, "char", vdbe.NewMemInt(65), vdbe.NewMemNull(), vdbe.NewMemInt(66))
	if result.TextValue() != "AB" {
		t.Errorf("char(65, NULL, 66) = %q, want 'AB'", result.TextValue())
	}
}

func TestPrintf(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "printf", vdbe.NewMemStr("hello %s"), vdbe.NewMemStr("world"))
	if result.TextValue() != "hello world" {
		t.Errorf("printf('hello %%s', 'world') = %q, want 'hello world'", result.TextValue())
	}

	// NULL format
	result = callScalar(r, "printf", vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("printf(NULL) should be NULL")
	}
}

func TestZeroblob(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "zeroblob", vdbe.NewMemInt(5))
	if result.Type != vdbe.MemBlob || len(result.Bytes) != 5 {
		t.Errorf("zeroblob(5) type=%v len=%d, want blob len=5", result.Type, len(result.Bytes))
	}
	for _, b := range result.Bytes {
		if b != 0 {
			t.Errorf("zeroblob byte should be 0, got %d", b)
		}
	}

	// NULL
	result = callScalar(r, "zeroblob", vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("zeroblob(NULL) should be NULL")
	}
}

func TestRandomblob(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "randomblob", vdbe.NewMemInt(16))
	if result.Type != vdbe.MemBlob || len(result.Bytes) != 16 {
		t.Errorf("randomblob(16) type=%v len=%d", result.Type, len(result.Bytes))
	}
}

func TestRandom(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "random")
	if result.Type != vdbe.MemInt {
		t.Errorf("random() type=%v, want MemInt", result.Type)
	}
	if result.IntVal < 0 {
		t.Errorf("random() should return non-negative (SQLite clears sign bit), got %d", result.IntVal)
	}
}

func TestMinMaxScalar(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "min", vdbe.NewMemInt(3), vdbe.NewMemInt(1), vdbe.NewMemInt(2))
	if result.IntVal != 1 {
		t.Errorf("min(3, 1, 2) = %d, want 1", result.IntVal)
	}

	result = callScalar(r, "max", vdbe.NewMemInt(3), vdbe.NewMemInt(1), vdbe.NewMemInt(2))
	if result.IntVal != 3 {
		t.Errorf("max(3, 1, 2) = %d, want 3", result.IntVal)
	}

	// NULL propagation
	result = callScalar(r, "min", vdbe.NewMemInt(1), vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("min(1, NULL) should be NULL")
	}
}

func TestLike(t *testing.T) {
	r := newRegistry()

	tests := []struct {
		pattern, str string
		expected     int64
	}{
		{"hello", "hello", 1},
		{"hello", "HELLO", 1}, // case-insensitive
		{"hel%", "hello", 1},
		{"hel%", "hel", 1},
		{"%", "anything", 1},
		{"h_llo", "hello", 1},
		{"h_llo", "hallo", 1},
		{"h%o", "hello", 1},
		{"h%o", "world", 0},
		{"a%b%c", "axybzc", 1},
	}
	for _, tt := range tests {
		result := callScalar(r, "like", vdbe.NewMemStr(tt.pattern), vdbe.NewMemStr(tt.str))
		if result.IntVal != tt.expected {
			t.Errorf("like(%q, %q) = %d, want %d", tt.pattern, tt.str, result.IntVal, tt.expected)
		}
	}

	// NULL
	result := callScalar(r, "like", vdbe.NewMemNull(), vdbe.NewMemStr("hello"))
	if result.Type != vdbe.MemNull {
		t.Errorf("like(NULL, 'hello') should be NULL")
	}
}

func TestGlob(t *testing.T) {
	r := newRegistry()

	tests := []struct {
		pattern, str string
		expected     int64
	}{
		{"hello", "hello", 1},
		{"hel*", "hello", 1},
		{"h?llo", "hello", 1},
		{"h?llo", "hallo", 1},
		{"h*o", "hello", 1},
		{"h*o", "world", 0},
		{"[hc]ello", "hello", 1},
		{"[hc]ello", "cello", 1},
		{"[hc]ello", "jello", 0},
	}
	for _, tt := range tests {
		result := callScalar(r, "glob", vdbe.NewMemStr(tt.pattern), vdbe.NewMemStr(tt.str))
		if result.IntVal != tt.expected {
			t.Errorf("glob(%q, %q) = %d, want %d", tt.pattern, tt.str, result.IntVal, tt.expected)
		}
	}
}

// --- Aggregate function tests ---

func TestCountStar(t *testing.T) {
	r := newRegistry()

	agg := &AggregateContext{}
	callAggStep(r, "count", 0, agg) // count(*)
	callAggStep(r, "count", 0, agg)
	callAggStep(r, "count", 0, agg)

	result := callAggFinalize(r, "count", 0, agg)
	if result.IntVal != 3 {
		t.Errorf("count(*) = %d, want 3", result.IntVal)
	}
}

func TestCount(t *testing.T) {
	r := newRegistry()

	agg := &AggregateContext{}
	callAggStep(r, "count", 1, agg, vdbe.NewMemInt(1))
	callAggStep(r, "count", 1, agg, vdbe.NewMemNull()) // NULL not counted
	callAggStep(r, "count", 1, agg, vdbe.NewMemInt(3))

	result := callAggFinalize(r, "count", 1, agg)
	if result.IntVal != 2 {
		t.Errorf("count(x) with NULL = %d, want 2", result.IntVal)
	}
}

func TestSum(t *testing.T) {
	r := newRegistry()

	agg := &AggregateContext{}
	callAggStep(r, "sum", 1, agg, vdbe.NewMemInt(10))
	callAggStep(r, "sum", 1, agg, vdbe.NewMemInt(20))
	callAggStep(r, "sum", 1, agg, vdbe.NewMemNull()) // NULL ignored
	callAggStep(r, "sum", 1, agg, vdbe.NewMemInt(30))

	result := callAggFinalize(r, "sum", 1, agg)
	if result.IntVal != 60 {
		t.Errorf("sum(x) = %d, want 60", result.IntVal)
	}

	// sum of all NULLs returns NULL
	agg2 := &AggregateContext{}
	callAggStep(r, "sum", 1, agg2, vdbe.NewMemNull())
	result = callAggFinalize(r, "sum", 1, agg2)
	if result.Type != vdbe.MemNull {
		t.Errorf("sum(all NULLs) should be NULL")
	}
}

func TestTotal(t *testing.T) {
	r := newRegistry()

	agg := &AggregateContext{}
	callAggStep(r, "total", 1, agg, vdbe.NewMemInt(10))
	callAggStep(r, "total", 1, agg, vdbe.NewMemInt(20))

	result := callAggFinalize(r, "total", 1, agg)
	if result.FloatVal != 30.0 {
		t.Errorf("total(x) = %f, want 30.0", result.FloatVal)
	}

	// total of all NULLs returns 0.0 (not NULL)
	agg2 := &AggregateContext{}
	callAggStep(r, "total", 1, agg2, vdbe.NewMemNull())
	result = callAggFinalize(r, "total", 1, agg2)
	if result.Type != vdbe.MemFloat || result.FloatVal != 0.0 {
		t.Errorf("total(all NULLs) = %v, want 0.0", result)
	}
}

func TestAvg(t *testing.T) {
	r := newRegistry()

	agg := &AggregateContext{}
	callAggStep(r, "avg", 1, agg, vdbe.NewMemInt(10))
	callAggStep(r, "avg", 1, agg, vdbe.NewMemInt(20))
	callAggStep(r, "avg", 1, agg, vdbe.NewMemInt(30))

	result := callAggFinalize(r, "avg", 1, agg)
	if result.FloatVal != 20.0 {
		t.Errorf("avg(x) = %f, want 20.0", result.FloatVal)
	}

	// avg of all NULLs returns NULL
	agg2 := &AggregateContext{}
	callAggStep(r, "avg", 1, agg2, vdbe.NewMemNull())
	result = callAggFinalize(r, "avg", 1, agg2)
	if result.Type != vdbe.MemNull {
		t.Errorf("avg(all NULLs) should be NULL")
	}
}

func TestMinMaxAggregate(t *testing.T) {
	r := newRegistry()

	agg := &AggregateContext{}
	callAggStep(r, "min", 1, agg, vdbe.NewMemInt(30))
	callAggStep(r, "min", 1, agg, vdbe.NewMemInt(10))
	callAggStep(r, "min", 1, agg, vdbe.NewMemNull()) // NULL ignored
	callAggStep(r, "min", 1, agg, vdbe.NewMemInt(20))

	result := callAggFinalize(r, "min", 1, agg)
	if result.IntVal != 10 {
		t.Errorf("min agg = %d, want 10", result.IntVal)
	}

	// max
	agg2 := &AggregateContext{}
	callAggStep(r, "max", 1, agg2, vdbe.NewMemInt(30))
	callAggStep(r, "max", 1, agg2, vdbe.NewMemInt(10))
	callAggStep(r, "max", 1, agg2, vdbe.NewMemInt(20))

	result = callAggFinalize(r, "max", 1, agg2)
	if result.IntVal != 30 {
		t.Errorf("max agg = %d, want 30", result.IntVal)
	}

	// all NULLs
	agg3 := &AggregateContext{}
	callAggStep(r, "min", 1, agg3, vdbe.NewMemNull())
	result = callAggFinalize(r, "min", 1, agg3)
	if result.Type != vdbe.MemNull {
		t.Errorf("min agg (all NULLs) should be NULL")
	}
}

func TestGroupConcat(t *testing.T) {
	r := newRegistry()

	// group_concat(x) - default comma separator
	agg := &AggregateContext{}
	callAggStep(r, "group_concat", 1, agg, vdbe.NewMemStr("a"))
	callAggStep(r, "group_concat", 1, agg, vdbe.NewMemNull()) // NULL ignored
	callAggStep(r, "group_concat", 1, agg, vdbe.NewMemStr("b"))

	result := callAggFinalize(r, "group_concat", 1, agg)
	if result.TextValue() != "a,b" {
		t.Errorf("group_concat(x) = %q, want 'a,b'", result.TextValue())
	}

	// group_concat(x, y) - custom separator
	agg2 := &AggregateContext{}
	callAggStep(r, "group_concat", 2, agg2, vdbe.NewMemStr("a"), vdbe.NewMemStr("|"))
	callAggStep(r, "group_concat", 2, agg2, vdbe.NewMemStr("b"), vdbe.NewMemStr("|"))

	result = callAggFinalize(r, "group_concat", 2, agg2)
	if result.TextValue() != "a|b" {
		t.Errorf("group_concat(x, '|') = %q, want 'a|b'", result.TextValue())
	}

	// all NULLs
	agg3 := &AggregateContext{}
	callAggStep(r, "group_concat", 1, agg3, vdbe.NewMemNull())
	result = callAggFinalize(r, "group_concat", 1, agg3)
	if result.Type != vdbe.MemNull {
		t.Errorf("group_concat(all NULLs) should be NULL")
	}
}

// --- Date function tests ---

func TestDateFunc(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "date", vdbe.NewMemStr("2024-01-15"))
	if result.TextValue() != "2024-01-15" {
		t.Errorf("date('2024-01-15') = %q, want '2024-01-15'", result.TextValue())
	}

	// With modifier
	result = callScalar(r, "date", vdbe.NewMemStr("2024-01-15"), vdbe.NewMemStr("+1 day"))
	if result.TextValue() != "2024-01-16" {
		t.Errorf("date('2024-01-15', '+1 day') = %q, want '2024-01-16'", result.TextValue())
	}

	// start of month
	result = callScalar(r, "date", vdbe.NewMemStr("2024-01-15"), vdbe.NewMemStr("start of month"))
	if result.TextValue() != "2024-01-01" {
		t.Errorf("date('2024-01-15', 'start of month') = %q, want '2024-01-01'", result.TextValue())
	}

	// NULL
	result = callScalar(r, "date", vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("date(NULL) should be NULL")
	}

	// No args
	result = callScalar(r, "date")
	if result.Type != vdbe.MemNull {
		t.Errorf("date() should be NULL")
	}
}

func TestTimeFunc(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "time", vdbe.NewMemStr("2024-01-15 10:30:45"))
	if result.TextValue() != "10:30:45" {
		t.Errorf("time('2024-01-15 10:30:45') = %q, want '10:30:45'", result.TextValue())
	}

	// Time-only string
	result = callScalar(r, "time", vdbe.NewMemStr("14:25:30"))
	if result.TextValue() != "14:25:30" {
		t.Errorf("time('14:25:30') = %q, want '14:25:30'", result.TextValue())
	}
}

func TestDatetimeFunc(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "datetime", vdbe.NewMemStr("2024-01-15 10:30:45"))
	if result.TextValue() != "2024-01-15 10:30:45" {
		t.Errorf("datetime('2024-01-15 10:30:45') = %q, want '2024-01-15 10:30:45'", result.TextValue())
	}

	// With modifier
	result = callScalar(r, "datetime", vdbe.NewMemStr("2024-01-15 10:30:45"), vdbe.NewMemStr("+1 hour"))
	if result.TextValue() != "2024-01-15 11:30:45" {
		t.Errorf("datetime('2024-01-15 10:30:45', '+1 hour') = %q, want '2024-01-15 11:30:45'", result.TextValue())
	}
}

func TestJuliandayFunc(t *testing.T) {
	r := newRegistry()

	// Known Julian Day: 2000-01-01 12:00:00 = JD 2451545.0
	result := callScalar(r, "julianday", vdbe.NewMemStr("2000-01-01 12:00:00"))
	jd := result.FloatVal
	// Allow small floating point tolerance
	if math.Abs(jd-2451545.0) > 0.001 {
		t.Errorf("julianday('2000-01-01 12:00:00') = %f, want ~2451545.0", jd)
	}

	// 1970-01-01 = JD 2440587.5
	result = callScalar(r, "julianday", vdbe.NewMemStr("1970-01-01"))
	jd = result.FloatVal
	if math.Abs(jd-2440587.5) > 0.001 {
		t.Errorf("julianday('1970-01-01') = %f, want ~2440587.5", jd)
	}
}

func TestStrftimeFunc(t *testing.T) {
	r := newRegistry()

	// %Y-%m-%d
	result := callScalar(r, "strftime", vdbe.NewMemStr("%Y-%m-%d"), vdbe.NewMemStr("2024-06-15"))
	if result.TextValue() != "2024-06-15" {
		t.Errorf("strftime('%%Y-%%m-%%d', '2024-06-15') = %q, want '2024-06-15'", result.TextValue())
	}

	// %H:%M
	result = callScalar(r, "strftime", vdbe.NewMemStr("%H:%M"), vdbe.NewMemStr("2024-06-15 14:30:00"))
	if result.TextValue() != "14:30" {
		t.Errorf("strftime('%%H:%%M', '2024-06-15 14:30:00') = %q, want '14:30'", result.TextValue())
	}

	// %s - unix timestamp
	result = callScalar(r, "strftime", vdbe.NewMemStr("%s"), vdbe.NewMemStr("1970-01-01 00:00:00"))
	// Should be 0 or very close (depending on timezone handling)
	s := result.IntVal
	if s != 0 {
		t.Errorf("strftime('%%s', '1970-01-01 00:00:00') = %d, want 0", s)
	}

	// NULL format
	result = callScalar(r, "strftime", vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("strftime(NULL) should be NULL")
	}
}

// --- Math function tests ---

func TestRound(t *testing.T) {
	r := newRegistry()

	// round(x)
	result := callScalar(r, "round", vdbe.NewMemFloat(1.5))
	if result.FloatVal != 2.0 {
		t.Errorf("round(1.5) = %f, want 2.0", result.FloatVal)
	}

	result = callScalar(r, "round", vdbe.NewMemFloat(1.4))
	if result.FloatVal != 1.0 {
		t.Errorf("round(1.4) = %f, want 1.0", result.FloatVal)
	}

	result = callScalar(r, "round", vdbe.NewMemFloat(-1.5))
	if result.FloatVal != -2.0 {
		t.Errorf("round(-1.5) = %f, want -2.0", result.FloatVal)
	}

	// round(x, n)
	result = callScalar(r, "round", vdbe.NewMemFloat(1.2345), vdbe.NewMemInt(2))
	if result.FloatVal != 1.23 {
		t.Errorf("round(1.2345, 2) = %f, want 1.23", result.FloatVal)
	}

	result = callScalar(r, "round", vdbe.NewMemFloat(123.456), vdbe.NewMemInt(-1))
	// negative n is clamped to 0
	if result.FloatVal != 123.0 {
		t.Errorf("round(123.456, -1) = %f, want 123.0", result.FloatVal)
	}

	// NULL
	result = callScalar(r, "round", vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("round(NULL) should be NULL")
	}

	// NULL precision
	result = callScalar(r, "round", vdbe.NewMemFloat(1.5), vdbe.NewMemNull())
	if result.Type != vdbe.MemNull {
		t.Errorf("round(1.5, NULL) should be NULL")
	}
}

// --- Registry tests ---

func TestFuncRegistry(t *testing.T) {
	r := newRegistry()

	// Lookup existing function
	fn := r.Lookup("abs", 1)
	if fn == nil {
		t.Fatal("abs should be registered")
	}
	if fn.Name != "abs" {
		t.Errorf("Lookup('abs', 1).Name = %q, want 'abs'", fn.Name)
	}

	// Variable args
	fn = r.Lookup("coalesce", 3)
	if fn == nil {
		t.Fatal("coalesce should be registered with varargs")
	}

	// Aggregate
	fn = r.Lookup("count", 0)
	if fn == nil {
		t.Fatal("count(*) should be registered")
	}
	if !fn.IsAggregate {
		t.Error("count should be aggregate")
	}

	// Non-existent
	fn = r.Lookup("nonexistent", 1)
	if fn != nil {
		t.Error("nonexistent should not be found")
	}
}

func TestFuncRegistryOverloads(t *testing.T) {
	r := newRegistry()

	// unhex has 1-arg and 2-arg overloads
	fn1 := r.Lookup("unhex", 1)
	if fn1 == nil {
		t.Fatal("unhex(X) should be registered")
	}
	fn2 := r.Lookup("unhex", 2)
	if fn2 == nil {
		t.Fatal("unhex(X, Y) should be registered")
	}

	// They should be different functions
	if fn1 == fn2 {
		t.Error("unhex(1) and unhex(2) should be different overloads")
	}
}

// --- Edge case tests ---

func TestAbsMinInt64(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "abs", vdbe.NewMemInt(math.MinInt64))
	// Should return an error (integer overflow) -> NULL
	if result.Type != vdbe.MemNull {
		t.Errorf("abs(MinInt64) should return NULL due to overflow, got type=%v", result.Type)
	}
}

func TestSubstrUnicode(t *testing.T) {
	r := newRegistry()

	// Unicode substr
	result := callScalar(r, "substr", vdbe.NewMemStr("日本語テスト"), vdbe.NewMemInt(2), vdbe.NewMemInt(2))
	if result.TextValue() != "本語" {
		t.Errorf("substr('日本語テスト', 2, 2) = %q, want '本語'", result.TextValue())
	}
}

func TestLengthUnicode(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "length", vdbe.NewMemStr("日本語"))
	if result.IntVal != 3 {
		t.Errorf("length('日本語') = %d, want 3", result.IntVal)
	}
}

func TestLikeEscape(t *testing.T) {
	r := newRegistry()

	// With escape character
	result := callScalar(r, "like", vdbe.NewMemStr("a%b"), vdbe.NewMemStr("a%b"), vdbe.NewMemStr("\\"))
	// Without escape, a%b would match anything starting with 'a'
	// With escape '\', the % is literal but we're not escaping here
	// Actually the escape means: \ before a char means literal
	// So like('a\%b', 'a%b', '\') should match
	// But our pattern is 'a%b', not 'a\%b', so it's the normal behavior
	if result.IntVal != 1 {
		t.Errorf("like('a%%b', 'a%%b', '\\') = %d, want 1", result.IntVal)
	}
}

func TestDateArithmetic(t *testing.T) {
	r := newRegistry()

	// Add months
	result := callScalar(r, "date", vdbe.NewMemStr("2024-01-31"), vdbe.NewMemStr("+1 month"))
	if result.TextValue() != "2024-02-29" {
		// Jan 31 + 1 month = Feb 29 (leap year, clamped)
		t.Errorf("date('2024-01-31', '+1 month') = %q, want '2024-02-29'", result.TextValue())
	}

	// Subtract days
	result = callScalar(r, "date", vdbe.NewMemStr("2024-03-01"), vdbe.NewMemStr("-1 day"))
	if result.TextValue() != "2024-02-29" {
		t.Errorf("date('2024-03-01', '-1 day') = %q, want '2024-02-29'", result.TextValue())
	}
}

func TestInstrBlob(t *testing.T) {
	r := newRegistry()

	// Blob-on-blob search
	result := callScalar(r, "instr", vdbe.NewMemBlob([]byte{1, 2, 3, 4, 5}), vdbe.NewMemBlob([]byte{3, 4}))
	if result.IntVal != 3 {
		t.Errorf("instr(blob, blob) = %d, want 3", result.IntVal)
	}
}

func TestSumFloat(t *testing.T) {
	r := newRegistry()

	agg := &AggregateContext{}
	callAggStep(r, "sum", 1, agg, vdbe.NewMemFloat(1.5))
	callAggStep(r, "sum", 1, agg, vdbe.NewMemFloat(2.5))

	result := callAggFinalize(r, "sum", 1, agg)
	if math.Abs(result.FloatVal-4.0) > 0.001 {
		t.Errorf("sum(1.5, 2.5) = %f, want 4.0", result.FloatVal)
	}
}

func TestPrintfFormat(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "printf", vdbe.NewMemStr("%d"), vdbe.NewMemInt(42))
	if result.TextValue() != "42" {
		t.Errorf("printf('%%d', 42) = %q, want '42'", result.TextValue())
	}

	// Multiple args
	result = callScalar(r, "printf", vdbe.NewMemStr("%s is %d"), vdbe.NewMemStr("age"), vdbe.NewMemInt(25))
	if result.TextValue() != "age is 25" {
		t.Errorf("printf('%%s is %%d', 'age', 25) = %q, want 'age is 25'", result.TextValue())
	}

	// %% literal percent
	result = callScalar(r, "printf", vdbe.NewMemStr("100%%"))
	if result.TextValue() != "100%" {
		t.Errorf("printf('100%%%%') = %q, want '100%%'", result.TextValue())
	}
}

func TestGroupConcatEmpty(t *testing.T) {
	r := newRegistry()

	// No steps at all
	agg := &AggregateContext{}
	result := callAggFinalize(r, "group_concat", 1, agg)
	if result.Type != vdbe.MemNull {
		t.Errorf("group_concat (no steps) should be NULL, got %v", result)
	}
}

func TestDateStartOfDay(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "datetime", vdbe.NewMemStr("2024-06-15 14:30:45"), vdbe.NewMemStr("start of day"))
	if result.TextValue() != "2024-06-15 00:00:00" {
		t.Errorf("datetime('2024-06-15 14:30:45', 'start of day') = %q, want '2024-06-15 00:00:00'", result.TextValue())
	}
}

func TestReplaceMultiple(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "replace", vdbe.NewMemStr("aaa"), vdbe.NewMemStr("a"), vdbe.NewMemStr("bb"))
	if result.TextValue() != "bbbbbb" {
		t.Errorf("replace('aaa', 'a', 'bb') = %q, want 'bbbbbb'", result.TextValue())
	}
}

func TestInstrUTF8(t *testing.T) {
	r := newRegistry()

	// UTF-8 character position, not byte position
	result := callScalar(r, "instr", vdbe.NewMemStr("héllo"), vdbe.NewMemStr("l"))
	if result.IntVal != 3 {
		t.Errorf("instr('héllo', 'l') = %d, want 3 (character position)", result.IntVal)
	}
}

func TestSubstrEdgeCases(t *testing.T) {
	r := newRegistry()

	// Start beyond string
	result := callScalar(r, "substr", vdbe.NewMemStr("hi"), vdbe.NewMemInt(5), vdbe.NewMemInt(1))
	if result.TextValue() != "" {
		t.Errorf("substr('hi', 5, 1) = %q, want ''", result.TextValue())
	}

	// Length beyond string
	result = callScalar(r, "substr", vdbe.NewMemStr("hi"), vdbe.NewMemInt(1), vdbe.NewMemInt(100))
	if result.TextValue() != "hi" {
		t.Errorf("substr('hi', 1, 100) = %q, want 'hi'", result.TextValue())
	}
}

func TestRoundInteger(t *testing.T) {
	r := newRegistry()

	// round of integer
	result := callScalar(r, "round", vdbe.NewMemInt(5))
	if result.FloatVal != 5.0 {
		t.Errorf("round(5) = %f, want 5.0", result.FloatVal)
	}
}

func TestHexBlob(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "hex", vdbe.NewMemBlob([]byte{0xDE, 0xAD, 0xBE, 0xEF}))
	if result.TextValue() != "DEADBEEF" {
		t.Errorf("hex(blob) = %q, want 'DEADBEEF'", result.TextValue())
	}
}

func TestZeroblobNegative(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "zeroblob", vdbe.NewMemInt(-5))
	if result.Type != vdbe.MemBlob || len(result.Bytes) != 0 {
		t.Errorf("zeroblob(-5) should be empty blob")
	}
}

func TestDateStartOfYear(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "date", vdbe.NewMemStr("2024-06-15"), vdbe.NewMemStr("start of year"))
	if result.TextValue() != "2024-01-01" {
		t.Errorf("date('2024-06-15', 'start of year') = %q, want '2024-01-01'", result.TextValue())
	}
}

func TestStrftimeDayOfYear(t *testing.T) {
	r := newRegistry()

	// Jan 1 = day 1
	result := callScalar(r, "strftime", vdbe.NewMemStr("%j"), vdbe.NewMemStr("2024-01-01"))
	if result.TextValue() != "001" {
		t.Errorf("strftime('%%j', '2024-01-01') = %q, want '001'", result.TextValue())
	}
}

func TestStrftimeAmPm(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "strftime", vdbe.NewMemStr("%p"), vdbe.NewMemStr("2024-01-01 10:00:00"))
	if result.TextValue() != "AM" {
		t.Errorf("strftime('%%p', 10:00) = %q, want 'AM'", result.TextValue())
	}

	result = callScalar(r, "strftime", vdbe.NewMemStr("%p"), vdbe.NewMemStr("2024-01-01 14:00:00"))
	if result.TextValue() != "PM" {
		t.Errorf("strftime('%%p', 14:00) = %q, want 'PM'", result.TextValue())
	}
}

func TestMinString(t *testing.T) {
	r := newRegistry()

	result := callScalar(r, "min", vdbe.NewMemStr("banana"), vdbe.NewMemStr("apple"), vdbe.NewMemStr("cherry"))
	if result.TextValue() != "apple" {
		t.Errorf("min('banana', 'apple', 'cherry') = %q, want 'apple'", result.TextValue())
	}
}

func TestDatetimeArithmetic(t *testing.T) {
	r := newRegistry()

	// datetime + hours
	result := callScalar(r, "datetime", vdbe.NewMemStr("2024-01-15 23:30:00"), vdbe.NewMemStr("+2 hours"))
	if !strings.HasPrefix(result.TextValue(), "2024-01-16 01:30") {
		t.Errorf("datetime('2024-01-15 23:30:00', '+2 hours') = %q, want '2024-01-16 01:30:00'", result.TextValue())
	}
}

func TestFuncContextError(t *testing.T) {
	ctx := &Context{}
	if ctx.Error != nil {
		t.Error("new Context should have nil Error")
	}
}

// --- JSON Arrow operator tests ---

func TestJSONArrowOperator(t *testing.T) {
	r := newRegistry()

	doc := `{"a": 1, "b": "hello", "c": [1,2,3]}`

	// -> returns JSON text representation
	result := callScalar(r, "json_arrow",
		vdbe.NewMemStr(doc),
		vdbe.NewMemStr("$.a"),
	)
	if result.Type != vdbe.MemStr || result.TextValue() != "1" {
		t.Errorf("-> $.a = %v, want \"1\"", result)
	}

	result = callScalar(r, "json_arrow",
		vdbe.NewMemStr(doc),
		vdbe.NewMemStr("$.c"),
	)
	if result.Type != vdbe.MemStr || result.TextValue() != "[1,2,3]" {
		t.Errorf("-> $.c = %v, want \"[1,2,3]\"", result)
	}
}

func TestJSONArrow2Operator(t *testing.T) {
	r := newRegistry()

	doc := `{"a": 1, "b": "hello", "c": [10,20,30]}`

	// ->> returns SQL value (unwrapped)
	result := callScalar(r, "json_arrow2",
		vdbe.NewMemStr(doc),
		vdbe.NewMemStr("$.a"),
	)
	if result.Type != vdbe.MemInt || result.IntVal != 1 {
		t.Errorf("->> $.a = %v, want int 1", result)
	}

	result = callScalar(r, "json_arrow2",
		vdbe.NewMemStr(doc),
		vdbe.NewMemStr("$.b"),
	)
	if result.Type != vdbe.MemStr || result.TextValue() != "hello" {
		t.Errorf("->> $.b = %v, want string \"hello\"", result)
	}
}

// --- JSON Extended tests ---

func TestJSONPatch(t *testing.T) {
	r := newRegistry()

	// json_remove
	result := callScalar(r, "json_remove",
		vdbe.NewMemStr(`{"a":1,"b":2,"c":3}`),
		vdbe.NewMemStr("$.b"),
	)
	if result.Type != vdbe.MemStr {
		t.Fatalf("json_remove type = %v, want MemStr", result.Type)
	}
	if result.TextValue() != `{"a":1,"c":3}` {
		t.Errorf("json_remove = %q, want {\"a\":1,\"c\":3}", result.TextValue())
	}
}

func TestJSONNestedPath(t *testing.T) {
	r := newRegistry()

	doc := `{"users": [{"name": "alice", "age": 30}, {"name": "bob", "age": 25}]}`

	result := callScalar(r, "json_extract",
		vdbe.NewMemStr(doc),
		vdbe.NewMemStr("$.users[0].name"),
	)
	if result.Type != vdbe.MemStr || result.TextValue() != "alice" {
		t.Errorf("json_extract $.users[0].name = %v, want alice", result)
	}

	result = callScalar(r, "json_extract",
		vdbe.NewMemStr(doc),
		vdbe.NewMemStr("$.users[1].age"),
	)
	if result.Type != vdbe.MemInt || result.IntVal != 25 {
		t.Errorf("json_extract $.users[1].age = %v, want 25", result)
	}
}
