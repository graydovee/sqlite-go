package functions

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

func registerScalarFunctions(r *FuncRegistry) {
	// Single-argument scalar functions
	r.Register(&FuncDef{Name: "abs", NumArgs: 1, IsDeterministic: true, ScalarFunc: absFunc})
	r.Register(&FuncDef{Name: "typeof", NumArgs: 1, IsDeterministic: true, ScalarFunc: typeofFunc})
	r.Register(&FuncDef{Name: "length", NumArgs: 1, IsDeterministic: true, ScalarFunc: lengthFunc})
	r.Register(&FuncDef{Name: "lower", NumArgs: 1, IsDeterministic: true, ScalarFunc: lowerFunc})
	r.Register(&FuncDef{Name: "upper", NumArgs: 1, IsDeterministic: true, ScalarFunc: upperFunc})
	r.Register(&FuncDef{Name: "trim", NumArgs: 1, IsDeterministic: true, ScalarFunc: trimFunc})
	r.Register(&FuncDef{Name: "hex", NumArgs: 1, IsDeterministic: true, ScalarFunc: hexFunc})
	r.Register(&FuncDef{Name: "unhex", NumArgs: 1, IsDeterministic: true, ScalarFunc: unhexFunc1})
	r.Register(&FuncDef{Name: "unhex", NumArgs: 2, IsDeterministic: true, ScalarFunc: unhexFunc2})
	r.Register(&FuncDef{Name: "unicode", NumArgs: 1, IsDeterministic: true, ScalarFunc: unicodeFunc})
	r.Register(&FuncDef{Name: "zeroblob", NumArgs: 1, IsDeterministic: true, ScalarFunc: zeroblobFunc})
	r.Register(&FuncDef{Name: "randomblob", NumArgs: 1, IsDeterministic: false, ScalarFunc: randomblobFunc})
	r.Register(&FuncDef{Name: "random", NumArgs: 0, IsDeterministic: false, ScalarFunc: randomFunc})

	// Two-argument scalar functions
	r.Register(&FuncDef{Name: "ifnull", NumArgs: 2, IsDeterministic: true, ScalarFunc: ifnullFunc})
	r.Register(&FuncDef{Name: "nullif", NumArgs: 2, IsDeterministic: true, ScalarFunc: nullifFunc})
	r.Register(&FuncDef{Name: "instr", NumArgs: 2, IsDeterministic: true, ScalarFunc: instrFunc})
	r.Register(&FuncDef{Name: "ltrim", NumArgs: 1, IsDeterministic: true, ScalarFunc: ltrimFunc1})
	r.Register(&FuncDef{Name: "ltrim", NumArgs: 2, IsDeterministic: true, ScalarFunc: ltrimFunc2})
	r.Register(&FuncDef{Name: "rtrim", NumArgs: 1, IsDeterministic: true, ScalarFunc: rtrimFunc1})
	r.Register(&FuncDef{Name: "rtrim", NumArgs: 2, IsDeterministic: true, ScalarFunc: rtrimFunc2})
	r.Register(&FuncDef{Name: "replace", NumArgs: 3, IsDeterministic: true, ScalarFunc: replaceFunc})
	r.Register(&FuncDef{Name: "glob", NumArgs: 2, IsDeterministic: true, ScalarFunc: globFunc})

	// Three-argument functions
	r.Register(&FuncDef{Name: "substr", NumArgs: 2, IsDeterministic: true, ScalarFunc: substrFunc2})
	r.Register(&FuncDef{Name: "substr", NumArgs: 3, IsDeterministic: true, ScalarFunc: substrFunc3})
	r.Register(&FuncDef{Name: "iif", NumArgs: 3, IsDeterministic: true, ScalarFunc: iifFunc})

	// Variable-argument functions
	r.Register(&FuncDef{Name: "coalesce", NumArgs: -1, IsDeterministic: true, ScalarFunc: coalesceFunc})
	r.Register(&FuncDef{Name: "char", NumArgs: -1, IsDeterministic: true, ScalarFunc: charFunc})
	r.Register(&FuncDef{Name: "printf", NumArgs: -1, IsDeterministic: true, ScalarFunc: printfFunc})
	r.Register(&FuncDef{Name: "min", NumArgs: -1, IsDeterministic: true, ScalarFunc: minScalarFunc})
	r.Register(&FuncDef{Name: "max", NumArgs: -1, IsDeterministic: true, ScalarFunc: maxScalarFunc})
	r.Register(&FuncDef{Name: "like", NumArgs: 2, IsDeterministic: true, ScalarFunc: likeFunc2})
	r.Register(&FuncDef{Name: "like", NumArgs: 3, IsDeterministic: true, ScalarFunc: likeFunc3})
}

// --- Single-argument functions ---

func absFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	m := args[0]
	switch m.Type {
	case vdbe.MemNull:
		return vdbe.NewMemNull()
	case vdbe.MemInt:
		v := m.IntVal
		if v < 0 {
			if v == math.MinInt64 {
				ctx := &Context{}
				ctx.Error = fmt.Errorf("integer overflow")
				return vdbe.NewMemNull()
			}
			v = -v
		}
		return vdbe.NewMemInt(v)
	default:
		// float, string, blob -> try to get as float
		f := m.FloatValue()
		if f < 0 {
			f = -f
		}
		return vdbe.NewMemFloat(f)
	}
}

func typeofFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	m := args[0]
	var typ string
	switch m.Type {
	case vdbe.MemNull:
		typ = "null"
	case vdbe.MemInt:
		typ = "integer"
	case vdbe.MemFloat:
		typ = "real"
	case vdbe.MemStr:
		typ = "text"
	case vdbe.MemBlob:
		typ = "blob"
	default:
		typ = "null"
	}
	fmt.Printf("typeofFunc: m.Type=%d -> typ=%q\n", m.Type, typ)
	return vdbe.NewMemStr(typ)
}

func lengthFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	m := args[0]
	switch m.Type {
	case vdbe.MemNull:
		return vdbe.NewMemNull()
	case vdbe.MemStr:
		return vdbe.NewMemInt(int64(utf8.RuneCount(m.Bytes)))
	case vdbe.MemBlob:
		return vdbe.NewMemInt(int64(len(m.Bytes)))
	case vdbe.MemInt:
		// Length of string representation
		s := m.TextValue()
		return vdbe.NewMemInt(int64(len(s)))
	case vdbe.MemFloat:
		s := m.TextValue()
		return vdbe.NewMemInt(int64(len(s)))
	default:
		return vdbe.NewMemNull()
	}
}

func lowerFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	m := args[0]
	if m.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	return vdbe.NewMemStr(strings.ToLower(m.TextValue()))
}

func upperFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	m := args[0]
	if m.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	return vdbe.NewMemStr(strings.ToUpper(m.TextValue()))
}

func trimFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	m := args[0]
	if m.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	return vdbe.NewMemStr(strings.TrimSpace(m.TextValue()))
}

func hexFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	m := args[0]
	if m.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	var b []byte
	if m.Type == vdbe.MemBlob {
		b = m.Bytes
	} else {
		b = []byte(m.TextValue())
	}
	return vdbe.NewMemStr(strings.ToUpper(hex.EncodeToString(b)))
}

func unhexFunc1(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return unhexImpl(args[0], nil)
}

func unhexFunc2(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return unhexImpl(args[0], args[1])
}

func unhexImpl(hexArg, ignoreArg *vdbe.Mem) *vdbe.Mem {
	if hexArg.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	hexStr := hexArg.TextValue()
	if ignoreArg != nil && ignoreArg.Type != vdbe.MemNull {
		// Remove ignored characters from hex string
		ignoreChars := ignoreArg.TextValue()
		for _, c := range ignoreChars {
			hexStr = strings.ReplaceAll(hexStr, string(c), "")
		}
	}
	decoded, err := hex.DecodeString(hexStr)
	if err != nil {
		return vdbe.NewMemNull()
	}
	return vdbe.NewMemBlob(decoded)
}

func unicodeFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	m := args[0]
	if m.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	s := m.TextValue()
	if len(s) == 0 {
		return vdbe.NewMemNull()
	}
	r, _ := utf8.DecodeRuneInString(s)
	return vdbe.NewMemInt(int64(r))
}

func zeroblobFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	n, ok := toInt(args[0])
	if !ok {
		return vdbe.NewMemNull()
	}
	if n < 0 {
		n = 0
	}
	if n > 1000000000 {
		n = 1000000000
	}
	b := make([]byte, n)
	return vdbe.NewMemBlob(b)
}

func randomblobFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	n, ok := toInt(args[0])
	if !ok || n < 0 {
		n = 0
	}
	if n == 0 {
		n = 1
	}
	if n > 1000000000 {
		n = 1000000000
	}
	b := make([]byte, n)
	rand.Read(b)
	return vdbe.NewMemBlob(b)
}

func randomFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	var buf [8]byte
	rand.Read(buf[:])
	var val int64
	for i := 0; i < 8; i++ {
		val = (val << 8) | int64(buf[i])
	}
	// Clear sign bit for positive result like SQLite
	val &= 0x7FFFFFFFFFFFFFFF
	return vdbe.NewMemInt(val)
}

// --- Two-argument functions ---

func ifnullFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if args[0].Type != vdbe.MemNull {
		return args[0].Copy()
	}
	return args[1].Copy()
}

func nullifFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if vdbe.MemCompare(args[0], args[1]) == 0 {
		return vdbe.NewMemNull()
	}
	return args[0].Copy()
}

func instrFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if args[0].Type == vdbe.MemNull || args[1].Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}

	haystack := args[0].TextValue()
	needle := args[1].TextValue()

	// For blob-on-blob, do byte-level search
	if args[0].Type == vdbe.MemBlob && args[1].Type == vdbe.MemBlob {
		idx := bytes.Index(args[0].Bytes, args[1].Bytes)
		if idx < 0 {
			return vdbe.NewMemInt(0)
		}
		return vdbe.NewMemInt(int64(idx + 1))
	}

	idx := strings.Index(haystack, needle)
	if idx < 0 {
		return vdbe.NewMemInt(0)
	}
	// Count UTF-8 characters before the match
	charPos := utf8.RuneCountInString(haystack[:idx])
	return vdbe.NewMemInt(int64(charPos + 1))
}

func ltrimFunc1(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	m := args[0]
	if m.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	return vdbe.NewMemStr(strings.TrimLeft(m.TextValue(), " \t\n\r\x0b\x0c"))
}

func ltrimFunc2(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	m := args[0]
	if m.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	chars := " \t\n\r\x0b\x0c"
	if args[1].Type != vdbe.MemNull {
		chars = args[1].TextValue()
	}
	return vdbe.NewMemStr(strings.TrimLeft(m.TextValue(), chars))
}

func rtrimFunc1(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	m := args[0]
	if m.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	return vdbe.NewMemStr(strings.TrimRight(m.TextValue(), " \t\n\r\x0b\x0c"))
}

func rtrimFunc2(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	m := args[0]
	if m.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	chars := " \t\n\r\x0b\x0c"
	if args[1].Type != vdbe.MemNull {
		chars = args[1].TextValue()
	}
	return vdbe.NewMemStr(strings.TrimRight(m.TextValue(), chars))
}

func replaceFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if args[0].Type == vdbe.MemNull || args[1].Type == vdbe.MemNull || args[2].Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	str := args[0].TextValue()
	find := args[1].TextValue()
	repl := args[2].TextValue()
	return vdbe.NewMemStr(strings.ReplaceAll(str, find, repl))
}

// --- Three-argument functions ---

func substrFunc2(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return substrImpl(args[0], args[1], nil)
}

func substrFunc3(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return substrImpl(args[0], args[1], args[2])
}

func substrImpl(strMem, startMem, lenMem *vdbe.Mem) *vdbe.Mem {
	if strMem.Type == vdbe.MemNull || startMem.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	if lenMem != nil && lenMem.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}

	s := strMem.TextValue()
	start := startMem.IntValue()

	runes := []rune(s)
	runeCount := int64(len(runes))

	var length int64
	hasLen := lenMem != nil
	if hasLen {
		length = lenMem.IntValue()
	} else {
		length = runeCount
	}

	// Convert from 1-indexed to 0-indexed
	if start > 0 {
		start--
	} else if start == 0 {
		// start=0 is special: it means "before position 1"
		// For 3-arg, length counts from this phantom position
		if hasLen {
			if length <= 0 {
				return vdbe.NewMemStr("")
			}
			length--
		}
		start = 0 // 0-indexed position 0 = first char
	} else {
		// Negative start: count from end
		start = runeCount + start
		if start < 0 {
			// Overshot the beginning
			if hasLen {
				// 3-arg: reduce length by overshoot amount
				length += start
				if length < 0 {
					length = 0
				}
			}
			start = 0
		}
	}

	if length < 0 {
		// Negative length: take chars before start
		end := start
		start = start + length
		if start < 0 {
			start = 0
		}
		length = end - start
	}

	if start >= runeCount {
		return vdbe.NewMemStr("")
	}

	end := start + length
	if end > runeCount {
		end = runeCount
	}
	if end <= start {
		return vdbe.NewMemStr("")
	}

	return vdbe.NewMemStr(string(runes[start:end]))
}

func iifFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	cond := args[0]
	if cond.Bool() {
		return args[1].Copy()
	}
	return args[2].Copy()
}

// --- Variable-argument functions ---

func coalesceFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	for _, a := range args {
		if a.Type != vdbe.MemNull {
			return a.Copy()
		}
	}
	return vdbe.NewMemNull()
}

func charFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	var buf strings.Builder
	for _, a := range args {
		if a.Type == vdbe.MemNull {
			continue
		}
		v := a.IntValue()
		if v > 0 && v <= utf8.MaxRune {
			buf.WriteRune(rune(v))
		}
	}
	return vdbe.NewMemStr(buf.String())
}

func printfFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if len(args) == 0 {
		return vdbe.NewMemNull()
	}
	if args[0].Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	fmtStr := args[0].TextValue()

	// Build format arguments as interfaces
	fmtArgs := make([]interface{}, len(args)-1)
	for i := 1; i < len(args); i++ {
		switch args[i].Type {
		case vdbe.MemNull:
			fmtArgs[i-1] = "NULL"
		case vdbe.MemInt:
			fmtArgs[i-1] = args[i].IntVal
		case vdbe.MemFloat:
			fmtArgs[i-1] = args[i].FloatVal
		default:
			fmtArgs[i-1] = args[i].TextValue()
		}
	}

	result := sqlitePrintf(fmtStr, fmtArgs)
	return vdbe.NewMemStr(result)
}

// sqlitePrintf implements SQLite-style printf formatting.
// Supports %s, %d, %f, %!, %W, and standard fmt verbs.
func sqlitePrintf(format string, args []interface{}) string {
	var buf strings.Builder
	argIdx := 0
	i := 0
	for i < len(format) {
		if format[i] == '%' && i+1 < len(format) {
			i++
			// Handle %%
			if format[i] == '%' {
				buf.WriteByte('%')
				i++
				continue
			}
			// Parse flags and width
			for i < len(format) && (format[i] == '-' || format[i] == '+' || format[i] == ' ' || format[i] == '0' || format[i] == '#' || format[i] == '!') {
				i++
			}
			// Parse width
			for i < len(format) && format[i] >= '0' && format[i] <= '9' {
				i++
			}
			// Parse precision
			if i < len(format) && format[i] == '.' {
				i++
				for i < len(format) && format[i] >= '0' && format[i] <= '9' {
					i++
				}
			}
			// Get the verb
			if i < len(format) {
				verb := format[i]
				i++
				if argIdx < len(args) {
					arg := args[argIdx]
					argIdx++
					switch verb {
					case 's':
						buf.WriteString(fmt.Sprintf("%v", arg))
					case 'd', 'i':
						switch v := arg.(type) {
						case int64:
							buf.WriteString(fmt.Sprintf("%d", v))
						case float64:
							buf.WriteString(fmt.Sprintf("%d", int64(v)))
						default:
							buf.WriteString(fmt.Sprintf("%v", arg))
						}
					case 'f':
						switch v := arg.(type) {
						case float64:
							buf.WriteString(fmt.Sprintf("%f", v))
						case int64:
							buf.WriteString(fmt.Sprintf("%f", float64(v)))
						default:
							buf.WriteString(fmt.Sprintf("%v", arg))
						}
					case 'g':
						switch v := arg.(type) {
						case float64:
							buf.WriteString(fmt.Sprintf("%g", v))
						case int64:
							buf.WriteString(fmt.Sprintf("%g", float64(v)))
						default:
							buf.WriteString(fmt.Sprintf("%v", arg))
						}
					case 'c':
						switch v := arg.(type) {
						case int64:
							buf.WriteRune(rune(v))
						default:
							buf.WriteString(fmt.Sprintf("%c", arg))
						}
					case 'x':
						switch v := arg.(type) {
						case int64:
							buf.WriteString(fmt.Sprintf("%x", v))
						default:
							buf.WriteString(fmt.Sprintf("%x", arg))
						}
					case 'X':
						switch v := arg.(type) {
						case int64:
							buf.WriteString(fmt.Sprintf("%X", v))
						default:
							buf.WriteString(fmt.Sprintf("%X", arg))
						}
					default:
						buf.WriteString(fmt.Sprintf("%v", arg))
					}
				}
			}
		} else {
			buf.WriteByte(format[i])
			i++
		}
	}
	return buf.String()
}

// --- min/max scalar (non-aggregate) ---

func minScalarFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if len(args) == 0 {
		return vdbe.NewMemNull()
	}
	// If any argument is NULL, return NULL (SQLite behavior for non-aggregate min)
	best := 0
	for i, a := range args {
		if a.Type == vdbe.MemNull {
			return vdbe.NewMemNull()
		}
		if vdbe.MemCompare(args[best], a) > 0 {
			best = i
		}
	}
	return args[best].Copy()
}

func maxScalarFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if len(args) == 0 {
		return vdbe.NewMemNull()
	}
	// If any argument is NULL, return NULL (SQLite behavior for non-aggregate max)
	best := 0
	for i, a := range args {
		if a.Type == vdbe.MemNull {
			return vdbe.NewMemNull()
		}
		if vdbe.MemCompare(args[best], a) < 0 {
			best = i
		}
	}
	return args[best].Copy()
}

// --- LIKE and GLOB ---

func likeFunc2(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return likeImpl(args[0], args[1], vdbe.NewMemStr(""))
}

func likeFunc3(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return likeImpl(args[0], args[1], args[2])
}

func likeImpl(pattern, str, escapeMem *vdbe.Mem) *vdbe.Mem {
	if pattern.Type == vdbe.MemNull || str.Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	p := pattern.TextValue()
	s := str.TextValue()
	esc := ""
	if escapeMem != nil && escapeMem.Type != vdbe.MemNull {
		esc = escapeMem.TextValue()
	}

	result := likeMatch(p, s, esc)
	if result {
		return vdbe.NewMemInt(1)
	}
	return vdbe.NewMemInt(0)
}

// likeMatch implements SQL LIKE pattern matching.
// % matches any sequence; _ matches any single character.
func likeMatch(pattern, str, escape string) bool {
	escRune := rune(-1)
	if len(escape) > 0 {
		escRune, _ = utf8.DecodeRuneInString(escape)
	}

	return likeMatchRunes(
		[]rune(pattern),
		[]rune(str),
		0, 0,
		escRune,
		false,
	)
}

func likeMatchRunes(pattern, str []rune, pi, si int, esc rune, escaped bool) bool {
	for pi < len(pattern) {
		if !escaped && esc >= 0 && pattern[pi] == esc {
			pi++
			if pi >= len(pattern) {
				return false
			}
			escaped = true
			continue
		}
		ch := pattern[pi]
		if !escaped && ch == '%' {
			pi++
			// Skip consecutive %
			for pi < len(pattern) && pattern[pi] == '%' && !(esc >= 0 && pattern[pi] == esc) {
				pi++
			}
			if pi >= len(pattern) {
				return true
			}
			// Try matching rest at each position in str
			for si <= len(str) {
				if likeMatchRunes(pattern, str, pi, si, esc, false) {
					return true
				}
				si++
			}
			return false
		}
		if si >= len(str) {
			return false
		}
		if !escaped && ch == '_' {
			pi++
			si++
			escaped = false
			continue
		}
		if unicodeToLower(ch) != unicodeToLower(str[si]) {
			return false
		}
		pi++
		si++
		escaped = false
	}
	return si >= len(str)
}

func unicodeToLower(r rune) rune {
	if r >= 'A' && r <= 'Z' {
		return r + ('a' - 'A')
	}
	return r
}

// globFunc implements the GLOB function.
// * matches any sequence; ? matches any single character;
// [...] matches character classes.
func globFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if args[0].Type == vdbe.MemNull || args[1].Type == vdbe.MemNull {
		return vdbe.NewMemNull()
	}
	pattern := args[0].TextValue()
	str := args[1].TextValue()

	// Convert glob pattern to regex
	regex := globToRegex(pattern)
	matched, _ := regexp.MatchString("^"+regex+"$", str)
	if matched {
		return vdbe.NewMemInt(1)
	}
	return vdbe.NewMemInt(0)
}

func globToRegex(pattern string) string {
	var buf strings.Builder
	i := 0
	for i < len(pattern) {
		ch := pattern[i]
		switch ch {
		case '*':
			buf.WriteString(".*")
		case '?':
			buf.WriteString(".")
		case '[':
			// Pass through character class
			buf.WriteByte(ch)
			i++
			if i < len(pattern) && pattern[i] == '!' {
				buf.WriteString("^")
				i++
			} else if i < len(pattern) && pattern[i] == '^' {
				buf.WriteByte('^')
				i++
			}
			for i < len(pattern) && pattern[i] != ']' {
				buf.WriteByte(pattern[i])
				i++
			}
			if i < len(pattern) {
				buf.WriteByte(']')
			}
		case '.', '+', '(', ')', '|', '^', '$', '@', '%', '{', '}', '\\':
			buf.WriteByte('\\')
			buf.WriteByte(ch)
		default:
			buf.WriteByte(ch)
		}
		i++
	}
	return buf.String()
}
