package vdbe

import (
	"fmt"
	"math"
	"strings"
)

// Mem represents a value in the VDBE register.
type Mem struct {
	Type MemType
	// Scalar values
	IntVal   int64
	FloatVal float64
	// Variable-length
	Bytes []byte
	// Pointer holds an arbitrary Go value for aggregate context, RowSet, etc.
	Pointer interface{}
	// Flags
	IsNull    bool
	IsRowID   bool
	IsZero    bool
}

// MemType represents the type of a VDBE memory cell.
type MemType int

const (
	MemNull MemType = iota
	MemInt
	MemFloat
	MemStr
	MemBlob
)

// NewMemInt creates an integer Mem.
func NewMemInt(v int64) *Mem {
	return &Mem{Type: MemInt, IntVal: v}
}

// NewMemFloat creates a float Mem.
func NewMemFloat(v float64) *Mem {
	return &Mem{Type: MemFloat, FloatVal: v}
}

// NewMemStr creates a string Mem.
func NewMemStr(s string) *Mem {
	return &Mem{Type: MemStr, Bytes: []byte(s)}
}

// NewMemBlob creates a blob Mem.
func NewMemBlob(b []byte) *Mem {
	cp := make([]byte, len(b))
	copy(cp, b)
	return &Mem{Type: MemBlob, Bytes: cp}
}

// NewMemNull creates a null Mem.
func NewMemNull() *Mem {
	return &Mem{Type: MemNull, IsNull: true}
}

// IsNumeric returns true if the value is numeric.
func (m *Mem) IsNumeric() bool {
	return m.Type == MemInt || m.Type == MemFloat
}

// IntValue returns the integer value, converting if needed.
func (m *Mem) IntValue() int64 {
	switch m.Type {
	case MemInt:
		return m.IntVal
	case MemFloat:
		return int64(m.FloatVal)
	case MemStr:
		// Try to parse string as integer
		var v int64
		fmt.Sscanf(string(m.Bytes), "%d", &v)
		return v
	}
	return 0
}

// FloatValue returns the float value, converting if needed.
func (m *Mem) FloatValue() float64 {
	switch m.Type {
	case MemFloat:
		return m.FloatVal
	case MemInt:
		return float64(m.IntVal)
	case MemStr:
		var v float64
		fmt.Sscanf(string(m.Bytes), "%f", &v)
		return v
	}
	return 0
}

// StringValue returns the string representation.
func (m *Mem) StringValue() string {
	switch m.Type {
	case MemNull:
		return ""
	case MemInt:
		return fmt.Sprintf("%d", m.IntVal)
	case MemFloat:
		return fmt.Sprintf("%g", m.FloatVal)
	case MemStr, MemBlob:
		return string(m.Bytes)
	}
	return ""
}

// BlobValue returns the bytes.
func (m *Mem) BlobValue() []byte {
	if m.Type == MemBlob || m.Type == MemStr {
		return m.Bytes
	}
	return nil
}

// Bool returns true for non-zero/non-null values.
func (m *Mem) Bool() bool {
	switch m.Type {
	case MemNull:
		return false
	case MemInt:
		return m.IntVal != 0
	case MemFloat:
		return m.FloatVal != 0
	case MemStr, MemBlob:
		return len(m.Bytes) > 0
	}
	return false
}

// Compare compares two Mem values. Returns -1, 0, or 1.
func MemCompare(a, b *Mem) int {
	// NULL handling: NULL < everything
	if a.Type == MemNull && b.Type == MemNull {
		return 0
	}
	if a.Type == MemNull {
		return -1
	}
	if b.Type == MemNull {
		return 1
	}

	// Both numeric
	if a.IsNumeric() && b.IsNumeric() {
		return compareNumeric(a, b)
	}

	// Mixed: try to coerce to numeric
	if a.Type == MemStr && b.IsNumeric() {
		if f, ok := tryParseFloat(a); ok {
			af := &Mem{Type: MemFloat, FloatVal: f}
			return compareNumeric(af, b)
		}
	}
	if b.Type == MemStr && a.IsNumeric() {
		if f, ok := tryParseFloat(b); ok {
			bf := &Mem{Type: MemFloat, FloatVal: f}
			return compareNumeric(a, bf)
		}
	}

	// Both strings or blob
	if (a.Type == MemStr || a.Type == MemBlob) && (b.Type == MemStr || b.Type == MemBlob) {
		return compareBytes(a.Bytes, b.Bytes)
	}

	// Fallback
	return 0
}

func compareNumeric(a, b *Mem) int {
	if a.Type == MemInt && b.Type == MemInt {
		if a.IntVal < b.IntVal {
			return -1
		}
		if a.IntVal > b.IntVal {
			return 1
		}
		return 0
	}
	af, bf := a.FloatValue(), b.FloatValue()
	if af < bf {
		return -1
	}
	if af > bf {
		return 1
	}
	return 0
}

func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

func tryParseFloat(m *Mem) (float64, bool) {
	if m.Type == MemFloat {
		return m.FloatVal, true
	}
	if m.Type == MemInt {
		return float64(m.IntVal), true
	}
	s := strings.TrimSpace(string(m.Bytes))
	if s == "" {
		return 0, false
	}
	// Check if it looks numeric
	hasDigit := false
	for _, c := range s {
		if c >= '0' && c <= '9' {
			hasDigit = true
			break
		}
	}
	if !hasDigit {
		return 0, false
	}
	var f float64
	_, err := fmt.Sscanf(s, "%f", &f)
	return f, err == nil
}

// IsNaN checks if a float value is NaN.
func (m *Mem) IsNaN() bool {
	return m.Type == MemFloat && math.IsNaN(m.FloatVal)
}

// Copy creates a deep copy of a Mem.
func (m *Mem) Copy() *Mem {
	cp := &Mem{
		Type:     m.Type,
		IntVal:   m.IntVal,
		FloatVal: m.FloatVal,
		IsNull:   m.IsNull,
		IsRowID:  m.IsRowID,
		IsZero:   m.IsZero,
		Pointer:  m.Pointer, // shared reference is intentional for aggregate state
	}
	if m.Bytes != nil {
		cp.Bytes = make([]byte, len(m.Bytes))
		copy(cp.Bytes, m.Bytes)
	}
	return cp
}

// SetPointer sets the memory cell to hold an arbitrary Go pointer value.
func (m *Mem) SetPointer(val interface{}) {
	m.Type = MemNull
	m.IsNull = true
	m.Pointer = val
	m.Bytes = nil
	m.IntVal = 0
	m.FloatVal = 0
}
