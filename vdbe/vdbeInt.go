package vdbe

import (
	"fmt"
	"strconv"
)

// Mem methods for serialization that use the SerialType from opcodes.go.

// IsNull checks if this Mem is null.
func (m *Mem) IsNullField() bool { return m.Type == MemNull || m.IsNull }

// IsInt returns true if the memory cell holds an integer.
func (m *Mem) IsInt() bool { return m.Type == MemInt }

// IsFloat returns true if the memory cell holds a float.
func (m *Mem) IsFloat() bool { return m.Type == MemFloat }

// IsText returns true if the memory cell holds text.
func (m *Mem) IsText() bool { return m.Type == MemStr }

// IsBlob returns true if the memory cell holds a blob.
func (m *Mem) IsBlob() bool { return m.Type == MemBlob }

// SetInt sets the memory cell to an integer value.
func (m *Mem) SetInt(v int64) {
	m.Type = MemInt
	m.IntVal = v
	m.Bytes = nil
	m.IsNull = false
}

// SetFloat sets the memory cell to a float value.
func (m *Mem) SetFloat(v float64) {
	m.Type = MemFloat
	m.FloatVal = v
	m.Bytes = nil
	m.IsNull = false
}

// SetText sets the memory cell to a text value.
func (m *Mem) SetText(v string) {
	m.Type = MemStr
	m.Bytes = []byte(v)
	m.IsNull = false
}

// SetBlob sets the memory cell to a blob value.
func (m *Mem) SetBlob(v []byte) {
	m.Type = MemBlob
	m.Bytes = make([]byte, len(v))
	copy(m.Bytes, v)
	m.IsNull = false
}

// SetNull sets the memory cell to NULL.
func (m *Mem) SetNull() {
	m.Type = MemNull
	m.IntVal = 0
	m.FloatVal = 0
	m.Bytes = nil
	m.IsNull = true
}

// TextValue returns the text value, performing type coercion if needed.
func (m *Mem) TextValue() string {
	switch m.Type {
	case MemStr:
		return string(m.Bytes)
	case MemInt:
		return strconv.FormatInt(m.IntVal, 10)
	case MemFloat:
		return strconv.FormatFloat(m.FloatVal, 'g', -1, 64)
	case MemNull:
		return ""
	default:
		return ""
	}
}

// SerialType returns the serial type for encoding this value in a record.
func (m *Mem) SerialType() SerialType {
	switch m.Type {
	case MemNull:
		return SerialNull
	case MemInt:
		v := m.IntVal
		if v == 0 {
			return SerialZero
		}
		if v == 1 {
			return SerialOne
		}
		if v >= -128 && v <= 127 {
			return SerialInt8
		}
		if v >= -32768 && v <= 32767 {
			return SerialInt16
		}
		if v >= -8388608 && v <= 8388607 {
			return SerialInt24
		}
		if v >= -2147483648 && v <= 2147483647 {
			return SerialInt32
		}
		if v >= -140737488355328 && v <= 140737488355327 {
			return SerialInt48
		}
		return SerialInt64
	case MemFloat:
		return SerialFloat64
	case MemStr:
		return SerialType(int(SerialText) + len(m.Bytes)*2)
	case MemBlob:
		return SerialType(int(SerialBlob) + len(m.Bytes)*2)
	default:
		return SerialNull
	}
}

// MakeMem creates a Mem from a Go value.
func MakeMem(v interface{}) *Mem {
	switch val := v.(type) {
	case nil:
		return NewMemNull()
	case int:
		return NewMemInt(int64(val))
	case int64:
		return NewMemInt(val)
	case float64:
		return NewMemFloat(val)
	case string:
		return NewMemStr(val)
	case []byte:
		return NewMemBlob(val)
	case bool:
		if val {
			return NewMemInt(1)
		}
		return NewMemInt(0)
	default:
		return NewMemNull()
	}
}

// MemFromValue converts a record Value to a Mem.
func MemFromValue(v Value) *Mem {
	switch v.Type {
	case "null":
		return NewMemNull()
	case "int":
		return NewMemInt(v.IntVal)
	case "float":
		return NewMemFloat(v.FloatVal)
	case "text":
		return NewMemStr(string(v.Bytes))
	case "blob":
		return NewMemBlob(v.Bytes)
	default:
		return NewMemNull()
	}
}

// String returns a string representation of the memory cell.
func (m *Mem) String() string {
	switch m.Type {
	case MemNull:
		return "NULL"
	case MemInt:
		return fmt.Sprintf("%d", m.IntVal)
	case MemFloat:
		return fmt.Sprintf("%g", m.FloatVal)
	case MemStr:
		return string(m.Bytes)
	case MemBlob:
		return fmt.Sprintf("blob(%d)", len(m.Bytes))
	default:
		return "undefined"
	}
}
