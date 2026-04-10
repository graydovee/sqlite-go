package vdbe

import (
	"fmt"
	"strconv"
)

// Mem represents a VDBE memory cell (register).
type Mem struct {
	Type    MemType
	IntVal  int64
	FloatVal float64
	Bytes   []byte
	ENC     string // Text encoding
	Subtype byte
}

// NewMem creates a new NULL memory cell.
func NewMem() *Mem {
	return &Mem{Type: MemNull}
}

// IsNull returns true if the memory cell is NULL.
func (m *Mem) IsNull() bool { return m.Type == MemNull }

// IsInt returns true if the memory cell holds an integer.
func (m *Mem) IsInt() bool { return m.Type == MemInt }

// IsFloat returns true if the memory cell holds a float.
func (m *Mem) IsFloat() bool { return m.Type == MemFloat }

// IsText returns true if the memory cell holds text.
func (m *Mem) IsText() bool { return m.Type == MemText }

// IsBlob returns true if the memory cell holds a blob.
func (m *Mem) IsBlob() bool { return m.Type == MemBlob }

// SetInt sets the memory cell to an integer value.
func (m *Mem) SetInt(v int64) {
	m.Type = MemInt
	m.IntVal = v
	m.Bytes = nil
}

// SetFloat sets the memory cell to a float value.
func (m *Mem) SetFloat(v float64) {
	m.Type = MemFloat
	m.FloatVal = v
	m.Bytes = nil
}

// SetText sets the memory cell to a text value.
func (m *Mem) SetText(v string) {
	m.Type = MemText
	m.Bytes = []byte(v)
}

// SetBlob sets the memory cell to a blob value.
func (m *Mem) SetBlob(v []byte) {
	m.Type = MemBlob
	m.Bytes = make([]byte, len(v))
	copy(m.Bytes, v)
}

// SetNull sets the memory cell to NULL.
func (m *Mem) SetNull() {
	m.Type = MemNull
	m.IntVal = 0
	m.FloatVal = 0
	m.Bytes = nil
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
	case MemText:
		return string(m.Bytes)
	case MemBlob:
		return fmt.Sprintf("blob(%d)", len(m.Bytes))
	default:
		return "undefined"
	}
}

// IntValue returns the integer value, performing type coercion if needed.
func (m *Mem) IntValue() int64 {
	switch m.Type {
	case MemInt:
		return m.IntVal
	case MemFloat:
		return int64(m.FloatVal)
	case MemText:
		// Try to parse as integer
		n, err := strconv.ParseInt(string(m.Bytes), 10, 64)
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}

// FloatValue returns the float value, performing type coercion if needed.
func (m *Mem) FloatValue() float64 {
	switch m.Type {
	case MemFloat:
		return m.FloatVal
	case MemInt:
		return float64(m.IntVal)
	case MemText:
		n, err := strconv.ParseFloat(string(m.Bytes), 64)
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}

// TextValue returns the text value, performing type coercion if needed.
func (m *Mem) TextValue() string {
	switch m.Type {
	case MemText:
		return string(m.Bytes)
	case MemInt:
		return strconv.FormatInt(m.IntVal, 10)
	case MemFloat:
		return strconv.FormatFloat(m.FloatVal, 'g', -1, 64)
	case MemNull:
		return "NULL"
	default:
		return ""
	}
}

// BlobValue returns the blob value.
func (m *Mem) BlobValue() []byte {
	switch m.Type {
	case MemBlob, MemText:
		return m.Bytes
	case MemNull:
		return nil
	default:
		return []byte(m.TextValue())
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
	case MemText:
		return SerialType(int(SerialText) + len(m.Bytes)*2)
	case MemBlob:
		return SerialType(int(SerialBlob) + len(m.Bytes)*2)
	default:
		return SerialNull
	}
}
