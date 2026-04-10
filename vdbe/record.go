package vdbe

import (
	"encoding/binary"
	"fmt"
	"math"
)

// RecordBuilder constructs SQLite record-format data.
type RecordBuilder struct {
	values []Value
}

// Value holds a value for record encoding.
type Value struct {
	Type    string // "null", "int", "float", "text", "blob"
	IntVal  int64
	FloatVal float64
	Bytes   []byte
}

// NewRecordBuilder creates a new RecordBuilder.
func NewRecordBuilder() *RecordBuilder {
	return &RecordBuilder{}
}

// AddNull adds a NULL value.
func (rb *RecordBuilder) AddNull() *RecordBuilder {
	rb.values = append(rb.values, Value{Type: "null"})
	return rb
}

// AddInt adds an integer value.
func (rb *RecordBuilder) AddInt(v int64) *RecordBuilder {
	rb.values = append(rb.values, Value{Type: "int", IntVal: v})
	return rb
}

// AddFloat adds a float value.
func (rb *RecordBuilder) AddFloat(v float64) *RecordBuilder {
	rb.values = append(rb.values, Value{Type: "float", FloatVal: v})
	return rb
}

// AddText adds a text value.
func (rb *RecordBuilder) AddText(s string) *RecordBuilder {
	rb.values = append(rb.values, Value{Type: "text", Bytes: []byte(s)})
	return rb
}

// AddBlob adds a blob value.
func (rb *RecordBuilder) AddBlob(b []byte) *RecordBuilder {
	cp := make([]byte, len(b))
	copy(cp, b)
	rb.values = append(rb.values, Value{Type: "blob", Bytes: cp})
	return rb
}

// serialType returns the SQLite serial type for a value.
func serialType(v Value) int64 {
	switch v.Type {
	case "null":
		return 0
	case "int":
		i := v.IntVal
		if i == 0 {
			return 8
		}
		if i == 1 {
			return 9
		}
		if i >= -128 && i <= 127 {
			return 1
		}
		if i >= -32768 && i <= 32767 {
			return 2
		}
		if i >= -8388608 && i <= 8388607 {
			return 3
		}
		if i >= -2147483648 && i <= 2147483647 {
			return 4
		}
		if i >= -140737488355328 && i <= 140737488355327 {
			return 5
		}
		return 6
	case "float":
		return 7
	case "text":
		return 13 + int64(len(v.Bytes))*2
	case "blob":
		return 12 + int64(len(v.Bytes))*2
	}
	return 0
}

// serialTypeSize returns the byte size for a serial type.
func serialTypeSize(st int64) int {
	switch st {
	case 0, 8, 9:
		return 0
	case 1:
		return 1
	case 2:
		return 2
	case 3:
		return 3
	case 4:
		return 4
	case 5:
		return 6
	case 6, 7:
		return 8
	default:
		if st >= 12 {
			return int((st - 12) / 2)
		}
		return 0
	}
}

// putVarint writes a varint to buf, returns bytes written.
func putVarint(buf []byte, v int64) int {
	uv := uint64(v)
	if uv <= 127 {
		buf[0] = byte(uv)
		return 1
	}
	var tmp [9]byte
	n := 0
	for i := 8; i >= 0; i-- {
		tmp[i] = byte((uv & 0x7f) | 0x80)
		uv >>= 7
		n++
		if uv == 0 {
			tmp[8] &= 0x7f
			break
		}
	}
	copy(buf, tmp[9-n:])
	return n
}

// readVarint reads a varint from buf, returns value and bytes consumed.
func readVarint(buf []byte) (int64, int) {
	if len(buf) == 0 {
		return 0, 0
	}
	var v uint64
	for i := 0; i < 9 && i < len(buf); i++ {
		v = (v << 7) | uint64(buf[i]&0x7f)
		if buf[i]&0x80 == 0 {
			return int64(v), i + 1
		}
	}
	if len(buf) >= 9 {
		v = (v << 8) | uint64(buf[8])
		return int64(v), 9
	}
	return int64(v), len(buf)
}

// Build constructs the record bytes.
func (rb *RecordBuilder) Build() []byte {
	stypes := make([]int64, len(rb.values))
	var bodySize int
	for i, v := range rb.values {
		st := serialType(v)
		stypes[i] = st
		bodySize += serialTypeSize(st)
	}

	// Header: header_size (varint) + serial_types (varints)
	var headerContentLen int
	for _, st := range stypes {
		tmp := make([]byte, 9)
		headerContentLen += putVarint(tmp, st)
	}
	headerSize := putVarint(make([]byte, 9), int64(headerContentLen+1)) + headerContentLen

	total := headerSize + bodySize
	buf := make([]byte, total)
	pos := 0

	// Header size
	pos += putVarint(buf[pos:], int64(headerSize))
	// Serial types
	for _, st := range stypes {
		pos += putVarint(buf[pos:], st)
	}
	// Body
	for i, v := range rb.values {
		pos += encodeValueBody(buf[pos:], v, stypes[i])
	}

	return buf
}

func encodeValueBody(buf []byte, v Value, st int64) int {
	switch st {
	case 0, 8, 9:
		return 0
	case 1:
		buf[0] = byte(v.IntVal)
		return 1
	case 2:
		binary.BigEndian.PutUint16(buf, uint16(v.IntVal))
		return 2
	case 3:
		buf[0] = byte(v.IntVal >> 16)
		buf[1] = byte(v.IntVal >> 8)
		buf[2] = byte(v.IntVal)
		return 3
	case 4:
		binary.BigEndian.PutUint32(buf, uint32(v.IntVal))
		return 4
	case 5:
		buf[0] = byte(v.IntVal >> 40)
		buf[1] = byte(v.IntVal >> 32)
		buf[2] = byte(v.IntVal >> 24)
		buf[3] = byte(v.IntVal >> 16)
		buf[4] = byte(v.IntVal >> 8)
		buf[5] = byte(v.IntVal)
		return 6
	case 6:
		binary.BigEndian.PutUint64(buf, uint64(v.IntVal))
		return 8
	case 7:
		binary.BigEndian.PutUint64(buf, math.Float64bits(v.FloatVal))
		return 8
	default:
		if st >= 12 {
			copy(buf, v.Bytes)
			return len(v.Bytes)
		}
		return 0
	}
}

// ParseRecord decodes a SQLite record into Values.
func ParseRecord(data []byte) ([]Value, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty record")
	}
	headerSize, n := readVarint(data)
	if n == 0 {
		return nil, fmt.Errorf("invalid record")
	}
	hs := int(headerSize)
	if hs > len(data) {
		return nil, fmt.Errorf("header too large")
	}

	var stypes []int64
	pos := n
	for pos < hs {
		st, sn := readVarint(data[pos:])
		if sn == 0 {
			return nil, fmt.Errorf("invalid serial type")
		}
		stypes = append(stypes, st)
		pos += sn
	}

	bodyPos := hs
	values := make([]Value, len(stypes))
	for i, st := range stypes {
		sz := serialTypeSize(st)
		if bodyPos+sz > len(data) {
			return nil, fmt.Errorf("value overflow")
		}
		values[i] = decodeValueFrom(data[bodyPos:bodyPos+sz], st)
		bodyPos += sz
	}
	return values, nil
}

func decodeValueFrom(buf []byte, st int64) Value {
	switch {
	case st == 0:
		return Value{Type: "null"}
	case st == 8:
		return Value{Type: "int", IntVal: 0}
	case st == 9:
		return Value{Type: "int", IntVal: 1}
	case st == 1:
		return Value{Type: "int", IntVal: int64(int8(buf[0]))}
	case st == 2:
		return Value{Type: "int", IntVal: int64(int16(binary.BigEndian.Uint16(buf)))}
	case st == 3:
		v := int64(buf[0])<<16 | int64(buf[1])<<8 | int64(buf[2])
		if buf[0]&0x80 != 0 {
			v -= 1 << 24
		}
		return Value{Type: "int", IntVal: v}
	case st == 4:
		return Value{Type: "int", IntVal: int64(int32(binary.BigEndian.Uint32(buf)))}
	case st == 5:
		v := int64(buf[0])<<40 | int64(buf[1])<<32 | int64(buf[2])<<24 |
			int64(buf[3])<<16 | int64(buf[4])<<8 | int64(buf[5])
		if buf[0]&0x80 != 0 {
			v -= 1 << 48
		}
		return Value{Type: "int", IntVal: v}
	case st == 6:
		return Value{Type: "int", IntVal: int64(binary.BigEndian.Uint64(buf))}
	case st == 7:
		return Value{Type: "float", FloatVal: math.Float64frombits(binary.BigEndian.Uint64(buf))}
	case st >= 13 && st%2 == 1:
		return Value{Type: "text", Bytes: append([]byte{}, buf...)}
	case st >= 12 && st%2 == 0:
		return Value{Type: "blob", Bytes: append([]byte{}, buf...)}
	}
	return Value{Type: "null"}
}
