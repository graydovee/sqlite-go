package btree

import (
	"encoding/binary"
	"fmt"
	"math"
)

// SerialType represents a record serial type.
type SerialType int64

const (
	STNull    SerialType = 0
	STInt8    SerialType = 1
	STInt16   SerialType = 2
	STInt24   SerialType = 3
	STInt32   SerialType = 4
	STInt48   SerialType = 5
	STInt64   SerialType = 6
	STFloat64 SerialType = 7
	STZero    SerialType = 8
	STOne     SerialType = 9
)

const (
	STBlobBase SerialType = 12 // Blob of length N: serial type = 12 + 2*N
	STTextBase SerialType = 13 // Text of length N: serial type = 13 + 2*N
)

// SerialTypeSize returns the size in bytes for a given serial type.
func SerialTypeSize(st SerialType) int {
	switch st {
	case STNull, STZero, STOne:
		return 0
	case STInt8:
		return 1
	case STInt16:
		return 2
	case STInt24:
		return 3
	case STInt32:
		return 4
	case STInt48:
		return 6
	case STInt64:
		return 8
	case STFloat64:
		return 8
	default:
		if st >= STBlobBase {
			return int((st - STBlobBase) / 2)
		}
		if st >= STTextBase {
			return int((st - STTextBase) / 2)
		}
		return 0
	}
}

// Value represents a value in a record.
type Value struct {
	Type    string // "null", "int", "float", "text", "blob"
	IntVal  int64
	FloatVal float64
	Bytes   []byte
}

// NullValue returns a NULL value.
func NullValue() Value {
	return Value{Type: "null"}
}

// IntValue returns an integer value.
func IntValue(v int64) Value {
	return Value{Type: "int", IntVal: v}
}

// FloatValue returns a float value.
func FloatValue(v float64) Value {
	return Value{Type: "float", FloatVal: v}
}

// TextValue returns a text value.
func TextValue(s string) Value {
	return Value{Type: "text", Bytes: []byte(s)}
}

// BlobValue returns a blob value.
func BlobValue(b []byte) Value {
	return Value{Type: "blob", Bytes: b}
}

// valueSerialType returns the serial type for a value.
func valueSerialType(v Value) SerialType {
	switch v.Type {
	case "null":
		return STNull
	case "int":
		i := v.IntVal
		if i == 0 {
			return STZero
		}
		if i == 1 {
			return STOne
		}
		if i >= -128 && i <= 127 {
			return STInt8
		}
		if i >= -32768 && i <= 32767 {
			return STInt16
		}
		if i >= -8388608 && i <= 8388607 {
			return STInt24
		}
		if i >= -2147483648 && i <= 2147483647 {
			return STInt32
		}
		if i >= -140737488355328 && i <= 140737488355327 {
			return STInt48
		}
		return STInt64
	case "float":
		return STFloat64
	case "text":
		return STTextBase + SerialType(len(v.Bytes))*2
	case "blob":
		return STBlobBase + SerialType(len(v.Bytes))*2
	default:
		return STNull
	}
}

// MakeRecord encodes values into SQLite record format.
// Record format: header-size (varint) | serial-types (varints) | body
func MakeRecord(values []Value) []byte {
	// Calculate serial types and sizes
	serialTypes := make([]SerialType, len(values))
	var headerSize int
	var bodySize int

	for i, v := range values {
		st := valueSerialType(v)
		serialTypes[i] = st
		bodySize += SerialTypeSize(st)
	}

	// Calculate header size
	headerContentLen := 0
	for _, st := range serialTypes {
		headerContentLen += VarintLen(int64(st))
	}
	headerSize = VarintLen(int64(headerContentLen + 1)) + headerContentLen
	// Recalculate with correct header size
	headerSize = VarintLen(int64(headerSize)) + headerContentLen

	totalSize := headerSize + bodySize
	buf := make([]byte, totalSize)
	pos := 0

	// Write header size
	pos += PutVarint(buf[pos:], int64(headerSize))

	// Write serial types
	for _, st := range serialTypes {
		pos += PutVarint(buf[pos:], int64(st))
	}

	// Write body
	for i, st := range serialTypes {
		pos += encodeValue(buf[pos:], values[i], st)
	}

	return buf
}

// ParseRecord decodes a SQLite record into values.
func ParseRecord(data []byte) ([]Value, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty record")
	}

	// Read header size
	headerSize, n := ReadVarint(data)
	if n == 0 {
		return nil, fmt.Errorf("invalid record header size")
	}
	headerSizeInt := int(headerSize)
	if headerSizeInt > len(data) {
		return nil, fmt.Errorf("header size %d exceeds record length %d", headerSizeInt, len(data))
	}

	// Read serial types
	var serialTypes []SerialType
	pos := n
	for pos < headerSizeInt {
		st, sn := ReadVarint(data[pos:])
		if sn == 0 {
			return nil, fmt.Errorf("invalid serial type at position %d", pos)
		}
		serialTypes = append(serialTypes, SerialType(st))
		pos += sn
	}

	// Read values from body
	bodyPos := headerSizeInt
	values := make([]Value, len(serialTypes))
	for i, st := range serialTypes {
		size := SerialTypeSize(st)
		if bodyPos+size > len(data) {
			return nil, fmt.Errorf("value %d exceeds record boundary", i)
		}
		values[i] = decodeValue(st, data[bodyPos:bodyPos+size])
		bodyPos += size
	}

	return values, nil
}

// encodeValue writes a value to buf in the format specified by st.
func encodeValue(buf []byte, v Value, st SerialType) int {
	switch st {
	case STNull, STZero, STOne:
		return 0
	case STInt8:
		buf[0] = byte(v.IntVal)
		return 1
	case STInt16:
		binary.BigEndian.PutUint16(buf, uint16(v.IntVal))
		return 2
	case STInt24:
		buf[0] = byte(v.IntVal >> 16)
		buf[1] = byte(v.IntVal >> 8)
		buf[2] = byte(v.IntVal)
		return 3
	case STInt32:
		binary.BigEndian.PutUint32(buf, uint32(v.IntVal))
		return 4
	case STInt48:
		buf[0] = byte(v.IntVal >> 40)
		buf[1] = byte(v.IntVal >> 32)
		buf[2] = byte(v.IntVal >> 24)
		buf[3] = byte(v.IntVal >> 16)
		buf[4] = byte(v.IntVal >> 8)
		buf[5] = byte(v.IntVal)
		return 6
	case STInt64:
		binary.BigEndian.PutUint64(buf, uint64(v.IntVal))
		return 8
	case STFloat64:
		binary.BigEndian.PutUint64(buf, math.Float64bits(v.FloatVal))
		return 8
	default:
		if st >= STBlobBase || st >= STTextBase {
			copy(buf, v.Bytes)
			return len(v.Bytes)
		}
		return 0
	}
}

// decodeValue decodes a value from buf given serial type.
func decodeValue(st SerialType, buf []byte) Value {
	switch {
	case st == STNull:
		return NullValue()
	case st == STZero:
		return IntValue(0)
	case st == STOne:
		return IntValue(1)
	case st == STInt8:
		return IntValue(int64(int8(buf[0])))
	case st == STInt16:
		return IntValue(int64(int16(binary.BigEndian.Uint16(buf))))
	case st == STInt24:
		v := int64(buf[0])<<16 | int64(buf[1])<<8 | int64(buf[2])
		if buf[0]&0x80 != 0 {
			v -= 1 << 24
		}
		return IntValue(v)
	case st == STInt32:
		return IntValue(int64(int32(binary.BigEndian.Uint32(buf))))
	case st == STInt48:
		v := int64(buf[0])<<40 | int64(buf[1])<<32 | int64(buf[2])<<24 |
			int64(buf[3])<<16 | int64(buf[4])<<8 | int64(buf[5])
		if buf[0]&0x80 != 0 {
			v -= 1 << 48
		}
		return IntValue(v)
	case st == STInt64:
		return IntValue(int64(binary.BigEndian.Uint64(buf)))
	case st == STFloat64:
		return FloatValue(math.Float64frombits(binary.BigEndian.Uint64(buf)))
	case st >= STTextBase && st%2 == 1:
		b := make([]byte, len(buf))
		copy(b, buf)
		return Value{Type: "text", Bytes: b}
	case st >= STBlobBase && st%2 == 0:
		b := make([]byte, len(buf))
		copy(b, buf)
		return Value{Type: "blob", Bytes: b}
	default:
		return NullValue()
	}
}
