package btree

import "io"

// PutVarint encodes a 64-bit integer into a SQLite variable-length integer.
// Returns the number of bytes written.
func PutVarint(buf []byte, v int64) int {
	uv := uint64(v)
	if uv <= 127 {
		if len(buf) > 0 {
			buf[0] = byte(uv)
		}
		return 1
	}
	if uv <= 16383 {
		if len(buf) >= 2 {
			buf[0] = byte((uv >> 7) | 0x80)
			buf[1] = byte(uv & 0x7f)
		}
		return 2
	}

	// 9-byte case: values with high byte set (negative int64)
	if uv&0xff00000000000000 != 0 {
		if len(buf) >= 9 {
			buf[8] = byte(uv)
			uv >>= 8
			for i := 7; i >= 0; i-- {
				buf[i] = byte((uv & 0x7f) | 0x80)
				uv >>= 7
			}
		}
		return 9
	}

	// General case: 3-8 bytes
	var tmp [9]byte
	n := 0
	for i := 8; i >= 0; i-- {
		tmp[i] = byte((uv & 0x7f) | 0x80)
		uv >>= 7
		n++
		if uv == 0 {
			tmp[8] &= 0x7f // Clear high bit of last byte
			break
		}
	}
	copy(buf, tmp[9-n:])
	return n
}

// ReadVarint reads a SQLite variable-length integer from buf.
// Returns the value and the number of bytes consumed.
func ReadVarint(buf []byte) (int64, int) {
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

	// 9th byte: all 8 bits are used
	if len(buf) >= 9 {
		v = (v << 8) | uint64(buf[8])
		return int64(v), 9
	}

	return int64(v), len(buf)
}

// VarintLen returns the number of bytes needed to encode v.
func VarintLen(v int64) int {
	buf := make([]byte, 9)
	return PutVarint(buf, v)
}

// ReadVarintFrom reads a varint from an io.Reader.
func ReadVarintFrom(r io.Reader) (int64, error) {
	var buf [9]byte
	var v uint64
	for i := 0; i < 9; i++ {
		if _, err := r.Read(buf[i : i+1]); err != nil {
			return 0, err
		}
		v = (v << 7) | uint64(buf[i]&0x7f)
		if buf[i]&0x80 == 0 {
			return int64(v), nil
		}
	}
	// 9th byte uses all 8 bits
	v = (v << 8) | uint64(buf[8])
	return int64(v), nil
}
