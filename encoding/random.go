package encoding

import (
	crypto_rand "crypto/rand"
)

// RandomBytes fills buf with cryptographically random bytes.
func RandomBytes(buf []byte) error {
	_, err := crypto_rand.Read(buf)
	return err
}

// RandomInt returns a random int64.
func RandomInt() int64 {
	var buf [8]byte
	_, _ = crypto_rand.Read(buf[:])
	return int64(buf[0]) | int64(buf[1])<<8 | int64(buf[2])<<16 | int64(buf[3])<<24 |
		int64(buf[4])<<32 | int64(buf[5])<<40 | int64(buf[6])<<48 | int64(buf[7])<<56
}
