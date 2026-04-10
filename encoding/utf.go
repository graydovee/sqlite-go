package encoding

import (
	"strings"
	"unicode/utf8"
)

// Utf8CharLen returns the expected character length based on the lead byte.
func Utf8CharLen(b byte) int {
	switch {
	case b < 0x80:
		return 1
	case b < 0xE0:
		return 2
	case b < 0xF0:
		return 3
	default:
		return 4
	}
}

// Utf8ReadRune reads a single UTF-8 rune from data.
func Utf8ReadRune(data []byte) (rune, int) {
	if len(data) == 0 {
		return 0, 0
	}
	r, size := utf8.DecodeRune(data)
	return r, size
}

// Utf8ToUpper converts a string to uppercase (Unicode-aware).
func Utf8ToUpper(s string) string {
	return strings.ToUpper(s)
}

// Utf8ToLower converts a string to lowercase (Unicode-aware).
func Utf8ToLower(s string) string {
	return strings.ToLower(s)
}

// Utf8CaseCmp performs a case-insensitive comparison of two strings.
// Returns -1, 0, or 1.
func Utf8CaseCmp(a, b string) int {
	aUpper := Utf8ToUpper(a)
	bUpper := Utf8ToUpper(b)

	if aUpper < bUpper {
		return -1
	}
	if aUpper > bUpper {
		return 1
	}
	return 0
}

// Utf8Valid returns whether the string is valid UTF-8.
func Utf8Valid(s string) bool {
	return utf8.ValidString(s)
}

// Utf8Len returns the number of runes in a byte slice.
func Utf8Len(data []byte) int {
	return utf8.RuneCount(data)
}

// Utf8Strlen30 returns the length of the string capped at int (SQLite's strlen30).
func Utf8Strlen30(s string) int {
	if len(s) > 0x3FFFFFFF {
		return 0x3FFFFFFF
	}
	return len(s)
}
