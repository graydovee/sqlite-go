package encoding

import (
	"fmt"
	"strings"
)

// Format implements printf-style formatting with SQLite-specific specifiers.
// Supported: %d, %s, %f, %Q, %q, %w, %c, %x, %o, %%, %T
// %Q: Quoted string (wrapped in single quotes, internal quotes escaped)
// %q: Escaped string (internal quotes doubled, not wrapped)
// %w: DDL-safe string (quotes doubled for use in SQL DDL)
func Format(format string, args ...interface{}) string {
	var sb strings.Builder
	argIdx := 0

	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			sb.WriteByte(format[i])
			continue
		}
		i++
		if i >= len(format) {
			sb.WriteByte('%')
			break
		}

		switch format[i] {
		case '%':
			sb.WriteByte('%')

		case 'd':
			if argIdx < len(args) {
				switch v := args[argIdx].(type) {
				case int:
					sb.WriteString(fmt.Sprintf("%d", v))
				case int64:
					sb.WriteString(fmt.Sprintf("%d", v))
				default:
					sb.WriteString(fmt.Sprintf("%v", v))
				}
				argIdx++
			}

		case 's':
			if argIdx < len(args) {
				sb.WriteString(fmt.Sprintf("%v", args[argIdx]))
				argIdx++
			}

		case 'f':
			if argIdx < len(args) {
				sb.WriteString(fmt.Sprintf("%f", args[argIdx]))
				argIdx++
			}

		case 'c':
			if argIdx < len(args) {
				switch v := args[argIdx].(type) {
				case int:
					sb.WriteRune(rune(v))
				case byte:
					sb.WriteByte(v)
				}
				argIdx++
			}

		case 'x':
			if argIdx < len(args) {
				sb.WriteString(fmt.Sprintf("%x", args[argIdx]))
				argIdx++
			}

		case 'o':
			if argIdx < len(args) {
				sb.WriteString(fmt.Sprintf("%o", args[argIdx]))
				argIdx++
			}

		case 'T':
			if argIdx < len(args) {
				sb.WriteString(fmt.Sprintf("%T", args[argIdx]))
				argIdx++
			}

		case 'Q':
			if argIdx < len(args) {
				s := fmt.Sprintf("%v", args[argIdx])
				escaped := strings.ReplaceAll(s, "'", "''")
				sb.WriteByte('\'')
				sb.WriteString(escaped)
				sb.WriteByte('\'')
				argIdx++
			}

		case 'q':
			if argIdx < len(args) {
				s := fmt.Sprintf("%v", args[argIdx])
				sb.WriteString(strings.ReplaceAll(s, "'", "''"))
				argIdx++
			}

		case 'w':
			if argIdx < len(args) {
				s := fmt.Sprintf("%v", args[argIdx])
				sb.WriteString(strings.ReplaceAll(s, "'", "''"))
				argIdx++
			}

		default:
			sb.WriteByte('%')
			sb.WriteByte(format[i])
		}
	}

	return sb.String()
}
