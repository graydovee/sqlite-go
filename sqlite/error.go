package sqlite

import "fmt"

// ErrorCodeString returns a human-readable string for an ErrorCode.
func ErrorCodeString(code ErrorCode) string {
	names := map[ErrorCode]string{
		OK:         "SQLITE_OK",
		Error:      "SQLITE_ERROR",
		Internal:   "SQLITE_INTERNAL",
		Perm:       "SQLITE_PERM",
		Abort:      "SQLITE_ABORT",
		Busy:       "SQLITE_BUSY",
		Locked:     "SQLITE_LOCKED",
		NoMem:      "SQLITE_NOMEM",
		ReadOnly:   "SQLITE_READONLY",
		Interrupt:  "SQLITE_INTERRUPT",
		IOError:    "SQLITE_IOERR",
		Corrupt:    "SQLITE_CORRUPT",
		NotFound:   "SQLITE_NOTFOUND",
		Full:       "SQLITE_FULL",
		CantOpen:   "SQLITE_CANTOPEN",
		Protocol:   "SQLITE_PROTOCOL",
		Empty:      "SQLITE_EMPTY",
		Schema:     "SQLITE_SCHEMA",
		TooBig:     "SQLITE_TOOBIG",
		Constraint: "SQLITE_CONSTRAINT",
		Mismatch:   "SQLITE_MISMATCH",
		Misuse:     "SQLITE_MISUSE",
		NoLFS:      "SQLITE_NOLFS",
		Auth:       "SQLITE_AUTH",
		Format:     "SQLITE_FORMAT",
		Range:      "SQLITE_RANGE",
		NotADB:     "SQLITE_NOTADB",
		Notice:     "SQLITE_NOTICE",
		Warning:    "SQLITE_WARNING",
	}
	if s, ok := names[code]; ok {
		return s
	}
	return fmt.Sprintf("SQLITE_UNKNOWN(%d)", code)
}

// IsError returns true if the ErrorCode represents an error (non-OK).
func IsError(code ErrorCode) bool {
	return code != OK
}

// IsBusy returns true if the error code indicates a busy/locked condition.
func IsBusy(code ErrorCode) bool {
	return code == Busy || code == BusyRecovery || code == BusySnapshot || code == Locked
}

// IsIOError returns true if the error code is an I/O error (primary or extended).
func IsIOError(code ErrorCode) bool {
	return code == IOError || (code&0xFF) == IOError
}

// IsConstraint returns true if the error code is a constraint violation.
func IsConstraint(code ErrorCode) bool {
	return code == Constraint || (code&0xFF) == Constraint
}

// NewError creates a new SQLiteError with the given code and message.
func NewError(code ErrorCode, msg string) *SQLiteError {
	return &SQLiteError{
		Code:    code,
		ExtCode: code,
		Msg:     fmt.Sprintf("%s: %s", ErrorCodeString(code), msg),
	}
}

// NewErrorf creates a new SQLiteError with a formatted message.
func NewErrorf(code ErrorCode, format string, args ...interface{}) *SQLiteError {
	return &SQLiteError{
		Code:    code,
		ExtCode: code,
		Msg:     fmt.Sprintf("%s: %s", ErrorCodeString(code), fmt.Sprintf(format, args...)),
	}
}
