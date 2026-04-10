// Package sqlite provides the public API for sqlite-go.
package sqlite

// ErrorCode represents SQLite-compatible error codes.
type ErrorCode int

const (
	OK           ErrorCode = 0   // Successful result
	Error        ErrorCode = 1   // Generic error
	Internal     ErrorCode = 2   // Internal logic error
	Perm         ErrorCode = 3   // Access permission denied
	Abort        ErrorCode = 4   // Callback routine requested abort
	Busy         ErrorCode = 5   // The database file is locked
	Locked       ErrorCode = 6   // A table in the database is locked
	NoMem        ErrorCode = 7   // A malloc() failed
	ReadOnly     ErrorCode = 8   // Attempt to write a readonly database
	Interrupt    ErrorCode = 9   // Operation terminated by interrupt
	IOError      ErrorCode = 10  // Some kind of disk I/O error
	Corrupt      ErrorCode = 11  // The database disk image is malformed
	NotFound     ErrorCode = 12  // Unknown opcode in sqlite3_file_control()
	Full         ErrorCode = 13  // Insertion failed because database is full
	CantOpen     ErrorCode = 14  // Unable to open the database file
	Protocol     ErrorCode = 15  // Database lock protocol error
	Empty        ErrorCode = 16  // Internal use only
	Schema       ErrorCode = 17  // The database schema changed
	TooBig       ErrorCode = 18  // String or BLOB exceeds size limit
	Constraint   ErrorCode = 19  // Abort due to constraint violation
	Mismatch     ErrorCode = 20  // Data type mismatch
	Misuse       ErrorCode = 21  // Library used incorrectly
	NoLFS        ErrorCode = 22  // Uses OS features not supported on host
	Auth         ErrorCode = 23  // Authorization denied
	Format       ErrorCode = 24  // Not used
	Range        ErrorCode = 25  // 2nd parameter to bind out of range
	NotADB       ErrorCode = 26  // File opened that is not a database file
	Notice       ErrorCode = 27  // Notifications from sqlite3_log()
	Warning      ErrorCode = 28  // Warnings from sqlite3_log()
)

// Extended error codes.
const (
	IOErrorRead      ErrorCode = IOError | (1 << 8)
	IOErrorShortRead ErrorCode = IOError | (2 << 8)
	IOErrorWrite     ErrorCode = IOError | (3 << 8)
	IOErrorFsync     ErrorCode = IOError | (4 << 8)
	IOErrorDirFsync  ErrorCode = IOError | (5 << 8)
	IOErrorTruncate  ErrorCode = IOError | (6 << 8)
	IOErrorFStat     ErrorCode = IOError | (7 << 8)
	IOErrorUnlock    ErrorCode = IOError | (8 << 8)
	IOErrorRdlock    ErrorCode = IOError | (9 << 8)
	IOErrorDelete    ErrorCode = IOError | (10 << 8)
	IOErrorBlocked   ErrorCode = IOError | (11 << 8)
	IOErrorNoMem     ErrorCode = IOError | (12 << 8)
	IOErrorAccess    ErrorCode = IOError | (13 << 8)
	IOErrorCheckReservedLock ErrorCode = IOError | (14 << 8)
	IOErrorLock      ErrorCode = IOError | (15 << 8)
	IOErrorClose     ErrorCode = IOError | (16 << 8)
	IOErrorDirClose  ErrorCode = IOError | (17 << 8)
	IOErrorSHMOpen   ErrorCode = IOError | (18 << 8)
	IOErrorSHMSize   ErrorCode = IOError | (19 << 8)
	IOErrorSHMLock   ErrorCode = IOError | (20 << 8)
	IOErrorSHMMap    ErrorCode = IOError | (21 << 8)
	IOErrorSeek      ErrorCode = IOError | (22 << 8)
	IOErrorDeleteNoent ErrorCode = IOError | (23 << 8)
	IOErrorMmap      ErrorCode = IOError | (24 << 8)
	IOErrorGetTempPath ErrorCode = IOError | (25 << 8)
	IOErrorConvPath  ErrorCode = IOError | (26 << 8)
	IOErrorVnode     ErrorCode = IOError | (27 << 8)
	IOErrorAuth      ErrorCode = IOError | (28 << 8)
	IOErrorBeginAtomic ErrorCode = IOError | (29 << 8)
	IOErrorCommitAtomic ErrorCode = IOError | (30 << 8)
	IOErrorRollbackAtomic ErrorCode = IOError | (31 << 8)

	BusyRecovery    ErrorCode = Busy | (1 << 8)
	BusySnapshot    ErrorCode = Busy | (2 << 8)

	ConstraintPrimaryKey ErrorCode = Constraint | (6 << 8)
	ConstraintUnique   ErrorCode = Constraint | (8 << 8)
	ConstraintFK       ErrorCode = Constraint | (19 << 8)
	ConstraintCheck    ErrorCode = Constraint | (1 << 8)
	ConstraintNotNull  ErrorCode = Constraint | (5 << 8)
)

// ColumnType represents the declared type of a column.
type ColumnType int

const (
	ColInteger  ColumnType = iota
	ColFloat
	ColText
	ColBlob
	ColNull
)

// DataType represents SQLite data types (type affinity).
type DataType int

const (
	TypeBlob   DataType = iota
	TypeText
	TypeInteger
	TypeReal
	TypeNumeric
)

// ConfigOption represents configuration options.
type ConfigOption int

const (
	ConfigSingleThread  ConfigOption = 1
	ConfigMultiThread   ConfigOption = 2
	ConfigSerialized    ConfigOption = 3
	ConfigMalloc        ConfigOption = 4
	ConfigGetMalloc     ConfigOption = 5
	ConfigScratch       ConfigOption = 6
	ConfigPageCache     ConfigOption = 7
	ConfigHeap          ConfigOption = 8
	ConfigMemStatus     ConfigOption = 9
	ConfigMutex         ConfigOption = 10
	ConfigLookaside     ConfigOption = 11
	ConfigPCache        ConfigOption = 12
	ConfigGetPCache     ConfigOption = 13
	ConfigLog           ConfigOption = 14
	ConfigURI           ConfigOption = 15
	ConfigPCache2       ConfigOption = 16
	ConfigGetPCache2    ConfigOption = 17
	ConfigCoveringIndexScan ConfigOption = 18
	ConfigSQLLog        ConfigOption = 19
	ConfigMmapSize      ConfigOption = 22
	ConfigWin32HeapSize ConfigOption = 23
)

// OpenFlag represents database open flags.
type OpenFlag int

const (
	OpenReadWrite OpenFlag = 0x00000002
	OpenCreate    OpenFlag = 0x00000004
	OpenURI       OpenFlag = 0x00000040
	OpenMemory    OpenFlag = 0x00000080
	OpenNoFollow  OpenFlag = 0x01000000
)

// SQLiteError implements error interface for SQLite errors.
type SQLiteError struct {
	Code    ErrorCode
	ExtCode ErrorCode
	Msg     string
}

func (e *SQLiteError) Error() string {
	return e.Msg
}

// TraceCallback is called for SQL trace events.
type TraceCallback func(mask int, sql string)

// AuthorizerCallback is called to authorize SQL operations.
type AuthorizerCallback func(action int, arg1, arg2, arg3, arg4 string) ErrorCode

// CommitCallback is called on transaction commit.
type CommitCallback func() ErrorCode

// RollbackCallback is called on transaction rollback.
type RollbackCallback func()

// UpdateCallback is called on data change events.
type UpdateCallback func(action int, database, table string, rowid int64)

// WALCallback is called when WAL is checkpointed.
type WALCallback func(database string, pages int)
