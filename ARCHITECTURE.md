# sqlite-go Architecture

Pure Go implementation of SQLite, referencing the C source at `../sqlite/`.

## Project Structure

```
sqlite-go/
├── README.md
├── ARCHITECTURE.md
├── go.mod
├── go.sum
├── vfs/                    # Layer 1: OS Interface (Virtual File System)
│   ├── vfs.go             # VFS interface + registry
│   ├── file.go            # File abstraction
│   ├── unixvfs.go         # Unix VFS implementation
│   └── vfs_test.go
├── pager/                  # Layer 2: Pager (Page Cache + Transaction Log)
│   ├── pager.go           # Pager interface + core implementation
│   ├── wal.go             # Write-Ahead Log
│   ├── journal.go         # Rollback journal
│   ├── pcache.go          # Page cache
│   └── pager_test.go
├── btree/                  # Layer 3: B-Tree Engine
│   ├── btree.go           # B-Tree interface + core
│   ├── btreeInt.go        # Internal structures
│   ├── cursor.go          # B-Tree cursor
│   └── btree_test.go
├── vdbe/                   # Layer 4: Virtual Database Engine
│   ├── vdbe.go            # VM main loop + opcode dispatch
│   ├── opcodes.go         # All VDBE opcodes
│   ├── vdbeInt.go         # Internal structures (Mem, Cursor, etc.)
│   ├── vdbeapi.go         # Public API for VDBE
│   ├── vdbeaux.go         # Auxiliary functions
│   └── vdbe_test.go
├── compile/                # Layer 5: SQL Compiler
│   ├── tokenize.go        # Tokenizer (lexer)
│   ├── parser.go          # SQL parser (generated or hand-written)
│   ├── ast.go             # AST node definitions
│   ├── build.go           # AST → VDBE bytecode
│   ├── resolve.go         # Name resolution
│   ├── expr.go            # Expression handling
│   ├── select.go          # SELECT compilation
│   ├── codegen.go         # Code generation helpers
│   └── compile_test.go
├── func/                   # Layer 6: Built-in Functions
│   ├── builtin.go         # Built-in function registry
│   ├── scalar.go          # Scalar functions (abs, coalesce, etc.)
│   ├── aggregate.go       # Aggregate functions (sum, count, avg, etc.)
│   ├── date.go            # Date/time functions
│   ├── string.go          # String functions
│   ├── math.go            # Math functions
│   └── func_test.go
├── sql/                    # Layer 7: SQL Engine (upper-level features)
│   ├── analyze.go         # ANALYZE
│   ├── attach.go          # ATTACH/DETACH
│   ├── auth.go            # Authorization
│   ├── backup.go          # Online backup
│   ├── pragma.go          # PRAGMA implementation
│   ├── trigger.go         # Triggers
│   ├── vacuum.go          # VACUUM
│   ├── alter.go           # ALTER TABLE
│   ├── upsert.go          # UPSERT
│   ├── window.go          # Window functions
│   ├── json.go            # JSON extension
│   └── sql_test.go
├── sqlite/                 # Layer 8: Public API
│   ├── sqlite.go          # Main database connection API
│   ├── stmt.go            # Prepared statement API
│   ├── result.go          # Result types
│   ├── error.go           # Error codes + mapping
│   ├── config.go          # Configuration
│   └── sqlite_test.go
├── encoding/               # Utilities
│   ├── utf.go             # UTF-8/16 handling
│   ├── printf.go          # printf implementation
│   ├── hash.go            # Hash table
│   ├── bitvec.go          # Bit vector
│   ├── rowset.go          # Row set
│   ├── random.go          # Random number generation
│   └── encoding_test.go
└── tests/                  # Integration tests (ported from C test suite)
    ├── integration_test.go
    ├── fuzz_test.go
    └── compat/            # Compatibility test runner
        └── runner.go
```

## Layer Dependencies (bottom-up)

```
vfs/  ←  pager/  ←  btree/  ←  vdbe/  ←  compile/  ←  sql/  ←  sqlite/
                ↘                                  ↗
                  encoding/  ←  func/
```

Each layer only depends on the layers below it. No circular dependencies.

## Implementation Priority

Phase 1 (Foundation): vfs → pager → btree
Phase 2 (Core Engine): vdbe → compile (tokenizer + parser)
Phase 3 (Features): func → sql features
Phase 4 (API): sqlite public API
Phase 5 (Testing): Comprehensive test suite

## Conventions

- All types use Go naming conventions (PascalCase for exported)
- Error codes map to SQLite error codes (SQLITE_OK, SQLITE_ERROR, etc.)
- Thread safety via sync.Mutex where needed
- All file I/O through VFS abstraction
