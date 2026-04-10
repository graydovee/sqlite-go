# sqlite-go

Pure Go implementation of SQLite — no CGO required.

A complete, production-grade reimplementation of SQLite in Go, based on the original C source code.

## Architecture

```
┌─────────────────────────┐
│    API Layer (database/sql compatible)  │
├─────────────────────────┤
│    SQL Compiler                      │
│    (Tokenizer → Parser → Codegen)    │
├─────────────────────────┤
│    VDBE (Virtual Machine)            │
├─────────────────────────┤
│    B-Tree Engine                     │
├─────────────────────────┤
│    Pager (Page Cache)                │
├─────────────────────────┤
│    OS Interface (VFS)                │
└─────────────────────────┘
```

## Status

🚧 Under active development
