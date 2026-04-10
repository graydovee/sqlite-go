// Package compile implements the SQL compiler for sqlite-go.
// It tokenizes, parses, and generates VDBE bytecode from SQL statements.
package compile

// TokenType represents a SQL token type.
type TokenType int

const (
	TokenEOF          TokenType = iota
	TokenIllegal
	TokenWhitespace
	TokenComment
	TokenSemi
	TokenLParen
	TokenRParen
	TokenLBrace
	TokenRBrace
	TokenLBracket
	TokenRBracket
	TokenComma
	TokenDot
	TokenStar
	TokenEq
	TokenNe
	TokenLt
	TokenLe
	TokenGt
	TokenGe
	TokenPlus
	TokenMinus
	TokenSlash
	TokenRem
	TokenConcat      // ||
	TokenBitAnd
	TokenBitOr
	TokenBitNot      // ~
	TokenLShift
	TokenRShift
	TokenAssign      // =
	TokenInteger
	TokenFloat
	TokenString
	TokenVariable    // ? ?NN :AAA @AAA $AAA
	TokenKeyword
	TokenID          // identifier
	TokenBlob        // X'...'
)

// Token represents a SQL token.
type Token struct {
	Type    TokenType
	Value   string
	Line    int
	Col     int
}

// Keyword represents a SQL keyword.
type Keyword int

const (
	KwAbort       Keyword = iota
	KwAction
	KwAdd
	KwAfter
	KwAll
	KwAlter
	KwAlways
	KwAnalyze
	KwAnd
	KwAs
	KwAsc
	KwAttach
	KwAutoincrement
	KwBefore
	KwBegin
	KwBetween
	KwBy
	KwCascade
	KwCase
	KwCast
	KwCheck
	KwCollate
	KwColumn
	KwCommit
	KwConflict
	KwConstraint
	KwCreate
	KwCross
	KwCurrent
	KwCurrentDate
	KwCurrentTime
	KwCurrentTimestamp
	KwDatabase
	KwDefault
	KwDeferrable
	KwDeferred
	KwDelete
	KwDesc
	KwDetach
	KwDistinct
	KwDo
	KwDrop
	KwEach
	KwElse
	KwEnd
	KwEscape
	KwExcept
	KwExclude
	KwExclusive
	KwExists
	KwExplain
	KwFail
	KwFilter
	KwFirst
	KwFollowing
	KwFor
	KwForeign
	KwFrom
	KwFull
	KwGenerated
	KwGlob
	KwGroup
	KwGroups
	KwHaving
	KwIf
	KwIgnore
	KwImmediate
	KwIn
	KwIndex
	KwIndexed
	KwInitially
	KwInner
	KwInsert
	KwInstead
	KwIntersect
	KwInto
	KwIs
	KwIsnull
	KwJoin
	KwKey
	KwLast
	KwLeft
	KwLike
	KwLimit
	KwMatch
	KwMaterialized
	KwNatural
	KwNo
	KwNot
	KwNothing
	KwNotnull
	KwNull
	KwNulls
	KwOf
	KwOffset
	KwOn
	KwOr
	KwOrder
	KwOthers
	KwOuter
	KwOver
	KwPartition
	KwPlan
	KwPragma
	KwPreceding
	KwPrimary
	KwQuery
	KwRaise
	KwRange
	KwRecursive
	KwReferences
	KwRegexp
	KwReindex
	KwRelease
	KwRename
	KwReplace
	KwRestrict
	KwReturning
	KwRight
	KwRollback
	KwRow
	KwRows
	KwSavepoint
	KwSelect
	KwSet
	KwTable
	KwTemp
	KwTemporary
	KwThen
	KwTies
	KwTo
	KwTransaction
	KwTrigger
	KwUnbounded
	KwUnion
	KwUnique
	KwUpdate
	KwUsing
	KwVacuum
	KwValues
	KwView
	KwVirtual
	KwWhen
	KwWhere
	KwWindow
	KwWith
	KwWithout
)

// AST node types for SQL statements.
type StmtType int

const (
	StmtSelect      StmtType = iota
	StmtInsert
	StmtUpdate
	StmtDelete
	StmtCreateTable
	StmtCreateIndex
	StmtCreateView
	StmtCreateTrigger
	StmtDropTable
	StmtDropIndex
	StmtDropView
	StmtDropTrigger
	StmtAlterTable
	StmtBegin
	StmtCommit
	StmtRollback
	StmtSavepoint
	StmtRelease
	StmtPragma
	StmtVacuum
	StmtAttach
	StmtDetach
	StmtExplain
	StmtAnalyze
	StmtReindex
)
