// Package compile implements the SQL compiler for sqlite-go.
// It tokenizes, parses, and generates VDBE bytecode from SQL statements.

package compile

// =============================================================================
// Expression types
// =============================================================================

// ExprKind identifies the type of an expression node.
type ExprKind int

const (
	ExprLiteral      ExprKind = iota // Integer, float, string, blob, or NULL
	ExprVariable                     // ? ?NN :AAA @AAA $AAA
	ExprColumnRef                    // column or table.column or db.table.column
	ExprBinaryOp                     // +, -, *, /, %, ||, &, |, <<, >>, AND, OR, =, <, >, <=, >=, !=, <>, IS, IS NOT
	ExprUnaryOp                      // -, +, ~, NOT
	ExprFunctionCall                 // func(args) or func(DISTINCT args)
	ExprSubquery                     // (SELECT ...)
	ExprExists                       // EXISTS (SELECT ...)
	ExprInList                       // expr IN (expr, ...) or expr NOT IN (expr, ...)
	ExprInSelect                     // expr IN (SELECT ...) or expr NOT IN (SELECT ...)
	ExprInTable                      // expr IN table or expr NOT IN table
	ExprBetween                      // expr BETWEEN low AND high or expr NOT BETWEEN ...
	ExprLike                         // expr LIKE pattern [ESCAPE esc] or NOT LIKE
	ExprGlob                         // expr GLOB pattern or NOT GLOB
	ExprMatch                        // expr MATCH pattern or NOT MATCH
	ExprRegexp                       // expr REGEXP pattern or NOT REGEXP
	ExprIsNull                       // expr IS NULL
	ExprIsNotNull                    // expr IS NOT NULL
	ExprCase                         // CASE [operand] WHEN ... THEN ... [ELSE ...] END
	ExprCast                         // CAST(expr AS type)
	ExprCollate                      // expr COLLATE collation_name
	ExprDot                          // table.column (used internally for star expansion)
	ExprStar                         // * in SELECT list
	ExprDefault                      // DEFAULT keyword value
)

// Expr represents a SQL expression.
type Expr struct {
	Kind ExprKind

	// Literal fields
	IntValue    int64   // set for integer literals
	FloatValue  float64 // set for float literals
	StringValue string  // set for string, blob, and identifier literals; also variable names
	LiteralType string  // "integer", "float", "string", "blob", "null", "true", "false"

	// Binary/unary operator fields
	Op    string // The operator: "+", "-", "*", "/", etc.
	Left  *Expr  // Left operand (nil for unary)
	Right *Expr  // Right operand (nil for unary)

	// Column reference: Name, Table, Database (all optional for qualified refs)
	Database string // Database name qualifier
	Table    string // Table name qualifier
	Name     string // Column/identifier name

	// Function call
	FunctionName string  // Name of the function
	Distinct     bool    // SELECT DISTINCT inside function args
	Args         []*Expr // Function arguments (nil for func(*))
	StarArg      bool    // true for COUNT(*) etc.

	// Subquery / EXISTS
	Select *SelectStmt

	// IN list
	InValues []*Expr // List of values for IN (val1, val2, ...)
	Not      bool    // true for NOT IN, NOT BETWEEN, NOT LIKE, etc.

	// IN subquery
	InSelect *SelectStmt

	// IN table reference
	InTable string // Table name for expr IN table

	// BETWEEN
	Low  *Expr // Lower bound
	High *Expr // Upper bound

	// LIKE/GLOB/MATCH/REGEXP
	Pattern *Expr // Pattern expression
	Escape  *Expr // Optional ESCAPE expression

	// CASE
	Operand  *Expr   // Optional CASE operand (nil for simple CASE)
	WhenList []*WhenClause
	ElseExpr *Expr // Optional ELSE expression

	// CAST
	CastType string // Target type name

	// COLLATE
	Collation string // Collation name

	// Parenthesized expression marker
	Parenthesized bool // true if the expression was wrapped in parens
}

// WhenClause represents a WHEN condition THEN result pair in a CASE expression.
type WhenClause struct {
	Condition *Expr // WHEN condition
	Result    *Expr // THEN result
}

// =============================================================================
// Statement types
// =============================================================================

// Statement represents a parsed SQL statement.
type Statement struct {
	Type     StmtType
	Location string // file or context info (optional)

	// Exactly one of the following will be set based on Type.
	SelectStmt     *SelectStmt
	InsertStmt     *InsertStmt
	UpdateStmt     *UpdateStmt
	DeleteStmt     *DeleteStmt
	CreateTable    *CreateTableStmt
	CreateIndex    *CreateIndexStmt
	DropTable      *DropTableStmt
	DropIndex      *DropIndexStmt
	BeginStmt      *BeginStmt
	CommitStmt     *CommitStmt
	RollbackStmt   *RollbackStmt
	Explain        bool   // true if EXPLAIN prefix
	ExplainQuery   bool   // true if EXPLAIN QUERY PLAN
	RawSQL         string // Original SQL text for the statement

	// CTE (WITH clause) definitions
	CTEs      []*CTEDef
	Recursive bool // WITH RECURSIVE
}

// CTEDef represents a common table expression definition.
type CTEDef struct {
	Name    string      // CTE name
	Columns []string    // Optional column list
	Body    *SelectStmt // CTE body query
}

// WindowDef represents a window definition (basic parsing).
type WindowDef struct {
	Name         string   // Window name
	PartitionBy  []*Expr  // PARTITION BY expressions
	OrderBy      []*OrderItem // ORDER BY items
	// Frame spec omitted for now - basic support
}

// =============================================================================
// SELECT
// =============================================================================

// SelectStmt represents a SELECT statement.
type SelectStmt struct {
	Distinct    bool         // SELECT DISTINCT
	All         bool         // SELECT ALL
	Columns     []*ResultCol // Result columns
	From        *FromClause  // FROM clause (nil if no FROM)
	Where       *Expr        // WHERE condition
	GroupBy     []*Expr      // GROUP BY expressions
	Having      *Expr        // HAVING condition
	OrderBy     []*OrderItem // ORDER BY items
	Limit       *Expr        // LIMIT value
	Offset      *Expr        // OFFSET value (nil if not specified)
	CompoundOps []CompoundOp // Compound select operators (UNION, INTERSECT, EXCEPT)
	CompoundSelects []*SelectStmt // Right-hand selects for compound

	// Window definitions (parsed but not deeply analyzed yet)
	Windows []*WindowDef
}

// CompoundOp represents a compound select operator.
type CompoundOp int

const (
	CompoundUnion     CompoundOp = iota // UNION
	CompoundUnionAll                    // UNION ALL
	CompoundIntersect                   // INTERSECT
	CompoundExcept                      // EXCEPT
)

// ResultCol represents a column in the SELECT result list.
type ResultCol struct {
	Expr *Expr  // The expression
	Star bool   // true for *
	// For table.* form
	TableStar string // Non-empty for table.* form

	As string // Alias (AS name or trailing identifier)
}

// OrderItem represents an item in the ORDER BY clause.
type OrderItem struct {
	Expr  *Expr
	Order SortOrder // Ascending or Descending
}

// SortOrder represents sort direction.
type SortOrder int

const (
	SortDefault SortOrder = iota
	SortAsc
	SortDesc
)

// FromClause represents the FROM clause of a SELECT.
type FromClause struct {
	Tables []*TableRef // List of tables/subqueries
}

// JoinType represents the type of a join.
type JoinType int

const (
	JoinNone     JoinType = iota // No join (first table)
	JoinInner                    // INNER JOIN or JOIN or CROSS JOIN
	JoinLeft                     // LEFT [OUTER] JOIN
	JoinRight                    // RIGHT [OUTER] JOIN
	JoinFull                     // FULL [OUTER] JOIN
	JoinCross                    // CROSS JOIN (explicit)
	JoinNatural                  // NATURAL keyword modifier
)

// TableRef represents a table or subquery in the FROM clause.
type TableRef struct {
	// Table reference
	Schema    string // Schema/database name
	Name      string // Table name
	Alias     string // Alias (AS name)

	// Join info
	JoinType JoinType // Type of join to this table
	On       *Expr    // ON condition
	Using    []string // USING columns

	// Subquery
	Subquery *SelectStmt // Non-nil if this is a subquery

	// Function table (table-valued function)
	FuncArgs []*Expr // Arguments if this is a table-valued function
}

// =============================================================================
// INSERT
// =============================================================================

// InsertStmt represents an INSERT statement.
type InsertStmt struct {
	Table         *TableRef     // Target table
	Columns       []string      // Column list (nil if not specified)
	Values        [][]*Expr     // VALUES rows (each row is a list of expressions)
	Select        *SelectStmt   // INSERT ... SELECT ... (alternative to VALUES)
	DefaultValues bool          // DEFAULT VALUES
	OrReplace     bool          // INSERT OR REPLACE
	OrAbort       bool          // INSERT OR ABORT
	OrFail        bool          // INSERT OR FAIL
	OrIgnore      bool          // INSERT OR IGNORE
	Returning     []*ResultCol  // RETURNING clause
}

// =============================================================================
// UPDATE
// =============================================================================

// UpdateStmt represents an UPDATE statement.
type UpdateStmt struct {
	Table    *TableRef     // Target table
	Sets     []*SetClause  // SET clauses
	From     *FromClause   // FROM clause (nil if not specified)
	Where    *Expr         // WHERE condition
	OrReplace bool         // UPDATE OR REPLACE
	OrAbort   bool         // UPDATE OR ABORT
	OrFail    bool         // UPDATE OR FAIL
	OrIgnore  bool         // UPDATE OR IGNORE
	Returning []*ResultCol // RETURNING clause
}

// SetClause represents a single SET assignment.
type SetClause struct {
	Columns []string // Column names (usually one, but can be multiple for (a,b) = expr)
	Value   *Expr    // Value expression
}

// =============================================================================
// DELETE
// =============================================================================

// DeleteStmt represents a DELETE statement.
type DeleteStmt struct {
	Table     *TableRef    // Target table
	Where     *Expr        // WHERE condition
	Order     []*OrderItem // ORDER BY clause
	Limit     *Expr        // LIMIT
	Returning []*ResultCol // RETURNING clause
}

// =============================================================================
// CREATE TABLE
// =============================================================================

// CreateTableStmt represents a CREATE TABLE statement.
type CreateTableStmt struct {
	IfNotExists bool           // IF NOT EXISTS
	Temp        bool           // TEMPORARY or TEMP
	Schema      string         // Schema name (e.g., "main")
	Name        string         // Table name
	Columns     []*ColumnDef   // Column definitions
	Constraints []*TableConstraint // Table-level constraints
	AsSelect    *SelectStmt    // CREATE TABLE ... AS SELECT ...
	WithoutRowid bool          // WITHOUT ROWID
	Strict      bool           // STRICT
}

// ColumnDef represents a column definition in CREATE TABLE.
type ColumnDef struct {
	Name        string               // Column name
	Type        string               // Type name (e.g., "INTEGER", "TEXT(100)")
	Constraints []*ColumnConstraint  // Column constraints
}

// ColumnConstraint represents a constraint on a column.
type ColumnConstraint struct {
	Name        string // Constraint name (CONSTRAINT name)
	Type        ColumnConstraintType
	NotNull     bool      // for NOT NULL
	PrimaryKey  bool      // for PRIMARY KEY
	Autoincrement bool    // for AUTOINCREMENT
	Unique      bool      // for UNIQUE
	Default     *Expr     // for DEFAULT expr
	Check       *Expr     // for CHECK(expr)
	Collation   string    // for COLLATE name
	ForeignKey  *ForeignKeyRef // for REFERENCES
	OnConflict  string   // for ON CONFLICT (ROLLBACK, ABORT, FAIL, IGNORE, REPLACE)
}

// ColumnConstraintType identifies the type of column constraint.
type ColumnConstraintType int

const (
	CCNone ColumnConstraintType = iota
	CCPrimaryKey
	CCNotNull
	CCUnique
	CCDefault
	CCCheck
	CCForeignKey
	CCCollate
	CCGenerated
)

// TableConstraint represents a table-level constraint.
type TableConstraint struct {
	Name        string              // Constraint name (optional)
	Type        TableConstraintType
	Columns     []string            // For PRIMARY KEY, UNIQUE
	OrderBy     []*OrderItem        // For PRIMARY KEY, UNIQUE with ORDER BY
	Check       *Expr               // For CHECK
	ForeignKey  *ForeignKeyClause   // For FOREIGN KEY
	OnConflict  string              // for ON CONFLICT
}

// TableConstraintType identifies the type of table constraint.
type TableConstraintType int

const (
	TCNone TableConstraintType = iota
	TCPrimaryKey
	TCUnique
	TCCheck
	TCForeignKey
)

// ForeignKeyRef represents a REFERENCES clause on a column.
type ForeignKeyRef struct {
	Table   string   // Referenced table
	Columns []string // Referenced columns
}

// ForeignKeyClause represents a full FOREIGN KEY ... REFERENCES clause.
type ForeignKeyClause struct {
	Columns       []string       // Local columns
	RefTable      string         // Referenced table
	RefColumns    []string       // Referenced columns
	OnDelete      string         // ON DELETE action
	OnUpdate      string         // ON UPDATE action
}

// =============================================================================
// CREATE INDEX
// =============================================================================

// CreateIndexStmt represents a CREATE INDEX statement.
type CreateIndexStmt struct {
	Unique     bool         // UNIQUE index
	IfNotExists bool        // IF NOT EXISTS
	Schema     string       // Schema name
	Name       string       // Index name
	Table      string       // Table name
	Columns    []*OrderItem // Indexed columns (with sort order)
	Where      *Expr        // Partial index WHERE clause
}

// =============================================================================
// DROP TABLE
// =============================================================================

// DropTableStmt represents a DROP TABLE statement.
type DropTableStmt struct {
	IfExists bool   // IF EXISTS
	Schema   string // Schema name
	Name     string // Table name
}

// =============================================================================
// DROP INDEX
// =============================================================================

// DropIndexStmt represents a DROP INDEX statement.
type DropIndexStmt struct {
	IfExists bool   // IF EXISTS
	Schema   string // Schema name
	Name     string // Index name
}

// =============================================================================
// Transaction statements
// =============================================================================

// BeginStmt represents a BEGIN statement.
type BeginStmt struct {
	Deferred  bool // DEFERRED
	Immediate bool // IMMEDIATE
	Exclusive bool // EXCLUSIVE
}

// CommitStmt represents a COMMIT or END statement.
type CommitStmt struct{}

// RollbackStmt represents a ROLLBACK statement.
type RollbackStmt struct {
	Savepoint string // TO savepoint name (empty if not specified)
}
