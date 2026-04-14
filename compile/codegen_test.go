package compile

import (
	"strings"
	"testing"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// helperSchema creates a test schema with a "users" table and a "posts" table.
func helperSchema() *Schema {
	s := NewSchema()
	s.Tables["users"] = &TableInfo{
		Name:     "users",
		RootPage: 2,
		HasRowid: true,
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER", PrimaryKey: true, Autoincrement: true},
			{Name: "name", Type: "TEXT"},
			{Name: "email", Type: "TEXT"},
			{Name: "age", Type: "INTEGER"},
		},
	}
	s.Tables["posts"] = &TableInfo{
		Name:     "posts",
		RootPage: 3,
		HasRowid: true,
		Columns: []ColumnInfo{
			{Name: "id", Type: "INTEGER", PrimaryKey: true},
			{Name: "title", Type: "TEXT"},
			{Name: "body", Type: "TEXT"},
			{Name: "user_id", Type: "INTEGER"},
		},
	}
	s.Indexes["idx_users_email"] = &IndexInfo{
		Name:     "idx_users_email",
		Table:    "users",
		RootPage: 4,
		Unique:   true,
		Columns: []IndexColumn{
			{Name: "email", Order: SortAsc},
		},
	}
	return s
}

// =============================================================================
// Builder tests
// =============================================================================

func TestBuilderBasicEmit(t *testing.T) {
	b := NewBuilder()
	addr := b.Emit(vdbe.OpGoto, 0, 5, 0)
	if addr != 0 {
		t.Fatalf("first instruction should be at address 0, got %d", addr)
	}
	addr = b.Emit(vdbe.OpHalt, 0, 0, 0)
	if addr != 1 {
		t.Fatalf("second instruction should be at address 1, got %d", addr)
	}
	prog, err := b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	if len(prog.Instructions) != 2 {
		t.Fatalf("expected 2 instructions, got %d", len(prog.Instructions))
	}
}

func TestBuilderLabels(t *testing.T) {
	b := NewBuilder()
	loopStart := b.NewLabel()
	endLabel := b.NewLabel()

	b.DefineLabel(loopStart)
	b.Emit(vdbe.OpInteger, 0, 0, 0)
	b.EmitJump(vdbe.OpGoto, 0, loopStart, 0)
	b.DefineLabel(endLabel)
	b.Emit(vdbe.OpHalt, 0, 0, 0)

	prog, err := b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	// The Goto at address 1 should target address 0 (loopStart)
	if prog.Instructions[1].P2 != 0 {
		t.Fatalf("expected Goto target to be 0, got %d", prog.Instructions[1].P2)
	}
}

func TestBuilderUnresolvedLabel(t *testing.T) {
	b := NewBuilder()
	unresolved := b.NewLabel()
	b.EmitJump(vdbe.OpGoto, 0, unresolved, 0)
	_, err := b.BuildProgram()
	if err == nil {
		t.Fatal("expected error for unresolved label")
	}
}

func TestBuilderRegisterAllocation(t *testing.T) {
	b := NewBuilder()
	r1 := b.AllocReg(3)
	r2 := b.AllocReg(2)
	if r1 != 0 {
		t.Fatalf("first allocation should start at 0, got %d", r1)
	}
	if r2 != 3 {
		t.Fatalf("second allocation should start at 3, got %d", r2)
	}
	if b.NumRegs() != 5 {
		t.Fatalf("expected 5 total registers, got %d", b.NumRegs())
	}
}

func TestBuilderCursorAllocation(t *testing.T) {
	b := NewBuilder()
	c1 := b.AllocCursor()
	c2 := b.AllocCursor()
	if c1 != 0 {
		t.Fatalf("first cursor should be 0, got %d", c1)
	}
	if c2 != 1 {
		t.Fatalf("second cursor should be 1, got %d", c2)
	}
}

// =============================================================================
// Expression compilation tests
// =============================================================================

func TestCompileLiteralInteger(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 42}
	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	// Should emit an Int64 instruction
	found := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpInt64 {
			found = true
			if instr.P2 != reg {
				t.Fatalf("Int64 target register: expected %d, got %d", reg, instr.P2)
			}
			if instr.P4.(int64) != 42 {
				t.Fatalf("Int64 value: expected 42, got %v", instr.P4)
			}
		}
	}
	if !found {
		t.Fatal("expected Int64 instruction not found")
	}
}

func TestCompileLiteralString(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{Kind: ExprLiteral, LiteralType: "string", StringValue: "hello"}
	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpString8 {
			found = true
			if instr.P4.(string) != "hello" {
				t.Fatalf("String value: expected 'hello', got %v", instr.P4)
			}
		}
	}
	if !found {
		t.Fatal("expected String8 instruction not found")
	}
}

func TestCompileLiteralNull(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{Kind: ExprLiteral, LiteralType: "null"}
	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpNull {
			found = true
		}
	}
	if !found {
		t.Fatal("expected Null instruction not found")
	}
}

func TestCompileLiteralFloat(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{Kind: ExprLiteral, LiteralType: "float", FloatValue: 3.14}
	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpReal {
			found = true
			if instr.P4.(float64) != 3.14 {
				t.Fatalf("Real value: expected 3.14, got %v", instr.P4)
			}
		}
	}
	if !found {
		t.Fatal("expected Real instruction not found")
	}
}

func TestCompileBinaryAdd(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{
		Kind: ExprBinaryOp,
		Op:   "+",
		Left:  &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 10},
		Right: &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 20},
	}
	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpAdd {
			found = true
		}
	}
	if !found {
		t.Fatal("expected Add instruction not found")
	}
}

func TestCompileComparisonEq(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{
		Kind: ExprBinaryOp,
		Op:   "=",
		Left:  &Expr{Kind: ExprColumnRef, Name: "id"},
		Right: &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 1},
	}
	// Need to set up a table context
	bld.tables = append(bld.tables, &tableEntry{
		cursor: 0,
		table: &TableInfo{
			Name:     "t",
			RootPage: 1,
			Columns:  []ColumnInfo{{Name: "id"}, {Name: "name"}},
		},
	})
	bld.tableMap["T"] = bld.tables[0]

	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	// Should contain OpEq
	found := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpEq {
			found = true
		}
	}
	if !found {
		t.Fatal("expected Eq instruction not found")
	}
}

func TestCompileAnd(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{
		Kind: ExprBinaryOp,
		Op:   "AND",
		Left:  &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 1},
		Right: &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 0},
	}
	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	// Should contain IfNot (short-circuit AND: if left is false, skip right)
	hasIfNot := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpIfNot {
			hasIfNot = true
		}
	}
	if !hasIfNot {
		t.Fatal("expected IfNot for AND short-circuit")
	}
}

func TestCompileUnaryMinus(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{
		Kind:  ExprUnaryOp,
		Op:    "-",
		Right: &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 5},
	}
	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpSubtract {
			found = true
		}
	}
	if !found {
		t.Fatal("expected Subtract instruction for unary minus")
	}
}

func TestCompileUnaryNot(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{
		Kind:  ExprUnaryOp,
		Op:    "NOT",
		Right: &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 1},
	}
	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpNot {
			found = true
		}
	}
	if !found {
		t.Fatal("expected Not instruction")
	}
}

func TestCompileIsNull(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{
		Kind: ExprIsNull,
		Left: &Expr{Kind: ExprColumnRef, Name: "name"},
	}
	bld.tables = append(bld.tables, &tableEntry{
		cursor: 0,
		table: &TableInfo{
			Name:     "t",
			RootPage: 1,
			Columns:  []ColumnInfo{{Name: "id"}, {Name: "name"}},
		},
	})
	bld.tableMap["T"] = bld.tables[0]

	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpIsNull {
			found = true
		}
	}
	if !found {
		t.Fatal("expected IsNull instruction")
	}
}

func TestCompileBetween(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{
		Kind: ExprBetween,
		Left: &Expr{Kind: ExprColumnRef, Name: "age"},
		Low:  &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 18},
		High: &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 65},
	}
	bld.tables = append(bld.tables, &tableEntry{
		cursor: 0,
		table: &TableInfo{
			Name:     "t",
			RootPage: 1,
			Columns:  []ColumnInfo{{Name: "id"}, {Name: "age"}},
		},
	})
	bld.tableMap["T"] = bld.tables[0]

	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	// Should contain Lt and Gt instructions for the range check
	ltFound, gtFound := false, false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpLt {
			ltFound = true
		}
		if instr.Op == vdbe.OpGt {
			gtFound = true
		}
	}
	if !ltFound || !gtFound {
		t.Fatal("expected Lt and Gt instructions for BETWEEN")
	}
}

func TestCompileInList(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{
		Kind: ExprInList,
		Left: &Expr{Kind: ExprColumnRef, Name: "id"},
		InValues: []*Expr{
			{Kind: ExprLiteral, LiteralType: "integer", IntValue: 1},
			{Kind: ExprLiteral, LiteralType: "integer", IntValue: 2},
			{Kind: ExprLiteral, LiteralType: "integer", IntValue: 3},
		},
	}
	bld.tables = append(bld.tables, &tableEntry{
		cursor: 0,
		table: &TableInfo{
			Name:     "t",
			RootPage: 1,
			Columns:  []ColumnInfo{{Name: "id"}, {Name: "name"}},
		},
	})
	bld.tableMap["T"] = bld.tables[0]

	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	// Should contain 3 Eq instructions (one per value)
	eqCount := 0
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpEq {
			eqCount++
		}
	}
	if eqCount != 3 {
		t.Fatalf("expected 3 Eq instructions for IN (1,2,3), got %d", eqCount)
	}
}

func TestCompileCase(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{
		Kind: ExprCase,
		WhenList: []*WhenClause{
			{
				Condition: &Expr{
					Kind:  ExprBinaryOp,
					Op:    "=",
					Left:  &Expr{Kind: ExprColumnRef, Name: "id"},
					Right: &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 1},
				},
				Result: &Expr{Kind: ExprLiteral, LiteralType: "string", StringValue: "one"},
			},
		},
		ElseExpr: &Expr{Kind: ExprLiteral, LiteralType: "string", StringValue: "other"},
	}
	bld.tables = append(bld.tables, &tableEntry{
		cursor: 0,
		table: &TableInfo{
			Name:     "t",
			RootPage: 1,
			Columns:  []ColumnInfo{{Name: "id"}, {Name: "name"}},
		},
	})
	bld.tableMap["T"] = bld.tables[0]

	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	// Should contain Ne (for case comparison) and String8
	foundString := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpString8 {
			foundString = true
		}
	}
	if !foundString {
		t.Fatal("expected String8 instruction in CASE")
	}
}

func TestCompileFunctionCall(t *testing.T) {
	bld := newBuild(nil)
	reg := bld.b.AllocReg(1)
	expr := &Expr{
		Kind:         ExprFunctionCall,
		FunctionName: "ABS",
		Args: []*Expr{
			{Kind: ExprLiteral, LiteralType: "integer", IntValue: -5},
		},
	}
	if err := bld.compileExpr(expr, reg); err != nil {
		t.Fatal(err)
	}
	prog, err := bld.b.BuildProgram()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpFunction {
			found = true
			fi := instr.P4.(*vdbe.FuncInfo)
			if fi.Name != "ABS" {
				t.Fatalf("expected function name ABS, got %s", fi.Name)
			}
		}
	}
	if !found {
		t.Fatal("expected Function instruction")
	}
}

// =============================================================================
// SELECT compilation tests
// =============================================================================

func TestCompileSelectNoTable(t *testing.T) {
	schema := NewSchema()
	stmt := &Statement{
		Type: StmtSelect,
		SelectStmt: &SelectStmt{
			Columns: []*ResultCol{
				{Expr: &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 1}},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	if len(prog.Instructions) == 0 {
		t.Fatal("expected at least one instruction")
	}
	// First instruction should be Init
	if prog.Instructions[0].Op != vdbe.OpInit {
		t.Fatalf("expected first instruction to be Init, got %v", prog.Instructions[0].Op)
	}
	// Should have Halt
	hasHalt := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpHalt {
			hasHalt = true
		}
	}
	if !hasHalt {
		t.Fatal("expected Halt instruction")
	}
}

func TestCompileSelectSingleTable(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtSelect,
		SelectStmt: &SelectStmt{
			Columns: []*ResultCol{
				{Expr: &Expr{Kind: ExprColumnRef, Name: "id"}},
				{Expr: &Expr{Kind: ExprColumnRef, Name: "name"}},
			},
			From: &FromClause{
				Tables: []*TableRef{
					{Name: "users"},
				},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	// Should have OpenRead, Rewind, Column x2, ResultRow, Next, Close
	hasOpenRead, hasRewind, hasColumn, hasResultRow, hasNext, hasClose := false, false, false, false, false, false
	colCount := 0
	for _, instr := range prog.Instructions {
		switch instr.Op {
		case vdbe.OpOpenRead:
			hasOpenRead = true
		case vdbe.OpRewind:
			hasRewind = true
		case vdbe.OpColumn:
			hasColumn = true
			colCount++
		case vdbe.OpResultRow:
			hasResultRow = true
		case vdbe.OpNext:
			hasNext = true
		case vdbe.OpClose:
			hasClose = true
		}
	}
	if !hasOpenRead {
		t.Error("expected OpenRead")
	}
	if !hasRewind {
		t.Error("expected Rewind")
	}
	if !hasColumn {
		t.Error("expected Column")
	}
	if colCount != 2 {
		t.Errorf("expected 2 Column instructions, got %d", colCount)
	}
	if !hasResultRow {
		t.Error("expected ResultRow")
	}
	if !hasNext {
		t.Error("expected Next")
	}
	if !hasClose {
		t.Error("expected Close")
	}
}

func TestCompileSelectWithWhere(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtSelect,
		SelectStmt: &SelectStmt{
			Columns: []*ResultCol{
				{Expr: &Expr{Kind: ExprColumnRef, Name: "name"}},
			},
			From: &FromClause{
				Tables: []*TableRef{
					{Name: "users"},
				},
			},
			Where: &Expr{
				Kind: ExprBinaryOp,
				Op:   "=",
				Left:  &Expr{Kind: ExprColumnRef, Name: "age"},
				Right: &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 25},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	// Should have a comparison instruction (Eq) in the program
	hasEq := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpEq {
			hasEq = true
		}
	}
	if !hasEq {
		t.Error("expected Eq instruction for WHERE clause")
	}
}

func TestCompileSelectWithOrderBy(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtSelect,
		SelectStmt: &SelectStmt{
			Columns: []*ResultCol{
				{Expr: &Expr{Kind: ExprColumnRef, Name: "name"}},
				{Expr: &Expr{Kind: ExprColumnRef, Name: "age"}},
			},
			From: &FromClause{
				Tables: []*TableRef{
					{Name: "users"},
				},
			},
			OrderBy: []*OrderItem{
				{Expr: &Expr{Kind: ExprColumnRef, Name: "age"}, Order: SortDesc},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	// Should have SorterOpen, SorterInsert, SorterSort, SorterNext
	hasSorterOpen, hasSorterInsert, hasSorterSort, hasSorterNext := false, false, false, false
	for _, instr := range prog.Instructions {
		switch instr.Op {
		case vdbe.OpSorterOpen:
			hasSorterOpen = true
		case vdbe.OpSorterInsert:
			hasSorterInsert = true
		case vdbe.OpSorterSort:
			hasSorterSort = true
		case vdbe.OpSorterNext:
			hasSorterNext = true
		}
	}
	if !hasSorterOpen {
		t.Error("expected SorterOpen")
	}
	if !hasSorterInsert {
		t.Error("expected SorterInsert")
	}
	if !hasSorterSort {
		t.Error("expected SorterSort")
	}
	if !hasSorterNext {
		t.Error("expected SorterNext")
	}
}

func TestCompileSelectStar(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtSelect,
		SelectStmt: &SelectStmt{
			Columns: []*ResultCol{
				{Star: true},
			},
			From: &FromClause{
				Tables: []*TableRef{
					{Name: "users"},
				},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	// SELECT * from users (4 cols) should produce 4 Column instructions
	colCount := 0
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpColumn {
			colCount++
		}
	}
	if colCount != 4 {
		t.Errorf("expected 4 Column instructions for SELECT * from users, got %d", colCount)
	}
}

func TestCompileSelectQualifiedColumn(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtSelect,
		SelectStmt: &SelectStmt{
			Columns: []*ResultCol{
				{Expr: &Expr{Kind: ExprColumnRef, Table: "users", Name: "name"}},
			},
			From: &FromClause{
				Tables: []*TableRef{
					{Name: "users"},
				},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	// Should have a Column instruction
	hasColumn := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpColumn {
			hasColumn = true
			// Column "name" is at index 1 in users table
			if instr.P2 != 1 {
				t.Errorf("expected column index 1 for 'name', got %d", instr.P2)
			}
		}
	}
	if !hasColumn {
		t.Error("expected Column instruction")
	}
}

// =============================================================================
// INSERT compilation tests
// =============================================================================

func TestCompileInsertValues(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtInsert,
		InsertStmt: &InsertStmt{
			Table: &TableRef{Name: "users"},
			Values: [][]*Expr{
				{
					{Kind: ExprLiteral, LiteralType: "null"},
					{Kind: ExprLiteral, LiteralType: "string", StringValue: "Alice"},
					{Kind: ExprLiteral, LiteralType: "string", StringValue: "alice@example.com"},
					{Kind: ExprLiteral, LiteralType: "integer", IntValue: 30},
				},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	// Should have OpenWrite, NewRowid, MakeRecord, Insert, Close
	hasOpenWrite, hasNewRowid, hasMakeRecord, hasInsert, hasClose := false, false, false, false, false
	for _, instr := range prog.Instructions {
		switch instr.Op {
		case vdbe.OpOpenWrite:
			hasOpenWrite = true
		case vdbe.OpNewRowid:
			hasNewRowid = true
		case vdbe.OpMakeRecord:
			hasMakeRecord = true
		case vdbe.OpInsert:
			hasInsert = true
		case vdbe.OpClose:
			hasClose = true
		}
	}
	if !hasOpenWrite {
		t.Error("expected OpenWrite")
	}
	if !hasNewRowid {
		t.Error("expected NewRowid")
	}
	if !hasMakeRecord {
		t.Error("expected MakeRecord")
	}
	if !hasInsert {
		t.Error("expected Insert")
	}
	if !hasClose {
		t.Error("expected Close")
	}
}

func TestCompileInsertWithColumnList(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtInsert,
		InsertStmt: &InsertStmt{
			Table:   &TableRef{Name: "users"},
			Columns: []string{"name", "email"},
			Values: [][]*Expr{
				{
					{Kind: ExprLiteral, LiteralType: "string", StringValue: "Bob"},
					{Kind: ExprLiteral, LiteralType: "string", StringValue: "bob@test.com"},
				},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	if len(prog.Instructions) == 0 {
		t.Fatal("expected instructions")
	}
	// Should still have Insert
	hasInsert := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpInsert {
			hasInsert = true
		}
	}
	if !hasInsert {
		t.Error("expected Insert instruction")
	}
}

func TestCompileInsertDefaultValues(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtInsert,
		InsertStmt: &InsertStmt{
			Table:         &TableRef{Name: "users"},
			DefaultValues: true,
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	hasInsert := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpInsert {
			hasInsert = true
		}
	}
	if !hasInsert {
		t.Error("expected Insert for DEFAULT VALUES")
	}
}

// =============================================================================
// DELETE compilation tests
// =============================================================================

func TestCompileDeleteWithWhere(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtDelete,
		DeleteStmt: &DeleteStmt{
			Table: &TableRef{Name: "users"},
			Where: &Expr{
				Kind: ExprBinaryOp,
				Op:   "=",
				Left:  &Expr{Kind: ExprColumnRef, Name: "id"},
				Right: &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 1},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	// Should have OpenWrite, Rewind, Rowid, Column, Eq, Delete, Next, Close
	hasOpenWrite, hasDelete, hasNext, hasRewind := false, false, false, false
	for _, instr := range prog.Instructions {
		switch instr.Op {
		case vdbe.OpOpenWrite:
			hasOpenWrite = true
		case vdbe.OpDelete:
			hasDelete = true
		case vdbe.OpNext:
			hasNext = true
		case vdbe.OpRewind:
			hasRewind = true
		}
	}
	if !hasOpenWrite {
		t.Error("expected OpenWrite")
	}
	if !hasDelete {
		t.Error("expected Delete")
	}
	if !hasNext {
		t.Error("expected Next")
	}
	if !hasRewind {
		t.Error("expected Rewind")
	}
}

func TestCompileDeleteAll(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtDelete,
		DeleteStmt: &DeleteStmt{
			Table: &TableRef{Name: "users"},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	hasDelete := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpDelete {
			hasDelete = true
		}
	}
	if !hasDelete {
		t.Error("expected Delete instruction for DELETE without WHERE")
	}
}

// =============================================================================
// UPDATE compilation tests
// =============================================================================

func TestCompileUpdate(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtUpdate,
		UpdateStmt: &UpdateStmt{
			Table: &TableRef{Name: "users"},
			Sets: []*SetClause{
				{Columns: []string{"name"}, Value: &Expr{Kind: ExprLiteral, LiteralType: "string", StringValue: "Charlie"}},
			},
			Where: &Expr{
				Kind: ExprBinaryOp,
				Op:   "=",
				Left:  &Expr{Kind: ExprColumnRef, Name: "id"},
				Right: &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 1},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	// Should have OpenWrite, Rewind, Rowid, Column, MakeRecord, Update, Next, Close
	hasOpenWrite, hasUpdate, hasMakeRecord, hasRewind := false, false, false, false
	for _, instr := range prog.Instructions {
		switch instr.Op {
		case vdbe.OpOpenWrite:
			hasOpenWrite = true
		case vdbe.OpUpdate:
			hasUpdate = true
		case vdbe.OpMakeRecord:
			hasMakeRecord = true
		case vdbe.OpRewind:
			hasRewind = true
		}
	}
	if !hasOpenWrite {
		t.Error("expected OpenWrite")
	}
	if !hasUpdate {
		t.Error("expected Update")
	}
	if !hasMakeRecord {
		t.Error("expected MakeRecord")
	}
	if !hasRewind {
		t.Error("expected Rewind")
	}
}

// =============================================================================
// DDL compilation tests
// =============================================================================

func TestCompileCreateTable(t *testing.T) {
	schema := NewSchema()
	stmt := &Statement{
		Type: StmtCreateTable,
		CreateTable: &CreateTableStmt{
			Name: "products",
			Columns: []*ColumnDef{
				{Name: "id", Type: "INTEGER", Constraints: []*ColumnConstraint{
					{Type: CCPrimaryKey, PrimaryKey: true},
				}},
				{Name: "name", Type: "TEXT"},
				{Name: "price", Type: "REAL"},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	// Should have CreateBTree, OpenWrite (schema), MakeRecord, Insert
	hasCreateBTree, hasInsert := false, false
	for _, instr := range prog.Instructions {
		switch instr.Op {
		case vdbe.OpCreateBTree:
			hasCreateBTree = true
		case vdbe.OpInsert:
			hasInsert = true
		}
	}
	if !hasCreateBTree {
		t.Error("expected CreateBTree")
	}
	if !hasInsert {
		t.Error("expected Insert (into schema table)")
	}
}

func TestCompileCreateTableIfNotExists(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtCreateTable,
		CreateTable: &CreateTableStmt{
			Name:        "users", // already exists
			IfNotExists: true,
			Columns: []*ColumnDef{
				{Name: "id", Type: "INTEGER"},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	// Should NOT have CreateBTree since table already exists
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpCreateBTree {
			t.Error("expected no CreateBTree for IF NOT EXISTS when table exists")
		}
	}
}

func TestCompileDropTable(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtDropTable,
		DropTable: &DropTableStmt{
			Name: "users",
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	// Should have Destroy
	hasDestroy := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpDestroy {
			hasDestroy = true
		}
	}
	if !hasDestroy {
		t.Error("expected Destroy")
	}
}

func TestCompileDropTableIfExists(t *testing.T) {
	schema := NewSchema()
	stmt := &Statement{
		Type: StmtDropTable,
		DropTable: &DropTableStmt{
			Name:     "nonexistent",
			IfExists: true,
		},
	}
	// Should not error even though table doesn't exist
	_, err := Compile(stmt, schema)
	if err != nil {
		t.Fatalf("expected no error for DROP TABLE IF EXISTS on nonexistent table: %v", err)
	}
}

func TestCompileCreateIndex(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtCreateIndex,
		CreateIndex: &CreateIndexStmt{
			Name:  "idx_posts_user_id",
			Table: "posts",
			Columns: []*OrderItem{
				{Expr: &Expr{Kind: ExprColumnRef, Name: "user_id"}, Order: SortAsc},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	hasCreateBTree, hasIdxInsert := false, false
	for _, instr := range prog.Instructions {
		switch instr.Op {
		case vdbe.OpCreateBTree:
			hasCreateBTree = true
		case vdbe.OpIdxInsert:
			hasIdxInsert = true
		}
	}
	if !hasCreateBTree {
		t.Error("expected CreateBTree")
	}
	if !hasIdxInsert {
		t.Error("expected IdxInsert")
	}
}

func TestCompileDropIndex(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtDropIndex,
		DropIndex: &DropIndexStmt{
			Name: "idx_users_email",
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	hasDestroy := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpDestroy {
			hasDestroy = true
		}
	}
	if !hasDestroy {
		t.Error("expected Destroy for DROP INDEX")
	}
}

// =============================================================================
// Transaction compilation tests
// =============================================================================

func TestCompileBegin(t *testing.T) {
	schema := NewSchema()
	stmt := &Statement{
		Type:      StmtBegin,
		BeginStmt: &BeginStmt{Deferred: true},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	// Should have AutoCommit(0) and Transaction
	hasAutoCommit, hasTransaction := false, false
	for _, instr := range prog.Instructions {
		switch instr.Op {
		case vdbe.OpAutoCommit:
			hasAutoCommit = true
			if instr.P1 != 0 {
				t.Errorf("expected AutoCommit P1=0, got %d", instr.P1)
			}
		case vdbe.OpTransaction:
			hasTransaction = true
		}
	}
	if !hasAutoCommit {
		t.Error("expected AutoCommit")
	}
	if !hasTransaction {
		t.Error("expected Transaction")
	}
}

func TestCompileCommit(t *testing.T) {
	schema := NewSchema()
	stmt := &Statement{
		Type:       StmtCommit,
		CommitStmt: &CommitStmt{},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	hasAutoCommit := false
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpAutoCommit {
			hasAutoCommit = true
			if instr.P1 != 1 {
				t.Errorf("expected AutoCommit P1=1 for COMMIT, got %d", instr.P1)
			}
		}
	}
	if !hasAutoCommit {
		t.Error("expected AutoCommit for COMMIT")
	}
}

// =============================================================================
// Error handling tests
// =============================================================================

func TestCompileNilStatement(t *testing.T) {
	_, err := Compile(nil, NewSchema())
	if err == nil {
		t.Fatal("expected error for nil statement")
	}
}

func TestCompileInvalidTable(t *testing.T) {
	schema := NewSchema()
	stmt := &Statement{
		Type: StmtSelect,
		SelectStmt: &SelectStmt{
			Columns: []*ResultCol{
				{Expr: &Expr{Kind: ExprColumnRef, Name: "id"}},
			},
			From: &FromClause{
				Tables: []*TableRef{
					{Name: "nonexistent"},
				},
			},
		},
	}
	_, err := Compile(stmt, schema)
	if err == nil {
		t.Fatal("expected error for nonexistent table")
	}
	if !strings.Contains(err.Error(), "no such table") {
		t.Fatalf("expected 'no such table' error, got: %v", err)
	}
}

func TestCompileInvalidColumn(t *testing.T) {
	bld := newBuild(helperSchema())
	bld.addTableRef("users", "users", bld.schema.Tables["users"], 0)

	_, _, err := bld.resolveColumnRef("", "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent column")
	}
	if !strings.Contains(err.Error(), "no such column") {
		t.Fatalf("expected 'no such column' error, got: %v", err)
	}
}

func TestCompileAmbiguousColumn(t *testing.T) {
	bld := newBuild(helperSchema())
	bld.addTableRef("users", "users", bld.schema.Tables["users"], 0)
	bld.addTableRef("posts", "posts", bld.schema.Tables["posts"], 1)

	_, _, err := bld.resolveColumnRef("", "id")
	if err == nil {
		t.Fatal("expected error for ambiguous column")
	}
	if !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("expected 'ambiguous' error, got: %v", err)
	}
}

func TestCompileUnsupportedStatement(t *testing.T) {
	schema := NewSchema()
	stmt := &Statement{Type: StmtPragma}
	_, err := Compile(stmt, schema)
	if err == nil {
		t.Fatal("expected error for unsupported statement type")
	}
}

// =============================================================================
// Schema tests
// =============================================================================

func TestSchemaLookup(t *testing.T) {
	bld := newBuild(helperSchema())
	tbl, err := bld.lookupTable("users")
	if err != nil {
		t.Fatal(err)
	}
	if tbl.Name != "users" {
		t.Fatalf("expected users table, got %s", tbl.Name)
	}
}

// =============================================================================
// DDL SQL generation tests
// =============================================================================

func TestBuildCreateTableSQL(t *testing.T) {
	stmt := &CreateTableStmt{
		Name: "test",
		Columns: []*ColumnDef{
			{Name: "id", Type: "INTEGER", Constraints: []*ColumnConstraint{
				{Type: CCPrimaryKey, PrimaryKey: true, Autoincrement: true},
			}},
			{Name: "name", Type: "TEXT", Constraints: []*ColumnConstraint{
				{Type: CCNotNull, NotNull: true},
			}},
		},
	}
	sql := buildCreateTableSQL(stmt)
	if !strings.Contains(sql, "CREATE TABLE test") {
		t.Errorf("unexpected SQL: %s", sql)
	}
	if !strings.Contains(sql, "PRIMARY KEY") {
		t.Errorf("expected PRIMARY KEY in: %s", sql)
	}
	if !strings.Contains(sql, "AUTOINCREMENT") {
		t.Errorf("expected AUTOINCREMENT in: %s", sql)
	}
	if !strings.Contains(sql, "NOT NULL") {
		t.Errorf("expected NOT NULL in: %s", sql)
	}
}

func TestBuildCreateTableSQLWithConstraints(t *testing.T) {
	stmt := &CreateTableStmt{
		Name: "orders",
		Columns: []*ColumnDef{
			{Name: "id", Type: "INTEGER"},
			{Name: "user_id", Type: "INTEGER"},
		},
		Constraints: []*TableConstraint{
			{
				Type:    TCPrimaryKey,
				Columns: []string{"id"},
			},
			{
				Type:    TCForeignKey,
				Columns: []string{"user_id"},
				ForeignKey: &ForeignKeyClause{
					RefTable:   "users",
					RefColumns: []string{"id"},
				},
			},
		},
	}
	sql := buildCreateTableSQL(stmt)
	if !strings.Contains(sql, "PRIMARY KEY") {
		t.Errorf("expected PRIMARY KEY in: %s", sql)
	}
	if !strings.Contains(sql, "FOREIGN KEY") {
		t.Errorf("expected FOREIGN KEY in: %s", sql)
	}
	if !strings.Contains(sql, "REFERENCES users") {
		t.Errorf("expected REFERENCES in: %s", sql)
	}
}

func TestBuildCreateIndexSQL(t *testing.T) {
	stmt := &CreateIndexStmt{
		Unique: true,
		Name:   "idx_name",
		Table:  "users",
		Columns: []*OrderItem{
			{Expr: &Expr{Kind: ExprColumnRef, Name: "name"}, Order: SortDesc},
		},
		Where: &Expr{
			Kind:  ExprBinaryOp,
			Op:    "=",
			Left:  &Expr{Kind: ExprColumnRef, Name: "active"},
			Right: &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 1},
		},
	}
	sql := buildCreateIndexSQL(stmt)
	if !strings.Contains(sql, "CREATE UNIQUE INDEX") {
		t.Errorf("expected CREATE UNIQUE INDEX in: %s", sql)
	}
	if !strings.Contains(sql, "DESC") {
		t.Errorf("expected DESC in: %s", sql)
	}
	if !strings.Contains(sql, "WHERE") {
		t.Errorf("expected WHERE in: %s", sql)
	}
}

// =============================================================================
// Expression to string tests
// =============================================================================

func TestExprToString(t *testing.T) {
	tests := []struct {
		expr *Expr
		want string
	}{
		{&Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 42}, "42"},
		{&Expr{Kind: ExprLiteral, LiteralType: "string", StringValue: "hello"}, "'hello'"},
		{&Expr{Kind: ExprLiteral, LiteralType: "null"}, "NULL"},
		{&Expr{Kind: ExprColumnRef, Name: "id"}, "id"},
		{&Expr{Kind: ExprColumnRef, Table: "users", Name: "id"}, "users.id"},
	}
	for _, tt := range tests {
		got := exprToString(tt.expr)
		if got != tt.want {
			t.Errorf("exprToString(%v) = %q, want %q", tt.expr, got, tt.want)
		}
	}
}

// =============================================================================
// Helper function tests
// =============================================================================

func TestCaseInsensitiveEqual(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"Hello", "HELLO", true},
		{"abc", "abc", true},
		{"abc", "abd", false},
		{"", "", true},
		{"a", "ab", false},
	}
	for _, tt := range tests {
		got := caseInsensitiveEqual(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("caseInsensitiveEqual(%q, %q) = %v, want %v", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestFindColumnIndex(t *testing.T) {
	tbl := &TableInfo{
		Columns: []ColumnInfo{
			{Name: "id"},
			{Name: "name"},
			{Name: "email"},
		},
	}
	if idx := findColumnIndex(tbl, "id"); idx != 0 {
		t.Errorf("expected index 0 for id, got %d", idx)
	}
	if idx := findColumnIndex(tbl, "Name"); idx != 1 {
		t.Errorf("expected index 1 for Name, got %d", idx)
	}
	if idx := findColumnIndex(tbl, "EMAIL"); idx != 2 {
		t.Errorf("expected index 2 for EMAIL, got %d", idx)
	}
	if idx := findColumnIndex(tbl, "nonexistent"); idx != -1 {
		t.Errorf("expected -1 for nonexistent, got %d", idx)
	}
}

func TestIsAggregate(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"COUNT", true},
		{"count", true},
		{"SUM", true},
		{"AVG", true},
		{"MIN", true},
		{"MAX", true},
		{"ABS", false},
		{"LENGTH", false},
		{"UPPER", false},
	}
	for _, tt := range tests {
		got := isAggregate(tt.name)
		if got != tt.want {
			t.Errorf("isAggregate(%q) = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestAffinityFromType(t *testing.T) {
	tests := []struct {
		typeName string
		want     int
	}{
		{"INTEGER", 1},
		{"INT", 1},
		{"BIGINT", 1},
		{"TEXT", 2},
		{"VARCHAR(100)", 2},
		{"REAL", 3},
		{"FLOAT", 3},
		{"DOUBLE", 3},
		{"BLOB", 4},
		{"NUMERIC", 2},
	}
	for _, tt := range tests {
		got := affinityFromType(tt.typeName)
		if got != tt.want {
			t.Errorf("affinityFromType(%q) = %d, want %d", tt.typeName, got, tt.want)
		}
	}
}

func TestComparisonOpcode(t *testing.T) {
	tests := []struct {
		op   string
		want vdbe.Opcode
		ok   bool
	}{
		{"=", vdbe.OpEq, true},
		{"==", vdbe.OpEq, true},
		{"<>", vdbe.OpNe, true},
		{"!=", vdbe.OpNe, true},
		{"<", vdbe.OpLt, true},
		{"<=", vdbe.OpLe, true},
		{">", vdbe.OpGt, true},
		{">=", vdbe.OpGe, true},
		{"+", 0, false},
		{"*", 0, false},
	}
	for _, tt := range tests {
		got, ok := comparisonOpcode(tt.op)
		if ok != tt.ok {
			t.Errorf("comparisonOpcode(%q) ok = %v, want %v", tt.op, ok, tt.ok)
		}
		if ok && got != tt.want {
			t.Errorf("comparisonOpcode(%q) = %v, want %v", tt.op, got, tt.want)
		}
	}
}

func TestInverseComparison(t *testing.T) {
	tests := []struct {
		op   vdbe.Opcode
		want vdbe.Opcode
	}{
		{vdbe.OpEq, vdbe.OpNe},
		{vdbe.OpNe, vdbe.OpEq},
		{vdbe.OpLt, vdbe.OpGe},
		{vdbe.OpGt, vdbe.OpLe},
		{vdbe.OpLe, vdbe.OpGt},
		{vdbe.OpGe, vdbe.OpLt},
	}
	for _, tt := range tests {
		got := inverseComparison(tt.op)
		if got != tt.want {
			t.Errorf("inverseComparison(%v) = %v, want %v", tt.op, got, tt.want)
		}
	}
}

// =============================================================================
// Column resolution tests
// =============================================================================

func TestResolveColumnQualified(t *testing.T) {
	bld := newBuild(helperSchema())
	bld.addTableRef("users", "u", bld.schema.Tables["users"], 0)

	cursor, colIdx, err := bld.resolveColumnRef("u", "name")
	if err != nil {
		t.Fatal(err)
	}
	if cursor != 0 {
		t.Errorf("expected cursor 0, got %d", cursor)
	}
	if colIdx != 1 {
		t.Errorf("expected column index 1 for 'name', got %d", colIdx)
	}
}

func TestResolveColumnUnqualified(t *testing.T) {
	bld := newBuild(helperSchema())
	bld.addTableRef("users", "users", bld.schema.Tables["users"], 0)

	cursor, colIdx, err := bld.resolveColumnRef("", "email")
	if err != nil {
		t.Fatal(err)
	}
	if cursor != 0 {
		t.Errorf("expected cursor 0, got %d", cursor)
	}
	if colIdx != 2 {
		t.Errorf("expected column index 2 for 'email', got %d", colIdx)
	}
}

func TestResolveColumnByAlias(t *testing.T) {
	bld := newBuild(helperSchema())
	bld.addTableRef("users", "u", bld.schema.Tables["users"], 0)

	cursor, colIdx, err := bld.resolveColumnRef("u", "id")
	if err != nil {
		t.Fatal(err)
	}
	if cursor != 0 {
		t.Errorf("expected cursor 0, got %d", cursor)
	}
	if colIdx != 0 {
		t.Errorf("expected column index 0 for 'id', got %d", colIdx)
	}
}

// =============================================================================
// Program structure tests
// =============================================================================

func TestProgramStructure(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtSelect,
		SelectStmt: &SelectStmt{
			Columns: []*ResultCol{
				{Expr: &Expr{Kind: ExprLiteral, LiteralType: "integer", IntValue: 1}},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}

	// Program should have Init at start
	if prog.Instructions[0].Op != vdbe.OpInit {
		t.Fatalf("first instruction should be Init, got %v", prog.Instructions[0].Op)
	}

	// Init should jump to the next instruction (addr 1)
	initTarget := prog.Instructions[0].P2
	if initTarget != 1 {
		t.Errorf("Init target should be 1, got %d", initTarget)
	}

	// NumRegs should be > 0
	if prog.NumRegs <= 0 {
		t.Errorf("expected positive NumRegs, got %d", prog.NumRegs)
	}
}

func TestMultipleRowInsert(t *testing.T) {
	schema := helperSchema()
	stmt := &Statement{
		Type: StmtInsert,
		InsertStmt: &InsertStmt{
			Table: &TableRef{Name: "users"},
			Values: [][]*Expr{
				{
					{Kind: ExprLiteral, LiteralType: "null"},
					{Kind: ExprLiteral, LiteralType: "string", StringValue: "Alice"},
					{Kind: ExprLiteral, LiteralType: "string", StringValue: "a@b.com"},
					{Kind: ExprLiteral, LiteralType: "integer", IntValue: 25},
				},
				{
					{Kind: ExprLiteral, LiteralType: "null"},
					{Kind: ExprLiteral, LiteralType: "string", StringValue: "Bob"},
					{Kind: ExprLiteral, LiteralType: "string", StringValue: "c@d.com"},
					{Kind: ExprLiteral, LiteralType: "integer", IntValue: 30},
				},
			},
		},
	}
	prog, err := Compile(stmt, schema)
	if err != nil {
		t.Fatal(err)
	}
	// Should have 2 Insert instructions (one per row)
	insertCount := 0
	for _, instr := range prog.Instructions {
		if instr.Op == vdbe.OpInsert {
			insertCount++
		}
	}
	if insertCount != 2 {
		t.Errorf("expected 2 Insert instructions for 2-row VALUES, got %d", insertCount)
	}
}
