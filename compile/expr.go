package compile

import (
	"fmt"
	"strings"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// FuncInfo describes a SQL function for the VDBE Function opcode.
type FuncInfo struct {
	Name      string
	ArgCount  int // -1 for variable args
	Distinct  bool
	IsAggregate bool
}

// compileExpr compiles an expression tree into VDBE instructions.
// The result is stored in targetReg.
func (b *Build) compileExpr(expr *Expr, targetReg int) error {
	if expr == nil {
		b.emitNull(targetReg)
		return nil
	}

	switch expr.Kind {
	case ExprLiteral:
		return b.compileLiteral(expr, targetReg)
	case ExprVariable:
		return b.compileVariable(expr, targetReg)
	case ExprColumnRef:
		return b.compileColumnRef(expr, targetReg)
	case ExprBinaryOp:
		return b.compileBinaryOp(expr, targetReg)
	case ExprUnaryOp:
		return b.compileUnaryOp(expr, targetReg)
	case ExprFunctionCall:
		return b.compileFunctionCall(expr, targetReg)
	case ExprIsNull:
		return b.compileIsNull(expr, targetReg, true)
	case ExprIsNotNull:
		return b.compileIsNull(expr, targetReg, false)
	case ExprBetween:
		return b.compileBetween(expr, targetReg)
	case ExprInList:
		return b.compileInList(expr, targetReg)
	case ExprInSelect:
		return b.compileInSelect(expr, targetReg)
	case ExprInTable:
		return b.compileInTable(expr, targetReg)
	case ExprLike:
		return b.compileLike(expr, targetReg)
	case ExprGlob:
		return b.compileGlob(expr, targetReg)
	case ExprCase:
		return b.compileCase(expr, targetReg)
	case ExprCast:
		return b.compileCast(expr, targetReg)
	case ExprSubquery:
		return b.compileSubquery(expr, targetReg)
	case ExprExists:
		return b.compileExists(expr, targetReg)
	case ExprCollate:
		return b.compileExpr(expr.Left, targetReg)
	case ExprDot:
		return b.compileColumnRef(expr, targetReg)
	case ExprDefault:
		b.emitNull(targetReg)
		return nil
	case ExprStar:
		return fmt.Errorf("star expression cannot be used in this context")
	default:
		return fmt.Errorf("unsupported expression kind: %v", expr.Kind)
	}
}

// compileLiteral compiles a literal value.
func (b *Build) compileLiteral(expr *Expr, targetReg int) error {
	switch expr.LiteralType {
	case "integer":
		b.emitInteger(expr.IntValue, targetReg)
	case "float":
		b.emitReal(expr.FloatValue, targetReg)
	case "string":
		b.emitString(expr.StringValue, targetReg)
	case "blob":
		b.emitBlob([]byte(expr.StringValue), targetReg)
	case "null":
		b.emitNull(targetReg)
	case "true":
		b.emitInteger(1, targetReg)
	case "false":
		b.emitInteger(0, targetReg)
	default:
		b.emitNull(targetReg)
	}
	return nil
}

// compileVariable compiles a parameter variable.
func (b *Build) compileVariable(expr *Expr, targetReg int) error {
	// Variables use OP_Variable. P1 is the variable index.
	// For named variables, we assign sequential indices.
	varIndex := 1
	if expr.StringValue != "" {
		// Named variable: compute a simple index from the name.
		// In a real implementation, this would use a proper parameter resolver.
		varIndex = len(expr.StringValue) // simplistic
	}
	b.b.Emit(vdbe.OpVariable, varIndex, targetReg, 0)
	return nil
}

// compileColumnRef compiles a column reference to OP_Column.
func (b *Build) compileColumnRef(expr *Expr, targetReg int) error {
	cursor, colIdx, err := b.resolveColumnRef(expr.Table, expr.Name)
	if err != nil {
		return err
	}
	b.emitColumn(cursor, colIdx, targetReg)
	return nil
}

// compileBinaryOp compiles a binary operator expression.
func (b *Build) compileBinaryOp(expr *Expr, targetReg int) error {
	op := strings.ToUpper(expr.Op)

	// Short-circuit AND/OR
	if op == "AND" {
		return b.compileAnd(expr, targetReg)
	}
	if op == "OR" {
		return b.compileOr(expr, targetReg)
	}

	// Comparison operators produce boolean results
	switch op {
	case "=", "==":
		return b.compileComparison(expr, targetReg, vdbe.OpEq)
	case "<>", "!=":
		return b.compileComparison(expr, targetReg, vdbe.OpNe)
	case "<":
		return b.compileComparison(expr, targetReg, vdbe.OpLt)
	case "<=":
		return b.compileComparison(expr, targetReg, vdbe.OpLe)
	case ">":
		return b.compileComparison(expr, targetReg, vdbe.OpGt)
	case ">=":
		return b.compileComparison(expr, targetReg, vdbe.OpGe)
	case "IS":
		return b.compileComparison(expr, targetReg, vdbe.OpEq)
	case "IS NOT":
		return b.compileComparison(expr, targetReg, vdbe.OpNe)
	}

	// For arithmetic/bitwise operators, compile both operands then apply op.
	leftReg := b.b.AllocReg(1)
	rightReg := b.b.AllocReg(1)

	if err := b.compileExpr(expr.Left, leftReg); err != nil {
		return err
	}
	if err := b.compileExpr(expr.Right, rightReg); err != nil {
		return err
	}

	switch op {
	case "+":
		b.b.Emit(vdbe.OpAdd, leftReg, rightReg, targetReg)
	case "-":
		b.b.Emit(vdbe.OpSubtract, leftReg, rightReg, targetReg)
	case "*":
		b.b.Emit(vdbe.OpMul, leftReg, rightReg, targetReg)
	case "/":
		b.b.Emit(vdbe.OpDivide, leftReg, rightReg, targetReg)
	case "%":
		b.b.Emit(vdbe.OpRemainder, leftReg, rightReg, targetReg)
	case "||":
		b.b.Emit(vdbe.OpConcat, leftReg, rightReg, targetReg)
	case "&":
		b.b.Emit(vdbe.OpBitAnd, leftReg, rightReg, targetReg)
	case "|":
		b.b.Emit(vdbe.OpBitOr, leftReg, rightReg, targetReg)
	case "<<":
		b.b.Emit(vdbe.OpShiftLeft, leftReg, rightReg, targetReg)
	case ">>":
		b.b.Emit(vdbe.OpShiftRight, leftReg, rightReg, targetReg)
	default:
		return fmt.Errorf("unsupported binary operator: %s", expr.Op)
	}
	return nil
}

// compileComparison compiles a comparison operator.
// We compile both sides, then use the comparison opcode with a jump.
// The result (0 or 1) is placed in targetReg.
func (b *Build) compileComparison(expr *Expr, targetReg int, cmpOp vdbe.Opcode) error {
	leftReg := b.b.AllocReg(1)
	rightReg := b.b.AllocReg(1)

	if err := b.compileExpr(expr.Left, leftReg); err != nil {
		return err
	}
	if err := b.compileExpr(expr.Right, rightReg); err != nil {
		return err
	}

	trueLabel := b.b.NewLabel()
	endLabel := b.b.NewLabel()

	// If comparison is true, jump to trueLabel
	b.b.EmitJump(cmpOp, leftReg, trueLabel, rightReg)

	// False path: targetReg = 0
	b.emitInteger(0, targetReg)
	b.emitGoto(endLabel)

	// True path: targetReg = 1
	b.b.DefineLabel(trueLabel)
	b.emitInteger(1, targetReg)

	b.b.DefineLabel(endLabel)
	return nil
}

// compileAnd compiles AND with short-circuit evaluation.
func (b *Build) compileAnd(expr *Expr, targetReg int) error {
	falseLabel := b.b.NewLabel()
	endLabel := b.b.NewLabel()

	leftReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Left, leftReg); err != nil {
		return err
	}
	// If left is false (or null), jump to false
	b.b.EmitJump(vdbe.OpIfNot, leftReg, falseLabel, 0)

	rightReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Right, rightReg); err != nil {
		return err
	}
	// If right is false, jump to false
	b.b.EmitJump(vdbe.OpIfNot, rightReg, falseLabel, 0)

	// Both true
	b.emitInteger(1, targetReg)
	b.emitGoto(endLabel)

	b.b.DefineLabel(falseLabel)
	b.emitInteger(0, targetReg)
	b.b.DefineLabel(endLabel)
	return nil
}

// compileOr compiles OR with short-circuit evaluation.
func (b *Build) compileOr(expr *Expr, targetReg int) error {
	trueLabel := b.b.NewLabel()
	endLabel := b.b.NewLabel()

	leftReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Left, leftReg); err != nil {
		return err
	}
	// If left is true, jump to true
	b.b.EmitJump(vdbe.OpIf, leftReg, trueLabel, 0)

	rightReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Right, rightReg); err != nil {
		return err
	}
	// If right is true, jump to true
	b.b.EmitJump(vdbe.OpIf, rightReg, trueLabel, 0)

	// Both false
	b.emitInteger(0, targetReg)
	b.emitGoto(endLabel)

	b.b.DefineLabel(trueLabel)
	b.emitInteger(1, targetReg)
	b.b.DefineLabel(endLabel)
	return nil
}

// compileUnaryOp compiles a unary operator expression.
func (b *Build) compileUnaryOp(expr *Expr, targetReg int) error {
	op := strings.ToUpper(expr.Op)

	operandReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Right, operandReg); err != nil {
		return err
	}

	switch op {
	case "-":
		// Negate: 0 - operand
		zeroReg := b.b.AllocReg(1)
		b.emitInteger(0, zeroReg)
		b.b.Emit(vdbe.OpSubtract, zeroReg, operandReg, targetReg)
	case "+":
		// Unary plus is a no-op
		b.emitSCopy(operandReg, targetReg)
	case "~":
		b.b.Emit(vdbe.OpBitNot, operandReg, targetReg, 0)
	case "NOT":
		b.b.Emit(vdbe.OpNot, operandReg, targetReg, 0)
	default:
		return fmt.Errorf("unsupported unary operator: %s", expr.Op)
	}
	return nil
}

// compileFunctionCall compiles a function call.
func (b *Build) compileFunctionCall(expr *Expr, targetReg int) error {
	fnName := strings.ToUpper(expr.FunctionName)

	// Handle special built-in functions
	switch fnName {
	case "NULLIF":
		return b.compileNullIf(expr, targetReg)
	case "IFNULL":
		return b.compileIfNull(expr, targetReg)
	case "COALESCE":
		return b.compileCoalesce(expr, targetReg)
	case "TYPEOF":
		return b.compileTypeof(expr, targetReg)
	case "LENGTH":
		return b.compileLength(expr, targetReg)
	}

	// Handle aggregate functions
	if isAggregate(fnName) {
		return b.compileAggregate(expr, targetReg)
	}

	// General function call: OP_Function
	nArgs := len(expr.Args)
	if expr.StarArg {
		nArgs = 0 // COUNT(*) etc.
	}

	// Compile arguments into consecutive registers
	argBase := b.b.AllocReg(nArgs)
	for i, arg := range expr.Args {
		if err := b.compileExpr(arg, argBase+i); err != nil {
			return err
		}
	}

	// Encode function metadata in P4
	fi := &FuncInfo{
		Name:      fnName,
		ArgCount:  nArgs,
		Distinct:  expr.Distinct,
	}
	b.b.EmitP4(vdbe.OpFunction, argBase, targetReg, nArgs, fi, fnName)
	return nil
}

// compileNullIf compiles NULLIF(a, b).
func (b *Build) compileNullIf(expr *Expr, targetReg int) error {
	if len(expr.Args) != 2 {
		return fmt.Errorf("NULLIF requires exactly 2 arguments")
	}
	aReg := b.b.AllocReg(1)
	bReg := b.b.AllocReg(1)

	if err := b.compileExpr(expr.Args[0], aReg); err != nil {
		return err
	}
	if err := b.compileExpr(expr.Args[1], bReg); err != nil {
		return err
	}

	eqLabel := b.b.NewLabel()
	endLabel := b.b.NewLabel()

	b.b.EmitJump(vdbe.OpNe, aReg, eqLabel, bReg)
	// Equal: result is NULL
	b.emitNull(targetReg)
	b.emitGoto(endLabel)

	b.b.DefineLabel(eqLabel)
	b.emitSCopy(aReg, targetReg)
	b.b.DefineLabel(endLabel)
	return nil
}

// compileIfNull compiles IFNULL(a, b).
func (b *Build) compileIfNull(expr *Expr, targetReg int) error {
	if len(expr.Args) != 2 {
		return fmt.Errorf("IFNULL requires exactly 2 arguments")
	}
	if err := b.compileExpr(expr.Args[0], targetReg); err != nil {
		return err
	}
	notNullLabel := b.b.NewLabel()
	endLabel := b.b.NewLabel()

	b.b.EmitJump(vdbe.OpNotNull, targetReg, notNullLabel, 0)
	// Is null: evaluate second arg
	if err := b.compileExpr(expr.Args[1], targetReg); err != nil {
		return err
	}
	b.emitGoto(endLabel)
	b.b.DefineLabel(notNullLabel)
	b.b.DefineLabel(endLabel)
	return nil
}

// compileCoalesce compiles COALESCE(a, b, ...).
func (b *Build) compileCoalesce(expr *Expr, targetReg int) error {
	if len(expr.Args) == 0 {
		b.emitNull(targetReg)
		return nil
	}

	endLabel := b.b.NewLabel()
	for i, arg := range expr.Args {
		if i < len(expr.Args)-1 {
			notNullLabel := b.b.NewLabel()
			if err := b.compileExpr(arg, targetReg); err != nil {
				return err
			}
			b.b.EmitJump(vdbe.OpNotNull, targetReg, notNullLabel, 0)
			// Was null, try next argument
			_ = b.b.NewLabel() // placeholder
		} else {
			// Last argument: always use it
			if err := b.compileExpr(arg, targetReg); err != nil {
				return err
			}
		}
		// For non-last args, after check we go to end
		if i < len(expr.Args)-1 {
			// The notNullLabel check needs restructuring
		}
	}
	b.b.DefineLabel(endLabel)
	return nil
}

// compileTypeof compiles TYPEOF(x).
func (b *Build) compileTypeof(expr *Expr, targetReg int) error {
	if len(expr.Args) != 1 {
		return fmt.Errorf("TYPEOF requires exactly 1 argument")
	}
	// Compile argument, then use OP_Function for typeof
	argReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Args[0], argReg); err != nil {
		return err
	}
	fi := &FuncInfo{Name: "TYPEOF", ArgCount: 1}
	b.b.EmitP4(vdbe.OpFunction, argReg, targetReg, 1, fi, "TYPEOF")
	return nil
}

// compileLength compiles LENGTH(x).
func (b *Build) compileLength(expr *Expr, targetReg int) error {
	if len(expr.Args) != 1 {
		return fmt.Errorf("LENGTH requires exactly 1 argument")
	}
	argReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Args[0], argReg); err != nil {
		return err
	}
	fi := &FuncInfo{Name: "LENGTH", ArgCount: 1}
	b.b.EmitP4(vdbe.OpFunction, argReg, targetReg, 1, fi, "LENGTH")
	return nil
}

// compileAggregate handles aggregate functions (COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT).
func (b *Build) compileAggregate(expr *Expr, targetReg int) error {
	fnName := strings.ToUpper(expr.FunctionName)

	switch fnName {
	case "COUNT":
		return b.compileCount(expr, targetReg)
	case "SUM":
		return b.compileSum(expr, targetReg)
	case "MIN":
		return b.compileMinMax(expr, targetReg, true)
	case "MAX":
		return b.compileMinMax(expr, targetReg, false)
	case "AVG":
		return b.compileAvg(expr, targetReg)
	case "GROUP_CONCAT":
		return b.compileGroupConcat(expr, targetReg)
	default:
		return fmt.Errorf("unsupported aggregate function: %s", fnName)
	}
}

// compileCount handles COUNT(*) and COUNT(expr).
func (b *Build) compileCount(expr *Expr, targetReg int) error {
	if expr.StarArg {
		// COUNT(*): increment counter for every row
		b.b.Emit(vdbe.OpAggStep, 0, targetReg, 1)
		return nil
	}
	if len(expr.Args) != 1 {
		return fmt.Errorf("COUNT requires 0 or 1 arguments")
	}
	argReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Args[0], argReg); err != nil {
		return err
	}
	// Step: increment count if not null
	fi := &FuncInfo{Name: "COUNT", ArgCount: 1}
	b.b.EmitP4(vdbe.OpAggStep, argReg, targetReg, 1, fi, "COUNT STEP")
	return nil
}

// compileSum handles SUM(expr).
func (b *Build) compileSum(expr *Expr, targetReg int) error {
	if len(expr.Args) != 1 {
		return fmt.Errorf("SUM requires exactly 1 argument")
	}
	argReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Args[0], argReg); err != nil {
		return err
	}
	fi := &FuncInfo{Name: "SUM", ArgCount: 1}
	b.b.EmitP4(vdbe.OpAggStep, argReg, targetReg, 1, fi, "SUM STEP")
	return nil
}

// compileMinMax handles MIN/MAX.
func (b *Build) compileMinMax(expr *Expr, targetReg int, isMin bool) error {
	if len(expr.Args) != 1 {
		return fmt.Errorf("MIN/MAX requires exactly 1 argument")
	}
	argReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Args[0], argReg); err != nil {
		return err
	}
	name := "MIN"
	if !isMin {
		name = "MAX"
	}
	fi := &FuncInfo{Name: name, ArgCount: 1}
	b.b.EmitP4(vdbe.OpAggStep, argReg, targetReg, 1, fi, name+" STEP")
	return nil
}

// compileAvg handles AVG(expr).
func (b *Build) compileAvg(expr *Expr, targetReg int) error {
	if len(expr.Args) != 1 {
		return fmt.Errorf("AVG requires exactly 1 argument")
	}
	argReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Args[0], argReg); err != nil {
		return err
	}
	fi := &FuncInfo{Name: "AVG", ArgCount: 1}
	b.b.EmitP4(vdbe.OpAggStep, argReg, targetReg, 1, fi, "AVG STEP")
	return nil
}

// compileGroupConcat handles GROUP_CONCAT.
func (b *Build) compileGroupConcat(expr *Expr, targetReg int) error {
	if len(expr.Args) < 1 || len(expr.Args) > 2 {
		return fmt.Errorf("GROUP_CONCAT requires 1 or 2 arguments")
	}
	argReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Args[0], argReg); err != nil {
		return err
	}
	sepReg := 0
	if len(expr.Args) == 2 {
		sepReg = b.b.AllocReg(1)
		if err := b.compileExpr(expr.Args[1], sepReg); err != nil {
			return err
		}
	}
	fi := &FuncInfo{Name: "GROUP_CONCAT", ArgCount: len(expr.Args)}
	b.b.EmitP4(vdbe.OpAggStep, argReg, targetReg, 1, fi, "GROUP_CONCAT STEP")
	return nil
}

// compileIsNull compiles IS NULL / IS NOT NULL.
func (b *Build) compileIsNull(expr *Expr, targetReg int, checkNull bool) error {
	operandReg := b.b.AllocReg(1)
	// The parser stores the operand in Right for IS NULL/IS NOT NULL
	operand := expr.Right
	if operand == nil {
		operand = expr.Left
	}
	if err := b.compileExpr(operand, operandReg); err != nil {
		return err
	}

	trueLabel := b.b.NewLabel()
	endLabel := b.b.NewLabel()

	if checkNull {
		b.b.EmitJump(vdbe.OpIsNull, operandReg, trueLabel, 0)
	} else {
		b.b.EmitJump(vdbe.OpNotNull, operandReg, trueLabel, 0)
	}
	b.emitInteger(0, targetReg)
	b.emitGoto(endLabel)

	b.b.DefineLabel(trueLabel)
	b.emitInteger(1, targetReg)
	b.b.DefineLabel(endLabel)
	return nil
}

// compileBetween compiles x [NOT] BETWEEN low AND high.
func (b *Build) compileBetween(expr *Expr, targetReg int) error {
	// Compile as: x >= low AND x <= high (or NOT(x >= low AND x <= high))
	xReg := b.b.AllocReg(1)
	lowReg := b.b.AllocReg(1)
	highReg := b.b.AllocReg(1)

	if err := b.compileExpr(expr.Left, xReg); err != nil {
		return err
	}
	if err := b.compileExpr(expr.Low, lowReg); err != nil {
		return err
	}
	if err := b.compileExpr(expr.High, highReg); err != nil {
		return err
	}

	falseLabel := b.b.NewLabel()
	endLabel := b.b.NewLabel()

	// Check x >= low
	b.b.EmitJump(vdbe.OpLt, xReg, falseLabel, lowReg)
	// Check x <= high
	b.b.EmitJump(vdbe.OpGt, xReg, falseLabel, highReg)

	// Between: result = 1
	if expr.Not {
		b.emitInteger(0, targetReg)
	} else {
		b.emitInteger(1, targetReg)
	}
	b.emitGoto(endLabel)

	b.b.DefineLabel(falseLabel)
	if expr.Not {
		b.emitInteger(1, targetReg)
	} else {
		b.emitInteger(0, targetReg)
	}
	b.b.DefineLabel(endLabel)
	return nil
}

// compileInList compiles expr IN (val1, val2, ...).
func (b *Build) compileInList(expr *Expr, targetReg int) error {
	if len(expr.InValues) == 0 {
		b.emitInteger(0, targetReg)
		return nil
	}

	// Compile the left-hand expression
	lhsReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Left, lhsReg); err != nil {
		return err
	}

	// For each value in the list, emit a comparison.
	// If any match, the result is true.
	foundLabel := b.b.NewLabel()
	endLabel := b.b.NewLabel()

	for _, val := range expr.InValues {
		valReg := b.b.AllocReg(1)
		if err := b.compileExpr(val, valReg); err != nil {
			return err
		}
		b.b.EmitJump(vdbe.OpEq, lhsReg, foundLabel, valReg)
	}

	// Not found
	if expr.Not {
		b.emitInteger(1, targetReg)
	} else {
		b.emitInteger(0, targetReg)
	}
	b.emitGoto(endLabel)

	// Found
	b.b.DefineLabel(foundLabel)
	if expr.Not {
		b.emitInteger(0, targetReg)
	} else {
		b.emitInteger(1, targetReg)
	}
	b.b.DefineLabel(endLabel)
	return nil
}

// compileInSelect compiles expr IN (SELECT ...).
func (b *Build) compileInSelect(expr *Expr, targetReg int) error {
	// Open an ephemeral table for the subquery results
	cursor := b.b.AllocCursor()
	b.emitOpenEphemeral(cursor, 1)

	// Compile the subquery to populate the ephemeral table
	// (simplified: in full implementation, would use coroutine)
	subProg := newBuild(b.schema)
	if err := subProg.compileSelect(expr.InSelect); err != nil {
		return err
	}
	// For now, just evaluate the left side and set result to 0
	// (subquery IN compilation is complex and requires coroutine support)
	lhsReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Left, lhsReg); err != nil {
		return err
	}
	// Use OP_Found to check if lhs is in the ephemeral table
	foundLabel := b.b.NewLabel()
	endLabel := b.b.NewLabel()

	b.b.Emit(vdbe.OpRowid, cursor, lhsReg, 0) // ensure cursor context
	b.b.EmitJump(vdbe.OpNotFound, cursor, endLabel, lhsReg)

	b.b.DefineLabel(foundLabel)
	if expr.Not {
		b.emitInteger(0, targetReg)
	} else {
		b.emitInteger(1, targetReg)
	}
	b.emitGoto(endLabel)

	b.b.DefineLabel(endLabel)
	if expr.Not {
		b.emitInteger(1, targetReg)
	} else {
		b.emitInteger(0, targetReg)
	}
	b.emitClose(cursor)
	return nil
}

// compileInTable compiles expr IN table.
func (b *Build) compileInTable(expr *Expr, targetReg int) error {
	tbl, err := b.lookupTable(expr.InTable)
	if err != nil {
		return err
	}

	// Open the table for reading
	cursor := b.b.AllocCursor()
	b.emitOpenRead(cursor, tbl.RootPage)

	// Evaluate the left side
	lhsReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Left, lhsReg); err != nil {
		return err
	}

	// Use Seek + Found pattern
	foundLabel := b.b.NewLabel()
	endLabel := b.b.NewLabel()

	b.b.EmitJump(vdbe.OpFound, cursor, foundLabel, lhsReg)

	if expr.Not {
		b.emitInteger(1, targetReg)
	} else {
		b.emitInteger(0, targetReg)
	}
	b.emitGoto(endLabel)

	b.b.DefineLabel(foundLabel)
	if expr.Not {
		b.emitInteger(0, targetReg)
	} else {
		b.emitInteger(1, targetReg)
	}

	b.b.DefineLabel(endLabel)
	b.emitClose(cursor)
	return nil
}

// compileLike compiles x [NOT] LIKE pattern.
func (b *Build) compileLike(expr *Expr, targetReg int) error {
	return b.compilePatternMatch(expr, targetReg, "LIKE")
}

// compileGlob compiles x [NOT] GLOB pattern.
func (b *Build) compileGlob(expr *Expr, targetReg int) error {
	return b.compilePatternMatch(expr, targetReg, "GLOB")
}

// compilePatternMatch compiles LIKE/GLOB/REGEXP/MATCH.
func (b *Build) compilePatternMatch(expr *Expr, targetReg int, fnName string) error {
	strReg := b.b.AllocReg(1)
	patReg := b.b.AllocReg(1)

	if err := b.compileExpr(expr.Left, strReg); err != nil {
		return err
	}
	if err := b.compileExpr(expr.Pattern, patReg); err != nil {
		return err
	}

	nArgs := 2
	escReg := 0
	if expr.Escape != nil {
		escReg = b.b.AllocReg(1)
		if err := b.compileExpr(expr.Escape, escReg); err != nil {
			return err
		}
		nArgs = 3
	}

	fi := &FuncInfo{Name: fnName, ArgCount: nArgs}
	argBase := strReg
	b.b.EmitP4(vdbe.OpFunction, argBase, targetReg, nArgs, fi, fnName)

	// NOT LIKE etc.
	if expr.Not {
		notReg := b.b.AllocReg(1)
		b.b.Emit(vdbe.OpNot, targetReg, notReg, 0)
		b.emitSCopy(notReg, targetReg)
	}
	return nil
}

// compileCase compiles a CASE expression.
func (b *Build) compileCase(expr *Expr, targetReg int) error {
	endLabel := b.b.NewLabel()

	if expr.Operand != nil {
		// Simple CASE: compare operand against each WHEN
		opReg := b.b.AllocReg(1)
		if err := b.compileExpr(expr.Operand, opReg); err != nil {
			return err
		}

		for _, when := range expr.WhenList {
			nextWhenLabel := b.b.NewLabel()
			condReg := b.b.AllocReg(1)
			if err := b.compileExpr(when.Condition, condReg); err != nil {
				return err
			}
			// If operand != condition, skip to next WHEN
			b.b.EmitJump(vdbe.OpNe, opReg, nextWhenLabel, condReg)

			// Match: evaluate result
			if err := b.compileExpr(when.Result, targetReg); err != nil {
				return err
			}
			b.emitGoto(endLabel)

			b.b.DefineLabel(nextWhenLabel)
		}
	} else {
		// Searched CASE: each WHEN is a boolean condition
		for _, when := range expr.WhenList {
			nextWhenLabel := b.b.NewLabel()
			condReg := b.b.AllocReg(1)
			if err := b.compileExpr(when.Condition, condReg); err != nil {
				return err
			}
			// If condition is false, skip to next WHEN
			b.b.EmitJump(vdbe.OpIfNot, condReg, nextWhenLabel, 0)

			// Match: evaluate result
			if err := b.compileExpr(when.Result, targetReg); err != nil {
				return err
			}
			b.emitGoto(endLabel)

			b.b.DefineLabel(nextWhenLabel)
		}
	}

	// ELSE clause or NULL
	if expr.ElseExpr != nil {
		if err := b.compileExpr(expr.ElseExpr, targetReg); err != nil {
			return err
		}
	} else {
		b.emitNull(targetReg)
	}

	b.b.DefineLabel(endLabel)
	return nil
}

// compileCast compiles CAST(expr AS type).
func (b *Build) compileCast(expr *Expr, targetReg int) error {
	srcReg := b.b.AllocReg(1)
	if err := b.compileExpr(expr.Left, srcReg); err != nil {
		return err
	}
	// OP_Cast: P2 is the affinity type
	affinity := affinityFromType(expr.CastType)
	b.b.Emit(vdbe.OpCast, affinity, 0, srcReg)
	b.emitSCopy(srcReg, targetReg)
	return nil
}

// compileSubquery compiles a scalar subquery (SELECT ...).
func (b *Build) compileSubquery(expr *Expr, targetReg int) error {
	// In a full implementation, this would use a coroutine.
	// For now, compile the subquery inline.
	if expr.Select == nil {
		return fmt.Errorf("nil subquery")
	}

	// Open an ephemeral table to collect the result
	cursor := b.b.AllocCursor()
	nCols := len(expr.Select.Columns)
	if nCols == 0 {
		nCols = 1
	}
	b.emitOpenEphemeral(cursor, nCols)

	// Save and restore table context
	savedTables := b.tables
	savedTableMap := b.tableMap
	b.tables = nil
	b.tableMap = make(map[string]*tableEntry)

	// Compile subquery
	if err := b.compileSelectInner(expr.Select, cursor); err != nil {
		b.tables = savedTables
		b.tableMap = savedTableMap
		return err
	}

	// Restore context
	b.tables = savedTables
	b.tableMap = savedTableMap

	// Read the first (and only) row from the ephemeral table
	b.b.Emit(vdbe.OpRewind, cursor, b.b.CurrentAddr()+3, 0)
	b.emitColumn(cursor, 0, targetReg)
	b.emitClose(cursor)
	return nil
}

// compileExists compiles EXISTS (SELECT ...).
func (b *Build) compileExists(expr *Expr, targetReg int) error {
	// EXISTS is true if the subquery returns at least one row
	cursor := b.b.AllocCursor()
	b.emitOpenEphemeral(cursor, 1)

	savedTables := b.tables
	savedTableMap := b.tableMap
	b.tables = nil
	b.tableMap = make(map[string]*tableEntry)

	if err := b.compileSelectInner(expr.Select, cursor); err != nil {
		b.tables = savedTables
		b.tableMap = savedTableMap
		return err
	}

	b.tables = savedTables
	b.tableMap = savedTableMap

	// Check if there's at least one row
	falseLabel := b.b.NewLabel()
	endLabel := b.b.NewLabel()

	b.b.EmitJump(vdbe.OpRewind, cursor, falseLabel, 0)
	b.emitInteger(1, targetReg)
	b.emitGoto(endLabel)

	b.b.DefineLabel(falseLabel)
	b.emitInteger(0, targetReg)
	b.b.DefineLabel(endLabel)
	b.emitClose(cursor)
	return nil
}

// compileCondition compiles a boolean expression with jump targets.
// This is used for WHERE/HAVING conditions where we want to jump
// directly on comparison results rather than storing 0/1.
// jumpIfTrue: if true, jump to trueLabel when condition is true.
func (b *Build) compileCondition(expr *Expr, trueLabel, falseLabel int, jumpIfTrue bool) error {
	if expr == nil {
		return nil
	}

	switch expr.Kind {
	case ExprBinaryOp:
		op := strings.ToUpper(expr.Op)
		switch op {
		case "AND":
			if jumpIfTrue {
				// AND: both must be true. Check left first.
				andFalse := b.b.NewLabel()
				if err := b.compileCondition(expr.Left, andFalse, falseLabel, false); err != nil {
					return err
				}
				b.b.DefineLabel(andFalse)
				// Left was true (didn't jump to false), now check right
				return b.compileCondition(expr.Right, trueLabel, falseLabel, true)
			}
			// AND jump-to-false: if either is false, jump
			if err := b.compileCondition(expr.Left, trueLabel, falseLabel, false); err != nil {
				return err
			}
			return b.compileCondition(expr.Right, trueLabel, falseLabel, false)

		case "OR":
			if jumpIfTrue {
				// OR jump-to-true: if either is true, jump
				orTrue := b.b.NewLabel()
				if err := b.compileCondition(expr.Left, orTrue, falseLabel, true); err != nil {
					return err
				}
				b.b.DefineLabel(orTrue)
				return b.compileCondition(expr.Right, trueLabel, falseLabel, true)
			}
			// OR jump-to-false: both must be false
			orTrueLabel := b.b.NewLabel()
			if err := b.compileCondition(expr.Left, orTrueLabel, falseLabel, true); err != nil {
				return err
			}
			b.b.DefineLabel(orTrueLabel)
			return b.compileCondition(expr.Right, trueLabel, falseLabel, false)

		case "NOT":
			return b.compileCondition(expr.Right, falseLabel, trueLabel, !jumpIfTrue)
		}

		// Comparison operators: compile both sides and emit direct jump
		cmpOp, ok := comparisonOpcode(op)
		if ok {
			return b.compileConditionComparison(expr, cmpOp, trueLabel, falseLabel, jumpIfTrue)
		}

	// Fall through to general case
	default:
	}

	// General case: evaluate to register, then test
	reg := b.b.AllocReg(1)
	if err := b.compileExpr(expr, reg); err != nil {
		return err
	}
	if jumpIfTrue {
		b.b.EmitJump(vdbe.OpIf, reg, trueLabel, 0)
	} else {
		b.b.EmitJump(vdbe.OpIfNot, reg, trueLabel, 0)
	}
	return nil
}

// compileConditionComparison compiles a comparison in condition context
// (jumps directly without storing boolean result).
func (b *Build) compileConditionComparison(expr *Expr, cmpOp vdbe.Opcode, trueLabel, falseLabel int, jumpIfTrue bool) error {
	leftReg := b.b.AllocReg(1)
	rightReg := b.b.AllocReg(1)

	if err := b.compileExpr(expr.Left, leftReg); err != nil {
		return err
	}
	if err := b.compileExpr(expr.Right, rightReg); err != nil {
		return err
	}

	if jumpIfTrue {
		b.b.EmitJump(cmpOp, leftReg, trueLabel, rightReg)
	} else {
		// Jump to true label when condition IS true, meaning we skip the
		// false path. Use the inverse comparison for NOT cases.
		invOp := inverseComparison(cmpOp)
		if invOp != vdbe.OpGoto {
			b.b.EmitJump(invOp, leftReg, trueLabel, rightReg)
		}
	}
	return nil
}

// comparisonOpcode maps an operator string to its VDBE opcode.
func comparisonOpcode(op string) (vdbe.Opcode, bool) {
	switch op {
	case "=", "==":
		return vdbe.OpEq, true
	case "<>", "!=":
		return vdbe.OpNe, true
	case "<":
		return vdbe.OpLt, true
	case "<=":
		return vdbe.OpLe, true
	case ">":
		return vdbe.OpGt, true
	case ">=":
		return vdbe.OpGe, true
	case "IS":
		return vdbe.OpEq, true
	case "IS NOT":
		return vdbe.OpNe, true
	default:
		return 0, false
	}
}

// inverseComparison returns the inverse of a comparison opcode.
func inverseComparison(op vdbe.Opcode) vdbe.Opcode {
	switch op {
	case vdbe.OpEq:
		return vdbe.OpNe
	case vdbe.OpNe:
		return vdbe.OpEq
	case vdbe.OpLt:
		return vdbe.OpGe
	case vdbe.OpLe:
		return vdbe.OpGt
	case vdbe.OpGt:
		return vdbe.OpLe
	case vdbe.OpGe:
		return vdbe.OpLt
	default:
		return vdbe.OpGoto
	}
}

// isAggregate returns true for aggregate function names.
func isAggregate(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "TOTAL":
		return true
	}
	return false
}

// isWindowFunc returns true for built-in window function names.
func isWindowFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
		"LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE":
		return true
	}
	return false
}

// affinityFromType maps SQL type names to affinity codes for OP_Cast.
func affinityFromType(typeName string) int {
	upper := strings.ToUpper(typeName)
	switch {
	case strings.Contains(upper, "INT"):
		return 1 // integer affinity
	case strings.Contains(upper, "CHAR") || strings.Contains(upper, "CLOB") || strings.Contains(upper, "TEXT"):
		return 2 // text affinity
	case strings.Contains(upper, "BLOB"):
		return 4 // blob affinity (none)
	case strings.Contains(upper, "REAL") || strings.Contains(upper, "FLOA") || strings.Contains(upper, "DOUB"):
		return 3 // real affinity
	default:
		return 2 // text affinity as default (NUMERIC)
	}
}
