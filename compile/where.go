package compile

import "strings"

// scanType describes how a table will be accessed.
type scanType int

const (
	scanTableFull  scanType = iota // Full table scan (Rewind + Next)
	scanIndexEq                    // Index point query (equality on all index cols)
	scanIndexRange                 // Index range scan
)

// indexScanPlan describes how to access a table using an index.
type indexScanPlan struct {
	TableIdx   int       // Index into FROM clause tables list
	ScanType   scanType  // How to scan
	Index      *IndexInfo // Index to use (nil for full scan)
	IdxCursor  int       // VDBE cursor for the index (-1 if unused)
	TblCursor  int       // VDBE cursor for the table

	// Equality constraints matching leading index columns.
	EqCols  []int   // Column indices in the table
	EqExprs []*Expr // Expressions for each equality column

	// Range constraint on the column after the equality prefix.
	RangeCol       int    // -1 if none
	RangeLower     *Expr  // nil if none
	RangeUpper     *Expr  // nil if none
	LowerInclusive bool   // true for >=, false for >
	UpperInclusive bool   // true for <=, false for <

	// Terms handled by this plan (should not be re-evaluated).
	HandledTerms []*whereTerm

	Cost    float64 // Estimated cost
	EstRows float64 // Estimated output rows
}

// queryPlan is the result of planning a query.
type queryPlan struct {
	TablePlans     []*indexScanPlan
	RemainingTerms []*whereTerm
	WC             *whereClause
}

// planQuery analyzes a WHERE clause and determines the best access path
// for each table in the FROM clause.
func (b *Build) planQuery(where *Expr) *queryPlan {
	wc := analyzeWhere(where, b.tables)

	plan := &queryPlan{
		TablePlans: make([]*indexScanPlan, len(b.tables)),
		WC:         wc,
	}

	for i := range b.tables {
		plan.TablePlans[i] = b.bestPlan(i, wc)
	}

	// Collect remaining (unconsumed) terms
	for _, t := range wc.Terms {
		if !t.Consumed {
			plan.RemainingTerms = append(plan.RemainingTerms, t)
		}
	}

	return plan
}

// bestPlan finds the best access path for a table.
func (b *Build) bestPlan(tableIdx int, wc *whereClause) *indexScanPlan {
	terms := wc.indexableTermsForTable(tableIdx)
	tbl := b.tables[tableIdx]

	// Default: full table scan
	best := &indexScanPlan{
		TableIdx:  tableIdx,
		ScanType:  scanTableFull,
		TblCursor: tbl.cursor,
		Cost:      1000.0,
		EstRows:   1000.0,
	}

	// Try PK lookup
	if pk := b.tryPKLookup(tableIdx, tbl, terms); pk != nil && pk.Cost < best.Cost {
		best = pk
	}

	// Try each index
	indexes := indexesForTable(b.schema, tbl.name)
	for _, idx := range indexes {
		if ip := b.tryIndex(tableIdx, tbl, idx, terms); ip != nil && ip.Cost < best.Cost {
			best = ip
		}
	}

	return best
}

// tryPKLookup checks if there's a direct primary key equality constraint.
func (b *Build) tryPKLookup(tableIdx int, tbl *tableEntry, terms []*whereTerm) *indexScanPlan {
	for _, t := range terms {
		if t.hasEq() && t.ColIdx == 0 && isPK(tbl, 0) {
			return &indexScanPlan{
				TableIdx:     tableIdx,
				ScanType:     scanIndexEq,
				TblCursor:    tbl.cursor,
				EqCols:       []int{0},
				EqExprs:      []*Expr{t.EqExpr},
				HandledTerms: []*whereTerm{t},
				Cost:         2.0,
				EstRows:      1.0,
			}
		}
	}
	return nil
}

// tryIndex tries to use a specific index for the given table.
func (b *Build) tryIndex(tableIdx int, tbl *tableEntry, idx *IndexInfo, terms []*whereTerm) *indexScanPlan {
	nIdxCols := len(idx.Columns)
	if nIdxCols == 0 {
		return nil
	}

	// Map index columns to table column indices
	idxToTbl := make([]int, nIdxCols)
	for i, ic := range idx.Columns {
		idxToTbl[i] = tbl.table.FindColumn(ic.Name)
	}

	plan := &indexScanPlan{
		TableIdx:  tableIdx,
		Index:     idx,
		TblCursor: tbl.cursor,
		RangeCol:  -1,
	}

	// Match equality constraints to leading index columns
	consumed := make(map[int]bool) // tracks term indices
	eqCols, eqExprs := matchEqToIndex(idxToTbl, terms, consumed)
	plan.EqCols = eqCols
	plan.EqExprs = eqExprs

	// Mark consumed terms
	for i, t := range terms {
		if consumed[i] {
			t.Consumed = true
			plan.HandledTerms = append(plan.HandledTerms, t)
		}
	}

	nEq := len(eqCols)

	// All columns matched → point query
	if nEq == nIdxCols {
		plan.ScanType = scanIndexEq
		plan.Cost = 2.0 + float64(nEq)*0.1
		plan.EstRows = 1.0
		return plan
	}

	// Try constraints on the next index column
	if nEq < nIdxCols {
		nextTblCol := idxToTbl[nEq]
		if nextTblCol >= 0 {
			// BETWEEN
			for i, t := range terms {
				if consumed[i] || t.ColIdx != nextTblCol {
					continue
				}
				if t.hasBetween() {
					plan.ScanType = scanIndexRange
					plan.RangeCol = nextTblCol
					plan.RangeLower = t.LowExpr
					plan.RangeUpper = t.HighExpr
					plan.LowerInclusive = true
					plan.UpperInclusive = true
					t.Consumed = true
					consumed[i] = true
					plan.HandledTerms = append(plan.HandledTerms, t)
					plan.Cost = 50.0
					plan.EstRows = 100.0
					return plan
				}
			}

			// Range constraints
			lower, lIncl, upper, uIncl := findBounds(nextTblCol, terms, consumed)
			if lower != nil || upper != nil {
				plan.ScanType = scanIndexRange
				plan.RangeCol = nextTblCol
				plan.RangeLower = lower
				plan.RangeUpper = upper
				plan.LowerInclusive = lIncl
				plan.UpperInclusive = uIncl
				plan.Cost = 100.0
				plan.EstRows = 200.0
				return plan
			}
		}
	}

	// Partial equality match still helps
	if nEq > 0 {
		plan.ScanType = scanIndexRange
		plan.Cost = 300.0
		plan.EstRows = 500.0
		return plan
	}

	return nil // Index provides no benefit
}

// matchEqToIndex finds equality terms matching leading index columns.
func matchEqToIndex(idxToTbl []int, terms []*whereTerm, consumed map[int]bool) ([]int, []*Expr) {
	var cols []int
	var exprs []*Expr

	for _, tblCol := range idxToTbl {
		found := false
		for i, t := range terms {
			if consumed[i] || !t.hasEq() || t.ColIdx != tblCol {
				continue
			}
			cols = append(cols, tblCol)
			exprs = append(exprs, t.EqExpr)
			consumed[i] = true
			found = true
			break
		}
		if !found {
			break
		}
	}
	return cols, exprs
}

// findBounds finds lower/upper bounds for a column from unconsumed terms.
func findBounds(colIdx int, terms []*whereTerm, consumed map[int]bool) (
	lower *Expr, lowerIncl bool, upper *Expr, upperIncl bool,
) {
	for i, t := range terms {
		if consumed[i] || t.ColIdx != colIdx {
			continue
		}
		if t.isLower() && lower == nil {
			lower = t.RangeExpr
			lowerIncl = t.isInclusive()
			t.Consumed = true
			consumed[i] = true
		}
		if t.isUpper() && upper == nil {
			upper = t.RangeExpr
			upperIncl = t.isInclusive()
			t.Consumed = true
			consumed[i] = true
		}
	}
	return
}

// isPK checks if a column is the primary key.
func isPK(tbl *tableEntry, colIdx int) bool {
	if tbl.table == nil || colIdx >= len(tbl.table.Columns) {
		return false
	}
	return tbl.table.Columns[colIdx].PrimaryKey
}

// hasIndexesForTable checks if there are any indexes for the given table.
func hasIndexesForTable(schema *Schema, tableName string) bool {
	if schema == nil {
		return false
	}
	for _, idx := range schema.Indexes {
		if strings.EqualFold(idx.Table, tableName) {
			return true
		}
	}
	return false
}
