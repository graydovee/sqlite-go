package sqlite

import (
	"sort"
	"strconv"
	"strings"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

// isWindowExpr checks if an expression string contains a window function call.
func isWindowExpr(expr string) bool {
	lower := strings.ToLower(strings.TrimSpace(expr))
	lower = strings.ReplaceAll(lower, " (", "(")
	for _, fn := range []string{
		"row_number(", "rank(", "dense_rank(", "ntile(",
		"lag(", "lead(", "first_value(", "last_value(", "nth_value(",
	} {
		if strings.HasPrefix(lower, fn) {
			return true
		}
	}
	return false
}

// windowFuncInfo describes a parsed window function.
type windowFuncInfo struct {
	fn        string
	args      []string
	partition []string
	orderBy   string
}

// parseWindowFunc parses a window function expression.
func parseWindowFunc(expr string) *windowFuncInfo {
	normalized := strings.ReplaceAll(expr, " (", "(")
	normalized = strings.ReplaceAll(normalized, "( ", "(")
	normalized = strings.ReplaceAll(normalized, " )", ")")
	normalized = strings.TrimSpace(normalized)
	lower := strings.ToLower(normalized)

	for _, fn := range []string{
		"row_number", "rank", "dense_rank", "ntile",
		"lag", "lead", "first_value", "last_value", "nth_value",
	} {
		prefix := fn + "("
		if !strings.HasPrefix(lower, prefix) {
			continue
		}
		// Find closing ) for function args
		depth := 1
		argEnd := len(prefix)
		for argEnd < len(normalized) {
			if normalized[argEnd] == '(' {
				depth++
			} else if normalized[argEnd] == ')' {
				depth--
				if depth == 0 {
					break
				}
			}
			argEnd++
		}
		argsStr := strings.TrimSpace(normalized[len(prefix):argEnd])
		var args []string
		if argsStr != "" && argsStr != "*" {
			for _, a := range strings.Split(argsStr, ",") {
				args = append(args, strings.TrimSpace(a))
			}
		}

		rest := strings.TrimSpace(normalized[argEnd+1:])
		var partitionExprs []string
		var orderByStr string
		restLower := strings.ToLower(rest)
		if strings.HasPrefix(restLower, "over") {
			overRest := strings.TrimSpace(rest[4:])
			if len(overRest) > 0 && overRest[0] == '(' {
				depth := 0
				end := -1
				for i := 0; i < len(overRest); i++ {
					if overRest[i] == '(' {
						depth++
					}
					if overRest[i] == ')' {
						depth--
						if depth == 0 {
							end = i
							break
						}
					}
				}
				if end > 0 {
					inner := overRest[1:end]
					innerLower := strings.ToLower(inner)
					pIdx := strings.Index(innerLower, "partition")
					oIdx := strings.Index(innerLower, "order")
					if pIdx >= 0 {
						pStart := pIdx + 9
						pRest := inner[pStart:]
						pRestLower := strings.ToLower(pRest)
						if strings.HasPrefix(strings.TrimSpace(pRestLower), "by") {
							pRest = strings.TrimSpace(pRest)[2:]
						}
						pRest = strings.TrimSpace(pRest)
						endPos := len(pRest)
						if oIdx > pIdx {
							pRestLower2 := strings.ToLower(pRest)
							oi := strings.Index(pRestLower2, "order")
							if oi >= 0 {
								endPos = oi
							}
						}
						partStr := strings.TrimSpace(pRest[:endPos])
						if partStr != "" {
							for _, p := range strings.Split(partStr, ",") {
								partitionExprs = append(partitionExprs, strings.TrimSpace(p))
							}
						}
					}
					if oIdx >= 0 {
						oStart := oIdx + 5
						oRest := inner[oStart:]
						oRestLower := strings.ToLower(oRest)
						if strings.HasPrefix(strings.TrimSpace(oRestLower), "by") {
							oRest = strings.TrimSpace(oRest)[2:]
						}
						orderByStr = strings.TrimSpace(oRest)
					}
				}
			}
		}

		return &windowFuncInfo{
			fn:        fn,
			args:      args,
			partition: partitionExprs,
			orderBy:   orderByStr,
		}
	}
	return nil
}

// computeWindowFunctions evaluates window functions across all rows.
func (db *Database) computeWindowFunctions(
	cols []selectCol, rawData []rawRow, colNames []string,
	resultCols []ResultColumnInfo, tableColumns []columnEntry, args []interface{},
) []Row {
	type winCol struct {
		idx  int
		info *windowFuncInfo
	}
	var winCols []winCol
	for i, c := range cols {
		if isWindowExpr(c.expr) {
			wi := parseWindowFunc(c.expr)
			if wi != nil {
				winCols = append(winCols, winCol{idx: i, info: wi})
			}
		}
	}

	type partition struct {
		rows []int
	}
	var partitions []partition

	if len(winCols) > 0 && len(winCols[0].info.partition) > 0 {
		keyToPartIdx := make(map[string]int)
		for i, rd := range rawData {
			var keyParts []string
			for _, pe := range winCols[0].info.partition {
				val := evalExprWithRow(pe, args, colNames, rd.values)
				keyParts = append(keyParts, memStr(val))
			}
			key := strings.Join(keyParts, "\x00")
			if pidx, ok := keyToPartIdx[key]; ok {
				partitions[pidx].rows = append(partitions[pidx].rows, i)
			} else {
				keyToPartIdx[key] = len(partitions)
				partitions = append(partitions, partition{rows: []int{i}})
			}
		}
	} else {
		p := partition{rows: make([]int, len(rawData))}
		for i := range rawData {
			p.rows[i] = i
		}
		partitions = []partition{p}
	}

	for _, wc := range winCols {
		if wc.info.orderBy == "" {
			continue
		}
		obExpr := stripSortDir(wc.info.orderBy)
		desc := strings.HasSuffix(strings.ToLower(wc.info.orderBy), " desc")
		for pi := range partitions {
			p := &partitions[pi]
			sort.Slice(p.rows, func(a, b int) bool {
				va := evalExprWithRow(obExpr, args, colNames, rawData[p.rows[a]].values)
				vb := evalExprWithRow(obExpr, args, colNames, rawData[p.rows[b]].values)
				cmp := memCompare(va, vb)
				if desc {
					return cmp > 0
				}
				return cmp < 0
			})
		}
		break
	}

	winValues := make(map[int]map[int]*vdbe.Mem)
	for i := range rawData {
		winValues[i] = make(map[int]*vdbe.Mem)
	}

	for _, wc := range winCols {
		for pi := range partitions {
			p := partitions[pi]
			for posInPart, rowIdx := range p.rows {
				winValues[rowIdx][wc.idx] = evalWindowFunc(wc.info, posInPart, p, rawData, colNames, args)
			}
		}
	}

	var rows []Row
	for ri, rd := range rawData {
		row := Row{cols: resultCols}
		for ci, c := range cols {
			if wv, ok := winValues[ri][ci]; ok {
				row.values = append(row.values, wv)
			} else if c.expr == "*" {
				for i := range tableColumns {
					if i < len(rd.values) {
						row.values = append(row.values, vdbe.MemFromValue(rd.values[i]))
					} else {
						row.values = append(row.values, vdbe.NewMemNull())
					}
				}
			} else {
				val := evalExprWithRow(c.expr, args, colNames, rd.values)
				row.values = append(row.values, val)
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// evalWindowFunc computes a single window function value for a row.
func evalWindowFunc(
	info *windowFuncInfo, posInPart int, part struct{ rows []int },
	rawData []rawRow, colNames []string, args []interface{},
) *vdbe.Mem {
	switch info.fn {
	case "row_number":
		return vdbe.NewMemInt(int64(posInPart + 1))
	case "rank":
		if info.orderBy != "" && posInPart > 0 {
			obExpr := stripSortDir(info.orderBy)
			cur := evalExprWithRow(obExpr, args, colNames, rawData[part.rows[posInPart]].values)
			rank := int64(1)
			for k := 0; k < posInPart; k++ {
				prev := evalExprWithRow(obExpr, args, colNames, rawData[part.rows[k]].values)
				if memCompare(prev, cur) != 0 {
					rank = int64(k+1) + 1
				}
			}
			return vdbe.NewMemInt(rank)
		}
		return vdbe.NewMemInt(int64(posInPart + 1))
	case "dense_rank":
		if info.orderBy != "" && posInPart > 0 {
			obExpr := stripSortDir(info.orderBy)
			cur := evalExprWithRow(obExpr, args, colNames, rawData[part.rows[posInPart]].values)
			dr := int64(1)
			for k := 1; k <= posInPart; k++ {
				prev := evalExprWithRow(obExpr, args, colNames, rawData[part.rows[k-1]].values)
				if memCompare(prev, cur) != 0 {
					dr++
				}
			}
			return vdbe.NewMemInt(dr)
		}
		return vdbe.NewMemInt(int64(posInPart + 1))
	case "ntile":
		if len(info.args) > 0 {
			n, _ := strconv.ParseInt(info.args[0], 10, 64)
			if n > 0 {
				bs := (len(part.rows) + int(n) - 1) / int(n)
				bucket := posInPart/bs + 1
				if bucket > int(n) {
					bucket = int(n)
				}
				return vdbe.NewMemInt(int64(bucket))
			}
		}
	case "lag":
		if len(info.args) >= 1 {
			off := int64(1)
			if len(info.args) >= 2 {
				off, _ = strconv.ParseInt(info.args[1], 10, 64)
			}
			tp := posInPart - int(off)
			if tp >= 0 && tp < len(part.rows) {
				return evalExprWithRow(info.args[0], args, colNames, rawData[part.rows[tp]].values)
			}
			if len(info.args) >= 3 {
				return evalExprWithRow(info.args[2], args, colNames, nil)
			}
		}
	case "lead":
		if len(info.args) >= 1 {
			off := int64(1)
			if len(info.args) >= 2 {
				off, _ = strconv.ParseInt(info.args[1], 10, 64)
			}
			tp := posInPart + int(off)
			if tp >= 0 && tp < len(part.rows) {
				return evalExprWithRow(info.args[0], args, colNames, rawData[part.rows[tp]].values)
			}
			if len(info.args) >= 3 {
				return evalExprWithRow(info.args[2], args, colNames, nil)
			}
		}
	case "first_value":
		if len(info.args) >= 1 && len(part.rows) > 0 {
			return evalExprWithRow(info.args[0], args, colNames, rawData[part.rows[0]].values)
		}
	case "last_value":
		if len(info.args) >= 1 && len(part.rows) > 0 {
			return evalExprWithRow(info.args[0], args, colNames, rawData[part.rows[len(part.rows)-1]].values)
		}
	case "nth_value":
		if len(info.args) >= 2 {
			n, _ := strconv.ParseInt(info.args[1], 10, 64)
			if n > 0 && int(n) <= len(part.rows) {
				return evalExprWithRow(info.args[0], args, colNames, rawData[part.rows[int(n)-1]].values)
			}
		}
	}
	return vdbe.NewMemNull()
}

// stripSortDir removes trailing ASC/DESC from an ORDER BY expression.
func stripSortDir(expr string) string {
	obLower := strings.ToLower(expr)
	if strings.HasSuffix(obLower, " desc") {
		return strings.TrimSpace(expr[:len(expr)-5])
	}
	if strings.HasSuffix(obLower, " asc") {
		return strings.TrimSpace(expr[:len(expr)-4])
	}
	return expr
}
