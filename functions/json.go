// Package functions implements JSON SQL functions for sqlite-go.
package functions

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/sqlite-go/sqlite-go/vdbe"
)

func registerJSONFunctions(r *FuncRegistry) {
	// json_extract(json, path)
	r.Register(&FuncDef{Name: "json_extract", NumArgs: 2, IsDeterministic: true, ScalarFunc: jsonExtractFunc})
	r.Register(&FuncDef{Name: "json_extract", NumArgs: -1, IsDeterministic: true, ScalarFunc: jsonExtractFunc})

	// json_type(json, [path])
	r.Register(&FuncDef{Name: "json_type", NumArgs: 1, IsDeterministic: true, ScalarFunc: jsonTypeFunc1})
	r.Register(&FuncDef{Name: "json_type", NumArgs: 2, IsDeterministic: true, ScalarFunc: jsonTypeFunc2})

	// json_array([value, ...])
	r.Register(&FuncDef{Name: "json_array", NumArgs: -1, IsDeterministic: true, ScalarFunc: jsonArrayFunc})

	// json_object(key, value [, key, value ...])
	r.Register(&FuncDef{Name: "json_object", NumArgs: -1, IsDeterministic: true, ScalarFunc: jsonObjectFunc})

	// json_quote(value)
	r.Register(&FuncDef{Name: "json_quote", NumArgs: 1, IsDeterministic: true, ScalarFunc: jsonQuoteFunc})

	// json_insert(json, path, value [, path, value ...])
	r.Register(&FuncDef{Name: "json_insert", NumArgs: -1, IsDeterministic: true, ScalarFunc: jsonInsertFunc})

	// json_replace(json, path, value [, path, value ...])
	r.Register(&FuncDef{Name: "json_replace", NumArgs: -1, IsDeterministic: true, ScalarFunc: jsonReplaceFunc})

	// json_set(json, path, value [, path, value ...])
	r.Register(&FuncDef{Name: "json_set", NumArgs: -1, IsDeterministic: true, ScalarFunc: jsonSetFunc})

	// json_remove(json, path [, path ...])
	r.Register(&FuncDef{Name: "json_remove", NumArgs: -1, IsDeterministic: true, ScalarFunc: jsonRemoveFunc})

	// json_valid(json)
	r.Register(&FuncDef{Name: "json_valid", NumArgs: 1, IsDeterministic: true, ScalarFunc: jsonValidFunc})

	// json_length(json [, path])
	r.Register(&FuncDef{Name: "json_length", NumArgs: 1, IsDeterministic: true, ScalarFunc: jsonLengthFunc1})
	r.Register(&FuncDef{Name: "json_length", NumArgs: 2, IsDeterministic: true, ScalarFunc: jsonLengthFunc2})

	// json_group_array(x) - aggregate
	r.Register(&FuncDef{
		Name: "json_group_array", NumArgs: 1, IsAggregate: true,
		StepFunc: jsonGroupArrayStep, FinalizeFunc: jsonGroupArrayFinalize,
	})

	// json_group_object(key, value) - aggregate
	r.Register(&FuncDef{
		Name: "json_group_object", NumArgs: 2, IsAggregate: true,
		StepFunc: jsonGroupObjectStep, FinalizeFunc: jsonGroupObjectFinalize,
	})

	// -> and ->> operators (registered as "json_arrow" and "json_arrow2")
	r.Register(&FuncDef{Name: "json_arrow", NumArgs: 2, IsDeterministic: true, ScalarFunc: jsonArrowFunc})
	r.Register(&FuncDef{Name: "json_arrow2", NumArgs: 2, IsDeterministic: true, ScalarFunc: jsonArrow2Func})

	// json_each / json_tree helpers (these return scalar values for now)
	r.Register(&FuncDef{Name: "json_each", NumArgs: 1, IsDeterministic: true, ScalarFunc: jsonEachFunc})
	r.Register(&FuncDef{Name: "json_tree", NumArgs: 1, IsDeterministic: true, ScalarFunc: jsonTreeFunc})
}

// jsonExtractFunc implements json_extract(json, path [, path ...])
func jsonExtractFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if len(args) < 2 {
		return vdbe.NewMemNull()
	}
	doc := args[0].TextValue()
	var parsed interface{}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		return vdbe.NewMemNull()
	}

	// Single path: return the value directly
	if len(args) == 2 {
		path := args[1].TextValue()
		val := jsonExtractPath(parsed, path)
		return jsonValueToMem(val)
	}

	// Multiple paths: return JSON array
	var results []interface{}
	for i := 1; i < len(args); i++ {
		path := args[i].TextValue()
		val := jsonExtractPath(parsed, path)
		results = append(results, val)
	}
	encoded, err := json.Marshal(results)
	if err != nil {
		return vdbe.NewMemNull()
	}
	return vdbe.NewMemStr(string(encoded))
}

// jsonExtractPath extracts a value from parsed JSON using a JSON path.
func jsonExtractPath(parsed interface{}, path string) interface{} {
	if path == "$" {
		return parsed
	}
	if !strings.HasPrefix(path, "$.") && !strings.HasPrefix(path, "$[") {
		return nil
	}
	// Parse the path
	parts := jsonParsePath(path[1:]) // skip $
	return jsonFollowPath(parsed, parts)
}

// jsonParsePath parses a JSON path like ".key[0].sub" into path segments.
func jsonParsePath(path string) []jsonPathSegment {
	var segments []jsonPathSegment
	i := 0
	for i < len(path) {
		if path[i] == '.' {
			i++
			start := i
			for i < len(path) && path[i] != '.' && path[i] != '[' {
				i++
			}
			segments = append(segments, jsonPathSegment{key: path[start:i]})
		} else if path[i] == '[' {
			i++ // skip [
			start := i
			for i < len(path) && path[i] != ']' {
				i++
			}
			idxStr := path[start:i]
			if idxStr == "" {
				// Empty bracket - skip
			} else if idxStr[0] == '\'' || idxStr[0] == '"' {
				// String key
				segments = append(segments, jsonPathSegment{key: idxStr[1 : len(idxStr)-1]})
			} else {
				idx, err := strconv.Atoi(idxStr)
				if err == nil {
					segments = append(segments, jsonPathSegment{index: idx, isIndex: true})
				}
			}
			if i < len(path) {
				i++ // skip ]
			}
		} else {
			i++
		}
	}
	return segments
}

type jsonPathSegment struct {
	key     string
	index   int
	isIndex bool
}

func jsonFollowPath(v interface{}, segments []jsonPathSegment) interface{} {
	cur := v
	for _, seg := range segments {
		if cur == nil {
			return nil
		}
		if seg.isIndex {
			arr, ok := cur.([]interface{})
			if !ok || seg.index < 0 || seg.index >= len(arr) {
				return nil
			}
			cur = arr[seg.index]
		} else {
			m, ok := cur.(map[string]interface{})
			if !ok {
				return nil
			}
			cur = m[seg.key]
		}
	}
	return cur
}

func jsonValueToMem(v interface{}) *vdbe.Mem {
	if v == nil {
		return vdbe.NewMemNull()
	}
	switch val := v.(type) {
	case bool:
		if val {
			return vdbe.NewMemInt(1)
		}
		return vdbe.NewMemInt(0)
	case json.Number:
		if i, err := val.Int64(); err == nil {
			return vdbe.NewMemInt(i)
		}
		if f, err := val.Float64(); err == nil {
			return vdbe.NewMemFloat(f)
		}
		return vdbe.NewMemStr(val.String())
	case float64:
		if float64(int64(val)) == val && val >= -9.223372e+18 && val <= 9.223372e+18 {
			return vdbe.NewMemInt(int64(val))
		}
		return vdbe.NewMemFloat(val)
	case string:
		return vdbe.NewMemStr(val)
	case []interface{}:
		encoded, _ := json.Marshal(val)
		return vdbe.NewMemStr(string(encoded))
	case map[string]interface{}:
		encoded, _ := json.Marshal(val)
		return vdbe.NewMemStr(string(encoded))
	default:
		encoded, _ := json.Marshal(v)
		return vdbe.NewMemStr(string(encoded))
	}
}

// jsonTypeFunc1 implements json_type(json)
func jsonTypeFunc1(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return jsonTypeAtPath(args[0].TextValue(), "$")
}

// jsonTypeFunc2 implements json_type(json, path)
func jsonTypeFunc2(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return jsonTypeAtPath(args[0].TextValue(), args[1].TextValue())
}

func jsonTypeAtPath(doc, path string) *vdbe.Mem {
	var parsed interface{}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		return vdbe.NewMemNull()
	}
	val := jsonExtractPath(parsed, path)
	if val == nil {
		return vdbe.NewMemNull()
	}
	switch val.(type) {
	case nil:
		return vdbe.NewMemStr("null")
	case bool:
		return vdbe.NewMemStr("true")
	case float64, json.Number:
		return vdbe.NewMemStr("integer")
	case string:
		return vdbe.NewMemStr("text")
	case []interface{}:
		return vdbe.NewMemStr("array")
	case map[string]interface{}:
		return vdbe.NewMemStr("object")
	default:
		return vdbe.NewMemStr("null")
	}
}

// jsonArrayFunc implements json_array([value, ...])
func jsonArrayFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	arr := make([]interface{}, len(args))
	for i, arg := range args {
		arr[i] = memToJSONValue(arg)
	}
	encoded, _ := json.Marshal(arr)
	return vdbe.NewMemStr(string(encoded))
}

// jsonObjectFunc implements json_object(key, value [, key, value ...])
func jsonObjectFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if len(args)%2 != 0 {
		return memError("json_object() requires an even number of arguments")
	}
	obj := make(map[string]interface{})
	for i := 0; i < len(args); i += 2 {
		key := args[i].TextValue()
		obj[key] = memToJSONValue(args[i+1])
	}
	encoded, _ := json.Marshal(obj)
	return vdbe.NewMemStr(string(encoded))
}

// jsonQuoteFunc implements json_quote(value)
func jsonQuoteFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	v := args[0]
	switch v.Type {
	case vdbe.MemNull:
		return vdbe.NewMemStr("null")
	case vdbe.MemInt:
		return vdbe.NewMemStr(fmt.Sprintf("%d", v.IntVal))
	case vdbe.MemFloat:
		return vdbe.NewMemStr(fmt.Sprintf("%g", v.FloatVal))
	case vdbe.MemStr:
		encoded, _ := json.Marshal(v.TextValue())
		return vdbe.NewMemStr(string(encoded))
	default:
		encoded, _ := json.Marshal(v.TextValue())
		return vdbe.NewMemStr(string(encoded))
	}
}

// jsonInsertFunc implements json_insert(json, path, value [, path, value ...])
func jsonInsertFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return jsonModify(args, "insert")
}

// jsonReplaceFunc implements json_replace(json, path, value [, path, value ...])
func jsonReplaceFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return jsonModify(args, "replace")
}

// jsonSetFunc implements json_set(json, path, value [, path, value ...])
func jsonSetFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return jsonModify(args, "set")
}

// jsonRemoveFunc implements json_remove(json, path [, path ...])
func jsonRemoveFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	if len(args) < 1 {
		return vdbe.NewMemNull()
	}
	doc := args[0].TextValue()
	var parsed interface{}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		return vdbe.NewMemNull()
	}
	for i := 1; i < len(args); i++ {
		path := args[i].TextValue()
		parsed = jsonRemoveAtPath(parsed, path)
	}
	encoded, _ := json.Marshal(parsed)
	return vdbe.NewMemStr(string(encoded))
}

// jsonValidFunc implements json_valid(json)
func jsonValidFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	var v interface{}
	err := json.Unmarshal([]byte(args[0].TextValue()), &v)
	if err != nil {
		return vdbe.NewMemInt(0)
	}
	return vdbe.NewMemInt(1)
}

// jsonLengthFunc1 implements json_length(json)
func jsonLengthFunc1(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return jsonLengthAtPath(args[0].TextValue(), "$")
}

// jsonLengthFunc2 implements json_length(json, path)
func jsonLengthFunc2(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	return jsonLengthAtPath(args[0].TextValue(), args[1].TextValue())
}

func jsonLengthAtPath(doc, path string) *vdbe.Mem {
	var parsed interface{}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		return vdbe.NewMemNull()
	}
	val := jsonExtractPath(parsed, path)
	if val == nil {
		return vdbe.NewMemNull()
	}
	switch v := val.(type) {
	case []interface{}:
		return vdbe.NewMemInt(int64(len(v)))
	case map[string]interface{}:
		return vdbe.NewMemInt(int64(len(v)))
	case string:
		return vdbe.NewMemInt(int64(len(v)))
	default:
		return vdbe.NewMemInt(1)
	}
}

// jsonArrowFunc implements the -> operator (returns JSON representation)
func jsonArrowFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	doc := args[0].TextValue()
	path := args[1].TextValue()

	var parsed interface{}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		return vdbe.NewMemNull()
	}

	// Handle the path: can be "$.key" or just "key" for ->
	if !strings.HasPrefix(path, "$") {
		path = "$." + path
	}
	val := jsonExtractPath(parsed, path)
	if val == nil {
		return vdbe.NewMemNull()
	}
	encoded, _ := json.Marshal(val)
	return vdbe.NewMemStr(string(encoded))
}

// jsonArrow2Func implements the ->> operator (returns SQL value)
func jsonArrow2Func(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	doc := args[0].TextValue()
	path := args[1].TextValue()

	var parsed interface{}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		return vdbe.NewMemNull()
	}

	if !strings.HasPrefix(path, "$") {
		path = "$." + path
	}
	val := jsonExtractPath(parsed, path)
	return jsonValueToMem(val)
}

// jsonEachFunc returns info about JSON array/object elements (scalar stub).
func jsonEachFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	var parsed interface{}
	if err := json.Unmarshal([]byte(args[0].TextValue()), &parsed); err != nil {
		return vdbe.NewMemNull()
	}
	encoded, _ := json.Marshal(parsed)
	return vdbe.NewMemStr(string(encoded))
}

// jsonTreeFunc returns info about JSON tree traversal (scalar stub).
func jsonTreeFunc(_ *Context, args []*vdbe.Mem) *vdbe.Mem {
	var parsed interface{}
	if err := json.Unmarshal([]byte(args[0].TextValue()), &parsed); err != nil {
		return vdbe.NewMemNull()
	}
	encoded, _ := json.Marshal(parsed)
	return vdbe.NewMemStr(string(encoded))
}

// --- Aggregate JSON functions ---

type jsonGroupArrayState struct {
	values []interface{}
}

func jsonGroupArrayStep(_ *Context, aggCtx *AggregateContext, args []*vdbe.Mem) {
	st, ok := aggCtx.Data.(*jsonGroupArrayState)
	if !ok {
		st = &jsonGroupArrayState{}
		aggCtx.Data = st
	}
	st.values = append(st.values, memToJSONValue(args[0]))
}

func jsonGroupArrayFinalize(_ *Context, aggCtx *AggregateContext) *vdbe.Mem {
	st, ok := aggCtx.Data.(*jsonGroupArrayState)
	if !ok || st == nil {
		return vdbe.NewMemStr("[]")
	}
	encoded, _ := json.Marshal(st.values)
	return vdbe.NewMemStr(string(encoded))
}

type jsonGroupObjectState struct {
	keys   []string
	values []interface{}
}

func jsonGroupObjectStep(_ *Context, aggCtx *AggregateContext, args []*vdbe.Mem) {
	st, ok := aggCtx.Data.(*jsonGroupObjectState)
	if !ok {
		st = &jsonGroupObjectState{}
		aggCtx.Data = st
	}
	st.keys = append(st.keys, args[0].TextValue())
	st.values = append(st.values, memToJSONValue(args[1]))
}

func jsonGroupObjectFinalize(_ *Context, aggCtx *AggregateContext) *vdbe.Mem {
	st, ok := aggCtx.Data.(*jsonGroupObjectState)
	if !ok || st == nil {
		return vdbe.NewMemStr("{}")
	}
	obj := make(map[string]interface{}, len(st.keys))
	for i, k := range st.keys {
		obj[k] = st.values[i]
	}
	encoded, _ := json.Marshal(obj)
	return vdbe.NewMemStr(string(encoded))
}

// --- JSON modification helpers ---

func jsonModify(args []*vdbe.Mem, mode string) *vdbe.Mem {
	if len(args) < 3 || len(args)%2 != 1 {
		return vdbe.NewMemNull()
	}
	doc := args[0].TextValue()
	var parsed interface{}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		return vdbe.NewMemNull()
	}

	for i := 1; i+1 < len(args); i += 2 {
		path := args[i].TextValue()
		value := memToJSONValue(args[i+1])

		segments := jsonParsePath(path[1:]) // skip $
		if len(segments) == 0 {
			if mode == "set" || mode == "replace" {
				parsed = value
			}
			continue
		}

		switch mode {
		case "insert":
			parsed = jsonInsertAtPath(parsed, segments, value)
		case "replace":
			parsed = jsonReplaceAtPath(parsed, segments, value)
		case "set":
			parsed = jsonSetAtPath(parsed, segments, value)
		}
	}

	encoded, _ := json.Marshal(parsed)
	return vdbe.NewMemStr(string(encoded))
}

// jsonInsertAtPath inserts a value only if the path doesn't already exist.
func jsonInsertAtPath(v interface{}, segments []jsonPathSegment, value interface{}) interface{} {
	if len(segments) == 0 {
		return v
	}
	seg := segments[0]
	if seg.isIndex {
		arr, ok := v.([]interface{})
		if !ok {
			return v
		}
		if seg.index < len(arr) {
			// Already exists, don't insert
			return v
		}
		if seg.index == len(arr) {
			// Append
			arr = append(arr, value)
			return arr
		}
		return v
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		return v
	}
	if _, exists := m[seg.key]; exists {
		return v
	}
	m[seg.key] = value
	return m
}

// jsonReplaceAtPath replaces a value only if the path already exists.
func jsonReplaceAtPath(v interface{}, segments []jsonPathSegment, value interface{}) interface{} {
	if len(segments) == 0 {
		return v
	}
	seg := segments[0]
	if seg.isIndex {
		arr, ok := v.([]interface{})
		if !ok {
			return v
		}
		if seg.index < len(arr) {
			if len(segments) == 1 {
				arr[seg.index] = value
				return arr
			}
			arr[seg.index] = jsonReplaceAtPath(arr[seg.index], segments[1:], value)
		}
		return arr
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		return v
	}
	if _, exists := m[seg.key]; !exists {
		return v
	}
	if len(segments) == 1 {
		m[seg.key] = value
		return m
	}
	m[seg.key] = jsonReplaceAtPath(m[seg.key], segments[1:], value)
	return m
}

// jsonSetAtPath sets a value, creating the path if needed.
func jsonSetAtPath(v interface{}, segments []jsonPathSegment, value interface{}) interface{} {
	if len(segments) == 0 {
		return value
	}
	seg := segments[0]
	if seg.isIndex {
		arr, ok := v.([]interface{})
		if !ok {
			arr = []interface{}{}
		}
		for len(arr) <= seg.index {
			arr = append(arr, nil)
		}
		if len(segments) == 1 {
			arr[seg.index] = value
		} else {
			arr[seg.index] = jsonSetAtPath(arr[seg.index], segments[1:], value)
		}
		return arr
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		m = make(map[string]interface{})
	}
	if len(segments) == 1 {
		m[seg.key] = value
	} else {
		if _, exists := m[seg.key]; !exists {
			m[seg.key] = nil
		}
		m[seg.key] = jsonSetAtPath(m[seg.key], segments[1:], value)
	}
	return m
}

// jsonRemoveAtPath removes a value at the given path.
func jsonRemoveAtPath(v interface{}, path string) interface{} {
	if path == "$" {
		return nil
	}
	segments := jsonParsePath(path[1:])
	if len(segments) == 0 {
		return v
	}
	return jsonRemoveAtPathSegs(v, segments)
}

func jsonRemoveAtPathSegs(v interface{}, segments []jsonPathSegment) interface{} {
	if len(segments) == 0 {
		return v
	}
	seg := segments[0]
	if seg.isIndex {
		arr, ok := v.([]interface{})
		if !ok {
			return v
		}
		if seg.index >= len(arr) {
			return v
		}
		if len(segments) == 1 {
			newArr := make([]interface{}, 0, len(arr)-1)
			newArr = append(newArr, arr[:seg.index]...)
			newArr = append(newArr, arr[seg.index+1:]...)
			return newArr
		}
		arr[seg.index] = jsonRemoveAtPathSegs(arr[seg.index], segments[1:])
		return arr
	}
	m, ok := v.(map[string]interface{})
	if !ok {
		return v
	}
	if len(segments) == 1 {
		delete(m, seg.key)
		return m
	}
	if _, exists := m[seg.key]; exists {
		m[seg.key] = jsonRemoveAtPathSegs(m[seg.key], segments[1:])
	}
	return m
}

// memToJSONValue converts a Mem to a Go value suitable for JSON encoding.
func memToJSONValue(m *vdbe.Mem) interface{} {
	switch m.Type {
	case vdbe.MemNull:
		return nil
	case vdbe.MemInt:
		return m.IntVal
	case vdbe.MemFloat:
		return m.FloatVal
	case vdbe.MemStr:
		s := m.TextValue()
		// Try to parse as JSON
		var v interface{}
		if err := json.Unmarshal([]byte(s), &v); err == nil {
			return v
		}
		return s
	case vdbe.MemBlob:
		return m.TextValue()
	default:
		return nil
	}
}
