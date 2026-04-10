package sql

import (
	"fmt"
	"strings"

	"github.com/sqlite-go/sqlite-go/compile"
)

// TriggerTime specifies when the trigger fires.
type TriggerTime int

const (
	TriggerBefore TriggerTime = iota // BEFORE
	TriggerAfter                     // AFTER
	TriggerInstead                   // INSTEAD OF
)

// TriggerEvent specifies what event the trigger fires on.
type TriggerEvent int

const (
	TriggerDelete TriggerEvent = iota // DELETE
	TriggerInsert                     // INSERT
	TriggerUpdate                     // UPDATE
	TriggerUpdateOf                   // UPDATE OF col1, col2, ...
)

// CreateTriggerStmt represents a CREATE TRIGGER statement.
type CreateTriggerStmt struct {
	IfNotExists bool
	Name        string
	Schema      string
	Time        TriggerTime
	Event       TriggerEvent
	Table       string
	// UpdateOfColumns is set for UPDATE OF triggers
	UpdateOfColumns []string
	// For each row vs for each statement
	ForEachRow bool
	// WHEN condition (nil if not specified)
	WhenCondition string
	// Trigger body statements
	BodySQL string
}

// DropTriggerStmt represents a DROP TRIGGER statement.
type DropTriggerStmt struct {
	IfExists bool
	Name     string
	Schema   string
}

// TriggerInfo stores metadata about a registered trigger.
type TriggerInfo struct {
	Name   string
	Schema string
	Table  string
	Time   TriggerTime
	Event  TriggerEvent
	BodySQL string
}

// execCreateTrigger handles CREATE TRIGGER statements.
func (e *Engine) execCreateTrigger(tokens []compile.Token) error {
	pos := 0
	expectKeyword(tokens, &pos, "create")
	expectKeyword(tokens, &pos, "trigger")

	ifNotExists := false
	if pos < len(tokens) && isKeyword(tokens[pos], "if") {
		expectKeyword(tokens, &pos, "if")
		expectKeyword(tokens, &pos, "not")
		expectKeyword(tokens, &pos, "exists")
		ifNotExists = true
	}

	// Optional schema prefix
	schema := ""
	if pos >= len(tokens) {
		return fmt.Errorf("expected trigger name")
	}
	name := tokens[pos].Value
	pos++

	if pos < len(tokens) && tokens[pos].Type == compile.TokenDot {
		pos++
		if pos < len(tokens) {
			schema = name
			name = tokens[pos].Value
			pos++
		}
	}

	// Time: BEFORE, AFTER, or INSTEAD OF
	triggerTime := TriggerAfter
	if pos < len(tokens) {
		if isKeyword(tokens[pos], "before") {
			triggerTime = TriggerBefore
			pos++
		} else if isKeyword(tokens[pos], "after") {
			triggerTime = TriggerAfter
			pos++
		} else if isKeyword(tokens[pos], "instead") {
			pos++
			expectKeyword(tokens, &pos, "of")
			triggerTime = TriggerInstead
		}
	}

	// Event: DELETE, INSERT, UPDATE, UPDATE OF
	triggerEvent := TriggerDelete
	var updateOfCols []string
	if pos < len(tokens) {
		switch {
		case isKeyword(tokens[pos], "delete"):
			triggerEvent = TriggerDelete
			pos++
		case isKeyword(tokens[pos], "insert"):
			triggerEvent = TriggerInsert
			pos++
		case isKeyword(tokens[pos], "update"):
			triggerEvent = TriggerUpdate
			pos++
			// Optional OF column_list
			if pos < len(tokens) && isKeyword(tokens[pos], "of") {
				triggerEvent = TriggerUpdateOf
				pos++
				for pos < len(tokens) && !isKeyword(tokens[pos], "on") {
					col := tokens[pos].Value
					pos++
					updateOfCols = append(updateOfCols, col)
					if pos < len(tokens) && tokens[pos].Type == compile.TokenComma {
						pos++
					}
				}
			}
		default:
			return fmt.Errorf("expected DELETE, INSERT, or UPDATE in trigger, got: %s", tokens[pos].Value)
		}
	}

	// ON table-name
	expectKeyword(tokens, &pos, "on")
	if pos >= len(tokens) {
		return fmt.Errorf("expected table name after ON")
	}
	tableName := tokens[pos].Value
	pos++

	// Check that the table exists
	if _, ok := e.tables[tableName]; !ok {
		return fmt.Errorf("no such table: %s", tableName)
	}

	// Optional FOR EACH ROW
	forEachRow := false
	if pos < len(tokens) && isKeyword(tokens[pos], "for") {
		pos++
		expectKeyword(tokens, &pos, "each")
		expectKeyword(tokens, &pos, "row")
		forEachRow = true
	}

	// Optional WHEN (expr)
	whenCondition := ""
	if pos < len(tokens) && isKeyword(tokens[pos], "when") {
		pos++
		// Collect everything up to BEGIN
		var whenParts []string
		for pos < len(tokens) && !isKeyword(tokens[pos], "begin") {
			whenParts = append(whenParts, tokens[pos].Value)
			pos++
		}
		whenCondition = strings.Join(whenParts, " ")
	}

	// BEGIN
	expectKeyword(tokens, &pos, "begin")

	// Collect body statements until END
	var bodyParts []string
	for pos < len(tokens) && !isKeyword(tokens[pos], "end") {
		bodyParts = append(bodyParts, tokens[pos].Value)
		pos++
	}

	// END
	if pos < len(tokens) && isKeyword(tokens[pos], "end") {
		pos++
	}

	bodySQL := strings.Join(bodyParts, " ")

	stmt := &CreateTriggerStmt{
		IfNotExists:     ifNotExists,
		Name:            name,
		Schema:          schema,
		Time:            triggerTime,
		Event:           triggerEvent,
		Table:           tableName,
		UpdateOfColumns: updateOfCols,
		ForEachRow:      forEachRow,
		WhenCondition:   whenCondition,
		BodySQL:         bodySQL,
	}
	_ = stmt

	// TODO: Store trigger in schema and execute trigger body
	// when the triggering event occurs on the table.
	// For now, CREATE TRIGGER is accepted but the trigger is not stored.

	return nil
}

// execDropTrigger handles DROP TRIGGER statements.
func (e *Engine) execDropTrigger(tokens []compile.Token) error {
	pos := 0
	expectKeyword(tokens, &pos, "drop")
	expectKeyword(tokens, &pos, "trigger")

	ifExists := false
	if pos < len(tokens) && isKeyword(tokens[pos], "if") {
		expectKeyword(tokens, &pos, "if")
		expectKeyword(tokens, &pos, "exists")
		ifExists = true
	}

	schema := ""
	if pos >= len(tokens) {
		return fmt.Errorf("expected trigger name")
	}
	name := tokens[pos].Value
	pos++

	if pos < len(tokens) && tokens[pos].Type == compile.TokenDot {
		pos++
		if pos < len(tokens) {
			schema = name
			name = tokens[pos].Value
			pos++
		}
	}

	stmt := &DropTriggerStmt{
		IfExists: ifExists,
		Name:     name,
		Schema:   schema,
	}
	_ = stmt

	// TODO: Remove trigger from schema.
	// For now, DROP TRIGGER is accepted but is a no-op.

	return nil
}

// ParseCreateTrigger parses a CREATE TRIGGER statement from tokens.
func ParseCreateTrigger(tokens []compile.Token) (*CreateTriggerStmt, error) {
	pos := 0
	if len(tokens) < 3 || !isKeyword(tokens[pos], "create") {
		return nil, fmt.Errorf("expected CREATE TRIGGER")
	}
	pos++
	if !isKeyword(tokens[pos], "trigger") {
		return nil, fmt.Errorf("expected TRIGGER after CREATE")
	}
	pos++

	ifNotExists := false
	if pos < len(tokens) && isKeyword(tokens[pos], "if") {
		pos++
		if pos+1 < len(tokens) && isKeyword(tokens[pos], "not") && isKeyword(tokens[pos+1], "exists") {
			pos += 2
			ifNotExists = true
		}
	}

	if pos >= len(tokens) {
		return nil, fmt.Errorf("expected trigger name")
	}
	name := tokens[pos].Value

	return &CreateTriggerStmt{
		IfNotExists: ifNotExists,
		Name:        name,
	}, nil
}

// ParseDropTrigger parses a DROP TRIGGER statement from tokens.
func ParseDropTrigger(tokens []compile.Token) (*DropTriggerStmt, error) {
	pos := 0
	if len(tokens) < 3 || !isKeyword(tokens[pos], "drop") {
		return nil, fmt.Errorf("expected DROP TRIGGER")
	}
	pos++
	if !isKeyword(tokens[pos], "trigger") {
		return nil, fmt.Errorf("expected TRIGGER after DROP")
	}
	pos++

	ifExists := false
	if pos+1 < len(tokens) && isKeyword(tokens[pos], "if") && isKeyword(tokens[pos+1], "exists") {
		ifExists = true
		pos += 2
	}

	if pos >= len(tokens) {
		return nil, fmt.Errorf("expected trigger name")
	}
	name := tokens[pos].Value

	return &DropTriggerStmt{
		IfExists: ifExists,
		Name:     name,
	}, nil
}

// triggerTimeString converts a TriggerTime to its SQL keyword.
func triggerTimeString(t TriggerTime) string {
	switch t {
	case TriggerBefore:
		return "BEFORE"
	case TriggerAfter:
		return "AFTER"
	case TriggerInstead:
		return "INSTEAD OF"
	default:
		return "AFTER"
	}
}

// triggerEventString converts a TriggerEvent to its SQL keyword.
func triggerEventString(e TriggerEvent) string {
	switch e {
	case TriggerDelete:
		return "DELETE"
	case TriggerInsert:
		return "INSERT"
	case TriggerUpdate:
		return "UPDATE"
	case TriggerUpdateOf:
		return "UPDATE OF"
	default:
		return "UPDATE"
	}
}
