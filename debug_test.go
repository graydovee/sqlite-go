package sqlite_go

import (
	"fmt"
	"testing"

	"github.com/sqlite-go/sqlite-go/compile"
)

func TestDebugParse(t *testing.T) {
	sql := "SELECT FROM"
	stmts, err := compile.Parse(sql)
	fmt.Printf("SQL: %s\n", sql)
	fmt.Printf("Statements: %v\n", stmts)
	fmt.Printf("Error: %v\n", err)
	if err != nil {
		fmt.Printf("Error string: %s\n", err.Error())
	}
}
