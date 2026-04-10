// Package sql implements the authorization callback framework for sqlite-go.
package sql

import "fmt"

type AuthorizerAction int

const (
	AuthCreateIndex       AuthorizerAction = 1
	AuthCreateTable       AuthorizerAction = 2
	AuthCreateTempIndex   AuthorizerAction = 3
	AuthCreateTempTable   AuthorizerAction = 4
	AuthCreateTrigger     AuthorizerAction = 5
	AuthCreateView        AuthorizerAction = 6
	AuthDelete            AuthorizerAction = 7
	AuthDropIndex         AuthorizerAction = 8
	AuthDropTable         AuthorizerAction = 9
	AuthDropTempIndex     AuthorizerAction = 10
	AuthDropTempTable     AuthorizerAction = 11
	AuthDropTrigger       AuthorizerAction = 12
	AuthDropView          AuthorizerAction = 13
	AuthInsert            AuthorizerAction = 14
	AuthPragma            AuthorizerAction = 15
	AuthRead              AuthorizerAction = 16
	AuthSelect            AuthorizerAction = 17
	AuthTransaction       AuthorizerAction = 18
	AuthUpdate            AuthorizerAction = 19
	AuthAttach            AuthorizerAction = 20
	AuthDetach            AuthorizerAction = 21
	AuthAlterTable        AuthorizerAction = 22
	AuthReindex           AuthorizerAction = 23
	AuthAnalyze           AuthorizerAction = 24
	AuthCreateVTable      AuthorizerAction = 25
	AuthDropVTable        AuthorizerAction = 26
	AuthFunction          AuthorizerAction = 27
	AuthSavepoint         AuthorizerAction = 28
	AuthCopy              AuthorizerAction = 29
	AuthRecursive         AuthorizerAction = 30
)

// AuthorizerReturnCode represents the result from an authorizer callback.
type AuthorizerReturnCode int

const (
	AuthOk    AuthorizerReturnCode = 0 // Allow the operation
	AuthDeny  AuthorizerReturnCode = 1 // Deny the operation, return error
	AuthIgnore AuthorizerReturnCode = 2 // Don't allow, but don't error
)

// AuthorizerFunc is the callback type for authorization decisions.
// Parameters:
//   - action: the type of operation being authorized
//   - arg1: first argument (e.g., table name, function name)
//   - arg2: second argument (e.g., column name, database name)
//   - arg3: third argument (e.g., database name)
//   - arg4: fourth argument (e.g., trigger/view name)
//
// Return AuthOk to allow, AuthDeny to reject with error, AuthIgnore to skip.
type AuthorizerFunc func(action AuthorizerAction, arg1, arg2, arg3, arg4 string) AuthorizerReturnCode

// SetAuthorizer sets the authorizer callback for the engine.
// Set to nil to disable authorization.
func (e *Engine) SetAuthorizer(fn AuthorizerFunc) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.authorizer = fn
}

// authorize checks with the authorizer if an operation is allowed.
// Returns nil if allowed, or an error if denied.
func (e *Engine) authorize(action AuthorizerAction, arg1, arg2, arg3, arg4 string) error {
	if e.authorizer == nil {
		return nil
	}
	result := e.authorizer(action, arg1, arg2, arg3, arg4)
	switch result {
	case AuthOk:
		return nil
	case AuthDeny:
		return fmt.Errorf("not authorized: %s on %s.%s", authActionName(action), arg3, arg1)
	case AuthIgnore:
		return nil
	default:
		return nil
	}
}

// authActionName returns a human-readable name for an authorizer action.
func authActionName(action AuthorizerAction) string {
	switch action {
	case AuthCreateIndex:
		return "CREATE INDEX"
	case AuthCreateTable:
		return "CREATE TABLE"
	case AuthCreateTempIndex:
		return "CREATE TEMP INDEX"
	case AuthCreateTempTable:
		return "CREATE TEMP TABLE"
	case AuthCreateTrigger:
		return "CREATE TRIGGER"
	case AuthCreateView:
		return "CREATE VIEW"
	case AuthDelete:
		return "DELETE"
	case AuthDropIndex:
		return "DROP INDEX"
	case AuthDropTable:
		return "DROP TABLE"
	case AuthDropTempIndex:
		return "DROP TEMP INDEX"
	case AuthDropTempTable:
		return "DROP TEMP TABLE"
	case AuthDropTrigger:
		return "DROP TRIGGER"
	case AuthDropView:
		return "DROP VIEW"
	case AuthInsert:
		return "INSERT"
	case AuthPragma:
		return "PRAGMA"
	case AuthRead:
		return "READ"
	case AuthSelect:
		return "SELECT"
	case AuthTransaction:
		return "TRANSACTION"
	case AuthUpdate:
		return "UPDATE"
	case AuthAttach:
		return "ATTACH"
	case AuthDetach:
		return "DETACH"
	case AuthAlterTable:
		return "ALTER TABLE"
	case AuthReindex:
		return "REINDEX"
	case AuthAnalyze:
		return "ANALYZE"
	case AuthCreateVTable:
		return "CREATE VIRTUAL TABLE"
	case AuthDropVTable:
		return "DROP VIRTUAL TABLE"
	case AuthFunction:
		return "FUNCTION"
	case AuthSavepoint:
		return "SAVEPOINT"
	case AuthCopy:
		return "COPY"
	case AuthRecursive:
		return "RECURSIVE"
	default:
		return "UNKNOWN"
	}
}

// GetAuthorizer returns the current authorizer function.
func (e *Engine) GetAuthorizer() AuthorizerFunc {
	return e.authorizer
}
