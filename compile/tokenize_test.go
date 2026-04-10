package compile

import (
	"testing"
)

func TestTokenizeSelect(t *testing.T) {
	tokens := Tokenize("SELECT * FROM users WHERE id = 1;")
	expected := []struct {
		typ TokenType
		val string
	}{
		{TokenKeyword, "SELECT"},
		{TokenWhitespace, " "},
		{TokenStar, "*"},
		{TokenWhitespace, " "},
		{TokenKeyword, "FROM"},
		{TokenWhitespace, " "},
		{TokenID, "users"},
		{TokenWhitespace, " "},
		{TokenKeyword, "WHERE"},
		{TokenWhitespace, " "},
		{TokenID, "id"},
		{TokenWhitespace, " "},
		{TokenEq, "="},
		{TokenWhitespace, " "},
		{TokenInteger, "1"},
		{TokenSemi, ";"},
	}
	compareTokens(t, tokens, expected)
}

func TestTokenizeStringsAndBlobs(t *testing.T) {
	tokens := Tokenize("SELECT 'hello', X'deadbeef', \"col\"")
	expected := []struct {
		typ TokenType
		val string
	}{
		{TokenKeyword, "SELECT"},
		{TokenWhitespace, " "},
		{TokenString, "'hello'"},
		{TokenComma, ","},
		{TokenWhitespace, " "},
		{TokenBlob, "X'deadbeef'"},
		{TokenComma, ","},
		{TokenWhitespace, " "},
		{TokenID, "\"col\""},
	}
	compareTokens(t, tokens, expected)
}

func TestTokenizeOperators(t *testing.T) {
	tokens := Tokenize("a <> b != c <= d >= e || f << g >> h")
	expected := []struct {
		typ TokenType
		val string
	}{
		{TokenID, "a"},
		{TokenWhitespace, " "},
		{TokenNe, "<>"},
		{TokenWhitespace, " "},
		{TokenID, "b"},
		{TokenWhitespace, " "},
		{TokenNe, "!="},
		{TokenWhitespace, " "},
		{TokenID, "c"},
		{TokenWhitespace, " "},
		{TokenLe, "<="},
		{TokenWhitespace, " "},
		{TokenID, "d"},
		{TokenWhitespace, " "},
		{TokenGe, ">="},
		{TokenWhitespace, " "},
		{TokenID, "e"},
		{TokenWhitespace, " "},
		{TokenConcat, "||"},
		{TokenWhitespace, " "},
		{TokenID, "f"},
		{TokenWhitespace, " "},
		{TokenLShift, "<<"},
		{TokenWhitespace, " "},
		{TokenID, "g"},
		{TokenWhitespace, " "},
		{TokenRShift, ">>"},
		{TokenWhitespace, " "},
		{TokenID, "h"},
	}
	compareTokens(t, tokens, expected)
}

func TestTokenizeVariables(t *testing.T) {
	tokens := Tokenize("SELECT ?, ?123, :name, @var, $param FROM t")
	expected := []struct {
		typ TokenType
		val string
	}{
		{TokenKeyword, "SELECT"},
		{TokenWhitespace, " "},
		{TokenVariable, "?"},
		{TokenComma, ","},
		{TokenWhitespace, " "},
		{TokenVariable, "?123"},
		{TokenComma, ","},
		{TokenWhitespace, " "},
		{TokenVariable, ":name"},
		{TokenComma, ","},
		{TokenWhitespace, " "},
		{TokenVariable, "@var"},
		{TokenComma, ","},
		{TokenWhitespace, " "},
		{TokenVariable, "$param"},
		{TokenWhitespace, " "},
		{TokenKeyword, "FROM"},
		{TokenWhitespace, " "},
		{TokenID, "t"},
	}
	compareTokens(t, tokens, expected)
}

func TestTokenizeNumbers(t *testing.T) {
	tokens := Tokenize("1 42 3.14 .5 1.0e10 2E-3 0xFF")
	expected := []struct {
		typ TokenType
		val string
	}{
		{TokenInteger, "1"},
		{TokenWhitespace, " "},
		{TokenInteger, "42"},
		{TokenWhitespace, " "},
		{TokenFloat, "3.14"},
		{TokenWhitespace, " "},
		{TokenFloat, ".5"},
		{TokenWhitespace, " "},
		{TokenFloat, "1.0e10"},
		{TokenWhitespace, " "},
		{TokenFloat, "2E-3"},
		{TokenWhitespace, " "},
		{TokenInteger, "0xFF"},
	}
	compareTokens(t, tokens, expected)
}

func TestTokenizeComments(t *testing.T) {
	input := "SELECT 1 -- line comment\nFROM /* block */ t"
	tokens := Tokenize(input)
	expected := []struct {
		typ TokenType
		val string
	}{
		{TokenKeyword, "SELECT"},
		{TokenWhitespace, " "},
		{TokenInteger, "1"},
		{TokenWhitespace, " "},
		{TokenComment, "-- line comment"},
		{TokenWhitespace, "\n"},
		{TokenKeyword, "FROM"},
		{TokenWhitespace, " "},
		{TokenComment, "/* block */"},
		{TokenWhitespace, " "},
		{TokenID, "t"},
	}
	compareTokens(t, tokens, expected)
}

func TestTokenizeEscapedString(t *testing.T) {
	tokens := Tokenize("SELECT 'it''s ok'")
	expected := []struct {
		typ TokenType
		val string
	}{
		{TokenKeyword, "SELECT"},
		{TokenWhitespace, " "},
		{TokenString, "'it''s ok'"},
	}
	compareTokens(t, tokens, expected)
}

func TestTokenizeLineCol(t *testing.T) {
	tokens := Tokenize("a\nb")
	if tokens[0].Line != 1 || tokens[0].Col != 1 {
		t.Errorf("token 'a': expected line=1 col=1, got line=%d col=%d", tokens[0].Line, tokens[0].Col)
	}
	// tokens[1] is \n whitespace
	if tokens[1].Line != 1 || tokens[1].Col != 2 {
		t.Errorf("token '\\n': expected line=1 col=2, got line=%d col=%d", tokens[1].Line, tokens[1].Col)
	}
	if tokens[2].Line != 2 || tokens[2].Col != 1 {
		t.Errorf("token 'b': expected line=2 col=1, got line=%d col=%d", tokens[2].Line, tokens[2].Col)
	}
}

func compareTokens(t *testing.T, tokens []Token, expected []struct {
	typ TokenType
	val string
}) {
	t.Helper()
	if len(tokens) != len(expected) {
		t.Fatalf("expected %d tokens, got %d", len(expected), len(tokens))
	}
	for i, exp := range expected {
		if tokens[i].Type != exp.typ {
			t.Errorf("token %d: expected type %d, got %d (value=%q)", i, exp.typ, tokens[i].Type, tokens[i].Value)
		}
		if tokens[i].Value != exp.val {
			t.Errorf("token %d: expected value %q, got %q", i, exp.val, tokens[i].Value)
		}
	}
}
