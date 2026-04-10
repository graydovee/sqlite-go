package encoding

import (
	"testing"
)

func TestHashTableBasic(t *testing.T) {
	ht := NewHashTable(16)

	// Insert
	ht.Insert("hello", 1)
	ht.Insert("world", 2)
	ht.Insert("foo", 3)

	if ht.Count() != 3 {
		t.Errorf("expected count 3, got %d", ht.Count())
	}

	// Find
	if v := ht.Find("hello"); v != 1 {
		t.Errorf("expected 1, got %v", v)
	}
	if v := ht.Find("world"); v != 2 {
		t.Errorf("expected 2, got %v", v)
	}
	if v := ht.Find("nonexistent"); v != nil {
		t.Errorf("expected nil, got %v", v)
	}

	// Delete
	if !ht.Delete("hello") {
		t.Error("expected delete to return true")
	}
	if ht.Count() != 2 {
		t.Errorf("expected count 2, got %d", ht.Count())
	}
	if v := ht.Find("hello"); v != nil {
		t.Errorf("expected nil after delete, got %v", v)
	}

	// Delete non-existent
	if ht.Delete("nonexistent") {
		t.Error("expected delete of non-existent to return false")
	}
}

func TestHashTableIteration(t *testing.T) {
	ht := NewHashTable(4)
	ht.Insert("a", 1)
	ht.Insert("b", 2)
	ht.Insert("c", 3)

	count := 0
	seen := make(map[string]bool)
	for e := ht.First(); e != nil; e = e.Next() {
		count++
		seen[e.Key()] = true
	}
	if count != 3 {
		t.Errorf("expected 3 entries, got %d", count)
	}
	for _, k := range []string{"a", "b", "c"} {
		if !seen[k] {
			t.Errorf("expected to see key %s", k)
		}
	}
}

func TestBitVecBasic(t *testing.T) {
	bv := NewBitVec(1000)

	if bv.Test(1) {
		t.Error("bit 1 should not be set")
	}

	bv.Set(1)
	bv.Set(50)
	bv.Set(100)

	if !bv.Test(1) {
		t.Error("bit 1 should be set")
	}
	if !bv.Test(50) {
		t.Error("bit 50 should be set")
	}
	if !bv.Test(100) {
		t.Error("bit 100 should be set")
	}
	if bv.Test(2) {
		t.Error("bit 2 should not be set")
	}

	if count := bv.Count(); count != 3 {
		t.Errorf("expected 3 bits set, got %d", count)
	}

	bv.Clear(50)
	if bv.Test(50) {
		t.Error("bit 50 should be cleared")
	}
}

func TestBitVecFirstSet(t *testing.T) {
	bv := NewBitVec(100)
	bv.Set(42)
	bv.Set(10)
	bv.Set(80)

	first := bv.FirstSet()
	if first != 10 {
		t.Errorf("expected first set bit at 10, got %d", first)
	}

	// NextSet
	next := bv.NextSet(10)
	if next != 42 {
		t.Errorf("expected next set bit at 42, got %d", next)
	}

	next = bv.NextSet(42)
	if next != 80 {
		t.Errorf("expected next set bit at 80, got %d", next)
	}

	next = bv.NextSet(80)
	if next != 0 {
		t.Errorf("expected no more set bits, got %d", next)
	}
}

func TestPrintfFormat(t *testing.T) {
	tests := []struct {
		fmt string
		args []interface{}
		want string
	}{
		{"hello %s", []interface{}{"world"}, "hello world"},
		{"%d items", []interface{}{42}, "42 items"},
		{"%Q", []interface{}{"it's"}, "'it''s'"},
		{"%q", []interface{}{"it's"}, "it''s"},
		{"100%%", nil, "100%"},
		{"%d + %d = %d", []interface{}{1, 2, 3}, "1 + 2 = 3"},
	}
	for _, tt := range tests {
		got := Format(tt.fmt, tt.args...)
		if got != tt.want {
			t.Errorf("Format(%q, %v) = %q, want %q", tt.fmt, tt.args, got, tt.want)
		}
	}
}

func TestRandomBytes(t *testing.T) {
	buf1 := make([]byte, 32)
	buf2 := make([]byte, 32)

	if err := RandomBytes(buf1); err != nil {
		t.Fatal(err)
	}
	if err := RandomBytes(buf2); err != nil {
		t.Fatal(err)
	}

	// Extremely unlikely to be equal
	allSame := true
	for i := range buf1 {
		if buf1[i] != buf2[i] {
			allSame = false
			break
		}
	}
	if allSame {
		t.Error("two random buffers should not be identical")
	}
}

func TestUtf8CharLen(t *testing.T) {
	tests := []struct {
		byte  byte
		count int
	}{
		{'A', 1},
		{0xC3, 2},
		{0xE4, 3},
		{0xF0, 4},
	}
	for _, tt := range tests {
		got := Utf8CharLen(tt.byte)
		if got != tt.count {
			t.Errorf("Utf8CharLen(0x%02X) = %d, want %d", tt.byte, got, tt.count)
		}
	}
}

func TestUtf8CaseCmp(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"hello", "HELLO", 0},
		{"abc", "ABC", 0},
		{"a", "b", -1},
		{"B", "a", 1},
	}
	for _, tt := range tests {
		got := Utf8CaseCmp(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("Utf8CaseCmp(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestRowSetBasic(t *testing.T) {
	rs := NewRowSet()

	rs.Insert(5)
	rs.Insert(3)
	rs.Insert(8)
	rs.Insert(1)

	if !rs.Test(5) {
		t.Error("row 5 should be in set")
	}
	if rs.Test(2) {
		t.Error("row 2 should not be in set")
	}
	if rs.Count() != 4 {
		t.Errorf("expected count 4, got %d", rs.Count())
	}

	// Next should return in sorted order
	expected := []int64{1, 3, 5, 8}
	for _, exp := range expected {
		got, ok := rs.Next()
		if !ok {
			t.Fatal("unexpected empty")
		}
		if got != exp {
			t.Errorf("expected %d, got %d", exp, got)
		}
	}

	_, ok := rs.Next()
	if ok {
		t.Error("expected no more entries")
	}
}

func TestRowSetClear(t *testing.T) {
	rs := NewRowSet()
	rs.Insert(1)
	rs.Insert(2)
	rs.Clear()

	if !rs.IsEmpty() {
		t.Error("should be empty after Clear")
	}
	if rs.Test(1) {
		t.Error("row 1 should not be in set after Clear")
	}
}
