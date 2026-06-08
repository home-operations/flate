package deterministic

import (
	"encoding/base64"
	"testing"
	"unicode"

	"github.com/google/uuid"
)

// TestRandFuncs_SameSeedSameSequence pins that two independently constructed
// FuncMaps with the same seed replay an identical stream: calling the same
// generators in the same order yields equal output. This is the property that
// makes a cache hit match a cache miss.
func TestRandFuncs_SameSeedSameSequence(t *testing.T) {
	a := Funcs(SeedFor("rel", "ns"))
	b := Funcs(SeedFor("rel", "ns"))

	an := a["randAlphaNum"].(func(int) string)
	bn := b["randAlphaNum"].(func(int) string)
	for i := range 3 {
		if x, y := an(24), bn(24); x != y {
			t.Fatalf("randAlphaNum draw %d differs across same-seed maps: %q vs %q", i, x, y)
		}
	}
	if x, y := a["uuidv4"].(func() string)(), b["uuidv4"].(func() string)(); x != y {
		t.Errorf("uuidv4 differs across same-seed maps: %q vs %q", x, y)
	}
	ab, err := a["randBytes"].(func(int) (string, error))(32)
	if err != nil {
		t.Fatalf("randBytes a: %v", err)
	}
	bb, err := b["randBytes"].(func(int) (string, error))(32)
	if err != nil {
		t.Fatalf("randBytes b: %v", err)
	}
	if ab != bb {
		t.Errorf("randBytes differs across same-seed maps: %q vs %q", ab, bb)
	}
	if x, y := a["randInt"].(func(int, int) int)(0, 1000), b["randInt"].(func(int, int) int)(0, 1000); x != y {
		t.Errorf("randInt differs across same-seed maps: %d vs %d", x, y)
	}
}

// TestRandFuncs_DifferentSeedDiffers confirms the seed actually varies output
// — two releases that render the same chart get independent random material.
func TestRandFuncs_DifferentSeedDiffers(t *testing.T) {
	a := Funcs(SeedFor("rel-a", "ns"))["randAlphaNum"].(func(int) string)
	b := Funcs(SeedFor("rel-b", "ns"))["randAlphaNum"].(func(int) string)
	if a(24) == b(24) {
		t.Error("different seeds produced identical randAlphaNum output")
	}
}

// TestRandFuncs_CharacterClassesAndShape pins that the deterministic ports
// keep sprig's contracts: right length, right character class, and a valid v4
// UUID / correctly sized randBytes.
func TestRandFuncs_CharacterClassesAndShape(t *testing.T) {
	fm := Funcs(SeedFor("rel", "ns"))

	alpha := fm["randAlpha"].(func(int) string)(64)
	if n := len([]rune(alpha)); n != 64 {
		t.Errorf("randAlpha length = %d, want 64", n)
	}
	for _, r := range alpha {
		if !unicode.IsLetter(r) {
			t.Errorf("randAlpha produced non-letter %q in %q", r, alpha)
			break
		}
	}
	for _, r := range fm["randNumeric"].(func(int) string)(64) {
		if !unicode.IsDigit(r) {
			t.Errorf("randNumeric produced non-digit %q", r)
			break
		}
	}
	for _, r := range fm["randAlphaNum"].(func(int) string)(64) {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			t.Errorf("randAlphaNum produced non-alphanumeric %q", r)
			break
		}
	}

	u := fm["uuidv4"].(func() string)()
	parsed, err := uuid.Parse(u)
	if err != nil {
		t.Fatalf("uuidv4 not a valid UUID: %q: %v", u, err)
	}
	if parsed.Version() != 4 {
		t.Errorf("uuidv4 version = %d, want 4 (%q)", parsed.Version(), u)
	}

	rb, err := fm["randBytes"].(func(int) (string, error))(32)
	if err != nil {
		t.Fatalf("randBytes: %v", err)
	}
	decoded, err := base64.StdEncoding.DecodeString(rb)
	if err != nil {
		t.Fatalf("randBytes not valid base64: %v", err)
	}
	if len(decoded) != 32 {
		t.Errorf("randBytes decoded to %d bytes, want 32", len(decoded))
	}

	if v := fm["randInt"].(func(int, int) int)(10, 20); v < 10 || v >= 20 {
		t.Errorf("randInt(10,20) = %d, want [10,20)", v)
	}
}

// TestRandFuncs_StreamAdvances confirms successive draws on one FuncMap differ
// (the shared stream advances), so a chart generating two secrets doesn't get
// the same value twice.
func TestRandFuncs_StreamAdvances(t *testing.T) {
	gen := Funcs(SeedFor("rel", "ns"))["randAlphaNum"].(func(int) string)
	if first, second := gen(24), gen(24); first == second {
		t.Errorf("successive randAlphaNum draws identical (stream not advancing): %q", first)
	}
}
