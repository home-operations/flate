package deterministic

import (
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
)

// TestFuncs_NowIsFixed pins Tier 1: the "now" override returns fixedTime
// (a constant), stably across calls and across independent FuncMaps, so a
// chart's `{{ now | date … }}` renders identically every time instead of
// drawing the wall clock.
func TestFuncs_NowIsFixed(t *testing.T) {
	t.Parallel()
	fm := Funcs(SeedFor("rel", "ns"))
	assert.Equal(t, len(fm), len(staticFuncs)+streamFuncCount)
	raw, ok := fm["now"]
	if !ok {
		t.Fatal(`Funcs() missing "now" override`)
	}
	now, ok := raw.(func() time.Time)
	if !ok {
		t.Fatalf(`"now" override has type %T, want func() time.Time`, raw)
	}
	if got := now(); !got.Equal(fixedTime) {
		t.Errorf("now() = %v, want fixedTime %v", got, fixedTime)
	}
	// Stable on a repeat call (no hidden per-call state).
	if got := now(); !got.Equal(fixedTime) {
		t.Errorf("now() repeat = %v, want fixedTime %v", got, fixedTime)
	}
	// Stable across an independently constructed FuncMap.
	other, ok := Funcs(SeedFor("rel", "ns"))["now"].(func() time.Time)
	if !ok {
		t.Fatal(`fresh Funcs() "now" override has unexpected type`)
	}
	if got := other(); !got.Equal(fixedTime) {
		t.Errorf("now() from a fresh FuncMap = %v, want fixedTime %v", got, fixedTime)
	}
}

func TestFuncs_KeysAreSorted(t *testing.T) {
	t.Parallel()
	keys := Funcs(SeedFor("rel", "ns"))["keys"].(func(...map[string]any) []string)
	for _, tc := range []struct {
		name  string
		dicts []map[string]any
		want  []string
	}{
		{"no dictionaries", nil, []string{}},
		{"empty dictionaries", []map[string]any{nil, {}}, []string{}},
		{"one dictionary", []map[string]any{{"http": 80, "bittorrent": 50413}}, []string{"bittorrent", "http"}},
		{
			"multiple dictionaries keep duplicates",
			[]map[string]any{{"z": nil, "shared": 1}, {"a": true, "shared": 2}},
			[]string{"a", "shared", "shared", "z"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Diff(t, keys(tc.dicts...), tc.want)
		})
	}
}

func TestFuncs_ValuesAreKeySorted(t *testing.T) {
	t.Parallel()
	values, ok := Funcs(SeedFor("rel", "ns"))["values"].(func(map[string]any) []any)
	if !ok {
		t.Fatal("values override has unexpected type")
	}
	for _, tc := range []struct {
		name string
		dict map[string]any
		want []any
	}{
		{"lexical keys", map[string]any{"z": "first", "a": "last", "m": "middle"}, []any{"last", "middle", "first"}},
		{"nils and duplicates", map[string]any{"z": "same", "a": nil, "m": "same"}, []any{nil, "same", "same"}},
		{"empty", map[string]any{}, []any{}},
		{"nil", nil, []any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Diff(t, values(tc.dict), tc.want)
		})
	}
}
