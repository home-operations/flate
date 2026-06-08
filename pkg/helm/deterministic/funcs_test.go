package deterministic

import (
	"testing"
	"time"
)

// TestFuncs_NowIsFixed pins Tier 1: the "now" override returns FixedTime
// (a constant), stably across calls and across independent FuncMaps, so a
// chart's `{{ now | date … }}` renders identically every time instead of
// drawing the wall clock.
func TestFuncs_NowIsFixed(t *testing.T) {
	fm := Funcs(SeedFor("rel", "ns"))
	raw, ok := fm["now"]
	if !ok {
		t.Fatal(`Funcs() missing "now" override`)
	}
	now, ok := raw.(func() time.Time)
	if !ok {
		t.Fatalf(`"now" override has type %T, want func() time.Time`, raw)
	}
	if got := now(); !got.Equal(FixedTime) {
		t.Errorf("now() = %v, want FixedTime %v", got, FixedTime)
	}
	// Stable on a repeat call (no hidden per-call state).
	if got := now(); !got.Equal(FixedTime) {
		t.Errorf("now() repeat = %v, want FixedTime %v", got, FixedTime)
	}
	// Stable across an independently constructed FuncMap.
	other, ok := Funcs(SeedFor("rel", "ns"))["now"].(func() time.Time)
	if !ok {
		t.Fatal(`fresh Funcs() "now" override has unexpected type`)
	}
	if got := other(); !got.Equal(FixedTime) {
		t.Errorf("now() from a fresh FuncMap = %v, want FixedTime %v", got, FixedTime)
	}
}
