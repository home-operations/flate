// Package assert holds dependency-free test assertion helpers usable
// from any package's tests. It deliberately imports no flate packages
// so that even internal (package X) tests of packages that
// internal/testutil depends on — manifest, store — can use it without
// an import cycle.
package assert

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

// Equal fails t (non-fatally) when got != want, reporting both.
func Equal[T comparable](t testing.TB, got, want T) {
	t.Helper()
	if got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

// Diff fails t (non-fatally) with a structural diff when got != want.
// Uses go-cmp; intended for maps, slices, and structs with exported
// fields (cmp panics on unexported fields without options) — keep
// manual comparison for types with unexported state.
func Diff(t testing.TB, got, want any) {
	t.Helper()
	if d := cmp.Diff(want, got); d != "" {
		t.Errorf("mismatch (-want +got):\n%s", d)
	}
}
