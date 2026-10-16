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

// Equal fails tb (non-fatally) when got != want, reporting both.
func Equal[T comparable](tb testing.TB, got, want T) {
	tb.Helper()
	if got != want {
		tb.Errorf("got %v, want %v", got, want)
	}
}

// Diff fails tb (non-fatally) with a structural diff when got != want.
// Uses go-cmp; intended for maps, slices, and structs with exported
// fields (cmp panics on unexported fields without options) — keep
// manual comparison for types with unexported state.
func Diff(tb testing.TB, got, want any) {
	tb.Helper()
	if d := cmp.Diff(want, got); d != "" {
		tb.Errorf("mismatch (-want +got):\n%s", d)
	}
}

// NoError stops the test when an operation fails.
func NoError(tb testing.TB, err error) {
	tb.Helper()
	if err != nil {
		tb.Fatal(err)
	}
}
