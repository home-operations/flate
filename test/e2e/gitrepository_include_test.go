package e2e_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // cli.Run changes process-global logging and environment.
func TestE2E_GitRepositoryIncludeSOPSUnreadable(t *testing.T) {
	for _, name := range []string{"sops", "sops-required-secret"} {
		t.Run(name, func(t *testing.T) {
			testIncludeCase(t, name)
		})
	}
}

//nolint:paralleltest // cli.Run changes process-global logging and environment.
func TestE2E_GitRepositoryIncludeDiff(t *testing.T) {
	for _, name := range []string{"independent-diff", "outside-copied-path", "include-added", "include-removed", "include-destination", "include-reordered", "shallow-authored-pin", "nested-includes", "filtered-input-change", "remote-branch-history", "missing-historical-input"} {
		t.Run(name, func(t *testing.T) {
			testIncludeCase(t, name)
		})
	}
}

//nolint:paralleltest // cli.Run changes process-global logging and environment.
func TestE2E_GitRepositoryIncludeEntry(t *testing.T) {
	for _, name := range []string{"composed-entry", "directory-conflict"} {
		t.Run(name, func(t *testing.T) {
			testIncludeCase(t, name)
		})
	}
}

func testIncludeCase(t *testing.T, name string) {
	t.Helper()
	fixture := harness.Open(t, "artifact-composition", name)
	want := fixture.Want(t)
	first, second := fixture.Run(t), fixture.Run(t)
	if !slices.Equal(first, want) {
		t.Fatalf("first sequence:\nwant %#v\ngot %#v", want, first)
	}
	if !slices.Equal(second, want) {
		t.Fatalf("second sequence:\nwant %#v\ngot %#v", want, second)
	}
	if !slices.Equal(first, second) {
		t.Fatalf("sequences differ:\nfirst %#v\nsecond %#v", first, second)
	}
}
