package e2e_test

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // cli.Run changes process-wide logging and environment.
func TestE2E_ArtifactGenerator(t *testing.T) {
	for _, tc := range []struct {
		scenario, name string
	}{
		{"artifact-copy", "artifact-generator--all-strategies"},
		{"artifact-copy", "artifact-generator--partial-failure"},
		{"artifact-reach", "artifact-generator--bootstrap-input"},
		{"artifact-reach", "artifact-generator--emitted-changed-only-diff"},
		{"artifact-reach", "artifact-generator--input-source-changed-only"},
		{"artifact-reach", "artifact-generator--input-source-changed-only-reverse-committed"},
		{"artifact-reach", "artifact-generator--copied-input"},
		{"artifact-reach", "artifact-generator--copied-whole-source"},
		{"artifact-reach", "artifact-generator--copied-whole-source-reverse-committed"},
		{"artifact-reach", "artifact-generator--copied-renamed-destination"},
		{"artifact-reach", "artifact-generator--copied-unrelated-path"},
		{"artifact-reach", "artifact-generator--copied-consumer-edit"},
		{"artifact-reach", "artifact-generator--copied-reverse-committed"},
		{"artifact-convergence", "artifact-generator--consumed-input-convergence"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := harness.Open(t, tc.scenario, tc.name)
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
		})
	}
}
