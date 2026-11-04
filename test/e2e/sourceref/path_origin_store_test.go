package sourceref_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_PathOrigUsesOwnObjectStore(t *testing.T) {
	for _, tt := range []struct {
		name       string
		currentRef string
	}{
		{name: "different-spec", currentRef: "name: refs/tags/v1.0.0"},
		{name: "identical-spec", currentRef: "tag: v1.0.0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "identical-spec" {
				t.Skip(
					"changed-only selection ignores divergent source revisions with identical authored files; tracked in fl-5c42t",
				)
			}
			sourceRefFixtureResult1 := sourceRefFixture(t)
			current, _, _, _ := sourceRefFixtureResult1.root, sourceRefFixtureResult1.repository, sourceRefFixtureResult1.before, sourceRefFixtureResult1.after
			setSourceRef(t, current, tt.currentRef)
			sourceRefFixtureResult2 := sourceRefFixture(t)
			original, originalRepo, _, b := sourceRefFixtureResult2.root, sourceRefFixtureResult2.repository, sourceRefFixtureResult2.before, sourceRefFixtureResult2.after
			assert.NoError(t, originalRepo.Storer.SetReference(
				plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), b),
			))
			for _, side := range []struct {
				root  string
				value string
			}{
				{root: current, value: "v1.0.0"},
				{root: original, value: "v2.0.0"},
			} {
				requireCLIOKResult1 := harness.RequireCLIOK(
					t,
					"build",
					"all",
					"--path",
					filepath.Join(side.root, "flux"),
					"--concurrency",
					"2",
					"--cache-dir",
					t.TempDir(),
				)
				out, _ := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
				assert.FatalIf(
					t,
					!strings.Contains(out, "value: "+side.value),
					"expected %s from %s:\n%s",
					side.value,
					side.root,
					out,
				)
			}
			requireCLIOKResult2 := harness.RequireCLIOK(
				t,
				"diff",
				"all",
				"--path",
				filepath.Join(current, "flux"),
				"--path-orig",
				filepath.Join(original, "flux"),
				"--concurrency",
				"2",
				"--cache-dir",
				t.TempDir(),
				"--git-depth",
				"0",
				"--log-level",
				"warn",
				"-o",
				"diff",
			)
			out, _ := requireCLIOKResult2.Stdout, requireCLIOKResult2.Stderr
			assertSourceValueDiff(t, out, "v2.0.0", "v1.0.0")
		})
	}
}
