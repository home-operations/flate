package sourceref_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_SourceRef_NonHEADRendersPreserveCheckout(t *testing.T) {
	for _, kind := range []string{"tag", "commit", "semver", "head", "no-ref"} {
		t.Run(kind, func(t *testing.T) {
			sourceRefFixtureResult1 := sourceRefFixture(t)
			root, repo, a, _ := sourceRefFixtureResult1.root, sourceRefFixtureResult1.repository, sourceRefFixtureResult1.before, sourceRefFixtureResult1.after
			ref := "tag: v1.0.0"
			want := "v1.0.0"
			switch kind {
			case "commit":
				ref = "commit: " + a.String()
			case "semver":
				ref = "semver: '<2.0.0'"
			case "head":
				ref, want = "tag: v2.0.0", "dirty"
			case "no-ref":
				ref, want = "", "dirty"
			}
			setSourceRef(t, root, ref)
			harness.MutateFile(t, filepath.Join(root, "apps", "cm.yaml"), "value: v2.0.0", "value: dirty")
			wt, err := repo.Worktree()
			assert.NoError(t, err)
			beforeHead, err := repo.Head()
			assert.NoError(t, err)
			beforeStatus, err := wt.Status()
			assert.NoError(t, err)
			before := make(map[string][]byte)
			for _, path := range []string{".git/HEAD", ".git/index", "flux/entry.yaml", "apps/cm.yaml", "apps/kustomization.yaml"} {
				before[path], err = os.ReadFile(filepath.Join(root, path))
				assert.NoError(t, err)
			}
			requireCLIOKResult1 := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
				"--concurrency", "2", "--cache-dir", t.TempDir())
			out, _ := requireCLIOKResult1.Stdout, requireCLIOKResult1.Stderr
			assert.FatalIf(t, !strings.Contains(out, "value: "+want) ||
				kind != "head" && kind != "no-ref" &&
					strings.Contains(out, "dirty"), "expected %s content:\n%s", want, out)
			afterHead, err := repo.Head()
			assert.FatalIf(
				t,
				err != nil || afterHead.Hash() != beforeHead.Hash() || afterHead.Name() != beforeHead.Name(),
				"HEAD changed: %v, %v",
				afterHead,
				err,
			)
			afterStatus, err := wt.Status()
			{
				diff := cmp.Diff(beforeStatus, afterStatus)
				assert.FatalIf(t, err != nil || diff != "", "checkout status changed (-want +got):\n%s\n%v", diff, err)
			}
			for path, want := range before {
				got, err := os.ReadFile(filepath.Join(root, path))
				assert.FatalIf(t, err != nil || !bytes.Equal(got, want), "checkout file %s changed: %v", path, err)
			}
		})
	}
}
