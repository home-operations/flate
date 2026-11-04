package sourceref_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_PinnedDiff_BaseAndCurrentRefs(t *testing.T) {
	root, repo, _, _ := sourceRefFixture(t)
	loader := installSourceRefTransport(t, repo.Storer)
	setSourceRef(t, root, "tag: v2.0.0")
	for _, concurrency := range []string{"2", "4"} {
		t.Run("concurrency_"+concurrency, func(t *testing.T) {
			out, stderr := harness.RequireCLIOK(
				t,
				"diff",
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--base",
				"HEAD",
				"--concurrency",
				concurrency,
				"--cache-dir",
				t.TempDir(),
				"--git-depth",
				"0",
				"--log-level",
				"warn",
				"-o",
				"diff",
			)
			assertSourceValueDiff(t, out, "v1.0.0", "v2.0.0")
			if loader.calls.Load() != 0 || strings.Contains(stderr, "declared source ref cannot be satisfied locally") {
				t.Fatalf("locally available side refs warned/fetched: calls=%d\n%s", loader.calls.Load(), stderr)
			}
		})
	}
}

//nolint:paralleltest // CLI execution changes process-wide logging.
func TestE2E_PinnedDiff_StablePinIgnoresDirtyContent(t *testing.T) {
	root, repo, _, _ := sourceRefFixture(t)
	loader := installSourceRefTransport(t, repo.Storer)
	harness.MutateFile(t, filepath.Join(root, "apps/cm.yaml"), "value: v2.0.0", "value: dirty")
	local, _ := harness.RequireCLIOK(t, "build", "all", "--path", filepath.Join(root, "flux"),
		"--concurrency", "2", "--cache-dir", t.TempDir())
	if loader.calls.Load() != 0 {
		t.Fatalf("local pin accessed transport %d times", loader.calls.Load())
	}
	exported := t.TempDir()
	testutil.CopyTree(t, root, exported)
	assert.NoError(t, os.RemoveAll(filepath.Join(exported, ".git")))
	fetched, _ := harness.RequireCLIOK(
		t,
		"build",
		"all",
		"--path",
		exported,
		"--concurrency",
		"2",
		"--cache-dir",
		t.TempDir(),
		"--git-depth",
		"0",
	)
	assert.FatalIf(
		t,
		fetched != local || loader.calls.Load() == 0,
		"local/fetched pin bytes differ or no fetch occurred:\nlocal:\n%s\nfetched:\n%s",
		local,
		fetched,
	)
	for _, concurrency := range []string{"2", "4"} {
		t.Run("concurrency_"+concurrency, func(t *testing.T) {
			before := loader.calls.Load()
			out, stderr := harness.RequireCLIOK(
				t,
				"diff",
				"all",
				"--path",
				filepath.Join(root, "flux"),
				"--base",
				"HEAD",
				"--concurrency",
				concurrency,
				"--cache-dir",
				t.TempDir(),
				"--git-depth",
				"0",
				"--log-level",
				"warn",
				"-o",
				"diff",
			)
			if out != "" || loader.calls.Load() != before ||
				strings.Contains(stderr, "declared source ref cannot be satisfied locally") {
				t.Fatalf(
					"stable local pin changed, warned or fetched with dirty working files: calls=%d\n%s\nstderr:\n%s",
					loader.calls.Load()-before,
					out,
					stderr,
				)
			}
		})
	}
}
