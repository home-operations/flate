package baseline

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestAutoResolve_ModeOnlyMatchesDetect(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		before, after fs.FileMode
		changed       bool
	}{
		{"executable", 0o644, 0o755, true},
		{"non_executable", 0o755, 0o644, true},
		{"normalized_executable", 0o700, 0o755, false},
		{"normalized_readable", 0o600, 0o644, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const name = "cluster/manifest.yaml"
			const body = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: mode-only\n"
			current, original := t.TempDir(), t.TempDir()
			for _, dir := range []string{current, original} {
				testutil.WriteFile(t, dir, name, body)
				assert.NoError(t, os.Chmod(filepath.Join(dir, name), tc.before))
			}
			repo := testutil.GitInit(t, current)
			base := testutil.GitCommitAll(t, repo, "initial", &git.CommitOptions{
				Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
			})
			assert.NoError(t, os.Chmod(filepath.Join(current, name), tc.after))
			for i, dir := range []string{original, current} {
				data, err := fs.ReadFile(os.DirFS(dir), name)
				assert.NoError(t, err)
				assert.Equal(t, string(data), body)
				info, err := os.Stat(filepath.Join(dir, name))
				assert.NoError(t, err)
				assert.Equal(t, info.Mode().Perm(), []fs.FileMode{tc.before, tc.after}[i])
			}

			resolved, err := AutoResolve(t.Context(), current, base.String(), cachetest.New(t, "", sourcecache.New))
			assert.NoError(t, err)
			paths, err := ResolvePaths(t.Context(), current, original)
			assert.NoError(t, err)
			detected, err := change.Detect(t.Context(), paths.BaseInput, paths.Source, change.DetectConfig{})
			assert.NoError(t, err)

			gitPaths, directoryPaths := resolved.Changes.Paths(), detected.Paths()
			t.Logf("--base changed paths = %q; --path-orig changed paths = %q", gitPaths, directoryPaths)
			assert.Diff(t, directoryPaths, gitPaths)
			var want []string
			if tc.changed {
				want = []string{name}
			}
			assert.Diff(t, gitPaths, want)
			assert.Diff(t, directoryPaths, want)
		})
	}
}
