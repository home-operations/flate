package baseline

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/networktest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestAutoResolve_CacheMembership(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, ".active/payload", "before")
	testutil.WriteFile(t, root, ".hidden/kept", "kept")
	testutil.WriteFile(t, root, "file", "before")
	base := testutil.GitCommitAll(t, repo, "base", &git.CommitOptions{
		Author: &object.Signature{Name: "test", Email: "test@example.invalid"},
	})
	testutil.WriteFile(t, root, ".active/payload", "after")
	testutil.WriteFile(t, root, "file", "after")
	owner := cachetest.New(t, filepath.Join(root, ".active"), sourcecache.New)
	resolved, err := AutoResolve(
		t.Context(),
		Config{Network: networktest.New(t), Path: root, Base: base.String(), Cache: owner},
	)
	assert.NoError(t, err)
	for _, tc := range []struct {
		name, content string
		input         *store.SourceArtifact
	}{{"base", "before", resolved.BaseInput}, {"head", "after", resolved.Source}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			selected, _, viewErr := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{
				Mode: gittree.Source, Current: tc.input.FS,
			})
			assert.NoError(t, viewErr)
			_, readErr := fs.ReadFile(selected, ".active/payload")
			assert.Equal(t, os.IsNotExist(readErr), true)
			for name, want := range map[string]string{"file": tc.content, ".hidden/kept": "kept"} {
				data, err := fs.ReadFile(selected, name)
				assert.NoError(t, err)
				assert.Equal(t, string(data), want)
			}
			observed, observeErr := selected.FreshInputs(t.Context(), nil, nil)
			assert.NoError(t, observeErr)
			assert.Equal(t, len(observed.Files), 2)
		})
	}
}
