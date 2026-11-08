package orchestrator

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/baseline"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/gittree"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestPrepareTreeInputs_UsesActualCacheDirectory(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"disk", "symlink", "memory", "plain-filesystem"} {
		t.Run(backing, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for _, name := range []string{".active/payload", ".configured/kept", ".hidden/kept"} {
				testutil.WriteFile(t, root, name, name)
			}
			directory := filepath.Join(root, ".active")
			switch backing {
			case "symlink":
				link := filepath.Join(t.TempDir(), "owner")
				assert.NoError(t, os.Symlink(directory, link))
				directory = link
			case "memory":
				directory = ""
			}
			owner := cachetest.New(t, directory, sourcecache.New)
			input, _, err := baseline.AdmitDirectory(t.Context(), root, root, manifest.WorkScope{})
			assert.NoError(t, err)
			if backing == "plain-filesystem" {
				input.FS = os.DirFS(root)
			}
			trees := []Tree{
				{Input: input, BaseInput: input},
				{Input: input, BaseInput: input},
			}
			err = prepareTreeInputs(t.Context(), trees, map[string]*store.SourceArtifact{}, &Config{
				Cache: owner, CacheDir: filepath.Join(root, ".configured"),
			})
			assert.NoError(t, err)
			assert.Equal(t, trees[0].Input, trees[1].Input)
			assert.Equal(t, trees[0].Input, trees[0].BaseInput)
			selected, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{
				Mode: gittree.Source, Current: trees[0].Input.FS,
			})
			assert.NoError(t, err)
			_, err = fs.ReadFile(selected, ".active/payload")
			assert.Equal(t, os.IsNotExist(err), backing != "memory")
			for _, name := range []string{".configured/kept", ".hidden/kept"} {
				data, readErr := fs.ReadFile(selected, name)
				assert.NoError(t, readErr)
				assert.Equal(t, string(data), name)
			}
			data, err := fs.ReadFile(input.FS, ".active/payload")
			assert.NoError(t, err)
			assert.Equal(t, string(data), ".active/payload")
		})
	}
}
