package baseline

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func TestAutoResolve_LazyBaseReads(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, current string
		changed       bool
	}{
		{name: "settled", current: "head", changed: true},
		{name: "dirty", current: "dirty", changed: true},
		{name: "reversed", current: "base"},
		{name: "committed_deletion", changed: true},
		{name: "executable_directory", changed: true},
		{name: "unchanged", current: "base"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			initRepoWithFile(t, root, "file", "base")
			writeAndCommit(t, root, "unchanged.yaml", "same")
			pin := writeAndCommit(t, root, ".sourceignore", "ignored.yaml\n")
			repo := openHelper(t, root)
			wt, err := repo.Worktree()
			assert.NoError(t, err)
			switch tc.name {
			case "committed_deletion":
				_, err := wt.Remove("file")
				assert.NoError(t, err)
				_, err = wt.Commit("delete", &git.CommitOptions{Author: &object.Signature{Name: "test", Email: "test@example.com"}})
				assert.NoError(t, err)
			case "executable_directory":
				assert.NoError(t, os.Chmod(filepath.Join(root, "file"), 0o755))
				pin = writeAndCommit(t, root, "file", "base")
				assert.NoError(t, os.Remove(filepath.Join(root, "file")))
				assert.NoError(t, os.Mkdir(filepath.Join(root, "file"), 0o755))
				assert.NoError(t, os.Chmod(filepath.Join(root, "file"), 0o755))
			case "unchanged":
			default:
				writeAndCommit(t, root, "file", "head")
				testutil.WriteFile(t, root, "file", tc.current)
			}
			commit, err := repo.CommitObject(pin)
			assert.NoError(t, err)
			tree, err := commit.Tree()
			assert.NoError(t, err)
			var absent []plumbing.Hash
			for _, name := range []string{"unchanged.yaml", ".sourceignore"} {
				entry, err := tree.FindEntry(name)
				assert.NoError(t, err)
				oid := entry.Hash.String()
				assert.NoError(t, os.Remove(filepath.Join(root, ".git", "objects", oid[:2], oid[2:])))
				absent = append(absent, entry.Hash)
			}
			res, err := AutoResolve(t.Context(), root, pin.String(), cachetest.New(t, "", sourcecache.New))
			assert.NoError(t, err)
			if res.BaseInput.Key == "" || res.Source.Key != "" || res.BaseInput.Revision != pin.String() || res.BaseInput.LocalRoot != root {
				t.Fatalf("baseline identity = %+v; current identity = %+v", res.BaseInput, res.Source)
			}
			var want []string
			if tc.changed {
				want = []string{"file"}
			}
			if got := res.Changes.Paths(); !slices.Equal(got, want) {
				t.Fatalf("selected=%v, want %v", got, want)
			}
			for _, file := range []struct{ name, body string }{
				{"file", "base"}, {"unchanged.yaml", "same"}, {".sourceignore", "ignored.yaml\n"},
			} {
				data, err := fs.ReadFile(res.BaseInput.FS, file.name)
				assert.NoError(t, err)
				assert.Equal(t, string(data), file.body)
			}
			initial, err := fs.Stat(res.BaseInput.FS, "unchanged.yaml")
			assert.NoError(t, err)
			assert.Equal(t, initial.Size(), int64(len("same")))
			for _, body := range []string{"fresh-one", "fresh-two"} {
				testutil.WriteFile(t, root, "unchanged.yaml", body)
				if tc.changed && tc.name != "executable_directory" {
					testutil.WriteFile(t, root, "file", "live-mutated")
				}
				view, pin, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: res.BaseInput.FS})
				assert.NoError(t, err)
				assert.Equal(t, pin, res.BaseInput.Key)
				data, err := fs.ReadFile(view, "unchanged.yaml")
				assert.NoError(t, err)
				assert.Equal(t, string(data), body)
				info, err := fs.Stat(view, "unchanged.yaml")
				assert.NoError(t, err)
				assert.Equal(t, info.Size(), int64(len(body)))
				data, err = fs.ReadFile(view, "file")
				assert.NoError(t, err)
				assert.Equal(t, string(data), "base")
			}

			for _, hash := range absent {
				assert.Equal(t, errors.Is(repo.Storer.HasEncodedObject(hash), plumbing.ErrObjectNotFound), true)
			}
			testutil.WriteFile(t, root, ".sourceignore", "file\n")
			view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: res.BaseInput.FS})
			assert.NoError(t, err)
			_, err = fs.Stat(view, "file")
			assert.NoError(t, err)
		})
	}
}

func TestAutoResolve_IgnoreMembershipFlips(t *testing.T) {
	t.Parallel()
	for _, behavior := range []string{"exclude", "reinclude"} {
		t.Run(behavior, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			initRepoWithFile(t, root, "apps/a.yaml", "bystander")
			pin := writeAndCommit(t, root, "apps/b.yaml", "unchanged")
			if behavior == "reinclude" {
				pin = writeAndCommit(t, root, ".sourceignore", "apps/b.yaml\n")
				assert.NoError(t, os.Remove(filepath.Join(root, ".sourceignore")))
			} else {
				testutil.WriteFile(t, root, ".sourceignore", "apps/b.yaml\n")
			}
			res, err := AutoResolve(t.Context(), root, pin.String(), cachetest.New(t, "", sourcecache.New))
			assert.NoError(t, err)
			assert.Diff(t, res.Changes.Paths(), []string{".sourceignore", "apps/b.yaml"})
			testutil.WriteFile(t, root, ".sourceignore", "*.yaml\n")
			for side, input := range []*store.SourceArtifact{res.BaseInput, res.Source} {
				view, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Source, Current: input.FS})
				assert.NoError(t, err)
				_, err = fs.Stat(view, "apps/b.yaml")
				wantVisible := behavior == "exclude" && side == 0 || behavior == "reinclude" && side == 1
				assert.Equal(t, err == nil, wantVisible)
			}
		})
	}
}
