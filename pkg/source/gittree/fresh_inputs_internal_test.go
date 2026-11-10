package gittree

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFreshInputs_CurrentBytesAndMembership(t *testing.T) {
	t.Parallel()
	for _, mode := range []ViewMode{Direct, Source} {
		t.Run(map[ViewMode]string{Direct: "direct", Source: "source", RemoteBase: "remote"}[mode], func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "file", "alpha")
			testutil.WriteFile(t, root, "ignored", "hidden")
			testutil.WriteFile(t, root, ".sourceignore", "ignored\n")
			testutil.WriteFile(t, root, ".gitignore", "git-hidden\ntracked-later\nrule-hidden\n")
			old := time.Unix(946684800, 0)
			assert.NoError(t, os.Chtimes(filepath.Join(root, "file"), old, old))
			testutil.GitCommitAll(
				t,
				repo,
				"fixture",
				&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: old}},
			)
			for _, name := range []string{"git-hidden", "tracked-later", "rule-hidden"} {
				testutil.WriteFile(t, root, name, name)
			}
			input, err := admitLocal(t, root)
			assert.NoError(t, err)
			selected, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: input.FS})
			assert.NoError(t, err)
			_, err = ContentFingerprint(t.Context(), selected)
			assert.NoError(t, err)
			before, err := selected.FreshInputs(t.Context(), nil, nil)
			assert.NoError(t, err)
			_, err = selected.Stat("new")
			assert.Equal(t, os.IsNotExist(err), true)
			testutil.WriteFile(t, root, "file", "bravo")
			assert.NoError(t, os.Chtimes(filepath.Join(root, "file"), old, old))
			testutil.WriteFile(t, root, "new", "added")
			assert.NoError(t, os.Mkdir(filepath.Join(root, "empty"), 0o700))
			testutil.WriteFile(t, root, ".sourceignore", "")
			testutil.WriteFile(t, root, ".gitignore", "git-hidden\ntracked-later\nnoise.new\n")
			testutil.WriteFile(t, root, "noise.new", "new ignored bytes")
			worktree, err := repo.Worktree()
			assert.NoError(t, err)
			assert.NoError(t, worktree.AddWithOptions(&git.AddOptions{Path: "tracked-later", SkipStatus: true}))
			captured, err := selected.FreshInputs(
				t.Context(),
				[]string{"file", "new"},
				map[string]fs.FileMode{"file": 0},
			)
			assert.NoError(t, err)
			assert.Equal(
				t,
				captured.Files["file"],
				manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte("bravo"))),
			)
			assert.Equal(t, captured.Files["new"], "")
			after, err := selected.FreshInputs(t.Context(), nil, nil)
			assert.NoError(t, err)
			assert.Equal(
				t,
				after.Files["file"],
				manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte("bravo"))),
			)
			assert.Equal(t, after.Files["file"] != before.Files["file"], true)
			assert.Equal(t, after.Files["new"] != "", true)
			assert.Equal(t, after.Files["ignored"] != "", true)
			assert.Equal(t, after.Files["git-hidden"], "")
			assert.Equal(t, after.Files["noise.new"], "")
			assert.Equal(t, after.Files["tracked-later"] != "", true)
			assert.Equal(t, after.Files["rule-hidden"] != "", true)
			assert.Equal(t, before.Files["tracked-later"], "")
			assert.Equal(t, before.Files["rule-hidden"], "")
			for _, name := range []string{"git-hidden", "tracked-later", "rule-hidden"} {
				_, statErr := selected.Stat(name)
				assert.Equal(t, os.IsNotExist(statErr), true)
			}
			assert.Equal(t, after.Types["empty"]&fs.ModeDir != 0, mode == Direct)
			assert.NoError(t, os.Remove(filepath.Join(root, "file")))
			assert.NoError(t, os.Mkdir(filepath.Join(root, "file"), 0o700))
			replaced, err := selected.FreshInputs(t.Context(), nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, replaced.Files["file"], "")
			assert.Equal(t, replaced.Types["file"]&fs.ModeDir != 0, mode == Direct)
		})
	}
}

func TestFreshInputs_CapturedDirectory(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	assert.NoError(t, os.Mkdir(filepath.Join(root, "directory"), 0o700))
	input, err := admitLocal(t, root)
	assert.NoError(t, err)
	observed, err := input.FS.(*FS).FreshInputs(t.Context(), nil, map[string]fs.FileMode{"directory": fs.ModeDir})
	assert.NoError(t, err)
	_, identified := observed.Files["directory"]
	assert.Equal(t, identified, false)
	assert.Equal(t, observed.Types["directory"].IsDir(), true)
}

func TestFreshInputs_ReusesObservedRegularKind(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.WriteFile(t, root, "file", "alpha")
	testutil.WriteFile(t, root, "unrelated", "untouched")
	input, err := admitLocal(t, root)
	assert.NoError(t, err)
	original := input.FS.(*FS)
	observed := &observedRegularView{view: original.view}
	original.view = observed
	first, err := original.FreshInputs(t.Context(), nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, observed.regularReads, 1)
	assert.Equal(t, observed.unclassifiedReads, 0)
	testutil.WriteFile(t, root, "file", "bravo")
	second, err := original.FreshInputs(t.Context(), []string{"file"}, nil)
	assert.NoError(t, err)
	assert.Equal(t, observed.regularReads, 2)
	assert.Equal(t, observed.unclassifiedReads, 0)
	assert.Equal(t, first.Files["file"] != second.Files["file"], true)
	assert.Equal(t, second.Files["unrelated"], "")
	_, observedKind := second.Types["unrelated"]
	assert.Equal(t, observedKind, true)
	assert.Equal(t, observed.unrelatedReads, 1)
}

type observedRegularView struct {
	view

	regularReads, unclassifiedReads, unrelatedReads int
}

func (v *observedRegularView) ReadFile(name string) ([]byte, error) {
	if name == "file" {
		v.unclassifiedReads++
	}
	return v.view.ReadFile(name)
}

func (v *observedRegularView) OpenRegular(name string) (fs.File, error) {
	if name == "unrelated" {
		v.unrelatedReads++
	}
	if name == "file" {
		v.regularReads++
	}
	return v.view.OpenRegular(name)
}
