package gittree

import (
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/store"
)

func TestContentFingerprint_AcquisitionRemovedAfterAdmission(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	const acquisition = ".absolute-cache/oci/acquire-witness"
	const barrier = "!before-cache"
	testutil.WriteFile(t, root, acquisition, "temporary cache payload")
	testutil.WriteFile(t, root, barrier, "stable source bytes")
	testutil.WriteFile(t, root, "object.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: kept}\n")
	current := &cacheMembershipGateFS{
		FS: os.DirFS(root), barrier: barrier,
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	direct, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{
		Mode: Direct, Current: current, Location: &gitrepo.Location{ContentRoot: root},
	})
	assert.NoError(t, err)
	direct = ExcludeDirectory(
		&store.SourceArtifact{FS: direct, LocalPath: root}, filepath.Join(root, ".absolute-cache"),
	).FS.(*FS)
	input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: direct})
	assert.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		_, fingerprintErr := ContentFingerprint(t.Context(), input)
		result <- fingerprintErr
	}()
	<-current.entered
	removeErr := os.Remove(filepath.Join(root, filepath.FromSlash(acquisition)))
	close(current.release)
	err = <-result
	assert.NoError(t, removeErr)
	assert.NoError(t, err)
	assert.Equal(t, input.FileDigests()[barrier] != "", true)
	_, included := input.FileDigests()[acquisition]
	assert.Equal(t, included, false)
}

type cacheMembershipGateFS struct {
	fs.FS

	barrier string
	once    sync.Once
	entered chan struct{}

	release chan struct{}
}

func (f *cacheMembershipGateFS) Open(name string) (fs.File, error) {
	if name == f.barrier {
		f.once.Do(func() {
			close(f.entered)
			<-f.release
		})
	}
	return f.FS.Open(name)
}

func TestFS_ExcludedDirectoryMembership(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	for _, name := range []string{".cache/payload", ".cache-sibling/kept", ".hidden/kept", "nested/kept"} {
		testutil.WriteFile(t, root, name, name)
	}
	testutil.GitCommitAll(
		t,
		repo,
		"cache fixture",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
	)
	testutil.WriteFile(t, root, ".gitignore", "noise\n")
	testutil.WriteFile(t, root, "noise", "ignored")
	testutil.WriteFile(t, root, "nested/noise", "ignored")
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	original := artifact.FS.(*FS)
	captured := ExcludeDirectory(artifact, filepath.Join(root, ".cache"))
	filtered := captured.FS.(*FS)
	assert.Equal(t, ExcludeDirectory(captured, filepath.Join(root, ".cache")), captured)
	for _, tc := range []struct {
		name string
		mode ViewMode
	}{{"direct", Direct}, {"source", Source}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input, _, newErr := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: tc.mode, Current: filtered})
			assert.NoError(t, newErr)
			for _, candidate := range []*FS{input, input.OperationView()} {
				fresh, freshErr := candidate.freshInput(t.Context())
				assert.NoError(t, freshErr)
				for _, view := range []*FS{candidate, fresh} {
					_, statErr := fs.Stat(view, ".cache/payload")
					assert.Equal(t, os.IsNotExist(statErr), true)
					_, regularErr := view.OpenRegular(".cache/payload")
					assert.Equal(t, os.IsNotExist(regularErr), true)
					entries, readErr := fs.ReadDir(view, ".")
					assert.NoError(t, readErr)
					want := 3
					if tc.mode == Direct {
						want++
					}
					assert.Equal(t, len(entries), want)
					directory, openErr := view.Open(".")
					assert.NoError(t, openErr)
					listed, listErr := directory.(fs.ReadDirFile).ReadDir(-1)
					assert.NoError(t, listErr)
					assert.NoError(t, directory.Close())
					assert.Equal(t, len(listed), want)
					_, readErr = fs.ReadFile(view, "noise")
					assert.Equal(t, os.IsNotExist(readErr), true)
					for _, name := range []string{".cache-sibling/kept", ".hidden/kept", "nested/kept"} {
						data, readErr := fs.ReadFile(view, name)
						assert.NoError(t, readErr)
						assert.Equal(t, string(data), name)
					}
					observed, observeErr := view.FreshInputs(t.Context(), nil)
					assert.NoError(t, observeErr)
					assert.Equal(t, len(observed.Files), 3)
				}
			}
		})
	}
	data, err := fs.ReadFile(original, ".cache/payload")
	assert.NoError(t, err)
	assert.Equal(t, string(data), ".cache/payload")
	sub, err := Sub(filtered, "nested")
	assert.NoError(t, err)
	data, err = fs.ReadFile(sub, "kept")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "nested/kept")
	_, err = fs.ReadFile(sub, "noise")
	assert.Equal(t, os.IsNotExist(err), true)
}
