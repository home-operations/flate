package gittree

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/gitignore"
	"github.com/go-git/go-git/v6/plumbing/format/index"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/utils/merkletrie"
	merklefs "github.com/go-git/go-git/v6/utils/merkletrie/filesystem"
	merkleindex "github.com/go-git/go-git/v6/utils/merkletrie/index"
	"github.com/go-git/go-git/v6/utils/merkletrie/noder"

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
	t.Run("git traversal", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		testutil.WriteFile(t, root, "nested/object.yaml", "kept")
		testutil.GitCommitAll(
			t,
			repo,
			"fixture",
			&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
		)
		artifact, err := admitLocal(t, filepath.Join(root, "nested"))
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "nested/.absolute-cache/oci/acquire-witness", "temporary")
		admitted := ExcludeDirectory(artifact, filepath.Join(root, "nested", ".absolute-cache")).FS.(*FS)
		raw := &excludedMetadataBilly{Filesystem: osfs.New(filepath.Join(root, "nested"), osfs.WithBoundOS())}
		tree := &gitView{
			current:  admitted,
			worktree: raw,
			nodes:    map[string]*gitNode{".": {name: ".", mode: fs.ModeDir}},
		}
		assert.NoError(t, tree.walkWorktree(t.Context(), "."))
		assert.Equal(t, raw.observations, 0)
		_, included := tree.nodes[".absolute-cache"]
		assert.Equal(t, included, false)
		for _, current := range []fs.FS{admitted, nil} {
			source, _, newErr := New(
				t.Context(),
				nil,
				plumbing.ZeroHash,
				ViewOptions{
					Mode:       Source,
					Current:    current,
					Worktree:   raw,
					Location:   admitted.location,
					membership: admitted.membership,
				},
			)
			assert.NoError(t, newErr)
			assert.Equal(t, raw.observations, 0)
			_, statErr := source.Stat(".absolute-cache/oci/acquire-witness")
			assert.Equal(t, errors.Is(statErr, fs.ErrNotExist), true)
		}
	})
	t.Run("git acquisition", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		testutil.WriteFile(t, root, "nested/!before-cache", "stable source bytes")
		testutil.WriteFile(t, root, "nested/object.yaml", "kept")
		testutil.GitCommitAll(
			t,
			repo,
			"fixture",
			&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
		)
		artifact, err := admitLocal(t, filepath.Join(root, "nested"))
		assert.NoError(t, err)
		admitted := ExcludeDirectory(artifact, filepath.Join(root, "nested", ".absolute-cache")).FS.(*FS)
		testutil.WriteFile(t, root, "nested/.absolute-cache/oci/acquire-witness", "temporary")
		gate := &cacheMembershipGateFS{
			FS:      admitted,
			barrier: barrier,
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		raw := &excludedMetadataBilly{Filesystem: osfs.New(filepath.Join(root, "nested"), osfs.WithBoundOS())}
		input, _, err := New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			ViewOptions{Mode: Source, Current: gate, Worktree: raw, Location: admitted.location},
		)
		assert.NoError(t, err)
		result := make(chan error, 1)
		go func() { _, err := ContentFingerprint(t.Context(), input); result <- err }()
		<-gate.entered
		removeErr := os.Remove(filepath.Join(root, "nested", filepath.FromSlash(acquisition)))
		close(gate.release)
		assert.NoError(t, removeErr)
		assert.NoError(t, <-result)
		assert.Equal(t, raw.observations, 0)
		assert.Equal(t, input.FileDigests()[barrier] != "", true)
		assert.Equal(t, input.FileDigests()[acquisition], "")
	})
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
				fresh, freshErr := candidate.freshInput(t.Context(), candidate.membership.directory)
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
					observed, observeErr := view.FreshInputs(t.Context(), nil, nil)
					assert.NoError(t, observeErr)
					assert.Equal(t, len(observed.Files), 3)
					captured, captureErr := view.FreshInputs(
						t.Context(),
						nil,
						map[string]fs.FileMode{".cache/payload": 0, "noise": 0},
					)
					assert.NoError(t, captureErr)
					assert.Equal(t, len(captured.Files), 0)
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

type excludedMetadataBilly struct {
	billy.Filesystem

	observations int
}

func (f *excludedMetadataBilly) ReadDir(name string) ([]fs.DirEntry, error) {
	entries, err := f.Filesystem.ReadDir(name)
	for _, entry := range entries {
		if entry.Name() == ".absolute-cache" || name == ".absolute-cache" {
			f.observations++
			return nil, fs.ErrPermission
		}
	}
	return entries, err
}

type metadataObservationFS struct {
	fs.FS

	excluded      atomic.Int64
	admitted      atomic.Int64
	fail          string
	allowExcluded bool
}

func (f *metadataObservationFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if strings.Contains(name, ".absolute-cache") {
		f.excluded.Add(1)
		if !f.allowExcluded {
			return nil, fs.ErrPermission
		}
	}
	entries, err := fs.ReadDir(f.FS, name)
	for i, entry := range entries {
		entries[i] = metadataObservationEntry{
			DirEntry: entry,
			owner:    f,
			name:     filepath.ToSlash(filepath.Join(name, entry.Name())),
		}
	}
	return entries, err
}

type metadataObservationEntry struct {
	fs.DirEntry

	owner *metadataObservationFS
	name  string
}

func (e metadataObservationEntry) Info() (fs.FileInfo, error) {
	if strings.Contains(e.name, ".absolute-cache") {
		e.owner.excluded.Add(1)
		if !e.owner.allowExcluded {
			return nil, fs.ErrPermission
		}
	}
	e.owner.admitted.Add(1)
	if e.name == e.owner.fail {
		return nil, fs.ErrPermission
	}
	return e.DirEntry.Info()
}

func TestContentFingerprint_ExcludedGitReaders(t *testing.T) {
	t.Parallel()
	read := func(files billy.Filesystem, idx *index.Index, route string) error {
		switch route {
		case "identity":
			node := merklefs.NewRootNodeWithOptions(files, nil, merklefs.Options{Index: idx})
			children, err := node.Children()
			if err != nil {
				return err
			}
			for _, child := range children {
				if child.Name() == "nested" {
					_, err = child.Children()
					return err
				}
			}
			return fs.ErrNotExist
		case "rules":
			//nolint:staticcheck // Admission MUST capture ignore errors and observations before index access.
			_, err := gitignore.ReadPatterns(
				files,
				nil,
			)
			return err
		default:
			_, err := merkletrie.DiffTreeContext(
				t.Context(),
				merkleindex.NewRootNode(idx),
				merklefs.NewRootNodeWithOptions(files, nil, merklefs.Options{Index: idx}),
				func(_, _ noder.Hasher) bool { return false },
			)
			return err
		}
	}
	for _, route := range []string{"identity", "rules", "comparison"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			repo := testutil.GitInit(t, root)
			testutil.WriteFile(t, root, "nested/object.yaml", "tracked")
			testutil.WriteFile(t, root, "outside.yaml", "outside")
			testutil.GitCommitAll(
				t,
				repo,
				"fixture",
				&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e"}},
			)
			artifact, err := admitLocal(t, filepath.Join(root, "nested"))
			assert.NoError(t, err)
			testutil.WriteFile(t, root, "nested/.absolute-cache/oci/acquire-witness", "temporary")
			admitted := ExcludeDirectory(artifact, filepath.Join(root, "nested", ".absolute-cache")).FS.(*FS)
			witness := &metadataObservationFS{FS: os.DirFS(root)}
			bridge := admitted.memberFilesystem(osfs.New(root, osfs.WithBoundOS()), filepath.Join(root, "nested"))
			bridge.names = witness
			idx, err := repo.Storer.Index()
			assert.NoError(t, err)

			raw := &excludedMetadataBilly{Filesystem: osfs.New(root, osfs.WithBoundOS())}
			rawErr := read(raw, idx, route)
			t.Logf("raw %s reader: %v, excluded metadata observations: %d", route, rawErr, raw.observations)
			assert.Equal(t, rawErr != nil, true)
			assert.Equal(t, raw.observations, 1)
			assert.NoError(t, read(bridge, idx, route))
			assert.Equal(t, witness.excluded.Load(), int64(0))
			assert.Equal(t, witness.admitted.Load() > 0, true)
			witness.fail = "nested/object.yaml"
			assert.Equal(t, read(bridge, idx, route) != nil, true)
			_, err = bridge.ReadDir("nested")
			assert.Equal(t, err, fs.ErrPermission)
			assert.Equal(t, witness.excluded.Load(), int64(0))

			_, err = ContentFingerprint(t.Context(), admitted)
			assert.NoError(t, err)
			assert.Equal(t, admitted.FileDigests()["object.yaml"] != "", true)
			assert.Equal(t, admitted.FileDigests()[".absolute-cache/oci/acquire-witness"], "")
			testutil.WriteFile(t, root, "nested/.gitignore", "*.fresh\n")
			testutil.WriteFile(t, root, "nested/new.fresh", "ignored after refresh")
			refreshed, err := admitted.freshInput(t.Context(), admitted.membership.directory)
			assert.NoError(t, err)
			assert.Equal(t, refreshed.membership.directory, ".absolute-cache")
			assert.Equal(t, refreshed.excludes("new.fresh"), true)
			observed, err := admitted.FreshInputs(t.Context(), nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, observed.Files["object.yaml"] != "", true)
			assert.Equal(t, observed.Files["new.fresh"], "")
			assert.Equal(t, observed.Files[".absolute-cache/oci/acquire-witness"], "")
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			_, err = admitted.FreshInputs(ctx, nil, nil)
			assert.Equal(t, errors.Is(err, context.Canceled), true)
			_, err = ContentFingerprint(ctx, admitted)
			assert.Equal(t, errors.Is(err, context.Canceled), true)
		})
	}
}

func TestContentFingerprint_OuterExclusionSurvivesRefresh(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "nested/object.yaml", "kept")
	testutil.WriteFile(t, root, "nested/.absolute-cache/oci/acquire-witness", "temporary")
	location, err := gitrepo.Locate(t.Context(), filepath.Join(root, "nested"), filepath.Join(root, "nested"))
	assert.NoError(t, err)
	witness := &metadataObservationFS{FS: os.DirFS(location.ContentRoot), allowExcluded: true}
	direct := &FS{view: &directView{FS: witness}, location: location, snapshot: &localPolicy{}}
	source, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: direct})
	assert.NoError(t, err)
	source = ExcludeDirectory(&store.SourceArtifact{FS: source, LocalPath: location.ContentRoot}, filepath.Join(location.ContentRoot, ".absolute-cache")).FS.(*FS)
	witness.excluded.Store(0)
	witness.allowExcluded = false
	observed, err := source.FreshInputs(t.Context(), nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, observed.Files["object.yaml"] != "", true)
	assert.Equal(t, witness.excluded.Load(), int64(0))
	t.Run("scoped exclusion", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		testutil.WriteFile(t, root, "nested/object.yaml", "kept")
		witness := &metadataObservationFS{FS: os.DirFS(root)}
		input := &FS{view: &directView{FS: witness}, snapshot: &localPolicy{}}
		scoped, err := Sub(input, "nested")
		assert.NoError(t, err)
		testutil.WriteFile(t, root, "nested/.absolute-cache/payload.yaml", "excluded")
		selected := ExcludeDirectory(
			&store.SourceArtifact{FS: scoped, LocalPath: filepath.Join(root, "nested")},
			filepath.Join(root, "nested", ".absolute-cache"),
		).FS.(*FS)
		observed, err := selected.FreshInputs(t.Context(), nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, observed.Files["object.yaml"] != "", true)
		assert.Equal(t, observed.Files[".absolute-cache/payload.yaml"], "")
		assert.Equal(t, witness.excluded.Load(), int64(0))
	})
}
