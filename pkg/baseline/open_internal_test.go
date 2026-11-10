package baseline

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	gitindex "github.com/go-git/go-git/v5/plumbing/format/index"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

func (f *inputFailureFS) Open(name string) (fs.File, error) {
	file, err := f.FS.Open(name)
	if err != nil || !strings.HasSuffix(name, ".yaml") {
		return file, err
	}
	return &inputFailureFile{File: file, fail: func() error { return f.fail(name) }}, nil
}

type inputFailureFile struct {
	fs.File

	fail func() error
}

func (f *inputFailureFile) Read([]byte) (int, error) {
	return 0, f.fail()
}

func TestAutoResolve_SubmoduleWarningAttribution(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	pin := initRepoWithFile(t, root, "file", "base")
	repo := openHelper(t, root)
	commit, err := repo.CommitObject(pin)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	tree.Entries = append(tree.Entries, object.TreeEntry{Name: "module", Mode: filemode.Submodule, Hash: pin})
	sort.Sort(object.TreeEntrySorter(tree.Entries))
	encoded := repo.Storer.NewEncodedObject()
	assert.NoError(t, tree.Encode(encoded))
	commit.TreeHash, err = repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	encoded = repo.Storer.NewEncodedObject()
	assert.NoError(t, commit.Encode(encoded))
	pin, err = repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	setRef(t, repo, plumbing.NewBranchReferenceName("base-with-module"), pin)
	result, err := AutoResolve(t.Context(), root, "base-with-module", cachetest.New(t, "", sourcecache.New))
	assert.NoError(t, err)
	warnings := result.BaseInput.Warnings
	if len(warnings) != 1 || warnings[0].Category != manifest.WarnSkippedSubmodule ||
		warnings[0].Resource != manifest.BootstrapSourceID ||
		len(result.Source.Warnings) != 0 {
		t.Fatalf("baseline source evidence = %+v, current=%+v", warnings, result.Source.Warnings)
	}
	raw := result.BaseInput.FS.(*gittree.FS).Warnings

	assert.FatalIf(
		t,
		len(raw) != 1 || raw[0].Resource != (manifest.NamedResource{}),
		"borrowed filesystem evidence mutated: %+v",
		raw,
	)

	assert.Diff(t, warnings[0].Detail, raw[0].Detail)
}

func TestAutoResolve_SparseGitMemory(t *testing.T) {
	t.Parallel()
	for _, size := range []struct {
		name  string
		files int
		bytes int64
	}{
		{"routine", 24, 4096}, {"exact", 12000, 256 << 20},
	} {
		t.Run(size.name, func(t *testing.T) {
			t.Parallel()
			if size.name == "exact" && os.Getenv("FLATE_E8_SLOW") != "1" {
				t.Skip(
					"MEM-SPARSE not-run: env FLATE_E8_SLOW=1 go test ./pkg/baseline -run '^TestAutoResolve_SparseGitMemory/exact$' -count=1 -v",
				)
			}
			fixture := testutil.NewSparseGit(t, size.files, size.bytes)
			owner := cachetest.New(t, "", sourcecache.New)
			res, err := AutoResolve(t.Context(), filepath.Join(fixture.Root, "flux"), fixture.Base.String(), owner)
			assert.NoError(t, err)
			assert.Diff(t, res.Changes.Paths(), []string{"app/input.yaml"})

			{
				_, ok := res.BaseInput.FS.(*gittree.FS)
				assert.FatalIf(t, !ok, "%v", "baseline lost concrete admission")
			}

			{
				_, ok := res.Source.FS.(*gittree.FS)
				assert.FatalIf(t, !ok, "%v", "current lost concrete admission")
			}

			for _, side := range []struct {
				input *store.SourceArtifact
				value string
			}{{res.BaseInput, "before"}, {res.Source, "after!"}} {
				data, localErr := fs.ReadFile(side.input.FS, "app/input.yaml")
				assert.NoError(t, localErr)

				assert.FatalIf(t, !strings.Contains(string(data), side.value), "changed payload = %s", data)

				info, localErr := fs.Stat(side.input.FS, "unrelated/large.txt")
				assert.NoError(t, localErr)
				assert.Equal(t, info.Size(), size.bytes)
			}
			testutil.WriteFile(t, fixture.Root, "disjoint/growth.txt", "unconsumed growth\n")
			wt, err := fixture.Repo.Worktree()
			assert.NoError(t, err)
			_, err = wt.Add("disjoint/growth.txt")
			assert.NoError(t, err)
			_, err = wt.Commit(
				"disjoint growth",
				&git.CommitOptions{Author: &object.Signature{Name: "fixture", Email: "fixture@example.test"}},
			)
			assert.NoError(t, err)
			grown, err := AutoResolve(t.Context(), filepath.Join(fixture.Root, "flux"), fixture.Base.String(), owner)
			assert.NoError(t, err)
			assert.Diff(t, grown.Changes.Paths(), []string{"app/input.yaml", "disjoint/growth.txt"})
			t.Logf(
				"MEM-SPARSE public AutoResolve: committed-files=%d unchanged-blob-bytes=%d base=%s head=%s blob=%s; admission Reader counters unavailable on public route",
				size.files,
				size.bytes,
				fixture.Base,
				fixture.Head,
				fixture.Large,
			)
		})
	}
}

func TestAutoResolve_SparseHistoricalBodyUnavailable(t *testing.T) {
	t.Parallel()
	fixture := testutil.NewSparseGit(t, 24, 4096)
	commit, err := fixture.Repo.CommitObject(fixture.Base)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	entry, err := tree.FindEntry("unrelated/large.txt")
	assert.NoError(t, err)
	assert.Equal(t, entry.Hash, fixture.Large)
	oid := fixture.Large.String()
	assert.NoError(t, os.Remove(filepath.Join(fixture.Root, ".git", "objects", oid[:2], oid[2:])))
	res, err := AutoResolve(
		t.Context(),
		filepath.Join(fixture.Root, "flux"),
		fixture.Base.String(),
		cachetest.New(t, "", sourcecache.New),
	)
	assert.NoError(t, err)
	assert.Diff(t, res.Changes.Paths(), []string{"app/input.yaml"})
	for _, input := range []*store.SourceArtifact{res.BaseInput, res.Source} {
		data, err := fs.ReadFile(input.FS, "unrelated/large.txt")
		assert.NoError(t, err)
		assert.Equal(t, len(data), 4096)
		assert.Equal(t, bytes.Equal(data, make([]byte, 4096)), true)
	}
	assert.Equal(t, errors.Is(fixture.Repo.Storer.HasEncodedObject(fixture.Large), plumbing.ErrObjectNotFound), true)
}

func TestSelection_SparseGitBodies(t *testing.T) {
	t.Parallel()
	for _, size := range []struct {
		name  string
		files int
		bytes int64
	}{
		{"routine", 24, 4096}, {"exact", 12000, 256 << 20},
	} {
		t.Run(size.name, func(t *testing.T) {
			t.Parallel()
			if size.name == "exact" && os.Getenv("FLATE_E8_SLOW") != "1" {
				t.Skip(
					"MEM-SPARSE not-run: env FLATE_E8_SLOW=1 go test ./pkg/baseline -run '^TestSelection_SparseGitBodies/exact$' -count=1 -v",
				)
			}
			fixture := testutil.NewSparseGit(t, size.files, size.bytes)
			location, err := gitrepo.Locate(t.Context(), fixture.Root, "")
			assert.NoError(t, err)
			input, err := admitLocation(t.Context(), location, manifest.WorkScope{})
			assert.NoError(t, err)
			reads := &testutil.GitReads{LargeHash: fixture.Large}
			observed := *fixture.Repo
			observed.Storer = &testutil.CountingStorer{Storer: fixture.Repo.Storer, Reads: reads}
			commit, err := observed.CommitObject(fixture.Base)
			assert.NoError(t, err)
			tree, err := commit.Tree()
			assert.NoError(t, err)
			entry, err := tree.FindEntry("app/input.yaml")
			assert.NoError(t, err)
			blob, err := object.GetBlob(observed.Storer, entry.Hash)
			assert.NoError(t, err)
			reader, err := blob.Reader()
			assert.NoError(t, err)
			data, err := io.ReadAll(reader)
			assert.NoError(t, errors.Join(err, reader.Close()))
			if !strings.Contains(string(data), "before") || reads.Bodies.Load() != 1 || reads.Large.Load() != 0 {
				t.Fatal("blob Reader calibration failed")
			}
			reads.Reset()
			s := selection{
				repo:    &observed,
				base:    &resolution{Repo: &observed, Hash: fixture.Base},
				current: input.FS.(*gittree.FS),
			}
			entries, comparison, err := s.baseEntries(t.Context())
			assert.NoError(t, err)
			if reads.Metadata.Load() == 0 || reads.Bodies.Load() != 0 {
				t.Fatal("metadata traversal opened a blob Reader")
			}
			t.Logf(
				"baseEntries metadata=%d bodies=%d large=%d",
				reads.Metadata.Load(),
				reads.Bodies.Load(),
				reads.Large.Load(),
			)
			reads.Reset()
			candidates, warnings, err := s.candidates(t.Context(), entries, comparison)
			assert.NoError(t, err)
			assert.Equal(t, len(warnings), 0)
			assert.Diff(t, candidates, map[string]bool{"app/input.yaml": false})
			assert.Equal(t, reads.Bodies.Load(), int64(0))
			t.Logf(
				"candidates metadata=%d bodies=%d large=%d; checkout hashing is outside these counters",
				reads.Metadata.Load(),
				reads.Bodies.Load(),
				reads.Large.Load(),
			)
			reads.Reset()
			assert.NoError(t, s.acquireBodies(t.Context(), entries, candidates))
			paths := slices.Sorted(maps.Keys(candidates))
			assert.Diff(t, paths, []string{"app/input.yaml"})
			assert.Equal(t, reads.Bodies.Load(), int64(0))
			t.Logf(
				"committed comparison metadata=%d bodies=%d large=%d",
				reads.Metadata.Load(),
				reads.Bodies.Load(),
				reads.Large.Load(),
			)
			reads.Reset()
			base, _, err := gittree.New(
				t.Context(),
				fixture.Repo.Storer.(*filesystem.Storage).Filesystem(),
				fixture.Base,
				gittree.ViewOptions{Mode: gittree.Direct, Current: input.FS, Changes: paths},
			)
			assert.NoError(t, err)
			before := &sparseReadFS{FS: base, reads: reads, entries: entries}
			delta, err := change.Detect(t.Context(), &store.SourceArtifact{FS: before}, input,
				change.DetectConfig{Candidates: map[string]bool{"app/input.yaml": true}})
			assert.NoError(t, err)
			paths = delta.Paths()
			assert.Diff(t, paths, []string{"app/input.yaml"})
			assert.Equal(t, reads.Bodies.Load(), int64(1))
			assert.Equal(t, reads.Large.Load(), int64(0))
			t.Logf(
				"dirty comparison metadata=%d bodies=%d large=%d; selectChanges/public admission not wrapped",
				reads.Metadata.Load(),
				reads.Bodies.Load(),
				reads.Large.Load(),
			)
		})
	}
}

func TestAutoResolve_CheckoutInventory(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"unchanged_symlink", "ignored_file", "tracked_match", "unignored_file", "submodule_file"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			initRepoWithFile(t, root, "input.yaml", "before")
			repo := openHelper(t, root)
			worktree, err := repo.Worktree()
			assert.NoError(t, err)
			var want []string
			switch kind {
			case "unchanged_symlink":
				assert.NoError(t, os.Symlink("input.yaml", filepath.Join(root, "link.yaml")))
				_, err = worktree.Add("link.yaml")
				assert.NoError(t, err)
			case "ignored_file":
				testutil.WriteFile(t, root, ".gitignore", "ignored/\n")
				_, err = worktree.Add(".gitignore")
				assert.NoError(t, err)
				testutil.WriteFile(t, root, "ignored/input.yaml", "new")
			case "tracked_match":
				testutil.WriteFile(t, root, ".gitignore", "input.yaml\n")
				_, err = worktree.Add(".gitignore")
				assert.NoError(t, err)
			case "unignored_file":
				want = []string{"notes.txt"}
			case "submodule_file":
				testutil.WriteFile(t, root, ".gitignore", "module/\n")
				_, err = worktree.Add(".gitignore")
				assert.NoError(t, err)
				module := filepath.Join(root, "module")
				assert.NoError(t, os.Mkdir(module, 0o755))
				hash := initRepoWithFile(t, module, "input.yaml", "new")
				testutil.WriteFile(
					t,
					root,
					".gitmodules",
					"[submodule \"module\"]\n\tpath = module\n\turl = ./module\n",
				)
				_, err = worktree.Add(".gitmodules")
				assert.NoError(t, err)
				index, indexErr := repo.Storer.Index()
				assert.NoError(t, indexErr)
				index.Entries = append(
					index.Entries,
					&gitindex.Entry{Name: "module", Hash: hash, Mode: filemode.Submodule},
				)
				assert.NoError(t, repo.Storer.SetIndex(index))
				want = []string{"module/input.yaml"}
			}
			pin, err := worktree.Commit(
				"checkout inventory",
				&git.CommitOptions{
					AllowEmptyCommits: true,
					Author:            &object.Signature{Name: "test", Email: "test@example.invalid"},
				},
			)
			assert.NoError(t, err)
			if kind == "tracked_match" {
				testutil.WriteFile(t, root, "input.yaml", "modified tracked file")
				want = []string{"input.yaml"}
			}
			if kind == "unignored_file" {
				testutil.WriteFile(t, root, "notes.txt", "untracked input")
			}
			result, err := AutoResolve(t.Context(), root, pin.String(), cachetest.New(t, "", sourcecache.New))
			assert.NoError(t, err)
			assert.Diff(t, result.Changes.Paths(), want)
		})
	}
}

func TestAutoResolve_IgnoredHistoricalRemoval(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	for _, name := range []string{"historical.ignored", "tracked.ignored"} {
		testutil.WriteFile(t, root, name, "identical historical bytes")
	}
	base := testutil.GitCommitAll(t, repo, "historical inputs", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@e"},
	})
	worktree, err := repo.Worktree()
	assert.NoError(t, err)
	_, err = worktree.Remove("historical.ignored")
	assert.NoError(t, err)
	testutil.WriteFile(t, root, ".gitignore", "*.ignored\n")
	head := testutil.GitCommitAll(t, repo, "remove historical input", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@e"},
	})
	assert.Equal(t, base != head, true)
	testutil.WriteFile(t, root, "historical.ignored", "identical historical bytes")
	result, err := AutoResolve(t.Context(), root, base.String(), cachetest.New(t, "", sourcecache.New))
	assert.NoError(t, err)
	data, err := fs.ReadFile(result.BaseInput.FS, "historical.ignored")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "identical historical bytes")
	_, err = fs.ReadFile(result.Source.FS, "historical.ignored")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	for _, input := range []*store.SourceArtifact{result.BaseInput, result.Source} {
		data, readErr := fs.ReadFile(input.FS, "tracked.ignored")
		assert.NoError(t, readErr)
		assert.Equal(t, string(data), "identical historical bytes")
	}
	assert.Diff(t, result.Changes.Paths(), []string{"historical.ignored"})
}

type sparseReadFS struct {
	*gittree.FS

	reads   *testutil.GitReads
	entries map[string]object.TreeEntry
}

func (f *sparseReadFS) Open(name string) (fs.File, error) {
	file, err := f.FS.Open(name)
	if err == nil {
		f.reads.Bodies.Add(1)
		if f.entries[name].Hash == f.reads.LargeHash {
			f.reads.Large.Add(1)
		}
	}
	return file, err
}
