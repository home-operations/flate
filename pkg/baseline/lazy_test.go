package baseline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/memory"

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
		{name: "checkout_deletion", changed: true},
		{name: "untracked", current: "base"},
		{name: "malformed_index", current: "dirty", changed: true},
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
			case "checkout_deletion":
				assert.NoError(t, os.Remove(filepath.Join(root, "file")))
			case "untracked":
				testutil.WriteFile(t, root, "untracked.yaml", "new")
			case "malformed_index":
				testutil.WriteFile(t, root, "file", tc.current)
				testutil.WriteFile(t, root, ".git/index", "invalid index")
				repo = openHelper(t, root)
				wt, err := repo.Worktree()
				assert.NoError(t, err)
				if _, err := wt.Status(); err == nil {
					t.Fatal("malformed index did not fail Worktree.Status")
				}
				head, err := repo.Head()
				assert.NoError(t, err)
				assert.Equal(t, head.Hash(), pin)
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
				if tc.name == "malformed_index" {
					continue
				}
				entry, err := tree.FindEntry(name)
				assert.NoError(t, err)
				oid := entry.Hash.String()
				assert.NoError(t, os.Remove(filepath.Join(root, ".git", "objects", oid[:2], oid[2:])))
				absent = append(absent, entry.Hash)
			}
			if tc.name == "settled" || tc.name == "committed_deletion" {
				location, err := gitrepo.Locate(t.Context(), root, "")
				assert.NoError(t, err)
				input, err := admitLocation(t.Context(), location)
				assert.NoError(t, err)
				entries, err := treeEntries(t.Context(), tree)
				assert.NoError(t, err)
				s := selection{repo: repo, current: input.FS.(*gittree.FS), base: &resolution{Repo: &git.Repository{Storer: memory.NewStorage()}}}
				candidates, _, err := s.candidates(t.Context(), tree, entries)
				assert.NoError(t, err)
				dirty, present := candidates["file"]
				assert.Equal(t, present, true)
				assert.Equal(t, dirty, false)
				routing, err := s.compareCandidates(t.Context(), candidates, entries)
				assert.NoError(t, err)
				assert.Diff(t, routing, []string{"file"})
			}
			res, err := AutoResolve(t.Context(), root, pin.String(), cachetest.New(t, "", sourcecache.New))
			if tc.name == "malformed_index" {
				if res == nil || len(res.Source.Warnings) != 1 || res.Source.Warnings[0].Category != manifest.WarnBaselineStatusUnavailable || len(res.BaseInput.Warnings) != 0 {
					t.Fatalf("current-only status evidence = %+v", res)
				}
			}
			assert.NoError(t, err)
			if res.BaseInput.Key == "" || res.Source.Key != "" || res.BaseInput.Revision != pin.String() || res.BaseInput.LocalRoot != root {
				t.Fatalf("baseline identity = %+v; current identity = %+v", res.BaseInput, res.Source)
			}
			var want []string
			if tc.changed {
				want = []string{"file"}
			}
			if tc.name == "untracked" {
				want = []string{"untracked.yaml"}
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

func TestAutoResolve_LocalBaseMissingObjects(t *testing.T) {
	t.Parallel()
	for _, missing := range []string{"tree", "clean_candidate_blob"} {
		t.Run(missing, func(t *testing.T) {
			t.Parallel()
			root, origin := t.TempDir(), t.TempDir()
			pin := initRepoWithFile(t, root, "apps/file.yaml", "base")
			repo := openHelper(t, root)
			setRef(t, repo, plumbing.NewBranchReferenceName("selected"), pin)
			writeAndCommit(t, root, "apps/file.yaml", "head")
			testutil.CopyTree(t, root, origin)
			advanced := writeAndCommit(t, origin, "apps/file.yaml", "moving remote")
			remote := openHelper(t, origin)
			setRef(t, remote, plumbing.NewBranchReferenceName("selected"), advanced)
			setRef(t, remote, plumbing.NewBranchReferenceName("pinned"), pin)
			url := filepath.Join(origin, ".git")
			addOrigin(t, root, url)
			commit, err := repo.CommitObject(pin)
			assert.NoError(t, err)
			tree, err := commit.Tree()
			assert.NoError(t, err)
			absent := tree.Hash
			if missing == "clean_candidate_blob" {
				entry, err := tree.FindEntry("apps/file.yaml")
				assert.NoError(t, err)
				absent = entry.Hash
			}
			oid := absent.String()
			objectPath := filepath.Join(root, ".git", "objects", oid[:2], oid[2:])
			assert.NoError(t, os.Remove(objectPath))
			repo = openHelper(t, root)
			_, err = repo.CommitObject(pin)
			assert.NoError(t, err)
			wt, err := repo.Worktree()
			assert.NoError(t, err)
			status, err := wt.Status()
			assert.NoError(t, err)
			assert.Equal(t, status.IsClean(), true)
			owner := cachetest.New(t, "", sourcecache.New)
			var identity string
			for round := range 2 {
				res, err := AutoResolve(t.Context(), filepath.Join(root, "apps"), "selected", owner)
				assert.NoError(t, err)
				assert.Equal(t, res.BaseInput.Revision, pin.String())
				assert.Diff(t, res.Source.Metadata, map[string]string{"baseline-selection": "explicit --base=selected"})
				assert.Diff(t, res.Changes.Paths(), []string{"apps/file.yaml"})
				label := filepath.Join(root, ".flate-base", pin.String())
				assert.Equal(t, res.Source.LocalPath, root)
				assert.Equal(t, res.Source.LocalRoot, root)
				assert.Equal(t, res.Source.Key, "")
				assert.Equal(t, res.BaseInput.LocalPath, label)
				assert.Equal(t, res.BaseInput.LocalRoot, root)
				assert.Equal(t, res.ScanPath, filepath.Join(root, "apps"))
				assert.Equal(t, res.BaseScanPath, filepath.Join(label, "apps"))
				assert.Diff(t, res.SelfURLs, []string{url})
				if res.BaseInput.Key == "" {
					t.Fatal("baseline identity is empty")
				}
				if round == 0 {
					identity = res.BaseInput.Key
				}
				assert.Equal(t, res.BaseInput.Key, identity)
				before, err := fs.ReadFile(res.BaseInput.FS, "apps/file.yaml")
				assert.NoError(t, err)
				assert.Equal(t, string(before), "base")
				after, err := fs.ReadFile(res.Source.FS, "apps/file.yaml")
				assert.NoError(t, err)
				assert.Equal(t, string(after), "head")
				assert.Equal(t, errors.Is(repo.Storer.HasEncodedObject(absent), plumbing.ErrObjectNotFound), true)
				_, err = os.Stat(objectPath)
				assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
			}
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
			pin := writeAndCommit(t, root, "apps/b.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: flux-system}\ndata: {VALUE: unchanged}\n")
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
				app := testutil.KustomizationID("flux-system", "app")
				index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					app: {File: "flux/app.yaml", Refs: []manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "optional"}}},
				}}
				filter := change.NewFilter(res.Changes, index, nil)
				if !filter.ShouldReconcile(app) {
					t.Errorf("side %d ignore %s did not select optional input consumer", side, behavior)
				}
			}
		})
	}
}

func TestAutoResolve_TwoSidedInput(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	const body = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: flux-system}\n"
	pin := initRepoWithFile(t, root, "input.yaml", body+"data: {VALUE: before}\n")
	writeAndCommit(t, root, "input.yaml", body+"data: {VALUE: after}\n")
	res, err := AutoResolve(t.Context(), root, pin.String(), cachetest.New(t, "", sourcecache.New))
	assert.NoError(t, err)
	assert.Diff(t, res.Changes.Paths(), []string{"input.yaml"})
	app := testutil.KustomizationID("flux-system", "app")
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		app: {File: "flux/app.yaml", Refs: []manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "optional"}}},
	}}
	filter := change.NewFilter(res.Changes, index, nil)
	if filter.ShouldReconcile(app) {
		t.Fatal("two-sided input supplied an absent input identity")
	}
}

func TestSelection_OneSidedInputOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		routing []string
		want    []string
		readErr error
	}{
		{name: "duplicates", routing: []string{".sourceignore", "input.yaml", "z.yaml"}, want: []string{"input.yaml", "z.yaml"}, readErr: io.EOF},
		{name: "first_error", routing: []string{".sourceignore", "z.yaml"}, want: []string{"input.yaml"}, readErr: fs.ErrPermission},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			before, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
				".sourceignore": {Data: []byte("*.yaml\n")}, "input.yaml": {}, "z.yaml": {},
			}})
			assert.NoError(t, err)
			after, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
				".sourceignore": {}, "input.yaml": {}, "z.yaml": {},
			}})
			assert.NoError(t, err)
			var reads []string
			input := &inputFailureFS{FS: after, fail: func(name string) error {
				reads = append(reads, name)
				return tc.readErr
			}}
			current, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: input})
			assert.NoError(t, err)
			changes, err := (selection{current: current}).filterChanges(t.Context(), before, tc.routing, map[string]object.TreeEntry{
				".sourceignore": {}, "input.yaml": {}, "z.yaml": {},
			})
			assert.Diff(t, reads, tc.want)
			if errors.Is(tc.readErr, io.EOF) {
				assert.NoError(t, err)
				assert.Diff(t, changes.Paths(), []string{".sourceignore", "input.yaml", "z.yaml"})
			} else if !errors.Is(err, tc.readErr) || changes != nil || !strings.Contains(err.Error(), "read input input.yaml:") {
				t.Fatalf("first input failure: changes=%v err=%v", changes, err)
			}
		})
	}
}

func TestSelection_NoOneSidedInputCancellation(t *testing.T) {
	t.Parallel()
	before, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
		"input.yaml": {Data: []byte("before")},
	}})
	assert.NoError(t, err)
	after, _, err := gittree.New(t.Context(), nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
		"input.yaml": {Data: []byte("after")},
	}})
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := &inputCancelFS{FS: after, cancel: cancel}
	current, _, err := gittree.New(ctx, nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: input})
	assert.NoError(t, err)
	changes, err := (selection{current: current}).filterChanges(ctx, before, []string{"input.yaml"}, map[string]object.TreeEntry{"input.yaml": {}})
	assert.Equal(t, ctx.Err(), context.Canceled)
	if changes != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled two-sided selection published changes: changes=%v err=%v", changes, err)
	}
}

type inputCancelFS struct {
	*gittree.FS
	cancel context.CancelFunc
}

func (f *inputCancelFS) Lstat(name string) (fs.FileInfo, error) {
	info, err := f.FS.Lstat(name)
	f.cancel()
	return info, err
}

func TestSelection_AcquireBaseNoOrigin(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	hash := initRepoWithFile(t, dir, "a.yaml", "base")
	location, err := gitrepo.Locate(t.Context(), dir, "")
	assert.NoError(t, err)
	input, err := admitLocation(t.Context(), location)
	assert.NoError(t, err)
	s := selection{base: &resolution{Hash: hash}, current: input.FS.(*gittree.FS)}
	err = s.acquireBase(t.Context())
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	if err == nil {
		t.Fatal("expected missing-objects error")
	}
	assert.Equal(t, strings.Contains(err.Error(), hash.String()), true)
}

func TestAutoResolve_StatusWarningSurvivesFailure(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	pin := initRepoWithFile(t, root, "file", "base")
	repo := openHelper(t, root)
	commit, err := repo.CommitObject(pin)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	entry, err := tree.FindEntry("file")
	assert.NoError(t, err)
	writeAndCommit(t, root, "file", "head")
	oid := entry.Hash.String()
	assert.NoError(t, os.Remove(filepath.Join(root, ".git", "objects", oid[:2], oid[2:])))
	testutil.WriteFile(t, root, ".git/index", "invalid index")
	owner := cachetest.New(t, "", sourcecache.New)
	result, err := AutoResolve(t.Context(), root, pin.String(), owner)
	if !errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), "missing objects") {
		t.Fatalf("original body failure lost: %v", err)
	}
	if result == nil || result.Source == nil || result.Source.FS != nil || result.BaseInput != nil || len(result.Source.Warnings) != 1 || result.Source.Warnings[0].Category != manifest.WarnBaselineStatusUnavailable || result.Source.Warnings[0].Resource != (manifest.NamedResource{}) {
		t.Fatalf("warning-only baseline = %+v", result)
	}
}

func TestCollectInputs_BaselineFailure(t *testing.T) {
	t.Parallel()
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprintf("canceled_%t", canceled), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			pin := initRepoWithFile(t, root, "unchanged", "base")
			testutil.WriteFile(t, root, "input.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional}\n")
			testutil.WriteFile(t, root, ".git/index", "invalid index")
			location, err := gitrepo.Locate(t.Context(), root, "")
			assert.NoError(t, err)
			input, err := admitLocation(t.Context(), location)
			assert.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failing := &inputFailureFS{FS: input.FS.(*gittree.FS), fail: func(string) error {
				if canceled {
					cancel()
				}
				return fs.ErrPermission
			}}
			current, _, err := gittree.New(ctx, nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: failing, Location: location})
			assert.NoError(t, err)
			repo := openHelper(t, root)
			base, changes, identity, warnings, err := selectChanges(ctx, selection{
				repo: repo, base: &resolution{Repo: repo, Hash: pin}, current: current, owner: cachetest.New(t, "", sourcecache.New),
			})
			want := fs.ErrPermission
			if canceled {
				want = context.Canceled
			}
			if !errors.Is(err, want) || changes != nil || identity != "" {
				t.Fatalf("input failure published a change set or lost its cause: changes=%v identity=%q err=%v", changes, identity, err)
			}
			if base == nil || len(warnings) != 1 || warnings[0].Category != manifest.WarnBaselineStatusUnavailable {
				t.Fatalf("input failure lost baseline warnings: base=%v warnings=%v", base, warnings)
			}
		})
	}
}

type inputFailureFS struct {
	*gittree.FS
	fail func(string) error
}

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
	if len(warnings) != 1 || warnings[0].Category != manifest.WarnSkippedSubmodule || warnings[0].Resource != manifest.BootstrapSourceID || len(result.Source.Warnings) != 0 {
		t.Fatalf("baseline source evidence = %+v, current=%+v", warnings, result.Source.Warnings)
	}
	raw := result.BaseInput.FS.(*gittree.FS).Warnings
	if len(raw) != 1 || raw[0].Resource != (manifest.NamedResource{}) {
		t.Fatalf("borrowed filesystem evidence mutated: %+v", raw)
	}
	assert.Diff(t, warnings[0].Detail, raw[0].Detail)
}
