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
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/memory"

	"github.com/home-operations/flate/internal/admissiontest"
	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/inputprojection"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
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
			testAutoResolveLazyBaseReadsCase(t, &tc)
		},
		)
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
				entry, localErr := tree.FindEntry("apps/file.yaml")
				assert.NoError(t, localErr)
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
				assert.FatalIf(t, res.BaseInput.Key == "", "%v", "baseline identity is empty")
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
			testAutoResolveIgnoreMembershipFlipsCase(
				t,
				&behavior,
			)
		},
		)
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
		app: {
			File: "flux/app.yaml",
			Refs: []manifest.NamedResource{{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "optional"}},
		},
	}}
	filter := admissiontest.Decision(
		t,
		admissiontest.Config{Changes: res.Changes, Index: index, Inventory: nil, Root: ""},
	)
	assert.FatalIf(t, filter.Reconcile(app), "%v", "two-sided input supplied an absent input identity")
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
			before, _, err := gittree.New(
				t.Context(),
				nil,
				plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
					".sourceignore": {Data: []byte("*.yaml\n")}, "input.yaml": {}, "z.yaml": {},
				}},
			)
			assert.NoError(t, err)
			after, _, err := gittree.New(
				t.Context(),
				nil,
				plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
					".sourceignore": {}, "input.yaml": {}, "z.yaml": {},
				}},
			)
			assert.NoError(t, err)
			var reads []string
			input := &inputFailureFS{FS: after, fail: func(name string) error {
				reads = append(reads, name)
				return tc.readErr
			}}
			current, _, err := gittree.New(
				t.Context(),
				nil,
				plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Direct, Current: input},
			)
			assert.NoError(t, err)
			changes, err := (selection{current: current}).filterChanges(
				t.Context(),
				before,
				tc.routing,
				map[string]object.TreeEntry{
					".sourceignore": {}, "input.yaml": {}, "z.yaml": {},
				},
			)
			assert.Diff(t, reads, tc.want)
			if errors.Is(tc.readErr, io.EOF) {
				assert.NoError(t, err)
				assert.Diff(t, changes.Paths(), []string{".sourceignore", "input.yaml", "z.yaml"})
			} else {
				assert.FatalIf(
					t,
					!errors.Is(err, tc.readErr) || changes != nil ||
						!strings.Contains(err.Error(), "read input input.yaml:"),
					"first input failure: changes=%v err=%v",
					changes,
					err,
				)
			}
		})
	}
}

func TestSelection_NoOneSidedInputCancellation(t *testing.T) {
	t.Parallel()
	before, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
			"input.yaml": {Data: []byte("before")},
		}},
	)
	assert.NoError(t, err)
	after, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: fstest.MapFS{
			"input.yaml": {Data: []byte("after")},
		}},
	)
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	input := &inputCancelFS{FS: after, cancel: cancel}
	current, _, err := gittree.New(
		ctx,
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Direct, Current: input},
	)
	assert.NoError(t, err)
	changes, err := (selection{current: current}).filterChanges(
		ctx,
		before,
		[]string{"input.yaml"},
		map[string]object.TreeEntry{"input.yaml": {}},
	)
	assert.Equal(t, ctx.Err(), context.Canceled)
	assert.FatalIf(
		t,
		changes != nil || !errors.Is(err, context.Canceled),
		"canceled two-sided selection published changes: changes=%v err=%v",
		changes,
		err,
	)
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
	input, err := admitLocation(t.Context(), location, manifest.WorkScope{})
	assert.NoError(t, err)
	s := selection{base: &resolution{Hash: hash}, current: input.FS.(*gittree.FS)}
	err = s.acquireBase(t.Context())
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	assert.FatalIf(t, err == nil, "%v", "expected missing-objects error")
	assert.Equal(t, strings.Contains(err.Error(), hash.String()), true)
	assert.Equal(t, strings.Contains(err.Error(), "input error"), false)
	assert.Equal(t, strings.Contains(err.Error(), "flux error"), false)
}

func TestSelection_SharedIndexInitialization(t *testing.T) {
	t.Parallel()
	cause := errors.New("shared index unavailable")
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "nil"},
		{name: "wrapped_object_not_found", err: fmt.Errorf("shared index: %w", plumbing.ErrObjectNotFound)},
		{name: "unexpected", err: cause},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			pin := initRepoWithFile(t, root, "value.txt", "before")
			writeAndCommit(t, root, "value.txt", "after")
			testutil.WriteFile(t, root, ".git/index", "invalid index")
			location, err := gitrepo.Locate(t.Context(), root, "")
			assert.NoError(t, err)
			input, err := admitLocation(t.Context(), location, manifest.WorkScope{})
			assert.NoError(t, err)
			shared := &sharedIndexStorage{Storer: location.Repository.Storer, err: tc.err}
			location.Repository.Storer = shared
			repo := openHelper(t, root)
			worktree, err := repo.Worktree()
			assert.NoError(t, err)
			_, statusErr := worktree.Status()
			assert.FatalIf(t, statusErr == nil, "%v", "malformed index did not fail Worktree.Status")
			base, changes, identity, warnings, err := selectChanges(t.Context(), selection{
				repo: repo, base: &resolution{Repo: repo, Hash: pin}, current: input.FS.(*gittree.FS),
			})
			assert.Diff(t, shared.probes, []plumbing.Hash{plumbing.ZeroHash})
			assert.Diff(t, warnings, []manifest.Warning{
				{
					Category: manifest.WarnBaselineStatusUnavailable,
					Message:  "Git status unavailable; comparing checkout conservatively",
					Count:    1,
					Detail:   []string{statusErr.Error()},
				},
			})
			if errors.Is(tc.err, cause) {
				assert.FatalIf(
					t,
					!errors.Is(err, cause) || base != nil || changes != nil || identity != "",
					"shared index failure published inputs or lost its cause: base=%v changes=%v identity=%q err=%v",
					base,
					changes,
					identity,
					err,
				)
				return
			}
			assert.NoError(t, err)
			assert.FatalIf(
				t,
				base == nil || changes == nil || identity == "",
				"shared index initialization did not publish inputs: base=%v changes=%v identity=%q",
				base,
				changes,
				identity,
			)
			assert.Diff(t, changes.Paths(), []string{"value.txt"})
			before, err := fs.ReadFile(base, "value.txt")
			assert.NoError(t, err)
			assert.Equal(t, string(before), "before")
			after, err := fs.ReadFile(input.FS, "value.txt")
			assert.NoError(t, err)
			assert.Equal(t, string(after), "after")
		})
	}
}

type sharedIndexStorage struct {
	storage.Storer

	err    error
	probes []plumbing.Hash
}

func (s *sharedIndexStorage) HasEncodedObject(hash plumbing.Hash) error {
	s.probes = append(s.probes, hash)
	return s.err
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
	assert.FatalIf(
		t,
		!errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), "missing objects"),
		"original body failure lost: %v",
		err,
	)
	assert.FatalIf(t, result == nil || result.Source == nil || result.Source.FS != nil || result.BaseInput != nil ||
		len(result.Source.Warnings) != 1 ||
		result.Source.Warnings[0].Category != manifest.WarnBaselineStatusUnavailable ||
		result.Source.Warnings[0].Resource != (manifest.NamedResource{}), "warning-only baseline = %+v", result)
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
			input, err := admitLocation(t.Context(), location, manifest.WorkScope{})
			assert.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			failing := &inputFailureFS{FS: input.FS.(*gittree.FS), fail: func(string) error {
				if canceled {
					cancel()
				}
				return fs.ErrPermission
			}}
			current, _, err := gittree.New(
				ctx,
				nil,
				plumbing.ZeroHash,
				gittree.ViewOptions{Mode: gittree.Direct, Current: failing, Location: location},
			)
			assert.NoError(t, err)
			repo := openHelper(t, root)
			base, changes, identity, warnings, err := selectChanges(ctx, selection{
				repo:    repo,
				base:    &resolution{Repo: repo, Hash: pin},
				current: current,
				owner:   cachetest.New(t, "", sourcecache.New),
			})
			want := fs.ErrPermission
			if canceled {
				want = context.Canceled
			}
			assert.FatalIf(
				t,
				!errors.Is(err, want) || changes != nil || identity != "",
				"input failure published a change set or lost its cause: changes=%v identity=%q err=%v",
				changes,
				identity,
				err,
			)
			assert.FatalIf(
				t,
				base == nil || len(warnings) != 1 || warnings[0].Category != manifest.WarnBaselineStatusUnavailable,
				"input failure lost baseline warnings: base=%v warnings=%v",
				base,
				warnings,
			)
		})
	}
}

type inputFailureFS struct {
	*gittree.FS

	fail func(string) error
}

func testAutoResolveLazyBaseReadsCase(t *testing.T, testCaseInput *struct {
	name string

	current string
	changed bool
},
) {
	t.Helper()

	root := t.TempDir()
	initRepoWithFile(t, root, "file", "base")
	writeAndCommit(t, root, "unchanged.yaml", "same")
	pin := writeAndCommit(t, root, ".sourceignore", "ignored.yaml\n")
	repo := openHelper(t, root)
	wt, err := repo.Worktree()
	assert.NoError(t, err)
	switch testCaseInput.name {
	case "checkout_deletion":
		assert.NoError(t, os.Remove(filepath.Join(root, "file")))
	case "untracked":
		testutil.WriteFile(t, root, "untracked.yaml", "new")
	case "malformed_index":
		testutil.WriteFile(t, root, "file", testCaseInput.current)
		testutil.WriteFile(t, root, ".git/index", "invalid index")
		repo = openHelper(t, root)
		localWt, innerLocalErr := repo.Worktree()
		assert.NoError(t, innerLocalErr)
		{
			_, localErr := localWt.Status()
			assert.FatalIf(t, localErr == nil, "%v", "malformed index did not fail Worktree.Status")
		}
		head, innerLocalErr := repo.Head()
		assert.NoError(t, innerLocalErr)
		assert.Equal(t, head.Hash(), pin)
	case "committed_deletion":
		_, localErr := wt.Remove("file")
		assert.NoError(t, localErr)
		_, localErr = wt.Commit(
			"delete",
			&git.CommitOptions{Author: &object.Signature{Name: "test", Email: "test@example.com"}},
		)
		assert.NoError(t, localErr)
	case "executable_directory":
		assert.NoError(t, os.Chmod(filepath.Join(root, "file"), 0o755))
		pin = writeAndCommit(t, root, "file", "base")
		assert.NoError(t, os.Remove(filepath.Join(root, "file")))
		assert.NoError(t, os.Mkdir(filepath.Join(root, "file"), 0o755))
		assert.NoError(t, os.Chmod(filepath.Join(root, "file"), 0o755))
	case "unchanged":
	default:
		writeAndCommit(t, root, "file", "head")
		testutil.WriteFile(t, root, "file", testCaseInput.current)
	}
	commit, err := repo.CommitObject(pin)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	var absent []plumbing.Hash
	for _, name := range []string{"unchanged.yaml", ".sourceignore"} {
		if testCaseInput.name == "malformed_index" {
			continue
		}
		entry, localErr := tree.FindEntry(name)
		assert.NoError(t, localErr)
		oid := entry.Hash.String()
		assert.NoError(t, os.Remove(filepath.Join(root, ".git", "objects", oid[:2], oid[2:])))
		absent = append(absent, entry.Hash)
	}
	if testCaseInput.name == "settled" || testCaseInput.name == "committed_deletion" {
		location, localErr := gitrepo.Locate(t.Context(), root, "")
		assert.NoError(t, localErr)
		input, localErr := admitLocation(t.Context(), location, manifest.WorkScope{})
		assert.NoError(t, localErr)
		entries, comparison, localErr := treeEntries(t.Context(), tree)
		assert.NoError(t, localErr)
		s := selection{
			repo:    repo,
			current: input.FS.(*gittree.FS),
			base:    &resolution{Repo: &git.Repository{Storer: memory.NewStorage()}},
		}
		candidates, _, localErr := s.candidates(t.Context(), entries, comparison)
		assert.NoError(t, localErr)
		dirty, present := candidates["file"]
		assert.Equal(t, present, true)
		assert.Equal(t, dirty, false)
		routing, localErr := s.compareCandidates(t.Context(), candidates, entries)
		assert.NoError(t, localErr)
		assert.Diff(t, routing, []string{"file"})
	}
	res, err := AutoResolve(t.Context(), root, pin.String(), cachetest.New(t, "", sourcecache.New))
	if testCaseInput.name == "malformed_index" {
		assert.FatalIf(t, res == nil || len(res.Source.Warnings) != 1 ||
			res.Source.Warnings[0].Category != manifest.WarnBaselineStatusUnavailable ||
			len(res.BaseInput.Warnings) != 0, "current-only status evidence = %+v", res)
	}
	assert.NoError(t, err)
	if res.BaseInput.Content != res.BaseInput.Key || res.Source.Content != "" || res.BaseInput.Key == "" ||
		res.Source.Key != "" ||
		res.BaseInput.Revision != pin.String() ||
		res.BaseInput.LocalRoot != root {
		t.Fatalf("baseline identity = %+v; current identity = %+v", res.BaseInput, res.Source)
	}
	var want []string
	if testCaseInput.changed {
		want = []string{"file"}
	}
	if testCaseInput.name == "untracked" {
		want = []string{"untracked.yaml"}
	}
	{
		got := res.Changes.Paths()
		assert.FatalIf(t, !slices.Equal(got, want), "selected=%v, want %v", got, want)
	}
	for _, file := range []struct{ name, body string }{
		{"file", "base"}, {"unchanged.yaml", "same"}, {".sourceignore", "ignored.yaml\n"},
	} {
		data, localErr := fs.ReadFile(res.BaseInput.FS, file.name)
		assert.NoError(t, localErr)
		assert.Equal(t, string(data), file.body)
	}
	initial, err := fs.Stat(res.BaseInput.FS, "unchanged.yaml")
	assert.NoError(t, err)
	assert.Equal(t, initial.Size(), int64(len("same")))
	for _, body := range []string{"fresh-one", "fresh-two"} {
		testutil.WriteFile(t, root, "unchanged.yaml", body)
		if testCaseInput.changed && testCaseInput.name != "executable_directory" {
			testutil.WriteFile(t, root, "file", "live-mutated")
		}
		view, pin, localErr := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Direct, Current: res.BaseInput.FS},
		)
		assert.NoError(t, localErr)
		assert.Equal(t, pin, res.BaseInput.Key)
		data, localErr := fs.ReadFile(view, "unchanged.yaml")
		assert.NoError(t, localErr)
		assert.Equal(t, string(data), body)
		info, localErr := fs.Stat(view, "unchanged.yaml")
		assert.NoError(t, localErr)
		assert.Equal(t, info.Size(), int64(len(body)))
		data, localErr = fs.ReadFile(view, "file")
		assert.NoError(t, localErr)
		assert.Equal(t, string(data), "base")
	}

	for _, hash := range absent {
		assert.Equal(t, errors.Is(repo.Storer.HasEncodedObject(hash), plumbing.ErrObjectNotFound), true)
	}
	testutil.WriteFile(t, root, ".sourceignore", "file\n")
	view, _, err := gittree.New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		gittree.ViewOptions{Mode: gittree.Source, Current: res.BaseInput.FS},
	)
	assert.NoError(t, err)
	_, err = fs.Stat(view, "file")
	assert.NoError(t, err)
}

func testAutoResolveIgnoreMembershipFlipsCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	root := t.TempDir()
	initRepoWithFile(t, root, "apps/a.yaml", "bystander")
	pin := writeAndCommit(
		t,
		root,
		"apps/b.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: optional, namespace: flux-system}\ndata: {VALUE: unchanged}\n",
	)
	if (*testCaseInput) == "reinclude" {
		pin = writeAndCommit(t, root, ".sourceignore", "apps/b.yaml\n")
		assert.NoError(t, os.Remove(filepath.Join(root, ".sourceignore")))
	} else {
		testutil.WriteFile(t, root, ".sourceignore", "apps/b.yaml\n")
	}
	res, err := AutoResolve(t.Context(), root, pin.String(), cachetest.New(t, "", sourcecache.New))
	assert.NoError(t, err)
	assert.Diff(t, res.Changes.Paths(), []string{".sourceignore", "apps/b.yaml"})
	testutil.WriteFile(t, root, ".sourceignore", "*.yaml\n")
	var indexes [2]*manifest.DiscoveryIndex
	for side := range indexes {
		observation := manifest.InputVisibility{
			Source:   manifest.BootstrapSourceID,
			Root:     "apps",
			Complete: true,
		}
		if (*testCaseInput) == "exclude" && side == 0 ||
			(*testCaseInput) == "reinclude" && side == 1 {
			observation.Inputs = map[manifest.NamedResource][]string{
				{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "optional"}: {"apps/b.yaml"},
			}
		}
		indexes[side] = &manifest.DiscoveryIndex{Visibility: []manifest.InputVisibility{observation}}
	}
	changes, err := res.Changes.WithReaders(
		t.Context(), [2]*manifest.DiscoveryIndex{
			indexes[0],
			indexes[1],
		},
		[2]map[manifest.NamedResource]inputprojection.Projection{}, manifest.WorkScope{},
	)
	assert.NoError(t, err)
	for side, input := range []*store.SourceArtifact{res.BaseInput, res.Source} {
		_, directIdentity, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Direct, Current: input.FS},
		)
		assert.NoError(t, err)
		assert.Equal(t, input.Content, directIdentity)
		assert.FatalIf(
			t,
			(input.Content != "") != (side == 0),
			"%v",
			"diff sides lost committed versus mutable authority",
		)
		view, _, err := gittree.New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			gittree.ViewOptions{Mode: gittree.Source, Current: input.FS, Sparse: []string{"./apps"}},
		)
		assert.NoError(t, err)
		_, err = fs.Stat(view, "apps/b.yaml")
		wantVisible := (*testCaseInput) == "exclude" && side == 0 ||
			(*testCaseInput) == "reinclude" && side == 1
		assert.Equal(t, err == nil, wantVisible)
		app := testutil.KustomizationID("flux-system", "app")
		index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			app: {
				File: "flux/app.yaml",
				Refs: []manifest.NamedResource{
					{Kind: manifest.KindConfigMap, Namespace: "flux-system", Name: "optional"},
				},
			},
		}}
		objects := store.New()
		objects.SetArtifact(manifest.BootstrapSourceID, input)
		filter := admissiontest.Decision(
			t,
			admissiontest.Config{Changes: changes, Index: index, Inventory: objects, Root: input.LocalPath},
		)
		assert.ErrorIf(
			t,
			!filter.Reconcile(app),
			"side %d ignore %s did not select optional input consumer",
			side, (*testCaseInput),
		)
	}
}
