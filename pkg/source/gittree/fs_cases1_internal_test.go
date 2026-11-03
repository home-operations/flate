package gittree

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
)

func testNewEntryPolicyConcurrentFirstListing(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "dir/a", "a")
	testutil.WriteFile(t, root, "dir/child", "content")
	testutil.WriteFile(t, root, "dir/nested/file", "nested")
	assert.NoError(t, os.Symlink("../../absent", filepath.Join(root, "dir/link")))
	want, err := os.Lstat(filepath.Join(root, "dir/child"))
	assert.NoError(t, err)
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	view, _, err := operation(t, artifact.FS, Direct)
	assert.NoError(t, err)
	start := make(chan struct{})
	var ready, wg sync.WaitGroup
	ready.Add(8)
	for range 8 {
		wg.Go(func() {
			testNewEntryPolicyConcurrentFirstListingCallback(
				testNewEntryPolicyConcurrentFirstListingCallbackInputs{
					t:     &t,
					want:  &want,
					view:  &view,
					start: &start,
					ready: &ready,
				},
			)
		},
		)
	}
	ready.Wait()
	close(start)
	wg.Wait()
}

func testNewEntryPolicyRetainedDirectoryListing(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "dir/kept", "kept")
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	observed, _, err := operation(t, artifact.FS, Direct)
	assert.NoError(t, err)
	_, err = fs.ReadDir(observed, "dir")
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "dir/arrived", "arrived")
	_, err = fs.Stat(observed, "dir/arrived")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	fresh, pin, err := operation(t, observed, Direct)
	assert.NoError(t, err)
	assert.Equal(t, pin, "")
	data, err := fs.ReadFile(fresh, "dir/arrived")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "arrived")
}

func testNewEntryPolicyLocalMetadata(t *testing.T) {
	t.Helper()

	for _, directory := range []bool{false, true} {
		t.Run(strconv.FormatBool(directory), func(t *testing.T) {
			t.Parallel()
			testNewEntryPolicyLocalMetadataCase(t, testNewEntryPolicyLocalMetadataCaseInputs{directory: &directory})
		},
		)
	}
}

func testNewEntryPolicyPolicyReadOnceAndExtras(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, ".sourceignore", "discard\n")
	testutil.WriteFile(t, root, "nested/.sourceignore", "hidden\n")
	for _, name := range []string{"discard", "kept", "image.png", "nested/hidden"} {
		testutil.WriteFile(t, root, name, name)
	}
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	input := artifact.FS.(*FS)
	counted := &policyReadFS{FS: input}
	policy, err := input.snapshot.load(counted)
	assert.NoError(t, err)
	reads := counted.reads.Load()
	assert.Equal(t, reads, int64(2))
	assert.Equal(t, policy.Match("discard", false), true)
	assert.Equal(t, policy.Match("nested/hidden", false), true)
	want := []struct{ Path, Data string }{{".sourceignore", "discard\n"}, {"nested/.sourceignore", "hidden\n"}}
	assert.Diff(t, policy.Evidence(), want)
	content := []struct {
		Path string
		Data []byte
	}{{".sourceignore", []byte("discard\n")}, {"nested/.sourceignore", []byte("hidden\n")}}
	testutil.WriteFile(t, root, ".sourceignore", "kept\n")
	shaped, pin, err := New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		ViewOptions{Mode: Source, Current: input, Ignore: "kept\n"},
	)
	assert.NoError(t, err)
	assert.Equal(t, pin, "")
	assert.Diff(t, shaped.PolicyEvidence(), any(content))
	_, err = fs.Stat(shaped, "image.png")
	assert.NoError(t, err)
	for _, name := range []string{"discard", "kept", "nested/hidden"} {
		_, err = fs.Stat(shaped, name)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	}
	testutil.WriteFile(t, root, ".sourceignore", "image.png\n")
	second, pin, err := New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		ViewOptions{Mode: Source, Current: input, Ignore: "image.png\n"},
	)
	assert.NoError(t, err)
	assert.Equal(t, pin, "")
	assert.Diff(t, second.PolicyEvidence(), any(content))
	assert.Diff(t, shaped.PolicyEvidence(), any(content))
	assert.Diff(t, policy.Evidence(), want)
	_, err = fs.Stat(second, "kept")
	assert.NoError(t, err)
	for _, name := range []string{"image.png", "discard", "nested/hidden"} {
		_, err = fs.Stat(second, name)
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	}
	assert.Equal(t, policy.Match("kept", false), false)
	assert.Equal(t, policy.Match("image.png", false), false)
	_, err = fs.Stat(shaped, "kept")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	_, err = fs.Stat(shaped, "image.png")
	assert.NoError(t, err)
	retained, pin, err := operation(t, shaped, Source)
	assert.NoError(t, err)
	assert.Equal(t, pin, "")
	assert.Diff(t, retained.PolicyEvidence(), any(content))
	_, err = fs.Stat(retained, "kept")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	_, err = input.snapshot.load(counted)
	assert.NoError(t, err)
	assert.Equal(t, counted.reads.Load(), reads)
}

func testNewEntryPolicyPolicyEvidencePreservesBytes(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	var keys []string
	for _, data := range []string{"#\xff\n", "#\xfe\n"} {
		testutil.WriteFile(t, root, ".sourceignore", data)
		artifact, err := admitLocal(t, root)
		assert.NoError(t, err)
		input, pin, err := operation(t, artifact.FS, Source)
		assert.NoError(t, err)
		assert.Equal(t, pin, "")
		assert.Diff(t, input.PolicyEvidence(), any([]struct {
			Path string
			Data []byte
		}{{".sourceignore", []byte(data)}}))
		keys = append(keys, manifest.Fingerprint(input.PolicyEvidence()))
	}
	assert.Equal(t, keys[0] != keys[1], true)
}

func testNewEntryPolicyConcurrentFirstPolicyAndCancellation(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, ".sourceignore", "discard\n")
	testutil.WriteFile(t, root, "discard", "discard")
	testutil.WriteFile(t, root, "kept", "kept")
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			<-start
			input, pin, operationErr2 := operation(t, artifact.FS, Source)
			if operationErr2 != nil {
				t.Error(operationErr2)
				return
			}
			assert.Equal(t, pin, "")
			assert.Diff(t, input.PolicyEvidence(), any([]struct {
				Path string
				Data []byte
			}{{".sourceignore", []byte("discard\n")}}))
			_, operationErr2 = fs.Stat(input, "discard")
			assert.Equal(t, errors.Is(operationErr2, fs.ErrNotExist), true)
			data, operationErr2 := fs.ReadFile(input, "kept")
			assert.NoError(t, operationErr2)
			assert.Equal(t, string(data), "kept")
		})
	}
	close(start)
	wg.Wait()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = AdmitDirectory(ctx, artifact.FS.(*FS).Location())
	assert.Equal(t, err, context.Canceled)
	for _, mode := range []ViewMode{Direct, Source} {
		_, _, err = New(ctx, nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: artifact.FS})
		assert.Equal(t, err, context.Canceled)
	}
}

func testNewEntryPolicyIdentity(t *testing.T) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
	backing := repo.Storer.(*filesystem.Storage).Filesystem()
	current := fstest.MapFS{"file": {Data: []byte("dirty")}}
	location := &gitrepo.Location{ContentRoot: "root", Repository: repo}
	opts := ViewOptions{
		Content:  struct{ Root string }{"root"},
		Location: location,
		Mode:     Direct,
		Current:  current,
		Changes:  []string{"file"},
	}
	input, identity, err := New(t.Context(), backing, commit, opts)
	assert.NoError(t, err)
	assert.FatalIf(t, identity == "", "%s", "committed baseline with current routing has no identity")

	fingerprint, err := ContentFingerprint(t.Context(), input)
	assert.NoError(t, err)
	assert.Equal(t, fingerprint, identity)
	assert.Equal(t, input.Location() == location, true)
	refreshed, _, err := operation(t, input, Direct)
	assert.NoError(t, err)
	assert.Equal(t, refreshed.Location() == location, true)
	data, err := fs.ReadFile(input, "file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "malicious")
	for _, tc := range []struct {
		name   string
		change func(*ViewOptions)
		equal  bool
	}{
		{"empty_sparse", func(o *ViewOptions) { o.Sparse = []string{} }, true},
		{"routing", func(o *ViewOptions) {
			o.Current = fstest.MapFS{"file": {Data: []byte("other")}}
			o.Changes = []string{"file", "absent"}
		}, true},
		{"coordinates", func(o *ViewOptions) { o.Content = struct{ Root string }{"other"} }, false},
		{"mode", func(o *ViewOptions) { o.Mode = Source }, false},
		{"sparse", func(o *ViewOptions) { o.Sparse = []string{"file"} }, false},
		{"ignore", func(o *ViewOptions) { o.Ignore = "absent\n" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := opts
			tc.change(&changed)
			_, got, newErr := New(t.Context(), backing, commit, changed)
			assert.NoError(t, newErr)
			if got == "" || (got == identity) != tc.equal {
				t.Fatalf("identity = %q, base = %q; equal=%t", got, identity, tc.equal)
			}
		})
	}
	other := craftedCommit(t, repo, object.TreeEntry{Name: "other", Mode: filemode.Regular})
	_, changed, err := New(t.Context(), backing, other, opts)
	assert.NoError(t, err)
	assert.FatalIf(t, changed == identity, "%s", "commit change reused an admission identity")

	for _, mode := range []ViewMode{Direct, Source} {
		pinned, pin, err := New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			ViewOptions{Mode: mode, Current: current, CurrentContent: "acquired"},
		)
		assert.NoError(t, err)
		assert.FatalIf(t, pin == "", "mode %d lost the wrapped acquisition identity", mode)
		_, retained, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: pinned})
		assert.NoError(t, err)
		assert.Equal(t, retained, pin)
		_, next, err := New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			ViewOptions{Mode: mode, Current: current, CurrentContent: "replacement"},
		)
		assert.NoError(t, err)
		assert.FatalIf(t, next == pin, "%s", "wrapped generation change reused its identity")
	}
	for _, mode := range []ViewMode{Direct, Source} {
		mutable, key, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: current})
		assert.NoError(t, err)
		assert.FatalIf(t, key != "", "mutable mode %d has identity %q", mode, key)
		_, key, err = New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: mutable})
		assert.NoError(t, err)
		assert.FatalIf(t, key != "", "refreshed mutable mode %d has identity %q", mode, key)
	}
}

func testNewEntryPolicyCase(t *testing.T, tc *struct {
	name    string
	mode    filemode.FileMode
	invalid bool
},
) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	commit := craftedCommit(t, repo, object.TreeEntry{Name: tc.name, Mode: tc.mode})
	input, identity, err := New(
		t.Context(),
		repo.Storer.(*filesystem.Storage).Filesystem(),
		commit,
		ViewOptions{},
	)
	if tc.invalid {
		assert.FatalIf(t, err == nil || identity != "", "%s", "malformed entry accepted or assigned an identity")

		return
	}
	assert.NoError(t, err)
	assert.FatalIf(t, identity == "", "%s", "committed view has no identity")

	entries, err := fs.ReadDir(input, ".")
	assert.FatalIf(t, err != nil || len(entries) != 0, "gitlink included: %v, %v", entries, err)
}

func testNewEntryPolicyMutableMembershipAndLiveWorktree(t *testing.T) {
	t.Helper()

	for _, live := range []bool{false, true} {
		root := t.TempDir()
		repo := testutil.GitInit(t, root)
		commit := craftedCommit(t, repo, object.TreeEntry{Name: "committed", Mode: filemode.Regular})
		testutil.WriteFile(t, root, "old/file", "old")
		testutil.WriteFile(t, root, ".sourceignore", "ignored/\n")
		testutil.WriteFile(t, root, "ignored/file", "hidden")
		local, err := admitLocal(t, root)
		assert.NoError(t, err)
		opts := ViewOptions{Mode: Source, Current: local.FS}
		if live {
			opts.Current = nil
			opts.Worktree = osfs.New(root, osfs.WithBoundOS())
		}
		backing := repo.Storer.(*filesystem.Storage).Filesystem()
		if !live {
			backing, commit = nil, plumbing.ZeroHash
		}
		view, pin, err := New(t.Context(), backing, commit, opts)
		assert.NoError(t, err)
		assert.Equal(t, pin, "")
		_, missingErr := fs.Stat(view, "new/deep/file")
		assert.Equal(t, errors.Is(missingErr, fs.ErrNotExist), true)
		_, err = fs.ReadDir(view, "old")
		assert.NoError(t, err)
		assert.NoError(t, os.RemoveAll(filepath.Join(root, "old")))
		testutil.WriteFile(t, root, "new/deep/file", "new")
		fresh, pin, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: view})
		assert.NoError(t, err)
		assert.Equal(t, pin, "")
		data, err := fs.ReadFile(fresh, "new/deep/file")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "new")
		for _, name := range []string{"old/file", "ignored/file"} {
			_, err := fs.Stat(fresh, name)
			assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
		}
	}
}

func testNewEntryPolicyCommittedTreeOverridesCurrent(t *testing.T) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	backing := repo.Storer.(*filesystem.Storage).Filesystem()
	currentCommit := craftedCommit(t, repo, object.TreeEntry{Name: "current", Mode: filemode.Regular})
	current, _, err := New(t.Context(), backing, currentCommit, ViewOptions{})
	assert.NoError(t, err)
	target := craftedCommit(t, repo, object.TreeEntry{Name: "target", Mode: filemode.Regular})
	view, _, err := New(
		t.Context(),
		backing,
		target,
		ViewOptions{Mode: Direct, Current: current, Changes: []string{"target"}},
	)
	assert.NoError(t, err)
	data, err := fs.ReadFile(view, "target")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "malicious")
	_, err = fs.Stat(view, "current")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
}

func testNewEntryPolicyRetainedIdentity(t *testing.T) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
	identity := "source-git-acquired@" + commit.String()
	input, pin, err := New(
		t.Context(),
		repo.Storer.(*filesystem.Storage).Filesystem(),
		commit,
		ViewOptions{Mode: Direct, Content: identity},
	)
	assert.NoError(t, err)
	assert.Equal(t, pin == "", false)
	originalPin := pin
	held, pin, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
	assert.NoError(t, err)
	assert.Equal(t, pin, originalPin)
	assert.Equal(t, held == input, true)
	_, pin, err = New(
		t.Context(),
		repo.Storer.(*filesystem.Storage).Filesystem(),
		commit,
		ViewOptions{Mode: Direct},
	)
	assert.NoError(t, err)
	assert.Equal(t, pin == "", false)
}

func testNewEntryPolicySourceReuse(t *testing.T) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
	input, pin, err := New(
		t.Context(),
		repo.Storer.(*filesystem.Storage).Filesystem(),
		commit,
		ViewOptions{Mode: Source},
	)
	assert.NoError(t, err)
	for _, tc := range []struct {
		name   string
		ignore string
		sparse []string
		reuse  bool
	}{
		{name: "empty", reuse: true},
		{name: "ignore", ignore: "file\n"},
		{name: "sparse", sparse: []string{"other"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, key, err := New(
				t.Context(),
				nil,
				plumbing.ZeroHash,
				ViewOptions{Mode: Source, Current: input, Ignore: tc.ignore, Sparse: tc.sparse},
			)
			assert.NoError(t, err)
			assert.Equal(t, got == input, tc.reuse)
			assert.Equal(t, key == pin, tc.reuse)
			assert.Equal(t, key != "", true)
			_, err = fs.Stat(got, "file")
			assert.Equal(t, err == nil, tc.reuse)
		})
	}
}

func testNewEntryPolicyDirectArbitraryAndWrapperReuse(t *testing.T) {
	t.Helper()

	current := fstest.MapFS{
		"file":               {Data: []byte("current")},
		".git":               {Data: []byte("gitfile")},
		"nested/.git/config": {Data: []byte("config")},
	}
	input, key, err := New(
		t.Context(),
		nil,
		plumbing.ZeroHash,
		ViewOptions{Mode: Direct, Current: current, Ignore: "file", Sparse: []string{"other"}},
	)
	assert.NoError(t, err)
	assert.Equal(t, key, "")
	current["new"] = &fstest.MapFile{Data: []byte("new")}
	got, key, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
	assert.NoError(t, err)
	assert.Equal(t, got == input, true)
	assert.Equal(t, key, "")
	for _, name := range []string{"file", "new", ".git", "nested/.git/config"} {
		data, err := fs.ReadFile(got, name)
		assert.NoError(t, err)
		assert.Equal(t, string(data), string(current[name].Data))
	}
}

func testNewEntryPolicyDirectPinnedWorktree(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "file", "local")
	input, key, err := New(
		t.Context(),
		nil,
		plumbing.NewHash(strings.Repeat("a", 40)),
		ViewOptions{Mode: Direct, Worktree: osfs.New(root, osfs.WithBoundOS())},
	)
	assert.NoError(t, err)
	assert.Equal(t, key, "")
	data, err := fs.ReadFile(input, "file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "local")
	testutil.WriteFile(t, root, "new/deep/file", "new")
	data, err = fs.ReadFile(input, "new/deep/file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "new")
	fresh, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
	assert.NoError(t, err)
	assert.Equal(t, fresh != input, true)
}

func testNewEntryPolicyRefreshBeforePinnedConstruction(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	commit := craftedCommit(t, repo, object.TreeEntry{Name: "new", Mode: filemode.Regular})
	testutil.WriteFile(t, root, "old", "old")
	local, err := admitLocal(t, root)
	assert.NoError(t, err)
	current, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: local.FS})
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "new", "current")
	input, key, err := New(
		t.Context(),
		repo.Storer.(*filesystem.Storage).Filesystem(),
		commit,
		ViewOptions{Mode: Direct, Current: current},
	)
	assert.NoError(t, err)
	assert.Equal(t, key != "", true)
	data, err := fs.ReadFile(input, "new")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "current")
}

func testNewEntryPolicyRefreshSnapshots(t *testing.T) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
	opts := ViewOptions{
		Mode:    Source,
		Current: fstest.MapFS{"file": {Data: []byte("dirty")}},
		Sparse:  []string{"file"},
		Changes: []string{"file"},
	}
	input, key, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, opts)
	assert.NoError(t, err)
	opts.Sparse[0], opts.Changes[0] = "other", "other"
	fresh, pin, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: input})
	assert.NoError(t, err)
	assert.Equal(t, pin, key)
	data, err := fs.ReadFile(fresh, "file")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "malicious")
}

type testNewEntryPolicyConcurrentFirstListingCallbackInputs struct {
	t     **testing.T
	want  *os.FileInfo
	view  **FS
	start *chan struct{}
	ready *sync.WaitGroup
}

func testNewEntryPolicyConcurrentFirstListingCallback(
	testCaseInput testNewEntryPolicyConcurrentFirstListingCallbackInputs,
) {
	testCaseInput.ready.
		Done()
	<-(*testCaseInput.start)
	entries, err := (*testCaseInput.view).ReadDir("dir")
	if err != nil {
		(*testCaseInput.t).
			Error(err)
		return
	}
	if len(entries) != 3 {
		(*testCaseInput.t).
			Errorf("listing length = %d, want 3", len(entries))
		return
	}
	entries[1] = entries[0]
	entries, err = (*testCaseInput.view).ReadDir("dir")
	if err != nil {
		(*testCaseInput.t).
			Error(err)
		return
	}
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	assert.Diff((*testCaseInput.t), names, []string{"a", "child", "nested"})
	info, err := fs.Stat((*testCaseInput.view), "dir/child")
	if err != nil {
		(*testCaseInput.t).
			Error(err)
		return
	}
	assert.Equal((*testCaseInput.t), info.Name(), (*testCaseInput.want).Name())
	assert.Equal((*testCaseInput.t), info.Mode(), (*testCaseInput.want).Mode())
	assert.Equal((*testCaseInput.t), info.Size(), (*testCaseInput.want).Size())
	assert.Equal((*testCaseInput.t), info.ModTime().Equal((*testCaseInput.want).ModTime()), true)
	_, err = fs.ReadFile((*testCaseInput.view), "dir/link/child")
	assert.Equal((*testCaseInput.t), errors.Is(err, fs.ErrPermission), true)
}
