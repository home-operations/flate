package gittree

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-billy/v5/util"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/store"
)

func admitLocal(t *testing.T, root string) (*store.SourceArtifact, error) {
	t.Helper()
	location, err := gitrepo.Locate(t.Context(), root, root)
	if err != nil {
		return nil, err
	}
	return AdmitDirectory(t.Context(), location)
}

func operation(t *testing.T, input fs.FS, mode ViewMode) (*FS, string, error) {
	t.Helper()
	return New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: input})
}

func craftedCommit(t *testing.T, repo *git.Repository, entry object.TreeEntry) plumbing.Hash {
	t.Helper()
	blob := repo.Storer.NewEncodedObject()
	blob.SetType(plumbing.BlobObject)
	w, err := blob.Writer()
	assert.NoError(t, err)
	{
		_, writeErr := w.Write([]byte("malicious"))
		assert.NoError(t, writeErr)
	}

	assert.NoError(t, w.Close())
	entry.Hash, err = repo.Storer.SetEncodedObject(blob)
	assert.NoError(t, err)
	if entry.Mode == filemode.Dir {
		child := repo.Storer.NewEncodedObject()
		assert.NoError(t, (&object.Tree{}).Encode(child))
		entry.Hash, err = repo.Storer.SetEncodedObject(child)
		assert.NoError(t, err)
	}
	tree := &object.Tree{Entries: []object.TreeEntry{entry}}
	encoded := repo.Storer.NewEncodedObject()
	assert.NoError(t, tree.Encode(encoded))
	treeHash, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	commit := &object.Commit{
		TreeHash: treeHash,
		Author:   object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)},
	}
	encoded = repo.Storer.NewEncodedObject()
	assert.NoError(t, commit.Encode(encoded))
	hash, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	return hash
}

type admissionErrorFS struct{ err error }

func (f admissionErrorFS) Open(name string) (fs.File, error) {
	return nil, &fs.PathError{Op: "open", Path: name, Err: f.err}
}

type policyReadFS struct {
	fs.FS

	reads atomic.Int64
}

func (f *policyReadFS) ReadFile(name string) ([]byte, error) {
	if filepath.Base(name) == ".sourceignore" {
		f.reads.Add(1)
	}
	return fs.ReadFile(f.FS, name)
}

func TestNew_EntryPolicy(t *testing.T) {
	t.Parallel()
	t.Run("git ignored membership", func(t *testing.T) { t.Parallel(); testNewEntryPolicyGitIgnoredMembership(t) })
	t.Run("git capture observations", func(t *testing.T) { t.Parallel(); testNewEntryPolicyGitCaptureObservations(t) })
	t.Run(
		"git membership replacements",
		func(t *testing.T) { t.Parallel(); testNewEntryPolicyGitMembershipReplacements(t) },
	)
	t.Run("git rule capture", func(t *testing.T) { t.Parallel(); testNewEntryPolicyGitRuleCapture(t) })
	t.Run("git and flux policy", func(t *testing.T) { t.Parallel(); testNewEntryPolicyGitAndFluxPolicy(t) })
	t.Run("git admission routes", func(t *testing.T) { t.Parallel(); testNewEntryPolicyGitAdmissionRoutes(t) })
	t.Run("ownership labels", func(t *testing.T) { t.Parallel(); testNewEntryPolicyOwnershipLabels(t) })

	t.Run("warning evidence reuse", func(t *testing.T) { t.Parallel(); testNewEntryPolicyWarningEvidenceReuse(t) })

	t.Run("local reads", func(t *testing.T) { t.Parallel(); testNewEntryPolicyLocalReads(t) })
	t.Run(
		"local operation observations",
		func(t *testing.T) { t.Parallel(); testNewEntryPolicyLocalOperationObservations(t) },
	)
	t.Run("caller_owned_listing", func(t *testing.T) { t.Parallel(); testNewEntryPolicyCallerOwnedListing(t) })
	t.Run("concurrent first listing", func(t *testing.T) { t.Parallel(); testNewEntryPolicyConcurrentFirstListing(t) })
	t.Run(
		"retained directory listing",
		func(t *testing.T) { t.Parallel(); testNewEntryPolicyRetainedDirectoryListing(t) },
	)
	t.Run("local metadata", func(t *testing.T) { t.Parallel(); testNewEntryPolicyLocalMetadata(t) })
	t.Run(
		"policy read once and extras",
		func(t *testing.T) { t.Parallel(); testNewEntryPolicyPolicyReadOnceAndExtras(t) },
	)
	t.Run(
		"policy evidence preserves bytes",
		func(t *testing.T) { t.Parallel(); testNewEntryPolicyPolicyEvidencePreservesBytes(t) },
	)
	t.Run(
		"concurrent first policy and cancellation",
		func(t *testing.T) { t.Parallel(); testNewEntryPolicyConcurrentFirstPolicyAndCancellation(t) },
	)
	t.Run("identity", func(t *testing.T) { t.Parallel(); testNewEntryPolicyIdentity(t) })
	for _, tc := range []struct {
		name    string
		mode    filemode.FileMode
		invalid bool
	}{
		{"a/b", filemode.Regular, true}, {"module", filemode.Submodule, false},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) { t.Parallel(); testNewEntryPolicyCase(t, &tc) },
		)
	}

	t.Run(
		"mutable membership and live worktree",
		func(t *testing.T) { t.Parallel(); testNewEntryPolicyMutableMembershipAndLiveWorktree(t) },
	)
	t.Run(
		"committed tree overrides current",
		func(t *testing.T) { t.Parallel(); testNewEntryPolicyCommittedTreeOverridesCurrent(t) },
	)
	t.Run("retained identity", func(t *testing.T) { t.Parallel(); testNewEntryPolicyRetainedIdentity(t) })
	t.Run("source reuse", func(t *testing.T) { t.Parallel(); testNewEntryPolicySourceReuse(t) })
	t.Run(
		"direct arbitrary and wrapper reuse",
		func(t *testing.T) { t.Parallel(); testNewEntryPolicyDirectArbitraryAndWrapperReuse(t) },
	)
	t.Run("direct pinned worktree", func(t *testing.T) { t.Parallel(); testNewEntryPolicyDirectPinnedWorktree(t) })
	t.Run(
		"refresh before pinned construction",
		func(t *testing.T) { t.Parallel(); testNewEntryPolicyRefreshBeforePinnedConstruction(t) },
	)
	t.Run("refresh snapshots", func(t *testing.T) { t.Parallel(); testNewEntryPolicyRefreshSnapshots(t) })
	t.Run("shaping", func(t *testing.T) { t.Parallel(); testNewEntryPolicyShaping(t) })
	t.Run(
		"sparse shaping and refresh",
		func(t *testing.T) { t.Parallel(); testNewEntryPolicySparseShapingAndRefresh(t) },
	)
	t.Run("rejected links", func(t *testing.T) { t.Parallel(); testNewEntryPolicyRejectedLinks(t) })
	t.Run("unknown mode visibility", func(t *testing.T) { t.Parallel(); testNewEntryPolicyUnknownModeVisibility(t) })
	t.Run("link errors", func(t *testing.T) { t.Parallel(); testNewEntryPolicyLinkErrors(t) })
	t.Run("missing ordinary body", func(t *testing.T) { t.Parallel(); testNewEntryPolicyMissingOrdinaryBody(t) })
	t.Run("error classification", func(t *testing.T) { t.Parallel(); testNewEntryPolicyErrorClassification(t) })
	t.Run(
		"local permission diagnostic",
		func(t *testing.T) { t.Parallel(); testNewEntryPolicyLocalPermissionDiagnostic(t) },
	)
}

func TestNew_WarningsSurviveFailure(t *testing.T) {
	t.Parallel()
	for _, failure := range []string{"traversal", "shape", "refresh", "identity"} {
		t.Run(failure, func(t *testing.T) {
			t.Parallel()
			testNewWarningsSurviveFailureCase(t, &failure)
		},
		)
	}
}

type contentReadFS struct {
	fs.FS

	reads      atomic.Int64
	fail       string
	err        error
	cancel     context.CancelFunc
	readCancel context.CancelFunc
	entered    chan struct{}
	release    chan struct{}
	gate       atomic.Bool
}

func (f *contentReadFS) Open(name string) (fs.File, error) {
	if name == f.fail {
		if f.err != nil {
			return nil, f.err
		}
		return nil, fs.ErrPermission
	}
	if name == "file" {
		f.reads.Add(1)
		if f.entered != nil && f.gate.CompareAndSwap(false, true) {
			close(f.entered)
			<-f.release
		}
		if f.readCancel != nil {
			f.readCancel()
		}
	}
	return f.FS.Open(name)
}

func waitForOnceWaiter(t *testing.T, id string) {
	t.Helper()
	for {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		stack := string(buf[:n])
		for record := range strings.SplitSeq(stack, "\n\n") {
			if strings.HasPrefix(record, "goroutine "+id+" ") && strings.Contains(record, "sync.(*Once).doSlow") {
				return
			}
		}
		runtime.Gosched()
	}
}

func (f *contentReadFS) ReadDir(name string) ([]fs.DirEntry, error) {
	if f.cancel != nil {
		f.cancel()
	}
	return fs.ReadDir(f.FS, name)
}

type fingerprintJoinerContext struct {
	context.Context //nolint:containedctx // The test stores the producer context to assert deadline and cancellation propagation.

	input      *FS
	recomputed atomic.Bool
}

func (c *fingerprintJoinerContext) Err() error {
	err := c.Context.Err()
	if err != nil && c.input.content.Load() != nil {
		c.recomputed.Store(true)
	}
	return err
}

func TestFS_ContentFingerprint(t *testing.T) {
	t.Parallel()
	t.Run("git ignored noise", func(t *testing.T) { t.Parallel(); testFSContentFingerprintGitIgnoredNoise(t) })
	for _, mode := range []string{"equal", "payload", "name", "ignored", "canceled", "error", "read_error", "walk_error", "scan_canceled", "scan_deadline"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testFSContentFingerprintCase(t, &mode)
		},
		)
	}
	t.Run("native_membership", func(t *testing.T) { t.Parallel(); testFSContentFingerprintNativeMembership(t) })

	t.Run("fresh_admission", func(t *testing.T) { t.Parallel(); testFSContentFingerprintFreshAdmission(t) })

	t.Run("excluded_policy_file", func(t *testing.T) { t.Parallel(); testFSContentFingerprintExcludedPolicyFile(t) })
	t.Run("content_canceled", func(t *testing.T) { t.Parallel(); testFSContentFingerprintContentCanceled(t) })
	for _, name := range []string{"live_joiner_retries", "canceled_joiner", "canceled_joiner_live_executor"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			testFSContentFingerprintCase2(t, &name)
		},
		)
	}
}

type contentReadBilly struct {
	billy.Filesystem

	reads atomic.Int64
	all   bool
}

//nolint:ireturn // The billy.Filesystem implementation must preserve its upstream billy.File return type.
func (f *contentReadBilly) Open(name string) (billy.File, error) {
	if f.all || strings.HasPrefix(filepath.ToSlash(name), "dir/") {
		f.reads.Add(1)
	}
	return f.Filesystem.Open(name)
}

func TestFS_GitContentEvidence(t *testing.T) {
	t.Parallel()
	t.Run("committed membership", func(t *testing.T) { t.Parallel(); testFSGitContentEvidenceCommittedMembership(t) })
	t.Run("memory worktree", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		worktree, err := memfs.New().Chroot(root)
		assert.NoError(t, err)
		assert.NoError(t, util.WriteFile(worktree, "object.yaml", []byte("memory content"), 0o600))
		input, _, err := New(t.Context(), memfs.New(), plumbing.ZeroHash, ViewOptions{
			Mode: Source, Worktree: worktree,
		})
		assert.NoError(t, err)
		data, err := input.ReadFile("object.yaml")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "memory content")
		_, err = ContentFingerprint(t.Context(), input)
		assert.NoError(t, err)
		assert.Equal(t, len(input.FileDigests()), 1)
		assert.NoError(t, util.WriteFile(worktree, ".cache/payload.yaml", []byte("excluded"), 0o600))
		selected := ExcludeDirectory(
			&store.SourceArtifact{FS: input, LocalPath: root}, filepath.Join(root, ".cache"),
		).FS.(*FS)
		observed, err := selected.FreshInputs(t.Context(), nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, len(observed.Files), 1)
		assert.Equal(t, observed.Files["object.yaml"] != "", true)
		assert.Equal(t, observed.Files[".cache/payload.yaml"], "")
	})
	for _, mode := range []string{"clean", "dirty", "staged", "untracked", "racy", "subtree", "immutable", "sdk", "unreadable_clean", "source_unreadable", "unlocated", "routed", "scoped_local", "scoped_immutable", "direct_immutable"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testFSGitContentEvidenceCase(t, &mode)
		},
		)
	}
}

func TestGitView_SparseBodyReads(t *testing.T) {
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
			testGitViewSparseBodyReadsCase(t, &size)
		},
		)
	}
}

func testNewEntryPolicyOwnershipLabels(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	content := struct{ Tree string }{"captured"}
	key := manifest.Fingerprint(content)
	cases := []struct {
		name      string
		opts      LabelOptions
		want, key string
	}{
		{
			"baseline",
			LabelOptions{Root: root, Mode: Direct, Key: "revision"},
			filepath.Join(root, ".flate-base", "revision"),
			"revision",
		},
		{
			"local source",
			LabelOptions{Root: root, Mode: Source, Key: "content"},
			filepath.Join(filepath.Dir(root), ".flate-sources", "content"),
			"content",
		},
		{
			"alias",
			LabelOptions{
				Root:    root,
				Mode:    Source,
				Content: content,
				Source:  manifest.NamedResource{Namespace: "flux-system", Name: "cluster"},
			},
			filepath.Join(filepath.Dir(root), ".flate-sources", key, "flux-system", "cluster"),
			key,
		},
	}
	for index := range cases {
		tc := &cases[index]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			label, key := Label(tc.opts)
			assert.Equal(t, label, tc.want)
			assert.Equal(t, key, tc.key)
		})
	}
}

func testNewEntryPolicyWarningEvidenceReuse(t *testing.T) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	commit := craftedCommit(t, repo, object.TreeEntry{Name: "module", Mode: filemode.Submodule})
	input, _, err := New(
		t.Context(),
		repo.Storer.(*filesystem.Storage).Filesystem(),
		commit,
		ViewOptions{Content: "pinned", Mode: Direct},
	)
	assert.NoError(t, err)
	if len(input.Warnings) != 1 {
		t.Fatalf("traversal evidence = %+v", input.Warnings)
	}
	for _, mode := range []ViewMode{Direct, Source} {
		view, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: mode, Current: input})
		assert.NoError(t, err)
		var want []manifest.Warning
		if mode == Direct {
			want = input.Warnings
		}
		assert.Diff(t, view.Warnings, want)
	}
}

func testNewEntryPolicyLocalReads(t *testing.T) {
	t.Helper()

	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	testutil.WriteFile(t, root, "regular.txt", "content")
	testutil.WriteFile(t, root, "nested/deeper/regular.txt", "nested content")
	testutil.WriteFile(t, parent, "outside", "outside")
	for name, target := range map[string]string{"file-link": "regular.txt", "dir-link": ".", "dangling": "missing", "escape": "../outside", "broken-escape": "../absent"} {
		assert.Equal(t, os.Symlink(target, filepath.Join(root, name)), nil)
	}
	socket, err := net.Listen("unix", filepath.Join(root, "socket"))
	assert.NoError(t, err)

	t.Cleanup(func() { assert.Equal(t, socket.Close(), nil) })
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)

	assert.Equal(t, artifact.ContentFingerprint != nil, true)
	input := artifact.FS
	view, pin, err := operation(t, input, Direct)
	assert.Equal(t, pin, "")
	assert.NoError(t, err)

	data, err := fs.ReadFile(view, "nested/deeper/regular.txt")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "nested content")
	info, err := fs.Stat(view, "nested/deeper/regular.txt")
	assert.NoError(t, err)
	assert.Equal(t, info.Mode().IsRegular(), true)
	assert.Equal(t, info.Size(), int64(len("nested content")))
	for _, reader := range []struct {
		name string
		fs   fs.FS
	}{{name: "descriptor", fs: input}, {name: "view", fs: view}} {
		t.Run(reader.name, func(t *testing.T) {
			t.Parallel()
			testNewEntryPolicyLocalReadsCase(t, testNewEntryPolicyLocalReadsCaseInputs{reader: &reader})
		},
		)
	}
}

func testNewEntryPolicyLocalOperationObservations(t *testing.T) {
	t.Helper()

	t.Run(
		"ignore_snapshot",
		func(t *testing.T) {
			t.Parallel()
			testNewEntryPolicyLocalOperationObservationsIgnoreSnapshot(

				t)
		},
	)
	t.Run("point_lookup_without_listing", func(t *testing.T) {
		t.Parallel()
		testNewEntryPolicyLocalOperationObservationsPointLookupWithoutListing(

			t)
	},
	)
	root := t.TempDir()
	testutil.WriteFile(t, root, ".sourceignore", "/*\n!/dir/arrived\n!/hidden/empty/\n")
	testutil.WriteFile(t, root, "dir/excluded", "excluded")
	assert.Equal(t, os.MkdirAll(filepath.Join(root, "hidden", "empty"), 0o755), nil)
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)

	input := artifact.FS
	observed, pin, err := operation(t, input, Direct)
	assert.Equal(t, pin, "")
	assert.NoError(t, err)

	{
		_, readFileErr := fs.ReadFile(observed, "dir/arrived")
		assert.FatalIf(t, !errors.Is(readFileErr, fs.ErrNotExist), "initial missing file = %v", readFileErr)
	}
	hidden, pin, err := operation(t, input, Source)
	assert.NoError(t, err)
	snapshot := hidden.PolicyEvidence()
	assert.Equal(t, pin, "")
	assert.Equal(t, snapshot == nil, false)
	assert.Equal(t, err, nil)
	{
		_, statErr2 := fs.Stat(hidden, "dir")
		assert.FatalIf(t, !errors.Is(statErr2, fs.ErrNotExist), "directory without survivors = %v", statErr2)
	}
	{
		_, statErr3 := fs.Stat(hidden, "hidden/empty")
		assert.FatalIf(
			t,
			!errors.Is(statErr3, fs.ErrNotExist),
			"re-included empty directory below hidden ancestor = %v",
			statErr3,
		)
	}
	{
		_, statErr4 := fs.Stat(observed, "hidden/empty")
		assert.FatalIf(t, statErr4 != nil, "unfiltered empty directory = %v", statErr4)
	}
	testutil.WriteFile(t, root, "dir/arrived", "arrived")
	fresh, pin, err := operation(t, input, Source)
	assert.Equal(t, pin, "")
	assert.Diff(t, fresh.PolicyEvidence(), snapshot)
	assert.Equal(t, err, nil)
	testutil.WriteFile(t, root, ".sourceignore", "/*\n")
	readmitted, err := admitLocal(t, root)
	assert.NoError(t, err)

	ignored, pin, err := operation(t, readmitted.FS, Source)
	assert.Equal(t, pin, "")
	assert.FatalIf(t, cmp.Equal(ignored.PolicyEvidence(), snapshot), "%s", "re-admission retained changed ignore bytes")

	assert.Equal(t, err, nil)
	plain, pin, err := operation(t, readmitted.FS, Direct)
	assert.Equal(t, pin, "")
	assert.Equal(t, err, nil)
	memory := fstest.MapFS{"kept": {Data: []byte("content")}}
	passthrough, pin, err := operation(t, memory, Direct)
	assert.Equal(t, pin, "")
	assert.Equal(t, err, nil)
	memory["later"] = &fstest.MapFile{Data: []byte("same FS")}
	for _, tc := range []struct {
		name, path, content string
		view                fs.FS
		want                error
	}{
		{name: "retained directory observation", path: "dir/arrived", view: observed, want: fs.ErrNotExist},
		{name: "fresh view", path: "dir/arrived", content: "arrived", view: fresh},
		{name: "refreshed ignore policy", path: "dir/arrived", view: ignored, want: fs.ErrNotExist},
		{name: "unfiltered plain read", path: "dir/arrived", content: "arrived", view: plain},
		{name: "caller filesystem", path: "later", content: "same FS", view: passthrough},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testNewEntryPolicyLocalOperationObservationsCase(

				t, testNewEntryPolicyLocalOperationObservationsCaseInputs{
					tc: &tc,
				})
		},
		)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			<-start
			view, pin, err := operation(t, input, Source)
			assert.Equal(t, pin, "")
			assert.Diff(t, view.PolicyEvidence(), snapshot)
			if err != nil {
				t.Error(err)
				return
			}
			for _, reader := range []fs.FS{input, view} {
				data, err := fs.ReadFile(reader, "dir/arrived")
				assert.ErrorIf(
					t,
					err != nil || string(data) != "arrived",
					"concurrent fresh read = %q, %v",
					data,
					err,
				)
			}
		})
	}
	close(start)
	wg.Wait()
	assert.Equal(t, os.RemoveAll(filepath.Join(root, "dir")), nil)
	assert.Equal(t, os.Symlink(".", filepath.Join(root, "dir")), nil)
	for _, name := range []string{"dir", "dir/arrived"} {
		{
			_, err := fs.Stat(input, name)
			assert.FatalIf(
				t,
				!errors.Is(err, fs.ErrPermission),
				"fresh lookup through directory link: %s: %v",
				name,
				err,
			)
		}
	}
}

func testNewEntryPolicyCallerOwnedListing(t *testing.T) {
	t.Helper()

	for _, name := range []string{"replacement", "reversed"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			for name, data := range map[string]string{"a": "a", "child": "content", "z": "longer content"} {
				testutil.WriteFile(t, root, "dir/"+name, data)
			}
			want, err := os.Lstat(filepath.Join(root, "dir", "a"))
			assert.NoError(t, err)
			artifact, err := admitLocal(t, root)
			assert.NoError(t, err)
			view, _, err := operation(t, artifact.FS, Direct)
			assert.NoError(t, err)
			entries, err := view.ReadDir("dir")
			assert.NoError(t, err)
			assert.FatalIf(t, len(entries) != 3, "listing length = %d, want 3", len(entries))
			if name == "replacement" {
				entries[0] = entries[2]
			} else {
				slices.Reverse(entries)
			}
			entries, err = view.ReadDir("dir")
			assert.NoError(t, err)
			var names []string
			if len(entries) != 0 {
				names = make([]string, 0, len(entries))
			}
			for _, entry := range entries {
				names = append(names, entry.Name())
			}
			assert.Diff(t, names, []string{"a", "child", "z"})
			info, err := fs.Stat(view, "dir/a")
			assert.NoError(t, err)
			assert.Equal(t, info.Name(), want.Name())
			assert.Equal(t, info.Mode(), want.Mode())
			assert.Equal(t, info.Size(), want.Size())
			assert.Equal(t, info.ModTime().Equal(want.ModTime()), true)
		})
	}
}
