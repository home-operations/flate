package gittree

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/tree"
)

func testNewEntryPolicyShaping(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	for _, name := range []string{"target", ".rootfile", ".hidden/file", "node_modules/file", "dir/file", "templates/file", "crds/file"} {
		testutil.WriteFile(t, root, name, name)
	}
	for name, target := range map[string]string{
		"alias": ".hidden/link", ".hidden/link": "../target", "escape": "../outside", "missing": "absent", "directory": "dir",
	} {
		assert.NoError(t, os.Symlink(target, filepath.Join(root, name)))
	}
	commit := testutil.GitCommitAll(
		t,
		repo,
		"fixture",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
	)
	for _, tc := range []struct {
		name  string
		opts  ViewOptions
		files []string
	}{
		{"source", ViewOptions{Mode: Source, Sparse: []string{"target", ".root"}, Ignore: "target/\n"}, []string{".rootfile", "target"}},
		{"source directories", ViewOptions{Mode: Source}, []string{".hidden/file", ".rootfile", "crds/file", "dir/file", "node_modules/file", "target", "templates/file"}},
		{"direct", ViewOptions{Mode: Direct}, []string{".hidden/file", ".rootfile", "crds/file", "dir/file", "node_modules/file", "target", "templates/file"}},
		{"remote", ViewOptions{Mode: RemoteBase}, []string{".rootfile", "alias", "crds/file", "dir/file", "target", "templates/file"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testNewEntryPolicyShapingCase(t, testNewEntryPolicyShapingCaseInputs{repo: &repo, commit: &commit, tc: &tc})
		},
		)
	}
}

func testNewEntryPolicySparseShapingAndRefresh(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	files := map[string]string{
		".dot": "dot", ".sourceignore": "apps/drop.yaml\napps/keep.yaml\n",
		"apps/a.yaml": "a", "apps/b.yaml": "b", "apps-old/b.yaml": "old", "apps-extra/c.yaml": "extra",
		"apps/keep.yaml": "keep", "apps/drop.yaml": "drop", "infra/c/manifest.yaml": "infra",
		"target": "target", "target-old": "old-target",
	}
	for name, data := range files {
		testutil.WriteFile(t, root, name, data)
	}
	all := []string{
		".dot",
		".sourceignore",
		"apps-extra/c.yaml",
		"apps-old/b.yaml",
		"apps/a.yaml",
		"apps/b.yaml",
		"infra/c/manifest.yaml",
		"target",
		"target-old",
	}
	apps := []string{"apps-extra/c.yaml", "apps-old/b.yaml", "apps/a.yaml", "apps/b.yaml"}
	for _, tc := range []struct {
		name   string
		sparse []string
		ignore string
		want   []string
	}{
		{name: "nil", want: all},
		{name: "empty", sparse: []string{}, want: all},
		{name: "prefix", sparse: []string{"apps"}, want: apps},
		{name: "normalized prefix", sparse: []string{"./apps"}, want: apps},
		{name: "dot", sparse: []string{"."}, want: []string{".dot", ".sourceignore"}},
		{name: "missing", sparse: []string{"absent"}},
		{name: "file prefix", sparse: []string{"target"}, want: []string{"target", "target-old"}},
		{name: "normalized file prefix", sparse: []string{"./target"}, want: []string{"target", "target-old"}},
		{name: "empty normalized prefix", sparse: []string{"./"}, want: all},
		{name: "one prefix only", sparse: []string{"././apps"}},
		{name: "mixed", sparse: []string{"missing", "./apps", "./infra"}, want: []string{"apps-extra/c.yaml", "apps-old/b.yaml", "apps/a.yaml", "apps/b.yaml", "infra/c/manifest.yaml"}},
		{name: "ignore prefix", sparse: []string{"apps"}, ignore: "!apps/keep.yaml\n!infra/c/manifest.yaml\n", want: []string{"apps-extra/c.yaml", "apps-old/b.yaml", "apps/a.yaml", "apps/b.yaml", "apps/keep.yaml"}},
		{name: "ignore normalized prefix", sparse: []string{"./apps"}, ignore: "!apps/keep.yaml\n!infra/c/manifest.yaml\n", want: []string{"apps-extra/c.yaml", "apps-old/b.yaml", "apps/a.yaml", "apps/b.yaml", "apps/keep.yaml"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testNewEntryPolicySparseShapingAndRefreshCase(
				t,
				testNewEntryPolicySparseShapingAndRefreshCaseInputs{root: &root, files: &files, tc: &tc},
			)
		},
		)
	}
}

func testNewEntryPolicyRejectedLinks(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "regular", "content")
	testutil.WriteFile(t, root, "ignored", "ignored")
	testutil.WriteFile(t, root, ".sourceignore", "ignored\n")
	assert.NoError(t, os.Symlink("../absent", filepath.Join(root, "link")))
	commit := testutil.GitCommitAll(
		t,
		repo,
		"fixture",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}},
	)
	backing := repo.Storer.(*filesystem.Storage).Filesystem()
	local, err := admitLocal(t, root)
	assert.NoError(t, err)
	for _, mode := range []ViewMode{Direct, Source} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			t.Parallel()
			testNewEntryPolicyRejectedLinksCase(
				t,
				testNewEntryPolicyRejectedLinksCaseInputs{
					root:    &root,
					commit:  &commit,
					backing: &backing,
					local:   &local,
					mode:    &mode,
				},
			)
		},
		)
	}
}

func testNewEntryPolicyUnknownModeVisibility(t *testing.T) {
	t.Helper()

	current := fstest.MapFS{
		".rootfile": {Data: []byte("root")}, "target": {Data: []byte("target")},
		".hidden/file": {Data: []byte("hidden")}, "node_modules/file": {Data: []byte("modules")},
	}
	input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: ViewMode(255), Current: current})
	assert.NoError(t, err)
	entries, err := fs.ReadDir(input, ".")
	assert.NoError(t, err)
	var names []string
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	assert.Diff(t, names, []string{".rootfile", "target"})
}

func testNewEntryPolicyLinkErrors(t *testing.T) {
	t.Helper()

	for _, tc := range []struct {
		name    string
		missing bool
		want    error
	}{
		{"loop", false, tree.ErrLoop}, {"read", true, plumbing.ErrObjectNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			repo := testutil.GitInit(t, t.TempDir())
			commit := craftedCommit(t, repo, object.TreeEntry{Name: "malicious", Mode: filemode.Symlink})
			backing := repo.Storer.(*filesystem.Storage).Filesystem()
			if tc.missing {
				c, err := repo.CommitObject(commit)
				assert.NoError(t, err)
				tr, err := c.Tree()
				assert.NoError(t, err)
				hash := tr.Entries[0].Hash.String()
				assert.NoError(t, backing.Remove("objects/"+hash[:2]+"/"+hash[2:]))
			}
			_, _, err := New(t.Context(), backing, commit, ViewOptions{Mode: RemoteBase})
			assert.Equal(t, errors.Is(err, tc.want), true)
			for _, mode := range []ViewMode{Source, Direct} {
				input, _, err := New(t.Context(), backing, commit, ViewOptions{Mode: mode})
				assert.NoError(t, err)
				entries, err := fs.ReadDir(input, ".")
				assert.NoError(t, err)
				assert.Equal(t, len(entries), 0)
			}
		})
	}
}

func testNewEntryPolicyMissingOrdinaryBody(t *testing.T) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	commit := craftedCommit(t, repo, object.TreeEntry{Name: "file", Mode: filemode.Regular})
	c, err := repo.CommitObject(commit)
	assert.NoError(t, err)
	tr, err := c.Tree()
	assert.NoError(t, err)
	hash := tr.Entries[0].Hash.String()
	backing := repo.Storer.(*filesystem.Storage).Filesystem()
	assert.NoError(t, backing.Remove("objects/"+hash[:2]+"/"+hash[2:]))
	for _, mode := range []ViewMode{Source, Direct, RemoteBase, ViewMode(255)} {
		input, _, err := New(t.Context(), backing, commit, ViewOptions{Mode: mode})
		assert.NoError(t, err)
		entries, err := fs.ReadDir(input, ".")
		assert.NoError(t, err)
		assert.Equal(t, len(entries), 1)
		assert.Equal(t, entries[0].Name(), "file")
		_, err = fs.ReadFile(input, "file")
		assert.Equal(t, errors.Is(err, plumbing.ErrObjectNotFound), true)
	}
}

func testNewEntryPolicyErrorClassification(t *testing.T) {
	t.Helper()

	for _, want := range []error{fs.ErrPermission, fs.ErrInvalid} {
		_, _, err := New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			ViewOptions{Mode: Source, Current: admissionErrorFS{err: want}},
		)
		assert.Equal(t, errors.Is(err, want), true)
		assert.Equal(t, strings.HasPrefix(err.Error(), "sourceignore load: "), errors.Is(want, fs.ErrPermission))
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := New(
		ctx,
		nil,
		plumbing.ZeroHash,
		ViewOptions{Mode: Source, Current: admissionErrorFS{err: fs.ErrPermission}},
	)
	assert.Equal(t, err, context.Canceled)
}

func testNewEntryPolicyLocalPermissionDiagnostic(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, ".sourceignore", "ignored/\n")
	testutil.WriteFile(t, root, "denied/file", "file")
	ignore, denied := filepath.Join(root, ".sourceignore"), filepath.Join(root, "denied")
	assert.NoError(t, os.Chmod(ignore, 0))
	assert.NoError(t, os.Chmod(denied, 0))
	t.Cleanup(func() {
		assert.NoError(t, os.Chmod(ignore, 0o644))
		assert.NoError(t, os.Chmod(denied, 0o755))
	})
	if _, err := os.ReadFile(ignore); !errors.Is(err, fs.ErrPermission) {
		t.Skipf("filesystem cannot enforce ignore-file permission fixture: %v", err)
	}
	if _, err := os.ReadDir(denied); !errors.Is(err, fs.ErrPermission) {
		t.Skipf("filesystem cannot enforce directory permission fixture: %v", err)
	}
	local, err := admitLocal(t, root)
	assert.NoError(t, err)
	_, _, err = New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: local.FS})
	assert.Equal(t, errors.Is(err, fs.ErrPermission), true)
	pathErr, ok := errors.AsType[*fs.PathError](err)
	assert.FatalIf(t, !ok || pathErr.Path != ignore, "permission diagnostic = %v; want %s", err, ignore)
}

func testNewWarningsSurviveFailureCase(t *testing.T, failure *string,
) {
	t.Helper()

	repo := testutil.GitInit(t, t.TempDir())
	entries := []object.TreeEntry{
		{Name: "module", Mode: filemode.Submodule, Hash: plumbing.NewHash(strings.Repeat("a", 40))},
	}
	if (*failure) == "traversal" {
		entries = append(
			entries,
			object.TreeEntry{
				Name: "zmissing",
				Mode: filemode.Dir,
				Hash: plumbing.NewHash(strings.Repeat("b", 40)),
			},
		)
	} else {
		encoded := repo.Storer.NewEncodedObject()
		assert.NoError(t, (&object.Tree{}).Encode(encoded))
		hash, err := repo.Storer.SetEncodedObject(encoded)
		assert.NoError(t, err)
		name := ".sourceignore"
		if (*failure) == "refresh" {
			name = "zsubtree"
		}
		entries = append(entries, object.TreeEntry{Name: name, Mode: filemode.Dir, Hash: hash})
	}
	encoded := repo.Storer.NewEncodedObject()
	sort.Sort(object.TreeEntrySorter(entries))
	assert.NoError(t, (&object.Tree{Entries: entries}).Encode(encoded))
	treeHash, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	encoded = repo.Storer.NewEncodedObject()
	assert.NoError(t, (&object.Commit{TreeHash: treeHash}).Encode(encoded))
	commit, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	opts := ViewOptions{}
	if (*failure) == "refresh" {
		opts = ViewOptions{Mode: Direct, Content: "captured", Current: fstest.MapFS{}}
	}
	if (*failure) == "identity" {
		opts.Content = make(chan int)
	}
	input, identity, err := New(t.Context(), repo.Storer.(*filesystem.Storage).Filesystem(), commit, opts)
	if (*failure) == "refresh" {
		assert.NoError(t, err)
		for _, entry := range entries {
			if entry.Name == "zsubtree" {
				oid := entry.Hash.String()
				assert.NoError(
					t,
					os.Remove(
						filepath.Join(
							repo.Storer.(*filesystem.Storage).Filesystem().Root(),
							"objects",
							oid[:2],
							oid[2:],
						),
					),
				)
			}
		}
		input, identity, err = New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			ViewOptions{Mode: Direct, Current: input},
		)
	}
	assert.FatalIf(
		t,
		err == nil || identity != "" || input == nil || input.view != nil || len(input.Warnings) != 1 ||
			input.Warnings[0].Category != manifest.WarnSkippedSubmodule ||
			input.Warnings[0].Detail[0] != "module",
		"failed admission = %+v, %q, %v",
		input,
		identity,
		err,
	)
	assert.FatalIf(t, (*failure) == "identity" &&
		(!errors.Is(err, manifest.ErrInput) || !errors.Is(err, manifest.ErrFlux) || err.Error() != "git view content cannot be fingerprinted"), "identity diagnostic lost its cause or classification: %v", err)
	assert.FatalIf(
		t, (*failure) ==
			"traversal" && !errors.Is(err, plumbing.ErrObjectNotFound), "original error lost: %v",
		err,
	)
}

func testFSContentFingerprintCase(t *testing.T, mode *string,
) {
	t.Helper()

	var keys [2]string
	for i := range keys {
		files := fstest.MapFS{
			"file":          {Data: []byte("selected")},
			".sourceignore": {Data: []byte("ignored\n")},
			"ignored":       {Data: []byte("ignored")},
		}
		if i == 1 {
			switch *mode {
			case "payload":
				files["file"].Data = []byte("different")
			case "name":
				files["other"] = files["file"]
				delete(files, "file")
			case "ignored":
				files["ignored"].Data = []byte("different")
			}
		}
		reader := &contentReadFS{FS: files}
		input, key, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: reader})
		assert.NoError(t, err)
		assert.Equal(t, key, "")
		assert.Equal(t, reader.reads.Load(), int64(0))
		assert.Equal(t, input.FileDigests() == nil, true)
		ctx, cancel := context.WithCancel(t.Context())
		if (*mode) == "read_error" || (*mode) == "walk_error" ||
			(*mode) == "scan_canceled" ||
			(*mode) == "scan_deadline" {
			_, sourcePolicyErr := input.SourcePolicy(t.Context())
			assert.NoError(t, sourcePolicyErr)
			want := fs.ErrPermission
			reader.fail = "file"
			if (*mode) == "walk_error" {
				reader.fail = "."
			}
			if (*mode) == "scan_canceled" {
				reader.fail, reader.cancel, want = "", cancel, context.Canceled
			}
			if (*mode) == "scan_deadline" {
				reader.fail, reader.err, want = ".", context.DeadlineExceeded, context.DeadlineExceeded
			}
			_, sourcePolicyErr = ContentFingerprint(ctx, input)
			cancel()
			assert.Equal(t, input.FileDigests() == nil, true)
			assert.FatalIf(
				t,
				!errors.Is(sourcePolicyErr, want),
				"fingerprint error = %v, want %v",
				sourcePolicyErr,
				want,
			)
			reader.fail, reader.cancel, reader.err = "", nil, nil
			if (*mode) == "read_error" || (*mode) == "walk_error" {
				_, sourcePolicyErr = ContentFingerprint(t.Context(), input)
				assert.Equal(t, errors.Is(sourcePolicyErr, want), true)
				assert.Equal(t, input.FileDigests() == nil, true)
				continue
			}
		}
		if (*mode) == "canceled" {
			cancel()
			_, contentFingerprintErr := ContentFingerprint(ctx, input)
			assert.FatalIf(
				t,
				!errors.Is(contentFingerprintErr, context.Canceled),
				"fingerprint error = %v",
				contentFingerprintErr,
			)
		}
		cancel()
		if (*mode) == "error" {
			input.view = &directView{FS: admissionErrorFS{err: fs.ErrPermission}}
			_, contentFingerprintErr2 := ContentFingerprint(t.Context(), input)
			assert.Equal(t, input.FileDigests() == nil, true)
			assert.FatalIf(
				t,
				!errors.Is(contentFingerprintErr2, fs.ErrPermission),
				"fingerprint error = %v",
				contentFingerprintErr2,
			)
			continue
		}
		var wg sync.WaitGroup
		for range 2 {
			wg.Go(func() {
				fingerprint, contentFingerprintErr3 := ContentFingerprint(t.Context(), input)
				assert.NoError(t, contentFingerprintErr3)
				if fingerprint == "" {
					t.Error("mutable fingerprint is unknown")
				}
			})
		}
		wg.Wait()
		reads := reader.reads.Load()
		if (*mode) != "name" || i == 0 {
			assert.Equal(t, reads, int64(1))
		}
		for name, file := range files {
			if name != "ignored" {
				assert.Equal(
					t,
					input.FileDigests()[name],
					manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, file.Data)),
				)
			}
		}
		_, ignored := input.FileDigests()["ignored"]
		assert.Equal(t, ignored, false)
		keys[i], err = ContentFingerprint(t.Context(), input)
		assert.NoError(t, err)
		assert.Equal(t, reader.reads.Load(), reads)
		if (*mode) == "canceled" {
			_, err := ContentFingerprint(ctx, input)
			assert.FatalIf(t, !errors.Is(err, context.Canceled), "cached fingerprint error = %v", err)
		}
	}
	testFSContentFingerprintCaseCompletion(t, &testFSContentFingerprintCaseCompletionInputs{mode: &mode, keys: &keys})
}

func testFSContentFingerprintCaseCompletion(t *testing.T, captures *testFSContentFingerprintCaseCompletionInputs) {
	t.Helper()
	if (*(*captures.mode)) != "error" && (*(*captures.mode)) != "read_error" &&
		(*(*captures.mode)) != "walk_error" {
		assert.Equal(
			t,
			(*captures.keys)[0] == (*captures.keys)[1],
			(*(*captures.mode)) != "payload" && (*(*captures.mode)) != "name",
		)
	}
}

func testFSContentFingerprintNativeMembership(t *testing.T) {
	t.Helper()

	files := fstest.MapFS{
		".sourceignore": {Data: []byte("ignored\n")},
		"file":          {Data: []byte("selected")},
		"nested/other":  {Data: []byte("included")},
		"ignored":       {Data: []byte("ignored")},
	}
	reader := &contentReadFS{FS: files}
	input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: reader})
	assert.NoError(t, err)
	reader.fail = "."
	key, err := ContentFingerprint(t.Context(), input)
	assert.NoError(t, err)
	assert.Equal(t, key != "", true)
	assert.Equal(t, len(input.FileDigests()), 3)
	for _, name := range []string{".sourceignore", "file", "nested/other"} {
		assert.Equal(
			t,
			input.FileDigests()[name],
			manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, files[name].Data)),
		)
	}
}

func testFSContentFingerprintFreshAdmission(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "file", "before")
	first, err := admitLocal(t, root)
	assert.NoError(t, err)
	before, err := first.ContentFingerprint(t.Context(), first.FS)
	assert.NoError(t, err)
	testutil.WriteFile(t, root, "file", "after")
	second, err := admitLocal(t, root)
	assert.NoError(t, err)
	operation := first.FS.(*FS).OperationView()
	assert.Equal(t, operation.FileDigests() == nil, true)
	operationKey, err := ContentFingerprint(t.Context(), operation)
	assert.NoError(t, err)
	assert.Equal(t, operationKey != before, true)

	assert.Equal(t, second.FS.(*FS).FileDigests() == nil, true)
	after, err := second.ContentFingerprint(t.Context(), second.FS)
	assert.NoError(t, err)
	assert.Equal(t, first.Key, "")
	assert.Equal(t, second.Key, "")
	assert.Equal(t, before != after, true)
	assert.Equal(
		t,
		second.FS.(*FS).FileDigests()["file"],
		manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte("after"))),
	)
	retained, err := first.ContentFingerprint(t.Context(), first.FS)
	assert.NoError(t, err)
	assert.Equal(t, retained, before)
}

func testFSContentFingerprintExcludedPolicyFile(t *testing.T) {
	t.Helper()

	input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: fstest.MapFS{
		".sourceignore":     {Data: []byte("*.png\nsub/.sourceignore\n")},
		"sub/.sourceignore": {Data: []byte("!*.png\n")},
		"sub/keep.png":      {Data: []byte("selected")},
		"file":              {Data: []byte("retained")},
	}})
	assert.NoError(t, err)
	_, err = fs.Stat(input, "sub/keep.png")
	assert.NoError(t, err)
	_, err = ContentFingerprint(t.Context(), input)
	assert.NoError(t, err)
	_, included := input.FileDigests()["sub/keep.png"]
	assert.Equal(t, included, false)
	assert.Equal(t, input.FileDigests()["file"] != "", true)
}

func testFSContentFingerprintContentCanceled(t *testing.T) {
	t.Helper()

	reader := &contentReadFS{FS: fstest.MapFS{"file": {Data: []byte("first")}, "later": {Data: []byte("later")}}}
	input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: reader})
	assert.NoError(t, err)
	_, err = input.SourcePolicy(t.Context())
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reader.readCancel = cancel
	_, err = ContentFingerprint(ctx, input)
	assert.Equal(t, errors.Is(err, context.Canceled), true)
	assert.Equal(t, input.FileDigests() == nil, true)
	reader.readCancel = nil
	failedReads := reader.reads.Load()
	var keys [2]string
	var wg sync.WaitGroup
	for i := range keys {
		wg.Go(func() {
			key, contentFingerprintErr4 := ContentFingerprint(t.Context(), input)
			assert.NoError(t, contentFingerprintErr4)
			assert.Equal(t, key != "", true)
			keys[i] = key
		})
	}
	wg.Wait()
	assert.Equal(t, keys[0], keys[1])
	assert.Equal(t, reader.reads.Load(), failedReads+1)
	for name, data := range map[string]string{"file": "first", "later": "later"} {
		assert.Equal(
			t,
			input.FileDigests()[name],
			manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte(data))),
		)
	}
	reads := reader.reads.Load()
	key, err := ContentFingerprint(t.Context(), input)
	assert.NoError(t, err)
	assert.Equal(t, key, keys[0])
	assert.Equal(t, reader.reads.Load(), reads)
}

type testFSContentFingerprintCaseCompletionInputs struct {
	mode **string
	keys *[2]string
}
