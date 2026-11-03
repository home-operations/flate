package gittree

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/home-operations/flate/pkg/store"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/gitrepo"
)

func testFSContentFingerprintCase2(t *testing.T, name *string,
) {
	t.Helper()

	reader := &contentReadFS{
		FS:      fstest.MapFS{"file": {Data: []byte("first")}, "later": {Data: []byte("later")}},
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	input, _, err := New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Direct, Current: reader})
	assert.NoError(t, err)
	_, err = input.SourcePolicy(t.Context())
	assert.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	joinerCtx, cancelJoiner := context.WithCancel(t.Context())
	defer cancelJoiner()
	joiner := &fingerprintJoinerContext{Context: joinerCtx, input: input}
	first := make(chan error, 1)
	var firstKey string
	go func() {
		var innerErr error
		firstKey, innerErr = ContentFingerprint(ctx, input)
		first <- innerErr
	}()
	<-reader.entered
	assert.Equal(t, input.FileDigests() == nil, true)
	second := make(chan struct {
		key string
		err error
	}, 1)
	waiterID := make(chan string, 1)
	go func() {
		buf := make([]byte, 64)
		n := runtime.Stack(buf, false)
		_, header, _ := strings.Cut(string(buf[:n]), " ")
		id, _, _ := strings.Cut(header, " ")
		waiterID <- id
		key, contentFingerprintErr5 := ContentFingerprint(joiner, input)
		second <- struct {
			key string
			err error
		}{key, contentFingerprintErr5}
	}()
	waitForOnceWaiter(t, <-waiterID)
	if (*name) != "canceled_joiner_live_executor" {
		cancel()
	}
	if (*name) != "live_joiner_retries" {
		cancelJoiner()
	}
	close(reader.release)
	if (*name) == "canceled_joiner_live_executor" {
		assert.NoError(t, <-first)
		assert.Equal(t, firstKey != "", true)
		result := <-second
		assert.Equal(t, errors.Is(result.err, context.Canceled), true)
		assert.Equal(t, result.key, "")
		assert.Equal(t, len(input.FileDigests()), 2)
		reads := reader.reads.Load()
		key, contentFingerprintErr6 := ContentFingerprint(t.Context(), input)
		assert.NoError(t, contentFingerprintErr6)
		assert.Equal(t, key, firstKey)
		assert.Equal(t, reader.reads.Load(), reads)
		return
	}
	assert.Equal(t, errors.Is(<-first, context.Canceled), true)
	result := <-second
	assert.Equal(t, joiner.recomputed.Load(), false)
	if (*name) == "canceled_joiner" {
		assert.Equal(t, errors.Is(result.err, context.Canceled), true)
		assert.Equal(t, result.key, "")
		assert.Equal(t, input.FileDigests() == nil, true)
		assert.Equal(t, reader.reads.Load(), int64(1))
		return
	}
	assert.NoError(t, result.err)
	assert.Equal(t, result.key != "", true)
	assert.Equal(t, len(input.FileDigests()), 2)
	for name, data := range map[string]string{"file": "first", "later": "later"} {
		assert.Equal(
			t,
			input.FileDigests()[name],
			manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte(data))),
		)
	}
	assert.Equal(t, reader.reads.Load(), int64(2))
	key, err := ContentFingerprint(t.Context(), input)
	assert.NoError(t, err)
	assert.Equal(t, key, result.key)
	assert.Equal(t, reader.reads.Load(), int64(2))
}

func testFSGitContentEvidenceCase(t *testing.T, mode *string,
) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	testutil.WriteFile(t, root, "dir/file", "alpha")
	testutil.WriteFile(t, root, "dir/ignored", "ignored")
	testutil.WriteFile(t, root, ".sourceignore", "dir/ignored\n")
	old := time.Unix(946684800, 0)
	assert.NoError(t, os.Chtimes(filepath.Join(root, "dir/file"), old, old))
	commit := testutil.GitCommitAll(
		t,
		repo,
		"fixture",
		&git.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@e", When: old}},
	)
	want := "alpha"
	if (*mode) == "dirty" || (*mode) == "staged" || (*mode) == "racy" ||
		(*mode) == "routed" {
		want = "bravo"
		testutil.WriteFile(t, root, "dir/file", want)
	}
	if (*mode) == "staged" {
		assert.NoError(t, os.Chtimes(filepath.Join(root, "dir/file"), old, old))
		wt, err := repo.Worktree()
		assert.NoError(t, err)
		_, err = wt.Add("dir/file")
		assert.NoError(t, err)
	}
	if (*mode) == "racy" {
		assert.NoError(t, os.Chtimes(filepath.Join(root, "dir/file"), old, old))
		assert.NoError(t, os.Chtimes(filepath.Join(root, ".git/index"), old, old))
	}
	if (*mode) == "untracked" {
		testutil.WriteFile(t, root, "dir/new", "untracked")
	}
	contentRoot, name := root, "dir/file"
	if (*mode) == "subtree" {
		contentRoot, name = filepath.Join(root, "dir"), "file"
	}
	location, err := gitrepo.Locate(t.Context(), contentRoot, contentRoot)
	assert.NoError(t, err)
	reader := &contentReadBilly{Filesystem: osfs.New(root)}
	location.Repository, err = git.Open(repo.Storer, reader)
	assert.NoError(t, err)
	input, err := AdmitDirectory(t.Context(), location)
	assert.NoError(t, err)
	objects := &contentReadBilly{Filesystem: repo.Storer.(*filesystem.Storage).Filesystem(), all: true}
	if (*mode) == "immutable" || (*mode) == "scoped_immutable" ||
		(*mode) == "direct_immutable" {
		viewMode := Source
		if (*mode) == "direct_immutable" {
			viewMode = Direct
		}
		input.FS, _, err = New(t.Context(), objects, commit, ViewOptions{Mode: viewMode})
		assert.NoError(t, err)
	}
	if (*mode) == "sdk" {
		want = "sdkdata"
		input.FS, _, err = New(
			t.Context(),
			nil,
			plumbing.ZeroHash,
			ViewOptions{
				Mode:     Direct,
				Location: location,
				Current:  fstest.MapFS{"dir/file": {Data: []byte(want)}},
			},
		)
		assert.NoError(t, err)
	}
	testFSGitContentEvidenceCaseCompletion(
		t,
		&testFSGitContentEvidenceCaseCompletionInputs{
			mode:    &mode,
			input:   &input,
			name:    &name,
			want:    &want,
			commit:  &commit,
			reader:  &reader,
			err:     &err,
			objects: &objects,
			root:    &root,
		},
	)
}

func testFSGitContentEvidenceCaseCompletion(t *testing.T, captures *testFSGitContentEvidenceCaseCompletionInputs) {
	t.Helper()
	if (*(*captures.mode)) == "scoped_local" || (*(*captures.mode)) == "scoped_immutable" {
		current, subErr := Sub((*captures.input).FS, "dir")
		assert.NoError(t, subErr)
		(*captures.input).
			FS, _, subErr = New(t.Context(), nil, plumbing.ZeroHash, ViewOptions{Mode: Source, Current: current})
		assert.NoError(t, subErr)
		(*captures.name) = "file"
	}
	if (*(*captures.mode)) == "source_unreadable" || (*(*captures.mode)) == "routed" {
		opts := ViewOptions{Mode: Source, Current: (*captures.input).FS}
		ref := plumbing.ZeroHash
		if (*(*captures.mode)) == "routed" {
			(*captures.want), ref = "alpha", (*captures.commit)
			opts.Worktree, opts.Changes = (*captures.reader), []string{"dir/file"}
		}
		(*captures.input).
			FS, _, (*captures.err) = New(t.Context(), (*captures.objects), ref, opts)
		assert.NoError(t, (*captures.err))
	}
	selected := (*captures.input).FS.(*FS)
	if (*(*captures.mode)) == "unlocated" {
		selected.location = nil
	}
	_, (*captures.err) = selected.SourcePolicy(t.Context())
	assert.NoError(t, (*captures.err))
	(*captures.objects).
		reads.Store(0)
	if (*(*captures.mode)) == "unreadable_clean" || (*(*captures.mode)) == "source_unreadable" ||
		(*(*captures.mode)) == "subtree" ||
		(*(*captures.mode)) ==
			"scoped_local" {
		file := filepath.Join((*captures.root), "dir/file")
		assert.NoError(t, os.Chmod(file, 0))
		t.Cleanup(func() { assert.NoError(t, os.Chmod(file, 0o644)) })
		if _, readFileErr3 := os.ReadFile(file); !errors.Is(readFileErr3, fs.ErrPermission) {
			t.Skipf("filesystem cannot enforce unreadable file: %v", readFileErr3)
		}
	}
	assert.Equal(t, selected.FileDigests() == nil, true)
	_, (*captures.err) = ContentFingerprint(t.Context(), selected)
	assert.NoError(t, (*captures.err))
	assert.Equal(
		t,
		selected.FileDigests()[(*captures.name)],
		manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte((*captures.want)))),
	)
	if (*(*captures.mode)) == "untracked" {
		assert.Equal(
			t,
			selected.FileDigests()["dir/new"],
			manifest.Fingerprint(plumbing.ComputeHash(plumbing.BlobObject, []byte("untracked"))),
		)
	}
	if (*(*captures.mode)) != "subtree" {
		_, ignored := selected.FileDigests()["dir/ignored"]
		assert.Equal(t, ignored, false)
	}
	reads := (*captures.reader).reads.Load()
	assert.Equal(t, (*captures.objects).reads.Load(), int64(0))
	_, (*captures.err) = ContentFingerprint(t.Context(), selected)
	assert.NoError(t, (*captures.err))
	assert.Equal(t, (*captures.reader).reads.Load(), reads)
}

func testGitViewSparseBodyReadsCase(t *testing.T, size *struct {
	name  string
	files int
	bytes int64
},
) {
	t.Helper()

	if size.name == "exact" && os.Getenv("FLATE_E8_SLOW") != "1" {
		t.Skip(
			"MEM-SPARSE not-run: env FLATE_E8_SLOW=1 go test ./pkg/source/gittree -run '^TestGitView_SparseBodyReads/exact$' -count=1 -v",
		)
	}
	fixture := testutil.NewSparseGit(t, size.files, size.bytes)
	reads := &testutil.GitReads{LargeHash: fixture.Large}
	g := &gitView{
		commit: fixture.Base,
		mode:   Source,
		nodes:  map[string]*gitNode{".": {name: ".", mode: fs.ModeDir | 0o755}},
	}
	g.storers.New = func() any { return &testutil.CountingStorer{Storer: fixture.Repo.Storer, Reads: reads} }
	input := &FS{view: g, snapshot: &localPolicy{}}
	commit, err := fixture.Repo.CommitObject(fixture.Base)
	assert.NoError(t, err)
	tree, err := commit.Tree()
	assert.NoError(t, err)
	entry, err := tree.FindEntry("app/input.yaml")
	assert.NoError(t, err)
	reader, err := g.openBlob(entry.Hash)
	assert.NoError(t, err)
	data, err := io.ReadAll(reader)
	assert.NoError(t, errors.Join(err, reader.Close()))
	assert.FatalIf(
		t,
		!strings.Contains(string(data), "before") || reads.Bodies.Load() != 1 || reads.Large.Load() != 0,
		"%s",
		"pool blob Reader calibration failed",
	)

	reads.Reset()
	warnings, err := g.walk(t.Context(), fixture.Base)
	assert.NoError(t, err)
	assert.Equal(t, len(warnings), 0)
	assert.FatalIf(t, reads.Metadata.Load() == 0 || reads.Bodies.Load() != 0, "%s", "walk opened a blob Reader")

	t.Logf(
		"walk metadata=%d bodies=%d large=%d",
		reads.Metadata.Load(),
		reads.Bodies.Load(),
		reads.Large.Load(),
	)
	reads.Reset()
	assert.NoError(t, g.shape(t.Context(), ViewOptions{Mode: Source}, input))
	count := 0
	assert.NoError(t, fs.WalkDir(input, ".", func(_ string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() {
			count++
		}
		return err
	}))
	assert.Equal(t, count, size.files)
	info, err := fs.Stat(input, "unrelated/large.txt")
	assert.NoError(t, err)
	assert.Equal(t, info.Size(), size.bytes)
	assert.Equal(t, reads.Bodies.Load(), int64(0))
	t.Logf(
		"shape/list/stat metadata=%d bodies=%d large=%d",
		reads.Metadata.Load(),
		reads.Bodies.Load(),
		reads.Large.Load(),
	)
	reads.Reset()
	data, err = fs.ReadFile(input, "app/input.yaml")
	assert.NoError(t, err)
	assert.FatalIf(t, !strings.Contains(string(data), "before"), "base payload=%s", data)
	assert.Equal(t, reads.Bodies.Load(), int64(1))
	assert.Equal(t, reads.Large.Load(), int64(0))
	t.Logf(
		"consumed read metadata=%d bodies=%d large=%d",
		reads.Metadata.Load(),
		reads.Bodies.Load(),
		reads.Large.Load(),
	)
}

type testNewEntryPolicyLocalReadsCaseInputs struct {
	reader *struct {
		name string
		fs   fs.FS
	}
}

func testNewEntryPolicyLocalReadsCase(t *testing.T, testCaseInput testNewEntryPolicyLocalReadsCaseInputs) {
	t.Helper()

	entries, err := fs.ReadDir(testCaseInput.reader.fs, ".")
	assert.FatalIf(t, err != nil || len(entries) != 2 || entries[0].Name() != "nested" ||
		entries[1].Name() != "regular.txt", "admitted entries = %v, %v", entries, err)
	for _, name := range []string{"regular.txt", "file-link", "dir-link/regular.txt", "dangling", "escape", "escape/child", "broken-escape", "broken-escape/child", "socket", "absent"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var want error
			content := "content"
			if name != "regular.txt" {
				want, content = fs.ErrPermission, ""
				if name == "socket" || name == "absent" {
					want = fs.ErrNotExist
				}
			}
			data, err := fs.ReadFile(testCaseInput.reader.fs, name)
			assert.FatalIf(
				t,
				!errors.Is(err, want) || string(data) != content,
				"read = %q, %v; want %q, %v",
				data,
				err,
				content,
				want,
			)
			{
				_, err := fs.Stat(testCaseInput.reader.fs, name)
				assert.FatalIf(t, !errors.Is(err, want), "stat = %v; want %v", err, want)
			}
		})
	}
}

func testNewEntryPolicyLocalOperationObservationsIgnoreSnapshot(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, ".sourceignore", "root\n")
	testutil.WriteFile(t, root, "nested/.sourceignore", "first\n")
	testutil.WriteFile(t, root, ".git/.sourceignore", "excluded\n")
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	input := artifact.FS
	shaped, pin, err := operation(t, input, Source)
	assert.NoError(t, err)
	assert.Equal(t, pin, "")
	content := shaped.PolicyEvidence()
	want := []struct {
		Path string
		Data []byte
	}{{".sourceignore", []byte("root\n")}, {"nested/.sourceignore", []byte("first\n")}}
	assert.Diff(t, content, any(want))
	testutil.WriteFile(t, root, "nested/.sourceignore", "other\n")
	shaped, pin, err = operation(t, input, Source)
	assert.Equal(t, pin, "")
	retained := shaped.PolicyEvidence()
	assert.NoError(t, err)
	assert.Diff(t, retained, content)
	artifact, err = admitLocal(t, root)
	assert.NoError(t, err)
	input = artifact.FS
	shaped, pin, err = operation(t, input, Source)
	assert.Equal(t, pin, "")
	refreshed := shaped.PolicyEvidence()
	assert.NoError(t, err)
	want[1].Data = []byte("other\n")
	assert.Diff(t, refreshed, any(want))
	assert.NoError(t, os.Rename(filepath.Join(root, "nested"), filepath.Join(root, "renamed")))
	artifact, err = admitLocal(t, root)
	assert.NoError(t, err)
	input = artifact.FS
	shaped, pin, err = operation(t, input, Source)
	assert.Equal(t, pin, "")
	moved := shaped.PolicyEvidence()
	assert.NoError(t, err)
	want[1].Path = "renamed/.sourceignore"
	assert.Diff(t, moved, any(want))
	unfiltered, pin, err := operation(t, input, Direct)
	assert.Equal(t, pin, "")
	assert.NoError(t, err)
	assert.FatalIf(t, unfiltered.PolicyEvidence() != nil, "unfiltered policy = %v", unfiltered)
}

func testNewEntryPolicyLocalOperationObservationsPointLookupWithoutListing(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	dir := filepath.Join(root, "siblings")
	for _, name := range []string{"a", "b"} {
		testutil.WriteFile(t, dir, name, name)
	}
	assert.Equal(t, os.Chmod(dir, 0o111), nil)
	t.Cleanup(func() { assert.Equal(t, os.Chmod(dir, 0o755), nil) })
	if _, err := os.ReadDir(dir); err == nil {
		t.Skip("filesystem permits listing a search-only directory")
	} else if !errors.Is(err, fs.ErrPermission) {
		t.Fatal(err)
	}
	artifact, err := admitLocal(t, root)
	assert.Equal(t, err, nil)
	input := artifact.FS
	for _, name := range []string{"a", "b"} {
		file := "siblings/" + name
		data, readErr := fs.ReadFile(input, file)
		_, statErr := fs.Stat(input, file)
		assert.FatalIf(
			t,
			readErr != nil || statErr != nil || string(data) != name,
			"point lookup %s = %q, %v, %v",
			file,
			data,
			readErr,
			statErr,
		)
	}
}

type testNewEntryPolicyLocalOperationObservationsCaseInputs struct {
	tc *struct {
		name string
		path string

		content string
		view    fs.FS
		want    error
	}
}

func testNewEntryPolicyLocalOperationObservationsCase(
	t *testing.T,
	testCaseInput testNewEntryPolicyLocalOperationObservationsCaseInputs,
) {
	t.Helper()
	data, err := fs.ReadFile(testCaseInput.tc.view, testCaseInput.tc.path)
	if !errors.Is(err, testCaseInput.tc.want) || string(data) != testCaseInput.tc.content {
		t.Fatalf("view read = %q, %v; want %q, %v", data, err, testCaseInput.tc.content, testCaseInput.tc.want)
	}
}

type testNewEntryPolicyLocalMetadataCaseInputs struct {
	directory *bool
}

func testNewEntryPolicyLocalMetadataCase(t *testing.T, testCaseInput testNewEntryPolicyLocalMetadataCaseInputs) {
	t.Helper()

	root := t.TempDir()
	testutil.WriteFile(t, root, "file", "content")
	for _, prefix := range []string{"", "nested/"} {
		name := prefix + ".git"
		if *testCaseInput.directory {
			name += "/config"
		}
		testutil.WriteFile(t, root, name, "metadata")
	}
	artifact, err := admitLocal(t, root)
	assert.NoError(t, err)
	input := artifact.FS.(*FS)
	location := input.Location()
	assert.Equal(t, location.ContentRoot, root)
	for _, mode := range []ViewMode{Direct, Source, RemoteBase} {
		view, pin, operationErr := operation(t, input, mode)
		assert.NoError(t, operationErr)
		assert.Equal(t, pin, "")
		assert.Equal(t, view.Location() == location, true)
		for _, reader := range []fs.FS{input, view} {
			for _, name := range []string{".git", ".git/config", "nested/.git", "nested/.git/config"} {
				_, statErr2 := fs.Stat(reader, name)
				assert.Equal(t, errors.Is(statErr2, fs.ErrNotExist), true)
				_, statErr2 = fs.ReadFile(reader, name)
				assert.Equal(t, errors.Is(statErr2, fs.ErrNotExist), true)
				_, statErr2 = reader.Open(name)
				assert.Equal(t, errors.Is(statErr2, fs.ErrNotExist), true)
				_, statErr2 = fs.ReadDir(reader, name)
				assert.Equal(t, errors.Is(statErr2, fs.ErrNotExist), true)
			}
			assert.NoError(
				t,
				fs.WalkDir(reader, ".", func(name string, entry fs.DirEntry, err error) error {
					assert.NoError(t, err)
					assert.ErrorIf(t, entry.Name() == ".git", "metadata survived traversal: %s", name)
					return nil
				}),
			)
		}
	}
	assert.NoError(t, os.RemoveAll(filepath.Join(root, ".git")))
	testutil.WriteFile(t, root, ".git", "changed metadata")
	fresh, _, err := operation(t, input, Direct)
	assert.NoError(t, err)
	assert.Equal(t, fresh.Location() == location, true)
}

type testNewEntryPolicyShapingCaseInputs struct {
	repo   **git.Repository
	commit *plumbing.Hash
	tc     *struct {
		name string
		opts ViewOptions

		files []string
	}
}

func testNewEntryPolicyShapingCase(t *testing.T, testCaseInput testNewEntryPolicyShapingCaseInputs) {
	t.Helper()

	input, _, err := New(
		t.Context(),
		(*testCaseInput.repo).Storer.(*filesystem.Storage).Filesystem(),
		(*testCaseInput.commit),
		testCaseInput.tc.opts,
	)
	assert.NoError(t, err)
	var files []string
	assert.NoError(t, fs.WalkDir(input, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			files = append(files, name)
		}
		return nil
	}))
	assert.Diff(t, files, testCaseInput.tc.files)
	omitted := []string{"escape", "missing", "directory"}
	if testCaseInput.tc.opts.Mode == RemoteBase {
		omitted = append(omitted, ".hidden", "node_modules")
	} else {
		omitted = append(omitted, "alias", ".hidden/link")
	}
	for _, name := range omitted {
		want := fs.ErrNotExist
		if testCaseInput.tc.opts.Mode != RemoteBase {
			want = fs.ErrPermission
		}
		_, err := fs.Stat(input, name)
		assert.FatalIf(t, !errors.Is(err, want), "omitted %s: %v; want %v", name, err, want)
	}
	if testCaseInput.tc.opts.Mode == RemoteBase {
		data, err := fs.ReadFile(input, "alias")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "target")
	}
}

type testNewEntryPolicySparseShapingAndRefreshCaseInputs struct {
	root  *string
	files *map[string]string
	tc    *struct {
		name   string
		sparse []string
		ignore string
		want   []string
	}
}
type testFSGitContentEvidenceCaseCompletionInputs struct {
	mode    **string
	input   **store.SourceArtifact
	name    *string
	want    *string
	commit  *plumbing.Hash
	reader  **contentReadBilly
	err     *error
	objects **contentReadBilly
	root    *string
}
