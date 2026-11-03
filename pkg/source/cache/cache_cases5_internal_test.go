package cache

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage"
	"github.com/google/go-cmp/cmp"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testNewIndexRecoveryRejectsEscapingParentCase(
	t *testing.T, component *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	outside := cachetest.New(t, t.TempDir(), New)
	{
		err := errors.Join(c.Close(), outside.Close())
		assert.NoError(t, err)
	}

	index := filepath.Join(outside.root.Name(), indexPath())
	testutil.WriteFile(
		t,
		filepath.Join(outside.root.Name(), ociDir),
		ocispec.ImageIndexFile,
		`{"schemaVersion":`,
	)
	link, target := filepath.Join(c.root.Name(), ociDir), filepath.Join(outside.root.Name(), ociDir)
	if (*component) == "final" {
		link = filepath.Join(link, ocispec.ImageIndexFile)
		target = index
	}
	{
		err := os.Rename(link, link+"-saved")
		assert.NoError(t, err)
	}
	{
		err := os.Symlink(target, link)
		assert.NoError(t, err)
	}

	before := sweepSnapshot(t, outside.root.Name())
	recovered, err := New(t.Context(), c.root.Name())
	if (*component) == "parent" {
		{
			_, ok := errors.AsType[*CacheError](
				err,
			)
			assert.FatalIf(t, !ok || recovered != nil ||
				!errors.Is(err, manifest.ErrFlux), "parent recovery: cache=%v, error=%v", recovered, err)
		}
		{
			removeErr2 := os.Remove(link)
			assert.NoError(t, removeErr2)
		}
		{
			renameErr := os.Rename(link+"-saved", link)
			assert.NoError(t, renameErr)
		}
	} else {
		assert.NoError(t, err)
		{
			err := recovered.Close()
			assert.NoError(t, err)
		}

		info, err := os.Lstat(link)
		assert.FatalIf(t, err != nil || !info.Mode().IsRegular(), "rebuilt index: %v, %v", info, err)
		{
			err := os.Remove(link + "-saved")
			assert.NoError(t, err)
		}
	}
	{
		diff := cmp.Diff(before, sweepSnapshot(t, outside.root.Name()))
		assert.FatalIf(t, diff != "", "outside index changed (-want +got):\n%s", diff)
	}
	reopened := cachetest.New(t, c.root.Name(), New)
	{
		err := reopened.Close()
		assert.NoError(t, err)
	}
}

func testGitFailedCleanupRejectsEscapingParentCase(
	t *testing.T, phase *string,
) {
	t.Helper()

	c := cachetest.New(t, t.TempDir(), New)
	outside := t.TempDir()
	key := GitKey{Content: "source-git-escaping-cleanup", RecurseSubmodules: true}
	parent := filepath.Dir(filepath.Join(c.root.Name(), gitObjectsPath("")))
	index := filepath.Join(c.root.Name(), indexPath())
	failure := errors.New("producer failed after parent replacement")
	var before map[string]string
	g, _, err := c.Git(
		t.Context(),
		key,
		Policy{MaxAge: 0},
		func(callbackArg1 context.Context, callbackArg2 storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
			return testGitFailedCleanupRejectsEscapingParentCaseCallback(
				callbackArg1,
				callbackArg2,
				worktree,
				testGitFailedCleanupRejectsEscapingParentCaseCallbackInputs{
					t:       &t,
					phase:   &phase,
					outside: &outside,
					parent:  &parent,
					index:   &index,
					failure: &failure,
					before:  &before,
				},
			)
		},
	)
	assert.FatalIf(
		t,
		g != nil || !errors.Is(err, manifest.ErrFlux),
		"failed Git acquisition: generation=%v, error=%v",
		g,
		err,
	)
	if (*phase) == "publication" {
		{
			_, ok := errors.AsType[*CacheError](err)
			assert.FatalIf(t, !ok, "publication error = %v", err)
		}
	} else {
		assert.FatalIf(t, !errors.Is(err, failure), "producer error = %v", err)
	}
	{
		diff := cmp.Diff(before, sweepSnapshot(t, outside))
		assert.FatalIf(t, diff != "", "outside changed (-want +got):\n%s", diff)
	}
	c.entries.Range(func(_, v any) bool {
		if !v.(*entry).resolved.IsZero() {
			t.Error("failed Git acquisition installed a successful entry")
		}
		return true
	})
	{
		err := os.Remove(parent)
		assert.NoError(t, err)
	}
	{
		err := os.Rename(parent+"-saved", parent)
		assert.NoError(t, err)
	}

	if (*phase) == "publication" {
		{
			err := os.Remove(index)
			assert.NoError(t, err)
		}
		{
			err := os.Rename(index+"-saved", index)
			assert.NoError(t, err)
		}
	}
	{
		err := c.Close()
		assert.NoError(t, err)
	}

	reopened := cachetest.New(t, c.root.Name(), New)
	{
		err := reopened.store.Tags(t.Context(), "", func(tags []string) error {
			assert.FatalIf(t, len(tags) != 0, "failed generation published durable tags: %v", tags)
			return nil
		})
		assert.NoError(t, err)
	}
}

func testAcquireGeneratedPolicyCase(t *testing.T, disk *bool,
) {
	t.Helper()

	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	for _, header := range []tar.Header{{Name: "empty/", Mode: 0o700, Typeflag: tar.TypeDir}, {Name: ".sourceignore", Mode: 0o600, Size: 4}, {Name: "kept", Mode: 0o711, Size: 4}, {Name: "parent/plain", Mode: 0o664, Size: 4}} {
		header.Uid, header.Gid, header.Uname, header.Gname, header.ModTime = 42, 43, "owner", "group", time.Unix(
			100,
			0,
		)
		header.AccessTime, header.ChangeTime, header.Format = time.Unix(
			101,
			0,
		), time.Unix(
			102,
			0,
		), tar.FormatPAX
		{
			err := writer.WriteHeader(&header)
			assert.NoError(t, err)
		}

		if header.Size != 0 {
			{
				_, err := io.WriteString(writer, "kept")
				assert.NoError(t, err)
			}
		}
	}
	testAcquireGeneratedPolicyCaseCompletion(
		t,
		&testAcquireGeneratedPolicyCaseCompletionInputs{writer: &writer, archive: &archive, c: &c},
	)
}

func testAcquireGeneratedPolicyCaseCompletion(t *testing.T, captures *testAcquireGeneratedPolicyCaseCompletionInputs) {
	t.Helper()
	{
		err := (*captures.writer).Close()
		assert.NoError(t, err)
	}

	key := ArchiveKey{Content: "generated-fixed", Policy: "fixed"}
	calls := 0
	produce := func(ctx context.Context, target ProducerInput) (ProducerResult, error) {
		calls++
		return producer(captures.archive.Bytes(), &ocispec.Descriptor{})(ctx, target)
	}
	var identity string
	for run := range 2 {
		input, descriptor, id, err := (*captures.c).Acquire(t.Context(), key, Policy{MaxAge: Forever}, produce)
		assert.NoError(t, err)

		assert.FatalIf(
			t,
			calls != 1 || id == "" || descriptor.Annotations["flate.tree-digest"] == "",
			"run %d: calls=%d descriptor=%+v",
			run,
			calls,
			descriptor,
		)
		if run == 0 {
			identity = descriptor.Annotations["flate.tree-digest"]
		} else {
			assert.FatalIf(t, identity != descriptor.Annotations["flate.tree-digest"], "%s", "warm tree digest changed")
		}

		descriptor.Annotations["flate.tree-digest"] = "caller-mutated"
		{
			data, err := fs.ReadFile(input, "kept")
			assert.FatalIf(t, err != nil || string(data) != "kept", "fixed ignore read = %q, %v", data, err)
		}
		for _, entry := range []struct {
			name string
			mode fs.FileMode
		}{{"empty", fs.ModeDir | 0o750}, {"parent", fs.ModeDir | 0o750}, {"parent/plain", 0o600}, {"kept", 0o700}} {
			info, err := fs.Stat(input, entry.name)
			if err != nil || info.Mode() != entry.mode {
				t.Fatalf("normalized %s = %v, %v; want mode %v", entry.name, info, err, entry.mode)
			}
			if !info.ModTime().IsZero() {
				t.Fatalf("normalized %s retained timestamp %v", entry.name, info.ModTime())
			}
			if header, ok := info.Sys().(*tar.Header); ok &&
				(header.Uid != 0 || header.Gid != 0 || header.Uname != "" || header.Gname != "") {
				t.Fatalf("normalized %s retained ownership: %+v", entry.name, header)
			}
			if header, ok := info.Sys().(*tar.Header); ok &&
				(!header.AccessTime.IsZero() || !header.ChangeTime.IsZero()) {
				t.Fatalf("normalized %s retained access/change times: %+v", entry.name, header)
			}
		}
	}
}

func testAcquireExtractPolicyCase(t *testing.T, tc *struct {
	name string
	path string
	kind byte
},
	disk *bool,
) {
	t.Helper()

	root := ""
	if *disk {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	header := &tar.Header{
		Name:     tc.path,
		Typeflag: tc.kind,
		Linkname: "target",
		Mode:     0o755,
	}
	{
		err := writer.WriteHeader(header)
		assert.NoError(t, err)
	}
	{
		err := writer.Close()
		assert.NoError(t, err)
	}

	input, _, id, err := c.Acquire(
		t.Context(),
		ArchiveKey{Content: tc.name, Policy: "extract"},
		Policy{MaxAge: Forever},
		producer(archive.Bytes(), &ocispec.Descriptor{}),
	)
	assert.FatalIf(
		t,
		err == nil || input != nil || id != "" || !errors.Is(err, manifest.ErrFlux),
		"invalid extraction = %q, %v",
		id,
		err,
	)
	input, _, id, err = c.Acquire(
		t.Context(),
		ArchiveKey{Content: tc.name, Policy: "extract"},
		Policy{MaxAge: Forever},
		producer(orderedArchive(t, "file", "retry"), &ocispec.Descriptor{}),
	)
	assert.FatalIf(t, err != nil || input == nil || id == "", "retry invalid extraction = %q, %v", id, err)
}

func testAcquireMemoryRetirementCase(t *testing.T, backing *string,
) {
	t.Helper()

	root := ""
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	files, bytesPerFile := 8, 32<<10
	if os.Getenv("FLATE_E8_SLOW") == "1" {
		files, bytesPerFile = 64, 1<<20
	}
	calls := 0
	fill := func(value string) func(context.Context, ProducerInput) (ProducerResult, error) {
		return testAcquireMemoryRetirementCaseCallback(
			value,
			testAcquireMemoryRetirementCaseCallbackInputs{files: &files, bytesPerFile: &bytesPerFile, calls: &calls},
		)
	}

	check := func(input fs.FS, value byte) {
		t.Helper()
		entries, err := fs.ReadDir(input, ".")
		assert.NoError(t, err)
		assert.Equal(t, len(entries), files)
		for _, name := range []string{"file-000", fmt.Sprintf("file-%03d", files-1)} {
			body, err := fs.ReadFile(input, name)
			assert.NoError(t, err)
			assert.FatalIf(
				t,
				len(body) != bytesPerFile || bytes.Count(body, []byte{value}) != bytesPerFile,
				"retained payload %s invalid",
				name,
			)
		}
	}
	var retained [2]uint64
	func() {
		c, err := New(t.Context(), root)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, c.Close()) }()
		key := ArchiveKey{Content: "E8 B11 retirement", Policy: "fixed"}
		first, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill("a"))
		assert.NoError(t, err)
		warm, _, _, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, fill("a"))
		assert.NoError(t, err)
		assert.Equal(t, calls, 1)
		check(warm, 'a')
		next, _, _, err := c.Acquire(t.Context(), key, Policy{}, fill("b"))
		assert.NoError(t, err)
		assert.Equal(t, calls, 2)
		check(first, 'a')
		check(next, 'b')
		runtime.GC()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		retained[0] = stats.HeapAlloc
		runtime.KeepAlive(first)
		runtime.KeepAlive(warm)
		runtime.KeepAlive(next)
	}()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	retained[1] = stats.HeapAlloc
	func() {
		c, err := New(t.Context(), root)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, c.Close()) }()
		input, _, _, err := c.Acquire(
			t.Context(),
			ArchiveKey{Content: "E8 B11 retirement", Policy: "fixed"},
			Policy{MaxAge: Forever},
			fill("b"),
		)
		assert.NoError(t, err)
		check(input, 'b')
		wantCalls := 3
		if (*backing) == "disk" {
			wantCalls = 2
		}
		assert.Equal(t, calls, wantCalls)
	}()
	t.Logf(
		"B11 retirement backing=%s files=%d bytes_per_file=%d fills=%d held_heap=%d released_closed_gc_heap=%d GOMEMLIMIT=%q GOGC=%q; process-wide samples, not RSS/attributed retention; persistent source reuse is distinct from render-cache build eligibility",
		(*backing),
		files,
		bytesPerFile,
		calls,
		retained[0],
		retained[1],
		os.Getenv("GOMEMLIMIT"),
		os.Getenv("GOGC"),
	)
}

func testAcquireGenerationsAndFreshnessCaseCorruptReopen(
	t *testing.T,
	testCaseInput testAcquireGenerationsAndFreshnessCaseCorruptReopenInputs,
) {
	t.Helper()

	e, _ := (*testCaseInput.c).entries.Load((*testCaseInput.id))
	path := filepath.Join((*testCaseInput.root), blobPath(e.(*entry).stored.Digest))
	{
		err := (*testCaseInput.c).Close()
		assert.NoError(t, err)
	}
	{
		err := os.Chmod(path, 0o600)
		assert.NoError(t, err)
	}

	testutil.WriteFileAt(t, path, "truncated")
	(*testCaseInput.c) = cachetest.New(t, (*testCaseInput.root), New)
	for attempt := range 3 {
		_, _, _, err := (*testCaseInput.c).Acquire(
			t.Context(),
			(*testCaseInput.key),
			Policy{MaxAge: Forever},
			(*testCaseInput.unexpected),
		)
		{
			_, ok := errors.AsType[*CacheError](err)
			assert.FatalIf(
				t,
				!ok || !errors.Is(err, io.ErrUnexpectedEOF),
				"corrupt reload %d: %v",
				attempt,
				err,
			)
		}
	}
}

type testAcquireArchiveFailuresAndLinksCaseCaseInputs struct {
	disk     **bool
	root     *string
	c        **Cache
	defaults *bool
}

func testAcquireArchiveFailuresAndLinksCaseCase(
	t *testing.T,
	testCaseInput testAcquireArchiveFailuresAndLinksCaseCaseInputs,
) {
	t.Helper()

	var data bytes.Buffer
	w := tar.NewWriter(&data)
	for _, h := range []*tar.Header{
		{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "../absent"},
		{Name: "hardlink", Typeflag: tar.TypeLink, Linkname: "file"},
		{Name: "dir/link", Typeflag: tar.TypeSymlink, Linkname: "../../absent"},
		{Name: "file", Typeflag: tar.TypeReg, Size: 7},
		{Name: "ignored", Typeflag: tar.TypeReg, Size: 7},
	} {
		assert.NoError(t, w.WriteHeader(h))
		if h.Typeflag == tar.TypeReg {
			_, err := io.WriteString(w, "content")
			assert.NoError(t, err)
		}
	}
	assert.NoError(t, w.Close())
	key := ArchiveKey{
		Content: fmt.Sprintf("source-test-link-%t", (*testCaseInput.defaults)),
		Ignore:  "ignored\n",
		Policy:  map[bool]string{true: "source"}[(*testCaseInput.defaults)],
	}
	input, _, _, err := (*testCaseInput.c).Acquire(
		t.Context(),
		key,
		Policy{MaxAge: Forever},
		producer(data.Bytes(), &ocispec.Descriptor{}),
	)
	assert.NoError(t, err)
	readers := []fs.FS{input}
	if *(*testCaseInput.disk) {
		fresh := cachetest.New(t, (*testCaseInput.root), New)
		replayed, _, _, err := fresh.Acquire(
			t.Context(),
			key,
			Policy{MaxAge: Forever},
			func(context.Context, ProducerInput) (ProducerResult, error) {
				t.Error("fresh owner refetched cached archive")
				return ProducerResult{}, errors.New("unexpected fetch")
			},
		)
		assert.NoError(t, err)
		readers = append(readers, replayed)
	}
	for _, reader := range readers {
		entries, err := fs.ReadDir(reader, ".")
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			len(entries) != 1 || entries[0].Name() != "file",
			"archive entries = %v",
			entries,
		)
		for _, name := range []string{"link", "link/child", "hardlink", "hardlink/child", "dir/link", "dir/link/nested/child"} {
			data, readFileErr2 := fs.ReadFile(reader, name)
			assert.Equal(t, len(data), 0)
			assert.Equal(t, errors.Is(readFileErr2, fs.ErrPermission), true)
			_, readFileErr2 = fs.Stat(reader, name)
			assert.Equal(t, errors.Is(readFileErr2, fs.ErrPermission), true)
		}
		for _, name := range []string{"absent", "ignored"} {
			_, readFileErr3 := fs.ReadFile(reader, name)
			assert.Equal(t, errors.Is(readFileErr3, fs.ErrNotExist), true)
		}
		data, err := fs.ReadFile(reader, "file")
		assert.NoError(t, err)
		assert.Equal(t, string(data), "content")
	}
}

type testAcquireConcurrentKeysAndCancellationDueCaseInputs struct {
	callers *int
	cold    *bool
	result  *string
}
type testGitFailedCleanupRejectsEscapingParentCaseCallbackInputs struct {
	t       **testing.T
	phase   **string
	outside *string
	parent  *string
	index   *string
	failure *error
	before  *map[string]string
}

func testGitFailedCleanupRejectsEscapingParentCaseCallback(
	_ context.Context,
	_ storage.Storer,
	worktree billy.Filesystem,
	testCaseInput testGitFailedCleanupRejectsEscapingParentCaseCallbackInputs,
) (plumbing.Hash, error) {
	id := filepath.Base(filepath.Dir(worktree.Root()))
	testutil.WriteFile((*testCaseInput.t), (*testCaseInput.outside), filepath.Join(id, "marker"), "untouched")
	(*testCaseInput.before) = sweepSnapshot((*testCaseInput.t), (*testCaseInput.outside))
	if err := os.Rename((*testCaseInput.parent), (*testCaseInput.parent)+"-saved"); err != nil {
		return plumbing.ZeroHash, err
	}
	if err := os.Symlink((*testCaseInput.outside), (*testCaseInput.parent)); err != nil {
		return plumbing.ZeroHash, err
	}
	if (*(*testCaseInput.phase)) == "publication" {
		if err := os.Rename((*testCaseInput.index), (*testCaseInput.index)+"-saved"); err != nil {
			return plumbing.ZeroHash, err
		}
		return plumbing.ZeroHash, os.Mkdir((*testCaseInput.index), 0o750)
	}
	return plumbing.ZeroHash, (*testCaseInput.failure)
}

type testAcquireMemoryRetirementCaseCallbackInputs struct {
	files        *int
	bytesPerFile *int
	calls        *int
}

func testAcquireMemoryRetirementCaseCallback(
	value string,
	testCaseInput testAcquireMemoryRetirementCaseCallbackInputs,
) func(context.Context, ProducerInput) (ProducerResult, error) {
	return func(_ context.Context, out ProducerInput) (ProducerResult, error) {
		(*testCaseInput.calls)++
		archive := tar.NewWriter(out)
		for n := range *testCaseInput.files {
			if err := archive.WriteHeader(
				&tar.Header{Name: fmt.Sprintf("file-%03d", n), Size: int64((*testCaseInput.bytesPerFile)), Mode: 0o600},
			); err != nil {
				return ProducerResult{}, err
			}
			if _, err := io.CopyN(
				archive,
				strings.NewReader(strings.Repeat(value, (*testCaseInput.bytesPerFile))),
				int64((*testCaseInput.bytesPerFile)),
			); err != nil {
				return ProducerResult{}, err
			}
		}
		return ProducerResult{}, archive.Close()
	}
}

type testAcquireGeneratedPolicyCaseCompletionInputs struct {
	writer  **tar.Writer
	archive *bytes.Buffer
	c       **Cache
}
