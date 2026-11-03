package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testBlobRenderValidationCase(t *testing.T, backing *string,
) {
	t.Helper()
	processIdentity = ""
	root := ""
	if (*backing) != "memory" {
		root = t.TempDir()
	}
	if strings.HasPrefix((*backing), "identified disk") {
		processIdentity = "identified"
	}
	c := cachetest.New(t, root, New)
	mode := RenderPending
	if processIdentity != "" && root != "" {
		mode = RenderAccepted
	}
	synctest.Test(t, func(t *testing.T) {
		testBlobRenderValidationCaseCallback(
			t,
			testBlobRenderValidationCaseCallbackInputs{backing: &backing, c: &c, mode: &mode},
		)
	},
	)
}

func testBlobNonReusableGenerationCase(t *testing.T, backing *string,
) {
	t.Helper()
	root := ""
	if (*backing) != "memory" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	synctest.Test(t, func(t *testing.T) {
		testBlobNonReusableGenerationCaseCallback(
			t,
			testBlobNonReusableGenerationCaseCallbackInputs{backing: &backing, root: &root, c: &c},
		)
	},
	)
}

func testBlobRevalidationCase(t *testing.T, backing *string,
) {
	t.Helper()

	synctest.Test(t, func(t *testing.T) {
		root := ""
		if (*backing) == "disk" {
			root = t.TempDir()
		}
		c := cachetest.New(t, root, New)
		key := BlobKey{Content: "revalidated"}
		original := ocispec.Descriptor{Size: 7, Annotations: map[string]string{"validator": "opaque"}}
		r, _, id, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("payload"), &original))
		assert.NoError(t, err)

		view := NameBlob("content", r)
		identity := AcquisitionIdentity(view)

		loaded, _ := c.entries.Load(id)
		first := loaded.(*entry)
		resolved := first.resolved
		var before os.FileInfo
		if root != "" {
			before, err = c.root.Stat(blobPath(first.stored.Digest))
			assert.NoError(t, err)
			assert.NoError(t, c.Close())
			c = cachetest.New(t, root, New)
		}
		time.Sleep(time.Second)
		r, desc, gotID, err := c.Blob(
			t.Context(),
			key,
			Policy{},
			func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				previous, previousErr2 := w.Previous(ctx)
				assert.FatalIf(
					t,
					previousErr2 != nil || previous == nil || previous.Annotations["validator"] != "opaque",
					"previous=%+v, error=%v",
					previous,
					previousErr2,
				)
				previous.Annotations["validator"] = "mutated"
				independent, previousErr2 := w.Previous(ctx)
				assert.FatalIf(
					t,
					previousErr2 != nil || independent.Annotations["validator"] != "opaque",
					"metadata alias: %+v, %v",
					independent,
					previousErr2,
				)
				previous.Annotations = map[string]string{"validator": "updated"}
				return ProducerResult{Descriptor: *previous, Unchanged: true}, nil
			},
		)
		assert.NoError(t, err)

		assert.FatalIf(t, gotID != id || AcquisitionIdentity(NameBlob("content", r)) != identity ||
			desc.Annotations["validator"] != "updated", "unchanged id=%q, descriptor=%+v", gotID, desc)
		body, readErr := fs.ReadFile(r, "content")
		assert.NoError(t, readErr)

		assert.Equal(t, string(body), "payload")
		loaded, _ = c.entries.Load(id)
		next := loaded.(*entry)
		assert.FatalIf(t, !next.resolved.After(resolved), "%s", "validation did not renew resolution time")

		if root != "" {
			after, statErr := c.root.Stat(blobPath(next.stored.Digest))
			assert.NoError(t, statErr)
			assert.FatalIf(
				t,
				!os.SameFile(before, after) || !before.ModTime().Equal(after.ModTime()),
				"%s",
				"unchanged validation rewrote payload",
			)

			assert.NoError(t, c.Close())
			c = cachetest.New(t, root, New)
		}
		_, desc, _, err = c.Blob(t.Context(), key, Policy{MaxAge: time.Second}, nil)
		assert.NoError(t, err)

		assert.FatalIf(
			t,
			desc.Annotations["validator"] != "updated",
			"validation metadata not persisted: %+v",
			desc,
		)
		loaded, _ = c.entries.Load(id)
		assert.FatalIf(t, !loaded.(*entry).resolved.Equal(next.resolved), "%s", "resolution time not persisted")
	})
}

func testBlobRevalidationAbsent(t *testing.T) {
	t.Helper()

	c := cachetest.New(t, "", New)
	r, _, _, err := c.Blob(
		t.Context(),
		BlobKey{Content: "absent"},
		Policy{},
		func(context.Context, ProducerInput) (ProducerResult, error) {
			return ProducerResult{Unchanged: true}, nil
		},
	)
	assert.FatalIf(t, r != nil || !errors.Is(err, manifest.ErrInput), "unchanged without payload: %v", err)
}

func testBlobRevalidationUnchangedParentReplacement(t *testing.T) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	key := BlobKey{Content: "unchanged-parent-replacement"}
	upstream := ocispec.Descriptor{
		Digest:      digest.FromString("upstream"),
		Size:        7,
		Annotations: map[string]string{"validator": "opaque"},
	}
	r, _, id, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("payload"), &upstream))
	assert.NoError(t, err)
	identity := AcquisitionIdentity(NameBlob("content", r))

	stored, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)
	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	r, desc, gotID, err := c.Blob(
		t.Context(),
		key,
		Policy{},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			previous, previousErr3 := w.Previous(ctx)
			assert.FatalIf(t, previousErr3 != nil || previous == nil, "previous=%+v, error=%v", previous, previousErr3)
			return ProducerResult{Descriptor: *previous, Unchanged: true}, nil
		},
	)
	assert.NoError(t, err)
	assert.Diff(t, desc, upstream)
	assert.Equal(t, gotID, id)
	view := NameBlob("content", r)
	assert.Equal(t, AcquisitionIdentity(view), identity)
	body, readErr := fs.ReadFile(r, "content")
	assert.NoError(t, readErr)

	assert.Equal(t, string(body), "payload")
	current, err := c.store.Resolve(t.Context(), id)
	assert.NoError(t, err)
	assert.Equal(t, current.Digest, stored.Digest)
	parent := filepath.Dir(blobPath(stored.Digest))
	assert.NoError(t, c.root.Rename(parent, parent+"-held"))
	assert.NoError(t, c.root.Mkdir(parent, 0o750))
	{
		body, err := fs.ReadFile(view, "content")
		assert.FatalIf(
			t,
			!errors.Is(err, fs.ErrNotExist),
			"unchanged view ignored replacement: body=%q, error=%v",
			body,
			err,
		)
	}
	assert.NoError(t, c.root.Remove(parent))
	outside := t.TempDir()
	testutil.WriteFile(t, outside, stored.Digest.Encoded(), "outside")
	assert.NoError(t, os.Symlink(outside, filepath.Join(root, parent)))
	{
		body, err := fs.ReadFile(view, "content")
		assert.FatalIf(t, err == nil, "unchanged view accepted escaping parent: body=%q", body)
	}
}

func testBlobRevalidationCase2(t *testing.T, invalid *string,
) {
	t.Helper()

	root := t.TempDir()
	c := cachetest.New(t, root, New)
	key := BlobKey{Content: "missing"}
	_, _, id, err := c.Blob(t.Context(), key, Policy{}, producer([]byte("old"), &ocispec.Descriptor{}))
	assert.NoError(t, err)

	e, _ := c.entries.Load(id)
	path := blobPath(e.(*entry).stored.Digest)
	assert.NoError(t, c.root.Remove(path))
	if (*invalid) == "corrupted-size" {
		testutil.WriteFile(t, root, path, "truncated")
	}
	assert.NoError(t, c.Close())
	c = cachetest.New(t, root, New)
	r, _, _, err := c.Blob(
		t.Context(),
		key,
		Policy{},
		func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
			previous, previousErr4 := w.Previous(ctx)
			assert.FatalIf(
				t,
				previousErr4 != nil || previous != nil,
				"invalid previous=%+v, error=%v",
				previous,
				previousErr4,
			)
			_, previousErr4 = io.WriteString(w, "old")
			return ProducerResult{}, previousErr4
		},
	)
	assert.NoError(t, err)
	body, readErr := fs.ReadFile(r, "content")
	assert.NoError(t, readErr)

	assert.Equal(t, string(body), "old")
	info, err := c.root.Stat(path)
	assert.NoError(t, err)
	assert.Equal(t, info.Mode().Perm(), fs.FileMode(0o444))
	entries, err := os.ReadDir(filepath.Join(root, ociDir))
	assert.NoError(t, err)
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "acquire-") {
			t.Errorf("successful repair left acquisition name %q", entry.Name())
		}
	}
}

func testBlobRepairFailureCleanupCase(t *testing.T, failure *bool,
) {
	t.Helper()

	root := t.TempDir()
	c, err := New(t.Context(), root)
	assert.NoError(t, err)

	t.Cleanup(func() {
		if closeErr4 := c.Close(); closeErr4 != nil && !errors.Is(closeErr4, os.ErrClosed) {
			t.Error(closeErr4)
		}
	})
	payload := []byte("repair payload")
	path := blobPath(digest.FromBytes(payload))
	assert.NoError(t, c.root.MkdirAll(path, 0o750))
	before, err := c.root.Stat(path)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		before.Size() == int64(len(payload)),
		"%s",
		"destination directory must have a different size from the payload",
	)

	var names []string
	releases := 0
	r, _, id, err := c.Blob(
		t.Context(),
		BlobKey{Content: "repair-failure"},
		Policy{Admit: func(context.Context) (func(), error) {
			return func() { releases++ }, nil
		}},
		func(callbackArg1 context.Context, w ProducerInput) (ProducerResult, error) {
			return testBlobRepairFailureCleanupCaseCallback(
				callbackArg1,
				w,
				testBlobRepairFailureCleanupCaseCallbackInputs{
					t:        &t,
					failure:  &failure,
					root:     &root,
					c:        &c,
					payload:  &payload,
					names:    &names,
					releases: &releases,
				},
			)
		},
	)
	assert.FatalIf(t, releases != 1, "repair releases=%d, want one after cleanup/publication failure", releases)
	rename, ok := errors.AsType[*os.LinkError](err)
	assert.FatalIf(
		t,
		!ok || rename.Op != "renameat" || rename.New != path || !slices.Contains(names, rename.Old),
		"missing repair rename error: %v",
		err,
	)
	{
		_, ok := errors.AsType[*CacheError](
			err,
		)
		assert.FatalIf(t, !ok || !errors.Is(err, manifest.ErrFlux) || r != nil ||
			id != "", "failed repair reader=%v, identity=%q, error=%v", r, id, err)
	}
	shouldReturn := testBlobRepairFailureCleanupCaseCompletion(
		t,
		&testBlobRepairFailureCleanupCaseCompletionInputs{
			failure: &failure,
			err:     &err,
			names:   &names,
			c:       &c,
			path:    &path,
			before:  &before,
		},
	)
	if shouldReturn {
		return
	}
}

func testBlobRepairFailureCleanupCaseCompletion(
	t *testing.T,
	captures *testBlobRepairFailureCleanupCaseCompletionInputs,
) bool {
	t.Helper()
	if *(*captures.failure) {
		removals := make(map[string]int)
		pending := []error{(*captures.err)}
		for len(pending) != 0 {
			cause := pending[0]
			pending = pending[1:]
			switch cause := cause.(type) {
			case *os.PathError:
				if cause.Op == "removeat" {
					assert.ErrorIf(
						t,
						!errors.Is(cause, os.ErrClosed),
						"removal lost closed-root cause: %v",
						cause,
					)
					removals[cause.Path]++
				}
			case interface{ Unwrap() []error }:
				pending = append(pending, cause.Unwrap()...)
			case interface{ Unwrap() error }:
				pending = append(pending, cause.Unwrap())
			}
		}
		for _, name := range *captures.names {
			if removals[name] != 1 {
				t.Errorf("removal errors for %q=%d, want one: %v", name, removals[name], (*captures.err))
			}
		}
		return true
	}
	for _, name := range *captures.names {
		{
			_, statErr2 := (*captures.c).root.Stat(name)
			assert.ErrorIf(
				t,
				!errors.Is(statErr2, fs.ErrNotExist),
				"repair left acquisition name %q: %v",
				name,
				statErr2,
			)
		}
	}
	var after os.FileInfo

	after, (*captures.err) = (*captures.c).root.Stat((*captures.path))
	assert.NoError(t, (*captures.err))
	assert.FatalIf(
		t,
		!after.IsDir() || !os.SameFile((*captures.before), after),
		"%s",
		"failed repair replaced the destination directory",
	)
	return false
}

type testBlobRenderValidationCaseCallbackInputs struct {
	backing **string
	c       **Cache
	mode    *RenderMode
}

func testBlobRenderValidationCaseCallback(t *testing.T, testCaseInput testBlobRenderValidationCaseCallbackInputs) {
	t.Helper()
	if (*(*testCaseInput.backing)) == "identified disk due" {
		_, _, _, err := (*testCaseInput.c).Blob(
			t.Context(),
			BlobKey{Content: "shared-render", Render: (*testCaseInput.mode)},
			Policy{MaxAge: Forever},
			func(context.Context, ProducerInput) (ProducerResult, error) { return ProducerResult{}, connReset() },
		)
		assert.FatalIf(t, !errors.Is(err, syscall.ECONNRESET), "initial failure: %v", err)
		time.Sleep(time.Second)
	}
	var calls, validations atomic.Int64
	var current atomic.Value
	current.Store("old")
	entered, release := make(chan struct{}), make(chan struct{})
	run := func(view func() string, pause bool) {
		key := BlobKey{
			Content: "shared-render",
			Render:  (*testCaseInput.mode),
			Validate: func(_ context.Context, r fs.FS) (bool, error) {
				validations.Add(1)
				data, err := fs.ReadFile(r, "content")
				return string(data) == view(), err
			},
		}
		r, _, _, err := (*testCaseInput.c).Blob(
			t.Context(),
			key,
			Policy{MaxAge: Forever},
			func(_ context.Context, w ProducerInput) (ProducerResult, error) {
				calls.Add(1)
				if pause {
					close(entered)
					<-release
				}
				_, err := io.WriteString(w, view())
				return ProducerResult{}, err
			},
		)
		assert.NoError(t, err)
		data, _ := fs.ReadFile(r, "content")

		assert.Equal(t, string(data), view())
	}
	var wg sync.WaitGroup
	wg.Go(func() { run(func() string { return "old" }, true) })
	<-entered
	for range 2 {
		wg.Go(func() { run(func() string { return current.Load().(string) }, false) })
	}
	synctest.Wait()
	current.Store("new")
	close(release)
	wg.Wait()
	assert.Equal(t, calls.Load(), int64(2))
	assert.Equal(t, validations.Load(), int64(5))
	run(func() string { return "new" }, false)
	assert.Equal(t, calls.Load(), int64(2))
	failure := errors.New("caller validation failed")
	key := BlobKey{
		Content:  "shared-render",
		Render:   (*testCaseInput.mode),
		Validate: func(context.Context, fs.FS) (bool, error) { return false, failure },
	}
	r, _, _, err := (*testCaseInput.c).Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
	assert.FatalIf(t, r != nil || !errors.Is(err, failure), "validator failure: %v", err)
	key.Validate = func(context.Context, fs.FS) (bool, error) { return false, nil }
	r, _, _, err = (*testCaseInput.c).Blob(t.Context(), key, Policy{MaxAge: Forever}, nil)
	assert.FatalIf(t, r != nil || !errors.Is(err, errdef.ErrNotFound), "invalid lookup: %v", err)
}

type testBlobNonReusableGenerationCaseCallbackInputs struct {
	backing **string
	root    *string
	c       **Cache
}

func testBlobNonReusableGenerationCaseCallback(
	t *testing.T,
	testCaseInput testBlobNonReusableGenerationCaseCallbackInputs,
) {
	t.Helper()
	key := BlobKey{Content: "changing-input", Render: RenderPending}
	if (*testCaseInput.root) != "" {
		key.Render = RenderAccepted
	}
	if (*(*testCaseInput.backing)) == "disk due" {
		_, _, _, err := (*testCaseInput.c).Blob(
			t.Context(),
			key,
			Policy{MaxAge: Forever},
			func(context.Context, ProducerInput) (ProducerResult, error) { return ProducerResult{}, connReset() },
		)
		assert.FatalIf(t, !errors.Is(err, syscall.ECONNRESET), "initial failure: %v", err)
		time.Sleep(time.Second)
	}
	var reads atomic.Int64
	readInput := func() string { return strconv.FormatInt(reads.Add(1), 10) }
	entered, release := make(chan struct{}), make(chan struct{})
	var generations [2]fs.FS
	var payloads [2]string
	run := func(i int) {
		calls, validations := 0, 0
		request := key
		request.Validate = func(_ context.Context, generation fs.FS) (bool, error) {
			validations++
			data, err := fs.ReadFile(generation, "content")
			return string(data) == readInput(), err
		}
		generation, _, _, err := (*testCaseInput.c).Blob(
			t.Context(),
			request,
			Policy{MaxAge: Forever},
			func(_ context.Context, w ProducerInput) (ProducerResult, error) {
				calls++
				if calls > 1 {
					return ProducerResult{}, fmt.Errorf("produced more than once in call %d", i)
				}
				payloads[i] = readInput()
				if i == 0 {
					close(entered)
					<-release
				}
				_, err := io.WriteString(w, payloads[i])
				return ProducerResult{}, err
			},
		)
		if err != nil || calls != 1 || validations != 1 {
			t.Errorf("call %d: productions=%d validations=%d error=%v", i, calls, validations, err)
			return
		}
		generations[i] = generation
	}
	var wg sync.WaitGroup
	wg.Go(func() { run(0) })
	<-entered
	wg.Go(func() { run(1) })
	synctest.Wait()
	close(release)
	wg.Wait()
	for i, generation := range generations {
		if generation == nil {
			continue
		}
		data, err := fs.ReadFile(generation, "content")
		if err != nil || string(data) != payloads[i] {
			t.Errorf("call %d: payload=%q want=%q error=%v", i, data, payloads[i], err)
		}
	}
	{
		generation, _, _, err := (*testCaseInput.c).Blob(
			t.Context(),
			key,
			Policy{MaxAge: Forever},
			nil,
		)
		assert.FatalIf(t, generation != nil ||
			!errors.Is(err, errdef.ErrNotFound), "reused rejected generation: %v", err)
	}
	if (*testCaseInput.root) != "" {
		assert.NoError(t, (*testCaseInput.c).Close())
		(*testCaseInput.c) = cachetest.New(t, (*testCaseInput.root), New)
		{
			generation, _, _, err := (*testCaseInput.c).Blob(
				t.Context(),
				key,
				Policy{MaxAge: Forever},
				nil,
			)
			assert.FatalIf(t, generation != nil ||
				!errors.Is(err, errdef.ErrNotFound), "persisted rejected generation: %v", err)
		}
	}
}

type testBlobRepairFailureCleanupCaseCallbackInputs struct {
	t        **testing.T
	failure  **bool
	root     *string
	c        **Cache
	payload  *[]byte
	names    *[]string
	releases *int
}

func testBlobRepairFailureCleanupCaseCallback(
	_ context.Context,
	w ProducerInput,
	testCaseInput testBlobRepairFailureCleanupCaseCallbackInputs,
) (ProducerResult, error) {
	w.payload.discard = func() error {
		if (*testCaseInput.releases) != 0 {
			(*testCaseInput.t).
				Error("admission released before staged cleanup")
		}
		entries, readDirErr := os.ReadDir(filepath.Join((*testCaseInput.root), ociDir))
		assert.NoError((*testCaseInput.t), readDirErr)

		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "acquire-") {
				(*testCaseInput.names) = append((*testCaseInput.names), filepath.Join(ociDir, entry.Name()))
			}
		}
		assert.FatalIf((*testCaseInput.t), len((*testCaseInput.names)) != 2,
			"acquisition names before cleanup=%v, want two hard links", (*testCaseInput.names),
		)
		first, readDirErr := (*testCaseInput.c).root.Stat((*testCaseInput.names)[0])
		assert.NoError((*testCaseInput.t), readDirErr)
		second, readDirErr := (*testCaseInput.c).root.Stat((*testCaseInput.names)[1])
		assert.NoError((*testCaseInput.t), readDirErr)
		assert.FatalIf(
			(*testCaseInput.t),
			!os.SameFile(first, second) || first.Size() != int64(len((*testCaseInput.payload))),
			"%s",
			"acquisition names must hard-link the staged payload",
		)

		if *(*testCaseInput.failure) {
			return (*testCaseInput.c).root.Close()
		}
		return nil
	}
	_, writeErr2 := w.Write((*testCaseInput.payload))
	return ProducerResult{}, writeErr2
}

type testBlobRepairFailureCleanupCaseCompletionInputs struct {
	failure **bool
	err     *error
	names   *[]string
	c       **Cache
	path    *string
	before  *os.FileInfo
}
