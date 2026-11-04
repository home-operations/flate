package cache

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

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
			//nolint:errorlint // Exact replay node types and wrapper positions are part of this assertion.
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
