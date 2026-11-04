package cache

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
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
			&Policy{MaxAge: Forever},
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
			&Policy{MaxAge: Forever},
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
	r, _, _, err := (*testCaseInput.c).Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
	assert.FatalIf(t, r != nil || !errors.Is(err, failure), "validator failure: %v", err)
	key.Validate = func(context.Context, fs.FS) (bool, error) { return false, nil }
	r, _, _, err = (*testCaseInput.c).Blob(t.Context(), key, &Policy{MaxAge: Forever}, nil)
	assert.FatalIf(t, r != nil || !errors.Is(err, errdef.ErrNotFound), "invalid lookup: %v", err)
}
