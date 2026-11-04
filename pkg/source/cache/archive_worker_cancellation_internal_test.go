package cache

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

func testAcquireCancelAfterArchiveWorkersCase(
	t *testing.T, backing *string,
	result *string,
) {
	t.Helper()

	root := ""
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	c := cachetest.New(t, root, New)
	data := orderedArchive(t, "file", "same")
	want := ocispec.Descriptor{
		Digest:      digest.FromString("upstream"),
		Size:        123,
		Annotations: map[string]string{"flate.revision": "original"},
	}
	keys := []ArchiveKey{{Content: "completion-one"}, {Content: "completion-two"}}
	ids, pins := make([]string, len(keys)), make([]string, len(keys))
	previous := make([]struct {
		resolved time.Time
		stored   ocispec.Descriptor
	}, len(keys))
	for i, key := range keys {
		input, _, id, err := c.Acquire(t.Context(), key, Policy{}, producer(data, &want))
		assert.NoError(t, err)
		ids[i], pins[i] = id, AcquisitionIdentity(input)
		v, _ := c.entries.Load(id)
		e := v.(*entry)
		previous[i].resolved, previous[i].stored = e.resolved, e.stored
	}
	synctest.Test(t, func(t *testing.T) {
		testAcquireCancelAfterArchiveWorkersCaseCallback(
			t,
			testAcquireCancelAfterArchiveWorkersCaseCallbackInputs{
				result: &result,
				c:      &c,
				data:   &data,
				want:   &want,
				keys:   &keys,
			},
		)
	},
	)
	unexpected := func(context.Context, ProducerInput) (ProducerResult, error) {
		t.Error("retained generation called producer")
		return ProducerResult{}, errors.New("unexpected refetch")
	}
	for i, key := range keys {
		v, _ := c.entries.Load(ids[i])
		e := v.(*entry)
		if !e.resolved.Equal(previous[i].resolved) || e.outcome != nil ||
			cmp.Diff(previous[i].stored, e.stored) != "" {
			t.Errorf(
				"canceled attempt replaced original generation: resolved=%v, want %v",
				e.resolved,
				previous[i].resolved,
			)
		}
		if (*backing) == "disk" {
			stored, err := c.store.Resolve(t.Context(), ids[i])
			assert.NoError(t, err)
			if diff := cmp.Diff(previous[i].stored, stored); diff != "" {
				t.Error(diff)
			}
			payload, err := c.root.ReadFile(blobPath(stored.Digest))
			assert.FatalIf(
				t,
				err != nil || !bytes.Equal(payload, data),
				"retained archive payload: %v",
				err,
			)
		}
		input, desc, id, err := c.Acquire(t.Context(), key, Policy{MaxAge: Forever}, unexpected)
		assert.FatalIf(t, err != nil || id != ids[i] || AcquisitionIdentity(input) != pins[i] ||
			cmp.Diff(want, desc) != "", "retained acquisition: identity=%q, error=%v", id, err)
		{
			content, err := fs.ReadFile(input, "file")
			assert.FatalIf(
				t,
				err != nil || string(content) != "same",
				"retained content: %q, %v",
				content,
				err,
			)
		}
	}
	testAcquireCancelAfterArchiveWorkersCaseCompletion(
		t,
		&testAcquireCancelAfterArchiveWorkersCaseCompletionInputs{
			backing:    &backing,
			c:          &c,
			root:       &root,
			keys:       &keys,
			unexpected: &unexpected,
			ids:        &ids,
			pins:       &pins,
			previous:   &previous,
			want:       &want,
		},
	)
}

func testAcquireCancelAfterArchiveWorkersCaseCompletion(
	t *testing.T,
	captures *testAcquireCancelAfterArchiveWorkersCaseCompletionInputs,
) {
	t.Helper()
	if (*(*captures.backing)) == "disk" {
		assert.NoError(t, (*captures.c).Close())
		(*captures.c) = cachetest.New(t, (*captures.root), New)
	}
	for i, key := range *captures.keys {
		input, _, id, err := (*captures.c).Acquire(t.Context(), key, Policy{MaxAge: Forever}, (*captures.unexpected))
		assert.NoError(t, err)
		assert.FatalIf(
			t,
			id != (*captures.ids)[i] || AcquisitionIdentity(input) != (*captures.pins)[i],
			"%s",
			"reopened generation changed identity",
		)

		v, _ := (*captures.c).entries.Load(id)
		assert.FatalIf(
			t,
			!v.(*entry).resolved.Equal((*captures.previous)[i].resolved),
			"%s",
			"canceled attempt renewed resolution time",
		)

		input, _, id, err = (*captures.c).Acquire(
			t.Context(),
			key,
			Policy{},
			func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
				prior, previousErr := w.Previous(ctx)
				if previousErr != nil || prior == nil || cmp.Diff((*captures.want), *prior) != "" {
					t.Errorf("forced refresh lost previous descriptor: %+v, %v", prior, previousErr)
					return ProducerResult{}, manifest.ErrInput
				}
				return ProducerResult{Descriptor: *prior, Unchanged: true}, nil
			},
		)
		assert.FatalIf(
			t,
			err != nil || id != (*captures.ids)[i] || AcquisitionIdentity(input) != (*captures.pins)[i],
			"forced refresh: identity=%q, error=%v",
			id,
			err,
		)
		{
			content, err := fs.ReadFile(input, "file")
			assert.FatalIf(
				t,
				err != nil || string(content) != "same",
				"forced refresh content: %q, %v",
				content,
				err,
			)
		}
	}
}

type testAcquireCancelAfterArchiveWorkersCaseCallbackInputs struct {
	result **string
	c      **Cache
	data   *[]byte
	want   *ocispec.Descriptor
	keys   *[]ArchiveKey
}

func testAcquireCancelAfterArchiveWorkersCaseCallback(
	t *testing.T,
	testCaseInput testAcquireCancelAfterArchiveWorkersCaseCallbackInputs,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, produce := make(chan struct{}), make(chan struct{})
	done := make(chan struct{}, len((*testCaseInput.keys)))
	callbacks := make([]*archiveCompletionContext, len((*testCaseInput.keys)))
	for i, key := range *testCaseInput.keys {
		callback := &archiveCompletionContext{
			Context: ctx,
			entered: make(chan struct{}),
			release: make(chan struct{}),
		}
		callbacks[i] = callback
		go func() {
			input, _, id, err := (*testCaseInput.c).Acquire(
				callback,
				key,
				Policy{},
				func(ctx context.Context, w ProducerInput) (ProducerResult, error) {
					prior, err := w.Previous(ctx)
					assert.ErrorIf(
						t,
						err != nil || prior == nil || cmp.Diff((*testCaseInput.want), *prior) != "",
						"previous descriptor: %+v, %v",
						prior,
						err,
					)
					child := ctx.Done()
					callback.child.Store(&child)
					started <- struct{}{}
					<-produce
					if (*(*testCaseInput.result)) == "fresh" {
						return producer((*testCaseInput.data), testCaseInput.want)(ctx, w)
					}
					return ProducerResult{Descriptor: (*testCaseInput.want), Unchanged: true}, nil
				},
			)
			if input != nil || id != "" || !errors.Is(err, context.Canceled) ||
				!errors.Is(err, manifest.ErrFlux) {
				var contents []byte
				if input != nil {
					contents, _ = fs.ReadFile(input, "file")
				}
				t.Errorf(
					"post-worker cancellation: filesystem=%v identity=%q contents=%q error=%v",
					input != nil,
					id,
					contents,
					err,
				)
			}
			done <- struct{}{}
		}()
	}
	for range *testCaseInput.keys {
		<-started
	}
	close(produce)
	synctest.Wait()
	for _, callback := range callbacks {
		select {
		case <-callback.entered:
		default:
			t.Error("post-worker cancellation checkpoint unreachable")
		}
	}
	cancel()
	for _, callback := range callbacks {
		close(callback.release)
	}
	for range *testCaseInput.keys {
		<-done
	}
}

type testAcquireCancelAfterArchiveWorkersCaseCompletionInputs struct {
	backing    **string
	c          **Cache
	root       *string
	keys       *[]ArchiveKey
	unexpected *func(context.Context, ProducerInput) (ProducerResult, error)
	ids        *[]string
	pins       *[]string
	previous   *[]struct {
		resolved time.Time
		stored   ocispec.Descriptor
	}
	want *ocispec.Descriptor
}
