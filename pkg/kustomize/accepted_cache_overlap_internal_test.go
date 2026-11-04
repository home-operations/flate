package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

//nolint:paralleltest // The input-read barrier MUST not queue behind other users of the process-global krusty BuildMutex.
func TestRender_AcceptedCacheAcquisitionOverlap(t *testing.T) {
	for _, validate := range []bool{false, true} {
		t.Run(fmt.Sprintf("validation_%t", validate), func(t *testing.T) {
			testRenderAcceptedCacheAcquisitionOverlapCase(
				t,
				&validate,
			)
		},

		// Release the filesystem only after all acquisitions completed.

		)
	}
}

func testRenderAcceptedCacheAcquisitionOverlapCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()
	eligible := renderPersistenceEligible(t)
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("fresh")},
	)
	cache := newTestTreeCache(t, t.TempDir())
	key := sourcecache.BlobKey{
		Content: renderKey(
			&RenderRequest{Source: &store.SourceArtifact{}, Spec: demoSpec, SubPath: "."},
			root,
			nil,
		),
		Render: sourcecache.RenderAccepted,
	}
	frame := testRenderFrame(
		t,
		new(readSetSnapshot{
			Files: []localEntry{{Path: "cm.yaml", Hash: manifest.SHA256Hex([]byte(cmYAML("fresh")))}},
		}),
		testRenderPayload(t, "VALIDATED"),
	)
	if *testCaseInput {
		publishTestFrame(t, cache.owner, key, frame)
	}
	lower := &pausedRenderFS{FS: os.DirFS(root), entered: make(chan struct{}), release: make(chan struct{})}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	released := false
	defer func() {
		if !released {
			close(lower.release)
		}
		wg.Wait()
	}()
	rendered := make(chan error, 1)
	wg.Go(func() {
		out, receipt25, err := testutil.RenderMaps(
			Render(
				ctx,
				cache,
				RenderRequest{
					Source:  &store.SourceArtifact{FS: lower},
					Label:   root,
					SubPath: ".",
					Spec:    demoSpec,
				},
				sourcecache.RetryConfig{},
			),
		)
		if err == nil {
			{
				recordErr := cache.RecordRender(
					ctx,
					RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt25},
				)
				assert.NoError(t, recordErr)
			}
		}

		if err == nil && (*testCaseInput) && eligible && !hasRenderField(out, "witness", "VALIDATED") {
			err = fmt.Errorf("validation did not return its acquired frame: %q", out)
		}
		if err == nil && (!(*testCaseInput) || !eligible) && !hasRenderField(out, "name", "fresh") {
			err = fmt.Errorf("real render = %q", out)
		}
		rendered <- err
	})
	select {
	case <-lower.entered:
	case <-ctx.Done():
		t.Fatal("render did not reach input-read barrier")
	}
	progress := make(chan error, 1)
	wg.Go(func() {
		progress <- acquireAcceptedOverlapSources(ctx, &acceptedOverlapAcquisitionInputs{
			cache: &cache, key: &key, frame: &frame,
			scenario: &testCaseInput, eligible: &eligible,
		})
	})
	select {
	case err := <-progress:
		{
			assert.NoError(t, err)
		}
	case <-ctx.Done():
		t.Fatal("same-frame lookup/publication or source acquisition blocked behind input read")
	}

	close(lower.release)
	released = true
	select {
	case err := <-rendered:
		{
			assert.NoError(t, err)
		}
	case <-ctx.Done():
		t.Fatal("render did not finish")
	}
}

type acceptedOverlapAcquisitionInputs struct {
	cache    **TreeCache
	key      *sourcecache.BlobKey
	frame    *[]byte
	scenario **bool
	eligible *bool
}

func acquireAcceptedOverlapSources(ctx context.Context, inputs *acceptedOverlapAcquisitionInputs) error {
	_, _, _, err := (*inputs.cache).owner.Blob(ctx, (*inputs.key), sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
	if (*(*inputs.scenario)) && (*inputs.eligible) && err != nil {
		return err
	}
	if (!(*(*inputs.scenario)) || !(*inputs.eligible)) && !errors.Is(err, errdef.ErrNotFound) {
		return fmt.Errorf("lookup before completed publication: %w", err)
	}
	_, _, _, err = (*inputs.cache).owner.Blob(
		ctx,
		(*inputs.key),
		sourcecache.Policy{MaxAge: 0},
		func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			_, writeErr := w.Write((*inputs.frame))
			return sourcecache.ProducerResult{}, writeErr
		},
	)
	if err != nil {
		return err
	}
	_, _, _, err = (*inputs.cache).owner.Blob(ctx, sourcecache.BlobKey{Content: struct {
		Name string `json:"Name"`
	}{"ordinary-source"}}, sourcecache.Policy{MaxAge: 0}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
		_, writeStringErr := io.WriteString(w, "source")
		return sourcecache.ProducerResult{}, writeStringErr
	})
	if err != nil {
		return err
	}
	return nil
}
