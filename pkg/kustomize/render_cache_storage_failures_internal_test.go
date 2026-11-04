package kustomize

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/opencontainers/go-digest"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestRender_CacheStorageFailures(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"lookup", "read", "publication"} {
		t.Run(fault, func(t *testing.T) {
			t.Parallel()
			testRenderCacheStorageFailuresCase(t, &fault)
		},
		)
	}
}

func testRenderCacheStorageFailuresCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	if !renderPersistenceEligible(t) {
		t.Skip("storage faults require an eligible build")
	}
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("fresh")},
	)

	cacheDir := t.TempDir()
	cache := newTestTreeCache(t, cacheDir)
	req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: demoSpec}
	if (*testCaseInput) == "lookup" || (*testCaseInput) == "read" {
		_, receipt26, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
		if err == nil {
			{
				recordErr := cache.RecordRender(
					t.Context(),
					RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt26},
				)
				assert.NoError(t, recordErr)
			}
		}
		assert.NoError(t, err)
	}
	if (*testCaseInput) == "read" {
		key := sourcecache.BlobKey{Content: renderKey(&req, root, nil), Render: sourcecache.RenderAccepted}
		reader, _, _, err := cache.owner.Blob(
			t.Context(),
			key,
			&sourcecache.Policy{MaxAge: sourcecache.Forever},
			nil,
		)
		assert.NoError(t, err)
		data, err := fs.ReadFile(reader, "content")
		assert.NoError(t, err)

		sum := digest.FromBytes(data)
		blob := filepath.Join(cacheDir, "oci", "blobs", sum.Algorithm().String(), sum.Encoded())
		assert.NoError(t, os.Remove(blob))
		assert.NoError(t, os.Mkdir(blob, 0o750))
	} else {
		assert.NoError(t, os.RemoveAll(cacheDir))
	}
	for range 2 {
		out, receipt27, err := testutil.RenderMaps(Render(t.Context(), cache, req, sourcecache.RetryConfig{}))
		if err == nil {
			err = cache.RecordRender(t.Context(), RenderAcceptance{Owner: demoSpec.Named(), Receipt: receipt27})
		}

		if (*testCaseInput) == "read" {
			{
				pathErr, ok := errors.AsType[*fs.PathError](err)
				assert.FatalIf(
					t,
					!ok || pathErr.Op != "read",
					"read fault did not precede frame decoding: %v",
					err,
				)
			}
		}
		wantDocuments := 0
		if (*testCaseInput) == "publication" {
			wantDocuments = 1
		}
		assert.FatalIf(t, len(out) != wantDocuments || !errors.Is(err, manifest.ErrFlux) ||
			errors.Is(
				err,
				errdef.ErrNotFound,
			), "%s fault returned output %q, error %v", (*testCaseInput), out, err)
	}
}
