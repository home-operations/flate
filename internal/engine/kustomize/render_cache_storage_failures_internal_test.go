package kustomize

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/opencontainers/go-digest"
	"oras.land/oras-go/v2/errdef"

	"github.com/home-operations/flate/internal/engine/controllers/base"
	sourcecache "github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/manifest"
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
	spec := &manifest.Kustomization{Name: "demo", Namespace: "flux-system", Path: "./"}

	persistent := sourcecache.RenderPersistenceDisabledReason() == ""
	root := testutil.WriteTree(
		t,
		map[string]string{"kustomization.yaml": existingKustomization("cm.yaml"), "cm.yaml": cmYAML("fresh")},
	)

	cacheDir := t.TempDir()
	cache := newTestTreeCache(t, cacheDir)
	req := RenderRequest{Source: localSource(t, root), Label: root, SubPath: ".", Spec: spec}
	publish := func(docs []manifest.DocOccurrence, receipt *manifest.RenderReceipt) error {
		t.Helper()
		st := store.New()
		st.AddObject(spec)
		st.SetArtifact(spec.EffectiveSourceID(), req.Source)
		controller := base.New(st, task.NewBounded(2))
		execution := controllertest.NewExecution(controller, spec.Named())
		defer execution.Retire()
		base.PrepareDocuments(spec, docs)
		shaping := PrepareShape(&req, docs)
		defer shaping.Retire()
		content := shaping.Content()
		parsed := execution.ParseChildren(spec, docs, manifest.ParseDocOptions{})
		emission := &store.Emission{
			Children: parsed.Children, Manifests: parsed.Visible, Failures: parsed.Failures,
			Fingerprint: manifest.Fingerprint(content), Complete: true, Render: receipt,
		}
		execution.Reads.Bind(emission)
		assert.NoError(t, controllertest.Emit(t.Context(), controller, spec.Named(), emission))
		records, sources := emission.Footprint(), emission.SourcePaths()
		assert.Equal(t, len(emission.Children), 1)
		assert.Equal(t, records != nil && sources != nil, true)
		return cache.owner.RecordRender(t.Context(), sourcecache.RenderAcceptance{
			Owner:   spec.Named(),
			Receipt: receipt,
			Records: records,
			Sources: sources,
			Content: content,
			Source: req.Source.HistoryIdentity(
				spec.EffectiveSourceID(),
			),
			Reads:       emission.Reads,
			OutputReads: emission.OutputReads,
		})
	}

	if (*testCaseInput) != "publication" {
		docs, receipt26, err := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
		if err == nil {
			recordErr := publish(docs, receipt26)
			assert.NoError(t, recordErr)
		}
		assert.NoError(t, err)
	}
	switch *testCaseInput {
	case "read":
		key := sourcecache.BlobKey{Content: renderKey(&req, root, ""), Render: sourcecache.RenderAccepted}
		reader, _, _, err := cache.owner.Blob(
			t.Context(),
			key,
			&sourcecache.Policy{MaxAge: sourcecache.Forever},
			nil,
		)
		if !persistent {
			assert.Equal(t, errors.Is(err, errdef.ErrNotFound), true)
			assert.Equal(t, reader == nil, true)
		} else {
			assert.NoError(t, err)
			data, err := fs.ReadFile(reader, "content")
			assert.NoError(t, err)

			sum := digest.FromBytes(data)
			blob := filepath.Join(cacheDir, "oci", "blobs", sum.Algorithm().String(), sum.Encoded())
			assert.NoError(t, os.Remove(blob))
			assert.NoError(t, os.Mkdir(blob, 0o750))
		}
	default:
		assert.NoError(t, os.RemoveAll(cacheDir))
	}
	docs, receipt, found := cache.PreviousRender(t.Context(), req)
	assert.Equal(t, docs == nil, true)
	assert.Equal(t, receipt, nil)
	assert.Equal(t, found, false)
	wantDocuments := 0
	if !persistent || (*testCaseInput) == "publication" {
		wantDocuments = 1
	}
	wantReadError := persistent && (*testCaseInput) == "read"
	for range 2 {
		docs, receipt27, renderErr := Render(t.Context(), cache, req, sourcecache.RetryConfig{})
		out, _, err := testutil.RenderMaps(docs, receipt27, renderErr)
		if err == nil {
			err = publish(docs, receipt27)
		}

		if !persistent {
			assert.NoError(t, err)
			assert.Equal(t, len(out), 1)
			assert.Equal(t, hasRenderField(out, "name", "fresh"), true)
			continue
		}
		if wantReadError {
			pathErr, ok := errors.AsType[*fs.PathError](err)
			assert.FatalIf(
				t,
				!ok || pathErr.Op != "read",
				"read fault did not precede frame decoding: %v",
				err,
			)
		}
		assert.FatalIf(t, len(out) != wantDocuments || !errors.Is(err, manifest.ErrFlux) ||
			errors.Is(
				err,
				errdef.ErrNotFound,
			), "%s fault returned output %q, error %v", (*testCaseInput), out, err)
	}
}
