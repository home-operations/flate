package oci

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_Revalidation_SelectedTagMetadata(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{sourcev1.OCILayerExtract, sourcev1.OCILayerCopy} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			testFetcherRevalidationSelectedTagMetadataCase(
				t,
				&operation,
			)
		},
		)
	}
}

func testFetcherRevalidationSelectedTagMetadataCase(
	t *testing.T, operation *string,
) {
	t.Helper()

	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	var m ocispec.Manifest
	assert.NoError(t, json.Unmarshal(artifact.manifest, &m))
	m.Annotations = map[string]string{"content.annotation": "retained"}
	var err error
	artifact.manifest, err = json.Marshal(m)
	assert.NoError(t, err)
	artifact.manifestDigest = sha256Digest(artifact.manifest)
	registry := newMutableFakeRegistry(t, artifact)

	var newer atomic.Bool
	var tags, heads, manifests, layers atomic.Int32
	handler := registry.server.Config.Handler
	registry.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherRevalidationSelectedTagMetadataCaseCallback(
			w,
			r,
			testFetcherRevalidationSelectedTagMetadataCaseCallbackInputs{
				newer:     &newer,
				tags:      &tags,
				heads:     &heads,
				manifests: &manifests,
				layers:    &layers,
				handler:   &handler,
			},
		)
	},
	)
	registry.server.StartTLS()
	root := t.TempDir()
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, root, sourcecache.New)}
	repo := &manifest.OCIRepository{
		Name: "metadata", Namespace: "test",
		URL: "oci://" + mustURL(t, registry.server.URL).Host + "/app", Insecure: true,
		Reference:     &sourcev1.OCIRepositoryRef{SemVer: ">=1.0.0"},
		LayerSelector: &sourcev1.OCILayerSelector{Operation: (*operation)},
	}
	first, err := f.Fetch(
		t.Context(),
		repo,
		(testInputs{}).readSet(repo),
		&sourcecache.Policy{MaxAge: repo.SourceInterval()},
	)
	assert.NoError(t, err)
	firstMetadata := map[string]string{
		"content.annotation": "retained",
		"flate.revision":     "v1.0.0@" + artifact.manifestDigest,
		"flate.resolved-tag": "v1.0.0",
	}
	assert.Diff(t, first.Metadata, firstMetadata)
	newer.Store(true)
	for _, phase := range []string{"newer tag", "reopened"} {
		if phase == "reopened" {
			assert.NoError(t, f.Cache.Close())
			f.Cache = cachetest.New(t, root, sourcecache.New)
		}
		beforeTags, beforeHeads, beforeManifests, beforeLayers := tags.Load(), heads.Load(), manifests.Load(), layers.Load()
		got, err := f.Fetch(
			t.Context(),
			repo,
			(testInputs{}).readSet(repo),
			&sourcecache.Policy{MaxAge: repo.SourceInterval()},
		)
		assert.NoError(t, err)
		if tags.Load()-beforeTags != 1 || heads.Load()-beforeHeads != 1 ||
			manifests.Load() != beforeManifests ||
			layers.Load() != beforeLayers {
			t.Fatalf(
				"%s: tag lists/HEAD/manifest GET/layer GET=%d/%d/%d/%d",
				phase,
				tags.Load()-beforeTags,
				heads.Load()-beforeHeads,
				manifests.Load()-beforeManifests,
				layers.Load()-beforeLayers,
			)
		}
		assert.Equal(t, got.Key, first.Key)
		assert.Equal(t, sourcecache.AcquisitionIdentity(got.FS), sourcecache.AcquisitionIdentity(first.FS))
		assert.FatalIf(t, first.Content == "", "%s", "acquired content has no generation")

		assert.Equal(t, first.Content, sourcecache.AcquisitionIdentity(first.FS))
		assert.Equal(t, got.Content, first.Content)
		assert.Equal(t, got.Digest, first.Digest)
		assert.Equal(t, got.Size, first.Size)
		assert.Equal(t, got.Revision, "v1.1.0@"+artifact.manifestDigest)
		assert.Diff(
			t,
			got.Metadata,
			map[string]string{
				"content.annotation": "retained",
				"flate.revision":     got.Revision,
				"flate.resolved-tag": "v1.1.0",
			},
		)
		assert.Diff(t, first.Metadata, firstMetadata)
		if (*operation) == sourcev1.OCILayerExtract {
			assert.Equal(t, mustReadFile(t, got.FS, "app.yaml"), "version: v1\n")
		} else {
			body, err := fs.ReadFile(got.FS, source.OCICopiedLayerFilename)
			assert.NoError(t, err)
			assert.Diff(t, body, artifact.layer)
		}
	}
}

type testFetcherRevalidationSelectedTagMetadataCaseCallbackInputs struct {
	newer     *atomic.Bool
	tags      *atomic.Int32
	heads     *atomic.Int32
	manifests *atomic.Int32
	layers    *atomic.Int32
	handler   *http.Handler
}

func testFetcherRevalidationSelectedTagMetadataCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherRevalidationSelectedTagMetadataCaseCallbackInputs,
) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/tags/list"):
		testCaseInput.tags.
			Add(1)
		available := []string{"v1.0.0"}
		if testCaseInput.newer.Load() {
			available = append(available, "v1.1.0")
		}
		w.Header().Set("Content-Type", "application/json")
		//nolint:errchkjson // Fixture payloads are encodable; client cancellation may fail their response writes.
		_ = json.NewEncoder(w).Encode(map[string]any{"name": "app", "tags": available})
		return
	case strings.Contains(r.URL.Path, "/manifests/") && r.Method == http.MethodHead:
		testCaseInput.heads.
			Add(1)
	case strings.Contains(r.URL.Path, "/manifests/"):
		testCaseInput.manifests.
			Add(1)
	case strings.Contains(r.URL.Path, "/blobs/"):
		testCaseInput.layers.
			Add(1)
	}
	(*testCaseInput.handler).
		ServeHTTP(w, r)
}
