package oci

import (
	"encoding/json"
	"errors"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"helm.sh/helm/v4/pkg/registry"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_EmptyArtifact(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{sourcev1.OCILayerCopy, sourcev1.OCILayerExtract} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			testFetcherEmptyArtifactCase(t, &operation)
		},
		)
	}
}

func testFetcherEmptyArtifactCase(t *testing.T, operation *string,
) {
	t.Helper()

	art := newFakeOCIArtifact(t, map[string]string{"file": "x"})
	var m ocispec.Manifest
	{
		err := json.Unmarshal(art.manifest, &m)
		assert.NoError(t, err)
	}

	m.Layers = nil
	data, err := json.Marshal(m)
	assert.NoError(t, err)

	srv := startFakeRegistry(t, data, art.config, art.layer)
	repo := &manifest.OCIRepository{
		URL:           "oci://" + mustURL(t, srv.URL).Host + "/app",
		Insecure:      true,
		Reference:     &sourcev1.OCIRepositoryRef{Tag: "1.0.0"},
		LayerSelector: &sourcev1.OCILayerSelector{Operation: (*operation)},
	}
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), sourcecache.New)}
	for _, media := range []string{"", registry.ChartLayerMediaType} {
		t.Run("media="+media, func(t *testing.T) {
			t.Parallel()
			selected := *repo
			selected.LayerSelector = &sourcev1.OCILayerSelector{Operation: (*operation), MediaType: media}
			for range 2 {
				got, err := f.Fetch(
					t.Context(),
					&selected,
					(testInputs{}).readSet(&selected),
					&sourcecache.Policy{},
				)
				assert.FatalIf(
					t,
					got != nil || !errors.Is(err, manifest.ErrFlux) || !errors.Is(err, manifest.ErrInput),
					"empty %s: artifact=%v error=%v; want nil artifact, ErrInput and ErrFlux",
					(*operation),
					got,
					err,
				)
			}
		})
	}
}
