package oci

import (
	"bytes"
	"errors"
	"io/fs"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"helm.sh/helm/v4/pkg/registry"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func TestFetcher_LayerSelectionAndIgnoreFlavor(t *testing.T) {
	t.Parallel()
	art := newFakeOCIArtifact(t, map[string]string{"file.yaml": "x", ".git/config": "git", "image.png": "image"})
	second := newFakeOCIArtifact(t, map[string]string{"file.yaml": "second"})
	archive := manifestBlob{data: art.layer, mediaType: registry.ChartLayerMediaType}
	other := manifestBlob{data: second.layer, mediaType: registry.ChartLayerMediaType}
	prov := manifestBlob{data: []byte("plain provenance"), mediaType: registry.ProvLayerMediaType}
	for _, tc := range []struct {
		name     string
		layers   []manifestBlob
		selector *sourcev1.OCILayerSelector
		digest   bool
		wantCopy []byte
		wantErr  bool
	}{
		{name: "defaults", layers: []manifestBlob{archive}},
		{name: "digest selectors", layers: []manifestBlob{archive}, digest: true},
		{name: "unmatched extract", layers: []manifestBlob{archive}, selector: &sourcev1.OCILayerSelector{MediaType: "unmatched/type"}, wantErr: true},
		{name: "unmatched copy", layers: []manifestBlob{archive}, selector: &sourcev1.OCILayerSelector{MediaType: "unmatched/type", Operation: sourcev1.OCILayerCopy}, wantErr: true},
		{name: "provenance only extract", layers: []manifestBlob{prov}, wantErr: true},
		{name: "provenance only copy", layers: []manifestBlob{prov}, selector: &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy}, wantCopy: prov.data},
		{name: "provenance first extract", layers: []manifestBlob{prov, archive}, wantErr: true},
		{name: "provenance first copy", layers: []manifestBlob{prov, archive}, selector: &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy}, wantCopy: prov.data},
		{name: "empty media extract", layers: []manifestBlob{prov, archive}, selector: &sourcev1.OCILayerSelector{}, wantErr: true},
		{name: "empty media copy", layers: []manifestBlob{prov, archive}, selector: &sourcev1.OCILayerSelector{MediaType: "", Operation: sourcev1.OCILayerCopy}, wantCopy: prov.data},
		{name: "first matching extract", layers: []manifestBlob{prov, archive, other}, selector: &sourcev1.OCILayerSelector{MediaType: registry.ChartLayerMediaType}},
		{name: "first matching copy", layers: []manifestBlob{prov, archive, other}, selector: &sourcev1.OCILayerSelector{MediaType: registry.ChartLayerMediaType, Operation: sourcev1.OCILayerCopy}, wantCopy: art.layer},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherLayerSelectionAndIgnoreFlavorCase(
				t,
				testFetcherLayerSelectionAndIgnoreFlavorCaseInputs{art: &art, second: &second, prov: &prov, tc: &tc},
			)
		},
		)
	}
}

type testFetcherLayerSelectionAndIgnoreFlavorCaseInputs struct {
	art    *fakeOCIArtifact
	second *fakeOCIArtifact
	prov   *manifestBlob
	tc     *struct {
		name     string
		layers   []manifestBlob
		selector *sourcev1.
				OCILayerSelector
		digest   bool
		wantCopy []byte
		wantErr  bool
	}
}

func testFetcherLayerSelectionAndIgnoreFlavorCase(
	t *testing.T,
	testCaseInput testFetcherLayerSelectionAndIgnoreFlavorCaseInputs,
) {
	t.Helper()

	data := mustManifestJSON(
		t,
		manifestBlob{
			data:      testCaseInput.art.config,
			mediaType: "application/vnd.cncf.flux.config.v1+json",
		}, testCaseInput.tc.
			layers...)
	srv := startFakeRegistry(
		t,
		data,
		testCaseInput.art.config,
		testCaseInput.art.layer,
		testCaseInput.second.layer,
		testCaseInput.prov.data,
	)
	repo := &manifest.OCIRepository{
		URL:           "oci://" + mustURL(t, srv.URL).Host + "/artifact",
		Insecure:      true,
		Reference:     &sourcev1.OCIRepositoryRef{Tag: "latest"},
		LayerSelector: testCaseInput.tc.selector,
	}
	if testCaseInput.tc.digest {
		repo.Reference = &sourcev1.OCIRepositoryRef{Digest: sha256Digest(data)}
	}
	root := t.TempDir()
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, root, sourcecache.New)}
	var first *store.SourceArtifact
	for range 2 {
		got, err := f.Fetch(
			t.Context(),
			repo,
			(testInputs{}).readSet(repo),
			&sourcecache.Policy{MaxAge: repo.SourceInterval()},
		)
		if testCaseInput.tc.wantErr {
			assert.FatalIf(
				t,
				got != nil || !errors.Is(err, manifest.ErrFlux),
				"invalid selection: artifact=%v err=%v, want nil artifact and ErrFlux",
				got,
				err,
			)
			continue
		}
		assert.NoError(t, err)

		if testCaseInput.tc.wantCopy != nil {
			{
				data, err := fs.ReadFile(
					got.FS,
					source.OCICopiedLayerFilename,
				)
				assert.FatalIf(t, err != nil ||
					!bytes.Equal(
						data, testCaseInput.tc.wantCopy,
					), "copy changed selected layer: data=%q err=%v", data, err)
			}
		} else {
			for _, name := range []string{".git/config", "image.png"} {
				{
					_, err := fs.Stat(got.FS, name)
					assert.FatalIf(
						t,
						!errors.Is(err, fs.ErrNotExist),
						"default ignore retained %s: %v",
						name,
						err,
					)
				}
			}
			{
				got := mustReadFile(t, got.FS, "file.yaml")
				assert.FatalIf(t, got != "x", "content=%q", got)
			}
		}
		if first != nil {
			before, after := *first, *got
			before.FS, after.FS = nil, nil
			{
				diff := cmp.Diff(
					before,
					after,
					cmpopts.IgnoreFields(store.SourceArtifact{}, "material"),
				)
				assert.ErrorIf(t, diff != "", "acquired artifact mismatch (-want +got):\n%s", diff)
			}
		}
		first = got
	}
	if !testCaseInput.tc.digest {
		return
	}
	copyRepo := *repo
	copyRepo.LayerSelector = &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy}
	copied, err := f.Fetch(
		t.Context(),
		&copyRepo,
		(testInputs{}).readSet(&copyRepo),
		&sourcecache.Policy{MaxAge: copyRepo.SourceInterval()},
	)
	assert.NoError(t, err)

	{
		data, err := fs.ReadFile(
			copied.FS,
			source.OCICopiedLayerFilename,
		)
		assert.FatalIf(t, err != nil ||
			!bytes.Equal(data, testCaseInput.art.layer), "copy artifact changed layer bytes: %v", err)
	}
	if copied.Key == first.Key {
		t.Fatalf("different layer selectors reused cache key %q", first.Key)
	}
	{
		err := f.Cache.Close()
		assert.NoError(t, err)
	}

	f.Cache = cachetest.New(t, root, sourcecache.New)
	for range 2 {
		warm, err := f.Fetch(
			t.Context(),
			&copyRepo,
			(testInputs{}).readSet(&copyRepo),
			&sourcecache.Policy{MaxAge: copyRepo.SourceInterval()},
		)
		assert.NoError(t, err)

		{
			data, err := fs.ReadFile(
				warm.FS,
				source.OCICopiedLayerFilename,
			)
			assert.FatalIf(t, err != nil ||
				!bytes.Equal(data, testCaseInput.art.layer), "warm copy changed layer bytes: %v", err)
		}
		before, after := *copied, *warm
		before.FS, after.FS = nil, nil
		{
			diff := cmp.Diff(
				before,
				after,
				cmpopts.IgnoreFields(store.SourceArtifact{}, "material"),
			)
			assert.ErrorIf(t, diff != "", "acquired artifact mismatch (-want +got):\n%s", diff)
		}
	}
}
