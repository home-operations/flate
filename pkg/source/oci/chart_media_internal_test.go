package oci

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"helm.sh/helm/v4/pkg/registry"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/helmchart"
)

func TestFetcher_ChartMediaSelector(t *testing.T) {
	t.Parallel()
	for _, media := range []string{registry.ChartLayerMediaType, "application/tar+gzip"} {
		t.Run(media, func(t *testing.T) {
			t.Parallel()
			art := newFakeOCIArtifact(t, map[string]string{"Chart.yaml": "apiVersion: v2\nname: app\nversion: 1.0.0\n"})
			art.config = []byte(`{"name":"app","version":"1.0.0","apiVersion":"v2"}`)
			data := mustManifestJSON(t,
				manifestBlob{data: art.config, mediaType: registry.ConfigMediaType},
				manifestBlob{data: art.layer, mediaType: media},
			)
			srv := startFakeRegistry(t, data, art.config, art.layer)
			repo := &manifest.OCIRepository{
				URL:       "oci://" + mustURL(t, srv.URL).Host + "/charts/app",
				Insecure:  true,
				Reference: &sourcev1.OCIRepositoryRef{Tag: "1.0.0"},
			}
			f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), sourcecache.New)}
			got, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{})
			assert.FatalIf(t, err != nil || got == nil, "default request: artifact=%v error=%v", got, err)
			{
				_, readFileErr := fs.ReadFile(got.FS, "Chart.yaml")
				assert.NoError(t, readFileErr)
			}

			explicit := *repo
			explicit.LayerSelector = &sourcev1.OCILayerSelector{MediaType: registry.ChartLayerMediaType}
			got, err = f.Fetch(t.Context(), &explicit, (testInputs{}).readSet(&explicit), sourcecache.Policy{})
			if media == registry.ChartLayerMediaType {
				assert.FatalIf(t, err != nil || got == nil, "explicit chart selector: artifact=%v error=%v", got, err)
			} else {
				assert.FatalIf(
					t,
					got != nil || !errors.Is(err, manifest.ErrFlux),
					"explicit chart selector must reject a different media type: artifact=%v error=%v",
					got,
					err,
				)
			}
		})
	}
}

func TestFetcher_ChartMediaAfterProvenance(t *testing.T) {
	t.Parallel()
	for _, media := range []string{registry.ChartLayerMediaType, "application/tar+gzip"} {
		t.Run(media, func(t *testing.T) {
			t.Parallel()
			testFetcherChartMediaAfterProvenanceCase(t, &media)
		},
		)
	}
}

func TestFetcher_ChartMediaCacheIsolation(t *testing.T) {
	t.Parallel()
	generic := newFakeOCIArtifact(t, map[string]string{"file.yaml": "generic"})
	chart := newFakeOCIArtifact(t, map[string]string{"file.yaml": "chart"})
	data := mustManifestJSON(
		t,
		manifestBlob{data: generic.config, mediaType: "application/vnd.cncf.flux.config.v1+json"},
		manifestBlob{data: generic.layer, mediaType: "application/vnd.cncf.flux.content.v1.tar+gzip"},
		manifestBlob{data: chart.layer, mediaType: registry.ChartLayerMediaType},
	)
	srv := startFakeRegistry(t, data, generic.config, generic.layer, chart.layer)
	repo := &manifest.OCIRepository{
		URL:       "oci://" + mustURL(t, srv.URL).Host + "/artifact",
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "latest"},
	}
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), sourcecache.New)}
	gotGeneric, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{})
	assert.NoError(t, err)

	gotChart, err := f.FetchChart(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{})
	assert.NoError(t, err)

	assert.Equal(t, mustReadFile(t, gotGeneric.FS, "file.yaml"), "generic")
	assert.Equal(t, mustReadFile(t, gotChart.FS, "file.yaml"), "chart")
}

func testFetcherChartMediaAfterProvenanceCase(
	t *testing.T, media *string,
) {
	t.Helper()

	art := newFakeOCIArtifact(t, map[string]string{"Chart.yaml": "apiVersion: v2\nname: app\nversion: 1.0.0\n"})
	art.config = []byte(`{"name":"app","version":"1.0.0","apiVersion":"v2"}`)
	data := mustManifestJSON(
		t,
		manifestBlob{data: art.config, mediaType: registry.ConfigMediaType},
		manifestBlob{data: art.layer, mediaType: (*media)},
	)
	prov := []byte("plain provenance")
	var m ocispec.Manifest
	{
		err := json.Unmarshal(data, &m)
		assert.NoError(t, err)
	}

	m.Layers = append(
		[]ocispec.Descriptor{
			{MediaType: registry.ProvLayerMediaType, Digest: digest.FromBytes(prov), Size: int64(len(prov))},
		},
		m.Layers...)
	data, err := json.Marshal(m)
	assert.NoError(t, err)

	upstream := startFakeRegistry(t, data, art.config, art.layer)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/blobs/"+digest.FromBytes(prov).String()) {
			_, _ = w.Write(prov)
			return
		}
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	url := "oci://" + mustURL(t, srv.URL).Host
	repo := &manifest.OCIRepository{
		Name:      "app",
		Namespace: "ns",
		URL:       url + "/charts/app",
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{Tag: "1.0.0"},
	}
	cache := cachetest.New(t, t.TempDir(), sourcecache.New)
	network := newTestNetwork(t)
	f := &Fetcher{Network: network, Cache: cache}
	got, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), sourcecache.Policy{})
	assert.ErrorIf(
		t,
		got != nil || !errors.Is(err, manifest.ErrFlux),
		"default OCI extract must fail on first provenance layer: artifact=%v error=%v",
		got,
		err,
	)
	copyRepo := *repo
	copyRepo.LayerSelector = &sourcev1.OCILayerSelector{Operation: sourcev1.OCILayerCopy}
	got, err = f.Fetch(t.Context(), &copyRepo, (testInputs{}).readSet(&copyRepo), sourcecache.Policy{})
	assert.NoError(t, err)

	data, err = fs.ReadFile(got.FS, source.OCICopiedLayerFilename)
	assert.ErrorIf(
		t,
		err != nil || !bytes.Equal(data, prov),
		"default OCI copy=%q error=%v; want exact provenance bytes",
		data,
		err,
	)
	r := &manifest.HelmRepository{
		Name:      "charts",
		Namespace: "ns",
		URL:       url + "/charts",
		Type:      sourcev1.HelmRepositoryTypeOCI,
		Insecure:  true,
	}
	hc := manifest.NewHelmChartSource(
		sourcev1.HelmChart{
			Name:      "app",
			Namespace: r.Namespace,
			Spec: sourcev1.HelmChartSpec{
				Chart:   "app",
				Version: "1.0.0",
				SourceRef: sourcev1.LocalHelmChartSourceReference{
					Kind: manifest.KindHelmRepository,
					Name: r.Name,
				},
			},
		},
	)
	reads := (testInputs{repos: func(string, string) *manifest.HelmRepository { return r }}).readSet(hc)
	charts := helmchart.New(f, helmchart.Config{Cache: cache, Network: network})
	chart, err := charts.Fetch(t.Context(), hc, reads, sourcecache.Policy{})
	assert.FatalIf(
		t,
		err != nil || chart == nil,
		"chart acquisition after same-spec OCI failure: artifact=%v error=%v",
		chart,
		err,
	)
	{
		got := mustReadFile(t, chart.FS, "Chart.yaml")
		assert.FatalIf(t, got != "apiVersion: v2\nname: app\nversion: 1.0.0\n", "chart content=%q", got)
	}
	if chart.Kind != manifest.KindHelmChart {
		t.Fatalf("chart kind=%s", chart.Kind)
	}
}
