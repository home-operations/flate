package oci

import (
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

//nolint:paralleltest,tparallel // Children update shared acquisition identities before their parent advances source content.
func TestFetcher_CacheIsolatesSources(t *testing.T) {
	t.Parallel()

	c := cachetest.New(t, t.TempDir(), sourcecache.New)
	f := &Fetcher{Network: newTestNetwork(t), Cache: c}
	var keys []string
	for _, body := range []string{"first", "second"} {
		v := newFakeOCIArtifact(t, map[string]string{"app.yaml": body})
		srv := startFakeRegistry(t, v.manifest, v.config, v.layer)
		repo := &manifest.OCIRepository{
			URL:       "oci://" + mustURL(t, srv.URL).Host + "/app",
			Insecure:  true,
			Reference: &sourcev1.OCIRepositoryRef{Digest: v.manifestDigest},
		}
		art, err := f.Fetch(
			t.Context(),
			repo,
			(testInputs{}).readSet(repo),
			sourcecache.Policy{MaxAge: repo.SourceInterval()},
		)
		assert.NoError(t, err)

		{
			got := mustReadFile(t, art.FS, "app.yaml")
			assert.FatalIf(t, got != body, "source collision: %q, want %q", got, body)
		}
		keys = append(keys, art.Key)
		if body != "first" {
			continue
		}
		request, err := (&operation{Fetcher: f}).newRepoClient(t.Context(), new(repo.RegistryAccess()))
		assert.NoError(t, err)

		identities := map[string]string{art.Key: "original"}
		for _, tc := range []struct {
			name   string
			change func(*manifest.OCIRepository, *Fetcher)
		}{
			{"ref", func(r *manifest.OCIRepository, _ *Fetcher) { r.Reference = &sourcev1.OCIRepositoryRef{Tag: "latest"} }},
			{"ignore", func(r *manifest.OCIRepository, _ *Fetcher) { r.Ignore = new("*.tmp") }},
			{"insecure", func(r *manifest.OCIRepository, _ *Fetcher) { r.Insecure = false }},
			{"layer selector", func(r *manifest.OCIRepository, _ *Fetcher) {
				r.LayerSelector = &manifest.OCILayerSelector{Operation: sourcev1.OCILayerExtract}
			}},
			{"registry config", func(_ *manifest.OCIRepository, f *Fetcher) { f.RegistryConfig = "other-config.json" }},
			{"secret", func(r *manifest.OCIRepository, _ *Fetcher) {
				r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
			}},
			{"auth namespace", func(r *manifest.OCIRepository, _ *Fetcher) {
				r.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
				r.Namespace = "other"
			}},
			{"cert", func(r *manifest.OCIRepository, _ *Fetcher) {
				r.CertSecretRef = &manifest.LocalObjectReference{Name: "cert"}
			}},
			{"proxy", func(r *manifest.OCIRepository, _ *Fetcher) {
				r.ProxySecretRef = &manifest.LocalObjectReference{Name: "proxy"}
			}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				testFetcherCacheIsolatesSourcesCase(
					t,
					testFetcherCacheIsolatesSourcesCaseInputs{
						c:          &c,
						body:       &body,
						repo:       &repo,
						request:    &request,
						identities: &identities,
						tc:         &tc,
					},
				)
			},
			)
		}
	}
	assert.FatalIf(t, keys[0] == keys[1], "%s", "distinct sources share their published identity")
}

type testFetcherCacheIsolatesSourcesCaseInputs struct {
	c          **sourcecache.Cache
	body       *string
	repo       **manifest.OCIRepository
	request    *registryAcquisition
	identities *map[string]string
	tc         *struct {
		name   string
		change func(*manifest.OCIRepository, *Fetcher)
	}
}

func testFetcherCacheIsolatesSourcesCase(t *testing.T, testCaseInput testFetcherCacheIsolatesSourcesCaseInputs) {
	t.Helper()
	current := *(*testCaseInput.repo)
	fetcher := &Fetcher{Network: newTestNetwork(t), Cache: (*testCaseInput.c)}
	testCaseInput.tc.
		change(&current, fetcher)
	got, err := (&operation{Fetcher: fetcher}).fetchLayer(
		t.Context(),
		&current, new((*testCaseInput.request)), sourcecache.Policy{MaxAge: current.SourceInterval()},
	)
	assert.NoError(t, err)

	if previous, ok := (*testCaseInput.identities)[got.Key]; ok {
		t.Fatalf("%s reused %s acquisition", testCaseInput.tc.name, previous)
	}
	(*testCaseInput.identities)[got.Key] = testCaseInput.tc.name
	{
		got := mustReadFile(t, got.FS, "app.yaml")
		assert.FatalIf(t, got != (*testCaseInput.body), "variant content=%q, want %q", got, (*testCaseInput.body))
	}
}
