package source_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/source"
	"github.com/home-operations/flate/internal/source/bucket"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/external"
	"github.com/home-operations/flate/internal/source/git"
	"github.com/home-operations/flate/internal/source/helmchart"
	"github.com/home-operations/flate/internal/source/oci"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type admittedCredentialCase struct {
	name, reference        string
	data                   map[string]any
	encoded                bool
	wantMissing, wantError bool
}

func TestFetcher_AdmittedContentCredentials(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { assert.Equal(t, requests.Load(), int64(0)) })
	host := strings.TrimPrefix(server.URL, "http://")
	credentials := map[string]any{
		"username":          "user",
		"password":          "password",
		"accesskey":         "user",
		"secretkey":         "password",
		".dockerconfigjson": `{"auths":{"` + host + `":{"username":"user","password":"password"}}}`,
	}
	wiped := map[string]any{}
	malformed := map[string]any{}
	for key := range credentials {
		wiped[key] = ""
		malformed[key] = "!"
	}
	malformed["bearerToken"] = "!"
	for _, kind := range []string{manifest.KindGitRepository, manifest.KindOCIRepository, manifest.KindBucket} {
		for _, tc := range []admittedCredentialCase{
			{name: "none"},
			{name: "auth", reference: "auth", data: credentials},
			{name: "missing_auth", reference: "auth", wantError: true, wantMissing: true},
			{name: "wiped_auth", reference: "auth", data: wiped, wantError: true, wantMissing: true},
			{name: "malformed_auth", reference: "auth", data: malformed, encoded: true, wantError: true},
			{name: "missing_cert", reference: "cert", wantError: true, wantMissing: true},
			{name: "wiped_cert", reference: "cert", data: map[string]any{"ca.crt": ""}, wantError: true, wantMissing: true},
			{name: "malformed_cert", reference: "cert", data: map[string]any{"ca.crt": "invalid certificate"}, wantError: true},
			{name: "missing_proxy", reference: "proxy", wantError: true, wantMissing: true},
			{name: "wiped_proxy", reference: "proxy", data: map[string]any{"address": ""}, wantError: true, wantMissing: true},
			{name: "malformed_proxy", reference: "proxy", data: map[string]any{"address": "://invalid proxy"}, wantError: true},
		} {
			t.Run(
				kind+"/"+tc.name,
				func(t *testing.T) { t.Parallel(); testAdmittedCredentials(t, server.URL, kind, &tc) },
			)
		}
	}
}

func testAdmittedCredentials(t *testing.T, endpoint, kind string, tc *admittedCredentialCase) {
	t.Helper()
	obj, fetcher := admittedCredentialSource(t, endpoint, kind, tc.reference)
	st := store.New()
	st.AddObject(obj)
	if tc.data != nil {
		secret := &manifest.Secret{Name: tc.reference, Namespace: "apps", StringData: tc.data}
		if tc.encoded {
			secret.StringData = nil
			secret.Data = tc.data
		}
		st.AddObject(secret)
	}
	var reads store.ReadSet
	reads.Init(st, obj.Named(), nil)
	defer reads.Retire()
	admitted := &store.SourceArtifact{InputAlias: true, FS: fstest.MapFS{}}
	got, err := fetchtest.Fetch(t.Context(), fetcher.Fetch, obj, &reads, &sourcecache.Policy{}, admitted)
	assert.Equal(t, err != nil, tc.wantError)
	assert.Equal(t, errors.Is(err, manifest.ErrMissingSecret), tc.wantMissing)
	if tc.wantError {
		assert.Equal(t, got, (*store.SourceArtifact)(nil))
		return
	}
	assert.Equal(t, got, admitted)
	if tc.reference != "" {
		st.AddObject(&manifest.Secret{Name: tc.reference, Namespace: "apps"})
		assert.Equal(t, reads.Valid(), false)
	}
}

//nolint:ireturn // The fixture selects a manifest and fetcher by source kind.
func admittedCredentialSource(t *testing.T, endpoint, kind, reference string) (manifest.BaseManifest, source.Fetcher) {
	t.Helper()
	network := &source.Network{}
	t.Cleanup(network.Close)
	spec := map[string]any{"url": endpoint, "endpoint": endpoint, "bucketName": "data"}
	switch reference {
	case "auth":
		spec["secretRef"] = map[string]any{"name": "auth"}
	case "cert":
		key := "certSecretRef"
		if kind == manifest.KindGitRepository {
			key = "secretRef"
		}
		spec[key] = map[string]any{"name": "cert"}
	case "proxy":
		spec["proxySecretRef"] = map[string]any{"name": "proxy"}
	}
	if kind == manifest.KindOCIRepository {
		spec["url"] = "oci://" + strings.TrimPrefix(endpoint, "http://") + "/repo"
	}
	parsed, err := manifest.ParseDoc(
		map[string]any{
			"apiVersion": manifest.SourceDomain + "/v1",
			"kind":       kind,
			"metadata":   map[string]any{"name": "repo", "namespace": "apps"},
			"spec":       spec,
		},
		manifest.ParseDocOptions{},
	)
	assert.NoError(t, err)
	switch kind {
	case manifest.KindGitRepository:
		return parsed.Object, source.Wrap(kind, &git.Fetcher{})
	case manifest.KindOCIRepository:
		return parsed.Object, source.Wrap(kind, &oci.Fetcher{Network: network})
	default:
		return parsed.Object, source.Wrap(kind, &bucket.Fetcher{Network: network})
	}
}

func TestFetcher_AdmittedLocalGitUnderRestriction(t *testing.T) {
	t.Parallel()
	repo := &manifest.GitRepository{Name: "local", Namespace: "apps"}
	repo.URL = "file:///working-tree"
	st := store.New()
	st.AddObject(repo)
	var reads store.ReadSet
	reads.Init(st, repo.Named(), nil)
	defer reads.Retire()
	admitted := &store.SourceArtifact{InputAlias: true, FS: fstest.MapFS{}}
	f := &git.Fetcher{RestrictSchemes: true}
	got, err := fetchtest.Fetch(t.Context(), f.Fetch, repo, &reads, &sourcecache.Policy{}, admitted)
	assert.NoError(t, err)
	assert.Equal(t, got, admitted)
}

func TestFetcher_PreparedMissingGitCredentials(t *testing.T) {
	t.Parallel()
	repo := &manifest.GitRepository{
		Name: "repo", Namespace: "apps", URL: "https://git.example/repo",
		SecretRef: &manifest.LocalObjectReference{Name: "auth"},
	}
	s := store.New()
	s.AddObject(repo)
	var reads store.ReadSet
	reads.Init(s, repo.Named(), nil)
	defer reads.Close()
	f := &git.Fetcher{}
	prepared, err := f.Fetch(t.Context(), repo, &reads, &sourcecache.Policy{})
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, !ok, "missing credentials: %v", err)
	assert.Equal(t, prepared.Fetch == nil, true)
	prepared, err = missing.RetryMachine(t.Context(), &reads)
	assert.NoError(t, err)
	assert.Equal(t, prepared.LocalUnavailable, missing)
	assert.Equal(t, prepared.Fetch != nil, true)
}

func TestFetcher_PreparedLocalContent(t *testing.T) {
	t.Parallel()
	auth := &manifest.LocalObjectReference{Name: "auth"}
	repository := &manifest.HelmRepository{
		Name: "charts", Namespace: "apps", URL: "https://charts.example", SecretRef: auth,
	}
	for _, tc := range []struct {
		name    string
		object  manifest.BaseManifest
		fetcher source.Fetcher
	}{
		{
			name: "git",
			object: &manifest.GitRepository{
				Name: "repo", Namespace: "apps", URL: "https://git.example/repo", SecretRef: auth,
			},
			fetcher: source.Wrap(manifest.KindGitRepository, &git.Fetcher{}),
		},
		{
			name: "oci",
			object: &manifest.OCIRepository{
				Name: "repo", Namespace: "apps", URL: "oci://registry.example/repo", SecretRef: auth,
			},
			fetcher: source.Wrap(manifest.KindOCIRepository, &oci.Fetcher{}),
		},
		{
			name: "bucket",
			object: &manifest.Bucket{
				Name: "repo", Namespace: "apps", Endpoint: "https://bucket.example", BucketName: "data", SecretRef: auth,
			},
			fetcher: source.Wrap(manifest.KindBucket, &bucket.Fetcher{}),
		},
		{
			name: "helm",
			object: &manifest.HelmChartSource{
				Name: "chart", Namespace: "apps", Chart: "app", SourceRef: sourcev1.LocalHelmChartSourceReference{
					Kind: manifest.KindHelmRepository, Name: repository.Name,
				},
			},
			fetcher: source.Wrap(manifest.KindHelmChart, &helmchart.Fetcher{}),
		},
		{
			name:    "external",
			object:  &manifest.ExternalArtifact{Name: "artifact", Namespace: "apps"},
			fetcher: source.Wrap(manifest.KindExternalArtifact, &external.Fetcher{}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			s.AddObject(tc.object)
			s.AddObject(repository)
			s.AddObject(&manifest.Secret{Name: "auth", Namespace: "apps", StringData: map[string]any{
				"username": "user", "password": "password", "accesskey": "user", "secretkey": "password",
				".dockerconfigjson": `{"auths":{"registry.example":{"username":"user","password":"password"}}}`,
			}})
			var reads store.ReadSet
			reads.Init(s, tc.object.Named(), nil)
			defer reads.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			prepared, err := tc.fetcher.Fetch(ctx, tc.object, &reads, &sourcecache.Policy{})
			assert.NoError(t, err)
			assert.Equal(t, len(prepared.Inputs), 0)
			assert.Equal(t, prepared.LocalUnavailable, (*source.MissingSecretError)(nil))
			cancel()
			local := &store.SourceArtifact{InputAlias: true, FS: fstest.MapFS{}}
			artifact, err := prepared.Fetch(t.Context(), local)
			assert.NoError(t, err)
			assert.Equal(t, artifact, local)
		})
	}
}
