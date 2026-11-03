package oci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/adler32"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"oras.land/oras-go/v2/registry/remote/auth"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type testInputs struct {
	secrets source.SecretGetter
	repos   func(string, string) *manifest.HelmRepository
}

func (f testInputs) readSet(obj manifest.BaseManifest) *store.ReadSet {
	s := store.New()
	seen := make(map[manifest.NamedResource]bool)
	var admit func(manifest.BaseManifest)
	admit = func(obj manifest.BaseManifest) {
		s.AddObject(obj)
		for ref := range manifest.References(obj, manifest.ReferenceRelations) {
			if seen[ref.NamedResource] {
				continue
			}
			seen[ref.NamedResource] = true
			if ref.Kind == manifest.KindSecret && f.secrets != nil {
				if sec := f.secrets(ref.Namespace, ref.Name); sec != nil {
					clone := *sec
					clone.Namespace, clone.Name = ref.Namespace, ref.Name
					s.AddObject(&clone)
				}
			}
			if ref.Kind == manifest.KindHelmRepository && f.repos != nil {
				if repo := f.repos(ref.Namespace, ref.Name); repo != nil {
					admit(repo)
				}
			}
		}
	}
	admit(obj)
	reads := new(store.ReadSet)
	reads.Init(s, obj.Named(), nil)
	return reads
}

func (f testInputs) lookup(obj manifest.BaseManifest) source.SecretGetter {
	if f.secrets == nil {
		return nil
	}
	return source.SecretLookup(f.readSet(obj))
}

func TestFetcher_InputsRequireRegistry(t *testing.T) {
	t.Parallel()
	provider := &manifest.ResourceSetInputProvider{
		Name:      "static",
		Namespace: "ns",
		Type:      fluxopv1.InputProviderStatic,
		URL:       "oci://registry.test/app",
	}
	_, err := (&Fetcher{}).FetchInputs(t.Context(), provider, nil, cache.Policy{})
	assert.FatalIf(t, !errors.Is(err, manifest.ErrInput), "non-registry inputs: %v", err)
}

func TestFetcher_InputsAcquisitionWaves(t *testing.T) {
	t.Parallel()
	var lists atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		lists.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"name": "fixture", "tags": []string{}})
	}))
	t.Cleanup(srv.Close)
	host := mustURL(t, srv.URL).Host
	provider := &manifest.ResourceSetInputProvider{
		Name:      "images",
		Namespace: "apps",
		Type:      fluxopv1.InputProviderOCIArtifactTag,
		URL:       "oci://" + host + "/fixture",
		Insecure:  true,
		SecretRef: &manifest.LocalObjectReference{Name: "auth"},
	}
	inputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		return &manifest.Secret{
			StringData: map[string]any{
				".dockerconfigjson": fmt.Sprintf(`{"auths":{%q:{"username":"fixture","password":"fixture"}}}`, host),
			},
		}
	}}
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), cache.New)}
	var artifacts [2]*store.SourceArtifact
	var failures [2]error
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		first := sync.OnceFunc(func() { close(entered); <-release })
		policy := cache.Policy{Admit: func(context.Context) (func(), error) { first(); return func() {}, nil }}
		var wg sync.WaitGroup
		run := func(index int) {
			reads := inputs.readSet(provider)
			defer reads.Close()
			artifacts[index], failures[index] = f.FetchInputs(t.Context(), provider, reads, policy)
		}
		wg.Go(func() { run(0) })
		<-entered
		wg.Go(func() { run(1) })
		synctest.Wait()
		close(release)
		wg.Wait()
		for i, err := range failures {
			assert.NoError(t, err)
			assert.FatalIf(t, artifacts[i] == nil, "acquisition %d returned no artifact", i)
		}
		{
			got := lists.Load()
			assert.FatalIf(t, got != 1, "one acquisition wave listed tags %d times, want 1", got)
		}
		reads := inputs.readSet(provider)
		defer reads.Close()
		_, err := f.FetchInputs(t.Context(), provider, reads, cache.Policy{})
		assert.NoError(t, err)
		{
			got := lists.Load()
			assert.FatalIf(t, got != 2, "new acquisition wave listed tags %d times, want 2", got)
		}
		f.Network.Close()
	})
}

func TestFetcher_InputsSelectionAndRefresh(t *testing.T) {
	t.Parallel()
	for _, plainHTTP := range []bool{false, true} {
		t.Run(fmt.Sprintf("plain_http=%t", plainHTTP), func(t *testing.T) {
			t.Parallel()
			testFetcherInputsSelectionAndRefreshCase(
				t,
				&plainHTTP,
			)
		},
		)
	}
}

func TestFetcher_InputsAuthentication(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                                                                                                             string
		status                                                                                                                           int
		challenge, bearer, machine, badMachine, secret, missing, wiped, badAuth, badCert, missingCert, canceled, deadline, invalidFilter bool
		wantMissing, wantAnonymous                                                                                                       bool
	}{
		{name: "anonymous basic denial", status: 401, challenge: true, wantMissing: true, wantAnonymous: true},
		{name: "anonymous forbidden", status: 403, wantMissing: true, wantAnonymous: true},
		{name: "anonymous bearer denial", status: 403, bearer: true, wantMissing: true, wantAnonymous: true},
		{name: "malformed machine configuration", badMachine: true},
		{name: "invalid filter", invalidFilter: true},
		{name: "machine rejected", status: 403, machine: true, wantMissing: true},
		{name: "secret rejected", status: 403, secret: true, wantMissing: true},
		{name: "explicit missing", missing: true, wantMissing: true},
		{name: "wiped auth", secret: true, wiped: true, wantMissing: true},
		{name: "malformed auth", secret: true, badAuth: true},
		{name: "malformed cert", badCert: true},
		{name: "missing cert", missingCert: true, wantMissing: true},
		{name: "server failure", status: 500},
		{name: "canceled", canceled: true},
		{name: "deadline", deadline: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testFetcherInputsAuthenticationCase(t, &tc)
		},
		)
	}
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_InputsMachineCredentialsReuse(t *testing.T) {
	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "kind: ConfigMap"})
	upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
	var password atomic.Value
	password.Store("first")
	var lists, heads atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherInputsMachineCredentialsReuseCallback(
			w,
			r,
			testFetcherInputsMachineCredentialsReuseCallbackInputs{
				upstream: &upstream,
				password: &password,
				lists:    &lists,
				heads:    &heads,
			},
		)
	},
	))
	t.Cleanup(srv.Close)
	bin := t.TempDir()
	marker := filepath.Join(t.TempDir(), "called")
	t.Setenv("PATH", bin)
	t.Setenv("FLATE_HELPER_MARKER", marker)
	t.Setenv("FLATE_HELPER_SECRET", "first")
	helper := filepath.Join(bin, "docker-credential-fixture")
	testutil.WriteFileAt(
		t,
		helper,
		"#!/bin/sh\nprintf called >> \"$FLATE_HELPER_MARKER\"\nprintf '{\"Username\":\"fixture\",\"Secret\":\"%s\"}' \"$FLATE_HELPER_SECRET\"\n",
	)
	assert.NoError(t, os.Chmod(helper, 0o700))
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, config, fmt.Sprintf(`{"credHelpers":{%q:"fixture"}}`, mustURL(t, srv.URL).Host))
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, t.TempDir(), cache.New), RegistryConfig: config}
	provider := &manifest.ResourceSetInputProvider{
		Name:      "images",
		Namespace: "apps",
		Type:      fluxopv1.InputProviderOCIArtifactTag,
		URL:       "oci://" + mustURL(t, srv.URL).Host + "/fixture",
		Insecure:  true,
	}
	repo := &manifest.OCIRepository{Name: "source", Namespace: "apps", URL: provider.URL, Insecure: true}
	sourceReads := (testInputs{}).readSet(repo)
	defer sourceReads.Close()
	_, err := f.Fetch(t.Context(), repo, sourceReads, cache.Policy{})
	assert.NoError(t, err)
	var original *store.SourceArtifact
	for invocation := range 3 {
		if invocation == 2 {
			password.Store("second")
			t.Setenv("FLATE_HELPER_SECRET", "second")
		}
		reads := (testInputs{}).readSet(provider)
		got, err := f.FetchInputs(t.Context(), provider, reads, cache.Policy{MaxAge: time.Hour})
		reads.Close()
		assert.NoError(t, err)
		if invocation == 0 {
			original = got
		}
		if invocation == 1 {
			assert.Equal(t, got.Key, original.Key)
		}
		assert.FatalIf(
			t,
			invocation == 2 && got.Key == original.Key,
			"%s",
			"rotated helper credential reused input identity",
		)

		data, err := os.ReadFile(marker)
		assert.NoError(t, err)
		assert.Equal(t, string(data), strings.Repeat("called", invocation+2))
	}
	assert.Equal(t, lists.Load(), int32(2))
	assert.Equal(t, heads.Load(), int32(3))
}

func TestFetcher_InputsCanceledWork(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"listing", "selected digest"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			testFetcherInputsCanceledWorkCase(t, &stage)
		},
		)
	}
}

func TestFetcher_InputsRetry(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"listing", "selected digest"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			testFetcherInputsRetryCase(t, &stage)
		},
		)
	}
}

func testFetcherInputsSelectionAndRefreshCase(
	t *testing.T, plainHTTP *bool,
) {
	t.Helper()

	first := newFakeOCIArtifact(t, map[string]string{"image": "first"})
	second := newFakeOCIArtifact(t, map[string]string{"image": "second"})
	var changed atomic.Bool
	var lists, heads atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherInputsSelectionAndRefreshCaseCallback(
			w,
			r,
			testFetcherInputsSelectionAndRefreshCaseCallbackInputs{
				t:       &t,
				first:   &first,
				second:  &second,
				changed: &changed,
				lists:   &lists,
				heads:   &heads,
			},
		)
	},
	))
	if *plainHTTP {
		srv.Start()
	} else {
		srv.StartTLS()
	}
	t.Cleanup(srv.Close)
	provider := &manifest.ResourceSetInputProvider{
		Name:      "images",
		Namespace: "apps",
		Type:      fluxopv1.InputProviderOCIArtifactTag,
		URL:       "oci://" + mustURL(t, srv.URL).Host + "/fixture",
		Insecure:  true,
		Filter:    &fluxopv1.ResourceSetInputFilter{IncludeTag: `^v?1\.`, Semver: ">=1.0.0", Limit: 2},
		DefaultValues: fluxopv1.ResourceSetInput{
			"color":  &apiextensionsv1.JSON{Raw: []byte(`"blue"`)},
			"id":     &apiextensionsv1.JSON{Raw: []byte(`"wrong"`)},
			"tag":    &apiextensionsv1.JSON{Raw: []byte(`"wrong"`)},
			"digest": &apiextensionsv1.JSON{Raw: []byte(`"wrong"`)},
		},
	}
	registryConfig := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, registryConfig, `{}`)
	f := &Fetcher{
		Network:        newTestNetwork(t),
		Cache:          cachetest.New(t, t.TempDir(), cache.New),
		RegistryConfig: registryConfig,
	}
	var previous *store.SourceArtifact
	for run := range 3 {
		if run == 2 {
			changed.Store(true)
		}
		reads := (testInputs{}).readSet(provider)
		artifact, err := f.FetchInputs(t.Context(), provider, reads, cache.Policy{})
		reads.Close()
		assert.NoError(t, err)
		var got []map[string]any
		assert.NoError(t, json.Unmarshal([]byte(mustReadFile(t, artifact.FS, "inputs.json")), &got))
		digest := first.manifestDigest
		if run == 2 {
			digest = second.manifestDigest
		}
		want := []map[string]any{}
		for _, tag := range []string{"v1.2.0", "1.2.0"} {
			want = append(
				want,
				map[string]any{
					"id":     strconv.FormatUint(uint64(adler32.Checksum([]byte(tag))), 10),
					"tag":    tag,
					"digest": digest,
					"color":  "blue",
				},
			)
		}
		assert.Diff(t, got, want)
		assert.FatalIf(t, artifact.Kind != manifest.KindResourceSetInputProvider || artifact.Content == "" ||
			artifact.Key == "" ||
			len(artifact.Warnings) != 0, "incomplete input artifact: %+v", artifact)
		if previous != nil {
			if run == 1 {
				assert.Equal(t, artifact.Content, previous.Content)
			} else {
				assert.FatalIf(t, artifact.Content == previous.Content, "%s", "changed digest retained input identity")
			}
			assert.FatalIf(t, run == 2 &&
				strings.Contains(
					mustReadFile(t, previous.FS, "inputs.json"),
					second.manifestDigest,
				), "%s", "refresh mutated held input artifact")
		}
		previous = artifact
	}
	assert.Equal(t, lists.Load(), int32(3))
	assert.Equal(t, heads.Load(), int32(6))
	empty := *provider
	empty.Filter = &fluxopv1.ResourceSetInputFilter{IncludeTag: "^absent$"}
	reads := (testInputs{}).readSet(&empty)
	defer reads.Close()
	artifact, err := f.FetchInputs(t.Context(), &empty, reads, cache.Policy{})
	assert.NoError(t, err)
	assert.Equal(t, mustReadFile(t, artifact.FS, "inputs.json"), "[]\n")
	assert.Equal(t, heads.Load(), int32(6))
}

func testFetcherInputsAuthenticationCase(t *testing.T, tc *struct {
	name          string
	status        int
	challenge     bool
	bearer        bool
	machine       bool
	badMachine    bool
	secret        bool
	missing       bool
	wiped         bool
	badAuth       bool
	badCert       bool
	missingCert   bool
	canceled      bool
	deadline      bool
	invalidFilter bool
	wantMissing   bool
	wantAnonymous bool
},
) {
	t.Helper()

	var requests atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherInputsAuthenticationCaseCallback(
			w,
			r,
			testFetcherInputsAuthenticationCaseCallbackInputs{t: &t, tc: &tc, requests: &requests},
		)
	},
	))
	t.Cleanup(srv.Close)
	host := mustURL(t, srv.URL).Host
	provider := &manifest.ResourceSetInputProvider{
		Name:      "images",
		Namespace: "apps",
		Type:      fluxopv1.InputProviderOCIArtifactTag,
		URL:       "oci://" + host + "/fixture",
		Insecure:  true,
	}
	credentials := fmt.Sprintf(`{"auths":{%q:{"username":"fixture","password":"fixture"}}}`, host)
	registryConfig := filepath.Join(t.TempDir(), "config.json")
	body := `{}`
	if tc.machine {
		body = credentials
	}
	if tc.badMachine {
		body = "{"
	}
	if tc.invalidFilter {
		provider.Filter = &fluxopv1.ResourceSetInputFilter{IncludeTag: "["}
	}
	testutil.WriteFileAt(t, registryConfig, body)
	var material map[string]any
	if tc.secret || tc.missing {
		provider.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
		if tc.secret {
			material = map[string]any{".dockerconfigjson": credentials}
		}
	}
	if tc.wiped {
		material[".dockerconfigjson"] = "..PLACEHOLDER_auth.."
	}
	if tc.badAuth {
		material[".dockerconfigjson"] = "{"
	}
	if tc.badCert || tc.missingCert {
		provider.CertSecretRef = &manifest.LocalObjectReference{Name: "cert"}
		if tc.badCert {
			material = map[string]any{"ca.crt": "malformed"}
		}
	}
	inputs := testInputs{secrets: func(_, _ string) *manifest.Secret {
		if material == nil {
			return nil
		}
		return &manifest.Secret{StringData: material}
	}}
	reads := inputs.readSet(provider)
	defer reads.Close()
	f := &Fetcher{
		Network:        newTestNetwork(t),
		Cache:          cachetest.New(t, "", cache.New),
		RegistryConfig: registryConfig,
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if tc.canceled {
		cancel()
	}
	testFetcherInputsAuthenticationCaseCompletion(
		t,
		&testFetcherInputsAuthenticationCaseCompletionInputs{
			ctx:      &ctx,
			tc:       &tc,
			f:        &f,
			provider: &provider,
			reads:    &reads,
			requests: &requests,
		},
	)
}

func testFetcherInputsAuthenticationCaseCompletion(
	t *testing.T,
	captures *testFetcherInputsAuthenticationCaseCompletionInputs,
) {
	t.Helper()
	if (*captures.tc).deadline {
		var done context.CancelFunc
		(*captures.ctx), done = context.WithDeadline((*captures.ctx), time.Unix(0, 0))
		defer done()
	}
	artifact, err := (*captures.f).FetchInputs((*captures.ctx), (*captures.provider), (*captures.reads), cache.Policy{})
	assert.FatalIf(
		t,
		artifact != nil || err == nil || errors.Is(err, manifest.ErrMissingSecret) != (*captures.tc).wantMissing,
		"acquisition artifact=%v error=%v",
		artifact,
		err,
	)
	missing, hasMissing := errors.AsType[*source.MissingSecretError](err)
	anonymous := hasMissing && missing.AnonymousDenied
	assert.Equal(t, anonymous, (*captures.tc).wantAnonymous)
	if (*captures.tc).wantAnonymous {
		assert.FatalIf(
			t,
			missing.Owner != (*captures.provider).Named() || missing.Secret != (manifest.NamedResource{}) ||
				missing.Cause == nil,
			"denial lost identity or cause: %+v",
			missing,
		)
		if (*captures.tc).challenge {
			assert.FatalIf(
				t,
				!errors.Is(err, auth.ErrBasicCredentialNotFound),
				"lost typed Basic absence: %v",
				err,
			)
		} else {
			response, ok := errors.AsType[*fetcherr.ResponseError](err)
			assert.FatalIf(
				t,
				!ok || response.StatusCode != (*captures.tc).status,
				"lost typed denial response: %v",
				err,
			)
		}
	}
	if (*captures.tc).missing || (*captures.tc).wiped {
		assert.FatalIf(
			t,
			!hasMissing || missing.RetryAmbient == nil || missing.Secret.Name != "auth",
			"explicit missing lost continuation: %v",
			err,
		)
	}
	assert.FatalIf(t, (*captures.tc).canceled && !errors.Is(err, context.Canceled), "lost cancellation: %v", err)
	assert.FatalIf(
		t, (*captures.tc).
			deadline && !errors.Is(err, context.DeadlineExceeded), "lost deadline: %v",
		err,
	)
	wantRequests := int32(1)
	if (*captures.tc).missing || (*captures.tc).wiped || (*captures.tc).badAuth || (*(*captures.tc)).
		badCert || (*captures.tc).
		missingCert || (*captures.tc).
		canceled || (*captures.tc).
		deadline || (*captures.tc).
		badMachine {
		wantRequests = 0
	}
	if (*captures.tc).bearer {
		wantRequests = 2
	}
	assert.Equal(t, captures.requests.Load(), wantRequests)
}

func testFetcherInputsCanceledWorkCase(t *testing.T, stage *string,
) {
	t.Helper()

	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "kind: ConfigMap"})
	upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
	entered := make(chan struct{})
	var blocked atomic.Bool
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := strings.HasSuffix(r.URL.Path, "/tags/list")
		if (*stage) == "selected digest" {
			target = strings.HasSuffix(r.URL.Path, "/manifests/a")
		}
		if target && blocked.CompareAndSwap(false, true) {
			close(entered)
			<-r.Context().Done()
			return
		}
		if strings.HasSuffix(r.URL.Path, "/tags/list") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"name":"fixture","tags":["a","b"]}`))
			return
		}
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, config, `{}`)
	provider := &manifest.ResourceSetInputProvider{
		Name:      "images",
		Namespace: "apps",
		Type:      fluxopv1.InputProviderOCIArtifactTag,
		URL:       "oci://" + mustURL(t, srv.URL).Host + "/fixture",
		Insecure:  true,
	}
	reads := (testInputs{}).readSet(provider)
	defer reads.Close()
	f := &Fetcher{
		Network:        newTestNetwork(t),
		Cache:          cachetest.New(t, t.TempDir(), cache.New),
		RegistryConfig: config,
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error)
	go func() {
		artifact, err := f.FetchInputs(ctx, provider, reads, cache.Policy{})
		if artifact != nil {
			err = fmt.Errorf("partial artifact admitted: %v", artifact)
		}
		done <- err
	}()
	<-entered
	cancel()
	err := <-done
	assert.FatalIf(
		t,
		!errors.Is(err, context.Canceled) || errors.Is(err, manifest.ErrMissingSecret),
		"canceled acquisition: %v",
		err,
	)
	got, err := f.FetchInputs(t.Context(), provider, reads, cache.Policy{})
	assert.NoError(t, err)
	var inputs []map[string]any
	assert.NoError(t, json.Unmarshal([]byte(mustReadFile(t, got.FS, "inputs.json")), &inputs))
	assert.Equal(t, len(inputs), 2)
}

func testFetcherInputsRetryCase(t *testing.T, stage *string,
) {
	t.Helper()

	artifact := newFakeOCIArtifact(t, map[string]string{"app.yaml": "kind: ConfigMap"})
	upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
	var lists, heads atomic.Int32
	var failed atomic.Bool
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		listing := strings.HasSuffix(r.URL.Path, "/tags/list")
		if listing {
			lists.Add(1)
		} else {
			heads.Add(1)
		}
		if listing == ((*stage) == "listing") && failed.CompareAndSwap(false, true) {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if listing {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"name":"fixture","tags":["1.0.0"]}`))
			return
		}
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, config, `{}`)
	provider := &manifest.ResourceSetInputProvider{
		Name:      "images",
		Namespace: "apps",
		Type:      fluxopv1.InputProviderOCIArtifactTag,
		URL:       "oci://" + mustURL(t, srv.URL).Host + "/fixture",
		Insecure:  true,
	}
	reads := (testInputs{}).readSet(provider)
	defer reads.Close()
	f := &Fetcher{
		Network:        newTestNetwork(t),
		Cache:          cachetest.New(t, t.TempDir(), cache.New),
		RegistryConfig: config,
	}
	got, err := f.FetchInputs(
		t.Context(),
		provider,
		reads,
		cache.Policy{Retry: cache.RetryConfig{Attempts: 2, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}},
	)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		!strings.Contains(mustReadFile(t, got.FS, "inputs.json"), artifact.manifestDigest),
		"%s",
		"retry lost resolved digest",
	)

	assert.Equal(t, lists.Load(), int32(2))
	wantHeads := int32(1)
	if (*stage) == "selected digest" {
		wantHeads = 2
	}
	assert.Equal(t, heads.Load(), wantHeads)
}

type testFetcherInputsMachineCredentialsReuseCallbackInputs struct {
	upstream **httptest.Server
	password *atomic.Value
	lists    *atomic.Int32
	heads    *atomic.Int32
}

func testFetcherInputsMachineCredentialsReuseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherInputsMachineCredentialsReuseCallbackInputs,
) {
	user, pass, ok := r.BasicAuth()
	if !ok || user != "fixture" || pass != testCaseInput.password.Load().(string) {
		w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if strings.HasSuffix(r.URL.Path, "/tags/list") {
		testCaseInput.lists.
			Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"fixture","tags":["1.0.0"]}`))
		return
	}
	if r.Method == http.MethodHead && strings.Contains(r.URL.Path, "/manifests/") {
		testCaseInput.heads.
			Add(1)
	}
	(*testCaseInput.upstream).
		Config.Handler.ServeHTTP(w, r)
}

type testFetcherInputsSelectionAndRefreshCaseCallbackInputs struct {
	t       **testing.T
	first   *fakeOCIArtifact
	second  *fakeOCIArtifact
	changed *atomic.Bool
	lists   *atomic.Int32
	heads   *atomic.Int32
}

func testFetcherInputsSelectionAndRefreshCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherInputsSelectionAndRefreshCaseCallbackInputs,
) {
	if strings.HasSuffix(r.URL.Path, "/tags/list") {
		testCaseInput.lists.
			Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).
			Encode(map[string]any{"name": "fixture", "tags": []string{"1.1.0", "v1.2.0", "1.2.0", "2.0.0", "dev", "1.0.0"}})
		return
	}
	if strings.Contains(r.URL.Path, "/manifests/") {
		testCaseInput.heads.
			Add(1)
		tag := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		assert.ErrorIf((*testCaseInput.t), tag != "v1.2.0" && tag != "1.2.0", "resolved filtered-out tag %q", tag)
		selected := (*testCaseInput.first)
		if testCaseInput.changed.Load() {
			selected = (*testCaseInput.second)
		}
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		w.Header().Set("Docker-Content-Digest", selected.manifestDigest)
		w.Header().Set("Content-Length", strconv.Itoa(len(selected.manifest)))
		w.WriteHeader(http.StatusOK)
		return
	}
	http.NotFound(w, r)
}

type testFetcherInputsAuthenticationCaseCallbackInputs struct {
	t  **testing.T
	tc **struct {
		name   string
		status int

		challenge     bool
		bearer        bool
		machine       bool
		badMachine    bool
		secret        bool
		missing       bool
		wiped         bool
		badAuth       bool
		badCert       bool
		missingCert   bool
		canceled      bool
		deadline      bool
		invalidFilter bool
		wantMissing   bool
		wantAnonymous bool
	}
	requests *atomic.Int32
}

func testFetcherInputsAuthenticationCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherInputsAuthenticationCaseCallbackInputs,
) {
	testCaseInput.requests.
		Add(1)
	if (*testCaseInput.tc).bearer && r.URL.Path != "/token" {
		w.Header().
			Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="https://%s/token",service="fixture",scope="repository:fixture:pull"`, r.Host))
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	if (*testCaseInput.tc).invalidFilter {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"fixture","tags":[]}`))
		return
	}
	if (*testCaseInput.tc).challenge {
		w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
	}
	if !(*testCaseInput.tc).machine && !(*testCaseInput.tc).secret && r.Header.Get("Authorization") != "" {
		(*testCaseInput.t).
			Error("anonymous registry request supplied credentials")
	}
	w.WriteHeader((*testCaseInput.tc).status)
}

type testFetcherInputsAuthenticationCaseCompletionInputs struct {
	ctx *context.Context
	tc  **struct {
		name          string
		status        int
		challenge     bool
		bearer        bool
		machine       bool
		badMachine    bool
		secret        bool
		missing       bool
		wiped         bool
		badAuth       bool
		badCert       bool
		missingCert   bool
		canceled      bool
		deadline      bool
		invalidFilter bool
		wantMissing   bool
		wantAnonymous bool
	}
	f        **Fetcher
	provider **manifest.ResourceSetInputProvider
	reads    **store.ReadSet
	requests *atomic.Int32
}
