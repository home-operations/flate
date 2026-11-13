package oci

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/controllers/base"
	sourcecontroller "github.com/home-operations/flate/pkg/controllers/source"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type ambientObserver struct {
	*Fetcher

	missing      *source.MissingSecretError
	artifact     *store.SourceArtifact
	ambientError error
	calls        int
}

func (f *ambientObserver) Fetch(
	ctx context.Context,
	repo *manifest.OCIRepository,
	reads *store.ReadSet,
	policy *sourcecache.Policy,
) (source.PreparedFetch, error) {
	artifact, err := f.Fetcher.Fetch(ctx, repo, reads, policy)
	if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && missing.RetryAmbient != nil {
		f.missing = missing
		retry := missing.RetryAmbient
		missing.RetryAmbient = func(ctx context.Context) (source.PreparedFetch, error) {
			f.calls++
			prepared, prepareErr := retry(ctx)
			if prepareErr != nil {
				f.ambientError = prepareErr
				return prepared, prepareErr
			}
			fetch := prepared.Fetch
			prepared.Fetch = func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
				f.artifact, f.ambientError = fetch(ctx, local)
				return f.artifact, f.ambientError
			}
			return prepared, nil
		}
	}
	return artifact, err
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_LateProducerInvalidatesAmbient(t *testing.T) {
	for _, timing := range []string{"before_settlement", "during_acquisition"} {
		t.Run(timing, func(t *testing.T) {
			testFetcherLateProducerInvalidatesAmbientCase(
				t,
				&timing,
			)
		},
		)
	}
}

func TestFetcher_AmbientFailedAcquisition(t *testing.T) {
	t.Parallel()
	var authenticated atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user, password, ok := r.BasicAuth(); ok && user == "fixture" && password == "fixture" {
			authenticated.Add(1)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)
	host := mustURL(t, srv.URL).Host
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(t, config, fmt.Sprintf(`{"auths":{%q:{"username":"fixture","password":"fixture"}}}`, host))
	repo := &manifest.OCIRepository{
		Name:      "image",
		Namespace: "apps",
		URL:       "oci://" + host + "/fixture",
		Insecure:  true,
		SecretRef: &manifest.LocalObjectReference{Name: "auth"},
	}
	s := store.New()
	s.AddObject(repo)
	var reads store.ReadSet
	reads.Init(s, repo.Named(), nil)
	defer reads.Close()
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New), Network: newTestNetwork(t), RegistryConfig: config}
	_, err := fetchtest.Fetch(t.Context(), f.Fetch, repo, &reads, &sourcecache.Policy{}, nil)
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "prepare = %v", err)
	artifact, err := fetchtest.Retry(t.Context(), missing.RetryAmbient)
	if artifact != nil || err == nil || authenticated.Load() == 0 || !errors.Is(err, manifest.ErrMissingSecret) ||
		!errors.Is(err, manifest.ErrFlux) {
		t.Fatalf("ambient failure: artifact=%v error=%v authenticated=%d", artifact, err, authenticated.Load())
	}
}

//nolint:paralleltest // Credential fixture helpers change the process environment.
//nolint:paralleltest // Credential and DNS fixtures change process environment or resolver state.
func TestFetcher_HelmAmbientReload(t *testing.T) {
	for _, mode := range []string{"rotated", "removed"} {
		t.Run(mode, func(t *testing.T) {
			testFetcherHelmAmbientReloadCase(t, &mode)
		},
		)
	}
}

func TestFetcher_SuppliedRegistryIdentity(t *testing.T) {
	dockerDir := t.TempDir()
	t.Setenv("DOCKER_CONFIG", dockerDir)
	testutil.WriteFile(t, dockerDir, "config.json", `{`)
	helmPath := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("HELM_REGISTRY_CONFIG", helmPath)
	testutil.WriteFileAt(t, helmPath, `{`)
	repo := &manifest.OCIRepository{
		Name:      "image",
		Namespace: "apps",
		URL:       "oci://ghcr.io/fixture/image",
		SecretRef: &manifest.LocalObjectReference{Name: "auth"},
	}
	f := &Fetcher{Network: newTestNetwork(t)}
	identities := make([]any, 0, 4)
	for _, tc := range []struct{ path, password, repository string }{
		{"first.yaml", "one", ""},
		{"second.yaml", "one", ""},
		{"second.yaml", "two", ""},
		{"third.yaml", "ignored", "one"},
	} {
		config := func(password string) string {
			return fmt.Sprintf(`{"auths":{"ghcr.io":{"username":"fixture","password":%q}}}`, password)
		}
		secret := &manifest.Secret{
			Name:       "auth",
			Namespace:  "apps",
			StringData: map[string]any{".dockerconfigjson": config(tc.password)},
		}
		snapshot := []store.Declaration{
			{
				Obj:       secret,
				Doc:       testutil.ObjectDocument(secret),
				Origin:    manifest.SuppliedDeclaration,
				InputPath: tc.path,
			},
		}
		s := store.New()
		for _, declaration := range snapshot {
			s.AddDeclaration(declaration)
		}
		if tc.repository != "" {
			s.AddObject(
				&manifest.Secret{
					Name:       "auth",
					Namespace:  "apps",
					StringData: map[string]any{".dockerconfigjson": config(tc.repository)},
				},
			)
		}
		var reads store.ReadSet
		reads.Init(s, repo.Named(), nil)
		op := &operation{Fetcher: f, reads: &reads, secrets: source.SecretLookup(&reads)}
		request, err := op.resolvedRepository(t.Context(), new(repo.RegistryAccess()))
		assert.NoError(t, err)
		identities = append(identities, request.material)
		reads.Close()
	}
	assert.FatalIf(
		t,
		identities[0] == nil || !cmp.Equal(identities[0], identities[1]) || cmp.Equal(identities[0], identities[2]) ||
			!cmp.Equal(
				identities[0],
				identities[3],
			),
		"%s",
		"credential identity depends on input path or shadowed supply, or misses rotation",
	)
}

//nolint:paralleltest // The test changes the process logging destination.
//nolint:paralleltest // The test changes process logging state.
func TestFetcher_AmbientAcquisitionErrors(t *testing.T) {
	var diagnostic bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&diagnostic, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	for _, deferred := range []bool{false, true} {
		for _, stage := range []string{"canceled", "malformed", "unreadable", "helper failure", "missing helper", "unavailable", "producer"} {
			if !deferred && stage == "producer" {
				continue
			}
			t.Run(fmt.Sprintf("deferred=%t/%s", deferred, stage), func(t *testing.T) {
				testFetcherAmbientAcquisitionErrorsCase(
					t,

					&diagnostic,
					&deferred,
					&stage,
				)
			},
			)
		}
	}
}

func testFetcherLateProducerInvalidatesAmbientCase(
	t *testing.T, timing *string,
) {
	t.Helper()
	artifact := newFakeOCIArtifact(t, map[string]string{"data.yaml": "ambient"})
	upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	block := sync.OnceFunc(func() {
		close(entered)
		<-release
	})
	var authenticated atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if user, password, ok := r.BasicAuth(); !ok || user != "fixture" || password != "fixture" {
			w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		authenticated.Add(1)
		block()
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	host := mustURL(t, srv.URL).Host
	config := filepath.Join(t.TempDir(), "config.json")
	testutil.WriteFileAt(
		t,
		config,
		fmt.Sprintf(`{"auths":{%q:{"username":"fixture","password":"fixture"}}}`, host),
	)
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	t.Setenv("HELM_REGISTRY_CONFIG", config)
	f := &ambientObserver{
		Fetcher: &Fetcher{Cache: cachetest.New(t, "", sourcecache.New), Network: newTestNetwork(t)},
	}
	s := store.New()
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{})
	tasks := task.NewBounded(2)
	c := sourcecontroller.New(s, tasks)
	c.Fetchers[manifest.KindOCIRepository] = source.Wrap(manifest.KindOCIRepository, f)
	c.Configure(sourcecontroller.FetchOptions{}, sourcecache.RetryConfig{})
	c.StartLifecycle()
	repo := &manifest.OCIRepository{
		Name:      "image",
		Namespace: "apps",
		URL:       "oci://" + host + "/fixture",
		Insecure:  true,
		SecretRef: &manifest.LocalObjectReference{Name: "auth"},
	}
	owner := &manifest.Kustomization{Name: "producer", Namespace: repo.Namespace}
	s.AddObjects([]manifest.BaseManifest{repo, owner})
	reconcile := func() *base.Execution {
		var run *base.Execution
		tasks.Go(
			t.Context(),
			"source",
			func(ctx context.Context) {
				run = c.ReconcileNode(ctx, repo.Named(), manifest.DrainNone, schedule.Result{})
			},
		)
		tasks.BlockTillDone()
		return run
	}
	first := reconcile()
	defer first.Close()
	assert.FatalIf(
		t,
		first.Outcome() != schedule.Blocked || f.missing == nil || f.calls != 0,
		"%s",
		"source did not retain its missing Secret before ambient settlement",
	)

	missing := f.missing
	publish := func() {
		producer := &manifest.RawObject{
			Kind:       "ExternalSecret",
			APIVersion: "external-secrets.io/v1",
			Name:       "auth",
			Namespace:  repo.Namespace,
		}
		s.RecordEmitted(
			owner.Named(),
			&store.Emission{Children: []store.Child{{Obj: producer}}, Complete: true},
			nil,
		)
	}
	if (*timing) == "before_settlement" {
		unblock()
		publish()
	}
	tasks.Go(t.Context(), "settle", func(ctx context.Context) { first.Settle(ctx, manifest.DrainCascade) })
	if (*timing) == "during_acquisition" {
		<-entered
		publish()
		unblock()
	}
	tasks.BlockTillDone()
	assert.FatalIf(
		t,
		first.Valid() || s.GetArtifact(repo.Named(), store.ArtifactContent) != nil,
		"%s",
		"late producer did not invalidate the attempt and reject its ambient artifact",
	)

	wantCalls := 0
	if (*timing) == "during_acquisition" {
		wantCalls = 1
		if f.artifact == nil || f.ambientError != nil || authenticated.Load() == 0 ||
			mustReadFile(t, f.artifact.FS, "data.yaml") != "ambient" {
			t.Fatalf("ambient acquisition control: artifact=%v error=%v", f.artifact, f.ambientError)
		}
	}
	if f.calls != wantCalls {
		t.Fatalf("ambient calls=%d; want %d", f.calls, wantCalls)
	}
	first.Retire()
	next := reconcile()
	defer next.Close()
	if next.Outcome() != schedule.Blocked || !slices.Contains(next.Blocked, missing.Secret) ||
		f.missing == missing ||
		f.calls != wantCalls {
		t.Fatalf("rerun did not wait on the captured producer: blocked=%v calls=%d", next.Blocked, f.calls)
	}
	assert.FatalIf(t, f.missing.Owner != missing.Owner || f.missing.Secret != missing.Secret ||
		f.missing.Detail != "is produced in the cluster with unknown data; supply it with --supply", "%s", "rerun did not retain identities with the current unavailable cause")

	before := authenticated.Load()
	tasks.Go(
		t.Context(),
		"producer settlement",
		func(ctx context.Context) { next.Settle(ctx, manifest.DrainCascade) },
	)
	tasks.BlockTillDone()
	status, _ := s.GetStatus(repo.Named())
	if !next.Valid() || status.Outcome() != store.OutcomeSkipped || f.calls != wantCalls ||
		authenticated.Load() != before ||
		s.GetArtifact(repo.Named(), store.ArtifactContent) != nil {
		t.Fatalf("producer settlement: status=%+v error=%v calls=%d", status, f.ambientError, f.calls)
	}
}

func testFetcherHelmAmbientReloadCase(t *testing.T, mode *string,
) {
	t.Helper()
	artifact := newFakeOCIArtifact(t, map[string]string{"data.yaml": "current"})
	upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
	var expected atomic.Value
	expected.Store("first")
	var accepted atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := expected.Load().(string)
		user, password, present := r.BasicAuth()
		if want == "" && present || want != "" && (!present || user != "fixture" || password != want) {
			w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		accepted.Add(1)
		upstream.Config.Handler.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	host := mustURL(t, srv.URL).Host
	path := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	t.Setenv("HELM_REGISTRY_CONFIG", path)
	write := func(password string) {
		t.Helper()
		testutil.WriteFileAt(
			t,
			path,
			fmt.Sprintf(`{"auths":{%q:{"username":"fixture","password":%q}}}`, host, password),
		)
	}
	write("first")
	repo := &manifest.OCIRepository{
		Name:      "image",
		Namespace: "apps",
		URL:       "oci://" + host + "/fixture",
		Insecure:  true,
		SecretRef: &manifest.LocalObjectReference{Name: "missing"},
	}
	var lookups atomic.Int32
	inputs := testInputs{
		secrets: func(_, _ string) store.ReferenceFacts { lookups.Add(1); return store.ReferenceFacts{} },
	}
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New), Network: newTestNetwork(t)}
	op := &operation{Fetcher: f, secrets: inputs.lookup(repo)}
	_, err := fetchtest.Retry(t.Context(), func(context.Context) (source.PreparedFetch, error) {
		return op.fetchSource(t.Context(), repo, &sourcecache.Policy{})
	})
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	if !ok || missing.RetryAmbient == nil || lookups.Load() != 1 {
		t.Fatalf("Helm eligibility: lookups=%d error=%v", lookups.Load(), err)
	}
	first, err := fetchtest.Machine(t.Context(), missing, nil)
	assert.NoError(t, err)
	if (*mode) == "removed" {
		expected.Store("")
		assert.NoError(t, os.Remove(path))
	} else {
		expected.Store("second")
		write("second")
	}
	before := accepted.Load()
	f = &Fetcher{Cache: f.Cache, Network: newTestNetwork(t)}
	_, err = fetchtest.Retry(t.Context(), func(context.Context) (source.PreparedFetch, error) {
		return (&operation{Fetcher: f, secrets: inputs.lookup(repo)}).fetchSource(
			t.Context(),
			repo,
			&sourcecache.Policy{},
		)
	})
	missing, _ = errors.AsType[*source.MissingSecretError](err)
	got, err := fetchtest.Machine(t.Context(), missing, nil)
	if (*mode) == "removed" {
		assert.FatalIf(
			t,
			err != nil || got == nil || accepted.Load() == before,
			"unavailable credential reached source: %v",
			err,
		)
		return
	}
	assert.NoError(t, err)
	if got.Key == first.Key || accepted.Load() == before || lookups.Load() != 2 {
		t.Fatalf(
			"retry reused stale credentials/identity or reread missing Secret: requests=%d->%d lookups=%d",
			before,
			accepted.Load(),
			lookups.Load(),
		)
	}
	assert.Equal(t, mustReadFile(t, got.FS, "data.yaml"), "current")
}

func testFetcherAmbientAcquisitionErrorsCase(
	t *testing.T, diagnostic *bytes.Buffer,
	deferred *bool,
	stage *string,
) {
	t.Helper()
	root, bin := t.TempDir(), t.TempDir()
	marker := filepath.Join(root, "called")
	t.Setenv("PATH", bin)
	t.Setenv("FLATE_HELPER_MARKER", marker)
	helper := filepath.Join(bin, "docker-credential-fixture")
	if (*stage) != "missing helper" {
		testutil.WriteFileAt(
			t,
			helper,
			"#!/bin/sh\nprintf called >> \"$FLATE_HELPER_MARKER\"\nprintf '%s' 'credential-canary'\nexit 1\n",
		)
		assert.NoError(t, os.Chmod(helper, 0o700))
	}
	var remote atomic.Int32
	srv := httptest.NewTLSServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, _ *http.Request) { remote.Add(1); w.WriteHeader(http.StatusForbidden) },
		),
	)
	t.Cleanup(srv.Close)
	repo := &manifest.OCIRepository{
		Name:      "image",
		Namespace: "apps",
		URL:       "oci://" + mustURL(t, srv.URL).Host + "/fixture",
		Insecure:  true,
	}
	if *deferred {
		repo.SecretRef = &manifest.LocalObjectReference{Name: "auth"}
	}
	config := filepath.Join(root, "credential-canary.json")
	body := `{"credsStore":"fixture"}`
	if (*stage) == "malformed" {
		body = "{"
	}
	if (*stage) == "unavailable" {
		body = `{}`
		repo.Provider = "aws"
	}
	testutil.WriteFileAt(t, config, body)
	if (*stage) == "unreadable" {
		config = filepath.Join(config, "config.json")
	}
	st := store.New()
	st.AddObject(repo)
	if (*stage) == "producer" {
		secret := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "auth"}
		st.SetDiscoveryIndex(
			&manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					secret: {
						Productions: []manifest.Production{
							{
								Declaration: manifest.NamedResource{
									Kind:      "ExternalSecret",
									Namespace: "apps",
									Name:      "auth",
								},
							},
						},
					},
				},
			},
		)
	}
	var reads store.ReadSet
	reads.Init(st, repo.Named(), nil)
	defer reads.Close()
	cacheRoot := t.TempDir()
	f := &Fetcher{
		Cache:          cachetest.New(t, cacheRoot, sourcecache.New),
		Network:        newTestNetwork(t),
		RegistryConfig: config,
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if (*stage) == "canceled" {
		cancel()
	}
	testFetcherAmbientAcquisitionErrorsCaseCompletion(
		t,
		&testFetcherAmbientAcquisitionErrorsCaseCompletionInputs{
			ctx:        &ctx,
			deferred:   &deferred,
			f:          &f,
			repo:       &repo,
			reads:      &reads,
			stage:      &stage,
			remote:     &remote,
			diagnostic: &diagnostic,
			marker:     &marker,
		},
	)
}

func testFetcherAmbientAcquisitionErrorsCaseCompletion(
	t *testing.T,
	captures *testFetcherAmbientAcquisitionErrorsCaseCompletionInputs,
) {
	t.Helper()
	var art *store.SourceArtifact
	var err error
	if *(*captures.deferred) {
		_, err = fetchtest.Fetch(
			t.Context(),
			(*captures.f).Fetch,
			(*captures.repo),
			captures.reads,
			&sourcecache.Policy{},
			nil,
		)
		missing, ok := errors.AsType[*source.MissingSecretError](err)
		assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "missing continuation: %v", err)
		art, err = fetchtest.Machine((*captures.ctx), missing, captures.reads)
		assert.FatalIf(t, ((*(*captures.stage)) == "producer" || (*(*captures.stage)) == "unavailable") &&
			!errors.Is(err, missing), "%s", "unavailable selection changed missing identity")
	} else {
		art, err = fetchtest.Fetch(
			(*captures.ctx),
			(*captures.f).Fetch,
			(*captures.repo),
			captures.reads,
			&sourcecache.Policy{},
			nil,
		)
	}
	if art != nil || err == nil || captures.remote.
		Load() != map[bool]int32{false: 0, true: 1}[(*(*captures.stage)) == "unavailable"] ||
		strings.Contains(err.Error(), "canary") ||
		strings.Contains((*captures.diagnostic).String(), "canary") {
		t.Fatalf(
			"preparation admitted remote work or exposed provider text: remote=%d %v", captures.remote.
				Load(), err,
		)
	}
	assert.FatalIf(
		t, (*(*captures.stage)) ==
			"canceled" && !errors.Is(err, context.Canceled), "lost cancellation: %v",
		err,
	)
	if (*(*captures.stage)) == "missing helper" {
		{
			_, ok := errors.AsType[*exec.Error](err)
			assert.FatalIf(t, !ok || !errors.Is(err, manifest.ErrFlux), "lost typed native error: %v", err)
		}
	}
	if (*(*captures.stage)) == "unreadable" {
		{
			_, ok := errors.AsType[*os.PathError](err)
			assert.FatalIf(t, !ok, "lost typed file error: %v", err)
		}
	}
	calls, readErr := os.ReadFile((*captures.marker))
	if (*(*captures.stage)) == "helper failure" {
		assert.FatalIf(
			t,
			readErr != nil || string(calls) != "called",
			"provider was not selected exactly once: %q %v",
			calls,
			readErr,
		)
	} else {
		assert.FatalIf(
			t,
			!errors.Is(readErr, os.ErrNotExist),
			"unexpected native-provider call: %q %v",
			calls,
			readErr,
		)
	}
}

type testFetcherAmbientAcquisitionErrorsCaseCompletionInputs struct {
	ctx        *context.Context
	deferred   **bool
	f          **Fetcher
	repo       **manifest.OCIRepository
	reads      *store.ReadSet
	stage      **string
	remote     *atomic.Int32
	diagnostic **bytes.Buffer
	marker     *string
}
