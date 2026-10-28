package oci

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

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

func (f *ambientObserver) Fetch(ctx context.Context, repo *manifest.OCIRepository, reads *store.ReadSet, policy sourcecache.Policy) (*store.SourceArtifact, error) {
	artifact, err := f.Fetcher.Fetch(ctx, repo, reads, policy)
	if missing, ok := errors.AsType[*source.MissingSecretError](err); ok && missing.RetryAmbient != nil {
		f.missing = missing
		retry := missing.RetryAmbient
		missing.RetryAmbient = func(ctx context.Context) (*store.SourceArtifact, error) {
			f.calls++
			f.artifact, f.ambientError = retry(ctx)
			return f.artifact, f.ambientError
		}
	}
	return artifact, err
}

func TestFetcher_LateProducerInvalidatesAmbient(t *testing.T) {
	for _, timing := range []string{"before_settlement", "during_acquisition"} {
		t.Run(timing, func(t *testing.T) {
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
			testutil.WriteFileAt(t, config, fmt.Sprintf(`{"auths":{%q:{"username":"fixture","password":"fixture"}}}`, host))
			t.Setenv("DOCKER_CONFIG", t.TempDir())
			t.Setenv("HELM_REGISTRY_CONFIG", config)
			f := &ambientObserver{Fetcher: &Fetcher{Cache: cachetest.New(t, "", sourcecache.New), Network: newTestNetwork(t)}}
			s := store.New()
			s.SetDiscoveryIndex(&manifest.DiscoveryIndex{})
			tasks := task.NewBounded(2)
			c := sourcecontroller.New(s, tasks)
			c.Fetchers[manifest.KindOCIRepository] = source.Wrap(manifest.KindOCIRepository, f)
			c.Configure(sourcecontroller.FetchOptions{}, sourcecache.RetryConfig{})
			c.StartLifecycle()
			repo := &manifest.OCIRepository{Name: "image", Namespace: "apps", URL: "oci://" + host + "/fixture", Insecure: true, SecretRef: &manifest.LocalObjectReference{Name: "auth"}}
			owner := &manifest.Kustomization{Name: "producer", Namespace: repo.Namespace}
			s.AddObjects([]manifest.BaseManifest{repo, owner})
			reconcile := func() *base.Execution {
				var run *base.Execution
				tasks.Go(t.Context(), "source", func(ctx context.Context) { run = c.ReconcileNode(ctx, repo.Named(), manifest.DrainNone) })
				tasks.BlockTillDone()
				return run
			}
			first := reconcile()
			defer first.Close()
			if first.Outcome() != schedule.Blocked || f.missing == nil || f.calls != 0 {
				t.Fatal("source did not retain its missing Secret before ambient settlement")
			}
			missing := f.missing
			publish := func() {
				producer := &manifest.RawObject{Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Name: "auth", Namespace: repo.Namespace}
				s.RecordEmitted(owner.Named(), &store.Emission{Children: []store.Child{{Obj: producer}}, Complete: true}, nil)
			}
			if timing == "before_settlement" {
				unblock()
				publish()
			}
			tasks.Go(t.Context(), "settle", func(ctx context.Context) { first.Settle(ctx, manifest.DrainCascade) })
			if timing == "during_acquisition" {
				<-entered
				publish()
				unblock()
			}
			tasks.BlockTillDone()
			if first.Valid() || s.GetArtifact(repo.Named(), store.ArtifactContent) != nil {
				t.Fatal("late producer did not invalidate the attempt and reject its ambient artifact")
			}
			wantCalls := 0
			if timing == "during_acquisition" {
				wantCalls = 1
				if f.artifact == nil || f.ambientError != nil || authenticated.Load() == 0 || mustReadFile(t, f.artifact.FS, "data.yaml") != "ambient" {
					t.Fatalf("ambient acquisition control: artifact=%v error=%v", f.artifact, f.ambientError)
				}
			}
			if f.calls != wantCalls {
				t.Fatalf("ambient calls=%d; want %d", f.calls, wantCalls)
			}
			first.Retire()
			next := reconcile()
			defer next.Close()
			if next.Outcome() != schedule.Blocked || !slices.Contains(next.Blocked, missing.Secret) || f.missing == missing || f.calls != wantCalls {
				t.Fatalf("rerun did not wait on the captured producer: blocked=%v calls=%d", next.Blocked, f.calls)
			}
			if f.missing.Owner != missing.Owner || f.missing.Secret != missing.Secret || f.missing.Detail != missing.Detail {
				t.Fatal("rerun changed the structured missing-material outcome")
			}
			before := authenticated.Load()
			tasks.Go(t.Context(), "producer settlement", func(ctx context.Context) { next.Settle(ctx, manifest.DrainCascade) })
			tasks.BlockTillDone()
			status, _ := s.GetStatus(repo.Named())
			if !next.Valid() || status.Outcome() != store.OutcomeSkipped || f.calls != wantCalls || authenticated.Load() != before || s.GetArtifact(repo.Named(), store.ArtifactContent) != nil {
				t.Fatalf("producer settlement: status=%+v error=%v calls=%d", status, f.ambientError, f.calls)
			}
		})
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
	repo := &manifest.OCIRepository{Name: "image", Namespace: "apps", URL: "oci://" + host + "/fixture", Insecure: true, SecretRef: &manifest.LocalObjectReference{Name: "auth"}}
	s := store.New()
	s.AddObject(repo)
	var reads store.ReadSet
	reads.Init(s, repo.Named(), nil)
	defer reads.Close()
	f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New), Network: newTestNetwork(t), RegistryConfig: config}
	_, err := f.Fetch(t.Context(), repo, &reads, sourcecache.Policy{})
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	if !ok || missing.RetryAmbient == nil {
		t.Fatalf("prepare = %v", err)
	}
	artifact, err := missing.RetryAmbient(t.Context())
	if artifact != nil || err == nil || authenticated.Load() == 0 || errors.Is(err, manifest.ErrMissingSecret) || !errors.Is(err, manifest.ErrFlux) {
		t.Fatalf("ambient failure: artifact=%v error=%v authenticated=%d", artifact, err, authenticated.Load())
	}
}

func TestFetcher_HelmAmbientReload(t *testing.T) {
	for _, mode := range []string{"rotated", "removed"} {
		t.Run(mode, func(t *testing.T) {
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
				testutil.WriteFileAt(t, path, fmt.Sprintf(`{"auths":{%q:{"username":"fixture","password":%q}}}`, host, password))
			}
			write("first")
			repo := &manifest.OCIRepository{Name: "image", Namespace: "apps", URL: "oci://" + host + "/fixture", Insecure: true, SecretRef: &manifest.LocalObjectReference{Name: "missing"}}
			var lookups atomic.Int32
			inputs := testInputs{secrets: func(_, _ string) *manifest.Secret { lookups.Add(1); return nil }}
			f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New), Network: newTestNetwork(t)}
			op := &operation{Fetcher: f, secrets: inputs.lookup(repo)}
			_, err := op.fetchSource(t.Context(), repo, sourcecache.Policy{})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.RetryAmbient == nil || lookups.Load() != 1 {
				t.Fatalf("Helm eligibility: lookups=%d error=%v", lookups.Load(), err)
			}
			selected, err := loadCredentials(t.Context(), "", host)
			assert.NoError(t, err)
			first, err := op.fetchWithCredentials(t.Context(), repo, selected, sourcecache.Policy{})
			assert.NoError(t, err)
			if mode == "removed" {
				expected.Store("")
				assert.NoError(t, os.Remove(path))
			} else {
				expected.Store("second")
				write("second")
			}
			before := accepted.Load()
			got, err := missing.RetryAmbient(t.Context())
			assert.NoError(t, err)
			if got.Key == first.Key || accepted.Load() == before || lookups.Load() != 1 {
				t.Fatalf("retry reused stale credentials/identity or reread missing Secret: requests=%d->%d lookups=%d", before, accepted.Load(), lookups.Load())
			}
			assert.Equal(t, mustReadFile(t, got.FS, "data.yaml"), "current")
		})
	}
}

func TestFetcher_SuppliedRegistryIdentity(t *testing.T) {
	dockerDir := t.TempDir()
	t.Setenv("DOCKER_CONFIG", dockerDir)
	testutil.WriteFile(t, dockerDir, "config.json", `{`)
	helmPath := filepath.Join(t.TempDir(), "config.json")
	t.Setenv("HELM_REGISTRY_CONFIG", helmPath)
	testutil.WriteFileAt(t, helmPath, `{`)
	repo := &manifest.OCIRepository{Name: "image", Namespace: "apps", URL: "oci://ghcr.io/fixture/image", SecretRef: &manifest.LocalObjectReference{Name: "auth"}}
	f := &Fetcher{Network: newTestNetwork(t)}
	var identities []any
	for _, tc := range []struct{ path, password, repository string }{
		{"first.yaml", "one", ""},
		{"second.yaml", "one", ""},
		{"second.yaml", "two", ""},
		{"third.yaml", "ignored", "one"},
	} {
		config := func(password string) string {
			return fmt.Sprintf(`{"auths":{"ghcr.io":{"username":"fixture","password":%q}}}`, password)
		}
		secret := &manifest.Secret{Name: "auth", Namespace: "apps", StringData: map[string]any{".dockerconfigjson": config(tc.password)}}
		snapshot, err := store.NewSupplied([]store.SuppliedObject{{Object: secret, Path: tc.path}})
		assert.NoError(t, err)
		s := store.New()
		s.SetSupplied(snapshot)
		if tc.repository != "" {
			s.AddObject(&manifest.Secret{Name: "auth", Namespace: "apps", StringData: map[string]any{".dockerconfigjson": config(tc.repository)}})
		}
		var reads store.ReadSet
		reads.Init(s, repo.Named(), nil)
		op := &operation{Fetcher: f, reads: &reads, secrets: source.SecretLookup(&reads)}
		credentials, err := op.resolveRegistryConfig(t.Context(), repo, "ghcr.io", sourcecache.Policy{})
		assert.NoError(t, err)
		request, err := op.newRepoClient(t.Context(), repo, credentials, sourcecache.Policy{})
		assert.NoError(t, err)
		identities = append(identities, request.material)
		reads.Close()
	}
	if identities[0] == nil || !cmp.Equal(identities[0], identities[1]) || cmp.Equal(identities[0], identities[2]) || !cmp.Equal(identities[0], identities[3]) {
		t.Fatal("credential identity depends on input path or shadowed supply, or misses rotation")
	}
}

func TestFetcher_AmbientAcquisitionErrors(t *testing.T) {
	t.Parallel()
	for _, malformed := range []bool{false, true} {
		t.Run(fmt.Sprintf("malformed=%t", malformed), func(t *testing.T) {
			t.Parallel()
			configPath := filepath.Join(t.TempDir(), "config.json")
			config := `{"auths":{"ghcr.io":{"username":"fixture","password":"fixture"}}}`
			if malformed {
				config = "{"
			}
			testutil.WriteFileAt(t, configPath, config)
			repo := &manifest.OCIRepository{Name: "image", Namespace: "apps", URL: "oci://ghcr.io/example/fixture", SecretRef: &manifest.LocalObjectReference{Name: "auth"}}
			s := store.New()
			s.AddObject(repo)
			var reads store.ReadSet
			reads.Init(s, repo.Named(), nil)
			defer reads.Close()
			f := &Fetcher{Cache: cachetest.New(t, "", sourcecache.New), Network: newTestNetwork(t), RegistryConfig: configPath}
			_, err := f.Fetch(t.Context(), repo, &reads, sourcecache.Policy{})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.RetryAmbient == nil {
				t.Fatalf("prepare = %v", err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			artifact, err := missing.RetryAmbient(ctx)
			want := context.Canceled
			if malformed {
				want = manifest.ErrInput
			}
			if artifact != nil || !errors.Is(err, want) || !errors.Is(err, manifest.ErrFlux) || errors.Is(err, manifest.ErrMissingSecret) {
				t.Fatalf("ambient acquisition = %v, %v; want %v and ErrFlux", artifact, err, want)
			}
		})
	}
}
