package source

import (
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/schedule"

	"github.com/home-operations/flate/internal/assert"

	"github.com/opencontainers/go-digest"

	src "github.com/home-operations/flate/internal/source"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/source/oci"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/internal/task"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // This test changes process-global state.
//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestController_RenderedSecretWinsOverRegistryConfig(t *testing.T) {
	for _, name := range []string{"stale global credentials", "unrelated global credentials", "corrupt global config", "stale Helm credentials", "corrupt Helm config"} {
		t.Run(name, func(t *testing.T) {
			testControllerRenderedSecretWinsOverRegistryConfigCase(
				t,
				&name,
			)
		},
		)
	}
}

func testControllerRenderedSecretWinsOverRegistryConfigCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()
	const user, pass = "alice", "hunter2"
	configBytes := []byte(`{}`)
	configDigest := digest.FromBytes(configBytes)
	layerBytes := testutil.TarGz(
		t,
		map[string]string{"app.yaml": "kind: ConfigMap\n"},
		0o644,
		gzip.DefaultCompression,
	)
	layerDigest := digest.FromBytes(layerBytes)
	manifestBytes := fmt.Appendf(
		nil,
		`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.cncf.flux.config.v1+json","digest":%q,"size":%d},"layers":[{"mediaType":"application/vnd.cncf.flux.content.v1.tar+gzip","digest":%q,"size":%d}]}`,
		configDigest,
		len(configBytes),
		layerDigest,
		len(layerBytes),
	)
	manifestDigest := digest.FromBytes(manifestBytes)
	var requests, wrongCredentials atomic.Int64
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if u, p, ok := r.BasicAuth(); !ok || u != user || p != pass {
			if ok {
				wrongCredentials.Add(1)
			}
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Header().Set("Docker-Content-Digest", manifestDigest.String())
			_, _ = w.Write(manifestBytes)
		case strings.HasSuffix(r.URL.Path, "/blobs/"+configDigest.String()):
			_, _ = w.Write(configBytes)
		case strings.HasSuffix(r.URL.Path, "/blobs/"+layerDigest.String()):
			_, _ = w.Write(layerBytes)
		case r.URL.Path == "/v2/":
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	host := strings.TrimPrefix(srv.URL, "https://")
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	t.Setenv("HELM_REGISTRY_CONFIG", filepath.Join(t.TempDir(), "absent.json"))
	config := filepath.Join(t.TempDir(), "config.json")
	staleAuth := base64.StdEncoding.EncodeToString([]byte(user + ":stale"))
	configJSON := fmt.Sprintf(`{"auths":{%q:{"auth":%q}}}`, host, staleAuth)
	if (*testCaseInput) == "unrelated global credentials" {
		configJSON = fmt.Sprintf(`{"auths":{"quay.io":{"auth":%q}}}`, staleAuth)
	}
	testControllerRenderedSecretWinsOverRegistryConfigCaseTail(
		t,
		&testControllerRenderedSecretWinsOverRegistryConfigCaseTailInputs{
			testCaseInput:    &testCaseInput,
			configJSON:       &configJSON,
			config:           &config,
			host:             &host,
			requests:         &requests,
			user:             user,
			pass:             pass,
			wrongCredentials: &wrongCredentials,
		},
	)
}

func testControllerRenderedSecretWinsOverRegistryConfigCaseTail(
	t *testing.T,
	tailCaseInput *testControllerRenderedSecretWinsOverRegistryConfigCaseTailInputs,
) {
	t.Helper()
	if (*(*tailCaseInput.testCaseInput)) == "corrupt global config" ||
		(*(*tailCaseInput.testCaseInput)) == "corrupt Helm config" {
		(*tailCaseInput.configJSON) = `{"auths":`
	}
	testutil.WriteFileAt(t, (*tailCaseInput.config), (*tailCaseInput.configJSON))
	root := t.TempDir()
	st := store.New()
	cache := cachetest.New(t, root, sourcecache.New)
	network := new(src.Network)
	t.Cleanup(network.Close)
	f := &oci.Fetcher{
		Cache: cache, Network: network, RegistryConfig: (*tailCaseInput.config),
	}
	if strings.Contains((*(*tailCaseInput.testCaseInput)), "Helm") {
		f.RegistryConfig = ""
		t.Setenv("HELM_REGISTRY_CONFIG", (*tailCaseInput.config))
	}
	ts := task.NewBounded(2)
	c := New(st, ts)
	c.Fetchers[manifest.KindOCIRepository] = src.Wrap(manifest.KindOCIRepository, f)
	c.StartLifecycle()
	t.Cleanup(func() {
		ts.BlockTillDone()
	})
	repos := []*manifest.OCIRepository{
		{Name: "first", Namespace: "ns"},
		{Name: "second", Namespace: "ns"},
	}
	for _, repo := range repos {
		repo.URL = "oci://" + (*tailCaseInput.host) + "/app"
		repo.Insecure = true
		repo.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
		st.AddObject(repo)
		ts.Go(t.Context(), repo.Name, func(ctx context.Context) {
			deps := c.ReconcileNode(ctx, repo.Named(), 0, schedule.Result{}).Blocked
			assert.ErrorIf(t, len(deps) != 1 ||
				deps[0] != (manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "creds"}), "blocked dependencies = %v, want Secret ns/creds", deps)
		})
	}
	ts.BlockTillDone()
	{
		got := tailCaseInput.requests.Load()
		assert.FatalIf(t, got != 0, "requests before Secret render = %d, want 0", got)
	}
	auth := base64.StdEncoding.EncodeToString([]byte(tailCaseInput.user + ":" + tailCaseInput.pass))
	st.AddObject(&manifest.Secret{
		Name:      "creds",
		Namespace: "ns",
		StringData: map[string]any{
			".dockerconfigjson": fmt.Sprintf(`{"auths":{%q:{"auth":%q}}}`, (*tailCaseInput.host), auth),
		},
	})
	for _, repo := range repos {
		ts.Go(t.Context(), repo.Name, func(ctx context.Context) {
			{
				deps := c.ReconcileNode(ctx, repo.Named(), 0, schedule.Result{}).Blocked
				assert.ErrorIf(
					t,
					len(deps) != 0,
					"blocked dependencies after Secret render = %v, want none",
					deps,
				)
			}
		})
	}
	ts.BlockTillDone()
	for _, repo := range repos {
		if info, _ := st.GetStatus(
			repo.Named(),
		); info.Status != store.StatusReady || info.Outcome() == store.OutcomeSkipped ||
			st.GetArtifact(repo.Named(), store.ArtifactContent) == nil {
			t.Errorf("source %s status = %+v, want Ready with artifact", repo.Name, info)
		}
	}
	if tailCaseInput.requests.Load() == 0 || tailCaseInput.wrongCredentials.Load() != 0 {
		t.Errorf(
			"requests = %d, wrong credentials = %d, want successful rendered-Secret authentication",
			tailCaseInput.requests.
				Load(),
			tailCaseInput.wrongCredentials.
				Load(),
		)
	}
}

type testControllerRenderedSecretWinsOverRegistryConfigCaseTailInputs struct {
	testCaseInput **string

	configJSON *string

	config *string

	host *string

	requests *atomic.
			Int64

	user string

	pass string

	wrongCredentials *atomic.
				Int64
}
