package source

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/opencontainers/go-digest"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	src "github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/source/oci"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestController_RenderedSecretWinsOverRegistryConfig(t *testing.T) {
	for _, name := range []string{"stale global credentials", "corrupt global config"} {
		t.Run(name, func(t *testing.T) {
			const user, pass = "alice", "hunter2"
			configBytes := []byte(`{}`)
			configDigest := digest.FromBytes(configBytes)
			manifestBytes := fmt.Appendf(nil, `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.cncf.flux.config.v1+json","digest":%q,"size":%d},"layers":[]}`, configDigest, len(configBytes))
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
				case r.URL.Path == "/v2/":
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(srv.Close)
			host := strings.TrimPrefix(srv.URL, "https://")
			t.Setenv("DOCKER_CONFIG", t.TempDir())
			config := filepath.Join(t.TempDir(), "config.json")
			staleAuth := base64.StdEncoding.EncodeToString([]byte(user + ":stale"))
			configJSON := fmt.Sprintf(`{"auths":{%q:{"auth":%q}}}`, host, staleAuth)
			if name == "corrupt global config" {
				configJSON = `{"auths":`
			}
			testutil.WriteFileAt(t, config, configJSON)
			layout := cacheroot.New(t.TempDir())
			st := store.New()
			f := &oci.Fetcher{
				Cache: src.NewCache(layout), RegistryConfig: config,
				Secrets: func(ns, name string) *manifest.Secret {
					sec, _ := st.GetByName[*manifest.Secret](manifest.KindSecret, ns, name)
					return sec
				},
			}
			ts := task.NewBounded(2)
			c := New(st, ts)
			c.Fetchers[manifest.KindOCIRepository] = src.Wrap(manifest.KindOCIRepository, f)
			c.Start(t.Context())
			t.Cleanup(func() {
				c.Close()
				ts.BlockTillDone()
			})
			repos := []*manifest.OCIRepository{
				{Name: "first", Namespace: "ns"},
				{Name: "second", Namespace: "ns"},
			}
			for _, repo := range repos {
				repo.URL = "oci://" + host + "/app"
				repo.Insecure = true
				repo.SecretRef = &manifest.LocalObjectReference{Name: "creds"}
				st.AddObject(repo)
				ts.Go(t.Context(), repo.Name, func(ctx context.Context) {
					deps := c.ReconcileNode(ctx, repo.Named(), 0)
					if len(deps) != 1 || deps[0] != (manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "ns", Name: "creds"}) {
						t.Errorf("blocked dependencies = %v, want Secret ns/creds", deps)
					}
				})
			}
			ts.BlockTillDone()
			if got := requests.Load(); got != 0 {
				t.Fatalf("requests before Secret render = %d, want 0", got)
			}
			auth := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
			st.AddObject(&manifest.Secret{
				Name: "creds", Namespace: "ns",
				StringData: map[string]any{".dockerconfigjson": fmt.Sprintf(`{"auths":{%q:{"auth":%q}}}`, host, auth)},
			})
			for _, repo := range repos {
				ts.Go(t.Context(), repo.Name, func(ctx context.Context) {
					if deps := c.ReconcileNode(ctx, repo.Named(), 0); len(deps) != 0 {
						t.Errorf("blocked dependencies after Secret render = %v, want none", deps)
					}
				})
			}
			ts.BlockTillDone()
			for _, repo := range repos {
				if info, _ := st.GetStatus(repo.Named()); info.Status != store.StatusReady || store.IsSkipped(info) || st.GetArtifact(repo.Named()) == nil {
					t.Errorf("source %s status = %+v, want Ready with artifact", repo.Name, info)
				}
			}
			if requests.Load() == 0 || wrongCredentials.Load() != 0 {
				t.Errorf("requests = %d, wrong credentials = %d, want successful rendered-Secret authentication", requests.Load(), wrongCredentials.Load())
			}
		})
	}
}
