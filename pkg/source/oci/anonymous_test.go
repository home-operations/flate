package oci

import (
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_ProviderAnonymous(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	for _, private := range []bool{false, true} {
		t.Run(fmt.Sprint(private), func(t *testing.T) {
			artifact := newFakeOCIArtifact(t, map[string]string{"public.yaml": "public"})
			upstream := startFakeRegistry(t, artifact.manifest, artifact.config, artifact.layer)
			var challenges, tokens, manifests atomic.Int32
			srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "" && r.Header.Get("Authorization") != "Bearer public" {
					t.Error("anonymous acquisition sent credentials")
				}
				if r.URL.Path == "/token" {
					tokens.Add(1)
					if r.URL.Query().Get("account") != "" {
						t.Error("anonymous token request had an account")
					}
					if private {
						w.WriteHeader(http.StatusForbidden)
						return
					}
					_, _ = w.Write([]byte(`{"token":"public"}`))
					return
				}
				if r.Header.Get("Authorization") == "" {
					challenges.Add(1)
					w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm="https://%s/token",service="fixture",scope="repository:fixture:pull"`, r.Host))
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if strings.Contains(r.URL.Path, "/manifests/") && r.Method == http.MethodHead {
					manifests.Add(1)
				}
				upstream.Config.Handler.ServeHTTP(w, r)
			}))
			t.Cleanup(srv.Close)
			config := filepath.Join(t.TempDir(), "config.json")
			testutil.WriteFileAt(t, config, "{}")
			repo := &manifest.OCIRepository{Name: "public", Namespace: "apps", URL: "oci://" + mustURL(t, srv.URL).Host + "/fixture", Provider: "aws", Insecure: true}
			f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, "", cache.New), RegistryConfig: config}
			got, err := f.Fetch(t.Context(), repo, (testInputs{}).readSet(repo), cache.Policy{Retry: cache.RetryConfig{Attempts: 3}})
			if challenges.Load() != 1 || tokens.Load() != 1 {
				t.Fatalf("challenge sequences=%d token exchanges=%d: %v", challenges.Load(), tokens.Load(), err)
			}
			if private {
				if got != nil || !errors.Is(err, manifest.ErrMissingSecret) || manifests.Load() != 0 || !strings.Contains(err.Error(), "machine credential acquisition failed") {
					t.Fatalf("private acquisition=%v: %v", got, err)
				}
				return
			}
			if err != nil || got == nil || manifests.Load() != 1 {
				t.Fatalf("public acquisition: manifests=%d: %v", manifests.Load(), err)
			}
			data, err := fs.ReadFile(got.FS, "public.yaml")
			if err != nil || string(data) != "public" || len(got.Warnings) != 0 {
				t.Fatalf("public content=%q: %v", data, err)
			}
		})
	}
}
