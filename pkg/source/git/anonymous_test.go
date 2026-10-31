package git

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
)

func TestFetcher_ProviderAnonymous(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustInitRepoWithFiles(t, root, map[string]string{"public.yaml": "public"})
	for _, private := range []bool{false, true} {
		t.Run(fmt.Sprint(private), func(t *testing.T) {
			t.Parallel()
			certificate, ca := tlsGitCertificate(t)
			handler := gitHTTPHandler(t, root)
			var advertisements, packs atomic.Int32
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "" {
					t.Error("anonymous Git sent credentials")
				}
				switch r.Method {
				case http.MethodGet:
					advertisements.Add(1)
				case http.MethodPost:
					packs.Add(1)
				}
				if private {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				handler.ServeHTTP(w, r)
			}))
			srv.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}}
			srv.StartTLS()
			t.Cleanup(srv.Close)
			repo := gitRepoWithSecret("public", srv.URL, "ca")
			repo.Provider = "aws"
			inputs := testInputs{secrets: func(string, string) *manifest.Secret {
				return &manifest.Secret{StringData: map[string]any{"ca.crt": ca}}
			}}
			reads := inputs.readSet(repo)
			defer reads.Close()
			f := &Fetcher{Cache: cachetest.New(t, "", cache.New)}
			_, err := f.Fetch(t.Context(), repo, reads, cache.Policy{})
			missing, ok := errors.AsType[*source.MissingSecretError](err)
			if !ok || missing.RetryAmbient == nil {
				t.Fatalf("missing continuation: %v", err)
			}
			got, err := missing.RetryAmbient(t.Context())
			if advertisements.Load() != 1 {
				t.Fatalf("advertisements=%d: %v", advertisements.Load(), err)
			}
			if private {
				if got != nil || !errors.Is(err, manifest.ErrMissingSecret) || packs.Load() != 0 {
					t.Fatalf("private acquisition=%v: %v", got, err)
				}
				return
			}
			if err != nil || got == nil || packs.Load() != 1 {
				t.Fatalf("public packs=%d: %v", packs.Load(), err)
			}
			data, err := fs.ReadFile(got.FS, "public.yaml")
			if err != nil || string(data) != "public" || len(got.Warnings) != 0 {
				t.Fatalf("public content=%q: %v", data, err)
			}
		})
	}
}
