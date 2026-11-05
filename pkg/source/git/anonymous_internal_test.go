package git

import (
	"crypto/tls"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	"github.com/home-operations/flate/internal/assert"
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
		t.Run(strconv.FormatBool(private), func(t *testing.T) {
			t.Parallel()
			testFetcherProviderAnonymousCase(t, &root, &private)
		},
		)
	}
}

func testFetcherProviderAnonymousCase(t *testing.T, root *string,
	private *bool,
) {
	t.Helper()

	certificate2 := tlsGitCertificate(t)

	handler := gitHTTPHandler(t, (*root))
	var advertisements, packs atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherProviderAnonymousCaseCallback(
			w,
			r,
			testFetcherProviderAnonymousCaseCallbackInputs{
				t:              &t,
				private:        &private,
				handler:        &handler,
				advertisements: &advertisements,
				packs:          &packs,
			},
		)
	},
	))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{certificate2.certificate}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	repo := gitRepoWithSecret("public", srv.URL, "ca")
	repo.Provider = "aws"
	inputs := testInputs{secrets: func(string, string) *manifest.Secret {
		return &manifest.Secret{StringData: map[string]any{"ca.crt": certificate2.pem}}
	}}
	reads := inputs.readSet(repo)
	defer reads.Close()
	f := &Fetcher{Cache: cachetest.New(t, "", cache.New)}
	_, err := fetchtest.Fetch(t.Context(), f.Fetch, repo, reads, &cache.Policy{}, nil)
	missing, ok := errors.AsType[*source.MissingSecretError](err)
	assert.FatalIf(t, !ok || missing.RetryAmbient == nil, "missing continuation: %v", err)
	got, err := fetchtest.Retry(t.Context(), missing.RetryAmbient)
	if advertisements.Load() != 1 {
		t.Fatalf("advertisements=%d: %v", advertisements.Load(), err)
	}
	if *private {
		assert.FatalIf(
			t,
			got != nil || !errors.Is(err, manifest.ErrMissingSecret) || packs.Load() != 0,
			"private acquisition=%v: %v",
			got,
			err,
		)
		return
	}
	if err != nil || got == nil || packs.Load() != 1 {
		t.Fatalf("public packs=%d: %v", packs.Load(), err)
	}
	data, err := fs.ReadFile(got.FS, "public.yaml")
	assert.FatalIf(
		t,
		err != nil || string(data) != "public" || len(got.Warnings) != 0,
		"public content=%q: %v",
		data,
		err,
	)
}

type testFetcherProviderAnonymousCaseCallbackInputs struct {
	t              **testing.T
	private        **bool
	handler        *http.Handler
	advertisements *atomic.Int32
	packs          *atomic.Int32
}

func testFetcherProviderAnonymousCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherProviderAnonymousCaseCallbackInputs,
) {
	if r.Header.Get("Authorization") != "" {
		(*testCaseInput.t).
			Error("anonymous Git sent credentials")
	}
	switch r.Method {
	case http.MethodGet:
		testCaseInput.advertisements.
			Add(1)
	case http.MethodPost:
		testCaseInput.packs.
			Add(1)
	}
	if *(*testCaseInput.private) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	(*testCaseInput.handler).
		ServeHTTP(w, r)
}
