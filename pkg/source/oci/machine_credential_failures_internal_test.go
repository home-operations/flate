package oci

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testFetcherMachineFailureCausesCase(t *testing.T, name *string,
) {
	t.Helper()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherMachineFailureCausesCaseCallback(
			w,
			r,
			testFetcherMachineFailureCausesCaseCallbackInputs{t: &t, name: &name},
		)
	},
	))
	t.Cleanup(srv.Close)
	path := filepath.Join(t.TempDir(), "config.json")
	config := `{}`
	if (*name) == "helper absence" {
		config = `{"credsStore":"fixture-helper"}`
	}
	if (*name) == "rejected credential" {
		config = fmt.Sprintf(
			`{"auths":{%q:{"username":"fixture-user","password":"private-password"}}}`,
			mustURL(t, srv.URL).Host,
		)
	}
	testutil.WriteFileAt(t, path, config)
	repo := ociRepo("advice", func(s *sourcev1.OCIRepositorySpec) {
		s.URL = "oci://" + mustURL(t, srv.URL).Host + "/app"
		s.Insecure = true
	})
	op := &operation{
		Fetcher: &Fetcher{
			Network:        newTestNetwork(t),
			Cache:          cachetest.New(t, "", sourcecache.New),
			RegistryConfig: path,
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	policy := sourcecache.Policy{}
	if (*name) == "canceled" {
		policy.Admit = func(context.Context) (func(), error) {
			cancel()
			return func() {}, nil
		}
	}
	artifact, err := op.fetchSource(ctx, repo, policy)
	assert.FatalIf(
		t,
		err == nil || artifact != nil || !errors.Is(err, manifest.ErrFlux),
		"failure lost source classification: %v",
		err,
	)
	wantAdvice := false
	for _, text := range []string{"no login entry for " + mustURL(t, srv.URL).Host, "Docker config", "Helm's registry file", "credential helpers are not run", "--registry-config", "plain auths entry", "supplied Secret"} {
		assert.ErrorIf(t, strings.Contains(err.Error(), text) != wantAdvice, "advice %q: %v", text, err)
	}
	assert.ErrorIf(
		t,
		strings.Contains(err.Error(), "OCIRepository ns/advice"),
		"source identity repeated: %v",
		err,
	)
	assert.ErrorIf(
		t, (*name) ==
			"Basic absence" && !errors.Is(err, auth.ErrBasicCredentialNotFound), "Basic absence sentinel lost: %v",
		err,
	)
	if (*name) == "bearer absence" || (*name) == "rejected credential" ||
		(*name) == "forbidden" {
		response, ok := errors.AsType[*errcode.ErrorResponse](err)
		assert.ErrorIf(t, !ok || response.StatusCode == 0, "ORAS response chain lost: %v", err)
		{
			_, ok := errors.AsType[*fetcherr.ResponseError](err)
			assert.ErrorIf(t, !ok, "neutral response chain lost: %v", err)
		}
	}
	assert.ErrorIf(
		t, (*name) ==
			"missing content" && !errors.Is(err, errdef.ErrNotFound), "missing content sentinel lost: %v",
		err,
	)
	assert.ErrorIf(
		t,
		(*name) == "canceled" && !errors.Is(err, context.Canceled),
		"cancellation lost: %v",
		err,
	)
	for _, secret := range []string{"fixture-helper", "fixture-user", "private-password", base64.StdEncoding.EncodeToString([]byte("fixture-user:private-password"))} {
		assert.ErrorIf(t, strings.Contains(err.Error(), secret), "credential leaked in error")
	}
}

type testFetcherMachineFailureCausesCaseCallbackInputs struct {
	t    **testing.T
	name **string
}

func testFetcherMachineFailureCausesCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherMachineFailureCausesCaseCallbackInputs,
) {
	switch *(*testCaseInput.name) {
	case "Basic absence", "helper absence", "rejected credential":
		w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
		w.WriteHeader(http.StatusUnauthorized)
	case "bearer absence":
		if r.URL.Path != "/token" {
			w.Header().
				Set("WWW-Authenticate", fmt.Sprintf(`Bearer realm=%q,service="registry",scope="repository:app:pull"`, "https://"+r.Host+"/token"))
		}
		w.WriteHeader(http.StatusUnauthorized)
	case "forbidden":
		http.Error(w, "denied", http.StatusForbidden)
	case "missing content":
		http.NotFound(w, r)
	case "malformed content":
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		_, _ = io.WriteString(w, "malformed manifest")
	case "transport":
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			(*testCaseInput.t).
				Error(err)
			return
		}
		_ = conn.Close()
	}
}
