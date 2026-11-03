package oci

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

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
		//nolint:errchkjson // Fixture payloads are encodable; client cancellation may fail their response writes.
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
	tc **struct {
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
