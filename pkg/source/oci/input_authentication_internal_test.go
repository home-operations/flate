package oci

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

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
