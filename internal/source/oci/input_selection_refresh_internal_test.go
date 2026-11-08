package oci

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

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
