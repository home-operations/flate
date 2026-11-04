package oci

import (
	"bytes"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testFetcherRevalidationCase(t *testing.T, operation *string,
) {
	t.Helper()

	v1 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	v2 := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v2\n"})
	srv, setArtifact := newMutableFakeRegistry(t, v1, v2)
	var heads, manifests, layers atomic.Int32
	var failResolve atomic.Bool
	handler := srv.Config.Handler
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherRevalidationCaseCallback(
			w,
			r,
			testFetcherRevalidationCaseCallbackInputs{
				heads:       &heads,
				manifests:   &manifests,
				layers:      &layers,
				failResolve: &failResolve,
				handler:     &handler,
			},
		)
	},
	)
	srv.StartTLS()
	root := t.TempDir()
	f := &Fetcher{Network: newTestNetwork(t), Cache: cachetest.New(t, root, sourcecache.New)}
	repo := &manifest.OCIRepository{
		Name:          "revalidate",
		Namespace:     "test",
		URL:           "oci://" + mustURL(t, srv.URL).Host + "/app",
		Insecure:      true,
		Reference:     &sourcev1.OCIRepositoryRef{Tag: "latest"},
		LayerSelector: &sourcev1.OCILayerSelector{Operation: (*operation)},
	}
	var identity string
	for _, phase := range []string{"initial", "unchanged", "reopened", "changed", "missing", "truncated", "resolve failure"} {
		if phase == "missing" || phase == "truncated" {
			payload := v2.layer
			payload = revalidationPayload(t, payload, operation)
			found := 0
			err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
				return testFetcherRevalidationCaseCallback2(
					path,
					entry,
					err,
					testFetcherRevalidationCaseCallback2Inputs{phase: &phase, payload: &payload, found: &found},
				)
			},
			)
			assert.FatalIf(t, err != nil || found != 1, "%s backing: found=%d error=%v", phase, found, err)
		}
		if phase == "reopened" || phase == "missing" || phase == "truncated" {
			{
				err := f.Cache.Close()
				assert.NoError(t, err)
			}

			f.Cache = cachetest.New(t, root, sourcecache.New)
		}
		if phase == "changed" {
			setArtifact(1)
		}
		failResolve.Store(phase == "resolve failure")
		beforeHeads, beforeManifests, beforeLayers := heads.Load(), manifests.Load(), layers.Load()
		art, err := f.Fetch(
			t.Context(),
			repo,
			(testInputs{}).readSet(repo),
			sourcecache.Policy{MaxAge: repo.SourceInterval()},
		)
		if phase == "resolve failure" {
			assert.FatalIf(
				t,
				err == nil || !errors.Is(err, manifest.ErrFlux) || art != nil,
				"stale revalidation result: artifact=%+v error=%v",
				art,
				err,
			)
			assert.FatalIf(t, heads.Load()-beforeHeads != 1 || manifests.Load() != beforeManifests ||
				layers.Load() != beforeLayers, "%s", "failed resolve requested content")

			continue
		}
		assert.NoError(t, err)

		wantLayers := int32(0)
		if phase == "initial" || phase == "changed" || phase == "missing" || phase == "truncated" {
			wantLayers = 1
		}
		h, m, l := heads.Load()-beforeHeads, manifests.Load()-beforeManifests, layers.Load()-beforeLayers
		assert.FatalIf(
			t,
			h != 1 || m != wantLayers || l != wantLayers,
			"%s HEAD/manifest GET/layer GET=%d/%d/%d, want 1/%d/%d",
			phase,
			h,
			m,
			l,
			wantLayers,
			wantLayers,
		)
		t.Logf("%s HEAD/manifest GET/layer GET=%d/%d/%d", phase, h, m, l)
		got := sourcecache.AcquisitionIdentity(art.FS)
		checkRevalidationIdentity(t, phase, &identity, got)

		checkRevalidationOutput(t, revalidationOutput{
			operation: *operation, phase: phase, v1: &v1, v2: &v2, art: art,
		})
	}
}

type testFetcherRevalidationCaseCallbackInputs struct {
	heads       *atomic.Int32
	manifests   *atomic.Int32
	layers      *atomic.Int32
	failResolve *atomic.Bool
	handler     *http.Handler
}

func testFetcherRevalidationCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherRevalidationCaseCallbackInputs,
) {
	switch {
	case strings.Contains(r.URL.Path, "/manifests/") && r.Method == http.MethodHead:
		testCaseInput.heads.
			Add(1)
		if testCaseInput.failResolve.Load() {
			http.Error(w, "resolution failed", http.StatusBadRequest)
			return
		}
	case strings.Contains(r.URL.Path, "/manifests/"):
		testCaseInput.manifests.
			Add(1)
	case strings.Contains(r.URL.Path, "/blobs/"):
		testCaseInput.layers.
			Add(1)
	}
	(*testCaseInput.handler).
		ServeHTTP(w, r)
}

type testFetcherRevalidationCaseCallback2Inputs struct {
	phase   *string
	payload *[]byte
	found   *int
}

func testFetcherRevalidationCaseCallback2(
	path string,
	entry fs.DirEntry,
	err error,
	testCaseInput testFetcherRevalidationCaseCallback2Inputs,
) error {
	if err != nil || !entry.Type().IsRegular() {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, (*testCaseInput.payload)) {
		return err
	}
	(*testCaseInput.found)++
	if (*testCaseInput.phase) == "missing" {
		return os.Remove(path)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return err
	}
	return os.WriteFile(path, []byte("truncated"), 0o600)
}
