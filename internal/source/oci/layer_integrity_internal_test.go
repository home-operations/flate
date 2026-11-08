package oci

import (
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/fetchtest"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFetcher_LayerDigestMismatchPublishesNothing(t *testing.T) {
	t.Parallel()
	for _, backing := range []string{"memory", "disk"} {
		for _, corruption := range []string{"digest", "gzip header", "gzip trailer"} {
			t.Run(backing+"/"+corruption, func(t *testing.T) {
				t.Parallel()
				testFetcherLayerDigestMismatchPublishesNothingCase(
					t,

					&backing,
					&corruption,
				)
			},
			)
		}
	}
}

func testFetcherLayerDigestMismatchPublishesNothingCase(
	t *testing.T, backing *string,
	corruption *string,
) {
	t.Helper()

	honest := newFakeOCIArtifact(t, map[string]string{"app.yaml": "version: v1\n"})
	broken := honest
	payload := testutil.TarGz(
		t,
		map[string]string{"app.yaml": "version: v2\n"},
		0o644,
		gzip.DefaultCompression,
	)
	if (*corruption) != "digest" {
		if (*corruption) == "gzip header" {
			payload = []byte{0x1f, 0x8b}
		} else {
			payload = honest.layer[:len(honest.layer)-4]
		}
		broken.layerDigest = sha256Digest(payload)
		broken.manifest = mustManifestJSON(t,
			manifestBlob{data: honest.config, mediaType: "application/vnd.cncf.flux.config.v1+json"},
			manifestBlob{data: payload, mediaType: "application/vnd.cncf.flux.content.v1.tar+gzip"},
		)
		broken.manifestDigest = sha256Digest(broken.manifest)
	} else {
		assert.FatalIf(t, len(payload) != len(honest.layer), "%s", "fixture lengths differ")
	}

	var repaired atomic.Bool
	var layers atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testFetcherLayerDigestMismatchPublishesNothingCaseCallback(
			w,
			r,
			testFetcherLayerDigestMismatchPublishesNothingCaseCallbackInputs{
				honest:   &honest,
				broken:   &broken,
				payload:  &payload,
				repaired: &repaired,
				layers:   &layers,
			},
		)
	},
	))
	t.Cleanup(srv.Close)
	root := ""
	if (*backing) == "disk" {
		root = t.TempDir()
	}
	c, err := sourcecache.New(t.Context(), root)
	assert.NoError(t, err)

	t.Cleanup(func() { _ = c.Close() })
	f := &Fetcher{Network: newTestNetwork(t), Cache: c}
	repo := &manifest.OCIRepository{
		Name:      "tampered",
		Namespace: "test",
		URL:       "oci://" + mustURL(t, srv.URL).Host + "/chart",
		Insecure:  true,
		Reference: &sourcev1.OCIRepositoryRef{
			Tag: "latest",
		},
		Interval: &metav1.Duration{Duration: time.Hour},
	}
	for attempt := range 3 {
		if attempt == 2 && (*backing) == "disk" {
			{
				closeErr2 := c.Close()
				assert.NoError(t, closeErr2)
			}

			c, err = sourcecache.New(t.Context(), root)
			assert.NoError(t, err)

			f.Cache = c
		}
		_, fetchErr := fetchtest.Fetch(
			t.Context(),
			f.Fetch,
			repo,
			(testInputs{}).readSet(repo),
			&sourcecache.Policy{MaxAge: repo.SourceInterval()},
			nil,
		)
		terminal, _ := errors.AsType[*sourcecache.TerminalError](fetchErr)
		if fetchErr == nil || terminal != nil && !terminal.Attempted || layers.Load() != int32(attempt+1) {
			t.Fatalf(
				"integrity failure suppressed: layers=%d terminal=%+v error=%v",
				layers.Load(),
				terminal,
				fetchErr,
			)
		}
		assert.FatalIf(
			t, (*corruption) !=
				"digest" && !errors.Is(fetchErr, io.ErrUnexpectedEOF), "lost gzip cause: %v",
			fetchErr,
		)
	}
	repaired.Store(true)
	art, err := fetchtest.Fetch(
		t.Context(),
		f.Fetch,
		repo,
		(testInputs{}).readSet(repo),
		&sourcecache.Policy{MaxAge: repo.SourceInterval()},
		nil,
	)
	assert.NoError(t, err)

	{
		got := mustReadFile(t, art.FS, "app.yaml")
		assert.FatalIf(t, got != "version: v1\n", "published %q", got)
	}
}

type testFetcherLayerDigestMismatchPublishesNothingCaseCallbackInputs struct {
	honest   *fakeOCIArtifact
	broken   *fakeOCIArtifact
	payload  *[]byte
	repaired *atomic.Bool
	layers   *atomic.Int32
}

func testFetcherLayerDigestMismatchPublishesNothingCaseCallback(
	w http.ResponseWriter,
	r *http.Request,
	testCaseInput testFetcherLayerDigestMismatchPublishesNothingCaseCallbackInputs,
) {
	current := (*testCaseInput.broken)
	if testCaseInput.repaired.Load() {
		current = (*testCaseInput.honest)
	}
	switch {
	case strings.Contains(r.URL.Path, "/manifests/"):
		w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
		w.Header().Set("Docker-Content-Digest", current.manifestDigest)
		_, _ = w.Write(current.manifest)
	case strings.HasSuffix(r.URL.Path, current.layerDigest):
		testCaseInput.layers.
			Add(1)
		if testCaseInput.repaired.Load() {
			_, _ = w.Write(testCaseInput.honest.layer)
		} else {
			_, _ = w.Write((*testCaseInput.payload))
		}
	case strings.HasSuffix(r.URL.Path, current.configDigest):
		_, _ = w.Write(current.config)
	}
}
