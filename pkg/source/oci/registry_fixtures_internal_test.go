package oci

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
)

func startFakeRegistry(t *testing.T, manifestBytes, configBytes []byte, layers ...[]byte) *httptest.Server {
	t.Helper()
	blobs := map[string][]byte{sha256Digest(configBytes): configBytes}
	for _, layer := range layers {
		blobs[sha256Digest(layer)] = layer
	}
	manifestDigest := sha256Digest(manifestBytes)

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/v2/"):
			return
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", manifestDigest)
			_, _ = w.Write(manifestBytes)
		case strings.Contains(r.URL.Path, "/blobs/"):
			_, blobDigest, _ := strings.CutLast(r.URL.Path, "/")
			if blob, ok := blobs[blobDigest]; ok {
				_, _ = w.Write(blob)
				return
			}
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

type fakeOCIArtifact struct {
	manifest       []byte
	config         []byte
	layer          []byte
	manifestDigest string
	configDigest   string
	layerDigest    string
}

func newFakeOCIArtifact(t *testing.T, files map[string]string) fakeOCIArtifact {
	t.Helper()
	layer := testutil.TarGz(t, files, 0o644, gzip.DefaultCompression)
	config := []byte(`{}`)
	manifest := mustManifestJSON(t,
		manifestBlob{data: config, mediaType: "application/vnd.cncf.flux.config.v1+json"},
		manifestBlob{data: layer, mediaType: "application/vnd.cncf.flux.content.v1.tar+gzip"},
	)
	return fakeOCIArtifact{
		manifest:       manifest,
		config:         config,
		layer:          layer,
		manifestDigest: sha256Digest(manifest),
		configDigest:   sha256Digest(config),
		layerDigest:    sha256Digest(layer),
	}
}

func newMutableFakeRegistry(t *testing.T, artifacts ...fakeOCIArtifact) mutableRegistry {
	t.Helper()
	assert.FatalIf(t, len(artifacts) == 0, "%s", "no OCI artifacts")

	var (
		mu      sync.RWMutex
		current int
		blobs   = map[string][]byte{}
	)
	for _, art := range artifacts {
		blobs[art.configDigest] = art.config
		blobs[art.layerDigest] = art.layer
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/v2/"):
			return
		case strings.Contains(r.URL.Path, "/manifests/"):
			mu.RLock()
			art := artifacts[current]
			mu.RUnlock()
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", art.manifestDigest)
			_, _ = w.Write(art.manifest)
		case strings.Contains(r.URL.Path, "/blobs/"):
			_, d, _ := strings.CutLast(r.URL.Path, "/")
			if body, ok := blobs[d]; ok {
				_, _ = w.Write(body)
				return
			}
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	})
	srv := httptest.NewUnstartedServer(mux)
	t.Cleanup(srv.Close)
	setArtifact := func(i int) {
		mu.Lock()
		defer mu.Unlock()
		current = i
	}
	return mutableRegistry{server: srv, setArtifact: setArtifact}
}

type manifestBlob struct {
	data      []byte
	mediaType string
}

func mustManifestJSON(t *testing.T, config manifestBlob, layers ...manifestBlob) []byte {
	t.Helper()
	m := ocispec.Manifest{
		SchemaVersion: 2,
		MediaType:     ocispec.MediaTypeImageManifest,
		Config: ocispec.Descriptor{
			MediaType: config.mediaType,
			Digest:    digest.Digest(sha256Digest(config.data)),
			Size:      int64(len(config.data)),
		},
	}
	for _, layer := range layers {
		m.Layers = append(m.Layers, ocispec.Descriptor{
			MediaType: layer.mediaType,
			Digest:    digest.Digest(sha256Digest(layer.data)),
			Size:      int64(len(layer.data)),
		})
	}
	b, err := json.Marshal(m)
	assert.NoError(t, err)

	return b
}

func sha256Digest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	assert.NoError(t, err)

	return u
}

func mustReadFile(t *testing.T, input fs.FS, path string) string {
	t.Helper()
	b, err := fs.ReadFile(input, path)
	assert.NoError(t, err)

	return string(b)
}

type mutableRegistry struct {
	server      *httptest.Server
	setArtifact func(int)
}
