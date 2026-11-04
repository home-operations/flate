package testutil

import (
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
)

// ChartRegistry serves chart layers from an in-process TLS registry.
type ChartRegistry struct {
	URL, Digest   string
	Authenticated atomic.Int64
	Digests       []string
	TagGeneration atomic.Int64
	Requests      atomic.Uint64
}

// NewChartRegistry returns a local OCI registry with fixed chart contents.
func NewChartRegistry(t *testing.T, files []map[string]string, auth bool) *ChartRegistry {
	t.Helper()
	blobs := map[string][]byte{}
	manifests := map[string][]byte{}
	registry := &ChartRegistry{}
	descriptor := func(body []byte, mediaType string) ocispec.Descriptor {
		hash := digest.FromBytes(body)
		blobs[hash.String()] = body
		return ocispec.Descriptor{MediaType: mediaType, Digest: hash, Size: int64(len(body))}
	}
	const chartMode = 0o644
	const schemaVersion = 2
	for i, files := range files {
		chart := TarGz(t, files, chartMode, gzip.DefaultCompression)
		image := ocispec.Manifest{
			SchemaVersion: schemaVersion, MediaType: ocispec.MediaTypeImageManifest,
			Config: descriptor([]byte(`{}`), "application/vnd.cncf.helm.config.v1+json"),
			Layers: []ocispec.Descriptor{
				descriptor([]byte("unrelated provenance layer"), "application/vnd.cncf.helm.chart.provenance.v1.prov"),
				descriptor(chart, "application/vnd.cncf.helm.chart.content.v1.tar+gzip"),
			},
		}
		body, err := json.Marshal(image)
		assert.NoError(t, err)
		hash := digest.FromBytes(body).String()
		manifests[hash] = body
		registry.Digests = append(registry.Digests, hash)
		if i == 0 {
			registry.Digest = hash
			manifests["0.1.0"] = body
		} else {
			manifests["next"] = body
		}
	}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registry.Requests.Add(1)
		if auth {
			user, password, ok := r.BasicAuth()
			if !ok || user != "fixture" || password != "fixture-password" {
				w.Header().Set("WWW-Authenticate", `Basic realm="fixture"`)
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			registry.Authenticated.Add(1)
		}
		switch {
		case strings.Contains(r.URL.Path, "/missing/"):
			http.NotFound(w, r)
		case r.URL.Path == "/v2/":
		case strings.Contains(r.URL.Path, "/manifests/"):
			registry.serveManifest(w, r, manifests)
		case strings.Contains(r.URL.Path, "/blobs/"):
			_, key, _ := strings.CutLast(r.URL.Path, "/")
			if blob, ok := blobs[key]; ok {
				_, _ = w.Write(blob)
			} else {
				http.NotFound(w, r)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	registry.URL = "oci://" + strings.TrimPrefix(server.URL, "https://") + "/demo"
	return registry
}

func (r *ChartRegistry) serveManifest(w http.ResponseWriter, req *http.Request, manifests map[string][]byte) {
	_, ref, _ := strings.CutLast(req.URL.Path, "/")
	if ref == "0.1.0" {
		ref = r.Digests[r.TagGeneration.Load()]
	}
	body, ok := manifests[ref]
	if !ok {
		http.NotFound(w, req)
		return
	}
	w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
	w.Header().Set("Docker-Content-Digest", digest.FromBytes(body).String())
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	if req.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}
