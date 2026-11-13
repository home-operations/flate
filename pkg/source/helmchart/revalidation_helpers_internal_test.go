package helmchart

import (
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

type revalidationReply struct {
	body                   []byte
	etag, modified         string
	redirect, updatedETag  string
	status                 int
	ignore, omitValidators bool
}

type revalidationRequest struct{ etag, modified string }

type revalidationEndpoint struct {
	reply     atomic.Pointer[revalidationReply]
	last      atomic.Pointer[revalidationRequest]
	requests  atomic.Int32
	validated atomic.Int32
	unchanged atomic.Int32
	bodies    atomic.Int32
}

func (e *revalidationEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	e.requests.Add(1)
	etag, modified := r.Header.Get("If-None-Match"), r.Header.Get("If-Modified-Since")
	e.last.Store(&revalidationRequest{etag: etag, modified: modified})
	if etag != "" || modified != "" {
		e.validated.Add(1)
	}
	reply := e.reply.Load()
	if reply.redirect != "" {
		http.Redirect(w, r, reply.redirect, http.StatusFound)
		return
	}
	status := reply.status
	matchETag := reply.etag != "" && etag == reply.etag
	matchModified := reply.etag == "" && reply.modified != "" && modified == reply.modified
	if status == 0 && !reply.ignore && (matchETag || matchModified) {
		status = http.StatusNotModified
	}
	if !reply.omitValidators || status != http.StatusNotModified {
		if reply.etag != "" {
			w.Header().Set("ETag", reply.etag)
		}
		if reply.modified != "" {
			w.Header().Set("Last-Modified", reply.modified)
		}
	}
	if status == http.StatusNotModified {
		e.unchanged.Add(1)
		if reply.updatedETag != "" {
			w.Header().Set("ETag", reply.updatedETag)
		}
		w.WriteHeader(status)
		return
	}
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	e.bodies.Add(1)
	_, _ = w.Write(reply.body)
}

type helmRevalidationFixture struct {
	index, chart, alternate revalidationEndpoint
	fetcher                 *Fetcher
	repository              *httpRepository
	root                    string
}

func newHelmRevalidationFixture(t *testing.T) *helmRevalidationFixture {
	t.Helper()
	fixture := &helmRevalidationFixture{root: t.TempDir()}
	body := buildChartTarGz(t, "app-template", "1.0.0")
	fixture.chart.reply.Store(&revalidationReply{body: body, etag: `"chart"`})
	fixture.alternate.reply.Store(&revalidationReply{body: body, etag: `"alternate"`})
	fixture.index.reply.Store(&revalidationReply{body: []byte(helmRepoIndex(chartDigest(body))), etag: `"index"`})
	mux := http.NewServeMux()
	mux.Handle("/index.yaml", &fixture.index)
	mux.Handle("/app-template-1.0.0.tgz", &fixture.chart)
	mux.Handle("/alternate.tgz", &fixture.alternate)
	mux.Handle("/redirected", &fixture.alternate)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	r := httpRepo(server.URL)
	r.Interval.Duration = 0
	fixture.open(t, r)
	return fixture
}

func (f *helmRevalidationFixture) open(t *testing.T, repository *manifest.HelmRepository) {
	t.Helper()
	owner := cachetest.New(t, f.root, sourcecache.New)
	f.fetcher = New(nil, Config{Cache: owner, Network: newTestNetwork(t)})
	f.repository = preparedHTTP(t, f.fetcher, testInputs{}, repository)
}

func (f *helmRevalidationFixture) reopen(t *testing.T) {
	t.Helper()
	assert.NoError(t, f.fetcher.cache.Close())
	f.open(t, f.repository.HelmRepository)
}

func (f *helmRevalidationFixture) fetchIndex(t *testing.T) ([]byte, error) {
	t.Helper()
	index, err := f.fetcher.fetchIndex(t.Context(), f.repository, &sourcecache.Policy{})
	if err != nil {
		return nil, err
	}
	return json.Marshal(index)
}

func (f *helmRevalidationFixture) fetchChart(t *testing.T) (*store.SourceArtifact, error) {
	t.Helper()
	return f.fetcher.fetchHTTPChart(
		t.Context(),
		f.repository,
		helmChart("repo", "app-template", "*"),
		&sourcecache.Policy{},
	)
}

func removeHelmPayloads(t *testing.T, root string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "oci", "index.json"))
	assert.NoError(t, err)
	var index ocispec.Index
	assert.NoError(t, json.Unmarshal(data, &index))
	for _, descriptor := range index.Manifests {
		if descriptor.MediaType != "application/octet-stream" {
			continue
		}
		path := filepath.Join(root, "oci", "blobs", descriptor.Digest.Algorithm().String(), descriptor.Digest.Encoded())
		if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			t.Fatal(err)
		}
	}
}

func helmValidatorCases() []string {
	return []string{
		"etag",
		"last-modified",
		"ignored",
		"absent",
		"replace-200",
		"remove-200",
		"update-304",
		"retain-304",
		"invalid-304",
		"redirect",
		"missing-content",
	}
}

func testHelmValidators(
	t *testing.T,
	fixture *helmRevalidationFixture,
	endpoint *revalidationEndpoint,
	fetch func() ([]byte, error),
) {
	t.Helper()
	name := filepath.Base(t.Name())
	reply := *endpoint.reply.Load()
	if name == "last-modified" {
		reply.etag, reply.modified = "", "Mon, 01 Jun 2026 12:00:00 GMT"
	}
	if name == "absent" || name == "invalid-304" {
		reply.etag = ""
	}
	reply.ignore = name == "ignored"
	endpoint.reply.Store(&reply)
	first, err := fetch()
	assert.NoError(t, err)
	assert.Equal(t, endpoint.requests.Load(), int32(1))
	assert.Equal(t, endpoint.bodies.Load(), int32(1))
	wantBodies, want304, wantConditional := int32(1), int32(1), int32(1)
	switch name {
	case "ignored", "absent":
		wantBodies, want304 = 2, 0
	case "replace-200":
		reply.etag = `"replacement"`
		wantBodies, want304 = 2, 0
	case "remove-200":
		reply.etag = ""
		wantBodies, want304 = 2, 0
	case "update-304":
		reply.updatedETag = `"updated"`
	case "retain-304":
		reply.omitValidators = true
	case "invalid-304":
		reply.status = http.StatusNotModified
	case "redirect":
		reply.redirect = "/redirected"
		wantBodies, want304 = 1, 0
	case "missing-content":
		assert.NoError(t, fixture.fetcher.cache.Close())
		removeHelmPayloads(t, fixture.root)
		fixture.open(t, fixture.repository.HelmRepository)
		wantBodies, want304, wantConditional = 2, 0, 0
	}
	if name == "absent" || name == "invalid-304" {
		wantConditional = 0
	}
	endpoint.reply.Store(&reply)
	second, err := fetch()
	if name == "invalid-304" {
		assert.FatalIf(t, err == nil, "unexpected HTTP 304 succeeded without validators")
		assert.Equal(t, endpoint.bodies.Load(), int32(1))
		return
	}
	assert.NoError(t, err)
	assert.Equal(t, string(second), string(first))
	if name == "redirect" {
		assert.Equal(t, *fixture.alternate.last.Load(), revalidationRequest{})
	}
	assert.Equal(t, endpoint.requests.Load(), int32(2))
	assert.Equal(t, endpoint.bodies.Load(), wantBodies)
	assert.Equal(t, endpoint.unchanged.Load(), want304)
	assert.Equal(t, endpoint.validated.Load(), wantConditional)
	testHelmValidatorRefresh(t, endpoint, fetch, name)
}

func testHelmValidatorRefresh(t *testing.T, endpoint *revalidationEndpoint, fetch func() ([]byte, error), name string) {
	t.Helper()
	if name != "replace-200" && name != "remove-200" && name != "update-304" && name != "retain-304" {
		return
	}
	reply := *endpoint.reply.Load()
	if name == "update-304" {
		reply.etag, reply.updatedETag = reply.updatedETag, ""
	}
	endpoint.reply.Store(&reply)
	_, err := fetch()
	assert.NoError(t, err)
	assert.Equal(t, endpoint.last.Load().etag, reply.etag)
}
