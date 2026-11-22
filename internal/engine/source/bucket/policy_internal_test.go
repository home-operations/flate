package bucket

import (
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/engine/source/cache"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/internal/testutil/fetchtest"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestFetcher_RootPolicyRefresh(t *testing.T) {
	t.Parallel()
	const nested = "prefix/.sourceignore"
	const later = "prefix/add.yaml"
	const kept = "prefix/keep.yaml"
	const excluded = "prefix/skip.yaml"
	before := "/" + later + "\n/" + kept + "\n/" + excluded + "\n"
	after := "/" + kept + "\n/" + excluded + "\n"
	var rules atomic.Pointer[string]
	rules.Store(&before)
	var delayed, rejected atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("list-type") {
			assert.Equal(t, r.URL.Query().Get("prefix"), "prefix/")
			_, _ = io.WriteString(w, "<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>")
			for _, key := range []string{nested, later, kept, excluded} {
				_, _ = fmt.Fprintf(w, "<Contents><Key>%s</Key><Size>1</Size><ETag>etag</ETag></Contents>", key)
			}
			_, _ = io.WriteString(w, "</ListBucketResult>")
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/data/")
		body := "x"
		switch key {
		case ".sourceignore":
			body = *rules.Load()
		case later:
			delayed.Add(1)
		case excluded:
			rejected.Add(1)
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.Header().Set("ETag", `"etag"`)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	fetcher := &Fetcher{Cache: cachetest.New(t, t.TempDir(), cache.New), Network: newTestNetwork(t)}
	bucket := &manifest.Bucket{
		Name: "root-policy", Namespace: "test", BucketName: "data",
		Endpoint: server.URL, Region: "us-east-1", Insecure: true, Prefix: "prefix/",
	}
	bucket.Ignore = new("!/" + kept + "\n")
	reads := (testInputs{}).readSet(bucket)
	t.Cleanup(reads.Close)
	first, err := fetchtest.Fetch(t.Context(), fetcher.Fetch, bucket, reads, &cache.Policy{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, first.Metadata["objectCount"], "2")
	assert.Equal(t, delayed.Load(), int32(0))
	rules.Store(&after)
	second, err := fetchtest.Fetch(t.Context(), fetcher.Fetch, bucket, reads, &cache.Policy{}, nil)
	assert.NoError(t, err)
	assert.Equal(t, second.Metadata["objectCount"], "3")
	assert.Equal(t, second.Key, first.Key)
	assert.ErrorIf(t, second.Revision == first.Revision, "policy change retained the previous bucket revision")
	assert.Equal(t, delayed.Load(), int32(1))
	assert.Equal(t, rejected.Load(), int32(0))
	for path, want := range map[string]string{".sourceignore": after, nested: "x", later: "x", kept: "x"} {
		data, readErr := fs.ReadFile(second.FS, path)
		assert.NoError(t, readErr)
		assert.Equal(t, string(data), want)
	}

	commented := "# Same membership.\n" + after
	rules.Store(&commented)
	warm, err := fetchtest.Fetch(t.Context(), fetcher.Fetch, bucket, reads, &cache.Policy{}, nil)
	assert.NoError(t, err)
	coldFetcher := &Fetcher{Cache: cachetest.New(t, t.TempDir(), cache.New), Network: fetcher.Network}
	cold, err := fetchtest.Fetch(t.Context(), coldFetcher.Fetch, bucket, reads, &cache.Policy{}, nil)
	assert.NoError(t, err)
	for _, artifact := range []*store.SourceArtifact{warm, cold} {
		data, err := fs.ReadFile(artifact.FS, bucketPolicyFile)
		assert.NoError(t, err)
		assert.Equal(t, string(data), commented)
		assert.Equal(t, artifact.Revision, second.Revision)
	}
	assert.Equal(t, warm.Content, cold.Content)
}
