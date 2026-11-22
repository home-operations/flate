package bucket

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/engine/source"
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

func TestFetcher_RootPolicyLimit(t *testing.T) {
	t.Parallel()
	const limit = 8 << 20
	const policy = "/prefix/drop.yaml\n"
	const comment = "# policy padding\n"
	padding := limit - len(policy)
	full := policy + strings.Repeat(comment, padding/len(comment)) + strings.Repeat("#", padding%len(comment))
	tooLarge := full + "extra"
	readFailure := errors.New("policy transport failed on the last bytes")
	for _, tc := range []rootPolicyCase{
		{
			name: "advertised overflow", body: tooLarge, size: limit + 5, flux: true,
			wantErr: `bucket object ".sourceignore" is 8388613 bytes, limit 8388608 bytes`,
		},
		{name: "unknown length", body: policy, size: -1, wantBytes: len(policy)},
		{
			name: "unknown overflow", body: tooLarge, size: -1, wantBytes: limit + 1, flux: true,
			wantErr: `bucket object ".sourceignore" is at least 8388609 bytes, limit 8388608 bytes`,
		},
		{name: "inclusive limit", body: full, size: limit, wantBytes: limit},
		{
			name: "one byte past limit", body: tooLarge, size: limit, wantBytes: limit + 1, flux: true,
			wantErr: `bucket object ".sourceignore" is at least 8388609 bytes, limit 8388608 bytes`,
		},
		{
			name: "truncated", body: policy, size: int64(len(policy) + 1), wantBytes: len(policy),
			wantErr: "read bucket policy: unexpected EOF",
		},
		{
			name: "understated length", body: policy, size: 1, wantBytes: 2,
			wantErr: `bucket object ".sourceignore" exceeds its 1-byte declared size`,
		},
		{
			name: "read error with final bytes", body: policy, size: int64(len(policy)), wantBytes: len(policy),
			readErr: readFailure, wantErr: "read bucket policy: policy transport failed on the last bytes",
		},
		{
			name: "unknown length read error at limit", body: full, size: -1, wantBytes: limit,
			readErr: readFailure, wantErr: "read bucket policy: policy transport failed on the last bytes",
		},
		{
			name: "failed refresh", body: tooLarge, size: limit + 5, flux: true, previousPolicy: policy,
			wantErr: `bucket object ".sourceignore" is 8388613 bytes, limit 8388608 bytes`,
		},
		{name: "missing", missing: true},
		{name: "empty"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testRootPolicyLimit(t, &tc)
		})
	}
}

type rootPolicyCase struct {
	name           string
	body           string
	previousPolicy string
	size           int64
	wantBytes      int
	readErr        error
	wantErr        string
	flux, missing  bool
}

func testRootPolicyLimit(t *testing.T, tc *rootPolicyCase) {
	t.Helper()
	b := &manifest.Bucket{
		Name: "policy-limit", Namespace: "test", BucketName: "data",
		Endpoint: "https://bucket.invalid", Region: "us-east-1", Prefix: "prefix/",
	}
	reads := (testInputs{}).readSet(b)
	t.Cleanup(reads.Close)
	op := operation{
		Fetcher: &Fetcher{Cache: cachetest.New(t, t.TempDir(), cache.New)},
		secrets: source.SecretLookup(reads),
	}
	request, err := op.prepare(b)
	assert.NoError(t, err)
	body := &countedPolicyBody{Reader: strings.NewReader(tc.body), readErr: tc.readErr}
	fixture := policyBucketTransport{t: t, body: body, size: tc.size}
	if tc.missing {
		fixture.body = nil
	}
	if tc.previousPolicy != "" {
		fixture.body = &countedPolicyBody{Reader: strings.NewReader(tc.previousPolicy)}
		fixture.size = int64(len(tc.previousPolicy))
	}
	request.options.Transport = bucketRoundTrip(fixture.respond)
	fetch := func() (*store.SourceArtifact, error) {
		return fetchtest.Fetch(t.Context(), func(
			_ context.Context, _ *manifest.Bucket, _ *store.ReadSet, p *cache.Policy,
		) (source.PreparedFetch, error) {
			return source.PreparedFetch{
				Fetch: func(ctx context.Context, _ *store.SourceArtifact) (*store.SourceArtifact, error) {
					return op.fetch(ctx, &request, p)
				},
			}, nil
		}, b, reads, &cache.Policy{}, nil)
	}
	var previous *store.SourceArtifact
	if tc.previousPolicy != "" {
		previous, err = fetch()
		assert.NoError(t, err)
		assert.Equal(t, fixture.objects.Swap(0), int32(1))
		assert.Equal(t, fixture.policies.Swap(0), int32(1))
		fixture.body, fixture.size = body, tc.size
	}

	artifact, err := fetch()
	assert.Equal(t, fixture.policies.Load(), int32(1))
	if !tc.missing {
		assert.Equal(t, body.bytesRead, tc.wantBytes)
		assert.Equal(t, body.closes, 1)
		if tc.size > 8<<20 {
			assert.Equal(t, body.reads, 0)
		}
	}
	if tc.wantErr != "" {
		assert.FatalIf(t, err == nil, "fetch succeeded with an invalid policy")
		assert.Equal(t, err.Error(), tc.wantErr)
		assert.Equal(t, artifact, nil)
		assert.Equal(t, fixture.objects.Load(), int32(0))
		if tc.flux {
			classified, ok := errors.AsType[*manifest.Error](err)
			assert.FatalIf(t, !ok, "missing classified policy error: %v", err)
			assert.Equal(t, classified.Category(), manifest.ErrFlux)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
		}
		if tc.readErr != nil {
			assert.Equal(t, errors.Is(err, tc.readErr), true)
		}
		if previous != nil {
			assertPolicyArtifact(t, previous, tc.previousPolicy, false)
		}
		return
	}
	assert.NoError(t, err)
	assertPolicyArtifact(t, artifact, tc.body, tc.missing)
}

func assertPolicyArtifact(t *testing.T, artifact *store.SourceArtifact, policy string, missing bool) {
	t.Helper()
	data, err := fs.ReadFile(artifact.FS, bucketPolicyFile)
	if missing {
		assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
	} else {
		assert.NoError(t, err)
		assert.ErrorIf(
			t,
			string(data) != policy,
			"policy contents differ: got %d bytes, want %d",
			len(data),
			len(policy),
		)
	}
	data, err = fs.ReadFile(artifact.FS, "prefix/keep.yaml")
	assert.NoError(t, err)
	assert.Equal(t, string(data), "x")
	_, err = fs.ReadFile(artifact.FS, "prefix/drop.yaml")
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), policy != "")
}

type policyBucketTransport struct {
	t        *testing.T
	body     *countedPolicyBody
	size     int64
	objects  atomic.Int32
	policies atomic.Int32
}

func (p *policyBucketTransport) respond(r *http.Request) (*http.Response, error) {
	if r.URL.Query().Get("prefix") == "prefix/" {
		const listing = `<ListBucketResult><Name>data</Name><IsTruncated>false</IsTruncated>
<Contents><Key>prefix/drop.yaml</Key><Size>1</Size><ETag>etag</ETag></Contents>
<Contents><Key>prefix/keep.yaml</Key><Size>1</Size><ETag>etag</ETag></Contents>
</ListBucketResult>`
		return bucketReply(r, http.StatusOK, listing), nil
	}
	res := bucketReply(r, http.StatusOK, "x")
	res.Header.Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
	res.Header.Set("ETag", `"etag"`)
	if r.URL.Path == "/data/"+bucketPolicyFile {
		assert.Equal(p.t, r.Method, http.MethodGet)
		p.policies.Add(1)
		if p.body == nil {
			return missingBucketPolicyResponse(r), nil
		}
		res.Body, res.ContentLength = p.body, p.size
	} else {
		p.objects.Add(1)
	}
	if res.ContentLength >= 0 {
		res.Header.Set("Content-Length", strconv.FormatInt(res.ContentLength, 10))
	}
	return res, nil
}

type countedPolicyBody struct {
	*strings.Reader

	readErr          error
	reads, bytesRead int
	closes           int
}

func (b *countedPolicyBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.reads++
	b.bytesRead += n
	if n > 0 && b.Len() == 0 && b.readErr != nil {
		return n, b.readErr
	}
	return n, err
}

func (b *countedPolicyBody) Close() error {
	b.closes++
	return nil
}
