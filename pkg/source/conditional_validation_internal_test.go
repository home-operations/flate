package source

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func testAcquireConditionalValidationOwnerUserinfo(t *testing.T) {
	t.Helper()

	const password = "owner-cache-test-password"
	const token = "owner-cache-test-query-token"
	const content = "stable: true\n"
	var trace remoteHTTPTrace
	srv := trace.serve(t, func() string { return content })
	endpoint, err := url.Parse(srv.URL + "/input?token=" + token)
	assert.NoError(t, err)

	endpoint.User = url.UserPassword("reader", password)
	root := t.TempDir()
	disk := os.DirFS(root)
	var firstIdentity string
	for run := range 2 {
		c := newTestHTTPInputs(t, root)
		input, err := c.Acquire(
			t.Context(),
			&HTTPRequest{URL: endpoint.String(), Retry: sourcecache.RetryConfig{}, HeldSlot: nil},
		)
		assert.NoError(t, err)

		body, err := fs.ReadFile(input, "content")
		assert.NoError(t, err)
		assert.Equal(t, string(body), content)
		identity := sourcecache.AcquisitionIdentity(input)
		if identity == "" || run == 1 && identity != firstIdentity {
			t.Error("unchanged authenticated input did not reuse acquisition identity")
		}
		firstIdentity = identity
		closeTestHTTPInputs(t, c)
		assert.NoError(t, fs.WalkDir(disk, ".", func(name string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return err
			}
			data, err := fs.ReadFile(disk, name)
			if err != nil {
				return err
			}
			assert.ErrorIf(
				t,
				strings.Contains(string(data), password) || strings.Contains(string(data), token),
				"persistent cache file %s contains URL credentials",
				name,
			)
			return nil
		}))
	}
	assert.Diff(t, trace.snapshot(), []remoteHTTPRequest{
		{Method: http.MethodGet, Path: "/input", Status: http.StatusOK, Bytes: len(content)},
		{
			Method:   http.MethodGet,
			Path:     "/input",
			ETag:     `"` + manifest.Fingerprint(content) + `"`,
			Modified: "Mon, 01 Jun 2026 12:00:00 GMT",
			Status:   http.StatusNotModified,
		},
	})
}

func testAcquireConditionalValidationOwnerMissing(t *testing.T) {
	t.Helper()

	var trace remoteHTTPTrace
	srv := trace.serve(t, func() string { return "changed" })
	inputURL := srv.URL + "/input?version=2"
	root := t.TempDir()
	c := newTestHTTPInputs(t, root)
	reader, _, _, err := c.owner.Blob(
		t.Context(),
		sourcecache.BlobKey{Content: struct{ URL string }{inputURL}},
		&sourcecache.Policy{},
		func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			annotations := map[string]string{
				httpETagAnnotation:         `"` + manifest.Fingerprint("changed") + `"`,
				httpLastModifiedAnnotation: "Mon, 01 Jun 2026 12:00:00 GMT",
			}
			size, err := io.WriteString(w, "original")
			return sourcecache.ProducerResult{Size: int64(size), Annotations: annotations}, err
		},
	)
	assert.NoError(t, err)

	firstIdentity := sourcecache.AcquisitionIdentity(sourcecache.NameBlob("content", reader))

	closeTestHTTPInputs(t, c)
	c = newTestHTTPInputs(t, root)
	input, err := c.Acquire(t.Context(), &HTTPRequest{URL: inputURL, Retry: sourcecache.RetryConfig{}, HeldSlot: nil})
	assert.NoError(t, err)

	body, err := fs.ReadFile(input, "content")
	assert.NoError(t, err)
	assert.Equal(t, string(body), "changed")
	if sourcecache.AcquisitionIdentity(input) == firstIdentity {
		t.Error("different owner retained previous acquisition identity")
	}
	assert.Diff(
		t,
		trace.snapshot(),
		[]remoteHTTPRequest{{Method: http.MethodGet, Path: "/input", Status: http.StatusOK, Bytes: len("changed")}},
	)
	closeTestHTTPInputs(t, c)
}

func testAcquireConditionalValidationCase3(t *testing.T, scenario *string,
) {
	t.Helper()

	root := t.TempDir()
	const inputURL = "https://reader:owner-cache-test-password@example.invalid/input"
	etag := `W/"opaque-tag"`
	closeErr := errors.New("304 close failed")
	var firstIdentity string
	for phase := range 3 {
		c := newTestHTTPInputs(t, root)
		requests := 0
		c.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
			return testAcquireConditionalValidationCase3Callback(
				r,
				testAcquireConditionalValidationCase3CallbackInputs{
					t:        &t,
					scenario: &scenario,
					etag:     &etag,
					closeErr: &closeErr,
					phase:    &phase,
					requests: &requests,
				},
			)
		},
		)
		input, err := c.Acquire(
			t.Context(),
			&HTTPRequest{URL: inputURL, Retry: sourcecache.RetryConfig{}, HeldSlot: nil},
		)
		assert.FatalIf(t, requests != 1, "phase=%d requests=%d, want 1", phase, requests)
		if (*scenario) == "unsolicited-304" ||
			phase == 1 &&
				((*scenario) == "unauthorized" || (*scenario) == "close-304" || (*scenario) == "unvalidated-304") {
			assert.FatalIf(t, err == nil || input != nil, "invalid validation succeeded: %v", err)
			assert.FatalIf(
				t, (*scenario) ==
					"close-304" && !errors.Is(err, closeErr), "close identity lost: %v",
				err,
			)
			closeTestHTTPInputs(t, c)
			break
		}
		assert.NoError(t, err)

		identity := sourcecache.AcquisitionIdentity(input)
		switch {
		case phase == 0:
			firstIdentity = identity
		case (*scenario) == "clear":
			assert.FatalIf(t, identity == firstIdentity, "%s", "changed bytes retained identity")
		default:

			assert.FatalIf(t, identity != firstIdentity, "%s", "unchanged bytes changed identity")
		}

		body, err := fs.ReadFile(input, "content")
		wantBody := "original"
		if phase != 0 && (*scenario) == "clear" {
			wantBody = "changed"
		}
		assert.FatalIf(t, err != nil || string(body) != wantBody, "body=%q, error=%v", body, err)
		if phase == 1 && (*scenario) == "update-304" {
			etag = `"rotated"`
		}
		closeTestHTTPInputs(t, c)
		if phase == 0 && (*scenario) == "missing" {
			testRemoveValidationPayload(t, root)
		}
	}
}

type testAcquireConditionalValidationCase3CallbackInputs struct {
	t        **testing.T
	scenario **string
	etag     *string
	closeErr *error
	phase    *int
	requests *int
}

func testAcquireConditionalValidationCase3Callback(
	r *http.Request,
	testCaseInput testAcquireConditionalValidationCase3CallbackInputs,
) (*http.Response, error) {
	(*testCaseInput.requests)++
	const modified = "Mon, 01 Jun 2026 12:00:00 GMT"

	wantETag, wantModified := "", ""
	if (*testCaseInput.phase) != 0 && ((*testCaseInput.phase) != 1 || (*(*testCaseInput.scenario)) != "missing") {
		if (*(*testCaseInput.scenario)) != "last-modified" && (*(*testCaseInput.scenario)) != "none" &&
			(*(*testCaseInput.scenario)) != "unvalidated-304" {
			wantETag = (*testCaseInput.etag)
		}
		if (*(*testCaseInput.scenario)) != "etag" && (*(*testCaseInput.scenario)) != "none" &&
			(*(*testCaseInput.scenario)) != "unvalidated-304" {
			wantModified = modified
		}
		if (*testCaseInput.phase) == 2 && (*(*testCaseInput.scenario)) == "clear" {
			wantETag, wantModified = "", ""
		}
	}
	{
		got := r.Header.Get("If-None-Match")
		assert.ErrorIf(
			(*testCaseInput.t),
			got != wantETag,
			"phase=%d: ETag=%q, want %q",
			(*testCaseInput.phase),
			got,
			wantETag,
		)
	}
	{
		got := r.Header.Get("If-Modified-Since")
		assert.ErrorIf((*testCaseInput.t), got != wantModified,
			"phase=%d: modified=%q, want %q", (*testCaseInput.phase), got,
			wantModified,
		)
	}
	status := http.StatusOK
	body := "original"
	headers := make(http.Header)
	if (*(*testCaseInput.scenario)) != "last-modified" && (*(*testCaseInput.scenario)) != "none" &&
		(*(*testCaseInput.scenario)) != "unvalidated-304" {
		headers.Set("ETag", (*testCaseInput.etag))
	}
	if (*(*testCaseInput.scenario)) != "etag" && (*(*testCaseInput.scenario)) != "none" &&
		(*(*testCaseInput.scenario)) != "unvalidated-304" {
		headers.Set("Last-Modified", modified)
	}
	return testAcquireConditionalValidationCase3CallbackCompletion(
		&testAcquireConditionalValidationCase3CallbackCompletionInputs{
			testCaseInput: &testCaseInput,
			wantETag:      &wantETag,
			wantModified:  &wantModified,
			status:        &status,
			body:          &body,
			headers:       &headers,
			r:             &r,
		},
	)
}

func testAcquireConditionalValidationCase3CallbackCompletion(
	captures *testAcquireConditionalValidationCase3CallbackCompletionInputs,
) (*http.Response, error) {
	if (*captures.testCaseInput.phase) != 0 && ((*captures.wantETag) != "" || (*captures.wantModified) != "") {
		(*captures.status), (*captures.body) = http.StatusNotModified, ""
	}
	if (*captures.testCaseInput.phase) != 0 && (*(*captures.testCaseInput.scenario)) == "clear" {
		(*captures.status), (*captures.body), (*captures.headers) = http.StatusOK, "changed", make(http.Header)
	}
	if (*captures.testCaseInput.phase) == 1 && (*(*captures.testCaseInput.scenario)) == "update-304" {
		(*captures.headers) = http.Header{"Etag": {`"rotated"`}}
	}
	if (*(*captures.testCaseInput.scenario)) == "unsolicited-304" || (*captures.testCaseInput.phase) !=
		0 && (*(*captures.testCaseInput.scenario)) == "unvalidated-304" {
		(*captures.status), (*captures.body) = http.StatusNotModified, ""
	}
	if (*captures.testCaseInput.phase) == 1 && (*(*captures.testCaseInput.scenario)) == "unauthorized" {
		(*captures.status), (*captures.body) = http.StatusUnauthorized, ""
	}
	var closer error
	if (*captures.testCaseInput.phase) == 1 && (*(*captures.testCaseInput.scenario)) == "close-304" {
		closer = (*captures.testCaseInput.closeErr)
	}
	return &http.Response{
		Request:    (*captures.r).Clone((*captures.r).Context()),
		StatusCode: (*captures.status),
		Header:     (*captures.headers),
		Body:       remoteBody{Reader: strings.NewReader((*captures.body)), close: func() error { return closer }},
	}, nil
}

type testAcquireConditionalValidationCase3CallbackCompletionInputs struct {
	testCaseInput *testAcquireConditionalValidationCase3CallbackInputs
	wantETag      *string
	wantModified  *string
	status        *int
	body          *string
	headers       *http.Header
	r             **http.Request
}

func testRemoveValidationPayload(t *testing.T, root string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "oci", "index.json"))
	assert.NoError(t, err)

	var index ocispec.Index
	{
		err := json.Unmarshal(data, &index)
		assert.NoError(t, err)
	}

	for _, desc := range index.Manifests {
		if desc.MediaType == "application/octet-stream" {
			path := filepath.Join(
				root,
				"oci", "blobs",
				desc.Digest.Algorithm().String(),
				desc.Digest.Encoded(),
			)
			if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
				t.Fatal(err)
			}
		}
	}
}
