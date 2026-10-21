package kustomize

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/errdef"
)

type remoteHTTPRequest struct {
	Method, Path, ETag, Modified string
	Status, Bytes                int
}

type remoteHTTPTrace struct {
	mu          sync.Mutex
	requests    []remoteHTTPRequest
	connections atomic.Int32
}

func (tr *remoteHTTPTrace) serve(t *testing.T, body func() string) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		content := body()
		etag := `"` + manifest.Fingerprint(content) + `"`
		const modified = "Mon, 01 Jun 2026 12:00:00 GMT"
		w.Header().Set("ETag", etag)
		w.Header().Set("Last-Modified", modified)
		record := remoteHTTPRequest{Method: r.Method, Path: r.URL.Path, ETag: r.Header.Get("If-None-Match"), Modified: r.Header.Get("If-Modified-Since"), Status: http.StatusOK}
		if record.ETag == etag {
			record.Status = http.StatusNotModified
			w.WriteHeader(record.Status)
		} else {
			record.Bytes, _ = io.WriteString(w, content)
		}
		tr.mu.Lock()
		tr.requests = append(tr.requests, record)
		tr.mu.Unlock()
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			tr.connections.Add(1)
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}

func (tr *remoteHTTPTrace) snapshot() []remoteHTTPRequest {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return append([]remoteHTTPRequest(nil), tr.requests...)
}

func TestFetchRemote_ConditionalValidation(t *testing.T) {
	t.Parallel()
	for _, validator := range []string{"etag", "last-modified"} {
		t.Run("alias-304/"+validator, func(t *testing.T) {
			t.Parallel()
			const etag = `"colliding-target-tag"`
			const modified = "Mon, 01 Jun 2026 12:00:00 GMT"
			older := time.Date(2020, time.June, 1, 12, 0, 0, 0, time.UTC)
			var phase atomic.Int32
			var trace remoteHTTPTrace
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				record := remoteHTTPRequest{Method: r.Method, Path: r.URL.Path, ETag: r.Header.Get("If-None-Match"), Modified: r.Header.Get("If-Modified-Since"), Status: http.StatusOK}
				defer func() {
					trace.mu.Lock()
					trace.requests = append(trace.requests, record)
					trace.mu.Unlock()
				}()
				if r.URL.Path == "/latest.yaml" {
					since, err := http.ParseTime(record.Modified)
					if phase.Load() != 0 && (record.ETag == etag || err == nil && !older.After(since)) {
						record.Status = http.StatusNotModified
						w.WriteHeader(record.Status)
						return
					}
					target := "/v2.yaml"
					if phase.Load() != 0 {
						target = "/v1.yaml"
					}
					record.Status = http.StatusFound
					http.Redirect(w, r, target, record.Status)
					return
				}
				if validator == "etag" {
					w.Header().Set("ETag", etag)
				} else if r.URL.Path == "/v2.yaml" {
					w.Header().Set("Last-Modified", modified)
				} else {
					w.Header().Set("Last-Modified", older.Format(http.TimeFormat))
				}
				record.Bytes, _ = fmt.Fprintf(w, "version: %s\n", strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/"), ".yaml"))
			}))
			t.Cleanup(srv.Close)
			root := t.TempDir()
			var firstIdentity string
			for run := range 2 {
				phase.Store(int32(run))
				c := newTestTreeCache(t, root)
				input, err := c.FetchRemote(t.Context(), srv.URL+"/latest.yaml", sourcecache.RetryConfig{}, nil)
				if err != nil {
					t.Fatal(err)
				}
				body, err := fs.ReadFile(input, "content")
				identity := sourcecache.AcquisitionIdentity(input)
				t.Logf("run=%d body=%q identity=%s requests=%+v", run, body, identity, trace.snapshot())
				want := "version: v2\n"
				if run == 1 {
					want = "version: v1\n"
					if identity == firstIdentity {
						t.Error("changed alias target retained acquisition identity")
					}
				}
				if err != nil || string(body) != want {
					t.Errorf("run=%d body=%q, want %q, error=%v", run, body, want, err)
				}
				firstIdentity = identity
				closeTestTreeCache(t, c)
			}
			assert.Diff(t, trace.snapshot(), []remoteHTTPRequest{
				{Method: http.MethodGet, Path: "/latest.yaml", Status: http.StatusFound},
				{Method: http.MethodGet, Path: "/v2.yaml", Status: http.StatusOK, Bytes: len("version: v2\n")},
				{Method: http.MethodGet, Path: "/latest.yaml", Status: http.StatusFound},
				{Method: http.MethodGet, Path: "/v1.yaml", Status: http.StatusOK, Bytes: len("version: v1\n")},
			})
		})
	}
	for _, scenario := range []string{"changed-redirect", "redirected-304", "direct-to-redirect", "direct-to-redirected-304", "redirect-limit"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			const etag = `"target-local-tag"`
			const modified = "Mon, 01 Jun 2026 12:00:00 GMT"
			var phase, requests atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if scenario == "redirect-limit" {
					http.Redirect(w, r, "/input", http.StatusFound)
					return
				}
				if r.URL.Path == "/input" {
					wantETag, wantModified := "", ""
					if phase.Load() != 0 && strings.HasPrefix(scenario, "direct-to-") {
						wantETag, wantModified = etag, modified
					}
					if r.Header.Get("If-None-Match") != wantETag || r.Header.Get("If-Modified-Since") != wantModified {
						t.Errorf("origin validators=%v, want %q and %q", r.Header, wantETag, wantModified)
					}
					if phase.Load() == 0 && strings.HasPrefix(scenario, "direct-to-") {
						w.Header().Set("ETag", etag)
						w.Header().Set("Last-Modified", modified)
						_, _ = io.WriteString(w, "object: 0\n")
						return
					}
					http.Redirect(w, r, "/hop", http.StatusFound)
					return
				}
				if r.Header.Get("If-None-Match") != "" || r.Header.Get("If-Modified-Since") != "" {
					t.Errorf("redirect received conditional headers: %v", r.Header)
				}
				if r.URL.Path == "/hop" {
					http.Redirect(w, r, fmt.Sprintf("/object-%d", phase.Load()), http.StatusFound)
					return
				}
				lastModified := modified
				if phase.Load() != 0 {
					lastModified = "Mon, 01 Jun 2020 12:00:00 GMT"
				}
				w.Header().Set("ETag", etag)
				w.Header().Set("Last-Modified", lastModified)
				if phase.Load() != 0 && (scenario == "redirected-304" || scenario == "direct-to-redirected-304" || r.Header.Get("If-None-Match") == etag || r.Header.Get("If-Modified-Since") == modified) {
					w.WriteHeader(http.StatusNotModified)
					return
				}
				_, _ = fmt.Fprintf(w, "object: %d\n", phase.Load())
			}))
			t.Cleanup(srv.Close)
			root := t.TempDir()
			var firstIdentity string
			for run := range 2 {
				phase.Store(int32(run))
				c := newTestTreeCache(t, root)
				input, err := c.FetchRemote(t.Context(), srv.URL+"/input", sourcecache.RetryConfig{}, nil)
				if scenario == "redirect-limit" || run == 1 && (scenario == "redirected-304" || scenario == "direct-to-redirected-304") {
					if err == nil || input != nil {
						t.Fatalf("invalid redirect succeeded: %v", err)
					}
					if scenario == "redirect-limit" && requests.Load() != 10 {
						t.Fatalf("redirect requests=%d, want 10", requests.Load())
					}
					closeTestTreeCache(t, c)
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				body, err := fs.ReadFile(input, "content")
				if err != nil || string(body) != fmt.Sprintf("object: %d\n", run) {
					t.Fatalf("run=%d body=%q, error=%v", run, body, err)
				}
				identity := sourcecache.AcquisitionIdentity(input)
				if run == 1 && identity == firstIdentity {
					t.Fatal("changed redirect target retained acquisition identity")
				}
				firstIdentity = identity
				closeTestTreeCache(t, c)
			}
		})
	}
	t.Run("owner/userinfo", func(t *testing.T) {
		t.Parallel()
		const password = "owner-cache-test-password"
		const token = "owner-cache-test-query-token"
		const content = "stable: true\n"
		var trace remoteHTTPTrace
		srv := trace.serve(t, func() string { return content })
		endpoint, err := url.Parse(srv.URL + "/input?token=" + token)
		if err != nil {
			t.Fatal(err)
		}
		endpoint.User = url.UserPassword("reader", password)
		root := t.TempDir()
		disk := os.DirFS(root)
		var firstIdentity string
		for run := range 2 {
			c := newTestTreeCache(t, root)
			input, err := c.FetchRemote(t.Context(), endpoint.String(), sourcecache.RetryConfig{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			body, err := fs.ReadFile(input, "content")
			assert.NoError(t, err)
			assert.Equal(t, string(body), content)
			identity := sourcecache.AcquisitionIdentity(input)
			if identity == "" || run == 1 && identity != firstIdentity {
				t.Error("unchanged authenticated input did not reuse acquisition identity")
			}
			firstIdentity = identity
			closeTestTreeCache(t, c)
			assert.NoError(t, fs.WalkDir(disk, ".", func(name string, entry fs.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				data, err := fs.ReadFile(disk, name)
				if err != nil {
					return err
				}
				if strings.Contains(string(data), password) || strings.Contains(string(data), token) {
					t.Errorf("persistent cache file %s contains URL credentials", name)
				}
				return nil
			}))
		}
		assert.Diff(t, trace.snapshot(), []remoteHTTPRequest{
			{Method: http.MethodGet, Path: "/input", Status: http.StatusOK, Bytes: len(content)},
			{Method: http.MethodGet, Path: "/input", ETag: `"` + manifest.Fingerprint(content) + `"`, Modified: "Mon, 01 Jun 2026 12:00:00 GMT", Status: http.StatusNotModified},
		})
	})
	t.Run("owner/missing", func(t *testing.T) {
		t.Parallel()
		var trace remoteHTTPTrace
		srv := trace.serve(t, func() string { return "changed" })
		url := srv.URL + "/input?version=2"
		root := t.TempDir()
		c := newTestTreeCache(t, root)
		reader, _, _, err := c.owner.Blob(t.Context(), sourcecache.BlobKey{Content: struct{ URL string }{url}}, sourcecache.Policy{}, func(_ context.Context, w sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
			annotations := map[string]string{
				httpETagAnnotation:         `"` + manifest.Fingerprint("changed") + `"`,
				httpLastModifiedAnnotation: "Mon, 01 Jun 2026 12:00:00 GMT",
			}
			size, err := io.WriteString(w, "original")
			return sourcecache.ProducerResult{Size: int64(size), Annotations: annotations}, err
		})
		if err != nil {
			t.Fatal(err)
		}
		firstIdentity := sourcecache.AcquisitionIdentity(sourcecache.FileFS("content", reader))
		assert.NoError(t, reader.Close())
		closeTestTreeCache(t, c)
		c = newTestTreeCache(t, root)
		input, err := c.FetchRemote(t.Context(), url, sourcecache.RetryConfig{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		body, err := fs.ReadFile(input, "content")
		assert.NoError(t, err)
		assert.Equal(t, string(body), "changed")
		if sourcecache.AcquisitionIdentity(input) == firstIdentity {
			t.Error("different owner retained previous acquisition identity")
		}
		assert.Diff(t, trace.snapshot(), []remoteHTTPRequest{{Method: http.MethodGet, Path: "/input", Status: http.StatusOK, Bytes: len("changed")}})
		closeTestTreeCache(t, c)
	})
	var version atomic.Int32
	version.Store(1)
	var trace remoteHTTPTrace
	srv := trace.serve(t, func() string { return fmt.Sprintf("version: v%d\n", version.Load()) })
	root := t.TempDir()
	for _, run := range []struct {
		name              string
		version           int32
		status, bodyBytes int
	}{
		{"initial", 1, http.StatusOK, len("version: v1\n")},
		{"unchanged", 1, http.StatusNotModified, 0},
		{"changed", 2, http.StatusOK, len("version: v2\n")},
	} {
		version.Store(run.version)
		c := newTestTreeCache(t, root)
		before := len(trace.snapshot())
		beforeConnections := trace.connections.Load()
		for _, path := range []string{"/resource", "/other", "/resource", "/other"} {
			input, err := c.FetchRemote(t.Context(), srv.URL+path, sourcecache.RetryConfig{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			body, err := fs.ReadFile(input, "content")
			if err != nil || string(body) != fmt.Sprintf("version: v%d\n", run.version) {
				t.Fatalf("%s: body=%q, error=%v", run.name, body, err)
			}
		}
		got := trace.snapshot()[before:]
		t.Logf("%s: requests=%+v accepted_connections=%d", run.name, got, trace.connections.Load())
		if len(got) != 2 {
			t.Errorf("%s: requests=%d, want 1 per URL/policy/run", run.name, len(got))
		}
		for _, request := range got {
			if request.Method != http.MethodGet || request.Status != run.status || request.Bytes != run.bodyBytes {
				t.Errorf("%s: response=%+v, want GET status=%d bytes=%d", run.name, request, run.status, run.bodyBytes)
			}
			if run.name != "initial" && (request.ETag == "" || request.Modified == "") {
				t.Errorf("%s: missing persisted conditional headers: %+v", run.name, request)
			}
		}
		if got := trace.connections.Load() - beforeConnections; got != 1 {
			t.Errorf("%s: compatible URLs accepted_connections=%d, want 1", run.name, got)
		}
		closeTestTreeCache(t, c)
	}
	for _, scenario := range []string{"etag", "last-modified", "both", "none", "clear", "update-304", "missing", "unsolicited-304", "unvalidated-304", "unauthorized", "close-304"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			const url = "https://reader:owner-cache-test-password@example.invalid/input"
			const modified = "Mon, 01 Jun 2026 12:00:00 GMT"
			etag := `W/"opaque-tag"`
			closeErr := errors.New("304 close failed")
			var firstIdentity string
			for phase := range 3 {
				c := newTestTreeCache(t, root)
				requests := 0
				c.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
					requests++
					wantETag, wantModified := "", ""
					if phase != 0 && (phase != 1 || scenario != "missing") {
						if scenario != "last-modified" && scenario != "none" && scenario != "unvalidated-304" {
							wantETag = etag
						}
						if scenario != "etag" && scenario != "none" && scenario != "unvalidated-304" {
							wantModified = modified
						}
						if phase == 2 && scenario == "clear" {
							wantETag, wantModified = "", ""
						}
					}
					if got := r.Header.Get("If-None-Match"); got != wantETag {
						t.Errorf("phase=%d: ETag=%q, want %q", phase, got, wantETag)
					}
					if got := r.Header.Get("If-Modified-Since"); got != wantModified {
						t.Errorf("phase=%d: modified=%q, want %q", phase, got, wantModified)
					}
					status := http.StatusOK
					body := "original"
					headers := make(http.Header)
					if scenario != "last-modified" && scenario != "none" && scenario != "unvalidated-304" {
						headers.Set("ETag", etag)
					}
					if scenario != "etag" && scenario != "none" && scenario != "unvalidated-304" {
						headers.Set("Last-Modified", modified)
					}
					if phase != 0 && (wantETag != "" || wantModified != "") {
						status, body = http.StatusNotModified, ""
					}
					if phase != 0 && scenario == "clear" {
						status, body, headers = http.StatusOK, "changed", make(http.Header)
					}
					if phase == 1 && scenario == "update-304" {
						headers = http.Header{"Etag": {`"rotated"`}}
					}
					if scenario == "unsolicited-304" || phase != 0 && scenario == "unvalidated-304" {
						status, body = http.StatusNotModified, ""
					}
					if phase == 1 && scenario == "unauthorized" {
						status, body = http.StatusUnauthorized, ""
					}
					var closer error
					if phase == 1 && scenario == "close-304" {
						closer = closeErr
					}
					return &http.Response{Request: r.Clone(r.Context()), StatusCode: status, Header: headers, Body: remoteBody{Reader: strings.NewReader(body), close: func() error { return closer }}}, nil
				})
				input, err := c.FetchRemote(t.Context(), url, sourcecache.RetryConfig{}, nil)
				if requests != 1 {
					t.Fatalf("phase=%d requests=%d, want 1", phase, requests)
				}
				if scenario == "unsolicited-304" || phase == 1 && (scenario == "unauthorized" || scenario == "close-304" || scenario == "unvalidated-304") {
					if err == nil || input != nil {
						t.Fatalf("invalid validation succeeded: %v", err)
					}
					if scenario == "close-304" && !errors.Is(err, closeErr) {
						t.Fatalf("close identity lost: %v", err)
					}
					closeTestTreeCache(t, c)
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				identity := sourcecache.AcquisitionIdentity(input)
				if phase == 0 {
					firstIdentity = identity
				} else if scenario == "clear" {
					if identity == firstIdentity {
						t.Fatal("changed bytes retained identity")
					}
				} else if identity != firstIdentity {
					t.Fatal("unchanged bytes changed identity")
				}
				body, err := fs.ReadFile(input, "content")
				wantBody := "original"
				if phase != 0 && scenario == "clear" {
					wantBody = "changed"
				}
				if err != nil || string(body) != wantBody {
					t.Fatalf("body=%q, error=%v", body, err)
				}
				if phase == 1 && scenario == "update-304" {
					etag = `"rotated"`
				}
				closeTestTreeCache(t, c)
				if phase == 0 && scenario == "missing" {
					data, err := os.ReadFile(filepath.Join(root, "oci/index.json"))
					if err != nil {
						t.Fatal(err)
					}
					var index ocispec.Index
					if err := json.Unmarshal(data, &index); err != nil {
						t.Fatal(err)
					}
					for _, desc := range index.Manifests {
						if desc.MediaType == "application/octet-stream" {
							path := filepath.Join(root, "oci/blobs", desc.Digest.Algorithm().String(), desc.Digest.Encoded())
							if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
								t.Fatal(err)
							}
						}
					}
				}
			}
		})
	}

}

func TestRemoteInput_CancelQueuedGitBase(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		c := newTestTreeCache(t, "")
		c.tasks = task.NewBounded(2)
		entered, release := make(chan struct{}, 2), make(chan struct{})
		for range 2 {
			c.tasks.Go(t.Context(), "blocker", func(context.Context) {
				entered <- struct{}{}
				<-release
			})
		}
		<-entered
		<-entered
		calls := 0
		c.gitBase = func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
			calls++
			return &store.SourceArtifact{FS: fstest.MapFS{}, Key: "key", Revision: "revision"}, nil
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, _, err := (remoteInput{Operation: remoteGit, URL: "https://example.invalid/repo"}).acquire(ctx, c, &treeFS{})
			done <- err
		}()
		synctest.Wait()
		cancel()
		close(release)
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("queued acquisition=%v, want context cancellation", err)
		}
		c.tasks.BlockTillDone()
		if calls != 0 {
			t.Fatalf("canceled queued git fetches=%d, want 0", calls)
		}
	})
}

func TestFetchRemote_CancelDoesNotPoisonCache(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				entered, release := make(chan struct{}), make(chan struct{})
				hits := 0
				c := newTestTreeCache(t, "")
				c.httpClient = &http.Client{Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
					hits++
					if hits == 1 {
						close(entered)
						select {
						case <-release:
						case <-r.Context().Done():
							return nil, r.Context().Err()
						}
						return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
					}
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("body: ok\n")), Header: make(http.Header)}, nil
				})}
				retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
				ctx, cancel := context.WithCancel(t.Context())
				cancelled := make(chan error, 1)
				go func() { _, err := c.FetchRemote(ctx, "https://example.invalid/resource", retry, nil); cancelled <- err }()
				<-entered
				done := make(chan error, concurrency-1)
				for range concurrency - 1 {
					go func() {
						input, err := c.FetchRemote(t.Context(), "https://example.invalid/resource", retry, nil)
						var body []byte
						if err == nil {
							body, err = fs.ReadFile(input, "content")
						}
						if err == nil && string(body) != "body: ok\n" {
							err = fmt.Errorf("body=%q", body)
						}
						done <- err
					}()
				}
				synctest.Wait()
				cancel()
				if err := <-cancelled; !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled waiter=%v", err)
				}
				close(release)
				for range concurrency - 1 {
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				}
				if hits != 2 {
					t.Fatalf("requests=%d, want 2", hits)
				}
				if _, err := c.FetchRemote(ctx, "https://example.invalid/resource", retry, nil); !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled warm waiter: %v", err)
				}
			})
		})
	}
}

func TestIsHTTPClientError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"400", &fetcherr.ResponseError{StatusCode: 400}, true},
		{"401", &fetcherr.ResponseError{StatusCode: 401}, true},
		{"403", &fetcherr.ResponseError{StatusCode: 403}, true},
		{"404", &fetcherr.ResponseError{StatusCode: 404}, true},
		{"418", &fetcherr.ResponseError{StatusCode: 418}, true},
		{"499", &fetcherr.ResponseError{StatusCode: 499}, true},
		{"429 transient", &fetcherr.ResponseError{StatusCode: 429}, false},
		{"500 transient", &fetcherr.ResponseError{StatusCode: 500}, false},
		{"599 transient", &fetcherr.ResponseError{StatusCode: 599}, false},
		{"600 terminal", &fetcherr.ResponseError{StatusCode: 600}, false},
		{"client after transient", errors.Join(&fetcherr.ResponseError{StatusCode: 503}, fmt.Errorf("edge: %w", &fetcherr.ResponseError{StatusCode: 404})), true},
		{"client before transient", errors.Join(&fetcherr.ResponseError{StatusCode: 404}, &fetcherr.ResponseError{StatusCode: 503}), true},
		{"200 never-client-error", &fetcherr.ResponseError{StatusCode: 200}, false},
		{"wrapped 404", fmt.Errorf("preflight: %w", &fetcherr.ResponseError{StatusCode: 404}), true},
		{"wrapped 500", fmt.Errorf("preflight: %w", &fetcherr.ResponseError{StatusCode: 500}), false},
		{"transport error", fmt.Errorf("connection refused"), false},
		{"deadline", fmt.Errorf("context deadline exceeded"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		if got := fetcherr.Classes(tc.err)&fetcherr.Client != 0; got != tc.want {
			t.Errorf("isHTTPClientError(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestFetchRemote_RetryTransientCacheClientError(t *testing.T) {
	for _, tc := range []struct {
		status int
		client bool
		retry  bool
	}{
		{400, true, false}, {401, true, false}, {403, true, false}, {404, true, false},
		{429, false, true}, {499, true, false}, {500, false, true}, {599, false, true}, {600, false, false},
	} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			var hits atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if hits.Add(1) == 1 {
					w.WriteHeader(tc.status)
					return
				}
				_, _ = w.Write([]byte("retried"))
			}))
			t.Cleanup(srv.Close)
			cache := newTestTreeCache(t, "")
			retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
			input, err := cache.FetchRemote(t.Context(), srv.URL, retry, nil)
			var body []byte
			if err == nil {
				body, err = fs.ReadFile(input, "content")
			}
			if !tc.retry {
				status, ok := errors.AsType[*fetcherr.ResponseError](err)
				if !ok || status.StatusCode != tc.status || status.Op != http.MethodGet || hits.Load() != 1 {
					t.Fatalf("initial status=%+v err=%v hits=%d", status, err, hits.Load())
				}
				input, err = cache.FetchRemote(t.Context(), srv.URL, retry, nil)
				if err == nil {
					body, err = fs.ReadFile(input, "content")
				}
			}
			if tc.client {
				if fetcherr.Classes(err)&fetcherr.Client == 0 || hits.Load() != 1 {
					t.Fatalf("4xx cache: err=%v, hits=%d", err, hits.Load())
				}
			} else if err != nil || string(body) != "retried" || hits.Load() != 2 {
				t.Fatalf("retry or next fetch: body=%q, err=%v, hits=%d", body, err, hits.Load())
			}
		})
	}
}

type remoteRoundTrip func(*http.Request) (*http.Response, error)

func (f remoteRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := f(r)
	if resp != nil && resp.Request == nil {
		resp.Request = r
	}
	return resp, err
}

func TestFetchRemote_PolicyIsolation(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			for _, equal := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/reverse-%t/equal-%t", concurrency, reverse, equal), func(t *testing.T) {
					t.Parallel()
					synctest.Test(t, func(t *testing.T) {
						entered, release := make(chan struct{}), make(chan struct{})
						hits := 0
						c := newTestTreeCache(t, "")
						c.httpClient = &http.Client{Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
							hits++
							if hits == 1 {
								close(entered)
								select {
								case <-release:
								case <-r.Context().Done():
									return nil, r.Context().Err()
								}
							}
							return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
						})}
						done := make(chan error, concurrency)
						launch := func(i int) {
							go func() {
								retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
								if !equal && i == 0 {
									retry.Attempts = 1
								}
								if equal {
									switch i % 3 {
									case 0:
										retry.Jitter = math.NaN()
									case 1:
										retry.Jitter = math.Inf(1)
									}
								}
								_, err := c.FetchRemote(t.Context(), "https://example.invalid/resource", retry, nil)
								done <- err
							}()
						}
						first := 0
						if reverse {
							first = concurrency - 1
						}
						launch(first)
						<-entered
						for i := range concurrency {
							if i != first {
								launch(i)
							}
						}
						synctest.Wait()
						close(release)
						for range concurrency {
							err := <-done
							terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
							if !ok || !terminal.Attempted {
								t.Fatalf("terminal=%+v, error=%v", terminal, err)
							}
						}
						want := 4
						if equal {
							want = 3
						}
						if hits != want {
							t.Fatalf("requests=%d, want %d", hits, want)
						}
					})
				})
			}
		}
	}
}

func TestFetchRemote_PartialBodyRetry(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			for _, backing := range []string{"memory", "disk"} {
				t.Run(backing, func(t *testing.T) {
					var hits atomic.Int32
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						switch hits.Add(1) {
						case 1:
							w.Header().Set("Content-Length", "20")
							_, _ = io.WriteString(w, "partial")
						case 2:
							_, _ = io.WriteString(w, "complete")
						default:
							_, _ = io.WriteString(w, "replacement")
						}
					}))
					defer srv.Close()
					root := ""
					if backing == "disk" {
						root = t.TempDir()
					}
					c := newTestTreeCache(t, root)
					retry := sourcecache.RetryConfig{Attempts: 2, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}
					bodies := make(chan []byte, concurrency)
					for range concurrency {
						go func() {
							input, err := c.FetchRemote(t.Context(), srv.URL, retry, nil)
							var body []byte
							if err == nil {
								body, err = fs.ReadFile(input, "content")
							}
							if err != nil || string(body) != "complete" {
								t.Errorf("body=%q, error=%v", body, err)
							}
							bodies <- body
						}()
					}
					var body []byte
					for range concurrency {
						body = <-bodies
					}
					if hits.Load() != 2 {
						t.Fatalf("requests=%d, want 2", hits.Load())
					}
					body[0] = 'X'
					input, err := c.FetchRemote(t.Context(), srv.URL, sourcecache.RetryConfig{}, nil)
					if err == nil {
						body, err = fs.ReadFile(input, "content")
					}
					if err != nil || string(body) != "replacement" {
						t.Fatalf("replacement body=%q, error=%v", body, err)
					}
					input, err = c.FetchRemote(t.Context(), srv.URL, retry, nil)
					if err == nil {
						body, err = fs.ReadFile(input, "content")
					}
					if err != nil || string(body) != "complete" || hits.Load() != 3 {
						t.Fatalf("completed generation body=%q, error=%v, hits=%d", body, err, hits.Load())
					}
				})
			}
		})
	}
}

func TestFetchRemote_NetworkSuppression(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			var network source.Network
			c := NewTreeCache(cachetest.New(t, "", sourcecache.New), &network, task.NewBounded(2))
			t.Cleanup(c.Close)
			var calls atomic.Int32
			var failing atomic.Bool
			failing.Store(true)
			testutil.StubDNS(t, func(context.Context, string) error {
				if failing.Load() {
					calls.Add(1)
					return os.ErrDeadlineExceeded
				}
				return nil
			})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "new run") }))
			t.Cleanup(srv.Close)
			target := strings.Replace(srv.URL, "127.0.0.1", "network.test", 1)
			retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour}
			_, err := c.FetchRemote(t.Context(), target+"/first", retry, nil)
			first, ok := errors.AsType[*sourcecache.TerminalError](err)
			if !ok || !first.Attempted {
				t.Fatalf("first attempt: %v", err)
			}
			cause, before := first.Err, calls.Load()
			if before == 0 {
				t.Fatal("seed performed no DNS lookup")
			}
			done := make(chan error, concurrency)
			for i := range concurrency {
				go func() {
					_, err := c.FetchRemote(t.Context(), fmt.Sprintf("%s/%d", target, i), retry, nil)
					done <- err
				}()
			}
			for range concurrency {
				err := <-done
				terminal, ok := errors.AsType[*sourcecache.TerminalError](err)
				if !ok || terminal.Attempted || !errors.Is(err, cause) {
					t.Fatalf("suppression: %v", err)
				}
			}
			if calls.Load() != before {
				t.Fatalf("physical dials=%d want=1", calls.Load())
			}
			entries := 0
			c.remoteFetches.Range(func(_, _ any) bool { entries++; return true })
			if entries != 0 {
				t.Fatalf("retained terminal failures=%d", entries)
			}
			fresh := NewTreeCache(c.owner, new(source.Network), task.NewBounded(2))
			t.Cleanup(fresh.Close)
			failing.Store(false)
			input, err := fresh.FetchRemote(t.Context(), target+"/first", retry, nil)
			var body []byte
			if err == nil {
				body, err = fs.ReadFile(input, "content")
			}
			if err != nil || string(body) != "new run" {
				t.Fatalf("cache shared negative state: %q %v", body, err)
			}
		})
	}
}

type remoteRead func([]byte) (int, error)

func (f remoteRead) Read(p []byte) (int, error) { return f(p) }

type remoteBody struct {
	io.Reader
	close func() error
}

func (b remoteBody) Close() error { return b.close() }

func TestFetchRemote_CloseJoinsAcquisition(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, storage := range []string{"memory", "disk", "borrowed-network"} {
			for _, stage := range []string{"headers", "read", "cleanup", "publication", "backoff"} {
				t.Run(fmt.Sprintf("%d/%s/%s", concurrency, storage, stage), func(t *testing.T) {
					t.Parallel()
					synctest.Test(t, func(t *testing.T) {
						root := ""
						if storage == "disk" {
							root = t.TempDir()
						}
						var network *source.Network
						if storage == "borrowed-network" {
							network = new(source.Network)
							t.Cleanup(network.Close)
						}
						c := NewTreeCache(cachetest.New(t, root, sourcecache.New), network, task.NewBounded(concurrency))
						t.Cleanup(c.Close)
						entered, canceled, release, completed := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
						hits := 0
						c.httpClient = &http.Client{Transport: remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
							hits++
							if stage == "headers" {
								close(entered)
								select {
								case <-r.Context().Done():
									close(canceled)
								case <-release:
								}
								<-release
								close(completed)
								return nil, context.Canceled
							}
							status := http.StatusOK
							var reader io.Reader = strings.NewReader("complete")
							if stage == "read" {
								reader = remoteRead(func([]byte) (int, error) {
									close(entered)
									select {
									case <-r.Context().Done():
										close(canceled)
									case <-release:
									}
									<-release
									return 0, context.Canceled
								})
							}
							if stage == "backoff" {
								status = http.StatusServiceUnavailable
							}
							body := remoteBody{Reader: reader, close: func() error {
								defer close(completed)
								if stage == "cleanup" || stage == "publication" {
									close(entered)
									select {
									case <-r.Context().Done():
										close(canceled)
									case <-release:
									}
									<-release
									if stage == "cleanup" {
										return context.Canceled
									}
								}
								if stage == "backoff" {
									close(entered)
								}
								return nil
							}}
							return &http.Response{StatusCode: status, Body: body, Header: make(http.Header)}, nil
						})}
						retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour, MaxWait: time.Hour}
						url := "https://example.invalid/resource"
						ctx, cancel := context.WithCancel(t.Context())
						defer cancel()
						callers := make(chan error, concurrency)
						for range concurrency {
							go func() { _, err := c.FetchRemote(ctx, url, retry, nil); callers <- err }()
						}
						<-entered
						synctest.Wait()
						cancel()
						for range concurrency {
							if err := <-callers; !errors.Is(err, context.Canceled) {
								t.Fatalf("waiter cancellation: %v", err)
							}
						}
						workers := c.workers.Load()
						loaded, _ := c.remoteFetches.Load(remoteKey{url: url, retry: retry})
						rf := loaded.(*remoteFetch)
						closed := make(chan struct{}, concurrency)
						go func() { c.Close(); closed <- struct{}{} }()
						synctest.Wait()
						for range concurrency - 1 {
							go func() { c.Close(); closed <- struct{}{} }()
						}
						if stage != "backoff" {
							select {
							case <-canceled:
							default:
								t.Error("orphaned acquisition did not receive owner cancellation")
							}
							select {
							case <-closed:
								t.Error("Close returned before synchronous cleanup completed")
								closed <- struct{}{}
							default:
							}
						}
						close(release)
						for range concurrency {
							<-closed
						}
						<-completed
						<-rf.done
						if workers.state.Load() != remoteClosed || rf.cancel.Load() != nil || hits != 1 {
							t.Fatalf("incomplete teardown: state=%d cancel=%v hits=%d", workers.state.Load(), rf.cancel.Load(), hits)
						}
						if stage != "publication" && (!errors.Is(rf.err, context.Canceled) || !errors.Is(rf.err, manifest.ErrFlux)) {
							t.Fatalf("owner cancellation identity: %v", rf.err)
						}
						check := func(owner *sourcecache.Cache) {
							t.Helper()
							reader, _, _, err := owner.Blob(t.Context(), sourcecache.BlobKey{Content: struct{ URL string }{url}}, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
							if err != nil {
								if !errors.Is(err, errdef.ErrNotFound) {
									t.Fatal(err)
								}
								return
							}
							body, readErr := io.ReadAll(reader)
							closeErr := reader.Close()
							if stage != "publication" || string(body) != "complete" || errors.Join(readErr, closeErr) != nil {
								t.Fatalf("interrupted publication: %q %v %v", body, readErr, closeErr)
							}
						}
						check(c.owner)
						if err := c.owner.Close(); err != nil {
							t.Fatal(err)
						}
						if root != "" {
							check(cachetest.New(t, root, sourcecache.New))
						}
					})
				})
			}
		}
	}
}

func TestFetchRemote_CloseJoinsKeyWait(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		for _, backing := range []string{"memory", "disk"} {
			t.Run(fmt.Sprintf("%d/%s", concurrency, backing), func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					root := ""
					if backing == "disk" {
						root = t.TempDir()
					}
					c := newTestTreeCache(t, root)
					url := "https://example.invalid/key"
					key := sourcecache.BlobKey{Content: struct{ URL string }{url}}
					entered, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
					go func() {
						r, _, _, err := c.owner.Blob(t.Context(), key, sourcecache.Policy{}, func(context.Context, sourcecache.ProducerInput) (sourcecache.ProducerResult, error) {
							close(entered)
							<-release
							return sourcecache.ProducerResult{}, nil
						})
						if err == nil {
							err = r.Close()
						}
						held <- err
					}()
					<-entered
					c.httpClient = &http.Client{Transport: remoteRoundTrip(func(*http.Request) (*http.Response, error) {
						t.Error("key waiter reached producer after Close")
						return nil, context.Canceled
					})}
					done := make(chan error, concurrency)
					for range concurrency {
						go func() { _, err := c.FetchRemote(t.Context(), url, sourcecache.RetryConfig{}, nil); done <- err }()
					}
					synctest.Wait()
					c.Close()
					for range concurrency {
						if err := <-done; !errors.Is(err, context.Canceled) {
							t.Errorf("key waiter: %v", err)
						}
					}
					close(release)
					if err := <-held; err != nil {
						t.Fatal(err)
					}
				})
			})
		}
	}
}

func TestFetchRemote_ExactSuccessor(t *testing.T) {
	t.Parallel()
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := newTestTreeCache(t, "")
				entered, release := make(chan struct{}), make(chan struct{})
				c.httpClient = &http.Client{Transport: remoteRoundTrip(func(*http.Request) (*http.Response, error) {
					close(entered)
					<-release
					return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
				})}
				key := remoteKey{url: "https://example.invalid/successor", retry: sourcecache.RetryConfig{Attempts: 1}}
				done := make(chan error, concurrency)
				for range concurrency {
					go func() { _, err := c.FetchRemote(t.Context(), key.url, key.retry, nil); done <- err }()
				}
				<-entered
				synctest.Wait()
				successor := &remoteFetch{done: make(chan struct{})}
				c.remoteFetches.Store(key, successor)
				close(release)
				for range concurrency {
					if err := <-done; err == nil {
						t.Fatal("missing transient failure")
					}
				}
				if got, _ := c.remoteFetches.Load(key); got != successor {
					t.Fatal("predecessor retired its successor")
				}
			})
		})
	}
}

func TestFetchRemote_OwnerIsolation(t *testing.T) {
	for _, concurrency := range []int{2, 4} {
		for _, sharedNetwork := range []bool{false, true} {
			for _, sameKey := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/network-%t/key-%t", concurrency, sharedNetwork, sameKey), func(t *testing.T) {
					entered, served := make(chan struct{}), make(chan struct{})
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Header.Get("Owner") == "A" {
							close(entered)
							<-r.Context().Done()
							close(served)
							return
						}
						_, _ = io.WriteString(w, "healthy")
					}))
					t.Cleanup(srv.Close)
					owner := cachetest.New(t, "", sourcecache.New)
					na, nb := new(source.Network), new(source.Network)
					if sharedNetwork {
						nb = na
					}
					a, b := NewTreeCache(owner, na, task.NewBounded(2)), NewTreeCache(owner, nb, task.NewBounded(2))
					t.Cleanup(a.Close)
					t.Cleanup(b.Close)
					transport := a.httpClient.Transport
					a.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
						r = r.Clone(r.Context())
						r.Header.Set("Owner", "A")
						return transport.RoundTrip(r)
					})
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					adone, bdone := make(chan error, concurrency), make(chan error, concurrency)
					for range concurrency {
						go func() { _, err := a.FetchRemote(ctx, srv.URL, sourcecache.RetryConfig{}, nil); adone <- err }()
					}
					<-entered
					url := srv.URL
					if !sameKey {
						url += "/other"
					}
					for range concurrency {
						go func() {
							input, err := b.FetchRemote(t.Context(), url, sourcecache.RetryConfig{}, nil)
							var body []byte
							if err == nil {
								body, err = fs.ReadFile(input, "content")
							}
							if err == nil && string(body) != "healthy" {
								err = fmt.Errorf("body=%q", body)
							}
							bdone <- err
						}()
					}
					cancel()
					for range concurrency {
						if err := <-adone; !errors.Is(err, context.Canceled) {
							t.Error(err)
						}
					}
					a.Close()
					<-served
					for range concurrency {
						if err := <-bdone; err != nil {
							t.Fatal(err)
						}
					}
					if _, err := a.FetchRemote(t.Context(), srv.URL, sourcecache.RetryConfig{}, nil); !errors.Is(err, fs.ErrClosed) {
						t.Fatalf("closed owner: %v", err)
					}
					input, err := b.FetchRemote(t.Context(), srv.URL+"/after", sourcecache.RetryConfig{}, nil)
					var body []byte
					if err == nil {
						body, err = fs.ReadFile(input, "content")
					}
					if err != nil || string(body) != "healthy" {
						t.Fatalf("borrowed backing/Network closed: %q %v", body, err)
					}
				})
			}
		}
	}
}

func TestFetchRemote_NetworkSuppressionOwnerCancellation(t *testing.T) {
	// net's process-global resolver semaphore MUST be initialized outside the bubbles.
	_, _ = (&net.Resolver{PreferGo: true}).LookupHost(t.Context(), "invalid hostname")
	for _, concurrency := range []int{2, 4} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var network source.Network
				owner := cachetest.New(t, "", sourcecache.New)
				c, other := NewTreeCache(owner, &network, task.NewBounded(concurrency)), NewTreeCache(owner, &network, task.NewBounded(concurrency))
				testutil.StubDNS(t, func(context.Context, string) error { return os.ErrDeadlineExceeded })
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "healthy") }))
				entered := make(chan struct{}, concurrency)
				release := make(chan struct{})
				var senders sync.WaitGroup
				t.Cleanup(func() {
					select {
					case <-release:
					default:
						close(release)
					}
					c.Close()
					other.Close()
					senders.Wait()
					network.Close()
					srv.Close()
				})
				transport := c.httpClient.Transport
				c.httpClient.Transport = remoteRoundTrip(func(r *http.Request) (*http.Response, error) {
					entered <- struct{}{}
					if r.URL.Path == "/0" {
						<-release
						return transport.RoundTrip(r)
					}
					<-r.Context().Done()
					return nil, r.Context().Err()
				})
				done := make(chan error, concurrency)
				retry := sourcecache.RetryConfig{Attempts: 3, MinWait: time.Hour}
				for i := range concurrency {
					senders.Go(func() {
						_, err := c.FetchRemote(t.Context(), fmt.Sprintf("http://wave.test/%d", i), retry, nil)
						done <- err
					})
				}
				for range concurrency {
					<-entered
				}
				close(release)
				first := <-done
				terminal, ok := errors.AsType[*sourcecache.TerminalError](first)
				if !ok || !terminal.Attempted || !fetcherr.DialTimeout(terminal.Err) {
					t.Fatalf("live admitted timeout: %v", first)
				}
				cause := terminal.Err
				c.Close()
				for range concurrency - 1 {
					if err := <-done; !errors.Is(err, context.Canceled) {
						t.Errorf("canceled admitted request: %v", err)
					}
				}
				senders.Wait()
				_, err := other.FetchRemote(t.Context(), "http://wave.test/later", retry, nil)
				terminal, ok = errors.AsType[*sourcecache.TerminalError](err)
				if !ok || terminal.Attempted || !errors.Is(err, cause) {
					t.Fatalf("suppressed request: %v", err)
				}
				input, err := other.FetchRemote(t.Context(), srv.URL, retry, nil)
				var body []byte
				if err == nil {
					body, err = fs.ReadFile(input, "content")
				}
				if err != nil || string(body) != "healthy" {
					t.Fatalf("healthy endpoint: %q %v", body, err)
				}
			})
		})
	}
}

func TestRemoteInput_AcquisitionIdentity(t *testing.T) {
	for _, operation := range []remoteOperation{remoteHTTP, remoteGit} {
		t.Run(fmt.Sprint(operation), func(t *testing.T) {
			var version atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprint(w, version.Load())
			}))
			t.Cleanup(srv.Close)
			descriptor := remoteInput{Operation: operation, URL: srv.URL + "/repo.git//file"}
			if operation == remoteGit {
				descriptor.Ref = "main"
			}
			retry := sourcecache.RetryConfig{Attempts: 2}
			var first remoteIdentity
			for _, v := range []int32{1, 1, 2} {
				version.Store(v)
				c := newTestTreeCache(t, "")
				c.SetGitBaseFetcher(func(_ context.Context, request GitBaseRequest, gotRetry sourcecache.RetryConfig) (*store.SourceArtifact, error) {
					if operation != remoteGit || request.URL != descriptor.URL || request.Ref != descriptor.Ref || request.MaxAge != time.Minute || gotRetry != retry {
						t.Fatalf("acquisition request=%+v retry=%+v", request, gotRetry)
					}
					return &store.SourceArtifact{Key: "family", Revision: fmt.Sprint(v), FS: fstest.MapFS{"content": {Data: fmt.Append(nil, v)}}}, nil
				})
				input := newTreeFS(fstest.MapFS{}, "/src", nil)
				input.retry, input.maxAge = retry, time.Minute
				acquired, identity, err := descriptor.acquire(t.Context(), c, input)
				if err != nil {
					t.Fatal(err)
				}
				body, err := fs.ReadFile(acquired, "content")
				if err != nil || string(body) != fmt.Sprint(v) {
					t.Fatalf("acquired body=%q error=%v", body, err)
				}
				if operation == remoteHTTP && (identity.HTTP == "" || identity.HTTP != sourcecache.AcquisitionIdentity(acquired)) {
					t.Fatalf("HTTP identity=%+v", identity)
				}
				if operation == remoteGit && (identity.Key != "family" || identity.Revision != fmt.Sprint(v)) {
					t.Fatalf("Git identity=%+v", identity)
				}
				if first == (remoteIdentity{}) {
					first = identity
				}
				if (identity == first) != (v == 1) {
					t.Fatalf("generation %d identity=%+v first=%+v", v, identity, first)
				}
				c.Close()
			}
		})
	}
}

func TestRemoteInput_UnavailableGit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		artifact *store.SourceArtifact
	}{
		{name: "unavailable"},
		{name: "missing filesystem", artifact: &store.SourceArtifact{Key: "family", Revision: "revision"}},
		{name: "missing family", artifact: &store.SourceArtifact{FS: fstest.MapFS{}, Revision: "revision"}},
		{name: "missing generation", artifact: &store.SourceArtifact{FS: fstest.MapFS{}, Key: "family"}},
		{name: "digest generation", artifact: &store.SourceArtifact{FS: fstest.MapFS{}, Key: "family", Digest: "digest"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newTestTreeCache(t, "")
			c.SetGitBaseFetcher(func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
				return tc.artifact, nil
			})
			descriptor := remoteInput{Operation: remoteGit, URL: "https://example.invalid/repo.git", Ref: "main"}
			_, identity, err := descriptor.acquire(t.Context(), c, newTreeFS(fstest.MapFS{}, "/src", nil))
			if tc.name == "digest generation" {
				if err != nil || identity.Key != "family" || identity.Digest != "digest" {
					t.Fatalf("digest generation=%+v error=%v", identity, err)
				}
			} else if !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("unavailable Git error=%v", err)
			}
		})
	}
}
