package kustomize

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/internal/assert"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestRemoteInput_CancelQueuedGitBase(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 16} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				c := newTestTreeCache(t, "")
				c.tasks = task.NewBounded(workers)
				entered, release := make(chan struct{}, workers), make(chan struct{})
				for range workers {
					c.tasks.Go(t.Context(), "blocker", func(context.Context) {
						entered <- struct{}{}
						<-release
					})
				}
				for range workers {
					<-entered
				}
				calls := 0
				c.gitBase = func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
					calls++
					return &store.SourceArtifact{
						FS:       fstest.MapFS{},
						Key:      "key",
						Content:  "key-generation",
						Revision: "revision",
					}, nil
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				descriptor := remoteInput{Operation: remoteGit, URL: "https://example.invalid/repo"}
				done := make(chan error, 1)
				go func() {
					acquired, identity, err := descriptor.acquire(ctx, c, &treeFS{})
					{
						assert.ErrorIf(
							t,
							acquired != nil || identity != (remoteIdentity{}),
							"canceled acquisition filesystem=%v identity=%+v",
							acquired,
							identity,
						)
					}
					done <- err
				}()
				synctest.Wait()
				cancel()
				close(release)
				{
					err := <-done
					assert.ErrorIf(
						t,
						!errors.Is(err, context.Canceled),
						"queued acquisition=%v, want context cancellation",
						err,
					)
				}
				c.tasks.BlockTillDone()
				{
					_, _, err := descriptor.acquire(ctx, c, &treeFS{})
					assert.ErrorIf(t, !errors.Is(err, context.Canceled), "canceled reacquisition=%v", err)
				}
				c.tasks.BlockTillDone()
				{
					assert.FatalIf(t, calls != 0, "canceled git fetches=%d, want 0", calls)
				}
				{
					_, _, err := descriptor.acquire(t.Context(), c, &treeFS{})
					assert.NoError(t, err)
				}
				c.tasks.BlockTillDone()
				{
					assert.FatalIf(t, calls != 1, "surviving acquisition calls=%d, want 1", calls)
				}
			})
		})
	}
}

func TestRemoteInput_AcquisitionIdentity(t *testing.T) {
	t.Parallel()
	for _, operation := range []remoteOperation{remoteHTTP, remoteGit} {
		t.Run(fmt.Sprint(operation), func(t *testing.T) {
			t.Parallel()
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
				c.SetGitBaseFetcher(
					func(_ context.Context, request GitBaseRequest, gotRetry sourcecache.RetryConfig) (*store.SourceArtifact, error) {
						{
							assert.FatalIf(
								t,
								operation != remoteGit || request.URL != descriptor.URL || request.Ref != descriptor.Ref ||
									request.MaxAge != time.Minute ||
									gotRetry != retry,
								"acquisition request=%+v retry=%+v",
								request,
								gotRetry,
							)
						}
						return &store.SourceArtifact{
							Key:      "family",
							Content:  strconv.Itoa(int(v)),
							Revision: strconv.Itoa(int(v)),
							FS:       fstest.MapFS{"content": {Data: fmt.Append(nil, v)}},
						}, nil
					},
				)
				input := newTreeFS(fstest.MapFS{}, "/src", nil)
				input.retry, input.maxAge = retry, time.Minute
				acquired, identity, err := descriptor.acquire(t.Context(), c, input)
				{
					assert.NoError(t, err)
				}
				body, err := fs.ReadFile(acquired, "content")
				{
					assert.FatalIf(
						t,
						err != nil || string(body) != strconv.Itoa(int(v)),
						"acquired body=%q error=%v",
						body,
						err,
					)
				}
				{
					assert.FatalIf(t, operation == remoteHTTP &&
						(identity.Content == "" || identity.Content != sourcecache.AcquisitionIdentity(acquired)), "HTTP identity=%+v", identity)
				}
				{
					assert.FatalIf(
						t,
						operation == remoteGit &&
							(identity.Key != "family" || identity.Content != strconv.Itoa(int(v))),
						"Git identity=%+v",
						identity,
					)
				}
				if first == (remoteIdentity{}) {
					first = identity
				}
				{
					assert.FatalIf(
						t,
						(identity == first) != (v == 1),
						"generation %d identity=%+v first=%+v",
						v,
						identity,
						first,
					)
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
		err      error
	}{
		{name: "unavailable"},
		{name: "fetch-error", err: fs.ErrNotExist},
		{name: "missing filesystem", artifact: &store.SourceArtifact{Key: "family", Content: "family-generation", Revision: "revision"}},
		{name: "missing family", artifact: &store.SourceArtifact{FS: fstest.MapFS{}, Revision: "revision"}},
		{name: "missing generation", artifact: &store.SourceArtifact{FS: fstest.MapFS{}, Key: "family"}},
		{name: "digest without generation", artifact: &store.SourceArtifact{FS: fstest.MapFS{}, Key: "family", Digest: "digest"}},
		{name: "content generation", artifact: &store.SourceArtifact{FS: fstest.MapFS{}, Key: "family", Content: "generation", Digest: "digest"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newTestTreeCache(t, "")
			c.SetGitBaseFetcher(
				func(context.Context, GitBaseRequest, sourcecache.RetryConfig) (*store.SourceArtifact, error) {
					return tc.artifact, tc.err
				},
			)
			descriptor := remoteInput{Operation: remoteGit, URL: "https://example.invalid/repo.git", Ref: "main"}
			_, identity, err := descriptor.acquire(t.Context(), c, newTreeFS(fstest.MapFS{}, "/src", nil))
			if tc.name == "content generation" {
				{
					assert.FatalIf(
						t,
						err != nil || identity.Key != "family" || identity.Digest != "digest",
						"digest generation=%+v error=%v",
						identity,
						err,
					)
				}
			} else {
				wantErr := tc.err
				if wantErr == nil {
					wantErr = manifest.ErrInput
				}
				{
					assert.FatalIf(
						t,
						!errors.Is(err, wantErr) || identity != (remoteIdentity{}),
						"unavailable Git identity=%+v error=%v",
						identity,
						err,
					)
				}
			}
		})
	}
}

func TestRemoteInput_HTTPFailureObservation(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusTooManyRequests, http.StatusInternalServerError} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			t.Parallel()
			var hits atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				hits.Add(1)
				w.WriteHeader(status)
			}))
			t.Cleanup(srv.Close)
			c := newTestTreeCache(t, "")
			descriptor := remoteInput{Operation: remoteHTTP, URL: srv.URL}
			acquired, identity, err := descriptor.acquire(t.Context(), c, &treeFS{})
			response, ok := errors.AsType[*fetcherr.ResponseError](err)
			want := remoteIdentity{}
			if status == http.StatusNotFound {
				want.HTTPStatus = status
			}
			{
				assert.FatalIf(t, acquired != nil || !ok || response.StatusCode != status || identity != want ||
					!errors.Is(
						err,
						manifest.ErrFlux,
					), "HTTP %d: filesystem=%v identity=%+v error=%v", status, acquired, identity, err)
			}
			if status == http.StatusNotFound {
				_, original := c.remoteInputs.Acquire(t.Context(), descriptor.URL, sourcecache.RetryConfig{}, nil)
				if !errors.Is(original, err) ||
					hits.Load() != 1 {
					t.Fatalf(
						"acquisition changed error or repeated probe: original=%v error=%v requests=%d",
						original,
						err,
						hits.Load(),
					)
				}
			}
		})
	}
}

func TestRemoteInput_HTTPFailureIdentity(t *testing.T) {
	t.Parallel()
	notFound := &fetcherr.ResponseError{StatusCode: http.StatusNotFound}
	independent := errors.New("independent failure")
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "bare", err: notFound, want: true},
		{name: "wrapped", err: fmt.Errorf("probe: %w", notFound), want: true},
		{name: "terminal-domain-tree", err: &sourcecache.TerminalError{Err: manifest.NewError(manifest.ErrFlux, fmt.Errorf("probe: %w", errors.Join(notFound, nil)), ""), Attempted: true}, want: true},
		{name: "forbidden", err: &fetcherr.ResponseError{StatusCode: http.StatusForbidden}},
		{name: "rate-limited", err: &fetcherr.ResponseError{StatusCode: http.StatusTooManyRequests}},
		{name: "server", err: &fetcherr.ResponseError{StatusCode: http.StatusInternalServerError}},
		{name: "network", err: &net.OpError{Op: "dial", Net: "tcp", Err: independent}},
		{name: "canceled", err: errors.Join(notFound, context.Canceled)},
		{name: "deadline", err: errors.Join(context.DeadlineExceeded, notFound)},
		{name: "cache", err: &sourcecache.CacheError{Err: notFound}},
		{name: "independent-last", err: errors.Join(notFound, independent)},
		{name: "independent-first", err: errors.Join(independent, notFound)},
		{name: "conflicting-status-last", err: errors.Join(notFound, &fetcherr.ResponseError{StatusCode: http.StatusForbidden})},
		{name: "conflicting-status-first", err: errors.Join(&fetcherr.ResponseError{StatusCode: http.StatusForbidden}, notFound)},
		{name: "domain-only", err: manifest.ErrFlux},
		{name: "response-cause", err: &fetcherr.ResponseError{StatusCode: http.StatusNotFound, Err: independent}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			want := remoteIdentity{}
			if tc.want {
				want.HTTPStatus = http.StatusNotFound
			}
			{
				got := httpFailureIdentity(tc.err)
				assert.FatalIf(t, got != want, "observation=%+v, want %+v: %v", got, want, tc.err)
			}
		})
	}
}

type remoteHTTPRequest struct {
	Method   string `json:"Method"`
	Path     string `json:"Path"`
	ETag     string `json:"ETag"`
	Modified string `json:"Modified"`
	Status   int    `json:"Status"`
	Bytes    int    `json:"Bytes"`
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
		record := remoteHTTPRequest{
			Method:   r.Method,
			Path:     r.URL.Path,
			ETag:     r.Header.Get("If-None-Match"),
			Modified: r.Header.Get("If-Modified-Since"),
			Status:   http.StatusOK,
		}
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
