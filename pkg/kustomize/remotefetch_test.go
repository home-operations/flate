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
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

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
