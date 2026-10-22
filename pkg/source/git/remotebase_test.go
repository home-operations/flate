package git

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func newRemoteBaseFetcher(t *testing.T) *Fetcher {
	t.Helper()
	root := t.TempDir()
	return &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
}

func assertWorktree(t *testing.T, input fs.FS, file, wantContent string) {
	t.Helper()
	got, err := fs.ReadFile(input, file)
	if err != nil {
		t.Fatalf("read materialized %s: %v", file, err)
	}
	if string(got) != wantContent {
		t.Errorf("%s = %q, want %q", file, got, wantContent)
	}
	if _, err := fs.Stat(input, ".git"); !os.IsNotExist(err) {
		t.Errorf("materialized base must not contain .git (stat err = %v)", err)
	}
}

func TestFetchRemoteBase_References(t *testing.T) {
	t.Parallel()
	src := filepath.Join(t.TempDir(), "a@b")
	mustInitRepoWithFiles(t, src, map[string]string{"kustomization.yaml": "v1"})
	first := mustTagHEAD(t, src, "v0.2.2")
	mustSetRefToHash(t, src, "refs/heads/feature", first)
	head := mustCommitFile(t, src, "kustomization.yaml", "v2")
	mustSetRefToHash(t, src, "refs/heads/pinned", first)
	mustSetRefToHash(t, src, "refs/remotes/origin/pinned", head)
	for _, tc := range []struct {
		name, url, ref, revision, content string
	}{
		{"tag", "file://" + src, "v0.2.2", first, "v1"},
		{"branch", "file://" + src, "feature", first, "v1"},
		{"divergent named ref", "file://" + src, "refs/heads/pinned", first, "v1"},
		{"commit", "file://" + src, first, first, "v1"},
		{"HEAD file URL", "file://" + src, "", head, "v2"},
		{"HEAD local path", src, "", head, "v2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newRemoteBaseFetcher(t)
			art, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: tc.url, Ref: tc.ref}, sourcecache.Policy{MaxAge: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			if art.Revision != tc.revision {
				t.Errorf("revision = %q, want %q", art.Revision, tc.revision)
			}
			assertWorktree(t, art.FS, "kustomization.yaml", tc.content)
		})
	}
}

func TestFetchRemoteBase_Freshness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		hash bool
		age  time.Duration
	}{
		{name: "tag interval", age: time.Hour},
		{name: "due branch"},
		{name: "hash never expires", hash: true, age: time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			src := t.TempDir()
			mustInitRepoWithFiles(t, src, map[string]string{"kustomization.yaml": "x"})
			hash := mustTagHEAD(t, src, "v1.0.0")
			ref := "v1.0.0"
			if tc.hash {
				ref = hash
			}
			if tc.age == 0 {
				ref = "master"
			}
			var advertisements, uploads, connections atomic.Int32
			handler := gitHTTPHandler(t, src)
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					advertisements.Add(1)
				} else {
					uploads.Add(1)
				}
				handler.ServeHTTP(w, r)
			}))
			srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
			}
			srv.Start()
			t.Cleanup(srv.Close)
			root := t.TempDir()
			f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
			request := RemoteBaseRequest{URL: srv.URL, Ref: ref}
			first, err := f.FetchRemoteBase(t.Context(), request, sourcecache.Policy{MaxAge: tc.age})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("initial: advertisements=%d upload_pack=%d accepted_connections=%d", advertisements.Load(), uploads.Load(), connections.Load())
			wantAdvertisements := int32(2)
			if tc.hash {
				wantAdvertisements = 1
			}
			if advertisements.Load() != wantAdvertisements || uploads.Load() != 1 {
				t.Errorf("initial: advertisements=%d upload_pack=%d, want %d and 1", advertisements.Load(), uploads.Load(), wantAdvertisements)
			}
			beforeAdvertisements, beforeUploads, beforeConnections := advertisements.Load(), uploads.Load(), connections.Load()
			wantContent, wantRevision := "x", first.Revision
			if tc.age == 0 {
				wantContent = "changed"
				wantRevision = mustCommitFile(t, src, "kustomization.yaml", wantContent)
			} else {
				srv.Close()
			}
			if err := f.Cache.Close(); err != nil {
				t.Fatal(err)
			}
			f = &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
			second, err := f.FetchRemoteBase(t.Context(), request, sourcecache.Policy{MaxAge: tc.age})
			if err != nil {
				t.Fatalf("offline cache hit: %v", err)
			}
			if first.Key != second.Key || wantRevision != second.Revision {
				t.Fatalf("cache identity/revision changed: %+v / %+v", first, second)
			}
			assertWorktree(t, second.FS, "kustomization.yaml", wantContent)
			t.Logf("reopened: advertisements=%d upload_pack=%d new_connections=%d", advertisements.Load()-beforeAdvertisements, uploads.Load()-beforeUploads, connections.Load()-beforeConnections)
			wantRequests := int32(0)
			if tc.age == 0 {
				wantRequests = 1
			}
			if advertisements.Load()-beforeAdvertisements != 2*wantRequests || uploads.Load()-beforeUploads != wantRequests {
				t.Errorf("reopened: advertisements=%d upload_pack=%d, want %d and %d", advertisements.Load()-beforeAdvertisements, uploads.Load()-beforeUploads, 2*wantRequests, wantRequests)
			}
			if wantRequests == 0 && connections.Load() != beforeConnections {
				t.Error("fresh remote base opened a connection")
			}
		})
	}
}

func TestFetchRemoteBase_RequiresCache(t *testing.T) {
	t.Parallel()
	f := &Fetcher{}
	_, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "file:///nope", Ref: "v1"}, sourcecache.Policy{MaxAge: time.Hour})
	if !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("want ErrInput without cache, got %v", err)
	}
}

func TestFetchRemoteBase_RejectsBeforeAcquisition(t *testing.T) {
	t.Parallel()
	for _, url := range []string{"", "./a@b", "file:///a@b", "http://h/x@y"} {
		t.Run(url, func(t *testing.T) {
			t.Parallel()
			// The uninitialized cache panics if acquisition precedes validation.
			f := &Fetcher{Cache: new(sourcecache.Cache), RestrictSchemes: true}
			_, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: url, Ref: "v1"}, sourcecache.Policy{MaxAge: time.Hour})
			if !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("want ErrInput before acquisition, got %v", err)
			}
		})
	}
}

func TestFetchRemoteBase_TransientRecovery(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	mustInitRepoWithFiles(t, root, map[string]string{"kustomization.yaml": "content"})
	handler := gitHTTPHandler(t, root)
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			w.Header().Set("Content-Length", "100")
			_, _ = io.WriteString(w, "001e# service=git-upload-pack\n00000040abc")
			return
		}
		handler.ServeHTTP(w, r)
	}))
	defer srv.Close()
	f := newRemoteBaseFetcher(t)
	art, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: srv.URL}, sourcecache.Policy{MaxAge: time.Hour, Retry: sourcecache.RetryConfig{Attempts: 2, MinWait: time.Nanosecond, MaxWait: time.Nanosecond}})
	if err != nil {
		t.Fatalf("original caller: %v; requests=%d", err, hits.Load())
	}
	assertWorktree(t, art.FS, "kustomization.yaml", "content")
	if hits.Load() < 3 {
		t.Fatalf("requests=%d, want failed advertisement plus clone requests", hits.Load())
	}
}

func TestFetchRemoteBase_GenerationAdmission(t *testing.T) {
	t.Parallel()
	for _, disk := range []bool{false, true} {
		t.Run(fmt.Sprintf("disk=%t", disk), func(t *testing.T) {
			t.Parallel()
			src := malformedViewRepo(t)
			root := ""
			if disk {
				root = t.TempDir()
			}
			f := &Fetcher{Cache: cachetest.New(t, root, sourcecache.New)}
			request := RemoteBaseRequest{URL: "file://" + src}
			policy := sourcecache.Policy{MaxAge: time.Hour}
			artifact, err := f.FetchRemoteBase(t.Context(), request, policy)
			if !errors.Is(err, manifest.ErrInput) || artifact == nil || artifact.FS != nil || len(artifact.Warnings) != 1 || artifact.Warnings[0].Category != manifest.WarnSkippedSubmodule || artifact.Warnings[0].Resource != (manifest.NamedResource{}) {
				t.Fatalf("cold invalid view = %v, %v", artifact, err)
			}
			if err := os.RemoveAll(src); err != nil {
				t.Fatal(err)
			}
			mustInitRepo(t, src)
			artifact, err = f.FetchRemoteBase(t.Context(), request, policy)
			if err != nil || artifact == nil {
				t.Fatalf("repaired generation = %v, %v", artifact, err)
			}
			assertWorktree(t, artifact.FS, "hello.txt", "hi")
			t.Run("waiter and hit", func(t *testing.T) {
				t.Parallel()
				testGenerationWaiter(t, disk, func(ctx context.Context, f *Fetcher, url string) (*store.SourceArtifact, error) {
					return f.FetchRemoteBase(ctx, RemoteBaseRequest{URL: url}, policy)
				}, []string{".sourceignore", "apps/drop.tmp", "apps/keep.yaml", "other/keep.yaml"})
			})
		})
	}
}
