package git

import (
	"errors"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
)

func newRemoteBaseFetcher(t *testing.T) *Fetcher {
	t.Helper()
	root := t.TempDir()
	return &Fetcher{Cache: newGitCache(t, root)}
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
	// gittree.Materialize produces a clean worktree — no .git.
	if _, err := fs.Stat(input, ".git"); !os.IsNotExist(err) {
		t.Errorf("materialized base must not contain .git (stat err = %v)", err)
	}
}

func TestFetchRemoteBase_References(t *testing.T) {
	src := filepath.Join(t.TempDir(), "a@b")
	mustInitRepoWithFiles(t, src, map[string]string{"kustomization.yaml": "v1"})
	first := mustTagHEAD(t, src, "v0.2.2")
	mustSetRefToHash(t, src, "refs/heads/feature", first)
	head := mustCommitFile(t, src, "kustomization.yaml", "v2")
	for _, tc := range []struct {
		name, url, ref, revision, content string
	}{
		{"tag", "file://" + src, "v0.2.2", first, "v1"},
		{"branch", "file://" + src, "feature", first, "v1"},
		{"commit", "file://" + src, first, first, "v1"},
		{"HEAD file URL", "file://" + src, "", head, "v2"},
		{"HEAD local path", src, "", head, "v2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newRemoteBaseFetcher(t)
			art, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: tc.url, Ref: tc.ref, MaxAge: time.Hour}, sourcecache.RetryConfig{})
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
	for _, tc := range []struct {
		name string
		hash bool
		age  time.Duration
	}{
		{name: "tag interval", age: time.Hour},
		{name: "hash never expires", hash: true, age: time.Nanosecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := t.TempDir()
			mustInitRepoWithFiles(t, src, map[string]string{"kustomization.yaml": "x"})
			hash := mustTagHEAD(t, src, "v1.0.0")
			ref := "v1.0.0"
			if tc.hash {
				ref = hash
			}
			f := newRemoteBaseFetcher(t)
			request := RemoteBaseRequest{URL: "file://" + src, Ref: ref, MaxAge: tc.age}
			first, err := f.FetchRemoteBase(t.Context(), request, sourcecache.RetryConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(src); err != nil {
				t.Fatal(err)
			}
			second, err := f.FetchRemoteBase(t.Context(), request, sourcecache.RetryConfig{})
			if err != nil {
				t.Fatalf("offline cache hit: %v", err)
			}
			if first.Key != second.Key || first.Revision != second.Revision {
				t.Fatalf("cache identity/revision changed: %+v / %+v", first, second)
			}
			assertWorktree(t, second.FS, "kustomization.yaml", "x")
		})
	}
}

func TestFetchRemoteBase_RequiresCache(t *testing.T) {
	f := &Fetcher{}
	_, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "file:///nope", Ref: "v1", MaxAge: time.Hour}, sourcecache.RetryConfig{})
	if !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("want ErrInput without cache, got %v", err)
	}
}

func TestFetchRemoteBase_RejectsBeforeAcquisition(t *testing.T) {
	for _, url := range []string{"", "./a@b", "file:///a@b", "http://h/x@y"} {
		t.Run(url, func(t *testing.T) {
			// The uninitialized cache panics if acquisition precedes validation.
			f := &Fetcher{Cache: new(sourcecache.Cache), RestrictSchemes: true}
			_, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: url, Ref: "v1", MaxAge: time.Hour}, sourcecache.RetryConfig{})
			if !errors.Is(err, manifest.ErrInput) {
				t.Fatalf("want ErrInput before acquisition, got %v", err)
			}
		})
	}
}

func TestFetchRemoteBase_TransientRecovery(t *testing.T) {
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
	art, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: srv.URL, MaxAge: time.Hour}, sourcecache.RetryConfig{Attempts: 2, MinWait: time.Nanosecond, MaxWait: time.Nanosecond})
	if err != nil {
		t.Fatalf("original caller: %v; requests=%d", err, hits.Load())
	}
	assertWorktree(t, art.FS, "kustomization.yaml", "content")
	if hits.Load() < 3 {
		t.Fatalf("requests=%d, want failed advertisement plus clone requests", hits.Load())
	}
}
