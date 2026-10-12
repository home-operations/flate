package git

import (
	"errors"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

func newRemoteBaseFetcher(t *testing.T) *Fetcher {
	t.Helper()
	layout := cacheroot.New(t.TempDir())
	return &Fetcher{Cache: newGitCache(t, layout.Root)}
}

func assertWorktree(t *testing.T, input fs.FS, file, wantContent string) {
	t.Helper()
	got, err := fs.ReadFile(input, file) //nolint:gosec // dir is the test cache slot
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

func TestFetchRemoteBase_Tag(t *testing.T) {
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{"kustomization.yaml": "tag-content"})
	tagged := mustTagHEAD(t, src, "v0.2.2")

	f := newRemoteBaseFetcher(t)
	art, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "file://" + src, Ref: "v0.2.2", MaxAge: time.Hour})
	if err != nil {
		t.Fatalf("FetchRemoteBase tag: %v", err)
	}
	if art.Revision != tagged {
		t.Errorf("revision = %q, want tagged %q", art.Revision, tagged)
	}
	assertWorktree(t, art.FS, "kustomization.yaml", "tag-content")
}

func TestFetchRemoteBase_Branch(t *testing.T) {
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{"kustomization.yaml": "v1"})
	initial := mustHead(t, src)
	mustCommitFile(t, src, "kustomization.yaml", "v2") // advance the default branch
	mustSetRefToHash(t, src, "refs/heads/feature", initial)

	f := newRemoteBaseFetcher(t)
	art, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "file://" + src, Ref: "feature", MaxAge: time.Hour})
	if err != nil {
		t.Fatalf("FetchRemoteBase branch: %v", err)
	}
	if art.Revision != initial {
		t.Errorf("branch revision = %q, want %q (the branch tip, not HEAD)", art.Revision, initial)
	}
	assertWorktree(t, art.FS, "kustomization.yaml", "v1")
}

func TestFetchRemoteBase_Commit(t *testing.T) {
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{"kustomization.yaml": "c1"})
	first := mustHead(t, src)
	mustCommitFile(t, src, "kustomization.yaml", "c2")

	f := newRemoteBaseFetcher(t)
	art, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "file://" + src, Ref: first, MaxAge: time.Hour})
	if err != nil {
		t.Fatalf("FetchRemoteBase commit: %v", err)
	}
	if art.Revision != first {
		t.Errorf("commit revision = %q, want %q", art.Revision, first)
	}
	assertWorktree(t, art.FS, "kustomization.yaml", "c1")
}

func TestFetchRemoteBase_HashNeverExpires(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	ref := mustSetRefToHEAD(t, src, "refs/tags/pin")
	f := newRemoteBaseFetcher(t)
	request := RemoteBaseRequest{URL: "file://" + src, Ref: ref, MaxAge: time.Nanosecond}
	first, err := f.FetchRemoteBase(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(src); err != nil {
		t.Fatal(err)
	}
	second, err := f.FetchRemoteBase(t.Context(), request)
	if err != nil || second.Key != first.Key {
		t.Fatalf("expired hash hit: %+v, %v", second, err)
	}
}

func TestFetchRemoteBase_HEAD(t *testing.T) {
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{"kustomization.yaml": "head-content"})
	head := mustHead(t, src)

	f := newRemoteBaseFetcher(t)
	art, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "file://" + src, Ref: "", MaxAge: time.Hour})
	if err != nil {
		t.Fatalf("FetchRemoteBase HEAD: %v", err)
	}
	if art.Revision != head {
		t.Errorf("HEAD revision = %q, want %q", art.Revision, head)
	}
	assertWorktree(t, art.FS, "kustomization.yaml", "head-content")
}

func TestFetchRemoteBase_CacheHit(t *testing.T) {
	src := t.TempDir()
	mustInitRepoWithFiles(t, src, map[string]string{"kustomization.yaml": "x"})
	mustTagHEAD(t, src, "v1.0.0")

	f := newRemoteBaseFetcher(t)
	art1, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "file://" + src, Ref: "v1.0.0", MaxAge: time.Hour})
	if err != nil {
		t.Fatalf("first FetchRemoteBase: %v", err)
	}
	if err := os.RemoveAll(src); err != nil {
		t.Fatal(err)
	}
	art2, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "file://" + src, Ref: "v1.0.0", MaxAge: time.Hour})
	if err != nil {
		t.Fatalf("second FetchRemoteBase: %v", err)
	}
	if art1.Key != art2.Key {
		t.Errorf("cache hit should reuse slot: %q vs %q", art1.Key, art2.Key)
	}
	if art1.Revision != art2.Revision {
		t.Errorf("revision mismatch across cache hit: %q vs %q", art1.Revision, art2.Revision)
	}
}

func TestFetchRemoteBase_RequiresMirrorAndCache(t *testing.T) {
	f := &Fetcher{} // no Mirrors, no Cache
	_, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "file:///nope", Ref: "v1", MaxAge: time.Hour})
	if !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("want ErrInput without mirror+cache, got %v", err)
	}
}

func TestFetchRemoteBase_MissingURL(t *testing.T) {
	f := newRemoteBaseFetcher(t)
	_, err := f.FetchRemoteBase(t.Context(), RemoteBaseRequest{URL: "", Ref: "v1", MaxAge: time.Hour})
	if !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("want ErrInput for empty url, got %v", err)
	}
}
