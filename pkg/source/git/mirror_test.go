package git

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/source/git/mirror"
)

// TestMirror_BareClonePersistsAcrossFetches confirms that a Fetcher
// with Mirrors set creates the bare mirror once and reuses it on the
// second fetch — the per-URL mirror dir is present after run 1 and
// still present after run 2, while the slot worktree is materialized
// each time (the slot lives at a different path than the mirror).
func TestMirror_BareClonePersistsAcrossFetches(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	cacheDir := t.TempDir()
	layout := cacheroot.New(cacheDir)
	mirrorDir := layout.GitMirrors()

	cache := source.NewCache(layout)
	repo := &manifest.GitRepository{
		Name: "t", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + src},
	}
	f := &Fetcher{Cache: cache, Mirrors: mirror.New(layout)}

	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch 1: %v", err)
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "hello.txt")); err != nil {
		t.Errorf("worktree missing hello.txt: %v", err)
	}

	// The mirror directory must exist after the first fetch.
	entries, err := os.ReadDir(mirrorDir)
	if err != nil {
		t.Fatalf("mirror dir missing: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("expected one mirror dir, got %d: %v", len(entries), entries)
	}

	// Second fetch reuses the slot AND the mirror.
	art2, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch 2: %v", err)
	}
	if art2.LocalPath != art.LocalPath {
		t.Errorf("slot path drifted: %s vs %s", art.LocalPath, art2.LocalPath)
	}
	entries2, _ := os.ReadDir(mirrorDir)
	if len(entries2) != 1 {
		t.Errorf("mirror dir count drifted: %d → %d", len(entries), len(entries2))
	}
}

// TestMirror_FallsBackForSubmodules confirms that RecurseSubmodules=true
// disables the mirror path so go-git's submodule support still works.
// We don't actually wire up a submodule repo — we only assert the
// branch decision via canUseMirror.
func TestMirror_FallsBackForSubmodules(t *testing.T) {
	f := &Fetcher{Mirrors: mirror.New(cacheroot.New(t.TempDir()))}
	repo := &manifest.GitRepository{
		Name: "t", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL: "https://example.com/x", RecurseSubmodules: true,
		},
	}
	if f.canUseMirror(repo, "https://example.com/x") {
		t.Error("RecurseSubmodules should disable the mirror path")
	}
	repo.RecurseSubmodules = false
	repo.SparseCheckout = []string{"sub/"}
	if f.canUseMirror(repo, "https://example.com/x") {
		t.Error("SparseCheckout should disable the mirror path")
	}
	repo.SparseCheckout = nil
	if !f.canUseMirror(repo, "https://example.com/x") {
		t.Error("vanilla fetch should be mirror-eligible")
	}

	// Nil Mirrors → legacy.
	f2 := &Fetcher{}
	if f2.canUseMirror(repo, "https://example.com/x") {
		t.Error("nil Mirrors should disable the mirror path")
	}
}

// TestMirror_TagResolvesAcrossRefs covers the cross-ref reuse the
// mirror enables: a single bare clone covers both the main branch and
// a tag, and fetching each materializes the right commit.
func TestMirror_TagResolvesAcrossRefs(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	tagged := mustTagHEAD(t, src, "v1.0.0")

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)
	f := &Fetcher{Cache: cache, Mirrors: mirror.New(layout)}

	// First: fetch a tag.
	tagRepo := &manifest.GitRepository{
		Name: "t", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"},
		},
	}
	art, err := f.Fetch(context.Background(), tagRepo)
	if err != nil {
		t.Fatalf("Fetch tag: %v", err)
	}
	if art.Revision != tagged {
		t.Errorf("tag revision = %q, want %q", art.Revision, tagged)
	}

	// Second: fetch HEAD (same commit but different ref path).
	headRepo := &manifest.GitRepository{
		Name: "u", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + src},
	}
	art2, err := f.Fetch(context.Background(), headRepo)
	if err != nil {
		t.Fatalf("Fetch HEAD: %v", err)
	}
	if art2.Revision != tagged {
		t.Errorf("HEAD revision = %q, want %q", art2.Revision, tagged)
	}
	if art.LocalPath == art2.LocalPath {
		t.Error("different refs should land in different slots")
	}
}
