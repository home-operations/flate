package git

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/source/git/mirror"
)

// openOnlyMirror opens the single bare mirror the cache layout holds and
// fails if there isn't exactly one. go-git's file:// transport shells out
// to the real git-upload-pack, so shallow boundaries are genuinely written
// to .git/shallow and observable via Storer.Shallow().
func openOnlyMirror(t *testing.T, layout cacheroot.Layout) *git.Repository {
	t.Helper()
	entries, err := os.ReadDir(layout.GitMirrors())
	if err != nil {
		t.Fatalf("ReadDir mirrors: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected exactly one mirror dir, got %d", len(entries))
	}
	repo, err := git.PlainOpen(filepath.Join(layout.GitMirrors(), entries[0].Name()))
	if err != nil {
		t.Fatalf("PlainOpen mirror: %v", err)
	}
	return repo
}

// TestMirror_ShallowCloneTruncatesHistory is the core proof that depth=1
// actually shallow-clones: the tip commit's full tree materializes, but
// the parent commit's object is absent from the mirror and a shallow
// boundary is recorded.
func TestMirror_ShallowCloneTruncatesHistory(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	parent := mustHead(t, src)
	tip := mustCommitFile(t, src, "second.txt", "two")

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)
	f := &Fetcher{Cache: cache, Mirrors: mirror.New(layout), Depth: 1}
	repo := &manifest.GitRepository{
		Name: "t", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + src},
	}

	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if art.Revision != tip {
		t.Errorf("revision = %q, want tip %q", art.Revision, tip)
	}
	// The tip tree is complete even under depth=1 — both files present.
	for _, name := range []string{"hello.txt", "second.txt"} {
		if _, err := os.Stat(filepath.Join(art.LocalPath, name)); err != nil {
			t.Errorf("worktree missing %s under shallow clone: %v", name, err)
		}
	}

	mirrorRepo := openOnlyMirror(t, layout)
	shallows, err := mirrorRepo.Storer.Shallow()
	if err != nil {
		t.Fatalf("Shallow(): %v", err)
	}
	if len(shallows) == 0 {
		t.Error("expected a non-empty shallow boundary; mirror was cloned full")
	}
	// The parent commit must be truncated out of the shallow mirror.
	if _, err := mirrorRepo.CommitObject(plumbing.NewHash(parent)); err == nil {
		t.Errorf("parent commit %s should be absent from a depth=1 mirror", parent)
	}
}

// TestMirror_ShallowTagFetch confirms a tag ref resolves and materializes
// under depth=1, with the pre-tag history truncated.
func TestMirror_ShallowTagFetch(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	parent := mustHead(t, src)
	mustCommitFile(t, src, "chart.yaml", "tagged")
	tagged := mustTagHEAD(t, src, "v1.0.0")

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)
	f := &Fetcher{Cache: cache, Mirrors: mirror.New(layout), Depth: 1}
	repo := &manifest.GitRepository{
		Name: "t", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"},
		},
	}

	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch tag: %v", err)
	}
	if art.Revision != tagged {
		t.Errorf("revision = %q, want %q", art.Revision, tagged)
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "chart.yaml")); err != nil {
		t.Errorf("worktree missing tagged file: %v", err)
	}
	if _, err := openOnlyMirror(t, layout).CommitObject(plumbing.NewHash(parent)); err == nil {
		t.Errorf("pre-tag commit %s should be absent from a depth=1 mirror", parent)
	}
}

// TestMirror_ShallowIncrementalFetchAfterTipMove is the risk test: an
// incremental shallow fetch into an existing shallow mirror when the
// branch tip advanced by more than `depth` commits must still resolve the
// new tip and materialize its tree. go-git's negotiation against a shallow
// boundary is the historically-fragile path; this guards it.
func TestMirror_ShallowIncrementalFetchAfterTipMove(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)
	f := &Fetcher{Cache: cache, Mirrors: mirror.New(layout), Depth: 1}
	repo := &manifest.GitRepository{
		Name: "t", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + src},
	}

	art1, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch 1: %v", err)
	}

	// Advance the tip by two commits — past the depth=1 boundary.
	mustCommitFile(t, src, "c1.txt", "one")
	newTip := mustCommitFile(t, src, "c2.txt", "two")
	if newTip == art1.Revision {
		t.Fatal("tip did not move")
	}

	art2, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch 2 (incremental shallow): %v", err)
	}
	if art2.Revision != newTip {
		t.Errorf("revision after tip move = %q, want new tip %q", art2.Revision, newTip)
	}
	if _, err := os.Stat(filepath.Join(art2.LocalPath, "c2.txt")); err != nil {
		t.Errorf("worktree missing new-tip file after incremental shallow fetch: %v", err)
	}
}

// TestMirror_ShallowSemverFetchesTagTips confirms depth=1 applied across
// the wildcard refs/tags/* refspec brings each tag's tip tree, so semver
// resolution still picks the highest matching tag.
func TestMirror_ShallowSemverFetchesTagTips(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	mustTagHEAD(t, src, "v1.0.0")
	want := mustCommitFile(t, src, "version.txt", "v2")
	mustTagHEAD(t, src, "v2.0.0")

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)
	f := &Fetcher{Cache: cache, Mirrors: mirror.New(layout), Depth: 1}
	repo := &manifest.GitRepository{
		Name: "t", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{SemVer: ">=1.0.0"},
		},
	}

	art, err := f.Fetch(context.Background(), repo)
	if err != nil {
		t.Fatalf("Fetch semver: %v", err)
	}
	if art.Revision != want {
		t.Errorf("semver revision = %q, want v2.0.0 tip %q", art.Revision, want)
	}
	if _, err := os.Stat(filepath.Join(art.LocalPath, "version.txt")); err != nil {
		t.Errorf("worktree missing v2 file: %v", err)
	}
}

// TestMirror_NarrowInitialFetchSkipsOtherRefs proves the initial mirror
// population fetches ONLY the requested ref, not every branch and tag.
// This is the unit-scale version of the real-world finding that a tag
// fetch was pulling thousands of unrelated branches + tags.
func TestMirror_NarrowInitialFetchSkipsOtherRefs(t *testing.T) {
	src := t.TempDir()
	mustInitRepo(t, src)
	mustTagHEAD(t, src, "v1.0.0")
	mustCommitFile(t, src, "f.txt", "two")
	mustTagHEAD(t, src, "v2.0.0")
	mustSetRefToHEAD(t, src, "refs/heads/feature")

	layout := cacheroot.New(t.TempDir())
	cache := source.NewCache(layout)
	f := &Fetcher{Cache: cache, Mirrors: mirror.New(layout), Depth: 1}
	repo := &manifest.GitRepository{
		Name: "t", Namespace: "flux-system",
		GitRepositorySpec: sourcev1.GitRepositorySpec{
			URL:       "file://" + src,
			Reference: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"},
		},
	}
	if _, err := f.Fetch(context.Background(), repo); err != nil {
		t.Fatalf("Fetch tag: %v", err)
	}

	mirrorRepo := openOnlyMirror(t, layout)
	refs, err := mirrorRepo.References()
	if err != nil {
		t.Fatalf("References: %v", err)
	}
	var heads, tags []string
	_ = refs.ForEach(func(r *plumbing.Reference) error {
		switch {
		case r.Name().IsBranch() || strings.HasPrefix(r.Name().String(), "refs/remotes/"):
			heads = append(heads, r.Name().String())
		case r.Name().IsTag():
			tags = append(tags, r.Name().String())
		}
		return nil
	})
	if len(heads) != 0 {
		t.Errorf("narrow tag fetch pulled branches: %v", heads)
	}
	if len(tags) != 1 || tags[0] != "refs/tags/v1.0.0" {
		t.Errorf("expected only refs/tags/v1.0.0, got tags: %v", tags)
	}
}

func TestEffectiveDepth(t *testing.T) {
	cases := []struct {
		name  string
		depth int
		ref   *manifest.GitRepositoryRef
		want  int
	}{
		{"head nil ref", 1, nil, 1},
		{"tag", 1, &sourcev1.GitRepositoryRef{Tag: "v1"}, 1},
		{"branch", 1, &sourcev1.GitRepositoryRef{Branch: "main"}, 1},
		{"name", 1, &sourcev1.GitRepositoryRef{Name: "refs/pull/1/head"}, 1},
		{"semver", 1, &sourcev1.GitRepositoryRef{SemVer: ">=1"}, 1},
		{"commit forces full", 1, &sourcev1.GitRepositoryRef{Commit: "abc"}, 0},
		{"commit+branch forces full", 1, &sourcev1.GitRepositoryRef{Commit: "abc", Branch: "main"}, 0},
		{"depth 0 stays full", 0, &sourcev1.GitRepositoryRef{Tag: "v1"}, 0},
		{"depth 0 commit stays full", 0, &sourcev1.GitRepositoryRef{Commit: "abc"}, 0},
		{"deeper depth passes through", 5, &sourcev1.GitRepositoryRef{Tag: "v1"}, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveDepth(tc.depth, tc.ref); got != tc.want {
				t.Errorf("effectiveDepth(%d, %+v) = %d, want %d", tc.depth, tc.ref, got, tc.want)
			}
		})
	}
}

// TestFetch_ShallowLegacyClone covers the non-mirror clone path (nil
// Mirrors): plain and sparse fetches must both succeed under depth=1 and
// resolve the right tip.
func TestFetch_ShallowLegacyClone(t *testing.T) {
	t.Run("plain", func(t *testing.T) {
		src := t.TempDir()
		mustInitRepo(t, src)
		tip := mustCommitFile(t, src, "second.txt", "two")

		cache := source.NewCache(cacheroot.New(t.TempDir()))
		f := &Fetcher{Cache: cache, Depth: 1} // nil Mirrors → legacy path
		repo := &manifest.GitRepository{
			Name: "t", Namespace: "flux-system",
			GitRepositorySpec: sourcev1.GitRepositorySpec{URL: "file://" + src},
		}
		art, err := f.Fetch(context.Background(), repo)
		if err != nil {
			t.Fatalf("Fetch legacy shallow: %v", err)
		}
		if art.Revision != tip {
			t.Errorf("revision = %q, want %q", art.Revision, tip)
		}
		if _, err := os.Stat(filepath.Join(art.LocalPath, "second.txt")); err != nil {
			t.Errorf("worktree missing tip file: %v", err)
		}
	})

	t.Run("sparse", func(t *testing.T) {
		src := t.TempDir()
		mustInitRepoWithFiles(t, src, map[string]string{
			"apps/a/manifest.yaml": "kind: ConfigMap",
			"apps/b/manifest.yaml": "kind: ConfigMap",
		})

		cache := source.NewCache(cacheroot.New(t.TempDir()))
		f := &Fetcher{Cache: cache, Depth: 1}
		repo := &manifest.GitRepository{
			Name: "t", Namespace: "flux-system",
			GitRepositorySpec: sourcev1.GitRepositorySpec{
				URL:            "file://" + src,
				SparseCheckout: []string{"apps/a"},
			},
		}
		art, err := f.Fetch(context.Background(), repo)
		if err != nil {
			t.Fatalf("Fetch legacy shallow sparse: %v", err)
		}
		if _, err := os.Stat(filepath.Join(art.LocalPath, "apps", "a", "manifest.yaml")); err != nil {
			t.Errorf("sparse-included file missing under shallow: %v", err)
		}
		if _, err := os.Stat(filepath.Join(art.LocalPath, "apps", "b", "manifest.yaml")); !os.IsNotExist(err) {
			t.Errorf("sparse-excluded file should be absent; stat err = %v", err)
		}
	})
}
