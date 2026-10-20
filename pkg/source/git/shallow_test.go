package git

import (
	"errors"
	"strings"
	"testing"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"

	objectcache "github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

func openGeneration(t *testing.T, f *Fetcher, spec *manifest.GitRepository, artifact *store.SourceArtifact) *git.Repository {
	t.Helper()
	ref := "HEAD"
	if label := (*manifest.GitRepositoryRef)(spec.Reference).Label(); label != "" {
		ref = label
	}
	request, err := f.resolveTransport(spec)
	if err != nil {
		t.Fatal(err)
	}
	key := sourcecache.GitKey{Content: struct {
		Source, Transport any
		Depth             int
	}{spec.Content((*manifest.GitRepositoryRef)(spec.Reference).Normalized()), request.material, f.Depth}, Ref: ref, RecurseSubmodules: spec.RecurseSubmodules}
	generation, id, err := f.Cache.Git(t.Context(), key, sourcecache.Policy{MaxAge: sourcecache.Forever}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if id != artifact.Key {
		t.Fatalf("generation identity = %q, want %q", id, artifact.Key)
	}
	storer := filesystem.NewStorageWithOptions(generation.Backing, objectcache.NewObjectLRU(0), filesystem.Options{LargeObjectThreshold: 1})
	t.Cleanup(func() {
		if err := storer.Close(); err != nil {
			t.Error(err)
		}
	})
	repo, err := git.Open(storer, nil)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

func TestFetcher_NarrowInitialFetchSkipsOtherRefs(t *testing.T) {
	t.Parallel()
	src := t.TempDir()
	mustInitRepo(t, src)
	parentHash := plumbing.NewHash(mustHead(t, src))
	mustCommitFile(t, src, "f.txt", "one")
	selectedHash := plumbing.NewHash(mustTagHEAD(t, src, "v1.0.0"))
	mustCommitFile(t, src, "f.txt", "two")
	mustTagHEAD(t, src, "v2.0.0")
	mustSetRefToHEAD(t, src, "refs/heads/feature")

	sourceRepo, err := git.PlainOpen(src)
	if err != nil {
		t.Fatal(err)
	}
	selected, err := sourceRepo.CommitObject(selectedHash)
	if err != nil {
		t.Fatal(err)
	}
	if len(selected.ParentHashes) != 1 || selected.ParentHashes[0] != parentHash {
		t.Fatalf("source commit parents = %v, want [%s]", selected.ParentHashes, parentHash)
	}
	if _, err := sourceRepo.CommitObject(parentHash); err != nil {
		t.Fatalf("source parent commit: %v", err)
	}

	root := t.TempDir()
	cache := cachetest.New(t, root, sourcecache.New)
	f := &Fetcher{Cache: cache, Depth: 1}
	repo := &manifest.GitRepository{
		Name: "t", Namespace: "flux-system",
		URL:       "file://" + src,
		Reference: &sourcev1.GitRepositoryRef{Tag: "v1.0.0"},
	}
	art, err := f.Fetch(t.Context(), repo, sourcecache.Policy{MaxAge: repo.SourceInterval()})
	if err != nil {
		t.Fatalf("Fetch tag: %v", err)
	}

	generationRepo := openGeneration(t, f, repo, art)
	selected, err = generationRepo.CommitObject(selectedHash)
	if err != nil {
		t.Fatalf("fetched commit: %v", err)
	}
	if len(selected.ParentHashes) != 1 || selected.ParentHashes[0] != parentHash {
		t.Fatalf("fetched commit parents = %v, want [%s]", selected.ParentHashes, parentHash)
	}
	if _, err := generationRepo.CommitObject(parentHash); !errors.Is(err, plumbing.ErrObjectNotFound) {
		t.Fatalf("shallow parent commit: got %v, want %v", err, plumbing.ErrObjectNotFound)
	}
	refs, err := generationRepo.References()
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

func TestResolveTransport_Depth(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		depth int
		ref   *sourcev1.GitRepositoryRef
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
			t.Parallel()
			f := &Fetcher{Depth: tc.depth}
			request, err := f.resolveTransport(&manifest.GitRepository{URL: "file:///repo", Reference: tc.ref})
			if err != nil {
				t.Fatal(err)
			}
			if got := request.options.Depth; got != tc.want {
				t.Errorf("depth = %d, want %d", got, tc.want)
			}
		})
	}
}
