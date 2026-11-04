package git

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveLocal_References(t *testing.T) {
	t.Parallel()
	fixture := localFixture(t)

	for name, hash := range map[string]plumbing.Hash{
		"refs/heads/pinned": fixture.base, "refs/remotes/origin/pinned": fixture.head,
		"refs/remotes/origin/tracking": fixture.base, "refs/pull/1/head": fixture.base,
	} {
		{
			err := fixture.repo.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(name), hash))
			assert.NoError(t, err)
		}
	}
	{
		_, err := fixture.repo.CreateTag("annotated", fixture.base, &git.CreateTagOptions{
			Tagger: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}, Message: "pin",
		})
		assert.NoError(t, err)
	}

	for _, tc := range []struct {
		name string
		ref  sourcev1.GitRepositoryRef
		want plumbing.Hash
		fail bool
	}{
		{"tag", sourcev1.GitRepositoryRef{Tag: "v1.0.0"}, fixture.base, false},
		{"annotated", sourcev1.GitRepositoryRef{Tag: "annotated"}, fixture.base, false},
		{"commit", sourcev1.GitRepositoryRef{Commit: fixture.base.String()}, fixture.base, false},
		{"name", sourcev1.GitRepositoryRef{Name: "refs/pull/1/head"}, fixture.base, false},
		{"branch-local-first", sourcev1.GitRepositoryRef{Branch: "pinned"}, fixture.base, false},
		{"branch-origin", sourcev1.GitRepositoryRef{Branch: "tracking"}, fixture.base, false},
		{"name-origin", sourcev1.GitRepositoryRef{Name: "refs/heads/tracking"}, fixture.base, false},
		{"semver", sourcev1.GitRepositoryRef{SemVer: "<2.0.0"}, fixture.base, false},
		{"commit-over-name", sourcev1.GitRepositoryRef{Commit: fixture.base.String(), Name: "bad name", SemVer: "invalid", Tag: "v2.0.0"}, fixture.base, false},
		{"name-over-semver", sourcev1.GitRepositoryRef{Name: "refs/pull/1/head", SemVer: "invalid", Tag: "v2.0.0", Branch: "bad branch"}, fixture.base, false},
		{"semver-over-tag", sourcev1.GitRepositoryRef{SemVer: "<2.0.0", Tag: "bad tag", Branch: "bad branch"}, fixture.base, false},
		{"tag-over-branch", sourcev1.GitRepositoryRef{Tag: "v1.0.0", Branch: "bad branch"}, fixture.base, false},
		{"head", sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, fixture.head, false},
		{"commit-branch-reachable", sourcev1.GitRepositoryRef{Commit: fixture.base.String(), Branch: "master"}, fixture.base, false},
		{"commit-branch-unreachable", sourcev1.GitRepositoryRef{Commit: fixture.head.String(), Branch: "pinned"}, plumbing.ZeroHash, true},
		{"commit-branch-missing", sourcev1.GitRepositoryRef{Commit: fixture.base.String(), Branch: "missing"}, plumbing.ZeroHash, false},
		{"missing-tag", sourcev1.GitRepositoryRef{Tag: "missing"}, plumbing.ZeroHash, false},
		{"missing-name", sourcev1.GitRepositoryRef{Name: "refs/heads/missing"}, plumbing.ZeroHash, false},
		{"missing-branch", sourcev1.GitRepositoryRef{Branch: "missing"}, plumbing.ZeroHash, false},
		{"unmatched-semver", sourcev1.GitRepositoryRef{SemVer: ">3.0.0"}, plumbing.ZeroHash, false},
		{"invalid-semver", sourcev1.GitRepositoryRef{SemVer: "invalid"}, plumbing.ZeroHash, true},
		{"short-commit", sourcev1.GitRepositoryRef{Commit: fixture.base.String()[:12]}, plumbing.ZeroHash, true},
		{"nonhex-commit", sourcev1.GitRepositoryRef{Commit: strings.Repeat("g", 40)}, plumbing.ZeroHash, true},
		{"missing-commit", sourcev1.GitRepositoryRef{Commit: strings.Repeat("f", 40)}, plumbing.ZeroHash, false},
		{"missing-commit-branch", sourcev1.GitRepositoryRef{Commit: strings.Repeat("f", 40), Branch: "master"}, plumbing.ZeroHash, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testResolveLocalReferencesCase(t, &fixture.root, &fixture.head, &tc)
		},
		)
	}
	for _, metadata := range []string{"directory", "gitfile"} {
		t.Run("metadata "+metadata, func(t *testing.T) {
			t.Parallel()
			testResolveLocalReferencesCase2(t, &metadata)
		},
		)
	}
}

func TestResolveRefHash_ObjectClassification(t *testing.T) {
	t.Parallel()
	fixture := localFixture(t)

	missing := plumbing.NewHash(strings.Repeat("f", 40))
	commit, err := fixture.repo.CommitObject(fixture.base)
	assert.NoError(t, err)
	tree := commit.TreeHash
	commit.TreeHash = missing
	encoded := fixture.repo.Storer.NewEncodedObject()
	assert.NoError(t, commit.Encode(encoded))
	broken, err := fixture.repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	for _, tc := range []struct {
		name        string
		hash        plumbing.Hash
		unavailable bool
	}{
		{"absent commit", missing, true},
		{"wrong object type", tree, false},
		{"missing tree", broken, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := resolveRefHash(fixture.repo, &sourcev1.GitRepositoryRef{Commit: tc.hash.String()}, false)
			if err == nil || errors.Is(err, errRefUnavailable) != tc.unavailable {
				t.Fatalf("resolution error = %v, want unavailable=%t", err, tc.unavailable)
			}
		})
	}
}

func TestResolveLocal_IncompleteHistory(t *testing.T) {
	t.Parallel()
	fixture := localFixture(t)

	commit, err := fixture.repo.CommitObject(fixture.head)
	assert.NoError(t, err)

	commit.ParentHashes = []plumbing.Hash{plumbing.NewHash(strings.Repeat("f", 40))}
	encoded := fixture.repo.Storer.NewEncodedObject()
	{
		encodeErr := commit.Encode(encoded)
		assert.NoError(t, encodeErr)
	}

	hash, err := fixture.repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	{
		setReferenceErr := fixture.repo.Storer.SetReference(
			plumbing.NewHashReference(plumbing.NewBranchReferenceName("master"), hash),
		)
		assert.NoError(t, setReferenceErr)
	}

	{
		_, resolveRefHashErr := resolveRefHash(
			fixture.repo,
			&sourcev1.GitRepositoryRef{Commit: fixture.base.String(), Branch: "master"},
			false,
		)
		assert.ErrorIf(t, !errors.Is(
			resolveRefHashErr,
			errRefUnavailable,
		), "incomplete history did not become unavailable: %v", resolveRefHashErr)
	}
	repository := localSource(&sourcev1.GitRepositoryRef{Commit: fixture.base.String(), Branch: "master"})
	got, err := ResolveLocal(t.Context(), localLocation(t, fixture.root), []*manifest.GitRepository{repository}, "")
	assert.FatalIf(t, err != nil || len(got) != 0, "incomplete history did not fall back: %v, %v", got, err)
}

//nolint:paralleltest // Allocation measurements use process-wide test accounting.
//nolint:paralleltest // Allocation measurements use process-wide test accounting.
func TestResolveLocal_NoWarningAllocations(t *testing.T) {
	empty := []*manifest.GitRepository{localSource(&sourcev1.GitRepositoryRef{})}
	authored := []*manifest.GitRepository{localSource(&sourcev1.GitRepositoryRef{Tag: "v1.0.0"})}
	selected := (*manifest.GitRepositoryRef)(authored[0].Reference)
	emptyAllocs := testing.AllocsPerRun(1000, func() {
		_, err := ResolveLocal(t.Context(), nil, empty, "")
		assert.NoError(t, err)
	})
	validationAllocs := testing.AllocsPerRun(1000, func() {
		assert.NoError(t, selected.Validate())
	})
	authoredAllocs := testing.AllocsPerRun(1000, func() {
		_, err := ResolveLocal(t.Context(), nil, authored, "")
		assert.NoError(t, err)
	})
	{
		want := emptyAllocs + validationAllocs
		assert.FatalIf(
			t,
			authoredAllocs > want,
			"reference formatting without warnings: %g allocations, want %g",
			authoredAllocs,
			want,
		)
	}
}

func TestResolveLocal_NoStoreValidatesEffectiveSyntax(t *testing.T) {
	t.Parallel()
	for _, ref := range []sourcev1.GitRepositoryRef{
		{Commit: "abc"}, {Name: "bad name"}, {Tag: "bad tag"}, {Branch: "bad branch"}, {SemVer: "invalid"},
	} {
		t.Run((*manifest.GitRepositoryRef)(&ref).String(), func(t *testing.T) {
			t.Parallel()
			{
				_, err := ResolveLocal(
					t.Context(),
					localLocation(t, t.TempDir()),
					[]*manifest.GitRepository{localSource(&ref)},
					"",
				)
				assert.FatalIf(t, !errors.Is(
					err,
					manifest.ErrInput,
				), "expected invalid spec, got %v", err)
			}
		})
	}
	fixture := localFixture(t)

	child := filepath.Join(fixture.root, "exported")
	assert.Equal(t, os.Mkdir(child, 0o750), nil)
	repository := localSource(&sourcev1.GitRepositoryRef{Tag: "v1.0.0"})
	got, err := ResolveLocal(t.Context(), localLocation(t, child), []*manifest.GitRepository{repository}, "")
	assert.Equal(t, err, nil)
	assert.Equal(t, len(got), 0)
	testutil.WriteFile(t, fixture.root, ".git/config", "[invalid\n")
	got, err = ResolveLocal(t.Context(), localLocation(t, child), []*manifest.GitRepository{repository}, "")
	assert.Equal(t, err, nil)
	assert.Equal(t, len(got), 0)
	assert.Equal(t, os.RemoveAll(filepath.Join(fixture.root, ".git")), nil)
	assert.Equal(t, os.Symlink(".git", filepath.Join(fixture.root, ".git")), nil)
	got, err = ResolveLocal(t.Context(), localLocation(t, child), []*manifest.GitRepository{repository}, "")
	assert.Equal(t, err, nil)
	assert.Equal(t, len(got), 0)
}

func TestResolveLocal_RejectsNilSource(t *testing.T) {
	t.Parallel()
	{
		_, err := ResolveLocal(
			t.Context(),
			localLocation(t, t.TempDir()),
			[]*manifest.GitRepository{nil},
			"",
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrInput,
		), "nil source = %v", err)
	}
}

func TestResolveLocal_CacheReuseIsolationAndMovedTags(t *testing.T) {
	t.Parallel()
	fixture := localFixture(t)

	fixture.base = submoduleCommit(t, fixture.repo, fixture.base)
	assert.NoError(
		t,
		fixture.repo.Storer.SetReference(
			plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), fixture.base),
		),
	)

	first := localSource(&sourcev1.GitRepositoryRef{Tag: "v1.0.0"})
	second := localSource(&sourcev1.GitRepositoryRef{Commit: fixture.base.String()})
	second.Name = "second"
	ignored := localSource(&sourcev1.GitRepositoryRef{Tag: "v1.0.0"})
	ignored.Name = "ignored"
	ignore := "/extra.txt\n"
	ignored.Ignore = &ignore
	got, err := ResolveLocal(
		t.Context(),
		localLocation(t, fixture.root),
		[]*manifest.GitRepository{first, second, ignored},
		"",
	)
	assert.NoError(t, err)

	for _, source := range []*manifest.GitRepository{first, second, ignored} {
		artifact := got[source.Named()]
		assert.FatalIf(t, len(artifact.Warnings) != 1 || artifact.Warnings[0].Resource != source.Named() ||
			artifact.Warnings[0].Category != manifest.WarnSkippedSubmodule, "pin evidence lost: %+v", artifact)
	}
	key := got[first.Named()].Key
	assert.FatalIf(
		t,
		key != got[second.Named()].Key || key == got[ignored.Named()].Key,
		"cache did not deduplicate commit/options or isolate ignores: %v",
		got,
	)
	{
		_, statErr := fs.Stat(got[ignored.Named()].FS, "extra.txt")
		assert.FatalIf(t, !os.IsNotExist(statErr), "ignore not applied: %v", statErr)
	}
	{
		content, readFileErr := fs.ReadFile(got[first.Named()].FS, "value.txt")
		assert.FatalIf(
			t,
			readFileErr != nil || string(content) != "A",
			"dirty content leaked: %q, %v",
			content,
			readFileErr,
		)
	}
	fixture2 := localFixture(t)

	other, err := ResolveLocal(t.Context(), localLocation(t, fixture2.root), []*manifest.GitRepository{first}, "")
	assert.FatalIf(
		t,
		err != nil || other[first.Named()].Key == key,
		"repository identities share artifacts: %v, %v",
		other,
		err,
	)
	{
		setReferenceErr2 := fixture.repo.Storer.SetReference(
			plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), fixture.head),
		)
		assert.NoError(t, setReferenceErr2)
	}

	moved, err := ResolveLocal(t.Context(), localLocation(t, fixture.root), []*manifest.GitRepository{first}, "")
	assert.FatalIf(
		t,
		err != nil || moved[first.Named()].LocalPath != fixture.root ||
			moved[first.Named()].Revision != fixture.head.String(),
		"moved tag used stale resolution: %v, %v",
		moved,
		err,
	)
}

func TestResolveLocal_AnnotatedSemverWinner(t *testing.T) {
	t.Parallel()
	fixture := localFixture(t)

	{
		_, err := fixture.repo.CreateTag("v1.5.0", fixture.base, &git.CreateTagOptions{
			Tagger: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}, Message: "pin",
		})
		assert.NoError(t, err)
	}

	repository := localSource(&sourcev1.GitRepositoryRef{SemVer: ">=1.0.0 <2.0.0"})
	artifacts, err := ResolveLocal(
		t.Context(),
		localLocation(t, fixture.root),
		[]*manifest.GitRepository{repository},
		"",
	)
	assert.NoError(t, err)

	{
		artifact := artifacts[repository.Named()]
		assert.FatalIf(
			t,
			artifact == nil || artifact.Revision != fixture.base.String(),
			"annotated semver winner = %+v, want revision %s",
			artifact,
			fixture.base,
		)
	}
}

func TestResolveSemver_EqualVersionCreationOrder(t *testing.T) {
	t.Parallel()
	for _, names := range [][]string{{"v1.0.0", "1.0.0"}, {"1.0.0", "v1.0.0"}} {
		t.Run(strings.Join(names, "_"), func(t *testing.T) {
			t.Parallel()
			fixture := localFixture(t)

			hashes := map[string]plumbing.Hash{"1.0.0": fixture.base, "v1.0.0": fixture.head}
			var refs []*plumbing.Reference
			if len(names) > 0 {
				refs = make([]*plumbing.Reference, 0, len(names))
			}
			for _, name := range names {
				ref := plumbing.NewHashReference(plumbing.NewTagReferenceName(name), hashes[name])
				{
					err := fixture.repo.Storer.SetReference(ref)
					assert.NoError(t, err)
				}

				refs = append(refs, ref)
			}
			// Filesystem ref iteration can sort away creation order; force both
			// orders so tie resolution cannot rely on the storage backend.
			fixture.repo.Storer = &orderedTagStore{Storer: fixture.repo.Storer, refs: refs}
			for range 10 {
				got, err := resolveSemver(fixture.repo, ">=1.0.0 <2.0.0")
				assert.FatalIf(
					t,
					err != nil || got != fixture.base,
					"equal-version winner = %s, %v, want %s",
					got,
					err,
					fixture.base,
				)
			}
		})
	}
}

type orderedTagStore struct {
	storage.Storer

	refs []*plumbing.Reference
}

//nolint:ireturn // The upstream go-git protocol interface fixes this return type.
func (s *orderedTagStore) IterReferences() (storer.ReferenceIter, error) {
	return storer.NewReferenceSliceIter(s.refs), nil
}

func TestResolveLocal_ConcurrentReuseAndCancellation(t *testing.T) {
	t.Parallel()
	fixture := localFixture(t)

	repository := localSource(&sourcev1.GitRepositoryRef{Tag: "v1.0.0"})
	var paths [2]string
	var wg sync.WaitGroup
	for i := range paths {
		wg.Go(func() {
			got, err := ResolveLocal(
				t.Context(),
				localLocation(t, fixture.root),
				[]*manifest.GitRepository{repository},
				"",
			)
			if err != nil {
				t.Error(err)
				return
			}
			artifact := got[repository.Named()]
			paths[i] = artifact.Key
			for range 2 {
				content, err := fs.ReadFile(artifact.FS, "value.txt")
				assert.ErrorIf(
					t,
					err != nil || string(content) != "A",
					"concurrent committed read = %q, %v",
					content,
					err,
				)
			}
		})
	}
	wg.Wait()
	assert.FatalIf(t, paths[0] == "" || paths[0] != paths[1], "local artifact identities differ: %v", paths)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	{
		_, err := ResolveLocal(
			ctx,
			localLocation(t, fixture.root),
			[]*manifest.GitRepository{repository},
			"",
		)
		assert.FatalIf(t, !errors.Is(
			err,
			context.Canceled,
		), "cancellation = %v", err)
	}
}

func TestResolveLocal_UnusableObjectsAndStore(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"tag-target", "head", "store", "config", "lookup", "tree", "tree-tag", "recursive-tree-tag"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			testResolveLocalUnusableObjectsAndStoreCase(
				t,
				&kind,
			)
		},
		)
	}
}

//nolint:paralleltest,tparallel // Child options must finish before the parent moves their shared repository metadata.
func TestResolveLocal_OptionsFallbackAndGitIndirection(t *testing.T) {
	t.Parallel()

	fixture := localFixture(t)

	for _, option := range []string{"sparse", "submodules"} {
		t.Run(option, func(t *testing.T) {
			testResolveLocalOptionsFallbackAndGitIndirectionCase(
				t,
				&fixture.root, &option,
			)
		},
		)
	}
	gitdir := filepath.Join(t.TempDir(), "objects")
	{
		err := os.Rename(filepath.Join(fixture.root, ".git"), gitdir)
		assert.NoError(t, err)
	}

	testutil.WriteFile(t, fixture.root, ".git", "gitdir: "+gitdir+"\n")
	got, err := ResolveLocal(
		t.Context(),
		localLocation(t, fixture.root),
		[]*manifest.GitRepository{localSource(&sourcev1.GitRepositoryRef{Tag: "v2.0.0"})},
		"",
	)
	assert.FatalIf(t, err != nil || len(got) != 1, "gitdir indirection = %v, %v", got, err)
}

func TestResolveLocal_MissingBlobFailsOnRead(t *testing.T) {
	t.Parallel()
	fixture := localFixture(t)

	inputTree := &object.Tree{
		Entries: []object.TreeEntry{
			{Name: "missing", Mode: filemode.Regular, Hash: plumbing.NewHash(strings.Repeat("f", 40))},
		},
	}
	encoded := fixture.repo.Storer.NewEncodedObject()
	{
		err := inputTree.Encode(encoded)
		assert.NoError(t, err)
	}

	treeHash, err := fixture.repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)

	commit, err := fixture.repo.CommitObject(fixture.base)
	assert.NoError(t, err)

	commit.TreeHash = treeHash
	encoded = fixture.repo.Storer.NewEncodedObject()
	{
		encodeErr2 := commit.Encode(encoded)
		assert.NoError(t, encodeErr2)
	}

	hash, err := fixture.repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)

	repository := localSource(&sourcev1.GitRepositoryRef{Commit: hash.String()})
	got, err := ResolveLocal(t.Context(), localLocation(t, fixture.root), []*manifest.GitRepository{repository}, "")
	assert.NoError(t, err)

	{
		_, err := fs.ReadFile(got[repository.Named()].FS, "missing")
		assert.FatalIf(t, !errors.Is(err, plumbing.ErrObjectNotFound), "missing blob: %v", err)
	}
}

func TestCheckoutRef_PreservesOriginFirstBranchLookup(t *testing.T) {
	t.Parallel()
	fixture := localFixture(t)

	for name, hash := range map[string]plumbing.Hash{"refs/heads/pinned": fixture.base, "refs/remotes/origin/pinned": fixture.head} {
		{
			err := fixture.repo.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(name), hash))
			assert.NoError(t, err)
		}
	}
	wt, err := fixture.repo.Worktree()
	assert.NoError(t, err)
	{
		checkoutErr := wt.Checkout(&git.CheckoutOptions{Hash: fixture.head, Force: true})
		assert.NoError(t, checkoutErr)
	}

	resolved, err := resolveRefHash(fixture.repo, &sourcev1.GitRepositoryRef{Branch: "pinned"}, true)
	assert.FatalIf(
		t,
		err != nil || resolved != fixture.head,
		"acquired branch resolved %s, %v, want %s",
		resolved,
		err,
		fixture.head,
	)
	{
		checkoutRefErr := checkoutRef(fixture.repo, resolved, nil)
		assert.NoError(t, checkoutRefErr)
	}

	head, err := fixture.repo.Head()
	assert.FatalIf(
		t,
		err != nil || head.Hash() != resolved,
		"checkout must use resolved hash: %v, %v (root %s)",
		head,
		err,
		fixture.root,
	)
	data, err := os.ReadFile(filepath.Join(fixture.root, "value.txt"))
	assert.FatalIf(t, err != nil || string(data) != "B", "checkout content = %q, %v, want B", data, err)

	for _, kind := range []string{"name", "commit-branch"} {
		t.Run("acquisition/"+kind, func(t *testing.T) {
			t.Parallel()
			testCheckoutRefPreservesOriginFirstBranchLookupCase(
				t,
				&kind,
			)
		},
		)
	}
}

func TestResolveSemver_IgnoresUnusableLowerTag(t *testing.T) {
	t.Parallel()
	fixture := localFixture(t)

	commit, err := fixture.repo.CommitObject(fixture.base)
	assert.NoError(t, err)
	{
		setReferenceErr3 := fixture.repo.Storer.SetReference(
			plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), commit.TreeHash),
		)
		assert.NoError(t, setReferenceErr3)
	}

	got, err := resolveSemver(fixture.repo, "*")
	assert.FatalIf(t, err != nil || got != fixture.head, "higher usable tag = %s, %v, want %s", got, err, fixture.head)
}

func TestResolveSemver_DeterministicTiesAndReadErrors(t *testing.T) {
	t.Parallel()
	fixture := localFixture(t)

	for name, hash := range map[string]plumbing.Hash{"1.0.0": fixture.base, "v1.0.0": fixture.head} {
		{
			err := fixture.repo.Storer.SetReference(
				plumbing.NewHashReference(plumbing.NewTagReferenceName(name), hash),
			)
			assert.NoError(t, err)
		}
	}
	for range 10 {
		got, err := resolveSemver(fixture.repo, "<2.0.0")
		assert.FatalIf(t, err != nil || got != fixture.base, "tie = %s, %v", got, err)
	}
	failure := errors.New("storage unavailable")
	fixture.repo.Storer = &failingRefStore{Storer: fixture.repo.Storer, err: failure}
	{
		_, err := resolveSemver(fixture.repo, "*")
		assert.FatalIf(t, !errors.Is(err, failure), "iterator error lost: %v", err)
	}
	{
		_, err := lookupBranch(fixture.repo, "master", false)
		assert.FatalIf(
			t,
			!errors.Is(err, failure) || errors.Is(err, errRefUnavailable),
			"reference read error became miss: %v",
			err,
		)
	}
}

type failingRefStore struct {
	storage.Storer

	err error
}

func (s *failingRefStore) Reference(plumbing.ReferenceName) (*plumbing.Reference, error) {
	return nil, s.err
}

//nolint:ireturn // The upstream go-git protocol interface fixes this return type.
func (s *failingRefStore) IterReferences() (storer.ReferenceIter, error) { return nil, s.err }

func localSource(refValue *sourcev1.GitRepositoryRef) *manifest.GitRepository {
	ref := *refValue
	repo := &manifest.GitRepository{Name: "cluster", Namespace: "flux-system"}
	repo.URL = "git://fixture.invalid/cluster"
	repo.Reference = &ref
	return repo
}
