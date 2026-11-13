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
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveLocal_References(t *testing.T) {
	t.Parallel()
	localFixtureResult := localFixture(t)
	root, repo, a, b := localFixtureResult.root, localFixtureResult.repo, localFixtureResult.base, localFixtureResult.head
	for name, hash := range map[string]plumbing.Hash{
		"refs/heads/pinned": a, "refs/remotes/origin/pinned": b,
		"refs/remotes/origin/tracking": a, "refs/pull/1/head": a,
	} {
		{
			err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(name), hash))
			assert.NoError(t, err)
		}
	}
	{
		_, err := repo.CreateTag("annotated", a, &git.CreateTagOptions{
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
		{"tag", sourcev1.GitRepositoryRef{Tag: "v1.0.0"}, a, false},
		{"annotated", sourcev1.GitRepositoryRef{Tag: "annotated"}, a, false},
		{"commit", sourcev1.GitRepositoryRef{Commit: a.String()}, a, false},
		{"name", sourcev1.GitRepositoryRef{Name: "refs/pull/1/head"}, a, false},
		{"branch-local-first", sourcev1.GitRepositoryRef{Branch: "pinned"}, a, false},
		{"branch-origin", sourcev1.GitRepositoryRef{Branch: "tracking"}, a, false},
		{"name-origin", sourcev1.GitRepositoryRef{Name: "refs/heads/tracking"}, a, false},
		{"semver", sourcev1.GitRepositoryRef{SemVer: "<2.0.0"}, a, false},
		{"commit-over-name", sourcev1.GitRepositoryRef{Commit: a.String(), Name: "bad name", SemVer: "invalid", Tag: "v2.0.0"}, a, false},
		{"name-over-semver", sourcev1.GitRepositoryRef{Name: "refs/pull/1/head", SemVer: "invalid", Tag: "v2.0.0", Branch: "bad branch"}, a, false},
		{"semver-over-tag", sourcev1.GitRepositoryRef{SemVer: "<2.0.0", Tag: "bad tag", Branch: "bad branch"}, a, false},
		{"tag-over-branch", sourcev1.GitRepositoryRef{Tag: "v1.0.0", Branch: "bad branch"}, a, false},
		{"head", sourcev1.GitRepositoryRef{Tag: "v2.0.0"}, b, false},
		{"commit-branch-reachable", sourcev1.GitRepositoryRef{Commit: a.String(), Branch: "master"}, a, false},
		{"commit-branch-unreachable", sourcev1.GitRepositoryRef{Commit: b.String(), Branch: "pinned"}, plumbing.ZeroHash, true},
		{"commit-branch-missing", sourcev1.GitRepositoryRef{Commit: a.String(), Branch: "missing"}, plumbing.ZeroHash, false},
		{"missing-tag", sourcev1.GitRepositoryRef{Tag: "missing"}, plumbing.ZeroHash, false},
		{"missing-name", sourcev1.GitRepositoryRef{Name: "refs/heads/missing"}, plumbing.ZeroHash, false},
		{"missing-branch", sourcev1.GitRepositoryRef{Branch: "missing"}, plumbing.ZeroHash, false},
		{"unmatched-semver", sourcev1.GitRepositoryRef{SemVer: ">3.0.0"}, plumbing.ZeroHash, false},
		{"invalid-semver", sourcev1.GitRepositoryRef{SemVer: "invalid"}, plumbing.ZeroHash, true},
		{"short-commit", sourcev1.GitRepositoryRef{Commit: a.String()[:12]}, plumbing.ZeroHash, true},
		{"nonhex-commit", sourcev1.GitRepositoryRef{Commit: strings.Repeat("g", 40)}, plumbing.ZeroHash, true},
		{"missing-commit", sourcev1.GitRepositoryRef{Commit: strings.Repeat("f", 40)}, plumbing.ZeroHash, false},
		{"missing-commit-branch", sourcev1.GitRepositoryRef{Commit: strings.Repeat("f", 40), Branch: "master"}, plumbing.ZeroHash, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testResolveLocalReferencesCase(t, &root, &b, &tc)
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
	localFixtureResult2 := localFixture(t)
	repo, hash := localFixtureResult2.repo, localFixtureResult2.base
	missing := plumbing.NewHash(strings.Repeat("f", 40))
	commit, err := repo.CommitObject(hash)
	assert.NoError(t, err)
	tree := commit.TreeHash
	commit.TreeHash = missing
	encoded := repo.Storer.NewEncodedObject()
	assert.NoError(t, commit.Encode(encoded))
	broken, err := repo.Storer.SetEncodedObject(encoded)
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
			_, err := resolveRefHash(repo, &sourcev1.GitRepositoryRef{Commit: tc.hash.String()}, false)
			if err == nil || errors.Is(err, errRefUnavailable) != tc.unavailable {
				t.Fatalf("resolution error = %v, want unavailable=%t", err, tc.unavailable)
			}
		})
	}
}

func TestResolveLocal_IncompleteHistory(t *testing.T) {
	t.Parallel()
	localFixtureResult3 := localFixture(t)
	root, repo, a, b := localFixtureResult3.root, localFixtureResult3.repo, localFixtureResult3.base, localFixtureResult3.head
	commit, err := repo.CommitObject(b)
	assert.NoError(t, err)

	commit.ParentHashes = []plumbing.Hash{plumbing.NewHash(strings.Repeat("f", 40))}
	encoded := repo.Storer.NewEncodedObject()
	{
		encodeErr := commit.Encode(encoded)
		assert.NoError(t, encodeErr)
	}

	hash, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)
	{
		setReferenceErr := repo.Storer.SetReference(
			plumbing.NewHashReference(plumbing.NewBranchReferenceName("master"), hash),
		)
		assert.NoError(t, setReferenceErr)
	}

	{
		_, resolveRefHashErr := resolveRefHash(
			repo,
			&sourcev1.GitRepositoryRef{Commit: a.String(), Branch: "master"},
			false,
		)
		assert.ErrorIf(t, !errors.Is(
			resolveRefHashErr,
			errRefUnavailable,
		), "incomplete history did not become unavailable: %v", resolveRefHashErr)
	}
	repository := localSource(&sourcev1.GitRepositoryRef{Commit: a.String(), Branch: "master"})
	got, err := ResolveLocal(
		t.Context(),
		localLocation(t, root),
		[]*manifest.GitRepository{repository},
		LocalOptions{Revision: ""},
	)
	assert.FatalIf(t, err != nil || len(got) != 0, "incomplete history did not fall back: %v, %v", got, err)
}

//nolint:paralleltest // Allocation measurements use process-wide test accounting.
//nolint:paralleltest // Allocation measurements use process-wide test accounting.
func TestResolveLocal_NoWarningAllocations(t *testing.T) {
	empty := []*manifest.GitRepository{localSource(&sourcev1.GitRepositoryRef{})}
	authored := []*manifest.GitRepository{localSource(&sourcev1.GitRepositoryRef{Tag: "v1.0.0"})}
	selected := (*manifest.GitRepositoryRef)(authored[0].Reference)
	emptyAllocs := testing.AllocsPerRun(1000, func() {
		_, err := ResolveLocal(t.Context(), nil, empty, LocalOptions{Revision: ""})
		assert.NoError(t, err)
	})
	validationAllocs := testing.AllocsPerRun(1000, func() {
		assert.NoError(t, selected.Validate())
	})
	authoredAllocs := testing.AllocsPerRun(1000, func() {
		_, err := ResolveLocal(t.Context(), nil, authored, LocalOptions{Revision: ""})
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
					LocalOptions{Revision: ""},
				)
				assert.FatalIf(t, !errors.Is(
					err,
					manifest.ErrInput,
				), "expected invalid spec, got %v", err)
			}
		})
	}
	localFixtureResult4 := localFixture(t)
	parent := localFixtureResult4.root
	child := filepath.Join(parent, "exported")
	assert.Equal(t, os.Mkdir(child, 0o750), nil)
	repository := localSource(&sourcev1.GitRepositoryRef{Tag: "v1.0.0"})
	got, err := ResolveLocal(
		t.Context(),
		localLocation(t, child),
		[]*manifest.GitRepository{repository},
		LocalOptions{Revision: ""},
	)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(got), 0)
	location := localLocation(t, child)
	testutil.WriteFile(t, parent, ".git/config", "[invalid\n")
	got, err = ResolveLocal(
		t.Context(),
		location,
		[]*manifest.GitRepository{repository},
		LocalOptions{Revision: ""},
	)
	assert.Equal(t, err, nil)
	assert.Equal(t, len(got), 0)
	assert.Equal(t, os.RemoveAll(filepath.Join(parent, ".git")), nil)
	assert.Equal(t, os.Symlink(".git", filepath.Join(parent, ".git")), nil)
	got, err = ResolveLocal(
		t.Context(),
		localLocation(t, child),
		[]*manifest.GitRepository{repository},
		LocalOptions{Revision: ""},
	)
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
			LocalOptions{Revision: ""},
		)
		assert.FatalIf(t, !errors.Is(
			err,
			manifest.ErrInput,
		), "nil source = %v", err)
	}
}

func TestResolveLocal_CacheReuseIsolationAndMovedTags(t *testing.T) {
	t.Parallel()
	localFixtureResult5 := localFixture(t)
	root, repo, a, b := localFixtureResult5.root, localFixtureResult5.repo, localFixtureResult5.base, localFixtureResult5.head

	a = submoduleCommit(t, repo, a)
	assert.NoError(t, repo.Storer.SetReference(plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), a)))

	first := localSource(&sourcev1.GitRepositoryRef{Tag: "v1.0.0"})
	second := localSource(&sourcev1.GitRepositoryRef{Commit: a.String()})
	second.Name = "second"
	ignored := localSource(&sourcev1.GitRepositoryRef{Tag: "v1.0.0"})
	ignored.Name = "ignored"
	ignore := "/extra.txt\n"
	ignored.Ignore = &ignore
	var work manifest.WorkRecorder
	got, err := ResolveLocal(
		t.Context(),
		localLocation(t, root),
		[]*manifest.GitRepository{first, second, ignored},
		LocalOptions{Work: work.Scope()},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []*manifest.GitRepository{first, second, ignored} {
		artifact := got[source.Named()]
		if len(artifact.Warnings) != 1 || artifact.Warnings[0].Resource != source.Named() ||
			artifact.Warnings[0].Category != manifest.WarnSkippedSubmodule {
			t.Fatalf("pin evidence lost: %+v", artifact)
		}
	}
	summary := work.Scope().Snapshot(0)
	assert.Equal(t, len(summary), 3)
	for recordIndex := range summary {
		record := &summary[recordIndex]
		assert.Equal(t, record.Counts, manifest.WorkCounts{Attempts: 1, Completed: 1, LocalAcquisitions: 1})
	}
	key := got[first.Named()].Key
	if key != got[second.Named()].Key || key == got[ignored.Named()].Key {
		t.Fatalf("cache did not deduplicate commit/options or isolate ignores: %v", got)
	}
	if _, statErr := fs.Stat(got[ignored.Named()].FS, "extra.txt"); !os.IsNotExist(statErr) {
		t.Fatalf("ignore not applied: %v", statErr)
	}
	if content, readErr := fs.ReadFile(got[first.Named()].FS, "value.txt"); readErr != nil || string(content) != "A" {
		t.Fatalf("dirty content leaked: %q, %v", content, readErr)
	}
	localFixtureResult6 := localFixture(t)
	otherRoot := localFixtureResult6.root
	other, err := ResolveLocal(
		t.Context(),
		localLocation(t, otherRoot),
		[]*manifest.GitRepository{first},
		LocalOptions{Revision: ""},
	)
	if err != nil || other[first.Named()].Key == key {
		t.Fatalf("repository identities share artifacts: %v, %v", other, err)
	}
	if referenceErr := repo.Storer.SetReference(
		plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), b),
	); referenceErr != nil {
		t.Fatal(referenceErr)
	}
	moved, err := ResolveLocal(
		t.Context(),
		localLocation(t, root),
		[]*manifest.GitRepository{first},
		LocalOptions{Revision: ""},
	)
	if err != nil || moved[first.Named()].LocalPath != root || moved[first.Named()].Revision != b.String() {
		t.Fatalf("moved tag used stale resolution: %v, %v", moved, err)
	}
}

func TestResolveLocal_AnnotatedSemverWinner(t *testing.T) {
	t.Parallel()
	localFixtureResult7 := localFixture(t)
	root, repo, revision := localFixtureResult7.root, localFixtureResult7.repo, localFixtureResult7.base
	{
		_, err := repo.CreateTag("v1.5.0", revision, &git.CreateTagOptions{
			Tagger: &object.Signature{Name: "t", Email: "t@e", When: time.Unix(0, 0)}, Message: "pin",
		})
		assert.NoError(t, err)
	}

	repository := localSource(&sourcev1.GitRepositoryRef{SemVer: ">=1.0.0 <2.0.0"})
	artifacts, err := ResolveLocal(
		t.Context(),
		localLocation(t, root),
		[]*manifest.GitRepository{repository},
		LocalOptions{Revision: ""},
	)
	assert.NoError(t, err)

	{
		artifact := artifacts[repository.Named()]
		assert.FatalIf(
			t,
			artifact == nil || artifact.Revision != revision.String(),
			"annotated semver winner = %+v, want revision %s",
			artifact,
			revision,
		)
	}
}

func TestResolveSemver_EqualVersionCreationOrder(t *testing.T) {
	t.Parallel()
	for _, names := range [][]string{{"v1.0.0", "1.0.0"}, {"1.0.0", "v1.0.0"}} {
		t.Run(strings.Join(names, "_"), func(t *testing.T) {
			t.Parallel()
			localFixtureResult8 := localFixture(t)
			repo, a, b := localFixtureResult8.repo, localFixtureResult8.base, localFixtureResult8.head
			hashes := map[string]plumbing.Hash{"1.0.0": a, "v1.0.0": b}
			var refs []*plumbing.Reference
			if len(names) > 0 {
				refs = make([]*plumbing.Reference, 0, len(names))
			}
			for _, name := range names {
				ref := plumbing.NewHashReference(plumbing.NewTagReferenceName(name), hashes[name])
				{
					err := repo.Storer.SetReference(ref)
					assert.NoError(t, err)
				}

				refs = append(refs, ref)
			}
			// Filesystem ref iteration can sort away creation order; force both
			// orders so tie resolution cannot rely on the storage backend.
			repo.Storer = &orderedTagStore{Storer: repo.Storer, refs: refs}
			for range 10 {
				got, err := resolveSemver(repo, ">=1.0.0 <2.0.0")
				assert.FatalIf(t, err != nil || got != a, "equal-version winner = %s, %v, want %s", got, err, a)
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
	localFixtureResult9 := localFixture(t)
	root := localFixtureResult9.root

	repository := localSource(&sourcev1.GitRepositoryRef{Tag: "v1.0.0"})
	var paths [2]string
	var wg sync.WaitGroup
	for i := range paths {
		wg.Go(func() {
			got, err := ResolveLocal(
				t.Context(),
				localLocation(t, root),
				[]*manifest.GitRepository{repository},
				LocalOptions{Revision: ""},
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
			localLocation(t, root),
			[]*manifest.GitRepository{repository},
			LocalOptions{Revision: ""},
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

	localFixtureResult10 := localFixture(t)
	root := localFixtureResult10.root
	for _, option := range []string{"sparse", "submodules"} {
		t.Run(option, func(t *testing.T) {
			testResolveLocalOptionsFallbackAndGitIndirectionCase(
				t,
				&root, &option,
			)
		},
		)
	}
	gitdir := filepath.Join(t.TempDir(), "objects")
	{
		err := os.Rename(filepath.Join(root, ".git"), gitdir)
		assert.NoError(t, err)
	}

	testutil.WriteFile(t, root, ".git", "gitdir: "+gitdir+"\n")
	got, err := ResolveLocal(
		t.Context(),
		localLocation(t, root),
		[]*manifest.GitRepository{localSource(&sourcev1.GitRepositoryRef{Tag: "v2.0.0"})},
		LocalOptions{Revision: ""},
	)
	assert.FatalIf(t, err != nil || len(got) != 1, "gitdir indirection = %v, %v", got, err)
}

func TestResolveLocal_MissingBlobFailsOnRead(t *testing.T) {
	t.Parallel()
	localFixtureResult11 := localFixture(t)
	root, repo, a := localFixtureResult11.root, localFixtureResult11.repo, localFixtureResult11.base
	inputTree := &object.Tree{
		Entries: []object.TreeEntry{
			{Name: "missing", Mode: filemode.Regular, Hash: plumbing.NewHash(strings.Repeat("f", 40))},
		},
	}
	encoded := repo.Storer.NewEncodedObject()
	{
		err := inputTree.Encode(encoded)
		assert.NoError(t, err)
	}

	treeHash, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)

	commit, err := repo.CommitObject(a)
	assert.NoError(t, err)

	commit.TreeHash = treeHash
	encoded = repo.Storer.NewEncodedObject()
	{
		encodeErr2 := commit.Encode(encoded)
		assert.NoError(t, encodeErr2)
	}

	hash, err := repo.Storer.SetEncodedObject(encoded)
	assert.NoError(t, err)

	repository := localSource(&sourcev1.GitRepositoryRef{Commit: hash.String()})
	got, err := ResolveLocal(
		t.Context(),
		localLocation(t, root),
		[]*manifest.GitRepository{repository},
		LocalOptions{Revision: ""},
	)
	assert.NoError(t, err)

	{
		_, err := fs.ReadFile(got[repository.Named()].FS, "missing")
		assert.FatalIf(t, !errors.Is(err, plumbing.ErrObjectNotFound), "missing blob: %v", err)
	}
}

func TestCheckoutRef_PreservesOriginFirstBranchLookup(t *testing.T) {
	t.Parallel()
	localFixtureResult12 := localFixture(t)
	root, repo, a, b := localFixtureResult12.root, localFixtureResult12.repo, localFixtureResult12.base, localFixtureResult12.head
	for name, hash := range map[string]plumbing.Hash{"refs/heads/pinned": a, "refs/remotes/origin/pinned": b} {
		{
			err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.ReferenceName(name), hash))
			assert.NoError(t, err)
		}
	}
	wt, err := repo.Worktree()
	assert.NoError(t, err)
	{
		checkoutErr := wt.Checkout(&git.CheckoutOptions{Hash: b, Force: true})
		assert.NoError(t, checkoutErr)
	}

	resolved, err := resolveRefHash(repo, &sourcev1.GitRepositoryRef{Branch: "pinned"}, true)
	assert.FatalIf(t, err != nil || resolved != b, "acquired branch resolved %s, %v, want %s", resolved, err, b)
	{
		checkoutRefErr := checkoutRef(repo, resolved, nil)
		assert.NoError(t, checkoutRefErr)
	}

	head, err := repo.Head()
	assert.FatalIf(
		t,
		err != nil || head.Hash() != resolved,
		"checkout must use resolved hash: %v, %v (root %s)",
		head,
		err,
		root,
	)
	data, err := os.ReadFile(filepath.Join(root, "value.txt"))
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
	localFixtureResult13 := localFixture(t)
	repo, a, b := localFixtureResult13.repo, localFixtureResult13.base, localFixtureResult13.head
	commit, err := repo.CommitObject(a)
	assert.NoError(t, err)
	{
		setReferenceErr3 := repo.Storer.SetReference(
			plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), commit.TreeHash),
		)
		assert.NoError(t, setReferenceErr3)
	}

	got, err := resolveSemver(repo, "*")
	assert.FatalIf(t, err != nil || got != b, "higher usable tag = %s, %v, want %s", got, err, b)
}

func TestResolveSemver_DeterministicTiesAndReadErrors(t *testing.T) {
	t.Parallel()
	localFixtureResult14 := localFixture(t)
	repo, a, b := localFixtureResult14.repo, localFixtureResult14.base, localFixtureResult14.head
	for name, hash := range map[string]plumbing.Hash{"1.0.0": a, "v1.0.0": b} {
		{
			err := repo.Storer.SetReference(
				plumbing.NewHashReference(plumbing.NewTagReferenceName(name), hash),
			)
			assert.NoError(t, err)
		}
	}
	for range 10 {
		got, err := resolveSemver(repo, "<2.0.0")
		assert.FatalIf(t, err != nil || got != a, "tie = %s, %v", got, err)
	}
	failure := errors.New("storage unavailable")
	repo.Storer = &failingRefStore{Storer: repo.Storer, err: failure}
	{
		_, err := resolveSemver(repo, "*")
		assert.FatalIf(t, !errors.Is(err, failure), "iterator error lost: %v", err)
	}
	{
		_, err := lookupBranch(repo, "master", false)
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
