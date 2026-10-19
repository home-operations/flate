package baseline

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/memory"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/cachetest"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
)

// An explicit base bypasses the auto-detection ladder.
func TestAutoResolve_ExplicitBase(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	commitA := initRepoWithFile(t, dir, "a.yaml", "original")
	writeAndCommit(t, dir, "a.yaml", "updated")

	res, err := AutoResolve(t.Context(), dir, commitA.String(), cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}

	if !strings.HasPrefix(res.Source.Metadata["baseline-selection"], "explicit --base=") {
		t.Errorf("Source = %q, want prefix 'explicit --base='", res.Source.Metadata["baseline-selection"])
	}
	assert.Equal(t, res.Source.LocalRoot, dir)
	assert.Equal(t, res.Source.URL, "")
	got, err := fs.ReadFile(res.BaseInput.FS, "a.yaml")
	if err != nil {
		t.Fatalf("read materialized: %v", err)
	}
	if string(got) != "original" {
		t.Errorf("materialized a.yaml = %q, want %q (should be commitA's tree, not HEAD's)", got, "original")
	}
}

// TestAutoResolve_SelfURLsCarryRemotes pins that Result.SelfURLs carries the
// working tree's remote URLs — the property the self-referential
// GitRepository alias relies on on the baseline side. The materialized tree
// has no .git of its own, so the caller hands these to the baseline render
// as Config.SelfURLs; without them the baseline's private self-source goes
// unresolved and the whole tree renders empty (every resource a wholesale
// addition).
func TestAutoResolve_SelfURLsCarryRemotes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	commit := initRepoWithFile(t, dir, "a.yaml", "x")
	repo, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatal(err)
	}
	const url = "ssh://git@github.com/example/cluster.git"
	if _, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{url}}); err != nil {
		t.Fatal(err)
	}

	res, err := AutoResolve(t.Context(), dir, commit.String(), cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}

	if !slices.Contains(res.SelfURLs, url) {
		t.Errorf("Result.SelfURLs = %v; want it to contain %q", res.SelfURLs, url)
	}
	// The materialized tree must NOT carry a synthetic .git anymore.
	if _, err := fs.Stat(res.BaseInput.FS, ".git"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("materialized baseline must have no .git marker; stat err = %v", err)
	}
}

func TestResolvePaths_AncestorLookupError(t *testing.T) {
	t.Parallel()
	for _, side := range []string{"original", "current"} {
		t.Run(side, func(t *testing.T) {
			t.Parallel()
			parent := t.TempDir()
			assert.Equal(t, os.Symlink(".git", filepath.Join(parent, ".git")), nil)
			child := filepath.Join(parent, "exported")
			assert.Equal(t, os.Mkdir(child, 0o750), nil)
			current, original := t.TempDir(), child
			if side == "current" {
				current, original = original, current
			}

			result, err := ResolvePaths(t.Context(), current, original)
			assert.Equal(t, err, nil)
			assert.Equal(t, result.Source.LocalPath, current)
			assert.Equal(t, result.BaseInput.LocalPath, original)
			assert.Equal(t, len(result.SelfURLs), 0)
		})
	}
}

// TestAutoResolve_UpstreamMergeBase exercises the @{u} rung: HEAD is on
// a branch whose config points at a remote-tracking ref, and the
// merge-base resolves correctly.
func TestAutoResolve_UpstreamMergeBase(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mainCommit := initRepoWithFile(t, dir, "a.yaml", "main")
	// Set up a remote-tracking ref that points at mainCommit (simulates
	// origin/main without an actual remote). Use the actual init branch
	// name (go-git's default may be master, not main).
	repo := openHelper(t, dir)
	head, err := repo.Head()
	if err != nil {
		t.Fatal(err)
	}
	branchName := head.Name().Short()
	setRef(t, repo, plumbing.NewRemoteReferenceName("origin", branchName), mainCommit)
	configureBranch(t, repo, &config.Branch{
		Name: branchName, Remote: "origin", Merge: head.Name(),
	})

	// Move forward on the local branch.
	writeAndCommit(t, dir, "a.yaml", "diverged")

	res, err := AutoResolve(t.Context(), dir, "", cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}

	if res.Source.Metadata["baseline-selection"] != "merge-base with @{u}" {
		t.Errorf("Source = %q, want 'merge-base with @{u}'", res.Source.Metadata["baseline-selection"])
	}
	got, err := fs.ReadFile(res.BaseInput.FS, "a.yaml")
	if err != nil {
		t.Fatalf("read materialized: %v", err)
	}
	if string(got) != "main" {
		t.Errorf("materialized = %q, want %q (should be merge-base contents)", got, "main")
	}
}

// TestAutoResolve_OriginHEAD: no @{u}, but origin/HEAD symref present.
// Verifies the second rung fires.
func TestAutoResolve_OriginHEAD(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mainCommit := initRepoWithFile(t, dir, "a.yaml", "base")
	repo := openHelper(t, dir)
	// Simulate the remote-tracking branch and origin/HEAD symref
	// pointing at it. No branch config — so @{u} is unset.
	setRef(t, repo, plumbing.NewRemoteReferenceName("origin", "main"), mainCommit)
	setSymRef(t, repo, plumbing.NewRemoteHEADReferenceName("origin"),
		plumbing.NewRemoteReferenceName("origin", "main"))

	writeAndCommit(t, dir, "a.yaml", "new")

	res, err := AutoResolve(t.Context(), dir, "", cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}

	if res.Source.Metadata["baseline-selection"] != "merge-base with origin/HEAD" {
		t.Errorf("Source = %q, want 'merge-base with origin/HEAD'", res.Source.Metadata["baseline-selection"])
	}
}

// TestAutoResolve_ExplicitBaseFallsBackToOrigin locks the CI ergonomic
// the workflow-side broke on: `--base main` on a checkout that has
// only `origin/main` (no local main ref) must resolve via the
// remote-tracking branch. actions/checkout with default
// fetch-depth=1 lands the PR branch and origin/* but no sibling
// local refs; we shouldn't force users to type `--base origin/main`.
func TestAutoResolve_ExplicitBaseFallsBackToOrigin(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mainCommit := initRepoWithFile(t, dir, "a.yaml", "base")
	repo := openHelper(t, dir)
	// Simulate the CI checkout: only the remote-tracking ref exists.
	setRef(t, repo, plumbing.NewRemoteReferenceName("origin", "main"), mainCommit)
	writeAndCommit(t, dir, "a.yaml", "pr-tip")

	res, err := AutoResolve(t.Context(), dir, "main", cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("AutoResolve(--base=main) should fall back to origin/main: %v", err)
	}

	if got := res.BaseInput.Revision; got != mainCommit.String() {
		t.Errorf("Revision = %q, want %q", got, mainCommit.String())
	}
	if !strings.Contains(res.Source.Metadata["baseline-selection"], "via origin/main") {
		t.Errorf("Source = %q, want to mention 'via origin/main'", res.Source.Metadata["baseline-selection"])
	}
}

func TestAutoResolve_ExplicitBaseRemoteTrackingBranch(t *testing.T) {
	t.Parallel()
	for _, base := range []string{"release/v1", "origin/release/v1"} {
		t.Run(base, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			commit := initRepoWithFile(t, dir, "a.yaml", "base")
			repo := openHelper(t, dir)
			setRef(t, repo, plumbing.NewRemoteReferenceName("origin", "release/v1"), commit)

			res, err := AutoResolve(t.Context(), dir, base, cachetest.New(t, "", sourcecache.New))
			assert.NoError(t, err)
			assert.Equal(t, res.BaseInput.Revision, commit.String())
			assert.Equal(t, res.BaseInput.Revision, commit.String())
			got, err := fs.ReadFile(res.BaseInput.FS, "a.yaml")
			assert.NoError(t, err)
			assert.Equal(t, string(got), "base")
		})
	}
}

func TestAutoResolve_ExplicitBaseLocalBranchPrecedesOrigin(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	older := initRepoWithFile(t, dir, "a.yaml", "older")
	newer := writeAndCommit(t, dir, "a.yaml", "newer")
	repo := openHelper(t, dir)
	setRef(t, repo, plumbing.NewBranchReferenceName("release/v1"), older)
	setRef(t, repo, plumbing.NewRemoteReferenceName("origin", "release/v1"), newer)

	res, err := AutoResolve(t.Context(), dir, "release/v1", cachetest.New(t, "", sourcecache.New))
	assert.NoError(t, err)
	assert.Equal(t, res.BaseInput.Revision, older.String())
	got, err := fs.ReadFile(res.BaseInput.FS, "a.yaml")
	assert.NoError(t, err)
	assert.Equal(t, string(got), "older")
}

// TestAutoResolve_ExplicitBaseNeitherLocalNorRemote: when --base
// resolves neither locally nor as origin/<base>, the error names
// both lookup attempts so the user knows what to fix.
func TestAutoResolve_ExplicitBaseNeitherLocalNorRemote(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	initRepoWithFile(t, dir, "a.yaml", "base")
	writeAndCommit(t, dir, "a.yaml", "pr-tip")

	_, err := AutoResolve(t.Context(), dir, "nonexistent", cachetest.New(t, "", sourcecache.New))
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	if err == nil {
		t.Fatal("expected error for unresolvable --base")
	}
	if !strings.Contains(err.Error(), "origin/nonexistent") {
		t.Errorf("error should mention the remote fallback was tried: %v", err)
	}
}

// TestAutoResolve_OriginMainFallback exercises the third rung: no
// @{u}, no origin/HEAD, but origin/main exists.
func TestAutoResolve_OriginMainFallback(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mainCommit := initRepoWithFile(t, dir, "a.yaml", "base")
	repo := openHelper(t, dir)
	setRef(t, repo, plumbing.NewRemoteReferenceName("origin", "main"), mainCommit)
	// Deliberately no origin/HEAD symref and no branch config.

	writeAndCommit(t, dir, "a.yaml", "new")

	res, err := AutoResolve(t.Context(), dir, "", cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}

	if res.Source.Metadata["baseline-selection"] != "merge-base with origin/main" {
		t.Errorf("Source = %q, want 'merge-base with origin/main'", res.Source.Metadata["baseline-selection"])
	}
}

func TestAutoResolve_Ladder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, main, master, remoteHEAD, upstream, want string
	}{
		{name: "master only", master: "base", want: "master"},
		{name: "main and master, different tips", main: "tip", master: "base", want: "main"},
		{name: "origin/HEAD disjoint, origin/main valid", remoteHEAD: "disjoint", main: "tip", want: "main"},
		{name: "@{u} disjoint, origin/HEAD valid", upstream: "disjoint", remoteHEAD: "tip", want: "HEAD"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			base := initRepoWithFile(t, dir, "a.yaml", "base")
			tip := writeAndCommit(t, dir, "a.yaml", "tip")
			repo := openHelper(t, dir)
			disjoint, err := repo.CommitObject(base)
			if err != nil {
				t.Fatal(err)
			}
			disjoint.Message = "disjoint root"
			encoded := repo.Storer.NewEncodedObject()
			if err := disjoint.Encode(encoded); err != nil {
				t.Fatal(err)
			}
			disjointHash, err := repo.Storer.SetEncodedObject(encoded)
			if err != nil {
				t.Fatal(err)
			}
			hashes := map[string]plumbing.Hash{"base": base, "tip": tip, "disjoint": disjointHash}
			for _, ref := range []struct{ name, commit string }{
				{"main", tc.main}, {"master", tc.master}, {"HEAD", tc.remoteHEAD}, {"upstream", tc.upstream},
			} {
				if ref.commit != "" {
					setRef(t, repo, plumbing.NewRemoteReferenceName("origin", ref.name), hashes[ref.commit])
				}
			}
			if tc.upstream != "" {
				head, err := repo.Head()
				if err != nil {
					t.Fatal(err)
				}
				configureBranch(t, repo, &config.Branch{
					Name: head.Name().Short(), Remote: "origin", Merge: plumbing.NewBranchReferenceName("upstream"),
				})
			}

			res, err := AutoResolve(t.Context(), dir, "", cachetest.New(t, "", sourcecache.New))
			if err != nil {
				t.Fatalf("AutoResolve: %v", err)
			}

			if want := "merge-base with origin/" + tc.want; res.Source.Metadata["baseline-selection"] != want {
				t.Errorf("Source = %q, want %q", res.Source.Metadata["baseline-selection"], want)
			}
		})
	}
}

// TestAutoResolve_DetachedHEAD: HEAD is at a SHA, not a branch. The
// @{u} rung is unreachable (no branch → no config); the algorithm
// must fall through to origin/HEAD. This is the GH Actions PR case.
func TestAutoResolve_DetachedHEAD(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	mainCommit := initRepoWithFile(t, dir, "a.yaml", "base")
	writeAndCommit(t, dir, "a.yaml", "pr-tip")
	repo := openHelper(t, dir)
	setRef(t, repo, plumbing.NewRemoteReferenceName("origin", "main"), mainCommit)
	setSymRef(t, repo, plumbing.NewRemoteHEADReferenceName("origin"),
		plumbing.NewRemoteReferenceName("origin", "main"))
	// Detach HEAD: write a non-symbolic HEAD pointing at the current commit.
	head, err := repo.Head()
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if err := repo.Storer.SetReference(plumbing.NewHashReference(plumbing.HEAD, head.Hash())); err != nil {
		t.Fatalf("detach HEAD: %v", err)
	}

	res, err := AutoResolve(t.Context(), dir, "", cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}

	if res.Source.Metadata["baseline-selection"] != "merge-base with origin/HEAD" {
		t.Errorf("Source = %q, want 'merge-base with origin/HEAD' (detached HEAD should skip @{u})", res.Source.Metadata["baseline-selection"])
	}
}

func TestAutoResolve_NoGit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	testutil.WriteFileAt(t, filepath.Join(dir, "f.yaml"), "x")
	_, err := AutoResolve(t.Context(), dir, "", cachetest.New(t, "", sourcecache.New))
	if !errors.Is(err, manifest.ErrInput) || !strings.Contains(err.Error(), "not inside a git working tree") {
		t.Errorf("error %q: should explain why and suggest --path-orig / --base", err)
	}
	assert.Equal(t, os.Symlink(".git", filepath.Join(dir, ".git")), nil)
	res, err := AutoResolve(t.Context(), dir, "", cachetest.New(t, "", sourcecache.New))
	assert.Equal(t, errors.Is(err, manifest.ErrInput) && errors.Is(err, manifest.ErrFlux), true)
	assert.Equal(t, res == nil, true)
}

func TestAutoResolve_Shallow(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	initRepoWithFile(t, dir, "a.yaml", "x")
	if err := os.WriteFile(filepath.Join(dir, ".git", "shallow"), []byte("deadbeef\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// No upstream, no origin refs → the resolution ladder falls
	// through; the shallow detection fires last.
	_, err := AutoResolve(t.Context(), dir, "", cachetest.New(t, "", sourcecache.New))
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	if err == nil {
		t.Fatal("expected shallow error")
	}
	if !strings.Contains(err.Error(), "shallow") {
		t.Errorf("error %q should mention shallow", err)
	}
	if !strings.Contains(err.Error(), "fetch-depth: 0") {
		t.Errorf("error %q should suggest fetch-depth: 0 fix", err)
	}
}

// TestAutoResolve_NoUpstream: real (non-shallow) repo with no upstream
// and no origin refs at all. Errors cleanly suggesting --base.
func TestAutoResolve_NoUpstream(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	initRepoWithFile(t, dir, "a.yaml", "x")
	_, err := AutoResolve(t.Context(), dir, "", cachetest.New(t, "", sourcecache.New))
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	if err == nil {
		t.Fatal("expected no-upstream error")
	}
	if strings.Contains(err.Error(), "shallow") {
		t.Errorf("error %q should not mention shallow on non-shallow repo", err)
	}
	if !strings.Contains(err.Error(), "--base") {
		t.Errorf("error %q should suggest --base flag", err)
	}
}

// TestAutoResolve_PathOutsideRepo: --path resolves outside the .git
// ancestor → refuse early with a clear suggestion.
func TestAutoResolve_PathOutsideRepo(t *testing.T) {
	t.Parallel()
	outerDir := t.TempDir()
	repoDir := filepath.Join(outerDir, "repo")
	if err := os.Mkdir(repoDir, 0o750); err != nil {
		t.Fatal(err)
	}
	initRepoWithFile(t, repoDir, "a.yaml", "x")
	// A sibling directory has no containing checkout.
	sibling := filepath.Join(outerDir, "sibling")
	if err := os.Mkdir(sibling, 0o750); err != nil {
		t.Fatal(err)
	}
	_, err := AutoResolve(t.Context(), sibling, "", cachetest.New(t, "", sourcecache.New))
	if err == nil {
		t.Fatal("expected error for path outside repo")
	}
}

func TestAutoResolve_PathOrigMappedToSubdir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	commit := initRepoWithFile(t, dir, "kubernetes/flux/cluster/cluster.yaml", "x")
	cluster := filepath.Join(dir, "kubernetes", "flux", "cluster")
	link := filepath.Join(t.TempDir(), "cluster")
	assert.NoError(t, os.Symlink(cluster, link))
	result, err := AutoResolve(t.Context(), link, commit.String(), cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatal(err)
	}
	if result.Source.LocalPath != dir || result.BaseInput.LocalRoot != dir {
		t.Fatalf("coordinates: %#v %#v", result.Source, result.BaseInput)
	}
	assert.Equal(t, result.ScanPath, cluster)
	assert.Equal(t, result.BaseScanPath, filepath.Join(result.BaseInput.LocalPath, "kubernetes", "flux", "cluster"))
	assert.Equal(t, result.BaseInput.FS.(*gittree.FS).Location(), result.Source.FS.(*gittree.FS).Location())
	if data, err := fs.ReadFile(result.BaseInput.FS, "kubernetes/flux/cluster/cluster.yaml"); err != nil || string(data) != "x" {
		t.Fatalf("baseline file: %q %v", data, err)
	}
}

// TestAutoResolve_NoGitMarker confirms the materialized baseline is pure
// files — no synthetic .git. The repo root + remotes are surfaced via
// Result (TempDir / SelfURLs) and threaded explicitly, so the marker the
// old repo-root "widen" heuristic needed is gone.
func TestAutoResolve_NoGitMarker(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	initRepoWithFile(t, dir, "a.yaml", "x")
	commit := writeAndCommit(t, dir, "a.yaml", "y")
	res, err := AutoResolve(t.Context(), dir, commit.String(), cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}

	// The materialized file is present...
	if _, err := fs.Stat(res.BaseInput.FS, "a.yaml"); err != nil {
		t.Errorf("expected materialized file in TempDir: %v", err)
	}
	// ...but no synthetic .git marker.
	if _, err := fs.Stat(res.BaseInput.FS, ".git"); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("materialized baseline must have no .git marker; stat err = %v", err)
	}
}

func TestAutoResolve_PreservesExecutableBit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	r := testutil.GitInit(t, dir)
	script := filepath.Join(dir, "run.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho hi\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	commit := testutil.GitCommitAll(t, r, "init", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
	res, err := AutoResolve(t.Context(), dir, commit.String(), cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}

	info, err := fs.Stat(res.BaseInput.FS, "run.sh")
	if err != nil {
		t.Fatalf("stat materialized run.sh: %v", err)
	}
	if info.Mode().Perm()&0o100 == 0 {
		t.Errorf("exec bit lost in materialization: mode = %v", info.Mode().Perm())
	}
}

// Linked metadata must open through the common directory.
func TestAutoResolve_LinkedWorktree(t *testing.T) {
	t.Parallel()
	mainDir := t.TempDir()
	baseCommit := initRepoWithFile(t, mainDir, "a.yaml", "base")
	repo := openHelper(t, mainDir)
	const remoteURL = "https://example.invalid/upstream.git"
	_, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{remoteURL}})
	assert.Equal(t, err, nil)
	setRef(t, repo, plumbing.NewRemoteReferenceName("origin", "main"), baseCommit)
	headCommit := writeAndCommit(t, mainDir, "a.yaml", "new")
	setRef(t, repo, plumbing.NewBranchReferenceName("feature"), headCommit)

	wtDir := linkedWorktree(t, mainDir, "wt", plumbing.NewBranchReferenceName("feature"))

	res, err := AutoResolve(t.Context(), wtDir, "", cachetest.New(t, "", sourcecache.New))
	if err != nil {
		t.Fatalf("AutoResolve in linked worktree: %v", err)
	}

	assert.Diff(t, res.SelfURLs, []string{remoteURL})
	if res.Source.Metadata["baseline-selection"] != "merge-base with origin/main" {
		t.Errorf("Source = %q, want 'merge-base with origin/main'", res.Source.Metadata["baseline-selection"])
	}
	got, err := fs.ReadFile(res.BaseInput.FS, "a.yaml")
	if err != nil {
		t.Fatalf("read materialized: %v", err)
	}
	if string(got) != "base" {
		t.Errorf("materialized a.yaml = %q, want %q (the baseline tree)", got, "base")
	}
}

// TestAutoResolve_ShallowLinkedWorktree: the shallow marker lives in the
// common dir, so a worktree that exhausts the ladder must still get the
// fetch-depth guidance rather than the "no upstream" advice.
func TestAutoResolve_ShallowLinkedWorktree(t *testing.T) {
	t.Parallel()
	mainDir := t.TempDir()
	commit := initRepoWithFile(t, mainDir, "a.yaml", "x")
	repo := openHelper(t, mainDir)
	setRef(t, repo, plumbing.NewBranchReferenceName("feature"), commit)
	if err := os.WriteFile(filepath.Join(mainDir, ".git", "shallow"), []byte(commit.String()+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	wtDir := linkedWorktree(t, mainDir, "wt", plumbing.NewBranchReferenceName("feature"))

	_, err := AutoResolve(t.Context(), wtDir, "", cachetest.New(t, "", sourcecache.New))
	assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
	assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
	if err == nil {
		t.Fatal("expected shallow error")
	}
	if !strings.Contains(err.Error(), "fetch-depth: 0") {
		t.Errorf("error %q should suggest fetch-depth: 0", err)
	}
}

func initRepoWithFile(t *testing.T, dir, path, content string) plumbing.Hash {
	t.Helper()
	r := testutil.GitInit(t, dir)
	testutil.WriteFileAt(t, filepath.Join(dir, path), content)
	return testutil.GitCommitAll(t, r, "init", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
}

func writeAndCommit(t *testing.T, dir, path, content string) plumbing.Hash {
	t.Helper()
	testutil.WriteFileAt(t, filepath.Join(dir, path), content)
	r := openHelper(t, dir)
	return testutil.GitCommitAll(t, r, "update", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
}

func openHelper(t *testing.T, dir string) *git.Repository {
	t.Helper()
	r, err := git.PlainOpen(dir)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func setRef(t *testing.T, repo *git.Repository, name plumbing.ReferenceName, hash plumbing.Hash) {
	t.Helper()
	if err := repo.Storer.SetReference(plumbing.NewHashReference(name, hash)); err != nil {
		t.Fatal(err)
	}
}

func setSymRef(t *testing.T, repo *git.Repository, name, target plumbing.ReferenceName) {
	t.Helper()
	if err := repo.Storer.SetReference(plumbing.NewSymbolicReference(name, target)); err != nil {
		t.Fatal(err)
	}
}

func configureBranch(t *testing.T, repo *git.Repository, branch *config.Branch) {
	t.Helper()
	cfg, err := repo.Config()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Branches == nil {
		cfg.Branches = map[string]*config.Branch{}
	}
	cfg.Branches[branch.Name] = branch
	if err := repo.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
}

// linkedWorktree builds the layout `git worktree add -b` produces and
// returns the worktree root.
func linkedWorktree(t *testing.T, mainDir, name string, branch plumbing.ReferenceName) string {
	t.Helper()
	wtDir := filepath.Join(t.TempDir(), name)
	if err := os.MkdirAll(wtDir, 0o750); err != nil {
		t.Fatal(err)
	}
	gitDir := filepath.Join(mainDir, ".git", "worktrees", name)
	if err := os.MkdirAll(gitDir, 0o750); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		filepath.Join(wtDir, ".git"):       "gitdir: " + gitDir + "\n",
		filepath.Join(gitDir, "gitdir"):    filepath.Join(wtDir, ".git") + "\n",
		filepath.Join(gitDir, "commondir"): "../..\n",
		filepath.Join(gitDir, "HEAD"):      "ref: " + branch.String() + "\n",
	}
	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return wtDir
}

func TestResolve_RetainedConfiguration(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"upstream", "origin"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			base := initRepoWithFile(t, root, "a.yaml", "base")
			repo := openHelper(t, root)
			head, err := repo.Head()
			assert.NoError(t, err)
			var explicit string
			if route == "upstream" {
				setRef(t, repo, plumbing.NewRemoteReferenceName("upstream", "trunk"), base)
				configureBranch(t, repo, &config.Branch{Name: head.Name().Short(), Remote: "upstream", Merge: plumbing.NewBranchReferenceName("trunk")})
				writeAndCommit(t, root, "a.yaml", "head")
			} else {
				remote := t.TempDir()
				base = initRepoWithFile(t, remote, "a.yaml", "remote")
				setRef(t, openHelper(t, remote), plumbing.NewBranchReferenceName("trunk"), base)
				addOrigin(t, root, filepath.Join(remote, ".git"))
				explicit = "trunk"
			}
			input, _, err := AdmitDirectory(t.Context(), root, "")
			assert.NoError(t, err)
			location := input.FS.(*gittree.FS).Location()
			assert.NoError(t, os.Remove(filepath.Join(root, ".git", "config")))
			resolved, err := resolve(t.Context(), location, explicit, cachetest.New(t, "", sourcecache.New))
			assert.NoError(t, err)
			assert.Equal(t, resolved.Hash, base)
			if route == "upstream" {
				assert.Equal(t, resolved.Source, "merge-base with @{u}")
			} else if !strings.Contains(resolved.Source, "fetched from origin") {
				t.Fatalf("origin selection = %q", resolved.Source)
			}
		})
	}
}

func TestAdmitDirectory_ExplicitRootRetainsRepository(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	initRepoWithFile(t, root, "cluster/a.yaml", "body")
	const url = "https://example.invalid/cluster.git"
	addOrigin(t, root, url)
	content := filepath.Join(root, "cluster")
	input, scan, err := AdmitDirectory(t.Context(), content, content)
	assert.NoError(t, err)
	location := input.FS.(*gittree.FS).Location()
	assert.Equal(t, scan, content)
	assert.Equal(t, input.LocalRoot, content)
	assert.Equal(t, location.ContentRoot, content)
	assert.Equal(t, location.RepositoryRoot, root)
	assert.NoError(t, os.Remove(filepath.Join(root, ".git", "config")))
	got, ok := originURL(location.Config)
	assert.Equal(t, ok, true)
	assert.Equal(t, got, url)
	body, err := fs.ReadFile(input.FS, "a.yaml")
	assert.NoError(t, err)
	assert.Equal(t, string(body), "body")
	_, _, err = AdmitDirectory(t.Context(), root, content)
	if err == nil || !strings.Contains(err.Error(), "outside the source root") {
		t.Fatalf("escaped scan was admitted: %v", err)
	}
}

func TestResolvePaths_ReusesRoot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	initRepoWithFile(t, root, "current/a.yaml", "current")
	testutil.WriteFile(t, root, "original/a.yaml", "original")
	for _, sameScan := range []bool{false, true} {
		t.Run(fmt.Sprintf("same_scan_%t", sameScan), func(t *testing.T) {
			t.Parallel()
			current, original := filepath.Join(root, "current"), filepath.Join(root, "original")
			if sameScan {
				original = current
			}
			result, err := ResolvePaths(t.Context(), current, original)
			assert.NoError(t, err)
			assert.Equal(t, result.Source, result.BaseInput)
			assert.Equal(t, result.ScanPath, current)
			assert.Equal(t, result.BaseScanPath, original)
			missing := filepath.Join(root, "missing")
			_, err = ResolvePaths(t.Context(), missing, original)
			if !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("missing same-root scan was admitted: %v", err)
			}
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			assert.Equal(t, strings.Contains(err.Error(), missing), true)
		})
	}
}

func TestResolve_InputErrors(t *testing.T) {
	t.Parallel()
	headCause := errors.New("read HEAD")
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{name: "HEAD lookup", cause: headCause},
		{name: "missing HEAD commit", cause: plumbing.ErrObjectNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var storer storage.Storer = memory.NewStorage()
			if tc.name == "HEAD lookup" {
				storer = &headErrorStorage{Storer: storer, err: tc.cause}
			} else {
				assert.NoError(t, storer.SetReference(plumbing.NewHashReference(plumbing.HEAD, plumbing.NewHash(strings.Repeat("a", 40)))))
			}
			_, err := resolve(t.Context(), &gitrepo.Location{Repository: &git.Repository{Storer: storer}}, "", nil)
			assert.Equal(t, errors.Is(err, manifest.ErrInput), true)
			assert.Equal(t, errors.Is(err, manifest.ErrFlux), true)
			assert.Equal(t, errors.Is(err, tc.cause), true)
		})
	}
}

type headErrorStorage struct {
	storage.Storer
	err error
}

func (s *headErrorStorage) Reference(plumbing.ReferenceName) (*plumbing.Reference, error) {
	return nil, s.err
}
