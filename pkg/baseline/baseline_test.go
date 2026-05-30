package baseline

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// TestAutoResolve_ExplicitBase pins the --base=<rev> escape hatch: an
// explicit rev bypasses the auto-detection ladder and the named commit
// becomes the baseline.
func TestAutoResolve_ExplicitBase(t *testing.T) {
	dir := t.TempDir()
	commitA := initRepoWithFile(t, dir, "a.yaml", "original")
	commitB := writeAndCommit(t, dir, "a.yaml", "updated")

	res, err := AutoResolve(dir, commitA.String(), cacheroot.Layout{})
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}
	defer func() { _ = os.RemoveAll(res.TempDir) }()
	if !strings.HasPrefix(res.Source, "explicit --base=") {
		t.Errorf("Source = %q, want prefix 'explicit --base='", res.Source)
	}
	got, err := os.ReadFile(filepath.Join(res.TempDir, "a.yaml"))
	if err != nil {
		t.Fatalf("read materialized: %v", err)
	}
	if string(got) != "original" {
		t.Errorf("materialized a.yaml = %q, want %q (should be commitA's tree, not HEAD's)", got, "original")
	}
	_ = commitB
}

// TestAutoResolve_UpstreamMergeBase exercises the @{u} rung: HEAD is on
// a branch whose config points at a remote-tracking ref, and the
// merge-base resolves correctly.
func TestAutoResolve_UpstreamMergeBase(t *testing.T) {
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
	configureBranch(t, repo, branchName, "origin", "refs/heads/"+branchName)

	// Move forward on the local branch.
	writeAndCommit(t, dir, "a.yaml", "diverged")

	res, err := AutoResolve(dir, "", cacheroot.Layout{})
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}
	defer func() { _ = os.RemoveAll(res.TempDir) }()
	if res.Source != "merge-base with @{u}" {
		t.Errorf("Source = %q, want 'merge-base with @{u}'", res.Source)
	}
	got, err := os.ReadFile(filepath.Join(res.TempDir, "a.yaml"))
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
	dir := t.TempDir()
	mainCommit := initRepoWithFile(t, dir, "a.yaml", "base")
	repo := openHelper(t, dir)
	// Simulate the remote-tracking branch and origin/HEAD symref
	// pointing at it. No branch config — so @{u} is unset.
	setRef(t, repo, plumbing.NewRemoteReferenceName("origin", "main"), mainCommit)
	setSymRef(t, repo, plumbing.NewRemoteHEADReferenceName("origin"),
		plumbing.NewRemoteReferenceName("origin", "main"))

	writeAndCommit(t, dir, "a.yaml", "new")

	res, err := AutoResolve(dir, "", cacheroot.Layout{})
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}
	defer func() { _ = os.RemoveAll(res.TempDir) }()
	if res.Source != "merge-base with origin/HEAD" {
		t.Errorf("Source = %q, want 'merge-base with origin/HEAD'", res.Source)
	}
}

// TestAutoResolve_ExplicitBaseFallsBackToOrigin locks the CI ergonomic
// the workflow-side broke on: `--base main` on a checkout that has
// only `origin/main` (no local main ref) must resolve via the
// remote-tracking branch. actions/checkout with default
// fetch-depth=1 lands the PR branch and origin/* but no sibling
// local refs; we shouldn't force users to type `--base origin/main`.
func TestAutoResolve_ExplicitBaseFallsBackToOrigin(t *testing.T) {
	dir := t.TempDir()
	mainCommit := initRepoWithFile(t, dir, "a.yaml", "base")
	repo := openHelper(t, dir)
	// Simulate the CI checkout: only the remote-tracking ref exists.
	setRef(t, repo, plumbing.NewRemoteReferenceName("origin", "main"), mainCommit)
	writeAndCommit(t, dir, "a.yaml", "pr-tip")

	res, err := AutoResolve(dir, "main", cacheroot.Layout{})
	if err != nil {
		t.Fatalf("AutoResolve(--base=main) should fall back to origin/main: %v", err)
	}
	defer func() { _ = os.RemoveAll(res.TempDir) }()
	if got := res.Rev; got != shortRev(mainCommit) {
		t.Errorf("Rev = %q, want %q", got, shortRev(mainCommit))
	}
	if !strings.Contains(res.Source, "via origin/main") {
		t.Errorf("Source = %q, want to mention 'via origin/main'", res.Source)
	}
}

// TestAutoResolve_ExplicitBaseNeitherLocalNorRemote: when --base
// resolves neither locally nor as origin/<base>, the error names
// both lookup attempts so the user knows what to fix.
func TestAutoResolve_ExplicitBaseNeitherLocalNorRemote(t *testing.T) {
	dir := t.TempDir()
	initRepoWithFile(t, dir, "a.yaml", "base")
	writeAndCommit(t, dir, "a.yaml", "pr-tip")

	_, err := AutoResolve(dir, "nonexistent", cacheroot.Layout{})
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
	dir := t.TempDir()
	mainCommit := initRepoWithFile(t, dir, "a.yaml", "base")
	repo := openHelper(t, dir)
	setRef(t, repo, plumbing.NewRemoteReferenceName("origin", "main"), mainCommit)
	// Deliberately no origin/HEAD symref and no branch config.

	writeAndCommit(t, dir, "a.yaml", "new")

	res, err := AutoResolve(dir, "", cacheroot.Layout{})
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}
	defer func() { _ = os.RemoveAll(res.TempDir) }()
	if res.Source != "merge-base with origin/main" {
		t.Errorf("Source = %q, want 'merge-base with origin/main'", res.Source)
	}
}

// TestAutoResolve_DetachedHEAD: HEAD is at a SHA, not a branch. The
// @{u} rung is unreachable (no branch → no config); the algorithm
// must fall through to origin/HEAD. This is the GH Actions PR case.
func TestAutoResolve_DetachedHEAD(t *testing.T) {
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

	res, err := AutoResolve(dir, "", cacheroot.Layout{})
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}
	defer func() { _ = os.RemoveAll(res.TempDir) }()
	if res.Source != "merge-base with origin/HEAD" {
		t.Errorf("Source = %q, want 'merge-base with origin/HEAD' (detached HEAD should skip @{u})", res.Source)
	}
}

// TestAutoResolve_NoGit: a path that isn't inside a .git ancestor must
// error with a message naming the alternative flags.
func TestAutoResolve_NoGit(t *testing.T) {
	dir := t.TempDir()
	testutil.WriteFileAt(t, filepath.Join(dir, "f.yaml"), "x")
	_, err := AutoResolve(dir, "", cacheroot.Layout{})
	if err == nil {
		t.Fatal("expected error for non-git path")
	}
	if !strings.Contains(err.Error(), "not inside a git working tree") {
		t.Errorf("error %q: should explain why and suggest --path-orig / --base", err)
	}
}

// TestAutoResolve_Shallow: presence of .git/shallow is the CI shallow-
// clone signal and must produce a distinct error from "no upstream"
// so users know to set fetch-depth: 0.
func TestAutoResolve_Shallow(t *testing.T) {
	dir := t.TempDir()
	initRepoWithFile(t, dir, "a.yaml", "x")
	if err := os.WriteFile(filepath.Join(dir, ".git", "shallow"), []byte("deadbeef\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// No upstream, no origin refs → the resolution ladder falls
	// through; the shallow detection fires last.
	_, err := AutoResolve(dir, "", cacheroot.Layout{})
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
	dir := t.TempDir()
	initRepoWithFile(t, dir, "a.yaml", "x")
	_, err := AutoResolve(dir, "", cacheroot.Layout{})
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
	outerDir := t.TempDir()
	repoDir := filepath.Join(outerDir, "repo")
	if err := os.Mkdir(repoDir, 0o750); err != nil {
		t.Fatal(err)
	}
	initRepoWithFile(t, repoDir, "a.yaml", "x")
	// Sibling directory, outside the repo. We expect openRepo to find
	// no .git ancestor for outerDir/sibling, and surface a clear error.
	sibling := filepath.Join(outerDir, "sibling")
	if err := os.Mkdir(sibling, 0o750); err != nil {
		t.Fatal(err)
	}
	_, err := AutoResolve(sibling, "", cacheroot.Layout{})
	if err == nil {
		t.Fatal("expected error for path outside repo")
	}
}

// TestAutoResolve_PathOrigMappedToSubdir: when --path is a subdir,
// the synthetic PathOrig must be <tempdir>/<rel>, not just tempdir.
// This is the load-bearing case the agents flagged in PR #348's
// validation: real users point at cluster/, not the repo root.
func TestAutoResolve_PathOrigMappedToSubdir(t *testing.T) {
	dir := t.TempDir()
	initRepoWithFile(t, dir, "kubernetes/flux/cluster/cluster.yaml", "x")
	commit := writeAndCommit(t, dir, "kubernetes/flux/cluster/cluster.yaml", "y")

	clusterDir := filepath.Join(dir, "kubernetes", "flux", "cluster")
	res, err := AutoResolve(clusterDir, commit.String(), cacheroot.Layout{})
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}
	defer func() { _ = os.RemoveAll(res.TempDir) }()
	wantSuffix := filepath.Join("kubernetes", "flux", "cluster")
	if !strings.HasSuffix(res.PathOrig, wantSuffix) {
		t.Errorf("PathOrig = %q, want suffix %q", res.PathOrig, wantSuffix)
	}
	if !strings.HasPrefix(res.PathOrig, res.TempDir) {
		t.Errorf("PathOrig = %q, want prefix %q", res.PathOrig, res.TempDir)
	}
	// The mapped file must exist at the synthetic --path-orig.
	if _, err := os.Stat(filepath.Join(res.PathOrig, "cluster.yaml")); err != nil {
		t.Errorf("expected materialized file at PathOrig: %v", err)
	}
}

// TestAutoResolve_DropsGitMarker confirms the .git marker is present
// in TempDir so discovery.FindRepoRoot (used by PR #348's repo-root
// widening) lifts up correctly when --path-orig is a subdir.
func TestAutoResolve_DropsGitMarker(t *testing.T) {
	dir := t.TempDir()
	initRepoWithFile(t, dir, "a.yaml", "x")
	commit := writeAndCommit(t, dir, "a.yaml", "y")
	res, err := AutoResolve(dir, commit.String(), cacheroot.Layout{})
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}
	defer func() { _ = os.RemoveAll(res.TempDir) }()
	info, err := os.Stat(filepath.Join(res.TempDir, ".git"))
	if err != nil {
		t.Fatalf("expected .git marker in TempDir: %v", err)
	}
	if !info.IsDir() {
		t.Errorf(".git marker should be a directory")
	}
}

// TestAutoResolve_CachedReusesSlot: when cacheRoot is non-empty, the
// baseline lands at <cacheRoot>/baselines/<sha>/. A second AutoResolve
// against the same commit returns Persistent=true with the same dir
// and skips re-materialization. Removing a file from the materialized
// tree before the second call would normally be a no-op since we don't
// re-walk — locking that contract here.
func TestAutoResolve_CachedReusesSlot(t *testing.T) {
	dir := t.TempDir()
	initRepoWithFile(t, dir, "a.yaml", "x")
	commit := writeAndCommit(t, dir, "a.yaml", "y")
	cacheRoot := t.TempDir()

	res1, err := AutoResolve(dir, commit.String(), cacheroot.New(cacheRoot))
	if err != nil {
		t.Fatalf("AutoResolve 1: %v", err)
	}
	if !res1.Persistent {
		t.Error("cached AutoResolve should report Persistent=true")
	}
	want := filepath.Join(cacheRoot, "baselines", commit.String())
	if res1.TempDir != want {
		t.Errorf("TempDir = %q, want %q", res1.TempDir, want)
	}
	// Stamp a sentinel so we can prove run 2 didn't re-materialize
	// (which would replace the stamp).
	sentinel := filepath.Join(res1.TempDir, ".flate-test-stamp")
	if err := os.WriteFile(sentinel, []byte("kept"), 0o600); err != nil {
		t.Fatal(err)
	}

	res2, err := AutoResolve(dir, commit.String(), cacheroot.New(cacheRoot))
	if err != nil {
		t.Fatalf("AutoResolve 2: %v", err)
	}
	if res2.TempDir != res1.TempDir {
		t.Errorf("second call drifted: %q vs %q", res2.TempDir, res1.TempDir)
	}
	if got, _ := os.ReadFile(sentinel); string(got) != "kept" { //nolint:gosec // sentinel built under t.TempDir
		t.Errorf("second call re-materialized; sentinel = %q", got)
	}
}

// TestAutoResolve_NoCacheRootIsTempdir confirms the legacy path still
// works: empty cacheRoot → MkdirTemp directory, Persistent=false.
func TestAutoResolve_NoCacheRootIsTempdir(t *testing.T) {
	dir := t.TempDir()
	initRepoWithFile(t, dir, "a.yaml", "x")
	commit := writeAndCommit(t, dir, "a.yaml", "y")

	res, err := AutoResolve(dir, commit.String(), cacheroot.Layout{})
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}
	defer func() { _ = os.RemoveAll(res.TempDir) }()
	if res.Persistent {
		t.Error("uncached AutoResolve should report Persistent=false")
	}
	if !strings.HasPrefix(filepath.Base(res.TempDir), "flate-baseline-") {
		t.Errorf("uncached TempDir = %q, want flate-baseline-* basename", res.TempDir)
	}
}

// TestMaterialize_PreservesExecutableBit: blobs stored as executable
// must be written with the exec bit set so downstream consumers (e.g.,
// scripts referenced by configMapGenerator with envs:) behave the
// same against the baseline as against the working tree.
func TestMaterialize_PreservesExecutableBit(t *testing.T) {
	dir := t.TempDir()
	r, err := git.PlainInit(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(dir, "run.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho hi\n"), 0o755); err != nil { //nolint:gosec // test fixture
		t.Fatal(err)
	}
	wt, err := r.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wt.Add("run.sh"); err != nil {
		t.Fatal(err)
	}
	commit, err := wt.Commit("init", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := AutoResolve(dir, commit.String(), cacheroot.Layout{})
	if err != nil {
		t.Fatalf("AutoResolve: %v", err)
	}
	defer func() { _ = os.RemoveAll(res.TempDir) }()
	info, err := os.Stat(filepath.Join(res.TempDir, "run.sh"))
	if err != nil {
		t.Fatalf("stat materialized run.sh: %v", err)
	}
	if info.Mode().Perm()&0o100 == 0 {
		t.Errorf("exec bit lost in materialization: mode = %v", info.Mode().Perm())
	}
}

// --- helpers --------------------------------------------------------

func initRepoWithFile(t *testing.T, dir, path, content string) plumbing.Hash {
	t.Helper()
	r, err := git.PlainInit(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	testutil.WriteFileAt(t, filepath.Join(dir, path), content)
	wt, err := r.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wt.Add(path); err != nil {
		t.Fatal(err)
	}
	hash, err := wt.Commit("init", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return hash
}

func writeAndCommit(t *testing.T, dir, path, content string) plumbing.Hash {
	t.Helper()
	testutil.WriteFileAt(t, filepath.Join(dir, path), content)
	r := openHelper(t, dir)
	wt, err := r.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wt.Add(path); err != nil {
		t.Fatal(err)
	}
	hash, err := wt.Commit("update", &git.CommitOptions{
		Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return hash
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

func configureBranch(t *testing.T, repo *git.Repository, branch, remote, merge string) {
	t.Helper()
	cfg, err := repo.Config()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Branches == nil {
		cfg.Branches = map[string]*config.Branch{}
	}
	cfg.Branches[branch] = &config.Branch{
		Name:   branch,
		Remote: remote,
		Merge:  plumbing.ReferenceName(merge),
	}
	if err := repo.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
}
