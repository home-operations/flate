package baseline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/home-operations/flate/pkg/source/cacheroot"
	"github.com/home-operations/flate/pkg/source/gittree"
)

// Result describes a materialized baseline tree on disk.
type Result struct {
	// PathOrig is the path the caller should use as the synthetic
	// --path-orig: <TempDir>/<rel> where rel is the user's --path
	// relative to the git repo root.
	PathOrig string
	// TempDir is the root of the materialized tree. When Persistent
	// is false, caller is responsible for os.RemoveAll once the diff
	// is done. When Persistent is true, the directory lives under the
	// cache root and is meant to be reused across runs — caller MUST
	// NOT remove it.
	TempDir string
	// Persistent reports whether TempDir lives under a content-
	// addressed cache root (and so survives across runs) or is a
	// disposable MkdirTemp directory (legacy behavior). Callers wire
	// cleanup conditionally on this flag.
	Persistent bool
	// Rev is the resolved short SHA of the baseline commit, for
	// logging and error context.
	Rev string
	// Source is a human-readable description of how the rev was
	// picked (e.g. "merge-base with origin/HEAD", "explicit
	// --base=main"), surfaced in the startup log line.
	Source string
}

// resolution carries the result of picking a baseline rev: the commit
// hash plus a Source label naming how it was found.
type resolution struct {
	Hash   plumbing.Hash
	Source string
}

// AutoResolve picks a baseline for path, materializes it, and returns
// a Result with the synthetic --path-orig already mapped into the
// baseline tree's coordinate system. When base is non-empty it is the
// explicit --base=<rev> override; otherwise the auto-detection ladder
// runs.
//
// layout enables content-addressed reuse: when layout.Root is non-
// empty the baseline lands at layout.Baseline(commitSHA) and Result
// is marked Persistent — subsequent runs against the same commit
// skip materialization entirely. When layout.Root is "" the legacy
// MkdirTemp path runs and the caller is responsible for cleanup.
//
// Errors carry the suggested next flag so the user knows whether to
// pass --base=<rev> or --path-orig=<dir>.
func AutoResolve(path, base string, layout cacheroot.Layout) (*Result, error) {
	repo, repoRoot, err := openRepo(path)
	if err != nil {
		return nil, err
	}
	pathOrig, err := mapToTempDir(repoRoot, "", path) // sanity-check the relpath BEFORE we materialize
	if err != nil {
		return nil, err
	}
	_ = pathOrig // computed only to surface "outside the repo" early
	r, err := resolve(repo, base)
	if err != nil {
		return nil, err
	}

	dir, persistent, err := materializeAt(repo, r.Hash, layout)
	if err != nil {
		return nil, err
	}
	pathOrig, err = mapToTempDir(repoRoot, dir, path)
	if err != nil {
		if !persistent {
			_ = os.RemoveAll(dir)
		}
		return nil, err
	}
	return &Result{
		PathOrig:   pathOrig,
		TempDir:    dir,
		Persistent: persistent,
		Rev:        shortRev(r.Hash),
		Source:     r.Source,
	}, nil
}

// materializeAt produces the baseline tree on disk for hash and
// returns (dir, persistent, err). When layout.Root is non-empty the
// directory lives at layout.Baseline(hash) and is reused across runs;
// otherwise an MkdirTemp directory is allocated.
func materializeAt(repo *git.Repository, hash plumbing.Hash, layout cacheroot.Layout) (string, bool, error) {
	if layout.Root != "" {
		slot := layout.Baseline(hash.String())
		if info, err := os.Stat(slot); err == nil && info.IsDir() {
			return slot, true, nil
		}
		if err := os.MkdirAll(filepath.Dir(slot), 0o750); err != nil {
			return "", false, fmt.Errorf("baseline cache parent: %w", err)
		}
		// Stage in a sibling temp dir on the same filesystem so the
		// finalize is an atomic rename — concurrent diffs against the
		// same commit will either share the finished slot or fall
		// through to their own stage (one wins the rename, the rest
		// see ErrExist and clean up).
		staging, err := os.MkdirTemp(filepath.Dir(slot), filepath.Base(slot)+".tmp.*")
		if err != nil {
			return "", false, fmt.Errorf("baseline staging: %w", err)
		}
		if err := materialize(repo, hash, staging); err != nil {
			_ = os.RemoveAll(staging)
			return "", false, err
		}
		if err := os.Mkdir(filepath.Join(staging, ".git"), 0o700); err != nil {
			_ = os.RemoveAll(staging)
			return "", false, fmt.Errorf("baseline .git marker: %w", err)
		}
		if err := os.Rename(staging, slot); err != nil {
			_ = os.RemoveAll(staging)
			// A racing diff may have finalized first; if the slot now
			// exists we can adopt it without re-materializing.
			if info, statErr := os.Stat(slot); statErr == nil && info.IsDir() {
				return slot, true, nil
			}
			return "", false, fmt.Errorf("baseline finalize: %w", err)
		}
		return slot, true, nil
	}

	tmp, err := os.MkdirTemp("", "flate-baseline-*")
	if err != nil {
		return "", false, fmt.Errorf("baseline tempdir: %w", err)
	}
	if err := materialize(repo, hash, tmp); err != nil {
		_ = os.RemoveAll(tmp)
		return "", false, err
	}
	// Drop a .git marker so discovery.FindRepoRoot (called by
	// orchestrator.buildChangeFilter's repo-root widening, PR #348)
	// lifts the synthetic --path-orig to tmp, lining up with the
	// current side's repoRoot. Without it, the per-side .git roots
	// match (both fall back to the passed path) and the widening
	// short-circuits.
	if err := os.Mkdir(filepath.Join(tmp, ".git"), 0o700); err != nil {
		_ = os.RemoveAll(tmp)
		return "", false, fmt.Errorf("baseline .git marker: %w", err)
	}
	return tmp, false, nil
}

// mapToTempDir mirrors path's relative location under repoRoot into
// tempDir. Used twice: once with an empty tempDir to validate the
// path is inside the repo before we do anything expensive, and once
// with the real tempDir after materialization.
func mapToTempDir(repoRoot, tempDir, path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(repoRoot, abs)
	if err != nil {
		return "", err
	}
	if rel == "." {
		return tempDir, nil
	}
	if rel == ".." || filepath.IsAbs(rel) || len(rel) >= 3 && rel[:3] == ".."+string(filepath.Separator) {
		return "", fmt.Errorf("--path %q is outside the git repo at %q; pass --path-orig explicitly", path, repoRoot)
	}
	return filepath.Join(tempDir, rel), nil
}

// GitRepoRoot returns the .git ancestor of path, or "" when none
// exists. Exposed so callers can branch on "is this a git repo" before
// calling AutoResolve. Renamed from FindRepoRoot to disambiguate from
// `discovery.FindRepoRoot` which has different fallback semantics
// (returns p unchanged when no .git exists, rather than "").
func GitRepoRoot(path string) string {
	abs, err := filepath.Abs(path)
	if err != nil {
		return ""
	}
	for cur := abs; ; {
		if _, err := os.Stat(filepath.Join(cur, ".git")); err == nil {
			return cur
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return ""
		}
		cur = parent
	}
}

// openRepo opens the git repo containing path. Returns the *Repository
// and the repo root (the directory containing .git).
func openRepo(path string) (*git.Repository, string, error) {
	root := GitRepoRoot(path)
	if root == "" {
		return nil, "", fmt.Errorf("--path %q is not inside a git working tree; pass --path-orig=<dir> or --base=<rev> with a git checkout", path)
	}
	repo, err := git.PlainOpen(root)
	if err != nil {
		return nil, "", fmt.Errorf("open git repo at %q: %w", root, err)
	}
	return repo, root, nil
}

// resolve picks the baseline commit for the given repo. When base is
// non-empty it's an explicit rev (passed via --base=<rev>); otherwise
// the auto-detection ladder runs:
//
//  1. @{u} — current branch's configured upstream — merge-base with HEAD
//  2. origin/HEAD — the default remote's default-branch symref — merge-base with HEAD
//  3. origin/main / origin/master — merge-base with HEAD
//
// Each rung is tried in order; the first that resolves AND has a
// reachable merge-base wins. Falling off the end is an error (no silent
// fallback to HEAD — that would silently change "preview my PR" into
// "preview my dirty edits"). Shallow clones can't compute a merge-base
// against any of these refs because the necessary commits are absent;
// detect .git/shallow and emit a CI-friendly error.
func resolve(repo *git.Repository, base string) (*resolution, error) {
	head, err := repo.Head()
	if err != nil {
		return nil, fmt.Errorf("resolve HEAD: %w", err)
	}
	headCommit, err := repo.CommitObject(head.Hash())
	if err != nil {
		return nil, fmt.Errorf("load HEAD commit: %w", err)
	}

	if base != "" {
		h, err := repo.ResolveRevision(plumbing.Revision(base))
		if err != nil {
			return nil, fmt.Errorf("could not resolve --base=%q: %w", base, err)
		}
		return &resolution{Hash: *h, Source: "explicit --base=" + base}, nil
	}

	// 1. @{u} via the branch config (go-git's ResolveRevision doesn't
	//    accept @{u}; read branch.<name>.remote/.merge directly).
	if up, ok := upstreamHash(repo, head); ok {
		if base, err := mergeBase(repo, headCommit, up); err == nil {
			return &resolution{Hash: base, Source: "merge-base with @{u}"}, nil
		}
	}

	// 2. origin/HEAD: a symbolic ref under refs/remotes/origin/HEAD
	//    pointing at e.g. refs/remotes/origin/main. Resolve through
	//    the symref to the underlying branch tip.
	if h, ok := resolveRef(repo, plumbing.NewRemoteHEADReferenceName("origin")); ok {
		if base, err := mergeBase(repo, headCommit, h); err == nil {
			return &resolution{Hash: base, Source: "merge-base with origin/HEAD"}, nil
		}
	}

	// 3. Common defaults when origin/HEAD isn't set (older clones,
	//    some self-hosted setups).
	for _, name := range []string{"main", "master"} {
		ref := plumbing.NewRemoteReferenceName("origin", name)
		if h, ok := resolveRef(repo, ref); ok {
			if base, err := mergeBase(repo, headCommit, h); err == nil {
				return &resolution{Hash: base, Source: "merge-base with origin/" + name}, nil
			}
		}
	}

	// Distinguish "shallow clone, can't see the merge-base" from "no
	// upstream configured at all" — the first is a CI fix (fetch-depth),
	// the second is a flag fix (--base=<rev>).
	if isShallow(repo) {
		return nil, errors.New(
			"baseline: cannot compute merge-base — repo appears shallow (.git/shallow present); " +
				"in actions/checkout, set 'fetch-depth: 0', or locally run 'git fetch --unshallow', " +
				"or pass --base=<ref> / --path-orig=<dir> explicitly")
	}
	return nil, errors.New(
		"baseline: could not auto-detect — HEAD has no upstream, origin/HEAD is unset, " +
			"and origin/{main,master} are absent; pass --base=<ref> or --path-orig=<dir>")
}

// upstreamHash reads the current branch's upstream from the repo
// config (branch.<name>.remote + branch.<name>.merge) and resolves it
// to a commit. Returns ok=false when HEAD isn't on a branch (detached)
// or no upstream is configured.
func upstreamHash(repo *git.Repository, head *plumbing.Reference) (plumbing.Hash, bool) {
	if head.Name() == "" || !head.Name().IsBranch() {
		return plumbing.ZeroHash, false
	}
	cfg, err := repo.Config()
	if err != nil {
		return plumbing.ZeroHash, false
	}
	branch, ok := cfg.Branches[head.Name().Short()]
	if !ok || branch.Remote == "" || branch.Merge == "" {
		return plumbing.ZeroHash, false
	}
	// branch.Merge is a local-style ref (refs/heads/<name>); map it to
	// the remote-tracking equivalent (refs/remotes/<remote>/<name>) so
	// we read the fetched copy, not the (possibly stale or absent)
	// local branch.
	remote := branchMergeToRemoteTracking(branch.Remote, string(branch.Merge))
	if h, ok := resolveRef(repo, remote); ok {
		return h, true
	}
	// Fall back to the literal Merge ref in case the user has a
	// non-standard layout (no remote-tracking refs).
	if h, err := repo.ResolveRevision(plumbing.Revision(branch.Merge)); err == nil {
		return *h, true
	}
	return plumbing.ZeroHash, false
}

// branchMergeToRemoteTracking maps "refs/heads/<name>" to
// "refs/remotes/<remote>/<name>". Pass-through for any other shape so
// non-standard configs don't get mangled.
func branchMergeToRemoteTracking(remote, merge string) plumbing.ReferenceName {
	const prefix = "refs/heads/"
	if len(merge) > len(prefix) && merge[:len(prefix)] == prefix {
		return plumbing.NewRemoteReferenceName(remote, merge[len(prefix):])
	}
	return plumbing.ReferenceName(merge)
}

// resolveRef looks up a ref by name and returns its hash. ok=false on
// any failure (ref missing, symref chain broken, etc.).
func resolveRef(repo *git.Repository, name plumbing.ReferenceName) (plumbing.Hash, bool) {
	ref, err := repo.Reference(name, true)
	if err != nil {
		return plumbing.ZeroHash, false
	}
	return ref.Hash(), true
}

// mergeBase returns the merge-base of headCommit and other (a hash).
// Errors out when the two commits share no common ancestor (disjoint
// histories) — propagated up so the resolution ladder falls through to
// the next rung.
func mergeBase(repo *git.Repository, headCommit *object.Commit, other plumbing.Hash) (plumbing.Hash, error) {
	otherCommit, err := repo.CommitObject(other)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	bases, err := headCommit.MergeBase(otherCommit)
	if err != nil {
		return plumbing.ZeroHash, err
	}
	if len(bases) == 0 {
		return plumbing.ZeroHash, errors.New("no merge-base (disjoint histories)")
	}
	// Criss-cross merges produce >1 candidate base; go-git doesn't
	// guarantee a stable order. Pick the lexicographically-smallest
	// hash so two `flate diff` runs against the same repo pick the
	// same baseline — reproducibility is a stated guarantee.
	best := bases[0].Hash
	for _, b := range bases[1:] {
		if b.Hash.String() < best.String() {
			best = b.Hash
		}
	}
	return best, nil
}

// isShallow reports whether the repo is a shallow clone (presence of
// .git/shallow). Used to distinguish "merge-base unreachable because
// CI shallow-cloned" from "merge-base unreachable because no upstream".
func isShallow(repo *git.Repository) bool {
	root, err := repoStorerPath(repo)
	if err != nil {
		return false
	}
	_, err = os.Stat(filepath.Join(root, "shallow"))
	return err == nil
}

// repoStorerPath returns the on-disk .git directory for repo. go-git
// hides this behind the Storer interface, but every repo opened via
// PlainOpen uses a filesystem-backed Storer pointing at .git/. Walk up
// from cwd via the same FindRepoRoot we use elsewhere — simpler and
// avoids reflection on go-git's internals.
func repoStorerPath(repo *git.Repository) (string, error) {
	// repo.Worktree().Filesystem.Root() returns the worktree root;
	// .git lives at <root>/.git for non-bare repos. go-git's
	// Worktree.Filesystem is the worktree's billy.FS, so Root() is
	// guaranteed to point at the working tree.
	wt, err := repo.Worktree()
	if err != nil {
		return "", err
	}
	return filepath.Join(wt.Filesystem.Root(), ".git"), nil
}

// materialize extracts every blob in commit's tree to root via the
// shared gittree.Materialize helper. The materialization runs in
// parallel; submodules log at slog.Warn and are skipped (the
// submodule's state rarely matches what flate is rendering).
func materialize(repo *git.Repository, hash plumbing.Hash, root string) error {
	return gittree.Materialize(context.Background(), repo, hash, root, gittree.Options{
		OnSubmodule: func(path string) {
			slog.Warn("baseline: skipping submodule", "path", path)
		},
	})
}

// shortRev formats a hash as the conventional 7-char prefix used in
// log lines.
func shortRev(h plumbing.Hash) string {
	s := h.String()
	if len(s) <= 7 {
		return s
	}
	return s[:7]
}

