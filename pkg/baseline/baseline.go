package baseline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/safepath"
	"github.com/home-operations/flate/pkg/store"
)

// Result retains admitted inputs and one selection for both render sides.
type Result struct {
	Source    *store.SourceArtifact
	BaseInput *store.SourceArtifact
	Changes   *change.Set
	Rev       string
	SelfURLs  []string
}

type resolution struct {
	Hash    plumbing.Hash
	Source  string
	Repo    *git.Repository
	backing billy.Filesystem
	remote  *baseRemote
}

// AutoResolve resolves the base once and admits lazy readers owned by owner.
// The checkout MUST remain stable until all readers have joined.
func AutoResolve(ctx context.Context, path, base string, owner *sourcecache.Cache) (*Result, error) {
	if owner == nil {
		return nil, fmt.Errorf("%w: baseline cache owner is required", manifest.ErrInput)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("%w: baseline: locate working tree: %w", manifest.ErrInput, err)
	}
	root, repo, err := gitrepo.Locate(path)
	if err != nil {
		return nil, fmt.Errorf("%w: baseline: locate working tree: %w", manifest.ErrInput, err)
	}
	if repo == nil {
		return nil, fmt.Errorf("%w: --path %q is not inside a git working tree; pass --path-orig=<dir> or --base=<rev> with a git checkout", manifest.ErrInput, path)
	}
	if _, err := relToRepo(root, path); err != nil {
		return nil, err
	}
	local := repo.Storer.(*filesystem.Storage).Filesystem()
	worktree := osfs.New(root, osfs.WithBoundOS())
	current, err := gittree.New(ctx, local, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Worktree: worktree})
	if err != nil {
		return nil, err
	}
	r, err := resolve(ctx, repo, base, owner)
	if err != nil {
		return nil, err
	}
	before, selected, err := selectChanges(ctx, selection{repo: repo, base: r, current: current, worktree: worktree, owner: owner})
	if err != nil {
		return nil, err
	}
	label := filepath.Join(root, ".flate-base", r.Hash.String())
	urls, err := gitrepo.RemoteURLs(repo)
	if err != nil {
		urls = nil
	}
	identity := manifest.Fingerprint(append([]string{root, r.Hash.String(), "direct"}, urls...))
	return &Result{
		Source:    &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: root, LocalRoot: root, FS: current, Metadata: map[string]string{"baseline-selection": r.Source}},
		BaseInput: &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: label, LocalRoot: root, Revision: r.Hash.String(), Key: "base-" + identity, FS: before},
		Changes:   selected, Rev: shortRev(r.Hash), SelfURLs: urls,
	}, nil
}

// ResolvePaths selects source roots and the original tree's remote identity.
// Only an absent original repository selects the current repository's identity.
// The original path must be non-empty.
func ResolvePaths(ctx context.Context, current, original string) (currentRoot, originalRoot string, selfURLs []string, err error) {
	if err := ctx.Err(); err != nil {
		return "", "", nil, fmt.Errorf("%w: baseline: resolve paths: %w", manifest.ErrInput, err)
	}
	originalRoot, err = filepath.Abs(original)
	if err != nil {
		return "", "", nil, fmt.Errorf("%w: baseline: resolve baseline tree path: %w", manifest.ErrInput, err)
	}
	root, originalRepo, err := gitrepo.Locate(originalRoot)
	if err != nil {
		return "", "", nil, fmt.Errorf("%w: baseline: locate baseline tree: %w", manifest.ErrInput, err)
	}
	if root != "" {
		originalRoot = root
	}
	if originalRepo != nil {
		selfURLs, err = gitrepo.RemoteURLs(originalRepo)
		if err != nil {
			return "", "", nil, fmt.Errorf("%w: baseline: read baseline tree remotes: %w", manifest.ErrInput, err)
		}
	}
	currentRoot, err = filepath.Abs(current)
	if err != nil {
		return "", "", nil, fmt.Errorf("%w: baseline: resolve working tree path: %w", manifest.ErrInput, err)
	}
	root, repo, err := gitrepo.Locate(currentRoot)
	if err != nil && originalRepo == nil {
		return "", "", nil, fmt.Errorf("%w: baseline: locate working tree: %w", manifest.ErrInput, err)
	}
	if root != "" {
		currentRoot = root
	}
	if originalRepo == nil && repo != nil {
		selfURLs, err = gitrepo.RemoteURLs(repo)
		if err != nil {
			return "", "", nil, fmt.Errorf("%w: baseline: read working tree remotes: %w", manifest.ErrInput, err)
		}
	}
	return currentRoot, originalRoot, selfURLs, nil
}

// relToRepo returns path's location relative to repoRoot. Returns "."
// when path equals repoRoot. Errors when path escapes the repo.
func relToRepo(repoRoot, path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(repoRoot, abs)
	if err != nil {
		return "", err
	}
	// filepath.Rel returns a relative path on success (it errors
	// otherwise, handled above), so no IsAbs check is needed — reject
	// only the repo-escaping ".." and "../…" forms. A dir literally named
	// "..foo" is in-repo and stays accepted.
	if safepath.Escaped(rel) {
		return "", fmt.Errorf("--path %q is outside the git repo at %q; pass --path-orig explicitly", path, repoRoot)
	}
	return rel, nil
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
func resolve(ctx context.Context, repo *git.Repository, base string, owner *sourcecache.Cache) (*resolution, error) {
	if base != "" {
		return resolveExplicit(ctx, repo, base, owner)
	}
	head, err := repo.Head()
	if err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			return nil, errors.New(
				"baseline: HEAD has no commits to merge-base from; " +
					"pass --base=<rev> to fetch the baseline from origin, or --path-orig=<dir>")
		}
		return nil, fmt.Errorf("resolve HEAD: %w", err)
	}
	headCommit, err := repo.CommitObject(head.Hash())
	if err != nil {
		return nil, fmt.Errorf("load HEAD commit: %w", err)
	}

	// go-git's ResolveRevision does not accept @{u}; read the branch config.
	if up, ok := upstreamHash(repo, head); ok {
		if mb, err := mergeBase(repo, headCommit, up); err == nil {
			return &resolution{Hash: mb, Source: "merge-base with @{u}", Repo: repo}, nil
		}
	}

	for _, rung := range []struct {
		ref    plumbing.ReferenceName
		source string
	}{
		{plumbing.NewRemoteHEADReferenceName(git.DefaultRemoteName), "merge-base with " + git.DefaultRemoteName + "/HEAD"},
		{plumbing.NewRemoteReferenceName(git.DefaultRemoteName, "main"), "merge-base with " + git.DefaultRemoteName + "/main"},
		{plumbing.NewRemoteReferenceName(git.DefaultRemoteName, "master"), "merge-base with " + git.DefaultRemoteName + "/master"},
	} {
		if tip, ok := resolveRef(repo, rung.ref); ok {
			if mb, err := mergeBase(repo, headCommit, tip); err == nil {
				return &resolution{Hash: mb, Source: rung.source, Repo: repo}, nil
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

// resolveExplicit resolves --base=<rev>: locally first, then as
// origin/<rev>, and finally by acquiring it from origin in private backing. The fetch is what makes a checkout without the
// baseline's history work: a fetch-depth=1 clone of another branch, or
// a tree written straight to disk over `git init` with only a remote.
func resolveExplicit(ctx context.Context, repo *git.Repository, base string, owner *sourcecache.Cache) (*resolution, error) {
	if h, err := repo.ResolveRevision(plumbing.Revision(base)); err == nil {
		return &resolution{Hash: *h, Source: "explicit --base=" + base, Repo: repo}, nil
	}
	// CI checkouts (actions/checkout with default fetch-depth=1)
	// land the PR's branch but not local branch refs for sibling
	// branches — only remote-tracking ones. Retry as origin/<base>
	// so `--base main` works without forcing CI users to type
	// `--base origin/main`. Skip the fallback when base already
	// looks remote-qualified to keep the error close to intent.
	if !strings.ContainsRune(base, '/') {
		remote := git.DefaultRemoteName + "/" + base
		if h, err := repo.ResolveRevision(plumbing.Revision(remote)); err == nil {
			return &resolution{Hash: *h, Source: "explicit --base=" + base + " (via " + remote + ")", Repo: repo}, nil
		}
	}
	url, ok := originURL(repo)
	if !ok {
		return nil, fmt.Errorf("could not resolve --base=%q: not found locally or as origin/%s, and there is no origin remote to fetch it from", base, base)
	}
	f, err := fetchBase(ctx, url, base, owner)
	if err != nil {
		return nil, fmt.Errorf("could not resolve --base=%q: not found locally or as origin/%s, and fetching it from origin %s failed: %w", base, base, url, err)
	}
	return &resolution{Hash: f.hash, Source: "explicit --base=" + base + " (fetched from origin)", Repo: f.repo, backing: f.backing, remote: f.remote}, nil
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
	if name, ok := strings.CutPrefix(merge, prefix); ok {
		return plumbing.NewRemoteReferenceName(remote, name)
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
	best := slices.MinFunc(bases, func(a, b *object.Commit) int {
		return bytes.Compare(a.Hash[:], b.Hash[:])
	})
	return best.Hash, nil
}

// isShallow reports whether the repo is a shallow clone. Used to
// distinguish "merge-base unreachable because CI shallow-cloned" from
// "merge-base unreachable because no upstream". Read through the storer,
// which resolves `shallow` in the common dir — a linked worktree has no
// gitdir of its own to stat.
func isShallow(repo *git.Repository) bool {
	hashes, err := repo.Storer.Shallow()
	return err == nil && len(hashes) > 0
}

// shortRev formats a hash as the conventional 7-char prefix used in
// log lines.
func shortRev(h plumbing.Hash) string {
	s := h.String()
	return s[:min(len(s), 7)]
}
