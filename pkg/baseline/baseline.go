package baseline

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"

	"github.com/go-git/go-billy/v5"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/source/gitrepo"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// Result retains admitted inputs and one selection for both render sides.
type Result struct {
	Source       *store.SourceArtifact
	BaseInput    *store.SourceArtifact
	ScanPath     string
	BaseScanPath string
	Changes      *change.Set
	SelfURLs     []string
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
// An error may return diagnostic-only inputs with no usable content.
func AutoResolve(ctx context.Context, path, base string, owner *sourcecache.Cache) (*Result, error) {
	if owner == nil {
		return nil, fmt.Errorf("%w: baseline cache owner is required", manifest.ErrInput)
	}
	location, err := gitrepo.Locate(ctx, path, "")
	if err != nil {
		return nil, fmt.Errorf("%w: baseline: locate working tree: %w", manifest.ErrInput, err)
	}
	if location.LookupErr != nil {
		return nil, fmt.Errorf("%w: baseline: locate working tree: %w", manifest.ErrInput, location.LookupErr)
	}
	repo := location.Repository
	if repo == nil {
		return nil, fmt.Errorf("%w: --path %q is not inside a git working tree; pass --path-orig=<dir> or --base=<rev> with a git checkout", manifest.ErrInput, path)
	}
	root := location.ContentRoot
	input, err := admitLocation(ctx, location)
	if err != nil {
		return nil, err
	}
	current := input.FS.(*gittree.FS)
	r, err := resolve(ctx, location, base, owner)
	if err != nil {
		return nil, err
	}
	urls := location.URLs
	content := struct {
		Root    string
		Remotes []string
	}{root, urls}
	before, selected, identity, warnings, err := selectChanges(ctx, selection{repo: repo, base: r, current: current, owner: owner, content: content})
	input.Warnings = append(slices.Clone(input.Warnings), warnings...)
	var baseWarnings []manifest.Warning
	if before != nil {
		baseWarnings = slices.Clone(before.Warnings)
		for i := range baseWarnings {
			baseWarnings[i].Resource = manifest.BootstrapSourceID
		}
	}
	if err != nil {
		result := &Result{Source: &store.SourceArtifact{Warnings: input.Warnings}}
		if before != nil {
			result.BaseInput = &store.SourceArtifact{Warnings: baseWarnings}
		}
		return result, err
	}
	label, _ := gittree.Label(gittree.LabelOptions{Root: root, Mode: gittree.Direct, Key: r.Hash.String()})
	rel, err := filepath.Rel(root, location.ScanPath)
	if err != nil {
		return nil, err
	}
	input.URL = ""
	input.Metadata = map[string]string{"baseline-selection": r.Source}
	return &Result{
		Source:    input,
		BaseInput: &store.SourceArtifact{Kind: manifest.KindGitRepository, LocalPath: label, LocalRoot: root, Revision: r.Hash.String(), Key: identity, FS: before, Warnings: baseWarnings},
		ScanPath:  location.ScanPath, BaseScanPath: filepath.Join(label, rel),
		Changes: selected, SelfURLs: urls,
	}, nil
}

// ResolvePaths admits both inputs and retains their canonical scan paths.
// Only an absent original repository selects the current repository's identity.
// The original path must be non-empty.
func ResolvePaths(ctx context.Context, current, original string) (*Result, error) {
	before, err := gitrepo.Locate(ctx, original, "")
	if err != nil {
		return nil, fmt.Errorf("%w: baseline: resolve baseline tree path: %w", manifest.ErrInput, err)
	}
	if before.LookupErr != nil {
		return nil, fmt.Errorf("%w: baseline: locate baseline tree: %w", manifest.ErrInput, before.LookupErr)
	}
	if before.ConfigErr != nil {
		return nil, fmt.Errorf("%w: baseline: read baseline tree remotes: %w", manifest.ErrInput, before.ConfigErr)
	}
	after := before
	if current != original {
		after, err = gitrepo.Locate(ctx, current, "")
		if err != nil {
			return nil, fmt.Errorf("%w: baseline: resolve working tree path: %w", manifest.ErrInput, err)
		}
	}
	urls := before.URLs
	if before.Repository == nil {
		if after.LookupErr != nil {
			return nil, fmt.Errorf("%w: baseline: locate working tree: %w", manifest.ErrInput, after.LookupErr)
		}
		if after.ConfigErr != nil {
			return nil, fmt.Errorf("%w: baseline: read working tree remotes: %w", manifest.ErrInput, after.ConfigErr)
		}
		urls = after.URLs
	}
	baseInput, err := admitLocation(ctx, before)
	if err != nil {
		return nil, err
	}
	input := baseInput
	if after.ContentRoot != before.ContentRoot {
		input, err = admitLocation(ctx, after)
		if err != nil {
			return nil, err
		}
	} else if after.ScanPath != before.ScanPath {
		if _, err := os.Stat(after.ScanPath); err != nil {
			return nil, fmt.Errorf("%w: --path %q: %w", manifest.ErrInput, after.ScanPath, err)
		}
	}
	return &Result{Source: input, BaseInput: baseInput, ScanPath: after.ScanPath, BaseScanPath: before.ScanPath, SelfURLs: urls}, nil
}

func resolve(ctx context.Context, location *gitrepo.Location, base string, owner *sourcecache.Cache) (*resolution, error) {
	repo := location.Repository
	if base != "" {
		return resolveExplicit(ctx, location, base, owner)
	}
	head, err := repo.Head()
	if err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			return nil, fmt.Errorf("%w: baseline: HEAD has no commits to merge-base from; pass --base=<rev> to fetch the baseline from origin, or --path-orig=<dir>: %w", manifest.ErrInput, err)
		}
		return nil, fmt.Errorf("%w: resolve HEAD: %w", manifest.ErrInput, err)
	}
	headCommit, err := repo.CommitObject(head.Hash())
	if err != nil {
		return nil, fmt.Errorf("%w: load HEAD commit: %w", manifest.ErrInput, err)
	}

	// go-git's ResolveRevision does not accept @{u}; read the branch config.
	if up, ok := upstreamHash(repo, location.Config, head); ok {
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

	if isShallow(repo) {
		return nil, fmt.Errorf(
			"%w: baseline: cannot compute merge-base — repo appears shallow (.git/shallow present); "+
				"in actions/checkout, set 'fetch-depth: 0', or locally run 'git fetch --unshallow', "+
				"or pass --base=<ref> / --path-orig=<dir> explicitly", manifest.ErrInput)
	}
	return nil, fmt.Errorf(
		"%w: baseline: could not auto-detect — HEAD has no upstream, origin/HEAD is unset, "+
			"and origin/{main,master} are absent; pass --base=<ref> or --path-orig=<dir>", manifest.ErrInput)
}

func resolveExplicit(ctx context.Context, location *gitrepo.Location, base string, owner *sourcecache.Cache) (*resolution, error) {
	repo := location.Repository
	h, err := repo.ResolveRevision(plumbing.Revision(base))
	if err == nil {
		return &resolution{Hash: *h, Source: "explicit --base=" + base, Repo: repo}, nil
	}
	remote := git.DefaultRemoteName + "/" + base
	if h, err := repo.ResolveRevision(plumbing.Revision(remote)); err == nil {
		return &resolution{Hash: *h, Source: "explicit --base=" + base + " (via " + remote + ")", Repo: repo}, nil
	}
	url, ok := originURL(location.Config)
	if !ok {
		return nil, fmt.Errorf("%w: could not resolve --base=%q: not found locally or as origin/%s, and there is no origin remote to fetch it from: %w", manifest.ErrInput, base, base, err)
	}
	r, err := fetchBase(ctx, url, base, owner)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: could not resolve --base=%q: not found locally or as origin/%s, and fetching it from origin %s failed: %w", manifest.ErrInput, base, base, url, err)
	}
	r.Source = "explicit --base=" + base + " (fetched from origin)"
	return r, nil
}

// upstreamHash reads the current branch's upstream from the repo
// config (branch.<name>.remote + branch.<name>.merge) and resolves it
// to a commit. Returns ok=false when HEAD isn't on a branch (detached)
// or no upstream is configured.
func upstreamHash(repo *git.Repository, cfg *config.Config, head *plumbing.Reference) (plumbing.Hash, bool) {
	if head.Name() == "" || !head.Name().IsBranch() {
		return plumbing.ZeroHash, false
	}
	if cfg == nil {
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
