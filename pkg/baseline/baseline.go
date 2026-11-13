package baseline

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-git/go-billy/v6"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"

	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	sourcegit "github.com/home-operations/flate/pkg/source/git"
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

// Config requires Cache and Network to outlive all returned lazy readers.
type Config struct {
	Path          string
	Base          string
	Cache         *sourcecache.Cache
	Network       *source.Network
	GitTransports sourcegit.Transports
}

// AutoResolve requires that the checkout remain stable until all readers have joined.
// An error may return diagnostic-only inputs with no usable content.
func AutoResolve(ctx context.Context, cfg Config) (*Result, error) {
	cfg.GitTransports = maps.Clone(cfg.GitTransports)
	path, base, owner := cfg.Path, cfg.Base, cfg.Cache
	if owner == nil {
		return nil, manifest.NewError(manifest.ErrInput, nil, "baseline cache owner is required")
	}
	location, err := gitrepo.Locate(ctx, path, "")
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("baseline: locate working tree: %w", err), "")
	}
	if location.LookupErr != nil {
		return nil, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("baseline: locate working tree: %w", location.LookupErr),
			"",
		)
	}
	repo := location.Repository
	if repo == nil {
		return nil, manifest.NewError(
			manifest.ErrInput,
			nil,
			fmt.Sprintf(
				"--path %q is not inside a git working tree; pass --path-orig=<dir> or --base=<rev> with a git checkout",
				path,
			),
		)
	}
	root, urls := location.ContentRoot, location.URLs
	directory, err := admitLocation(ctx, location, manifest.WorkScope{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = directory.Close() }()
	input := gittree.ExcludeDirectory(directory.Artifact(), owner.Directory())
	current, _ := input.FS.(*gittree.FS)
	s := selection{
		repo:       repo,
		current:    current.OperationView(),
		input:      input,
		owner:      owner,
		network:    cfg.Network,
		transports: cfg.GitTransports,
	}
	r, err := s.resolve(ctx, location, base)
	if err != nil {
		return nil, err
	}
	content := struct {
		Root    string
		Remotes []string
	}{root, urls}
	s.base, s.content = r, content
	s.worktree, err = directory.Worktree(current)
	if err != nil {
		return nil, fmt.Errorf("worktree: %w", err)
	}
	before, selected, _, warnings, err := selectChanges(ctx, &s)
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
		return nil, fmt.Errorf("relative scan path: %w", err)
	}
	input.URL = ""
	input.Metadata = map[string]string{"baseline-selection": r.Source}
	_, origins, _ := selected.ForSource(manifest.BootstrapSourceID)
	origins[0].Warnings = baseWarnings
	return &Result{
		Source:       input,
		BaseInput:    origins[0],
		ScanPath:     location.ScanPath,
		BaseScanPath: filepath.Join(label, rel),
		Changes:      selected,
		SelfURLs:     urls,
	}, nil
}

// ResolvePaths admits both inputs and retains their canonical scan paths.
// Only an absent original repository selects the current repository's identity.
// The original path must be non-empty.
func ResolvePaths(ctx context.Context, current, original string) (*Result, error) {
	before, err := gitrepo.Locate(ctx, original, "")
	if err != nil {
		return nil, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("baseline: resolve baseline tree path: %w", err),
			"",
		)
	}
	if before.LookupErr != nil {
		return nil, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("baseline: locate baseline tree: %w", before.LookupErr),
			"",
		)
	}
	if before.ConfigErr != nil {
		return nil, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("baseline: read baseline tree remotes: %w", before.ConfigErr),
			"",
		)
	}
	after := before
	if current != original {
		after, err = gitrepo.Locate(ctx, current, "")
		if err != nil {
			return nil, manifest.NewError(
				manifest.ErrInput,
				fmt.Errorf("baseline: resolve working tree path: %w", err),
				"",
			)
		}
	}
	urls := before.URLs
	if before.Repository == nil {
		if after.LookupErr != nil {
			return nil, manifest.NewError(
				manifest.ErrInput,
				fmt.Errorf("baseline: locate working tree: %w", after.LookupErr),
				"",
			)
		}
		if after.ConfigErr != nil {
			return nil, manifest.NewError(
				manifest.ErrInput,
				fmt.Errorf("baseline: read working tree remotes: %w", after.ConfigErr),
				"",
			)
		}
		urls = after.URLs
	}
	baseDirectory, err := admitLocation(ctx, before, manifest.WorkScope{})
	if err != nil {
		return nil, err
	}
	_ = baseDirectory.Close()
	baseInput := baseDirectory.Artifact()
	input := baseInput
	if after.ContentRoot != before.ContentRoot {
		directory, err := admitLocation(ctx, after, manifest.WorkScope{})
		if err != nil {
			return nil, err
		}
		input = directory.Artifact()
		_ = directory.Close()
	} else if after.ScanPath != before.ScanPath {
		if _, err := os.Stat(after.ScanPath); err != nil {
			return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("--path %q: %w", after.ScanPath, err), "")
		}
	}
	return &Result{
		Source:       input,
		BaseInput:    baseInput,
		ScanPath:     after.ScanPath,
		BaseScanPath: before.ScanPath,
		SelfURLs:     urls,
	}, nil
}

func (s *selection) resolve(ctx context.Context, location *gitrepo.Location, base string) (*resolution, error) {
	repo := s.repo
	if base != "" {
		return s.resolveExplicit(ctx, location, base)
	}
	head, err := repo.Head()
	if err != nil {
		if errors.Is(err, plumbing.ErrReferenceNotFound) {
			return nil, manifest.NewError(
				manifest.ErrInput,
				fmt.Errorf(
					"baseline: HEAD has no commits to merge-base from; pass --base=<rev> to fetch the baseline from origin, or --path-orig=<dir>: %w",
					err,
				),
				"",
			)
		}
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("resolve HEAD: %w", err), "")
	}
	headCommit, err := repo.CommitObject(head.Hash())
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("load HEAD commit: %w", err), "")
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
		return nil, manifest.NewError(manifest.ErrInput, nil,
			"baseline: cannot compute merge-base — repo appears shallow (.git/shallow present); "+
				"in actions/checkout, set 'fetch-depth: 0', or locally run 'git fetch --unshallow', "+
				"or pass --base=<ref> / --path-orig=<dir> explicitly")
	}
	return nil, manifest.NewError(manifest.ErrInput, nil,
		"baseline: could not auto-detect — HEAD has no upstream, origin/HEAD is unset, "+
			"and origin/{main,master} are absent; pass --base=<ref> or --path-orig=<dir>")
}

func (s *selection) resolveExplicit(ctx context.Context, location *gitrepo.Location, base string) (*resolution, error) {
	repo := s.repo
	h, err := repo.ResolveRevision(plumbing.Revision(base))
	if err == nil {
		return &resolution{Hash: *h, Source: "explicit --base=" + base, Repo: repo}, nil
	}
	remote := git.DefaultRemoteName + "/" + base
	if h, localErr := repo.ResolveRevision(plumbing.Revision(remote)); localErr == nil {
		return &resolution{Hash: *h, Source: "explicit --base=" + base + " (via " + remote + ")", Repo: repo}, nil
	}
	url, ok := originURL(location.Config)
	if !ok {
		return nil, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf(
				"could not resolve --base=%q: not found locally or as origin/%s, and there is no origin remote to fetch it from: %w",
				base,
				base,
				err,
			),
			"",
		)
	}
	r, err := fetchBase(ctx, url, base, Config{Cache: s.owner, Network: s.network, GitTransports: s.transports})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf(
				"could not resolve --base=%q: not found locally or as origin/%s, and fetching it from origin %s failed: %w",
				base,
				base,
				url,
				err,
			),
			"",
		)
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
		return plumbing.ZeroHash, fmt.Errorf("commitobject: %w", err)
	}
	bases, err := headCommit.MergeBase(otherCommit)
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("mergebase: %w", err)
	}
	if len(bases) == 0 {
		return plumbing.ZeroHash, errors.New("no merge-base (disjoint histories)")
	}
	// Criss-cross merges produce >1 candidate base; go-git doesn't
	// guarantee a stable order. Pick the lexicographically-smallest
	// hash so two `flate diff` runs against the same repo pick the
	// same baseline — reproducibility is a stated guarantee.
	best := slices.MinFunc(bases, func(a, b *object.Commit) int {
		return a.Hash.Compare(b.Hash.Bytes())
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
