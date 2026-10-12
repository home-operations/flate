// Package git implements the source.Fetcher for KindGitRepository.
package git

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	// HTTPS operations must retain flate's header timeout and egress guard.
	_ "github.com/home-operations/flate/pkg/source/git/internal/gittransport"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/source/sourceignore"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher acquires immutable Git generations through the common cache owner.
type Fetcher struct {
	Cache   *sourcecache.Cache
	Secrets source.SecretGetter
	// Depth bounds history for refs that do not require reachability validation.
	Depth int
	// RestrictSchemes permits only HTTPS and SSH for untrusted inputs.
	RestrictSchemes bool
	// ForceGeneric uses supplied static credentials for non-generic providers.
	ForceGeneric bool
	fallback     sync.Map
}

// Fetch publishes an immutable source view only after acquisition succeeds.
func (f *Fetcher) Fetch(ctx context.Context, repo *manifest.GitRepository) (*store.SourceArtifact, error) {
	if !f.ForceGeneric && repo.Provider != "" && repo.Provider != sourcev1.GitProviderGeneric {
		return nil, source.ErrUnsupportedProvider("GitRepository",
			repo.Namespace, repo.Name, repo.Provider, sourcev1.GitProviderGeneric,
			"SecretRef-based credentials")
	}
	opts, err := f.resolveTransport(repo)
	if err != nil {
		return nil, err
	}
	return f.fetch(ctx, repo, opts)
}

func (f *Fetcher) resolveTransport(repo *manifest.GitRepository) (git.CloneOptions, error) {
	auth, err := f.resolveAuth(repo)
	if err != nil {
		return git.CloneOptions{}, err
	}
	opts, err := f.resolveTLS(repo)
	if err != nil {
		return git.CloneOptions{}, err
	}
	proxy, err := source.ResolveProxy(f.Secrets, repo.Namespace, "GitRepository",
		repo.Namespace+"/"+repo.Name, repo.ProxySecretRef)
	if err != nil {
		return git.CloneOptions{}, err
	}
	opts.URL, opts.NoCheckout, opts.Auth = repo.URL, true, auth
	opts.Depth = effectiveDepth(f.Depth, repo.Reference)
	if proxy != nil {
		if _, err := proxy.URL(); err != nil {
			return git.CloneOptions{}, fmt.Errorf("%w: %s: proxySecretRef %s/%s: invalid proxy address",
				manifest.ErrInput, gitID(repo), repo.Namespace, repo.ProxySecretRef.Name)
		}
		opts.ProxyOptions = transport.ProxyOptions{URL: proxy.Address, Username: proxy.Username, Password: proxy.Password}
	}
	return opts, nil
}

func (f *Fetcher) fetch(ctx context.Context, repo *manifest.GitRepository, opts git.CloneOptions) (*store.SourceArtifact, error) {
	if err := f.validateRepo(repo); err != nil {
		return nil, err
	}
	refLabel := "HEAD"
	if repo.Reference != nil {
		refLabel = cmp.Or(gitRefLabel(*repo.Reference), refLabel)
	}
	h, err := source.CacheKeyHash([]any{repo.URL, authIdentity(repo), gitCacheKey(repo, refLabel, sourceignore.RulesVersion), f.Depth}, 32)
	if err != nil {
		return nil, err
	}
	key := sourcecache.GitKey{ID: "source-git-" + h, Ref: refLabel, RecurseSubmodules: repo.RecurseSubmodules}
	maxAge := repo.Interval.Duration
	if canUseCachedGitSlot(repo.Reference) {
		maxAge = sourcecache.Forever
	}
	ignore := ""
	if repo.Ignore != nil {
		ignore = *repo.Ignore
	}
	viewOptions := gittree.ViewOptions{Sparse: repo.SparseCheckout, Ignore: ignore}
	var input *gittree.FS
	generation, err := f.Cache.Git(ctx, key, maxAge, func(storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
		cloned, err := f.acquire(ctx, storer, worktree, gitAcquisition{repo, opts})
		if err != nil {
			return plumbing.ZeroHash, err
		}
		hash, err := resolveRefHash(cloned, repo.Reference)
		if err != nil {
			return plumbing.ZeroHash, err
		}
		if worktree != nil {
			ref := effectiveRef(repo.Reference)
			if err := checkoutRef(cloned, ref, repo.SparseCheckout); err != nil {
				return plumbing.ZeroHash, err
			}
			if err := updateSubmodules(ctx, cloned, opts.Auth); err != nil {
				return plumbing.ZeroHash, err
			}
		}
		input, err = generationView(ctx, &sourcecache.GitGeneration{Backing: storer.(*filesystem.Storage).Filesystem(), Worktree: worktree, Commit: hash}, viewOptions, nil)
		return hash, err
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", gitID(repo), err)
	}
	input, err = generationView(ctx, generation, viewOptions, input)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", manifest.ErrInput, gitID(repo), err)
	}

	return &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: repo.URL, Revision: generation.Commit.String(), FS: input, Key: key.ID + "@" + generation.Commit.String()}, nil
}

type gitAcquisition struct {
	spec    *manifest.GitRepository
	options git.CloneOptions
}

func (f *Fetcher) acquire(ctx context.Context, storer storage.Storer, worktree billy.Filesystem, request gitAcquisition) (*git.Repository, error) {
	ref := request.spec.Reference
	options := request.options
	fetch := &git.FetchOptions{
		Auth: options.Auth, ProxyOptions: options.ProxyOptions, CABundle: options.CABundle,
		ClientCert: options.ClientCert, ClientKey: options.ClientKey,
		Depth: options.Depth, Tags: git.NoTags,
	}
	if canUseCachedGitSlot(ref) || ref != nil && ref.Commit != "" && ref.Branch == "" {
		repo, err := initRemote(storer, worktree, options.URL)
		if err != nil {
			return nil, err
		}
		fetch.Depth, fetch.RefSpecs = 1, []config.RefSpec{config.RefSpec("+" + ref.Commit + ":refs/flate/pin")}
		err = repo.FetchContext(ctx, fetch)
		if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, transport.ErrAuthenticationRequired) || errors.Is(err, transport.ErrAuthorizationFailed) {
				return nil, err
			}
			if _, loaded := f.fallback.LoadOrStore(options.URL, struct{}{}); !loaded {
				slog.Debug("git: exact commit fetch refused; fetching full history", "url", options.URL, "error", err)
			}
			fetch.Depth, fetch.RefSpecs = 0, []config.RefSpec{"+refs/*:refs/*"}
			if err := repo.FetchContext(ctx, fetch); err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
				return nil, err
			}
		}
		return repo, nil
	}
	if request.spec.RecurseSubmodules {
		options.RecurseSubmodules = git.DefaultSubmoduleRecursionDepth
		options.Depth, fetch.Depth = 0, 0
	}
	specs := refSpecs(ref)
	if len(specs) == 0 || worktree != nil {
		repo, err := git.CloneContext(ctx, storer, worktree, &options)
		if err != nil {
			return nil, err
		}
		if ref != nil && ref.Name != "" && ref.Commit == "" {
			specs = []config.RefSpec{config.RefSpec("+" + ref.Name + ":" + ref.Name)}
		} else if ref != nil && ref.Commit != "" {
			specs = []config.RefSpec{"+refs/*:refs/*"}
		} else {
			return repo, nil
		}
		fetch.Depth, fetch.RefSpecs = 0, specs
		err = repo.FetchContext(ctx, fetch)
		if errors.Is(err, git.NoErrAlreadyUpToDate) {
			err = nil
		}
		return repo, err
	}
	repo, err := initRemote(storer, worktree, options.URL)
	if err != nil {
		return nil, err
	}
	fetch.RefSpecs = specs
	err = repo.FetchContext(ctx, fetch)
	if errors.Is(err, git.NoErrAlreadyUpToDate) {
		err = nil
	}
	return repo, err
}

func initRemote(storer storage.Storer, worktree billy.Filesystem, url string) (*git.Repository, error) {
	repo, err := git.Init(storer, worktree)
	if err != nil {
		return nil, err
	}
	_, err = repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{url}})
	return repo, err
}

func generationView(ctx context.Context, generation *sourcecache.GitGeneration, options gittree.ViewOptions, validated *gittree.FS) (*gittree.FS, error) {
	if validated != nil {
		return validated, nil
	}
	options.Worktree = generation.Worktree
	return gittree.New(ctx, generation.Backing, generation.Commit, options)
}

func refSpecs(ref *manifest.GitRepositoryRef) []config.RefSpec {
	if ref == nil || ref.Commit != "" {
		return nil
	}
	var name string
	switch {
	case ref.Name != "":
		name = ref.Name
	case ref.SemVer != "":
		name = "refs/tags/*"
	case ref.Tag != "":
		name = "refs/tags/" + ref.Tag
	case ref.Branch != "":
		name = "refs/heads/" + ref.Branch
	default:
		return nil
	}
	return []config.RefSpec{config.RefSpec("+" + name + ":" + name)}
}

func (f *Fetcher) validateRepo(repo *manifest.GitRepository) error {
	if repo == nil {
		return errors.New("git repository is nil")
	}
	if repo.URL == "" {
		return fmt.Errorf("%w: %s missing url", manifest.ErrInput, gitID(repo))
	}
	if f.RestrictSchemes && !gitSchemeAllowed(repo.URL) {
		return fmt.Errorf("%w: %s url %q uses a scheme not allowed under --restrict-egress (only https:// and ssh:// remotes are permitted)",
			manifest.ErrInput, gitID(repo), repo.URL)
	}
	return nil
}

// gitSchemeAllowed reports whether url uses a remote transport safe for an
// untrusted render: https, ssh, or scp-style (user@host:path → SSH). It
// blocks file:// (local repo read, #743), git:// (plaintext), http://, and
// bare filesystem paths.
func gitSchemeAllowed(url string) bool {
	if strings.HasPrefix(url, "https://") || strings.HasPrefix(url, "ssh://") {
		return true
	}
	// scp-style git@host:path carries no "://" scheme: a ':' before any '/'
	// with a '@' in the host part. go-git routes this over SSH.
	if i := strings.IndexAny(url, ":/"); i >= 0 && url[i] == ':' && strings.Contains(url[:i], "@") {
		return true
	}
	return false
}

func gitRefLabel(ref manifest.GitRepositoryRef) string {
	if ref.Commit != "" && ref.Branch != "" {
		return "branch:" + ref.Branch + "@commit:" + ref.Commit
	}
	return manifest.GitRefString(ref)
}

func gitCacheKey(repo *manifest.GitRepository, refLabel, rulesVersion string) string {
	ignore := ""
	if repo.Ignore != nil {
		ignore = *repo.Ignore
	}
	payload := struct {
		Ref               string   `json:"ref"`
		Ignore            string   `json:"ignore,omitempty"`
		SparseCheckout    []string `json:"sparseCheckout,omitempty"`
		RecurseSubmodules bool     `json:"recurseSubmodules,omitempty"`
		RulesVersion      string   `json:"rulesVersion"`
	}{
		Ref:               refLabel,
		Ignore:            ignore,
		SparseCheckout:    repo.SparseCheckout,
		RecurseSubmodules: repo.RecurseSubmodules,
		RulesVersion:      rulesVersion,
	}
	h, _ := source.CacheKeyHash(payload, 32)
	return refLabel + "#opts:" + h
}

func canUseCachedGitSlot(ref *manifest.GitRepositoryRef) bool {
	return ref != nil &&
		ref.Commit != "" &&
		ref.Branch == "" &&
		ref.Tag == "" &&
		ref.SemVer == "" &&
		ref.Name == ""
}

func effectiveDepth(depth int, ref *manifest.GitRepositoryRef) int {
	if depth > 0 && ref != nil && ref.Commit != "" {
		return 0
	}
	return depth
}

// gitID is the "GitRepository <namespace>/<name>" error prefix, via the shared
// source.QualifiedName so every fetcher formats kind/ns/name identically.
func gitID(repo *manifest.GitRepository) string {
	return source.QualifiedName("GitRepository", repo.Namespace, repo.Name)
}

// authIdentity returns the cache-key auth tag for a GitRepository.
// Combines the SecretRef (HTTPS / SSH creds) and ProxySecretRef the
// fetcher binds. Returns "" for anonymous clones so they share slots
// with the legacy un-auth-keyed layout.
func authIdentity(repo *manifest.GitRepository) string {
	return source.AuthIdentityFromRefs(repo.Namespace,
		repo.SecretRef, repo.ProxySecretRef)
}
