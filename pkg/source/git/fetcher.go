// Package git implements the source.Fetcher for KindGitRepository.
package git

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
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
func (f *Fetcher) Fetch(ctx context.Context, repo *manifest.GitRepository, retry sourcecache.RetryConfig) (*store.SourceArtifact, error) {
	if !f.ForceGeneric && repo.Provider != "" && repo.Provider != sourcev1.GitProviderGeneric {
		return nil, source.ErrUnsupportedProvider(repo.Named(), repo.Provider, sourcev1.GitProviderGeneric,
			"SecretRef-based credentials")
	}
	request, err := f.resolveTransport(repo)
	if err != nil {
		return nil, err
	}
	return f.fetch(ctx, request, retry)
}

func (f *Fetcher) resolveTransport(repo *manifest.GitRepository) (gitAcquisition, error) {
	secrets := source.SnapshotSecrets(f.Secrets)
	var sec *manifest.Secret
	if repo.SecretRef != nil && secrets != nil {
		sec = secrets(repo.Namespace, repo.SecretRef.Name)
	}
	auth, authErr := f.resolveAuth(repo, sec)
	if authErr != nil && !errors.Is(authErr, manifest.ErrMissingSecret) {
		return gitAcquisition{}, authErr
	}
	var skipTLS bool
	if repo.SecretRef != nil {
		endpoint, err := transport.NewEndpoint(repo.URL)
		if err != nil {
			return gitAcquisition{}, fmt.Errorf("%w: %s: parse git endpoint: %w", manifest.ErrInput, gitID(repo), err)
		}
		skipTLS = endpoint.Protocol == "ssh"
	}
	var inputs []source.TransportInput
	if repo.SecretRef != nil && !skipTLS && secrets != nil {
		inputs = append(inputs, source.TransportInput{Reference: repo.SecretRef, Use: source.TransportSecret, OptionalTLS: true, CAFallback: true})
	}
	if authErr == nil {
		inputs = append(inputs, source.TransportInput{Reference: repo.ProxySecretRef, Use: source.TransportProxy})
	}
	owner := repo.Named()
	resolved, err := source.ResolveTransport(secrets, owner, inputs)
	if authErr != nil && (err == nil || errors.Is(err, manifest.ErrMissingSecret)) {
		return gitAcquisition{}, authErr
	}
	if err != nil {
		return gitAcquisition{}, err
	}
	cert := resolved.Certificate()
	opts := git.CloneOptions{
		URL: repo.URL, NoCheckout: true, Auth: auth,
		Depth:    effectiveDepth(f.Depth, repo.Reference),
		CABundle: []byte(cert[2]), ClientCert: []byte(cert[0]), ClientKey: []byte(cert[1]),
	}
	proxy := resolved.ProxyComponents()
	if proxy[0] != "" {
		opts.ProxyOptions = transport.ProxyOptions{URL: proxy[0], Username: proxy[1], Password: proxy[2]}
	} else if target, err := url.Parse(repo.URL); err == nil && (target.Scheme == "http" || target.Scheme == "https") {
		selected, err := resolved.ProxyURL(target)
		if err != nil {
			return gitAcquisition{}, fmt.Errorf("%w: %s: resolve proxy: invalid proxy configuration", manifest.ErrInput, gitID(repo))
		}
		if selected != nil {
			opts.ProxyOptions.URL = selected.String()
		}
	}
	var material any
	if sec != nil || opts.ProxyOptions.URL != "" {
		var secretMaterial any
		if sec != nil {
			secretMaterial = [2]any{sec.Data, sec.StringData}
		}
		material = [2]any{secretMaterial, opts.ProxyOptions}
	}
	return gitAcquisition{spec: repo, options: opts, material: material}, nil
}

func (f *Fetcher) fetch(ctx context.Context, request gitAcquisition, retry sourcecache.RetryConfig) (*store.SourceArtifact, error) {
	repo, opts := request.spec, request.options
	if err := f.validateRepo(repo); err != nil {
		return nil, err
	}
	refLabel := "HEAD"
	if repo.Reference != nil {
		refLabel = cmp.Or(gitRefLabel(*repo.Reference), refLabel)
	}
	content := struct {
		Source, Transport any
		Depth             int
	}{repo.Content(effectiveRef(repo.Reference)), request.material, f.Depth}
	key := sourcecache.GitKey{Content: content, Ref: refLabel, RecurseSubmodules: repo.RecurseSubmodules}
	maxAge := repo.Interval.Duration
	if canUseCachedGitSlot(repo.Reference) {
		maxAge = sourcecache.Forever
	}
	ignore := ""
	if repo.Ignore != nil {
		ignore = *repo.Ignore
	}
	viewOptions := gittree.ViewOptions{Content: content, Sparse: repo.SparseCheckout, Ignore: ignore}
	var input *gittree.FS
	generation, id, err := f.Cache.Git(ctx, key, sourcecache.Policy{MaxAge: maxAge, Retry: retry}, func(ctx context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
		input = nil
		cloned, err := f.acquire(ctx, storer, worktree, request)
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

		input, _, err = generationView(ctx, &sourcecache.GitGeneration{Backing: storer.(*filesystem.Storage).Filesystem(), Worktree: worktree, Commit: hash}, viewOptions, nil)
		return hash, err
	})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", gitID(repo), err)
	}

	input, _, err = generationView(ctx, generation, viewOptions, input)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", manifest.ErrInput, gitID(repo), err)
	}

	return &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: repo.URL, Revision: generation.Commit.String(), FS: input, Key: id}, nil
}

type gitAcquisition struct {
	spec     *manifest.GitRepository
	options  git.CloneOptions
	material any
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

func generationView(ctx context.Context, generation *sourcecache.GitGeneration, options gittree.ViewOptions, validated *gittree.FS) (*gittree.FS, string, error) {
	if validated != nil {
		return gittree.New(ctx, nil, plumbing.ZeroHash, gittree.ViewOptions{Mode: gittree.Direct, Current: validated})
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
	if f.RestrictSchemes {
		endpoint, err := transport.NewEndpoint(repo.URL)
		if err != nil {
			return fmt.Errorf("%w: %s: parse git endpoint: %w", manifest.ErrInput, gitID(repo), err)
		}
		// Restricted scp-style remotes require an explicit user.
		ssh := endpoint.Protocol == "ssh" && (endpoint.User != "" || strings.HasPrefix(repo.URL, "ssh://"))
		if endpoint.Protocol != "https" && !ssh {
			return fmt.Errorf("%w: %s url %q uses a scheme not allowed under --restrict-egress (only https:// and ssh:// remotes are permitted)",
				manifest.ErrInput, gitID(repo), repo.URL)
		}
	}
	return nil
}

func gitRefLabel(ref manifest.GitRepositoryRef) string {
	if ref.Commit != "" && ref.Branch != "" {
		return "branch:" + ref.Branch + "@commit:" + ref.Commit
	}
	return manifest.GitRefString(ref)
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
