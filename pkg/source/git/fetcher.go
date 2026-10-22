// Package git implements the source.Fetcher for KindGitRepository.
package git

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
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

// Fetch returns an immutable source view after acquisition succeeds.
// An error may retain warnings without usable content.
func (f *Fetcher) Fetch(ctx context.Context, repo *manifest.GitRepository, policy sourcecache.Policy) (*store.SourceArtifact, error) {
	if !f.ForceGeneric && repo.Provider != "" && repo.Provider != sourcev1.GitProviderGeneric {
		return nil, source.ErrUnsupportedProvider(repo.Named(), repo.Provider, sourcev1.GitProviderGeneric,
			"SecretRef-based credentials")
	}
	request, err := f.resolveTransport(repo)
	if err != nil {
		return nil, err
	}
	return f.fetch(ctx, request, policy)
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
			return gitAcquisition{}, fmt.Errorf("%w: %s: parse git endpoint: %w", manifest.ErrInput, source.QualifiedName(manifest.KindGitRepository, repo.Namespace, repo.Name), err)
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
		Depth:    f.Depth,
		CABundle: []byte(cert[2]), ClientCert: []byte(cert[0]), ClientKey: []byte(cert[1]),
	}
	if kind, _ := (*manifest.GitRepositoryRef)(repo.Reference).Effective(); opts.Depth > 0 && kind == "commit" {
		opts.Depth = 0
	}
	proxy := resolved.ProxyComponents()
	if proxy[0] != "" {
		opts.ProxyOptions = transport.ProxyOptions{URL: proxy[0], Username: proxy[1], Password: proxy[2]}
	} else if target, err := url.Parse(repo.URL); err == nil && (target.Scheme == "http" || target.Scheme == "https") {
		selected, err := resolved.ProxyURL(target)
		if err != nil {
			return gitAcquisition{}, fmt.Errorf("%w: %s: resolve proxy: invalid proxy configuration", manifest.ErrInput, source.QualifiedName(manifest.KindGitRepository, repo.Namespace, repo.Name))
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

func (f *Fetcher) fetch(ctx context.Context, request gitAcquisition, policy sourcecache.Policy) (*store.SourceArtifact, error) {
	repo, opts := request.spec, request.options
	if err := f.validateRepo(repo); err != nil {
		return nil, err
	}
	ref := (*manifest.GitRepositoryRef)(repo.Reference)
	refLabel := cmp.Or(ref.Label(), "HEAD")
	content := struct {
		Source, Transport any
		Depth             int
	}{repo.Content(ref.Normalized()), request.material, f.Depth}
	key := sourcecache.GitKey{Content: content, Ref: refLabel, RecurseSubmodules: repo.RecurseSubmodules}
	if ref.Pinned() {
		policy.MaxAge = sourcecache.Forever
	}
	ignore := ""
	if repo.Ignore != nil {
		ignore = *repo.Ignore
	}
	viewOptions := gittree.ViewOptions{Content: content, Sparse: repo.SparseCheckout, Ignore: ignore}
	var input *gittree.FS
	generation, id, err := f.Cache.Git(ctx, key, policy, func(ctx context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
		input = nil
		cloned, err := f.acquire(ctx, storer, worktree, request)
		if err != nil {
			return plumbing.ZeroHash, err
		}
		hash, err := resolveRefHash(cloned, repo.Reference, true)
		if err != nil {
			return plumbing.ZeroHash, err
		}
		if worktree != nil {
			if err := checkoutRef(cloned, hash, repo.SparseCheckout); err != nil {
				return plumbing.ZeroHash, err
			}
			if err := updateSubmodules(ctx, cloned, opts.Auth); err != nil {
				return plumbing.ZeroHash, err
			}
		}

		viewOptions.Worktree = worktree
		input, _, err = gittree.New(ctx, storer.(*filesystem.Storage).Filesystem(), hash, viewOptions)
		if err != nil {
			return plumbing.ZeroHash, fmt.Errorf("%w: validate git view: %w", manifest.ErrInput, err)
		}
		return hash, nil
	})
	if err != nil {
		return gitWarningArtifact(input, repo.Named(), repo.URL, refLabel), fmt.Errorf("%s: %w", source.QualifiedName(manifest.KindGitRepository, repo.Namespace, repo.Name), err)
	}

	viewOptions.Worktree = generation.Worktree
	if input == nil {
		input, _, err = gittree.New(ctx, generation.Backing, generation.Commit, viewOptions)
	}
	if err != nil {
		return gitWarningArtifact(input, repo.Named(), repo.URL, refLabel), fmt.Errorf("%w: %s: %w", manifest.ErrInput, source.QualifiedName(manifest.KindGitRepository, repo.Namespace, repo.Name), err)
	}

	return &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: repo.URL, Revision: generation.Commit.String(), FS: input, Key: id, Warnings: gitWarnings(input, repo.Named(), repo.URL, refLabel)}, nil
}

func gitWarningArtifact(input *gittree.FS, id manifest.NamedResource, repository, ref string) *store.SourceArtifact {
	warnings := gitWarnings(input, id, repository, ref)
	if len(warnings) == 0 {
		return nil
	}
	return &store.SourceArtifact{Warnings: warnings}
}

func gitWarnings(input *gittree.FS, id manifest.NamedResource, repository, ref string) []manifest.Warning {
	if input == nil || len(input.Warnings) == 0 {
		return nil
	}
	var coordinates []string
	if endpoint, err := transport.NewEndpoint(repository); err == nil {
		endpoint.User, endpoint.Password = "", ""
		if address, err := url.Parse(endpoint.String()); err == nil {
			address.RawQuery, address.Fragment = "", ""
			coordinates = append(coordinates, address.String())
		}
	}
	if ref != "" {
		coordinates = append(coordinates, ref)
	}
	warnings := slices.Clone(input.Warnings)
	for i := range warnings {
		warnings[i].Resource = id
		warnings[i].Detail = append(slices.Clone(warnings[i].Detail), coordinates...)
	}
	return warnings
}

type gitAcquisition struct {
	spec     *manifest.GitRepository
	options  git.CloneOptions
	material any
}

func (f *Fetcher) acquire(ctx context.Context, storer storage.Storer, worktree billy.Filesystem, request gitAcquisition) (*git.Repository, error) {
	ref := (*manifest.GitRepositoryRef)(request.spec.Reference)
	kind, value := ref.Effective()
	options := request.options
	fetch := &git.FetchOptions{
		Auth: options.Auth, ProxyOptions: options.ProxyOptions, CABundle: options.CABundle,
		ClientCert: options.ClientCert, ClientKey: options.ClientKey,
		Depth: options.Depth, Tags: git.NoTags,
	}
	if kind == "commit" && ref.Branch == "" {
		repo, err := initRemote(storer, worktree, options.URL)
		if err != nil {
			return nil, err
		}
		fetch.Depth, fetch.RefSpecs = 1, []config.RefSpec{config.RefSpec("+" + value + ":refs/flate/pin")}
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
	var specs []config.RefSpec
	if spec := ref.RefSpec(); spec != "" {
		specs = []config.RefSpec{spec}
	}
	if len(specs) == 0 || worktree != nil {
		repo, err := git.CloneContext(ctx, storer, worktree, &options)
		if err != nil {
			return nil, err
		}
		switch kind {
		case "name":
			specs = []config.RefSpec{ref.RefSpec()}
		case "commit":
			specs = []config.RefSpec{"+refs/*:refs/*"}
		default:
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

func (f *Fetcher) validateRepo(repo *manifest.GitRepository) error {
	if repo == nil {
		return errors.New("git repository is nil")
	}
	if repo.URL == "" {
		return fmt.Errorf("%w: %s missing url", manifest.ErrInput, source.QualifiedName(manifest.KindGitRepository, repo.Namespace, repo.Name))
	}
	if f.RestrictSchemes {
		endpoint, err := transport.NewEndpoint(repo.URL)
		if err != nil {
			return fmt.Errorf("%w: %s: parse git endpoint: %w", manifest.ErrInput, source.QualifiedName(manifest.KindGitRepository, repo.Namespace, repo.Name), err)
		}
		// Restricted scp-style remotes require an explicit user.
		ssh := endpoint.Protocol == "ssh" && (endpoint.User != "" || strings.HasPrefix(repo.URL, "ssh://"))
		if endpoint.Protocol != "https" && !ssh {
			return fmt.Errorf("%w: %s url %q uses a scheme not allowed under --restrict-egress (only https:// and ssh:// remotes are permitted)",
				manifest.ErrInput, source.QualifiedName(manifest.KindGitRepository, repo.Namespace, repo.Name), repo.URL)
		}
	}
	return nil
}
