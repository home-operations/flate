// Package git implements the source.Fetcher for KindGitRepository.
package git

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"

	// HTTPS operations must retain flate's egress guard.
	_ "github.com/home-operations/flate/pkg/source/git/internal/gittransport"
	"github.com/home-operations/flate/pkg/source/gittree"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher acquires immutable Git generations through the common cache owner.
type Fetcher struct {
	Cache *sourcecache.Cache

	// Depth bounds history for refs that do not require reachability validation.
	Depth int
	// RestrictSchemes permits only HTTPS and SSH for untrusted inputs.
	RestrictSchemes bool
	fallback        sync.Map
}

// Fetch publishes an immutable source view only after acquisition succeeds.
type operation struct {
	*Fetcher
	reads   *store.ReadSet
	secrets source.SecretGetter
}

// Fetch acquires content using one execution's captured credentials and declarations.
func (f *Fetcher) Fetch(ctx context.Context, repo *manifest.GitRepository, reads *store.ReadSet, policy sourcecache.Policy) (*store.SourceArtifact, error) {
	operation := operation{Fetcher: f, reads: reads, secrets: source.SecretLookup(reads)}
	return operation.fetchSource(ctx, repo, policy)
}

func (f *operation) fetchSource(ctx context.Context, repo *manifest.GitRepository, policy sourcecache.Policy) (*store.SourceArtifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	request, err := f.resolveTransport(repo, policy)
	if err != nil {

		return nil, err
	}
	return f.fetch(ctx, request, policy)
}

func (f *operation) resolveTransport(repo *manifest.GitRepository, policy sourcecache.Policy) (gitAcquisition, error) {
	endpoint, err := f.validateRepo(repo)
	if err != nil {
		return gitAcquisition{}, err
	}
	secrets := f.secrets
	var sec *manifest.Secret
	if repo.SecretRef != nil && secrets != nil {
		sec = secrets(repo.Namespace, repo.SecretRef.Name)
	}
	auth, authErr := f.resolveAuth(repo, sec, endpoint)
	if authErr != nil && !errors.Is(authErr, manifest.ErrMissingSecret) {
		return gitAcquisition{}, authErr
	}
	skipTLS := endpoint.Protocol == "ssh"
	var inputs []source.TransportInput
	if repo.SecretRef != nil && !skipTLS && secrets != nil {
		inputs = append(inputs, source.TransportInput{Reference: repo.SecretRef, Use: source.TransportSecret, OptionalTLS: true, CAFallback: true})
	}
	inputs = append(inputs, source.TransportInput{Reference: repo.ProxySecretRef, Use: source.TransportProxy})
	owner := manifest.DiagnosticFor(repo.Named(), repo)
	resolved, err := source.ResolveTransport(secrets, owner, inputs)
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
			return gitAcquisition{}, fmt.Errorf("%w: %s: resolve proxy: invalid proxy configuration", manifest.ErrInput, manifest.DiagnosticFor(repo.Named(), repo).Label())
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
	if authErr == nil && auth == nil && repo.Provider == sourcev1.GitProviderGitHub {
		authErr = source.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "", "provider authentication unavailable offline")
	}
	request := gitAcquisition{spec: repo, options: opts, material: material}
	if authErr != nil && repo.Provider != sourcev1.GitProviderGitHub && !skipTLS {
		missing, _ := errors.AsType[*source.MissingSecretError](authErr)
		missing.RetryAmbient = func(ctx context.Context) (*store.SourceArtifact, error) {
			return source.AcquireMachine(ctx, policy, func(context.Context) error { return nil }, func(ctx context.Context, policy sourcecache.Policy) (*store.SourceArtifact, error) {
				return f.fetch(ctx, request, policy)
			})
		}
	}
	if authErr != nil && skipTLS && repo.Provider != sourcev1.GitProviderGitHub {
		missing, _ := errors.AsType[*source.MissingSecretError](authErr)
		timeout := repo.OperationTimeout()
		policy.Timeout = &timeout
		missing.RetryAmbient = func(ctx context.Context) (*store.SourceArtifact, error) {
			user := cmp.Or(endpoint.User, "git")
			prepared := request
			var conn net.Conn
			defer func() {
				if conn != nil {
					_ = conn.Close()
				}
			}()
			return source.AcquireMachine(ctx, policy, func(ctx context.Context) error {
				socket := os.Getenv("SSH_AUTH_SOCK")
				if socket == "" {
					return source.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "", "SSH agent unavailable")
				}
				var dialer net.Dialer
				var err error
				conn, err = dialer.DialContext(ctx, "unix", socket)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
						return source.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "", "SSH agent unavailable")
					}
					return err
				}
				stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
				defer stop()
				signers, err := agent.NewClient(conn).Signers()
				if err != nil {
					return err
				}
				if len(signers) == 0 {
					return source.MissingSecretErr(manifest.DiagnosticFor(repo.Named(), repo), "", "SSH agent has no identities")
				}
				homeDir, err := os.UserHomeDir()
				if err != nil {
					return err
				}
				callback, err := gitssh.NewKnownHostsCallback(filepath.Join(homeDir, ".ssh", "known_hosts"))
				if err != nil {
					return err
				}
				auth := &gitssh.PublicKeysCallback{User: user, Callback: func() ([]ssh.Signer, error) { return signers, nil }}
				auth.HostKeyCallback = callback
				prepared.options.Auth = auth
				keys := make([]string, len(signers))
				for i, signer := range signers {
					keys[i] = string(ssh.MarshalAuthorizedKey(signer.PublicKey()))
				}
				slices.Sort(keys)
				prepared.material = [3]any{request.material, user, keys}
				return nil
			}, func(ctx context.Context, policy sourcecache.Policy) (*store.SourceArtifact, error) {
				stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
				defer stop()
				return f.fetch(ctx, prepared, policy)
			})
		}
	}
	return request, authErr
}

func (f *Fetcher) fetch(ctx context.Context, request gitAcquisition, policy sourcecache.Policy) (*store.SourceArtifact, error) {
	repo := request.spec
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
	timeout := repo.OperationTimeout()
	policy.Timeout = &timeout
	var input *gittree.FS
	var identity string
	generation, id, err := f.Cache.Git(ctx, key, policy, func(ctx context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
		input = nil
		cloned, err := f.acquire(ctx, storer, worktree, request)
		if err != nil {
			return plumbing.ZeroHash, errors.Join(ctx.Err(), err)
		}
		hash, err := resolveRefHash(cloned, repo.Reference, true)
		if err != nil {
			return plumbing.ZeroHash, err
		}
		if worktree != nil {
			if err := checkoutRef(cloned, hash, repo.SparseCheckout); err != nil {
				return plumbing.ZeroHash, err
			}
			if err := f.updateSubmodules(ctx, cloned, request.options.Auth, git.DefaultSubmoduleRecursionDepth); err != nil {
				return plumbing.ZeroHash, errors.Join(ctx.Err(), err)
			}
		}

		viewOptions.Worktree = worktree
		input, identity, err = gittree.New(ctx, storer.(*filesystem.Storage).Filesystem(), hash, viewOptions)
		if err != nil {
			return plumbing.ZeroHash, fmt.Errorf("%w: validate git view: %w", manifest.ErrInput, err)
		}
		return hash, nil
	})
	if err != nil {
		return gitWarningArtifact(input, repo.Named(), repo.URL, refLabel), fmt.Errorf("%s: %w", manifest.DiagnosticFor(repo.Named(), repo).Label(), err)
	}

	viewOptions.Worktree = generation.Worktree
	if input == nil {
		input, identity, err = gittree.New(ctx, generation.Backing, generation.Commit, viewOptions)
	}
	if err != nil {
		return gitWarningArtifact(input, repo.Named(), repo.URL, refLabel), fmt.Errorf("%w: %s: %w", manifest.ErrInput, manifest.DiagnosticFor(repo.Named(), repo).Label(), err)
	}

	warnings := gitWarnings(input, repo.Named(), repo.URL, refLabel)
	return &store.SourceArtifact{Kind: manifest.KindGitRepository, URL: repo.URL, Revision: generation.Commit.String(), FS: input, Key: id, Content: identity, Warnings: warnings}, nil
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
	timeout  time.Duration
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
	fetchRef := func(repo *git.Repository) error {
		fetchCtx := ctx
		if request.timeout > 0 {
			var cancel context.CancelFunc
			fetchCtx, cancel = context.WithTimeout(ctx, request.timeout)
			defer cancel()
		}
		return repo.FetchContext(fetchCtx, fetch)
	}

	if kind == "commit" && ref.Branch == "" {
		repo, err := initRemote(storer, worktree, options.URL)
		if err != nil {
			return nil, err
		}
		fetch.Depth, fetch.RefSpecs = 1, []config.RefSpec{config.RefSpec("+" + value + ":refs/flate/pin")}
		err = fetchRef(repo)
		if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, transport.ErrAuthenticationRequired) || errors.Is(err, transport.ErrAuthorizationFailed) {
				return nil, err
			}
			if _, loaded := f.fallback.LoadOrStore(options.URL, struct{}{}); !loaded {
				slog.Debug("git: exact commit fetch refused; fetching full history", "url", options.URL)
			}
			fetch.Depth, fetch.RefSpecs = 0, []config.RefSpec{"+refs/*:refs/*"}
			if err := fetchRef(repo); err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
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
		cloneCtx := ctx
		var cancel context.CancelFunc
		if request.timeout > 0 {
			cloneCtx, cancel = context.WithTimeout(ctx, request.timeout)
		}
		repo, err := git.CloneContext(cloneCtx, storer, worktree, &options)
		if cancel != nil {
			cancel()
		}
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
		err = fetchRef(repo)
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
	err = fetchRef(repo)
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

func (f *Fetcher) validateRepo(repo *manifest.GitRepository) (*transport.Endpoint, error) {
	if repo == nil {
		return nil, errors.New("git repository is nil")
	}
	if repo.URL == "" {
		return nil, fmt.Errorf("%w: %s missing url", manifest.ErrInput, manifest.DiagnosticFor(repo.Named(), repo).Label())
	}
	endpoint, err := transport.NewEndpoint(repo.URL)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: parse git endpoint: %w", manifest.ErrInput, manifest.DiagnosticFor(repo.Named(), repo).Label(), err)
	}
	if f.RestrictSchemes {
		// Restricted scp-style remotes require an explicit user.
		ssh := endpoint.Protocol == "ssh" && (endpoint.User != "" || strings.HasPrefix(repo.URL, "ssh://"))
		if endpoint.Protocol != "https" && !ssh {
			return nil, fmt.Errorf("%w: %s url %q uses a scheme not allowed under --restrict-egress (only https:// and ssh:// remotes are permitted)",
				manifest.ErrInput, manifest.DiagnosticFor(repo.Named(), repo).Label(), repo.URL)
		}
	}
	return endpoint, nil
}
