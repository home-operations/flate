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
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	sourcecache "github.com/home-operations/flate/pkg/source/cache"

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

	work manifest.AcquisitionWork

	reads   *store.ReadSet
	secrets source.SecretGetter
}

// Fetch acquires content using one execution's captured credentials and declarations.
func (f *Fetcher) Fetch(
	ctx context.Context,
	repo *manifest.GitRepository,
	reads *store.ReadSet,
	policyValue *sourcecache.Policy,
) (source.PreparedFetch, error) {
	policy := *policyValue
	innerOperation := operation{
		Fetcher: f,
		reads:   reads,
		secrets: source.SecretLookup(reads),
	}
	return innerOperation.fetchSource(ctx, repo, &policy)
}

func (f *operation) fetchSource(
	ctx context.Context,
	repo *manifest.GitRepository,
	policyValue *sourcecache.Policy,
) (source.PreparedFetch, error) {
	policy := *policyValue
	if err := ctx.Err(); err != nil {
		return source.PreparedFetch{}, err //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	f.work.Scope = f.reads.Work().Scope()
	if policy.Work != nil {
		f.work.Scope = policy.Work.Attribution()
	}
	if repo != nil {
		f.work.Scope = f.work.Scope.Source(repo.Named())
	}
	policy.Work = &f.work
	work := f.work.Scope.Record(manifest.WorkValidation, "")
	work.Add(&manifest.WorkCounts{Attempts: 1})
	request, err := f.resolveTransport(repo, &policy)
	work.Finish(err)
	if err != nil {
		return source.PreparedFetch{}, err
	}
	return source.PreparedFetch{
		Fetch: func(ctx context.Context, local *store.SourceArtifact) (*store.SourceArtifact, error) {
			return request.complete(ctx, f.Fetcher, &policy, local)
		},
	}, nil
}

func (f *operation) resolveTransport(
	repo *manifest.GitRepository,
	policyValue *sourcecache.Policy,
) (gitAcquisition, error) {
	policy := *policyValue
	endpoint, err := f.validateRepo(repo, true)
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
	skipTLS := endpoint.Protocol == sshScheme
	var inputs []source.TransportInput
	if repo.SecretRef != nil && !skipTLS && secrets != nil {
		inputs = append(
			inputs,
			source.TransportInput{
				Reference:   repo.SecretRef,
				Use:         source.TransportSecret,
				OptionalTLS: true,
				CAFallback:  true,
			},
		)
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
	if reference := (*manifest.GitRepositoryRef)(
		repo.Reference,
	).Effective(); opts.Depth > 0 &&
		reference.Kind == commitReference {
		opts.Depth = 0
	}
	opts.ProxyOptions, err = gitProxyOptions(repo.URL, &resolved)
	if err != nil {
		return gitAcquisition{}, err
	}
	var secretMaterial any
	if sec != nil {
		secretMaterial = [2]any{sec.Data, sec.StringData}
	}
	var material any
	if sec != nil || opts.ProxyOptions.URL != "" {
		material = [2]any{secretMaterial, opts.ProxyOptions}
	}
	if authErr == nil && auth == nil && repo.Provider == sourcev1.GitProviderGitHub {
		authErr = source.MissingSecretErr(
			manifest.DiagnosticFor(repo.Named(), repo),
			"",
			"provider authentication unavailable offline",
		)
	}
	request := gitAcquisition{work: &f.work, protocol: endpoint.Protocol, spec: repo, options: opts, material: material}
	if authErr == nil || repo.Provider == sourcev1.GitProviderGitHub {
		return request, authErr
	}
	missing, _ := errors.AsType[*source.MissingSecretError](authErr)
	if skipTLS {
		timeout := repo.OperationTimeout()
		policy.Timeout = &timeout
		missing.RetryAmbient = func(ctx context.Context) (source.PreparedFetch, error) {
			return f.retrySSH(ctx, &request, &policy, endpoint)
		}
		return request, authErr
	}
	missing.RetryAmbient = func(ctx context.Context) (source.PreparedFetch, error) {
		prepared, err := source.AcquireMachine(
			ctx,
			&policy,
			func(context.Context) error { return nil },
			func(ctx context.Context, policyValue *sourcecache.Policy, local *store.SourceArtifact) (*store.SourceArtifact, error) {
				policy := *policyValue
				return request.complete(ctx, f.Fetcher, &policy, local)
			},
		)
		prepared.LocalUnavailable = missing
		return prepared, err
	}
	return request, authErr
}

func gitProxyOptions(repoURL string, resolved *source.TransportMaterial) (transport.ProxyOptions, error) {
	proxy := resolved.ProxyComponents()
	if proxy[0] != "" {
		return transport.ProxyOptions{URL: proxy[0], Username: proxy[1], Password: proxy[2]}, nil
	}
	target, err := url.Parse(repoURL)
	if err != nil || target.Scheme != "http" && target.Scheme != "https" {
		return transport.ProxyOptions{}, nil //nolint:nilerr // Non-HTTP go-git endpoint syntaxes bypass HTTP proxy selection.
	}
	selected, err := resolved.ProxyURL(target)
	if err != nil {
		return transport.ProxyOptions{}, manifest.NewError(
			manifest.ErrInput,
			nil,
			"resolve proxy: invalid proxy configuration",
		)
	}
	var options transport.ProxyOptions
	if selected != nil {
		options.URL = selected.String()
	}
	return options, nil
}

type gitAcquisition struct {
	work     *manifest.AcquisitionWork
	protocol string
	timeout  time.Duration
	spec     *manifest.GitRepository
	options  git.CloneOptions
	material any
}

func (r *gitAcquisition) complete(
	ctx context.Context,
	f *Fetcher,
	policyValue *sourcecache.Policy,
	local *store.SourceArtifact,
) (*store.SourceArtifact, error) {
	policy := *policyValue
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("acquire git: %w", err)
	}
	if local != nil {
		return local, nil
	}
	if _, err := f.validateRepo(r.spec, false); err != nil {
		return nil, err
	}
	return f.fetch(ctx, r, &policy)
}

func (f *Fetcher) fetch(
	ctx context.Context, requestValue *gitAcquisition,
	policyValue *sourcecache.Policy,
) (*store.SourceArtifact, error) {
	request, policy := *requestValue, *policyValue
	repo := request.spec
	ref := (*manifest.GitRepositoryRef)(repo.Reference)
	refLabel := cmp.Or(ref.Label(), "HEAD")
	content := &gitContent{Source: repo.Content(ref.Normalized()), Transport: request.material, Depth: f.Depth}
	content.work.Scope, request.work, policy.Work = request.work.Attribution(), &content.work, &content.work
	key := sourcecache.GitKey{Content: content, Ref: refLabel, RecurseSubmodules: repo.RecurseSubmodules}
	if ref.Pinned() {
		policy.MaxAge = sourcecache.Forever
	}
	ignore := ""
	if repo.Ignore != nil {
		ignore = *repo.Ignore
	}
	viewOptions := gittree.ViewOptions{Content: content, Sparse: repo.SparseCheckout, Ignore: ignore}
	policy.Timeout = new(repo.OperationTimeout())
	var input *gittree.FS
	var identity string
	generation, id, err := f.Cache.Git(
		ctx,
		key,
		&policy,
		func(ctx context.Context, storer storage.Storer, worktree billy.Filesystem) (plumbing.Hash, error) {
			input = nil
			cloned, err := f.acquire(ctx, storer, worktree, &request)
			if err != nil {
				return plumbing.ZeroHash, errors.Join(ctx.Err(), err)
			}
			hash, err := resolveRefHash(cloned, repo.Reference, true)
			if err != nil {
				return plumbing.ZeroHash, err
			}
			if worktree != nil {
				if checkoutRefErr := checkoutRef(cloned, hash, repo.SparseCheckout); checkoutRefErr != nil {
					return plumbing.ZeroHash, checkoutRefErr
				}
				if updateSubmodulesErr := f.updateSubmodules(
					ctx,
					cloned,
					submoduleOptions{
						auth:  request.options.Auth,
						depth: git.DefaultSubmoduleRecursionDepth,
						work:  request.work,
					},
				); updateSubmodulesErr != nil {
					return plumbing.ZeroHash, errors.Join(ctx.Err(), updateSubmodulesErr)
				}
			}

			viewOptions.Worktree = worktree
			input, identity, err = gittree.New(
				ctx,
				storer.(*filesystem.Storage).Filesystem(), //nolint:errcheck // The owner stores this exact concrete type; a mismatched assertion must retain its panic.
				hash,
				viewOptions,
			)
			if err != nil {
				return plumbing.ZeroHash, manifest.NewError(
					manifest.ErrInput,
					fmt.Errorf("validate git view: %w", err),
					"",
				)
			}
			return hash, nil
		},
	)
	if err != nil {
		return gitWarningArtifact(input, repo.Named(), repo.URL, refLabel), err
	}

	viewOptions.Worktree = generation.Worktree
	if input == nil {
		input, identity, err = gittree.New(ctx, generation.Backing, generation.Commit, viewOptions)
	}
	if err != nil {
		return gitWarningArtifact(
				input,
				repo.Named(),
				repo.URL,
				refLabel,
			), manifest.NewError(
				manifest.ErrInput,
				err,
				"",
			)
	}

	warnings := gitWarnings(input, repo.Named(), repo.URL, refLabel)
	return &store.SourceArtifact{
		Kind:     manifest.KindGitRepository,
		URL:      repo.URL,
		Revision: generation.Commit.String(),
		FS:       input,
		Key:      id,
		Content:  identity,
		Warnings: warnings,
	}, nil
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

func (f *Fetcher) acquire(
	ctx context.Context,
	storer storage.Storer,
	worktree billy.Filesystem, requestValue *gitAcquisition,
) (*git.Repository, error) {
	request := *requestValue
	ref := (*manifest.GitRepositoryRef)(request.spec.Reference)
	reference2 := ref.Effective()
	kind, value := reference2.Kind, reference2.Value
	options := request.options
	fetch := &git.FetchOptions{
		Auth: options.Auth, ProxyOptions: options.ProxyOptions, CABundle: options.CABundle,
		ClientCert: options.ClientCert, ClientKey: options.ClientKey,
		Depth: options.Depth, Tags: git.NoTags,
	}

	if kind == commitReference && ref.Branch == "" {
		repo, err := initRemote(storer, worktree, options.URL)
		if err != nil {
			return nil, err
		}
		fetch.Depth, fetch.RefSpecs = 1, []config.RefSpec{config.RefSpec("+" + value + ":refs/flate/pin")}
		if err := request.fetchPin(ctx, f, repo, fetch); err != nil {
			return nil, err
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
		repo, err := request.clone(ctx, storer, worktree, &options)
		if err != nil {
			return nil, err
		}
		switch kind {
		case "name":
			specs = []config.RefSpec{ref.RefSpec()}
		case commitReference:
			specs = []config.RefSpec{fetchAllRefs}
		default:
			return repo, nil
		}
		fetch.Depth, fetch.RefSpecs = 0, specs
		err = request.fetchRevision(ctx, repo, fetch)
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
	err = request.fetchRevision(ctx, repo, fetch)
	if errors.Is(err, git.NoErrAlreadyUpToDate) {
		err = nil
	}
	return repo, err
}

func (r *gitAcquisition) clone(
	ctx context.Context,
	storer storage.Storer,
	worktree billy.Filesystem,
	options *git.CloneOptions,
) (*git.Repository, error) {
	cloneCtx := ctx
	var cancel context.CancelFunc
	if r.timeout > 0 {
		cloneCtx, cancel = context.WithTimeout(ctx, r.timeout)
	}
	repo, err := git.CloneContext(cloneCtx, storer, worktree, options)
	recordTransfer(r.work, r.protocol, err)
	if cancel != nil {
		cancel()
	}
	if err != nil {
		return nil, fmt.Errorf("clone git repository: %w", err)
	}
	return repo, nil
}

func initRemote(storer storage.Storer, worktree billy.Filesystem, remoteURL string) (*git.Repository, error) {
	repo, err := git.Init(storer, worktree)
	if err != nil {
		return nil, fmt.Errorf("initialize git repository: %w", err)
	}
	_, err = repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{remoteURL}})
	if err != nil {
		return repo, fmt.Errorf("create git remote: %w", err)
	}
	return repo, nil
}

func (f *Fetcher) validateRepo(repo *manifest.GitRepository, admitted bool) (*transport.Endpoint, error) {
	if repo == nil {
		return nil, errors.New("git repository is nil")
	}
	if repo.URL == "" {
		return nil, manifest.NewError(manifest.ErrInput, nil, "missing url")
	}
	endpoint, err := transport.NewEndpoint(repo.URL)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("parse git endpoint: %w", err), "")
	}
	if f.RestrictSchemes && !admitted {
		// Restricted scp-style remotes require an explicit user.
		allowedSSH := endpoint.Protocol == sshScheme && (endpoint.User != "" || strings.HasPrefix(repo.URL, "ssh://"))
		if endpoint.Protocol != "https" && !allowedSSH {
			return nil, manifest.NewError(
				manifest.ErrInput,
				nil,
				fmt.Sprintf(
					"url %q uses a scheme not allowed under --restrict-egress (only https:// and ssh:// remotes are permitted)",
					repo.URL,
				),
			)
		}
	}
	return endpoint, nil
}

func (f *operation) retrySSH(
	ctx context.Context, request *gitAcquisition, policyValue *sourcecache.Policy, endpoint *transport.Endpoint,
) (source.PreparedFetch, error) {
	policy := *policyValue
	repo := request.spec
	user := cmp.Or(endpoint.User, "git")
	prepared := *request
	var conn net.Conn
	var stop func() bool
	result, err := source.AcquireMachine(ctx, &policy, func(preparation context.Context) error {
		auth, opened, keys, err := machineSSHAuthentication(preparation, repo, user)
		conn = opened
		if err != nil {
			return err
		}
		stop = context.AfterFunc(ctx, func() { _ = opened.Close() })
		prepared.options.Auth = auth
		prepared.material = [3]any{request.material, user, keys}
		return nil
	}, func(ctx context.Context, policyValue *sourcecache.Policy, local *store.SourceArtifact) (*store.SourceArtifact, error) {
		policy := *policyValue
		defer stop()
		defer conn.Close()
		return prepared.complete(ctx, f.Fetcher, &policy, local)
	})
	if err != nil {
		if stop != nil {
			stop()
		}
		if conn != nil {
			_ = conn.Close()
		}
	} else {
		result.Close = func() {
			stop()
			_ = conn.Close()
		}
	}
	return result, err
}

func machineSSHAuthentication(
	ctx context.Context, repo *manifest.GitRepository, user string,
) (*gitssh.PublicKeysCallback, net.Conn, []string, error) {
	socket := os.Getenv("SSH_AUTH_SOCK")
	if socket == "" {
		return nil, nil, nil, source.MissingSecretErr(
			manifest.DiagnosticFor(repo.Named(), repo),
			"",
			"SSH agent unavailable",
		)
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", socket)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
			return nil, conn, nil, source.MissingSecretErr(
				manifest.DiagnosticFor(repo.Named(), repo),
				"",
				"SSH agent unavailable",
			)
		}
		return nil, conn, nil, fmt.Errorf("connect SSH agent: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	signers, err := agent.NewClient(conn).Signers()
	if err != nil {
		return nil, conn, nil, fmt.Errorf("read SSH agent identities: %w", err)
	}
	if len(signers) == 0 {
		return nil, conn, nil, source.MissingSecretErr(
			manifest.DiagnosticFor(repo.Named(), repo),
			"",
			"SSH agent has no identities",
		)
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, conn, nil, fmt.Errorf("locate SSH known hosts: %w", err)
	}
	callback, err := gitssh.NewKnownHostsCallback(filepath.Join(homeDir, ".ssh", "known_hosts"))
	if err != nil {
		return nil, conn, nil, fmt.Errorf("load SSH known hosts: %w", err)
	}
	auth := &gitssh.PublicKeysCallback{
		User:     user,
		Callback: func() ([]ssh.Signer, error) { return signers, nil },
	}
	auth.HostKeyCallback = callback
	keys := make([]string, len(signers))
	for i, signer := range signers {
		keys[i] = string(ssh.MarshalAuthorizedKey(signer.PublicKey()))
	}
	slices.Sort(keys)
	return auth, conn, keys, nil
}

func (r *gitAcquisition) fetchRevision(ctx context.Context, repo *git.Repository, fetch *git.FetchOptions) error {
	fetchCtx := ctx
	if r.timeout > 0 {
		var cancel context.CancelFunc
		fetchCtx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}
	err := repo.FetchContext(fetchCtx, fetch)
	recordTransfer(r.work, r.protocol, err)
	return err //nolint:wrapcheck // Preserve go-git protocol error identity and messages.
}

func (r *gitAcquisition) fetchPin(
	ctx context.Context,
	f *Fetcher,
	repo *git.Repository,
	fetch *git.FetchOptions,
) error {
	err := r.fetchRevision(ctx, repo, fetch)
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, transport.ErrAuthenticationRequired) ||
			errors.Is(err, transport.ErrAuthorizationFailed) {
			return err
		}
		if _, loaded := f.fallback.LoadOrStore(r.options.URL, struct{}{}); !loaded {
			slog.Debug("git: exact commit fetch refused; fetching full history", "url", r.options.URL)
		}
		fetch.Depth, fetch.RefSpecs = 0, []config.RefSpec{fetchAllRefs}
		if err := r.fetchRevision(ctx, repo, fetch); err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
			return err
		}
	}
	return nil
}

type gitContent struct {
	Source, Transport any
	Depth             int
	work              manifest.AcquisitionWork
}
