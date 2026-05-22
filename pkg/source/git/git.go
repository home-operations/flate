// Package git implements the source.Fetcher for KindGitRepository.
package git

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	gitssh "github.com/go-git/go-git/v5/plumbing/transport/ssh"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher is the source.Fetcher implementation for KindGitRepository.
// It owns a shared Cache so multiple GitRepository CRs writing to the
// same cache root serialize on slot allocation correctly. Secrets is
// optional; required when a GitRepository sets spec.secretRef.
type Fetcher struct {
	Cache   *source.Cache
	Secrets source.SecretGetter
}

// Fetch implements source.Fetcher for *manifest.GitRepository.
func (f *Fetcher) Fetch(ctx context.Context, obj manifest.BaseManifest) (*store.SourceArtifact, error) {
	repo, ok := obj.(*manifest.GitRepository)
	if !ok {
		return nil, fmt.Errorf("%w: Fetcher: unexpected payload %T", manifest.ErrInput, obj)
	}
	if repo.Provider != "" && repo.Provider != manifest.GitProviderGeneric {
		return nil, fmt.Errorf(
			"GitRepository %s/%s provider %q is not implemented; flate currently supports only %q (SecretRef-based credentials)",
			repo.Namespace, repo.Name, repo.Provider, manifest.GitProviderGeneric,
		)
	}
	auth, err := f.resolveAuth(repo)
	if err != nil {
		return nil, err
	}
	return fetch(ctx, f.Cache, repo, auth)
}

// resolveAuth turns repo.SecretRef into a go-git AuthMethod. Returns
// nil auth (anonymous) when no secret is configured, matching the
// pre-auth behavior. For HTTPS URLs the secret may carry either
// (username, password) or (bearerToken); for SSH URLs the secret
// carries `identity` (PEM private key) and an optional `password`.
func (f *Fetcher) resolveAuth(repo *manifest.GitRepository) (transport.AuthMethod, error) {
	if repo.SecretRef == nil {
		return nil, nil
	}
	if f.Secrets == nil {
		return nil, fmt.Errorf("GitRepository %s/%s references secretRef but no SecretGetter is wired",
			repo.Namespace, repo.Name)
	}
	sec := f.Secrets(repo.Namespace, repo.SecretRef.Name)
	if sec == nil {
		return nil, fmt.Errorf("GitRepository %s/%s: secret %s/%s not found",
			repo.Namespace, repo.Name, repo.Namespace, repo.SecretRef.Name)
	}
	if isSSHURL(repo.URL) {
		identity := source.StringFromSecret(sec, "identity")
		if identity == "" {
			return nil, fmt.Errorf("GitRepository %s/%s: secret %s/%s missing 'identity' for SSH auth",
				repo.Namespace, repo.Name, repo.Namespace, repo.SecretRef.Name)
		}
		password := source.StringFromSecret(sec, "password")
		user := sshUserFromURL(repo.URL)
		auth, err := gitssh.NewPublicKeys(user, []byte(identity), password)
		if err != nil {
			return nil, fmt.Errorf("GitRepository %s/%s: parse SSH identity: %w",
				repo.Namespace, repo.Name, err)
		}
		// Flate has no central known_hosts. If the secret carries one,
		// enforce strict host-key checking; otherwise skip (offline
		// renders against ephemeral worktrees are the norm). Users who
		// need strict checks provide `known_hosts` in the Secret.
		if kh := source.StringFromSecret(sec, "known_hosts"); kh != "" {
			cb, herr := knownHostsCallback([]byte(kh))
			if herr != nil {
				return nil, fmt.Errorf("GitRepository %s/%s: parse known_hosts: %w",
					repo.Namespace, repo.Name, herr)
			}
			auth.HostKeyCallback = cb
		} else {
			auth.HostKeyCallback = insecureIgnoreHostKey
		}
		return auth, nil
	}
	// HTTPS / HTTP: bearerToken takes precedence over basic auth, mirroring
	// source-controller's docs.
	if token := source.StringFromSecret(sec, "bearerToken"); token != "" {
		return &githttp.TokenAuth{Token: token}, nil
	}
	username := source.StringFromSecret(sec, "username")
	password := source.StringFromSecret(sec, "password")
	if username == "" || password == "" {
		return nil, fmt.Errorf("GitRepository %s/%s: secret %s/%s missing username/password (or bearerToken) for HTTPS auth",
			repo.Namespace, repo.Name, repo.Namespace, repo.SecretRef.Name)
	}
	return &githttp.BasicAuth{Username: username, Password: password}, nil
}

func isSSHURL(url string) bool {
	return strings.HasPrefix(url, "ssh://") ||
		(strings.Contains(url, "@") && !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://"))
}

// sshUserFromURL extracts the SSH user from "user@host:repo" or
// "ssh://user@host/repo" forms. Defaults to "git" when absent.
func sshUserFromURL(url string) string {
	u := strings.TrimPrefix(url, "ssh://")
	if at := strings.Index(u, "@"); at > 0 {
		return u[:at]
	}
	return "git"
}

// fetch clones the GitRepository referenced by repo into the supplied
// cache and returns a populated *store.SourceArtifact. If a usable
// cached copy already exists, it is reused. auth may be nil for
// anonymous clones.
//
// Supported transports: HTTPS (anonymous, basic, bearer), SSH (key
// from SecretRef or ssh-agent), and file:// URLs.
func fetch(ctx context.Context, cache *source.Cache, repo *manifest.GitRepository, auth transport.AuthMethod) (*store.SourceArtifact, error) {
	if repo == nil {
		return nil, errors.New("git repository is nil")
	}
	if repo.URL == "" {
		return nil, fmt.Errorf("%w: GitRepository %s missing url", manifest.ErrInput, repo.RepoName())
	}

	refStr := repo.Ref.RefString()
	if refStr == "" {
		refStr = "HEAD"
	}

	slot, exists, err := cache.Slot(repo.URL, refStr)
	if err != nil {
		return nil, fmt.Errorf("cache slot for %s: %w", repo.URL, err)
	}

	if exists {
		// Validate it's a usable repo before reusing.
		if _, err := git.PlainOpen(slot); err == nil {
			rev, _ := readResolvedRevision(slot)
			return &store.SourceArtifact{
				Kind: manifest.KindGitRepository,
				URL:  repo.URL, LocalPath: slot, Revision: rev,
			}, nil
		}
		// Stale slot — wipe and re-clone.
		_ = cache.Reset(slot)
		if err := os.MkdirAll(slot, 0o750); err != nil {
			return nil, err
		}
	}

	url := repo.URL
	// file:// URLs are accepted by go-git as bare filesystem paths.
	if rest, ok := strings.CutPrefix(url, "file://"); ok {
		url = rest
	}

	cloneOpts := &git.CloneOptions{URL: url, NoCheckout: true, Auth: auth}
	if repo.RecurseSubmodules {
		cloneOpts.RecurseSubmodules = git.DefaultSubmoduleRecursionDepth
	}
	cloned, err := git.PlainCloneContext(ctx, slot, false, cloneOpts)
	if err != nil {
		_ = cache.Reset(slot)
		return nil, fmt.Errorf("clone %s: %w", url, err)
	}

	if err := checkoutRef(cloned, repo.Ref, repo.SparseCheckout); err != nil {
		return nil, fmt.Errorf("checkout %s: %w", refStr, err)
	}
	if repo.RecurseSubmodules {
		if err := updateSubmodules(cloned, auth); err != nil {
			return nil, fmt.Errorf("submodules: %w", err)
		}
	}

	rev, _ := readResolvedRevision(slot)
	return &store.SourceArtifact{
		Kind: manifest.KindGitRepository,
		URL:  repo.URL, LocalPath: slot, Revision: rev,
	}, nil
}

func checkoutRef(repo *git.Repository, ref manifest.GitRepositoryRef, sparse []string) error {
	wt, err := repo.Worktree()
	if err != nil {
		return err
	}
	// newOpts builds a CheckoutOptions pre-populated with sparse-checkout
	// directories when configured. Repeated per call-site because
	// CheckoutOptions is consumed by each Checkout invocation.
	newOpts := func() *git.CheckoutOptions {
		opts := &git.CheckoutOptions{}
		if len(sparse) > 0 {
			opts.SparseCheckoutDirectories = append(opts.SparseCheckoutDirectories, sparse...)
		}
		return opts
	}
	checkout := func(set func(*git.CheckoutOptions)) error {
		opts := newOpts()
		set(opts)
		return wt.Checkout(opts)
	}
	switch {
	case ref.Name != "":
		// Full ref name takes precedence (e.g. "refs/pull/420/head",
		// "refs/tags/v1.2.3"). Resolve against the cloned repo so the
		// remote-tracking layer (refs/remotes/origin/...) is checked
		// too — go-git's PlainClone fetches all refs but normalizes
		// branches under refs/remotes/origin.
		rev := plumbing.Revision(ref.Name)
		if h, err := repo.ResolveRevision(rev); err == nil {
			return checkout(func(o *git.CheckoutOptions) { o.Hash = *h })
		}
		// Fall through: try resolving as a remote-tracking ref. This
		// handles refs/heads/<branch> by mapping to refs/remotes/origin/<branch>.
		if rest, ok := strings.CutPrefix(ref.Name, "refs/heads/"); ok {
			remote := plumbing.NewRemoteReferenceName("origin", rest)
			if h, err := repo.ResolveRevision(plumbing.Revision(remote)); err == nil {
				return checkout(func(o *git.CheckoutOptions) { o.Hash = *h })
			}
		}
		return fmt.Errorf("%w: unable to resolve git ref %q", manifest.ErrInput, ref.Name)
	case ref.Commit != "":
		return checkout(func(o *git.CheckoutOptions) { o.Hash = plumbing.NewHash(ref.Commit) })
	case ref.Tag != "":
		if h, err := repo.ResolveRevision(plumbing.Revision("refs/tags/" + ref.Tag)); err == nil {
			return checkout(func(o *git.CheckoutOptions) { o.Hash = *h })
		}
		return checkout(func(o *git.CheckoutOptions) { o.Branch = plumbing.NewTagReferenceName(ref.Tag) })
	case ref.Branch != "":
		remoteRef := plumbing.NewRemoteReferenceName("origin", ref.Branch)
		if h, err := repo.ResolveRevision(plumbing.Revision(remoteRef)); err == nil {
			return checkout(func(o *git.CheckoutOptions) { o.Hash = *h })
		}
		return checkout(func(o *git.CheckoutOptions) { o.Branch = plumbing.NewBranchReferenceName(ref.Branch) })
	case ref.Semver != "":
		return fmt.Errorf("%w: GitRepository semver ref is not supported yet", manifest.ErrInput)
	}
	// No ref: just check out HEAD (with sparse, if configured).
	return checkout(func(*git.CheckoutOptions) {})
}

// updateSubmodules initializes and pulls submodules in the cloned
// worktree. Mirrors `git submodule update --init --recursive`. The
// parent's auth is reused for each submodule's fetch — Flux's
// behavior assumes a single credential source per GitRepository CR,
// even when submodules live on different hosts.
func updateSubmodules(repo *git.Repository, auth transport.AuthMethod) error {
	wt, err := repo.Worktree()
	if err != nil {
		return err
	}
	subs, err := wt.Submodules()
	if err != nil {
		return err
	}
	return subs.Update(&git.SubmoduleUpdateOptions{
		Init:              true,
		RecurseSubmodules: git.DefaultSubmoduleRecursionDepth,
		Auth:              auth,
	})
}

// readResolvedRevision returns the current commit SHA at the worktree.
// Best-effort: returns empty string on any failure.
func readResolvedRevision(slot string) (string, error) {
	repo, err := git.PlainOpen(slot)
	if err != nil {
		return "", err
	}
	h, err := repo.Head()
	if err != nil {
		return "", err
	}
	return h.Hash().String(), nil
}
