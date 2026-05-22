package source

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
	"github.com/home-operations/flate/pkg/store"
)

// GitFetcher is the Fetcher implementation for KindGitRepository.
// It owns a shared Cache so multiple GitRepository CRs writing to the
// same cache root serialize on slot allocation correctly. Secrets is
// optional; required when a GitRepository sets spec.secretRef.
type GitFetcher struct {
	Cache   *Cache
	Secrets SecretGetter
}

// Fetch implements source.Fetcher for *manifest.GitRepository.
func (f *GitFetcher) Fetch(ctx context.Context, obj manifest.BaseManifest) (*store.SourceArtifact, error) {
	repo, ok := obj.(*manifest.GitRepository)
	if !ok {
		return nil, fmt.Errorf("%w: GitFetcher: unexpected payload %T", manifest.ErrInput, obj)
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
	return FetchGit(ctx, f.Cache, repo, auth)
}

// resolveAuth turns repo.SecretRef into a go-git AuthMethod. Returns
// nil auth (anonymous) when no secret is configured, matching the
// pre-auth behavior. For HTTPS URLs the secret may carry either
// (username, password) or (bearerToken); for SSH URLs the secret
// carries `identity` (PEM private key) and an optional `password`.
func (f *GitFetcher) resolveAuth(repo *manifest.GitRepository) (transport.AuthMethod, error) {
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
		identity := stringFromSecret(sec, "identity")
		if identity == "" {
			return nil, fmt.Errorf("GitRepository %s/%s: secret %s/%s missing 'identity' for SSH auth",
				repo.Namespace, repo.Name, repo.Namespace, repo.SecretRef.Name)
		}
		password := stringFromSecret(sec, "password")
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
		if kh := stringFromSecret(sec, "known_hosts"); kh != "" {
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
	if token := stringFromSecret(sec, "bearerToken"); token != "" {
		return &githttp.TokenAuth{Token: token}, nil
	}
	username := stringFromSecret(sec, "username")
	password := stringFromSecret(sec, "password")
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

// FetchGit clones the GitRepository referenced by repo into the supplied
// cache and returns a populated *store.SourceArtifact. If a usable cached
// copy already exists, it is reused. auth may be nil for anonymous clones.
//
// Supported transports: HTTPS (anonymous, basic, bearer), SSH (key from
// SecretRef or ssh-agent), and file:// URLs.
func FetchGit(ctx context.Context, cache *Cache, repo *manifest.GitRepository, auth transport.AuthMethod) (*store.SourceArtifact, error) {
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
	cloned, err := git.PlainCloneContext(ctx, slot, false, cloneOpts)
	if err != nil {
		_ = cache.Reset(slot)
		return nil, fmt.Errorf("clone %s: %w", url, err)
	}

	if err := checkoutRef(cloned, repo.Ref); err != nil {
		return nil, fmt.Errorf("checkout %s: %w", refStr, err)
	}

	rev, _ := readResolvedRevision(slot)
	return &store.SourceArtifact{
		Kind: manifest.KindGitRepository,
		URL:  repo.URL, LocalPath: slot, Revision: rev,
	}, nil
}

func checkoutRef(repo *git.Repository, ref manifest.GitRepositoryRef) error {
	wt, err := repo.Worktree()
	if err != nil {
		return err
	}
	switch {
	case ref.Commit != "":
		return wt.Checkout(&git.CheckoutOptions{Hash: plumbing.NewHash(ref.Commit)})
	case ref.Tag != "":
		// Tags are typically reachable via "refs/tags/<tag>".
		if h, err := repo.ResolveRevision(plumbing.Revision("refs/tags/" + ref.Tag)); err == nil {
			return wt.Checkout(&git.CheckoutOptions{Hash: *h})
		}
		return wt.Checkout(&git.CheckoutOptions{Branch: plumbing.NewTagReferenceName(ref.Tag)})
	case ref.Branch != "":
		// Default-branch HEAD is already checked out when NoCheckout=false.
		// With NoCheckout we must resolve the remote branch.
		remoteRef := plumbing.NewRemoteReferenceName("origin", ref.Branch)
		if h, err := repo.ResolveRevision(plumbing.Revision(remoteRef)); err == nil {
			return wt.Checkout(&git.CheckoutOptions{Hash: *h})
		}
		// Fallback: maybe the repo has a local branch (file:// case).
		return wt.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(ref.Branch)})
	case ref.Semver != "":
		return fmt.Errorf("%w: GitRepository semver ref is not supported yet", manifest.ErrInput)
	}
	// No ref: just check out HEAD.
	return wt.Checkout(&git.CheckoutOptions{})
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
