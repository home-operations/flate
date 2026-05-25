// Package git implements the source.Fetcher for KindGitRepository.
package git

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

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

// httpsTransportMu is declared in tls.go (with the only caller —
// the TLS-customized InstallProtocol path in Fetch below).

// Fetch implements source.Fetcher for *manifest.GitRepository.
func (f *Fetcher) Fetch(ctx context.Context, obj manifest.BaseManifest) (*store.SourceArtifact, error) {
	repo, ok := obj.(*manifest.GitRepository)
	if !ok {
		return nil, fmt.Errorf("%w: Fetcher: unexpected payload %T", manifest.ErrInput, obj)
	}
	if repo.Provider != "" && repo.Provider != sourcev1.GitProviderGeneric {
		return nil, fmt.Errorf(
			"GitRepository %s/%s provider %q is not implemented; flate currently supports only %q (SecretRef-based credentials)",
			repo.Namespace, repo.Name, repo.Provider, sourcev1.GitProviderGeneric,
		)
	}
	auth, err := f.resolveAuth(repo)
	if err != nil {
		return nil, err
	}
	tlsCfg, err := f.resolveTLS(repo)
	if err != nil {
		return nil, err
	}
	proxy, err := source.ResolveProxy(f.Secrets, repo.Namespace, "GitRepository",
		repo.Namespace+"/"+repo.Name, repo.ProxySecretRef)
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
		httpsTransportMu.Lock()
		defer httpsTransportMu.Unlock()
		tr := &http.Transport{TLSClientConfig: tlsCfg}
		if proxy != nil {
			pfn, perr := proxy.HTTPProxyFunc()
			if perr != nil {
				return nil, perr
			}
			tr.Proxy = pfn
		}
		httpsClient := &http.Client{Transport: tr}
		client.InstallProtocol("https", githttp.NewClient(httpsClient))
		defer client.InstallProtocol("https", githttp.DefaultClient)
	}
	return f.fetch(ctx, repo, auth, proxy)
}

// resolveTLS / resolveAuth / isSSHURL / sshUserFromURL split into
// tls.go and auth.go (mirroring the existing tls_test.go and
// auth_test.go layouts).

// fetch clones the GitRepository, then runs verification, ignore, and
// the cache-marker write — ALL inside the per-slot critical section.
// Holding the slot lock across post-clone work prevents another
// fetcher with the same (url, ref) from observing torn state (an
// in-progress clone, a missing marker, an unverified slot) or from
// racing a sibling cache.Reset against an in-flight write.
//
// auth may be nil for anonymous clones; proxy may be nil for direct.
// Supported transports: HTTPS (anonymous, basic, bearer), SSH (key
// from SecretRef or ssh-agent), and file:// URLs.
func (f *Fetcher) fetch(ctx context.Context, repo *manifest.GitRepository, auth transport.AuthMethod, proxy *source.ProxyConfig) (*store.SourceArtifact, error) {
	cache := f.Cache
	if repo == nil {
		return nil, errors.New("git repository is nil")
	}
	if repo.URL == "" {
		return nil, fmt.Errorf("%w: GitRepository %s missing url", manifest.ErrInput, repo.RepoName())
	}

	refStr := "HEAD"
	if repo.Reference != nil {
		if s := manifest.GitRefString(*repo.Reference); s != "" {
			refStr = s
		}
	}

	slot, exists, release, err := cache.Slot(repo.URL, refStr)
	if err != nil {
		return nil, fmt.Errorf("cache slot for %s: %w", repo.URL, err)
	}
	defer release()

	if exists {
		// The flate-revision marker is written AFTER a successful
		// clone+checkout, and survives ApplyIgnore (the caller wipes
		// .git/ as part of source-controller-compatible artifact
		// shaping; a `git.PlainOpen` check here would always fail on
		// a previously-cached slot). Presence + non-empty content is
		// enough to declare the slot valid.
		if rev := readCachedRevision(slot); rev != "" {
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
	if proxy != nil {
		cloneOpts.ProxyOptions = transport.ProxyOptions{
			URL:      proxy.Address,
			Username: proxy.Username,
			Password: proxy.Password,
		}
	}
	if repo.RecurseSubmodules {
		cloneOpts.RecurseSubmodules = git.DefaultSubmoduleRecursionDepth
	}
	cloned, err := git.PlainCloneContext(ctx, slot, false, cloneOpts)
	if err != nil {
		_ = cache.Reset(slot)
		return nil, fmt.Errorf("clone %s: %w", url, err)
	}

	var ref manifest.GitRepositoryRef
	if repo.Reference != nil {
		ref = *repo.Reference
	}
	if err := checkoutRef(cloned, ref, repo.SparseCheckout); err != nil {
		_ = cache.Reset(slot)
		return nil, fmt.Errorf("checkout %s: %w", refStr, err)
	}
	if repo.RecurseSubmodules {
		if err := updateSubmodules(cloned, auth); err != nil {
			_ = cache.Reset(slot)
			return nil, fmt.Errorf("submodules: %w", err)
		}
	}

	rev, _ := readResolvedRevision(slot)
	art := &store.SourceArtifact{
		Kind: manifest.KindGitRepository,
		URL:  repo.URL, LocalPath: slot, Revision: rev,
	}
	// Post-clone work — verification, ignore, marker — runs under the
	// slot lock so a sibling Fetcher of the same (url, ref) can't race
	// our writes or our error-path cache.Reset.
	if err := f.finalize(repo, art); err != nil {
		_ = cache.Reset(slot)
		return nil, err
	}
	return art, nil
}

// finalize runs PGP verification (when configured), applies the
// source-controller-compatible ignore patterns, and writes the cache-
// hit marker. Returns the first error encountered; the caller is
// expected to Reset the slot on error while still holding the lock.
func (f *Fetcher) finalize(repo *manifest.GitRepository, art *store.SourceArtifact) error {
	if repo.Verification != nil {
		cloned, oerr := git.PlainOpen(art.LocalPath)
		if oerr != nil {
			return fmt.Errorf("verify: reopen %s: %w", art.LocalPath, oerr)
		}
		head, herr := cloned.Head()
		if herr != nil {
			return fmt.Errorf("verify: resolve HEAD: %w", herr)
		}
		if err := verifySignatures(f.Secrets, repo, cloned, head.Hash()); err != nil {
			return err
		}
	}
	if err := source.ApplyIgnore(art.LocalPath, repo.Ignore); err != nil {
		return fmt.Errorf("GitRepository %s/%s: %w", repo.Namespace, repo.Name, err)
	}
	// Write the revision marker AFTER ApplyIgnore so it survives any
	// user-supplied "exclude all" patterns (common — `/*` + reincludes).
	// Next run's cache-hit check (readCachedRevision) avoids the
	// expensive re-clone for big repos whose .git/ was wiped by ignore.
	if art.Revision != "" {
		_ = writeCachedRevision(art.LocalPath, art.Revision)
	}
	return nil
}

// cachedRevisionFile holds the resolved commit SHA of a fetched slot.
// Written post-clone, BEFORE the caller's ApplyIgnore wipes .git/, so
// future cache-hit checks can validate the slot without re-running
// git.PlainOpen.
const cachedRevisionFile = ".flate-git-revision"

func writeCachedRevision(slot, rev string) error {
	return os.WriteFile(filepath.Join(slot, cachedRevisionFile), []byte(rev), 0o600)
}

func readCachedRevision(slot string) string {
	b, err := os.ReadFile(filepath.Join(slot, cachedRevisionFile)) //nolint:gosec // slot is fetcher-owned cache path
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
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
	case ref.SemVer != "":
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
