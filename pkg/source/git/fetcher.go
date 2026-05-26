// Package git implements the source.Fetcher for KindGitRepository.
//
// File map:
//
//	fetcher.go    — Fetcher type, Fetch entry, fetch + fetchViaMirror, authIdentity
//	auth.go       — SecretRef → transport.AuthMethod resolution
//	tls.go        — spec.secretRef.ca.crt → *tls.Config
//	ssh.go        — SSH URL / user extraction
//	checkout.go   — checkoutRef + updateSubmodules
//	resolve.go    — ref → commit hash (mirror path)
//	materialize.go — gittree.Materialize bridge
//	marker.go     — .flate-git-revision slot marker + worktree HEAD lookup
package git

import (
	"cmp"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/transport"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/git/internal/gittransport"
	"github.com/home-operations/flate/pkg/source/git/mirror"
	"github.com/home-operations/flate/pkg/source/git/verify"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher is the source.Fetcher implementation for KindGitRepository.
// It owns a shared Cache so multiple GitRepository CRs writing to the
// same cache root serialize on slot allocation correctly. Secrets is
// optional; required when a GitRepository sets spec.secretRef.
//
// Mirrors, when set, switches the default fetch path to an incremental
// bare-mirror-plus-worktree strategy: one bare clone per upstream URL
// (kept warm across runs and across refs), and per-slot worktrees are
// materialized by walking the commit tree out of the mirror. The
// legacy full PlainClone-into-slot path still runs for repos that
// need submodule recursion or sparse checkout — neither feature is
// expressible against a bare mirror without a separate fetch that
// defeats the cache. Leave nil to keep the legacy path everywhere.
type Fetcher struct {
	Cache   *source.Cache
	Secrets source.SecretGetter
	Mirrors *mirror.Cache
}

// Fetch implements source.TypedFetcher[*manifest.GitRepository].
// The typed signature is wrapped via source.Wrap at orchestrator
// registration — a payload mismatch returns ErrInput once at the
// adapter site rather than panicking here.
func (f *Fetcher) Fetch(ctx context.Context, repo *manifest.GitRepository) (*store.SourceArtifact, error) {
	if repo.Provider != "" && repo.Provider != sourcev1.GitProviderGeneric {
		return nil, source.ErrUnsupportedProvider("GitRepository",
			repo.Namespace, repo.Name, repo.Provider, sourcev1.GitProviderGeneric,
			"SecretRef-based credentials")
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
	restore, err := gittransport.InstallHTTPS(tlsCfg, proxy)
	if err != nil {
		return nil, err
	}
	defer restore()
	return f.fetch(ctx, repo, auth, proxy, tlsCfg)
}

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
//
// tlsCfg is the already-resolved spec.secretRef CA bundle (resolved by
// Fetch); pass through to fetchViaMirror so the mirror path doesn't
// re-resolve the Secret + re-parse the PEM bundle.
func (f *Fetcher) fetch(ctx context.Context, repo *manifest.GitRepository, auth transport.AuthMethod, proxy *source.ProxyConfig, tlsCfg *tls.Config) (*store.SourceArtifact, error) {
	cache := f.Cache
	if repo == nil {
		return nil, errors.New("git repository is nil")
	}
	if repo.URL == "" {
		return nil, fmt.Errorf("%w: GitRepository %s missing url", manifest.ErrInput, repo.RepoName())
	}

	refStr := "HEAD"
	if repo.Reference != nil {
		refStr = cmp.Or(manifest.GitRefString(*repo.Reference), refStr)
	}

	authID := authIdentity(repo)
	slot, err := cache.Slot(ctx, repo.URL, refStr, authID)
	if err != nil {
		return nil, fmt.Errorf("cache slot for %s: %w", repo.URL, err)
	}
	defer slot.Release()

	if slot.Exists {
		// The flate-revision marker is written AFTER a successful
		// clone+checkout (and committed via atomic rename only after
		// the marker write), so any committed slot will have it. A
		// missing marker means a legacy slot from a pre-marker flate
		// version or a hand-modified cache.
		if rev := readCachedRevision(slot.Path); rev != "" {
			return &store.SourceArtifact{
				Kind: manifest.KindGitRepository,
				URL:  repo.URL, LocalPath: slot.Path, Revision: rev,
			}, nil
		}
		// Stale slot — wipe and stage a fresh clone target.
		if err := slot.Reset(); err != nil {
			return nil, err
		}
		if err := slot.Stage(); err != nil {
			return nil, err
		}
	}

	url := repo.URL
	// file:// URLs are accepted by go-git as bare filesystem paths.
	if rest, ok := strings.CutPrefix(url, "file://"); ok {
		url = rest
	}

	if f.canUseMirror(repo, url) {
		return f.fetchViaMirror(ctx, repo, refStr, slot, auth, proxy, tlsCfg)
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
	// go-git's PlainCloneContext refuses to clone into a non-empty
	// directory — but our staging dir IS that empty directory, so
	// pass it directly. On any error, Release wipes staging; the
	// final slot is never touched.
	cloned, err := git.PlainCloneContext(ctx, slot.Path, false, cloneOpts)
	if err != nil {
		return nil, fmt.Errorf("clone %s: %w", url, err)
	}

	var ref manifest.GitRepositoryRef
	if repo.Reference != nil {
		ref = *repo.Reference
	}
	if err := checkoutRef(cloned, ref, repo.SparseCheckout); err != nil {
		return nil, fmt.Errorf("checkout %s: %w", refStr, err)
	}
	if repo.RecurseSubmodules {
		if err := updateSubmodules(cloned, auth); err != nil {
			return nil, fmt.Errorf("submodules: %w", err)
		}
	}

	rev, _ := readResolvedRevision(slot.Path)
	art := &store.SourceArtifact{
		Kind: manifest.KindGitRepository,
		URL:  repo.URL, LocalPath: slot.Path, Revision: rev,
	}
	if err := f.finalize(repo, art); err != nil {
		return nil, err
	}
	if err := slot.Commit(); err != nil {
		return nil, fmt.Errorf("GitRepository %s/%s: commit slot: %w", repo.Namespace, repo.Name, err)
	}
	art.LocalPath = slot.Path
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
		if err := verify.Signatures(f.Secrets, repo, cloned, head.Hash()); err != nil {
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

// canUseMirror reports whether this Fetcher can take the bare-mirror
// path for repo. The mirror path doesn't support submodule recursion
// or sparse checkout — both require a real worktree go-git can act on.
// Everything else (https, ssh, file://) is mirror-eligible.
func (f *Fetcher) canUseMirror(repo *manifest.GitRepository, _ string) bool {
	if f.Mirrors == nil {
		return false
	}
	if repo.RecurseSubmodules {
		return false
	}
	if len(repo.SparseCheckout) > 0 {
		return false
	}
	return true
}

// fetchViaMirror runs the bare-mirror path: open-or-update the
// per-URL mirror, resolve the requested ref to a commit hash, then
// materialize the tree into the slot's staging dir. PGP verification
// runs against the mirror (which has the object store); ApplyIgnore
// and the revision-marker write reuse the standard finalize.
//
// tlsCfg is the caller's pre-resolved spec.secretRef CA bundle —
// hoisting it out of this function avoids re-reading the Secret +
// re-parsing PEM on every mirror fetch.
func (f *Fetcher) fetchViaMirror(ctx context.Context, repo *manifest.GitRepository, refStr string, slot *source.Slot, auth transport.AuthMethod, proxy *source.ProxyConfig, tlsCfg *tls.Config) (*store.SourceArtifact, error) {
	mirrorRepo, err := f.Mirrors.OpenOrFetch(ctx, repo.URL, auth, proxy, tlsCfg)
	if err != nil {
		return nil, err
	}
	hash, err := resolveRefHash(mirrorRepo, repo.Reference)
	if err != nil {
		return nil, fmt.Errorf("GitRepository %s/%s ref %q: %w", repo.Namespace, repo.Name, refStr, err)
	}
	if err := materializeTree(ctx, mirrorRepo, hash, slot.Path); err != nil {
		return nil, fmt.Errorf("materialize %s at %s: %w", hash, refStr, err)
	}
	if repo.Verification != nil {
		if err := verify.Signatures(f.Secrets, repo, mirrorRepo, hash); err != nil {
			return nil, err
		}
	}
	art := &store.SourceArtifact{
		Kind: manifest.KindGitRepository,
		URL:  repo.URL, LocalPath: slot.Path, Revision: hash.String(),
	}
	if err := source.ApplyIgnore(art.LocalPath, repo.Ignore); err != nil {
		return nil, fmt.Errorf("GitRepository %s/%s: %w", repo.Namespace, repo.Name, err)
	}
	if art.Revision != "" {
		_ = writeCachedRevision(art.LocalPath, art.Revision)
	}
	if err := slot.Commit(); err != nil {
		return nil, fmt.Errorf("GitRepository %s/%s: commit slot: %w", repo.Namespace, repo.Name, err)
	}
	art.LocalPath = slot.Path
	return art, nil
}

// authIdentity returns the cache-key auth tag for a GitRepository.
// Combines the SecretRef (HTTPS / SSH creds) and ProxySecretRef the
// fetcher binds. Returns "" for anonymous clones so they share slots
// with the legacy un-auth-keyed layout.
func authIdentity(repo *manifest.GitRepository) string {
	return source.AuthIdentityFromRefs(repo.Namespace,
		repo.SecretRef, repo.ProxySecretRef)
}
