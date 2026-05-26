// Package mirror implements the bare-clone object store shared
// across GitRepository fetches. One bare mirror per upstream URL is
// kept warm across runs; per-(URL, ref) cache slots materialize
// their worktrees from it without re-cloning on every reconcile.
package mirror

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/transport"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// Cache holds one bare clone per unique upstream URL. The mirror is
// the persistent object store that incremental Fetches update; the
// per-(URL, ref) cache slots materialize their worktrees from it
// without re-cloning across runs or across refs of the same repo.
//
// Construct via New; pass to git.Fetcher.Mirrors. A nil
// Fetcher.Mirrors disables mirroring — the legacy PlainClone-into-slot
// path runs unchanged (used by tests and any caller that prefers the
// older behavior).
type Cache struct {
	layout cacheroot.Layout
	locks  *keylock.KeyMap[string]
}

// FetchPlan narrows the mirror update to the refs needed by one
// GitRepository. An empty RefSpecs slice preserves the historical full
// mirror refresh.
type FetchPlan struct {
	RefSpecs []config.RefSpec
}

// New constructs a Cache backed by the supplied Layout. The
// git-mirrors subtree is created lazily on first OpenOrFetch.
func New(layout cacheroot.Layout) *Cache {
	return &Cache{layout: layout, locks: keylock.New[string]()}
}

// urlHash returns the stable directory name for url's mirror. The hash
// keys ONLY on the URL — not on ref or auth — so all refs of one repo
// share one object store. Two CRs with different SecretRefs targeting
// the same URL share the mirror; their per-slot worktrees stay isolated
// via the cache slot's authID (see source.Cache.Slot).
func urlHash(url string) string {
	h := sha256.Sum256([]byte(url))
	return hex.EncodeToString(h[:])[:16]
}

func (m *Cache) pathFor(url string) string {
	return m.layout.GitMirror(urlHash(url))
}

// proxyOptions converts a nullable ProxyConfig into go-git's inline
// struct so every call site doesn't repeat the nil guard.
func proxyOptions(proxy *source.ProxyConfig) transport.ProxyOptions {
	if proxy == nil {
		return transport.ProxyOptions{}
	}
	return transport.ProxyOptions{
		URL: proxy.Address, Username: proxy.Username, Password: proxy.Password,
	}
}

// OpenOrFetch returns the bare mirror repo for url, ensuring it
// carries up-to-date refs. First call for a URL runs a bare clone;
// subsequent calls incrementally Fetch. Holds the per-URL lock across
// the network operation so two concurrent callers serialize.
//
// tlsCfg is accepted for API compatibility; the caller (Fetch) already
// holds the process-global HTTPS-transport lock for the duration of the
// mirror operation, so installing it here would deadlock.
func (m *Cache) OpenOrFetch(ctx context.Context, url string, auth transport.AuthMethod, proxy *source.ProxyConfig, tlsCfg *tls.Config, plan FetchPlan) (*git.Repository, error) {
	_ = tlsCfg // transport lock held by caller; see gittransport.InstallHTTPS
	release, err := m.locks.Acquire(ctx, urlHash(url))
	if err != nil {
		return nil, err
	}
	defer release()

	path := m.pathFor(url)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("mirror parent: %w", err)
	}

	repo, openErr := git.PlainOpen(path)
	if openErr == nil {
		if err := m.fetchInto(ctx, repo, auth, proxy, plan.RefSpecs); err != nil {
			return nil, err
		}
		return repo, nil
	}
	if !errors.Is(openErr, git.ErrRepositoryNotExists) {
		return nil, fmt.Errorf("mirror open %s: %w", path, openErr)
	}

	repo, err = git.PlainCloneContext(ctx, path, true, &git.CloneOptions{
		URL:          url,
		Auth:         auth,
		ProxyOptions: proxyOptions(proxy),
	})
	if err != nil {
		// Leave nothing partial behind so the next attempt re-clones
		// from scratch rather than tripping over a half-written mirror.
		_ = os.RemoveAll(path)
		return nil, fmt.Errorf("mirror clone %s: %w", url, err)
	}
	// Fetch the plan's narrow refspecs after cloning — clone only pulls the
	// server's default refs (typically refs/heads/*), so non-default refs
	// such as refs/pull/* won't be present until an explicit targeted fetch.
	if err := m.fetchInto(ctx, repo, auth, proxy, plan.RefSpecs); err != nil {
		_ = os.RemoveAll(path)
		return nil, err
	}
	return repo, nil
}

// fetchInto runs an incremental Fetch against the mirror's remote with
// the mirror refspec — every server ref updates in place, including
// explicit spec.ref.name targets such as refs/pull/* and refs/merge-*.
// Treats NoErrAlreadyUpToDate as a clean noop.
func (m *Cache) fetchInto(ctx context.Context, repo *git.Repository, auth transport.AuthMethod, proxy *source.ProxyConfig, refSpecs []config.RefSpec) error {
	if len(refSpecs) == 0 {
		refSpecs = []config.RefSpec{"+refs/*:refs/*"}
	}
	err := repo.FetchContext(ctx, &git.FetchOptions{
		Auth:         auth,
		RefSpecs:     refSpecs,
		ProxyOptions: proxyOptions(proxy),
	})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return fmt.Errorf("mirror fetch: %w", err)
	}
	return nil
}
