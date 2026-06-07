// Package mirror implements the bare-clone object store shared
// across GitRepository fetches. One bare mirror per upstream URL is
// kept warm across runs; per-(URL, ref) cache slots materialize
// their worktrees from it without re-cloning on every reconcile.
package mirror

import (
	"context"
	"crypto/sha256"
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
//
// Depth caps the clone/fetch history. 0 (the zero value) preserves the
// historical full clone. A positive depth maps to go-git's shallow
// CloneOptions.Depth / FetchOptions.Depth — only the tip commit's tree
// is what the worktree materialization needs, so depth=1 is sufficient
// for tag/branch/HEAD refs. The fetcher gates this off for commit-pinned
// refs (see git.effectiveDepth).
type FetchPlan struct {
	RefSpecs []config.RefSpec
	Depth    int
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
func (m *Cache) OpenOrFetch(ctx context.Context, url string, auth transport.AuthMethod, proxy *source.ProxyConfig, plan FetchPlan) (*git.Repository, error) {
	release, err := m.locks.Acquire(ctx, urlHash(url))
	if err != nil {
		return nil, err
	}
	defer release()

	path := m.layout.GitMirror(urlHash(url))
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("mirror parent: %w", err)
	}

	repo, openErr := git.PlainOpen(path)
	if openErr == nil {
		if err := m.fetchInto(ctx, repo, auth, proxy, plan.RefSpecs, plan.Depth); err != nil {
			return nil, err
		}
		return repo, nil
	}
	if !errors.Is(openErr, git.ErrRepositoryNotExists) {
		return nil, fmt.Errorf("mirror open %s: %w", path, openErr)
	}

	// Initial population. When the plan names specific refs (branch / tag /
	// name / semver), fetch ONLY those — go-git's PlainClone otherwise pulls
	// every branch (+refs/heads/*) and, by default, every tag, which on a
	// monorepo with thousands of refs dwarfs the one ref we need. The empty-
	// refspec case (nil/HEAD or a pinned commit) keeps the broad clone so
	// HEAD resolves and the commit is reachable on any branch.
	if len(plan.RefSpecs) > 0 {
		repo, err = m.initAndFetch(ctx, path, url, auth, proxy, plan)
	} else {
		repo, err = git.PlainCloneContext(ctx, path, true, &git.CloneOptions{
			URL:          url,
			Auth:         auth,
			ProxyOptions: proxyOptions(proxy),
			Depth:        plan.Depth,
		})
		if err == nil {
			// Broad clone pulls the server's default refs; the empty plan
			// refspec falls back to +refs/*:refs/* so non-default refs are
			// present too. Treat NoErrAlreadyUpToDate as success.
			err = m.fetchInto(ctx, repo, auth, proxy, plan.RefSpecs, plan.Depth)
		}
	}
	if err != nil {
		// Leave nothing partial behind so the next attempt re-clones
		// from scratch rather than tripping over a half-written mirror.
		_ = os.RemoveAll(path)
		return nil, fmt.Errorf("mirror init %s: %w", url, err)
	}
	return repo, nil
}

// initAndFetch creates a fresh bare mirror and populates it with exactly
// the plan's refspecs (at plan.Depth), instead of go-git's clone which
// pulls all branches and tags. HEAD is left at PlainInit's default and is
// intentionally not relied upon — every ref routed here (branch / tag /
// name / semver) resolves by explicit ref, never via HEAD.
func (m *Cache) initAndFetch(ctx context.Context, path, url string, auth transport.AuthMethod, proxy *source.ProxyConfig, plan FetchPlan) (*git.Repository, error) {
	repo, err := git.PlainInit(path, true)
	if err != nil {
		return nil, fmt.Errorf("mirror init: %w", err)
	}
	if _, err := repo.CreateRemote(&config.RemoteConfig{Name: "origin", URLs: []string{url}}); err != nil {
		return nil, fmt.Errorf("mirror remote: %w", err)
	}
	if err := m.fetchInto(ctx, repo, auth, proxy, plan.RefSpecs, plan.Depth); err != nil {
		return nil, err
	}
	return repo, nil
}

// fetchInto runs an incremental Fetch against the mirror's remote with
// the mirror refspec — every server ref updates in place, including
// explicit spec.ref.name targets such as refs/pull/* and refs/merge-*.
// Treats NoErrAlreadyUpToDate as a clean noop.
func (m *Cache) fetchInto(ctx context.Context, repo *git.Repository, auth transport.AuthMethod, proxy *source.ProxyConfig, refSpecs []config.RefSpec, depth int) error {
	if len(refSpecs) == 0 {
		refSpecs = []config.RefSpec{"+refs/*:refs/*"}
	}
	err := repo.FetchContext(ctx, &git.FetchOptions{
		Auth:         auth,
		RefSpecs:     refSpecs,
		ProxyOptions: proxyOptions(proxy),
		Depth:        depth,
		// All refspecs above are explicit (named refs, +refs/tags/*, or the
		// +refs/*:refs/* fallback), so suppress go-git's default tag auto-
		// following — it would silently pull every tag back in.
		Tags: git.NoTags,
	})
	if err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return fmt.Errorf("mirror fetch: %w", err)
	}
	return nil
}
