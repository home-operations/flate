package git

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/client"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/home-operations/flate/internal/keylock"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cacheroot"
)

// MirrorCache holds one bare clone per unique upstream URL. The mirror
// is the persistent object store that incremental Fetches update; the
// per-(URL, ref) cache slots materialize their worktrees from it
// without re-cloning across runs or across refs of the same repo.
//
// Construct via NewMirrorCache; pass to Fetcher.Mirrors. A nil
// Fetcher.Mirrors disables mirroring — the legacy PlainClone-into-slot
// path runs unchanged (used by tests and any caller that prefers the
// older behavior).
type MirrorCache struct {
	layout cacheroot.Layout
	locks  *keylock.KeyMap[string]
}

// NewMirrorCache constructs a MirrorCache backed by the supplied
// Layout. The git-mirrors subtree is created lazily on first
// openOrFetch.
func NewMirrorCache(layout cacheroot.Layout) *MirrorCache {
	return &MirrorCache{layout: layout, locks: keylock.New[string]()}
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

func (m *MirrorCache) pathFor(url string) string {
	return m.layout.GitMirror(urlHash(url))
}

// openOrFetch returns the bare mirror repo for url, ensuring it carries
// up-to-date refs. First call for a URL runs a bare clone; subsequent
// calls incrementally Fetch. Holds the per-URL lock across the network
// operation so two concurrent callers serialize.
//
// auth/proxy/tlsCfg are applied to whichever network operation runs
// (clone or fetch). For HTTPS with a custom TLS config, the global
// httpsTransportMu protocol-install dance is repeated to match the
// non-mirror path's contract.
func (m *MirrorCache) openOrFetch(ctx context.Context, url string, auth transport.AuthMethod, proxy *source.ProxyConfig, tlsCfg *tls.Config) (*git.Repository, error) {
	release, err := m.locks.Acquire(ctx, urlHash(url))
	if err != nil {
		return nil, err
	}
	defer release()

	if tlsCfg != nil {
		httpsTransportMu.Lock()
		defer httpsTransportMu.Unlock()
		tr, terr := source.NewHTTPTransport(tlsCfg, proxy)
		if terr != nil {
			return nil, terr
		}
		client.InstallProtocol("https", githttp.NewClient(&http.Client{Transport: tr}))
		defer client.InstallProtocol("https", githttp.DefaultClient)
	}

	path := m.pathFor(url)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("mirror parent: %w", err)
	}

	repo, openErr := git.PlainOpen(path)
	if openErr == nil {
		if err := m.fetchInto(ctx, repo, auth, proxy); err != nil {
			return nil, err
		}
		return repo, nil
	}
	if !errors.Is(openErr, git.ErrRepositoryNotExists) {
		return nil, fmt.Errorf("mirror open %s: %w", path, openErr)
	}

	cloneOpts := &git.CloneOptions{URL: url, Auth: auth}
	if proxy != nil {
		cloneOpts.ProxyOptions = transport.ProxyOptions{
			URL: proxy.Address, Username: proxy.Username, Password: proxy.Password,
		}
	}
	repo, err = git.PlainCloneContext(ctx, path, true, cloneOpts) // bare = true
	if err != nil {
		// Leave nothing partial behind so the next attempt re-clones
		// from scratch rather than tripping over a half-written mirror.
		_ = os.RemoveAll(path)
		return nil, fmt.Errorf("mirror clone %s: %w", url, err)
	}
	return repo, nil
}

// fetchInto runs an incremental Fetch against the mirror's remote with
// the mirror refspec — every branch and tag updates in place. Treats
// NoErrAlreadyUpToDate as a clean noop.
func (m *MirrorCache) fetchInto(ctx context.Context, repo *git.Repository, auth transport.AuthMethod, proxy *source.ProxyConfig) error {
	opts := &git.FetchOptions{
		Auth: auth,
		RefSpecs: []config.RefSpec{
			"+refs/heads/*:refs/heads/*",
			"+refs/tags/*:refs/tags/*",
		},
	}
	if proxy != nil {
		opts.ProxyOptions = transport.ProxyOptions{
			URL: proxy.Address, Username: proxy.Username, Password: proxy.Password,
		}
	}
	if err := repo.FetchContext(ctx, opts); err != nil && !errors.Is(err, git.NoErrAlreadyUpToDate) {
		return fmt.Errorf("mirror fetch: %w", err)
	}
	return nil
}
