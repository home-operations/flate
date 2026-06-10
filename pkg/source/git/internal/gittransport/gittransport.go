// Package gittransport carries the shared HTTPS-transport install lock
// serialized across git.Fetcher and the bare-mirror cache.
//
// go-git v5 has no per-CloneOptions TLS hook, so a custom-CA fetch must
// register its transport on go-git's process-global protocol map and
// restore the default afterward. The lock is package-global because the
// install itself is — a per-Fetcher mutex would race when two Fetchers ran
// concurrently and clobbered each other's transport.
//
// At init flate installs a *bounded* HTTPS client as go-git's process default
// (source.NewHTTPTransport carries ResponseHeaderTimeout) so an anonymous git
// fetch against a host that black-holes after dial can't hang the run — the
// consumer's dependency wait is now bound to fetch completion, not a wall
// clock. Anonymous fetches use this default concurrently with no per-fetch
// lock; custom-CA fetches swap in their own bounded transport under mu and
// restore to this bounded default (not go-git's unbounded githttp.DefaultClient).
package gittransport

import (
	"crypto/tls"
	"net/http"
	"sync"

	"github.com/go-git/go-git/v5/plumbing/transport/client"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"

	"github.com/home-operations/flate/pkg/source"
)

var mu sync.Mutex

// boundedHTTPSTransport is the ResponseHeaderTimeout-bounded transport behind
// the go-git default HTTPS client; kept as a package var for test introspection.
var boundedHTTPSTransport = func() *http.Transport {
	tr, _ := source.NewHTTPTransport(nil, nil) // nil/nil never errors
	return tr
}()

var boundedHTTPSClient = githttp.NewClient(&http.Client{Transport: boundedHTTPSTransport})

func init() {
	// Replace go-git's unbounded default HTTPS client with the bounded one so
	// every anonymous fetch (Fetcher + bare-mirror cache) inherits the
	// liveness backstop. Single-threaded at import — no race with fetches.
	client.InstallProtocol("https", boundedHTTPSClient)
}

// InstallHTTPS acquires the process-global mutex, installs a custom HTTPS
// transport on go-git's protocol map, and returns a restore func the caller
// MUST defer.
//
// sync.OnceFunc prevents a double-restore (defer + explicit call) from
// unlocking an already-unlocked mutex. When tlsCfg is nil there is nothing
// to customize — returns a no-op without acquiring the lock.
func InstallHTTPS(tlsCfg *tls.Config, proxy *source.ProxyConfig) (func(), error) {
	if tlsCfg == nil {
		return func() {}, nil
	}
	mu.Lock()
	tr, err := source.NewHTTPTransport(tlsCfg, proxy)
	if err != nil {
		mu.Unlock()
		return nil, err
	}
	client.InstallProtocol("https", githttp.NewClient(&http.Client{Transport: tr}))
	return sync.OnceFunc(func() {
		// Restore the BOUNDED default installed at init (not go-git's unbounded
		// githttp.DefaultClient) so anonymous fetches after a custom-CA fetch
		// keep the ResponseHeaderTimeout liveness backstop.
		client.InstallProtocol("https", boundedHTTPSClient)
		mu.Unlock()
	}), nil
}
