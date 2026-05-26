// Package gittransport carries the shared HTTPS-transport install lock
// serialized across git.Fetcher and the bare-mirror cache.
//
// go-git v5 has no per-CloneOptions TLS hook, so a custom-CA fetch must
// register its transport on go-git's process-global protocol map and
// restore the default afterward. The lock is package-global because the
// install itself is — a per-Fetcher mutex would race when two Fetchers ran
// concurrently and clobbered each other's transport.
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
		client.InstallProtocol("https", githttp.DefaultClient)
		mu.Unlock()
	}), nil
}
