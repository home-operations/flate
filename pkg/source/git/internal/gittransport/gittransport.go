// Package gittransport carries the shared HTTPS-transport install
// lock that's serialized across the git.Fetcher and the bare-mirror
// cache.
//
// go-git v5 has no per-CloneOptions TLS hook, so a custom-CA fetch
// must register its transport on go-git's process-global protocol
// map and restore the default afterward. The lock is package-global
// because the install itself is — a per-Fetcher mutex would race
// when two Fetchers ran concurrently and clobbered each other's
// transport.
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

// InstallHTTPS installs a TLS+proxy-customized HTTPS transport on
// go-git's global protocol map and returns a restore function the
// caller MUST defer. Acquires the process-global mutex on entry; the
// restore releases it after re-installing the default client.
//
// The restore func is wrapped with sync.OnceFunc so a double-call
// (e.g., explicit cleanup followed by defer) is a no-op rather than
// a panic on unlock-of-unlocked-mutex. Today's only call sites defer
// it immediately, but the OnceFunc guard makes the API hard to misuse.
//
// When tlsCfg is nil there's nothing to customize: returns a no-op
// restore and no error, no lock acquired.
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
