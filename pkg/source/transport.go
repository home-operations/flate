package source

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/home-operations/flate/pkg/source/ssrfguard"
)

// ResponseHeaderTimeout bounds the HTTP header wait, never the streamed body.
// Fetches must finish for scheduler quiescence, so an endpoint that completes
// TLS and stops responding must not hold the task pool forever. Tests may
// shorten this bound only before a run starts to avoid concurrent mutation.
var ResponseHeaderTimeout = 120 * time.Second

// NewHTTPTransport composes an *http.Transport for HTTP source fetches: a clone
// of http.DefaultTransport (inheriting net/http's production-tuned connection
// pool / keep-alive / TLS-minimum settings) with ResponseHeaderTimeout applied
// as a liveness backstop, plus optional TLS / proxy customization.
//
// It ALWAYS returns a non-nil transport so every caller is bounded — callers
// must NOT fall back to http.DefaultTransport (which has no header timeout).
// Cloning (not allocating fresh) is the idiomatic way to override exactly the
// dimensions we control without losing the rest of the defaults.
func NewHTTPTransport(tlsCfg *tls.Config, proxy *ProxyConfig) (*http.Transport, error) {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ResponseHeaderTimeout = ResponseHeaderTimeout
	if tlsCfg != nil {
		tr.TLSClientConfig = tlsCfg
	}
	if proxy != nil {
		u, err := proxy.URL()
		if err != nil {
			return nil, err
		}
		tr.Proxy = http.ProxyURL(u)
	}
	// Install the SSRF egress guard's dial Control on every source-fetch
	// transport (git/OCI/bucket here; helm + kustomize wrap their own clients
	// the same way). A no-op unless ssrfguard.Restrict is enabled, so untrusted
	// renders are guarded while trusted renders keep reaching private/LAN hosts.
	ssrfguard.WrapTransport(tr)
	return tr, nil
}
