package source

import (
	"crypto/tls"
	"net/http"
	"time"
)

// ResponseHeaderTimeout bounds how long an HTTP source fetch (git over HTTPS,
// OCI, bucket) waits for the FIRST response byte after writing the request —
// a liveness backstop, not a determinism knob. Consumers now wait for a
// fetch's OUTCOME (orchestrator quiescence) rather than a per-dep wall clock,
// so a host that completes TLS+dial then black-holes — connects but never
// sends response headers — would otherwise keep the task pool active forever
// and wedge the whole run. ResponseHeaderTimeout bounds only the header wait,
// NOT the streamed clone/pull/download body, so a large artifact on a slow-but-
// live link still completes; sized large enough never to trip a live repo.
// Mirrors helm's helmHTTPTimeout (helm's getter ignores ctx; this covers the
// ctx-aware git/OCI/bucket transports' header wait). A var so tests can shrink
// it — mutate only before a run starts to stay race-clean.
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
//
// Replaces three near-identical implementations (git, oci, bucket) that had
// drifted on whether to clone or to allocate fresh.
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
	return tr, nil
}
