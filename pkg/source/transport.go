package source

import (
	"crypto/tls"
	"net/http"
)

// NewHTTPTransport composes an *http.Transport from optional TLS and
// proxy configuration. Returns nil when neither is set so callers can
// substitute the runtime's default transport (and inherit its
// keep-alive / max-idle tuning) when no customization is needed.
//
// When at least one of tlsCfg / proxy is non-nil, the runtime default
// transport is cloned (NOT freshly allocated) so connection pool
// settings, timeouts, and TLS minimum version inherit. Setting the
// fields on a clone is the idiomatic way to override exactly the
// dimensions the caller controls without losing the rest of net/http's
// production-tuned defaults.
//
// Replaces three near-identical implementations (git, oci, bucket)
// that had drifted on whether to clone or to allocate fresh.
func NewHTTPTransport(tlsCfg *tls.Config, proxy *ProxyConfig) (*http.Transport, error) {
	if tlsCfg == nil && proxy == nil {
		return nil, nil
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	if tlsCfg != nil {
		tr.TLSClientConfig = tlsCfg
	}
	if proxy != nil {
		pfn, err := proxy.HTTPProxyFunc()
		if err != nil {
			return nil, err
		}
		tr.Proxy = pfn
	}
	return tr, nil
}
