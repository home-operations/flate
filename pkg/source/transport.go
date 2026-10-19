package source

import (
	"crypto/tls"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/http/httpproxy"

	"github.com/home-operations/flate/pkg/manifest"
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
	// The guard checks resolved first hops, including proxies and redirects.
	ssrfguard.WrapTransport(tr)
	return tr, nil
}

// HTTPTransportConfig specifies immutable effective transport material.
// A nil ProxyEnvironment captures the environment at selection time.
// Credentials separate source security boundaries; auth remains client-local.
type HTTPTransportConfig struct {
	TLS                *TLSMaterial
	Insecure           bool
	Proxy              *ProxyConfig
	ProxyEnvironment   *httpproxy.Config
	Credentials        any
	DisableCompression bool
}

// HTTPTransport selects a fully configured, guarded transport for this invocation.
// The Network MUST be non-nil and outlive every borrower.
func (n *Network) HTTPTransport(cfg HTTPTransportConfig) (http.RoundTripper, error) {
	environment := httpproxy.Config{}
	var proxyURL *url.URL
	if cfg.Proxy != nil {
		var err error
		proxyURL, err = cfg.Proxy.URL()
		if err != nil {
			return nil, err
		}
	} else if cfg.ProxyEnvironment != nil {
		environment = *cfg.ProxyEnvironment
	} else {
		environment = *httpproxy.FromEnvironment()
	}
	proxyIdentity := ""
	if proxyURL != nil {
		proxyIdentity = proxyURL.String()
	}
	var pem [3]string
	if cfg.TLS != nil {
		pem = cfg.TLS.pem
	}
	key := manifest.Fingerprint(struct {
		TLS                [3]string
		Insecure           bool
		Proxy              string
		Environment        httpproxy.Config
		Credentials        any
		DisableCompression bool
	}{pem, cfg.Insecure, proxyIdentity, environment, cfg.Credentials, cfg.DisableCompression})
	if key == "" {
		return nil, fmt.Errorf("%w: HTTP transport material cannot be fingerprinted", manifest.ErrInput)
	}
	current := n.transports.Load()
	if current != nil {
		if tr := (*current)[key]; tr != nil {
			return tr, nil
		}
	}
	var tlsCfg *tls.Config
	if cfg.TLS != nil {
		tlsCfg = cfg.TLS.config.Clone()
	}
	if cfg.Insecure {
		if tlsCfg == nil {
			tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		tlsCfg.InsecureSkipVerify = true
	}
	tr, err := NewHTTPTransport(tlsCfg, nil)
	if err != nil {
		return nil, err
	}
	tr.Protocols = new(http.Protocols)
	tr.Protocols.SetHTTP1(true)
	// A cloned transport may advertise h2; ALPN MUST match Protocols.
	if tr.TLSClientConfig != nil {
		tr.TLSClientConfig.NextProtos = []string{"http/1.1"}
	}
	tr.DisableCompression = cfg.DisableCompression
	if proxyURL != nil {
		tr.Proxy = http.ProxyURL(proxyURL)
	} else {
		proxyForURL := environment.ProxyFunc()
		tr.Proxy = func(r *http.Request) (*url.URL, error) { return proxyForURL(r.URL) }
	}
	selected := n.wrapHTTPTransport(tr)
	for {
		next := make(networkTransports)
		if current != nil {
			if winner := (*current)[key]; winner != nil {
				tr.CloseIdleConnections()
				return winner, nil
			}
			next = maps.Clone(*current)
		}
		next[key] = selected.(*networkTransport)
		if n.transports.CompareAndSwap(current, &next) {
			return selected, nil
		}
		current = n.transports.Load()
	}
}
