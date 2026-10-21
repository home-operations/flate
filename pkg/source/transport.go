package source

import (
	"crypto/tls"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/net/http/httpproxy"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/ssrfguard"
)

type transportUse uint8

const (
	// TransportCert selects required TLS material from certSecretRef.
	TransportCert transportUse = iota
	// TransportSecret selects TLS material from Git's auth secretRef.
	TransportSecret
	// TransportProxy selects routing material from proxySecretRef.
	TransportProxy
)

func (u transportUse) label() string {
	return [...]string{"certSecretRef", "secretRef", "proxySecretRef"}[u]
}

// TransportInput selects an ordered TLS or proxy Secret reference.
type TransportInput struct {
	Reference *manifest.LocalObjectReference
	Use       transportUse
	// OptionalTLS permits Git's absent or auth-only secretRef to supply no TLS.
	OptionalTLS bool
	// CAFallback enables Git's caFile key, with ca.crt taking precedence.
	CAFallback bool
}

// TransportMaterial captures immutable TLS, certificate identity and proxy policy.
type TransportMaterial struct {
	config              *tls.Config
	certificate         [3]string
	certificateIdentity any
	proxy               *proxyConfig
	proxyURL            *url.URL
	environment         *httpproxy.Config
}

// ResolveTransport resolves ordered inputs using one operation's Secret snapshot.
func ResolveTransport(secrets SecretGetter, owner manifest.NamedResource, inputs []TransportInput) (TransportMaterial, error) {
	var material TransportMaterial
	for _, input := range inputs {
		switch input.Use {
		case TransportProxy:
			proxy, selected, err := resolveProxy(secrets, owner, input.Reference)
			if err != nil {
				return TransportMaterial{}, err
			}
			material.proxy, material.proxyURL = proxy, selected
		case TransportCert, TransportSecret:
			if input.Reference == nil {
				continue
			}
			cfg, certificate, identity, err := resolveTLS(secrets, owner, input)
			if err != nil {
				return TransportMaterial{}, err
			}
			material.config, material.certificate, material.certificateIdentity = cfg, certificate, identity
		default:
			panic("invalid transport input use")
		}
	}
	return material.captureEnvironment(), nil
}

func (m TransportMaterial) captureEnvironment() TransportMaterial {
	if m.proxyURL == nil && m.environment == nil {
		m.environment = httpproxy.FromEnvironment()
	}
	return m
}

// Identity combines protocol credentials with full certificate and proxy acquisition material.
// Returned Secret maps remain read-only.
func (m TransportMaterial) Identity(credentials any) any {
	var proxy any
	if m.proxyURL != nil {
		proxy = m.proxyURL.String()
	} else if m.environment != nil && (m.environment.HTTPProxy != "" || m.environment.HTTPSProxy != "") {
		proxy = *m.environment
	}
	if credentials == nil && m.certificateIdentity == nil && proxy == nil {
		return nil
	}
	return [3]any{credentials, m.certificateIdentity, proxy}
}

// Certificate returns the validated client certificate, key and CA bytes.
func (m TransportMaterial) Certificate() [3]string { return m.certificate }

// ProxyComponents returns explicit proxy address, username and password for Git.
func (m TransportMaterial) ProxyComponents() [3]string {
	if m.proxy == nil {
		return [3]string{}
	}
	return [3]string{m.proxy.Address, m.proxy.Username, m.proxy.Password}
}

// ProxyURL selects the captured route for an actual request or redirect URL.
func (m TransportMaterial) ProxyURL(target *url.URL) (*url.URL, error) {
	if m.proxyURL != nil {
		selected := *m.proxyURL
		return &selected, nil
	}
	if m.environment == nil {
		return nil, nil
	}
	return m.environment.ProxyFunc()(target)
}

// HTTPPurpose selects the existing operation bounds for HTTP clients and contexts.
type HTTPPurpose uint8

const (
	// HTTPStream bounds setup and headers without a whole-body deadline.
	HTTPStream HTTPPurpose = iota
	// HTTPChart bounds a complete chart or index request.
	HTTPChart
	// HTTPResource bounds resource acquisition through its caller context.
	HTTPResource
)

var requestBounds = struct {
	header, dial time.Duration
	request      [3]time.Duration
}{120 * time.Second, 30 * time.Second, [3]time.Duration{0, 120 * time.Second, 5 * time.Second}}

// Timeout returns the operation's bound, or zero for streamed bodies.
func (p HTTPPurpose) Timeout() time.Duration { return requestBounds.request[p] }

// NewHTTPClient applies source client policy to an existing transport.
func NewHTTPClient(transport http.RoundTripper, purpose HTTPPurpose) *http.Client {
	client := &http.Client{Transport: transport}
	if purpose == HTTPChart {
		client.Timeout = purpose.Timeout()
	}
	return client
}

// NewHTTPTransport builds the concrete guarded kernel required by HTTP libraries.
func NewHTTPTransport(material TransportMaterial) *http.Transport {
	material = material.captureEnvironment()
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.ResponseHeaderTimeout = requestBounds.header
	if material.config != nil {
		tr.TLSClientConfig = material.config.Clone()
	}
	if material.proxyURL != nil {
		tr.Proxy = http.ProxyURL(material.proxyURL)
	} else {
		selectProxy := material.environment.ProxyFunc()
		tr.Proxy = func(r *http.Request) (*url.URL, error) { return selectProxy(r.URL) }
	}
	ssrfguard.WrapTransport(tr, &net.Dialer{Timeout: requestBounds.dial, KeepAlive: requestBounds.dial})
	return tr
}

// HTTPTransportConfig specifies immutable effective transport material.
// Zero Material captures environment policy at selection time.
// Credentials separate source security boundaries and remain client-bound.
// Compatible OCI clients share upstream auth tokens through the selected transport.
type HTTPTransportConfig struct {
	Material           TransportMaterial
	Insecure           bool
	Credentials        any
	DisableCompression bool
}

// HTTPTransport selects a fully configured, guarded transport for this invocation.
// The Network MUST be non-nil and outlive every borrower.
func (n *Network) HTTPTransport(cfg HTTPTransportConfig) (http.RoundTripper, error) {
	material := cfg.Material.captureEnvironment()
	environment := httpproxy.Config{}
	if material.environment != nil {
		environment = *material.environment
	}
	proxyIdentity := ""
	if material.proxyURL != nil {
		proxyIdentity = material.proxyURL.String()
	}
	pem := material.certificate
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
	tr := NewHTTPTransport(material)
	if cfg.Insecure {
		if tr.TLSClientConfig == nil {
			tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		tr.TLSClientConfig.InsecureSkipVerify = true
	}
	tr.Protocols = new(http.Protocols)
	tr.Protocols.SetHTTP1(true)
	// A cloned transport may advertise h2; ALPN MUST match Protocols.
	if tr.TLSClientConfig != nil {
		tr.TLSClientConfig.NextProtos = []string{"http/1.1"}
	}
	tr.DisableCompression = cfg.DisableCompression
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
