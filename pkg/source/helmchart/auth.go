package helmchart

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"golang.org/x/net/http/httpproxy"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// httpRepository fixes auth and transport inputs for a chart and its nested index.
type httpRepository struct {
	*manifest.HelmRepository
	identity, username, password string
	client                       *http.Client
	material                     any
}

func (f *Fetcher) prepareHTTP(r *manifest.HelmRepository) (*httpRepository, error) {
	secrets := f.secrets
	var certMaterial any
	if secrets != nil {
		captured := make(map[string]*manifest.Secret)
		secrets = func(ns, name string) *manifest.Secret {
			sec, ok := captured[name]
			if !ok {
				sec = f.secrets(ns, name)
				captured[name] = sec
			}
			if sec != nil && r.CertSecretRef != nil && name == r.CertSecretRef.Name {
				certMaterial = [2]any{sec.Data, sec.StringData}
			}
			return sec
		}
	}
	prepared := &httpRepository{HelmRepository: r}
	if r.SecretRef != nil {
		if secrets == nil {
			return nil, fmt.Errorf("%w: %s references secretRef but no SecretGetter is wired", manifest.ErrMissingSecret, helmID(r))
		}
		sec := secrets(r.Namespace, r.SecretRef.Name)
		if sec == nil {
			return nil, source.MissingSecretErr("HelmRepository", r.Namespace, r.Name, r.SecretRef.Name, "not found")
		}
		var err error
		prepared.username, prepared.password, err = source.BasicAuthFromSecret(sec, "HelmRepository", r.Namespace, r.Name, r.SecretRef.Name)
		if err != nil {
			return nil, err
		}
	}
	tlsCfg, err := source.ResolveCertSecret(secrets, r.Namespace, "HelmRepository", helmID(r), r.CertSecretRef)
	if err != nil {
		return nil, err
	}
	proxyPolicy := httpproxy.FromEnvironment()

	// Chart URLs and redirects may use a proxy even when the index is bypassed.
	var proxyIdentity any
	if proxyPolicy.HTTPProxy != "" || proxyPolicy.HTTPSProxy != "" {
		proxyIdentity = proxyPolicy
	}

	if prepared.username != "" || prepared.password != "" || certMaterial != nil || proxyIdentity != nil {
		prepared.material = [4]any{prepared.username, prepared.password, certMaterial, proxyIdentity}
		prepared.identity = manifest.Fingerprint(prepared.material)
	}
	memo, ok := f.clients.Load(prepared.identity)
	if !ok {
		entry := &httpClient{}
		entry.init = sync.OnceValues(func() (*http.Client, error) {
			tr, err := helmTransport(tlsCfg)
			if err != nil {
				return nil, err
			}
			proxyForURL := proxyPolicy.ProxyFunc()
			tr.Proxy = func(req *http.Request) (*url.URL, error) { return proxyForURL(req.URL) }
			entry.transport = tr
			return &http.Client{Transport: f.network.HTTPTransport(tr), Timeout: helmHTTPTimeout}, nil
		})
		memo, _ = f.clients.LoadOrStore(prepared.identity, entry)
	}
	prepared.client, err = memo.(*httpClient).init()
	if err != nil {
		return nil, err
	}
	return prepared, nil
}

// The transport MUST remain accessible when Network wraps the client's RoundTripper.
type httpClient struct {
	init      func() (*http.Client, error)
	transport *http.Transport
}

// helmTransport builds the helm-specific HTTP transport kernel: the SSRF
// egress guard (source.NewHTTPTransport) carrying tlsCfg, with compression
// disabled to match helm's own getter transport (chart tarballs are already
// compressed — the only source kind that disables it).
func helmTransport(tlsCfg *tls.Config) (*http.Transport, error) {
	tr, err := source.NewHTTPTransport(tlsCfg, nil)
	if err != nil {
		return nil, err
	}
	tr.DisableCompression = true
	return tr, nil
}
