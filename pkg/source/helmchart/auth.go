package helmchart

import (
	"fmt"
	"net/http"

	"golang.org/x/net/http/httpproxy"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

// httpRepository fixes auth and transport inputs for a chart and its nested index.
type httpRepository struct {
	*manifest.HelmRepository
	username, password string
	client             *http.Client
	material           any
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
			return nil, source.MissingSecretErr(r.Named(), r.SecretRef.Name, "not found")
		}
		var err error
		prepared.username, prepared.password, err = source.BasicAuthFromSecret(sec, r.Named(), r.SecretRef.Name)
		if err != nil {
			return nil, err
		}
	}
	owner := r.Named()
	owner.Name = helmID(r)
	tlsMaterial, err := source.ResolveCertSecret(secrets, owner, r.CertSecretRef)
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
	}
	var credentials any
	if prepared.username != "" || prepared.password != "" {
		credentials = [2]string{prepared.username, prepared.password}
	}
	tr, err := f.network.HTTPTransport(source.HTTPTransportConfig{TLS: tlsMaterial, ProxyEnvironment: proxyPolicy,
		Credentials: credentials, DisableCompression: true})
	if err != nil {
		return nil, err
	}
	prepared.client = &http.Client{Transport: tr, Timeout: helmHTTPTimeout}
	return prepared, nil
}
