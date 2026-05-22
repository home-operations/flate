package source

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/home-operations/flate/pkg/manifest"
)

// ProxyConfig is the resolved view of a Flux spec.proxySecretRef.
// Matches source-controller's Secret schema: data.address (required),
// data.username + data.password (optional, basic-auth).
type ProxyConfig struct {
	Address  string
	Username string
	Password string
}

// URL parses Address into a *url.URL. The caller pre-validated
// non-empty by checking the return of ResolveProxy.
func (p *ProxyConfig) URL() (*url.URL, error) {
	u, err := url.Parse(p.Address)
	if err != nil {
		return nil, fmt.Errorf("parse proxy address %q: %w", p.Address, err)
	}
	if p.Username != "" {
		u.User = url.UserPassword(p.Username, p.Password)
	}
	return u, nil
}

// HTTPProxyFunc returns a net/http.Transport.Proxy function pinned to
// this proxy. Use when configuring an http.Transport for the OCI,
// Bucket, or HTTP-Git transports.
func (p *ProxyConfig) HTTPProxyFunc() (func(*http.Request) (*url.URL, error), error) {
	u, err := p.URL()
	if err != nil {
		return nil, err
	}
	return http.ProxyURL(u), nil
}

// ResolveProxy reads ref's Secret via secrets and decodes it into a
// ProxyConfig. Returns (nil, nil) when ref is nil — proxy is opt-in.
// Surfaces a loud error when ref is set but the Secret is missing or
// lacks the required address key, matching source-controller's
// fail-loud behavior.
func ResolveProxy(secrets SecretGetter, ns, ownerKind, ownerID string, ref *manifest.LocalObjectReference) (*ProxyConfig, error) {
	if ref == nil {
		return nil, nil
	}
	if secrets == nil {
		return nil, fmt.Errorf("%s %s references proxySecretRef but no source.SecretGetter is wired",
			ownerKind, ownerID)
	}
	sec := secrets(ns, ref.Name)
	if sec == nil {
		return nil, fmt.Errorf("%s %s: proxy secret %s/%s not found",
			ownerKind, ownerID, ns, ref.Name)
	}
	addr := StringFromSecret(sec, "address")
	if addr == "" {
		return nil, fmt.Errorf("%s %s: proxy secret %s/%s missing required 'address' key",
			ownerKind, ownerID, ns, ref.Name)
	}
	return &ProxyConfig{
		Address:  addr,
		Username: StringFromSecret(sec, "username"),
		Password: StringFromSecret(sec, "password"),
	}, nil
}
