package source

import (
	"fmt"
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

// URL parses Address into a *url.URL with optional basic-auth user
// info applied. The caller pre-validated non-empty by checking the
// return of ResolveProxy. Used by NewHTTPTransport when pinning a
// transport's Proxy function; the OCI / Bucket / HTTP-Git transports
// pick this up uniformly.
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

// ResolveProxy reads the configured proxy address and optional credentials.
// Missing or wiped material returns MissingSecretError; a nil ref disables proxying.
func ResolveProxy(secrets SecretGetter, ns, ownerKind, ownerID string, ref *manifest.LocalObjectReference) (*ProxyConfig, error) {
	if ref == nil {
		return nil, nil
	}
	sec, err := resolveSecretRef(secrets, ns, ownerKind, ownerID, "proxySecretRef", ref)
	if err != nil {
		return nil, err
	}

	var material [3]string
	for i, key := range []string{"address", "username", "password"} {
		value, _, err := sec.Value(key)
		if err != nil {
			return nil, fmt.Errorf("proxy secret %s/%s: %w", ns, ref.Name, err)
		}
		if manifest.HasValuePlaceholder(value) {
			return nil, MissingSecretErr(ownerKind, ns, ownerID, ref.Name, "proxy material unavailable")
		}
		material[i] = value
	}
	if material[0] == "" {
		if material[1] != "" || material[2] != "" {
			return nil, fmt.Errorf("%w: proxy secret %s/%s missing required 'address' key", manifest.ErrInput, ns, ref.Name)
		}
		return nil, MissingSecretErr(ownerKind, ns, ownerID, ref.Name, "missing required 'address' key")
	}
	return &ProxyConfig{Address: material[0], Username: material[1], Password: material[2]}, nil
}
