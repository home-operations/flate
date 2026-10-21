package source

import (
	"fmt"
	"net/url"

	"github.com/home-operations/flate/pkg/manifest"
)

type proxyConfig struct {
	Address  string
	Username string
	Password string
}

func resolveProxy(secrets SecretGetter, owner manifest.NamedResource, ref *manifest.LocalObjectReference) (*proxyConfig, *url.URL, error) {
	if ref == nil {
		return nil, nil, nil
	}
	sec, err := resolveSecretRef(secrets, owner, "proxySecretRef", ref)
	if err != nil {
		return nil, nil, err
	}

	var material [3]string
	for i, key := range []string{"address", "username", "password"} {
		value, _, err := sec.Value(key)
		if err != nil {
			return nil, nil, fmt.Errorf("%s %s: proxySecretRef %s/%s: %w", owner.Kind, owner.Name, owner.Namespace, ref.Name, err)
		}
		material[i] = value
	}
	proxy := &proxyConfig{Address: material[0], Username: material[1], Password: material[2]}
	var selected *url.URL
	if proxy.Address != "" && !manifest.HasValuePlaceholder(proxy.Address) {
		var err error
		selected, err = url.Parse(proxy.Address)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %s %s: proxySecretRef %s/%s: invalid proxy address", manifest.ErrInput, owner.Kind, owner.Name, owner.Namespace, ref.Name)
		}
		if proxy.Username != "" {
			selected.User = url.UserPassword(proxy.Username, proxy.Password)
		}
	}
	for _, value := range material {
		if manifest.HasValuePlaceholder(value) {
			return nil, nil, MissingSecretErr(owner, ref.Name, "proxy material unavailable")
		}
	}
	if material[0] == "" {
		if material[1] != "" || material[2] != "" {
			return nil, nil, fmt.Errorf("%w: proxy secret %s/%s missing required 'address' key", manifest.ErrInput, owner.Namespace, ref.Name)
		}
		return nil, nil, MissingSecretErr(owner, ref.Name, "missing required 'address' key")
	}
	return proxy, selected, nil
}
