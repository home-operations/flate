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

func resolveProxy(
	secrets SecretGetter, ownerValue *manifest.DiagnosticIdentity,
	ref *manifest.LocalObjectReference,
) (*proxyConfig, *url.URL, error) {
	owner := *ownerValue
	if ref == nil {
		return nil, nil, nil
	}
	sec, err := resolveSecretRef(secrets, new(owner), "proxySecretRef", ref)
	if err != nil {
		return nil, nil, err
	}

	var material [3]string
	for i, key := range []string{"address", "username", "password"} {
		valueResult, err := sec.Value(key)
		value := valueResult.Value
		if err != nil {
			return nil, nil, fmt.Errorf("proxySecretRef %s/%s: %w", owner.ID.Namespace, ref.Name, err)
		}
		material[i] = value
	}
	proxy := &proxyConfig{Address: material[0], Username: material[1], Password: material[2]}
	var selected *url.URL
	if proxy.Address != "" {
		var err error
		selected, err = url.Parse(proxy.Address)
		if err != nil {
			return nil, nil, manifest.NewError(
				manifest.ErrInput,
				nil,
				fmt.Sprintf("proxySecretRef %s/%s: invalid proxy address", owner.ID.Namespace, ref.Name),
			)
		}
		if proxy.Username != "" {
			selected.User = url.UserPassword(proxy.Username, proxy.Password)
		}
	}
	if material[0] == "" {
		if material[1] != "" || material[2] != "" {
			return nil, nil, manifest.NewError(
				manifest.ErrInput,
				nil,
				fmt.Sprintf("proxy secret %s/%s missing required 'address' key", owner.ID.Namespace, ref.Name),
			)
		}
		return nil, nil, MissingSecretErr(owner, ref.Name, "missing required 'address' key")
	}
	return proxy, selected, nil
}
