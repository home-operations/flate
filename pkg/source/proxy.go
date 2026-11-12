package source

import (
	"fmt"
	"net/url"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
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

	material, err := proxyValues(&sec, &owner, ref.Name)
	if err != nil {
		return nil, nil, err
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

func proxyValues(sec *store.ReferenceFacts, ownerValue *manifest.DiagnosticIdentity, name string) ([3]string, error) {
	owner := *ownerValue
	var material [3]string
	var results [3]values.Result
	for i, key := range []string{"address", "username", "password"} {
		valueResult, err := SecretValue(sec, new(owner), name, key)
		value := valueResult.Value()
		if err != nil {
			return [3]string{}, fmt.Errorf("proxySecretRef %s/%s: %w", owner.ID.Namespace, name, err)
		}
		material[i], results[i] = value, valueResult
	}
	for i := range results {
		result := &results[i]
		if i != 0 && result.State() == values.Produced {
			continue
		}
		if result.State() != values.Readable && result.State() != values.MissingKey {
			return [3]string{}, UnavailableSecret(new(owner), name, result)
		}
	}
	return material, nil
}
