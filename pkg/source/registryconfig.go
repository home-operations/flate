package source

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
)

// RegistryCredentialStore returns a credentials.Store backed by the given
// docker-style config path (the --registry-config value). An empty
// configPath uses docker's default lookup (~/.docker/config.json,
// honoring DOCKER_CONFIG). A missing docker default is not fatal — a nil
// store is returned and the caller proceeds anonymously; permission /
// corrupt-JSON failures surface as errors instead of a silent "401
// unauthorized" from the registry.
func RegistryCredentialStore(configPath string) (credentials.Store, error) {
	if configPath != "" {
		s, err := credentials.NewFileStore(configPath)
		if err != nil {
			return nil, fmt.Errorf("load credentials %s: %w", configPath, err)
		}
		return s, nil
	}
	s, err := credentials.NewStoreFromDocker(credentials.StoreOptions{AllowPlaintextPut: false})
	if err != nil {
		// Missing docker config is not fatal — anonymous pulls work.
		// Distinguish os.ErrNotExist (the common case: no docker login
		// on this machine) from the other failures logged below so an
		// operator running flate with a broken ~/.docker/config.json
		// still gets a breadcrumb.
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		slog.Debug("docker credentials load failed; falling back to anonymous pulls",
			"err", err)
		return nil, nil
	}
	return s, nil
}

// CredentialForHost probes store for a credential for the given registry
// host — the affirmative evidence an unresolvable secretRef needs before
// a fetch may fall back to a global credential source. A store error
// (missing credential-helper binary, unreadable config, …) counts as no
// credential: the fallback must not invent an auth failure the
// ErrMissingSecret path would have soft-skipped.
func CredentialForHost(ctx context.Context, store credentials.Store, host string) (auth.Credential, bool) {
	cred, err := store.Get(ctx, host)
	return cred, err == nil && cred != auth.EmptyCredential
}
