package oci

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/store"
)

// dockerConfigJSONKey is the Secret data key a kubernetes.io/dockerconfigjson
// Secret stores its credentials under.
const dockerConfigJSONKey = ".dockerconfigjson"

// newRepoClient builds the oras registry client for repo: it parses the ref,
// uses the supplied credentials, and wires a bounded HTTP transport
// (TLS + proxy) plus docker-style auth.
//
// Retries are owned by the Fetch-level retry decorator (source.WithRetry) so
// they happen once, uniformly, across every source kind — this client uses a
// plain, NON-retrying transport. source.NewHTTPTransport always returns a
// bounded transport (a http.DefaultTransport clone carrying
// ResponseHeaderTimeout as a liveness backstop, plus any TLS/proxy), so oras
// never falls back to its retry-enabled auth.DefaultClient and a black-holed
// registry can't hang the fetch waiting on response headers.
func newRepoClient(repo *manifest.OCIRepository, credStore credentials.Store, tlsCfg *tls.Config, proxy *source.ProxyConfig) (*remote.Repository, error) {
	// parseOCIRef strips any oci:// prefix itself, so pass repo.URL as-is.
	reference, err := parseOCIRef(repo.URL)
	if err != nil {
		return nil, err
	}
	repoClient, err := remote.NewRepository(reference)
	if err != nil {
		return nil, fmt.Errorf("oras: %w", err)
	}
	transport, err := source.NewHTTPTransport(tlsCfg, proxy)
	if err != nil {
		return nil, err
	}
	authClient := &auth.Client{Client: &http.Client{Transport: transport}}
	if credStore != nil {
		authClient.Credential = credentials.Credential(credStore)
	}
	repoClient.Client = authClient
	return repoClient, nil
}

// resolveTLS builds a *tls.Config from spec.certSecretRef (PEM-encoded
// tls.crt + tls.key + ca.crt — any subset acceptable) and/or spec.insecure.
// Returns nil when no TLS customization is needed.
func (f *Fetcher) resolveTLS(repo *manifest.OCIRepository) (*tls.Config, error) {
	if repo.CertSecretRef == nil && !repo.Insecure {
		return nil, nil
	}
	cfg, err := source.ResolveCertSecret(f.Secrets, repo.Namespace, "OCIRepository",
		repo.Namespace+"/"+repo.Name, repo.CertSecretRef)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		cfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if repo.Insecure {
		cfg.InsecureSkipVerify = true //nolint:gosec // honoring user-declared spec.insecure
	}
	return cfg, nil
}

// resolveRegistryConfig keeps Secret credentials in memory. Explicit registry
// config and Docker defaults retain their upstream stores. Global credentials
// MUST wait for the source controller to exhaust rendered Secrets; eligible
// retries are carried by MissingSecretError.
func (f *Fetcher) resolveRegistryConfig(ctx context.Context, repo *manifest.OCIRepository) (credentials.Store, error) {
	if repo.SecretRef == nil {
		return loadCredentials(f.RegistryConfig)
	}
	if f.Secrets == nil {
		return nil, fmt.Errorf("%s references secretRef but no source.SecretGetter is wired", ociID(repo))
	}
	sec := f.Secrets(repo.Namespace, repo.SecretRef.Name)
	if sec == nil {
		return f.registryFallback(ctx, repo, source.MissingSecretErr("OCIRepository", repo.Namespace, repo.Name, repo.SecretRef.Name, "not found"))
	}
	configJSON, _, err := sec.Value(dockerConfigJSONKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %s read .dockerconfigjson: %w", manifest.ErrInput, ociID(repo), err)
	}
	if configJSON == "" || manifest.HasValuePlaceholder(configJSON) {
		return f.registryFallback(ctx, repo, source.MissingSecretErr("OCIRepository", repo.Namespace, repo.Name, repo.SecretRef.Name, "missing .dockerconfigjson (must be type kubernetes.io/dockerconfigjson)"))
	}
	credStore, err := credentials.NewMemoryStoreFromDockerConfig([]byte(configJSON))
	if err != nil {
		return nil, fmt.Errorf("%w: %s parse .dockerconfigjson: %w", manifest.ErrInput, ociID(repo), err)
	}
	// ORAS normalizes Docker Hub's credential key while its callback expects the
	// canonical Docker server address. Reconcile these in the same memory store.
	if cred, _ := credStore.Get(ctx, "index.docker.io"); cred != auth.EmptyCredential {
		_ = credStore.Put(ctx, credentials.ServerAddressFromRegistry("docker.io"), cred)
	}
	return credStore, nil
}

// registryFallback preserves the missing Secret until renders are exhausted.
// A pull retry requires a credential for this registry; unrelated credentials
// MUST leave producer-backed sources skippable.
func (f *Fetcher) registryFallback(ctx context.Context, repo *manifest.OCIRepository, missing error) (credentials.Store, error) {
	parsed, err := parseOCIRef(repo.URL)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", manifest.ErrInput, err)
	}
	// parseOCIRef validates URL syntax, not ORAS repository syntax.
	ref, err := registry.ParseReference(parsed)
	if err != nil {
		return nil, fmt.Errorf("%w: parse registry reference: %w", manifest.ErrInput, err)
	}
	mse, _ := errors.AsType[*source.MissingSecretError](missing)
	credStore, err := loadCredentials(f.RegistryConfig)
	if err != nil {
		// A corrupt global config MUST not preempt a later rendered Secret.
		configErr := fmt.Errorf("%w: %w", manifest.ErrInput, err)
		mse.RetryWithRegistryConfig = func(_ context.Context, _ string) (*store.SourceArtifact, error) {
			return nil, configErr
		}
		return nil, mse
	}
	if credStore == nil {
		return nil, missing
	}
	// A broken credential helper is treated as no credential, preserving skips.
	if cred, err := credentials.Credential(credStore)(ctx, ref.Host()); err != nil || cred == auth.EmptyCredential {
		return nil, missing
	}
	mse.RegistryConfig = f.RegistryConfig
	mse.RetryWithRegistryConfig = func(ctx context.Context, configPath string) (*store.SourceArtifact, error) {
		slog.Debug("oci: secretRef unresolvable, using registry config",
			"id", ociID(repo), "registry", ref.Registry)
		credStore, err := loadCredentials(configPath)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", manifest.ErrInput, err)
		}
		return f.fetchWithCredentials(ctx, repo, credStore)
	}
	return nil, mse
}

// loadCredentials returns a credentials.Store backed by the given config
// path. An empty configPath uses the docker default lookup.
func loadCredentials(configPath string) (credentials.Store, error) {
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
		// on this machine) from permission / corrupt-JSON errors so an
		// operator running flate with a broken ~/.docker/config.json
		// gets a breadcrumb instead of a silent "401 unauthorized"
		// from the registry.
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		slog.Debug("oci: docker credentials load failed; falling back to anonymous pulls",
			"err", err)
		return nil, nil
	}
	return s, nil
}
