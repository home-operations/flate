package oci

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"

	"golang.org/x/net/http/httpproxy"

	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// dockerConfigJSONKey is the Secret data key a kubernetes.io/dockerconfigjson
// Secret stores its credentials under.
const dockerConfigJSONKey = ".dockerconfigjson"

// newRepoClient binds one credential and transport snapshot before cache admission.
func (f *Fetcher) newRepoClient(ctx context.Context, repo *manifest.OCIRepository, credStore credentials.Store) (*remote.Repository, any, error) {
	reference, err := parseOCIRef(repo.URL)
	if err != nil {
		return nil, nil, err
	}
	client, err := remote.NewRepository(reference)
	if err != nil {
		return nil, nil, fmt.Errorf("oras: %w", err)
	}
	cred := auth.EmptyCredential
	if credStore != nil {
		cred, err = credentials.Credential(credStore)(ctx, client.Reference.Host())
		if err != nil {
			return nil, nil, fmt.Errorf("%w: resolve registry credentials: %w", manifest.ErrInput, err)
		}
	}
	var certMaterial any
	prepared := *f
	if f.Secrets != nil {
		prepared.Secrets = func(ns, name string) *manifest.Secret {
			sec := f.Secrets(ns, name)
			if sec != nil {
				certMaterial = [2]any{sec.Data, sec.StringData}
			}
			return sec
		}
	}
	tlsCfg, err := prepared.resolveTLS(repo)
	if err != nil {
		return nil, nil, err
	}
	owner := repo.Named()
	owner.Name = repo.Namespace + "/" + repo.Name
	proxy, err := source.ResolveProxy(f.Secrets, owner, repo.ProxySecretRef)
	if err != nil {
		return nil, nil, err
	}
	proxyPolicy := httpproxy.FromEnvironment()
	tr, err := source.NewHTTPTransport(tlsCfg, proxy)
	if err != nil {
		return nil, nil, err
	}
	if proxy == nil {
		proxyForURL := proxyPolicy.ProxyFunc()
		tr.Proxy = func(req *http.Request) (*url.URL, error) { return proxyForURL(req.URL) }
	}

	target := &url.URL{Scheme: "https", Host: client.Reference.Host()}
	selected, err := tr.Proxy(&http.Request{URL: target})
	if err != nil {
		return nil, nil, fmt.Errorf("%w: resolve registry proxy: %w", manifest.ErrInput, err)
	}
	var proxyIdentity any
	if proxy == nil && (proxyPolicy.HTTPProxy != "" || proxyPolicy.HTTPSProxy != "") {
		proxyIdentity = proxyPolicy
	} else if selected != nil {
		proxyIdentity = selected.String()
	}
	var material any
	if cred != auth.EmptyCredential || certMaterial != nil || proxyIdentity != nil {
		material = [3]any{cred, certMaterial, proxyIdentity}
	}
	client.Client = &auth.Client{Client: &http.Client{Transport: f.Network.HTTPTransport(tr)},
		Credential: auth.StaticCredential(client.Reference.Registry, cred)}
	return client, material, nil
}

// resolveTLS builds a *tls.Config from spec.certSecretRef (PEM-encoded
// tls.crt + tls.key + ca.crt — any subset acceptable) and/or spec.insecure.
// Returns nil when no TLS customization is needed.
func (f *Fetcher) resolveTLS(repo *manifest.OCIRepository) (*tls.Config, error) {
	if repo.CertSecretRef == nil && !repo.Insecure {
		return nil, nil
	}
	owner := repo.Named()
	owner.Name = repo.Namespace + "/" + repo.Name
	cfg, err := source.ResolveCertSecret(f.Secrets, owner, repo.CertSecretRef)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		cfg = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if repo.Insecure {
		cfg.InsecureSkipVerify = true
	}
	return cfg, nil
}

// resolveRegistryConfig keeps Secret credentials in memory. Explicit registry
// config and Docker defaults retain their upstream stores. Global credentials
// MUST wait for the source controller to exhaust rendered Secrets; eligible
// retries are carried by MissingSecretError.
func (f *Fetcher) resolveRegistryConfig(ctx context.Context, repo *manifest.OCIRepository, retry cache.RetryConfig) (credentials.Store, error) {
	if repo.SecretRef == nil {
		return loadCredentials(f.RegistryConfig)
	}
	if f.Secrets == nil {
		return nil, fmt.Errorf("%s references secretRef but no source.SecretGetter is wired", ociID(repo))
	}
	sec := f.Secrets(repo.Namespace, repo.SecretRef.Name)
	if sec == nil {
		return f.registryFallback(ctx, repo, source.MissingSecretErr(repo.Named(), repo.SecretRef.Name, "not found"), retry)
	}
	configJSON, _, err := sec.Value(dockerConfigJSONKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %s read .dockerconfigjson: %w", manifest.ErrInput, ociID(repo), err)
	}
	if configJSON == "" || manifest.HasValuePlaceholder(configJSON) {
		return f.registryFallback(ctx, repo, source.MissingSecretErr(repo.Named(), repo.SecretRef.Name, "missing .dockerconfigjson (must be type kubernetes.io/dockerconfigjson)"), retry)
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
func (f *Fetcher) registryFallback(ctx context.Context, repo *manifest.OCIRepository, missing error, retry cache.RetryConfig) (credentials.Store, error) {
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
		return f.fetchWithCredentials(ctx, repo, credStore, retry)
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
