package oci

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
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
func (f *operation) newRepoClient(ctx context.Context, repo *manifest.OCIRepository, credStore credentials.Store, policy cache.Policy) (*remote.Repository, any, error) {
	reference, err := parseOCIRef(repo.URL)
	if err != nil {
		return nil, nil, err
	}
	client, err := remote.NewRepository(reference)
	if err != nil {
		return nil, nil, fmt.Errorf("oras: %w", err)
	}
	owner := repo.Named()
	owner.Name = repo.Namespace + "/" + repo.Name
	resolved, err := source.ResolveTransport(f.secrets, owner, []source.TransportInput{
		{Reference: repo.CertSecretRef, Use: source.TransportCert},
		{Reference: repo.ProxySecretRef, Use: source.TransportProxy},
	})
	if err != nil {
		return nil, nil, err
	}
	if _, err := resolved.ProxyURL(&url.URL{Scheme: "https", Host: client.Reference.Host()}); err != nil {
		return nil, nil, fmt.Errorf("%w: resolve registry proxy: invalid proxy configuration", manifest.ErrInput)
	}
	if credStore == nil {
		credStore, err = f.resolveRegistryConfig(ctx, repo, policy)
		if err != nil {
			return nil, nil, err
		}
	}
	cred := auth.EmptyCredential
	if credStore != nil {
		cred, err = credentials.Credential(credStore)(ctx, client.Reference.Host())
		if err != nil {
			return nil, nil, fmt.Errorf("%w: resolve registry credentials: %w", manifest.ErrInput, err)
		}
	}
	if cred == auth.EmptyCredential && repo.Provider != "" && repo.Provider != sourcev1.GenericOCIProvider {
		if repo.SecretRef != nil {
			missing := source.MissingSecretErr(repo.Named(), repo.SecretRef.Name, "no credentials for the source registry")
			_, err := f.registryFallback(ctx, repo, missing, policy)
			return nil, nil, err
		}
		return nil, nil, source.MissingSecretErr(repo.Named(), "", "provider authentication unavailable offline")
	}
	var credentials any
	if cred != auth.EmptyCredential {
		credentials = cred
	}
	material := resolved.Identity(credentials)
	tr, err := f.Network.HTTPTransport(source.HTTPTransportConfig{Material: resolved, Insecure: repo.Insecure, Credentials: credentials})
	if err != nil {
		return nil, nil, err
	}
	client.Client = &auth.Client{Client: &http.Client{Transport: tr}, Cache: tr.(auth.Cache), Credential: auth.StaticCredential(client.Reference.Registry, cred)}
	return client, material, nil
}

// resolveRegistryConfig keeps Secret credentials in memory. Explicit registry
// config and Docker defaults retain their upstream stores. Global credentials
// MUST wait for the source controller to exhaust rendered Secrets; eligible
// retries are carried by MissingSecretError.
func (f *operation) resolveRegistryConfig(ctx context.Context, repo *manifest.OCIRepository, policy cache.Policy) (credentials.Store, error) {
	if repo.SecretRef == nil {
		return loadCredentials(f.RegistryConfig)
	}
	if f.secrets == nil {
		return nil, fmt.Errorf("%s references secretRef but no source.SecretGetter is wired", source.QualifiedName(manifest.KindOCIRepository, repo.Namespace, repo.Name))
	}
	sec := f.secrets(repo.Namespace, repo.SecretRef.Name)
	if sec == nil {
		return f.registryFallback(ctx, repo, source.MissingSecretErr(repo.Named(), repo.SecretRef.Name, "not found"), policy)
	}
	configJSON, _, err := sec.Value(dockerConfigJSONKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %s read .dockerconfigjson: %w", manifest.ErrInput, source.QualifiedName(manifest.KindOCIRepository, repo.Namespace, repo.Name), err)
	}
	if configJSON == "" || manifest.HasValuePlaceholder(configJSON) {
		return f.registryFallback(ctx, repo, source.MissingSecretErr(repo.Named(), repo.SecretRef.Name, "missing .dockerconfigjson (must be type kubernetes.io/dockerconfigjson)"), policy)
	}
	credStore, err := credentials.NewMemoryStoreFromDockerConfig([]byte(configJSON))
	if err != nil {
		return nil, fmt.Errorf("%w: %s parse .dockerconfigjson: %w", manifest.ErrInput, source.QualifiedName(manifest.KindOCIRepository, repo.Namespace, repo.Name), err)
	}
	// ORAS normalizes Docker Hub's credential key while its callback expects the
	// canonical Docker server address. Reconcile these in the same memory store.
	if cred, _ := credStore.Get(ctx, "index.docker.io"); cred != auth.EmptyCredential {
		_ = credStore.Put(ctx, credentials.ServerAddressFromRegistry("docker.io"), cred)
	}
	return credStore, nil
}

// Unrelated registry credentials MUST NOT enable an ambient retry.
func (f *operation) registryFallback(ctx context.Context, repo *manifest.OCIRepository, missing error, policy cache.Policy) (credentials.Store, error) {
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
	credStore, configErr := loadCredentials(f.RegistryConfig)
	if configErr == nil {
		if credStore == nil {
			return nil, missing
		}
		var cred auth.Credential
		cred, configErr = credentials.Credential(credStore)(ctx, ref.Host())
		if configErr == nil && cred == auth.EmptyCredential {
			return nil, missing
		}
	}
	mse.RetryAmbient = func(ctx context.Context) (*store.SourceArtifact, error) {
		if configErr != nil {
			return nil, fmt.Errorf("%w: %w", manifest.ErrInput, configErr)
		}
		slog.Debug("oci: secretRef unresolvable, using registry config",
			"id", source.QualifiedName(manifest.KindOCIRepository, repo.Namespace, repo.Name), "registry", ref.Registry)
		credentials, err := loadCredentials(f.RegistryConfig)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", manifest.ErrInput, err)
		}
		return f.fetchWithCredentials(ctx, repo, credentials, policy)
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
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		slog.Debug("oci: docker credentials load failed; falling back to anonymous pulls", "err", err)
		return nil, nil
	}
	return s, nil
}
