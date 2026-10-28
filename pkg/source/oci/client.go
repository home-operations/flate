package oci

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"helm.sh/helm/v4/pkg/helmpath"
	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

const dockerConfigJSONKey = ".dockerconfigjson"

func (f *operation) newRepoClient(ctx context.Context, repo *manifest.OCIRepository, selected *auth.Credential, policy cache.Policy) (registryAcquisition, error) {
	reference, err := parseOCIRef(repo.URL)
	if err != nil {
		return registryAcquisition{}, err
	}
	client, err := remote.NewRepository(reference)
	if err != nil {
		return registryAcquisition{}, fmt.Errorf("oras: %w", err)
	}
	owner := repo.Named()
	owner.Name = repo.Namespace + "/" + repo.Name
	resolved, err := source.ResolveTransport(f.secrets, owner, []source.TransportInput{
		{Reference: repo.CertSecretRef, Use: source.TransportCert},
		{Reference: repo.ProxySecretRef, Use: source.TransportProxy},
	})
	if err != nil {
		return registryAcquisition{}, err
	}
	if _, err := resolved.ProxyURL(&url.URL{Scheme: "https", Host: client.Reference.Host()}); err != nil {
		return registryAcquisition{}, fmt.Errorf("%w: resolve registry proxy: invalid proxy configuration", manifest.ErrInput)
	}
	if selected == nil {
		selected, err = f.resolveRegistryConfig(ctx, repo, client.Reference.Host(), policy)
		if err != nil {
			return registryAcquisition{}, err
		}
	}
	cred := *selected
	if cred == auth.EmptyCredential && repo.Provider != "" && repo.Provider != sourcev1.GenericOCIProvider {
		if repo.SecretRef != nil {
			missing := source.MissingSecretErr(repo.Named(), repo.SecretRef.Name, "no credentials for the source registry")
			_, err := f.registryFallback(ctx, repo, missing, policy)
			return registryAcquisition{}, err
		}
		return registryAcquisition{}, source.MissingSecretErr(repo.Named(), "", "provider authentication unavailable offline")
	}
	var credentials any
	if cred != auth.EmptyCredential {
		credentials = cred
	}
	material := resolved.Identity(credentials)
	tr, err := f.Network.HTTPTransport(source.HTTPTransportConfig{Material: resolved, Insecure: repo.Insecure, Credentials: credentials})
	if err != nil {
		return registryAcquisition{}, err
	}
	client.Client = &auth.Client{Client: &http.Client{Transport: tr}, Cache: tr.(auth.Cache), Credential: auth.StaticCredential(client.Reference.Registry, cred)}
	return registryAcquisition{client: client, material: material, hasCredentials: cred != auth.EmptyCredential}, nil
}

// Machine credentials MUST wait for the source controller to exhaust rendered Secrets.
func (f *operation) resolveRegistryConfig(ctx context.Context, repo *manifest.OCIRepository, hostname string, policy cache.Policy) (*auth.Credential, error) {
	if repo.SecretRef == nil {
		return loadCredentials(ctx, f.RegistryConfig, hostname)
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
	cred, err := credentials.Credential(credStore)(ctx, hostname)
	if err != nil {
		return nil, fmt.Errorf("%w: resolve registry credentials: %w", manifest.ErrInput, err)
	}
	return &cred, nil
}

// Unrelated registry credentials MUST NOT enable an ambient retry.
func (f *operation) registryFallback(ctx context.Context, repo *manifest.OCIRepository, missing error, policy cache.Policy) (*auth.Credential, error) {
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
	selected, configErr := loadCredentials(ctx, f.RegistryConfig, ref.Host())
	if configErr == nil && *selected == auth.EmptyCredential {
		return nil, missing
	}
	mse.RetryAmbient = func(ctx context.Context) (*store.SourceArtifact, error) {
		if configErr != nil {
			return nil, fmt.Errorf("%w: %w", manifest.ErrInput, configErr)
		}
		slog.Debug("oci: secretRef unresolvable, using registry config",
			"id", source.QualifiedName(manifest.KindOCIRepository, repo.Namespace, repo.Name), "registry", ref.Registry)
		selected, err := loadCredentials(ctx, f.RegistryConfig, ref.Host())
		if err != nil {
			return nil, fmt.Errorf("%w: %w", manifest.ErrInput, err)
		}
		return f.fetchWithCredentials(ctx, repo, selected, policy)
	}
	return nil, mse
}

func loadCredentials(ctx context.Context, configPath, hostname string) (*auth.Credential, error) {
	paths := []string{configPath}
	if configPath == "" {
		dockerDir := os.Getenv("DOCKER_CONFIG")
		if dockerDir == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				slog.Debug("oci: docker credentials path unavailable", "err", err)
			} else {
				dockerDir = filepath.Join(home, ".docker")
			}
		}
		paths = []string{"", cmp.Or(os.Getenv("HELM_REGISTRY_CONFIG"), helmpath.ConfigPath("registry/config.json"))}
		if dockerDir != "" {
			paths[0] = filepath.Join(dockerDir, "config.json")
		}
	}
	for i, path := range paths {
		if path == "" {
			continue
		}
		s, err := credentials.NewFileStore(path)
		if err != nil {
			if configPath == "" {
				if i == 0 {
					slog.Debug("oci: docker credentials load failed; trying Helm credentials", "err", err)
					continue
				}
				if _, ok := errors.AsType[*os.PathError](err); ok {
					continue
				}
				// ORAS discards the typed cause of decoder read failures.
				if _, readErr := os.ReadFile(path); readErr != nil { //nolint:gosec // Machine credential paths come from caller configuration or the local environment.
					continue
				}
			}
			return nil, fmt.Errorf("%w: load credentials %s: %w", manifest.ErrInput, path, err)
		}
		cred, err := credentials.Credential(s)(ctx, hostname)
		if err != nil {
			return nil, fmt.Errorf("%w: resolve registry credentials: %w", manifest.ErrInput, err)
		}
		if cred != auth.EmptyCredential {
			return &cred, nil
		}
	}
	return &auth.Credential{}, nil
}
