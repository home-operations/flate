// Package oci fetches verified OCI source layers through the shared acquisition cache.
package oci

import (
	"context"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher is the Fetcher implementation for KindOCIRepository.
// RegistryConfig is the global --registry-config docker-style
// config.json path used when no per-repo SecretRef is set. Secrets is
// the per-repo source.SecretGetter (typically the orchestrator-provided
// Store.GetByName), required when any OCIRepository has spec.secretRef
// pointing at a kubernetes.io/dockerconfigjson Secret.
type Fetcher struct {
	Cache          *cache.Cache
	RegistryConfig string
	Secrets        source.SecretGetter

	// ForceGeneric skips the provider gate; the generic path then needs
	// static credentials (SecretRef or --registry-config) or it fails.
	ForceGeneric bool
}

// Fetch implements source.TypedFetcher[*manifest.OCIRepository].
// Credentials, TLS and proxy policy are resolved before acquisition.
func (f *Fetcher) Fetch(ctx context.Context, repo *manifest.OCIRepository) (*store.SourceArtifact, error) {
	if !f.ForceGeneric && repo.Provider != "" && repo.Provider != sourcev1.GenericOCIProvider {
		return nil, source.ErrUnsupportedProvider("OCIRepository",
			repo.Namespace, repo.Name, repo.Provider, sourcev1.GenericOCIProvider,
			"SecretRef or --registry-config credentials")
	}
	configPath, cleanup, err := f.resolveRegistryConfig(ctx, repo)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	return f.fetchWithRegistryConfig(ctx, repo, configPath)
}

func (f *Fetcher) fetchWithRegistryConfig(ctx context.Context, repo *manifest.OCIRepository, configPath string) (*store.SourceArtifact, error) {
	tlsCfg, err := f.resolveTLS(repo)
	if err != nil {
		return nil, err
	}
	proxy, err := source.ResolveProxy(f.Secrets, repo.Namespace, "OCIRepository",
		repo.Namespace+"/"+repo.Name, repo.ProxySecretRef)
	if err != nil {
		return nil, err
	}
	client, err := newRepoClient(repo, configPath, tlsCfg, proxy)
	if err != nil {
		return nil, err
	}
	return f.fetch(ctx, repo, client)
}

// ociID is the "OCIRepository <namespace>/<name>" prefix shared by this
// package's error messages — wraps source.QualifiedName so every fetcher
// formats its kind/ns/name identically.
func ociID(repo *manifest.OCIRepository) string {
	return source.QualifiedName("OCIRepository", repo.Namespace, repo.Name)
}

// authIdentity returns the cache-key auth tag for an OCIRepository.
// Combines SecretRef (registry creds), CertSecretRef (TLS material), and
// ProxySecretRef since each can change which artifact bytes a reconcile
// resolves to.
func authIdentity(repo *manifest.OCIRepository) string {
	return source.AuthIdentityFromRefs(repo.Namespace,
		repo.SecretRef, repo.CertSecretRef, repo.ProxySecretRef)
}
