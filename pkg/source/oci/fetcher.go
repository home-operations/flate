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
// the per-repo source.SecretGetter (the orchestrator's core Secret store
// lookup), required when any OCIRepository has spec.secretRef
// pointing at a kubernetes.io/dockerconfigjson Secret.
type Fetcher struct {
	Cache          *cache.Cache
	RegistryConfig string
	Secrets        source.SecretGetter
	// Network is the required non-nil invocation owner, borrowed by this fetcher.
	Network *source.Network

	// ForceGeneric skips the provider gate; the generic path then needs
	// static credentials (SecretRef or --registry-config) or it fails.
	ForceGeneric bool
}

// Fetch implements source.TypedFetcher[*manifest.OCIRepository].
// Credentials, TLS and proxy policy are resolved before acquisition.
func (f *Fetcher) Fetch(ctx context.Context, repo *manifest.OCIRepository, retry cache.RetryConfig) (*store.SourceArtifact, error) {
	if !f.ForceGeneric && repo.Provider != "" && repo.Provider != sourcev1.GenericOCIProvider {
		return nil, source.ErrUnsupportedProvider(repo.Named(), repo.Provider, sourcev1.GenericOCIProvider,
			"SecretRef or --registry-config credentials")
	}
	prepared := *f
	prepared.Secrets = source.SnapshotSecrets(f.Secrets)
	if prepared.Secrets != nil {
		for _, ref := range []*manifest.LocalObjectReference{repo.SecretRef, repo.CertSecretRef, repo.ProxySecretRef} {
			if ref != nil {
				prepared.Secrets(repo.Namespace, ref.Name)
			}
		}
	}

	credStore, err := prepared.resolveRegistryConfig(ctx, repo, retry)
	if err != nil {
		return nil, err
	}
	return prepared.fetchWithCredentials(ctx, repo, credStore, retry)
}

// ociID is the "OCIRepository <namespace>/<name>" prefix shared by this
// package's error messages — wraps source.QualifiedName so every fetcher
// formats its kind/ns/name identically.
func ociID(repo *manifest.OCIRepository) string {
	return source.QualifiedName("OCIRepository", repo.Namespace, repo.Name)
}
