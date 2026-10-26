// Package oci fetches verified OCI source layers through the shared acquisition cache.
package oci

import (
	"context"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"helm.sh/helm/v4/pkg/registry"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/store"
)

// Fetcher is the Fetcher implementation for KindOCIRepository.
// RegistryConfig is the global --registry-config docker-style
// config.json path used when no per-repo SecretRef is set. Each Fetch
// resolves per-repository credentials through its captured inputs.
type Fetcher struct {
	Cache          *cache.Cache
	RegistryConfig string

	// Network is the required non-nil invocation owner, borrowed by this fetcher.
	Network *source.Network

	// ForceGeneric skips the provider gate; the generic path then needs
	// static credentials (SecretRef or --registry-config) or it fails.
	ForceGeneric bool
}

type operation struct {
	*Fetcher
	reads      *store.ReadSet
	secrets    source.SecretGetter
	mediaTypes []string
}

// Fetch acquires content using one execution's captured credentials and declarations.
func (f *Fetcher) Fetch(ctx context.Context, repo *manifest.OCIRepository, reads *store.ReadSet, policy cache.Policy) (*store.SourceArtifact, error) {
	var mediaTypes []string
	if mediaType := repo.LayerMediaType(); mediaType != "" {
		mediaTypes = []string{mediaType}
	}
	operation := operation{Fetcher: f, reads: reads, secrets: source.SecretLookup(reads), mediaTypes: mediaTypes}
	return operation.fetchSource(ctx, repo, policy)
}

// FetchChart acquires chart layers using Helm's supported media types.
func (f *Fetcher) FetchChart(ctx context.Context, repo *manifest.OCIRepository, reads *store.ReadSet, policy cache.Policy) (*store.SourceArtifact, error) {
	operation := operation{Fetcher: f, reads: reads, secrets: source.SecretLookup(reads),
		mediaTypes: []string{registry.ChartLayerMediaType, registry.LegacyChartLayerMediaType}}
	return operation.fetchSource(ctx, repo, policy)
}

func (f *operation) fetchSource(ctx context.Context, repo *manifest.OCIRepository, policy cache.Policy) (*store.SourceArtifact, error) {
	if !f.ForceGeneric && repo.Provider != "" && repo.Provider != sourcev1.GenericOCIProvider {
		return nil, source.ErrUnsupportedProvider(repo.Named(), repo.Provider, sourcev1.GenericOCIProvider,
			"SecretRef or --registry-config credentials")
	}
	if f.secrets != nil {
		for _, ref := range []*manifest.LocalObjectReference{repo.SecretRef, repo.CertSecretRef, repo.ProxySecretRef} {
			if ref != nil {
				f.secrets(repo.Namespace, ref.Name)
			}
		}
	}

	credStore, err := f.resolveRegistryConfig(ctx, repo, policy)
	if err != nil {
		return nil, err
	}
	return f.fetchWithCredentials(ctx, repo, credStore, policy)
}
