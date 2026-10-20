package helm

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// SourceResolver is the lookup surface helm.Client needs to resolve a
// HelmRelease's sourceRef against the canonical object store.
//
// The interface exists so helm.Client reads source CRs from the
// canonical store on every lookup rather than maintaining a parallel
// registry — see NewStoreSourceResolver for the production-side
// adapter. Embedders rendering a single HelmRelease without an
// Orchestrator can implement this directly.
//
// A nil return from any method means "not found" — callers translate
// that into a typed manifest.ErrObjectNotFound error.
type SourceResolver interface {
	// HelmRepository returns the HelmRepository at the given (ns, name)
	// or nil. Used for index.yaml fetch + SecretRef/CertSecretRef.
	HelmRepository(namespace, name string) *manifest.HelmRepository
	// LocalSourceArtifact returns an admitted source artifact or nil.
	// FS supplies content and acquisition authority through source.OperationView.
	LocalSourceArtifact(manifest.NamedResource) *store.SourceArtifact
	// HelmChart returns the HelmChartSource at the given full identity or
	// nil. Used by HelmRelease.ResolveChartRef to materialize a
	// spec.chartRef into a concrete chart reference.
	HelmChart(manifest.NamedResource) *manifest.HelmChartSource
}

// NewStoreSourceResolver returns a SourceResolver backed by the
// canonical object store. The orchestrator wires this into helm.Client
// so chart-source lookups read straight from the Store.
func NewStoreSourceResolver(s *store.Store) SourceResolver {
	return &storeResolver{store: s}
}

type storeResolver struct {
	store *store.Store
}

func (r *storeResolver) HelmRepository(namespace, name string) *manifest.HelmRepository {
	obj, _ := r.store.Get[*manifest.HelmRepository](manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindHelmRepository, Namespace: namespace, Name: name})
	return obj
}

func (r *storeResolver) LocalSourceArtifact(id manifest.NamedResource) *store.SourceArtifact {
	art, _ := r.store.GetArtifact(id).(*store.SourceArtifact)
	return art
}

func (r *storeResolver) HelmChart(id manifest.NamedResource) *manifest.HelmChartSource {
	obj, _ := r.store.Get[*manifest.HelmChartSource](id)
	return obj
}
