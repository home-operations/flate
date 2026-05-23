package helm

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// SourceResolver is the lookup surface helm.Client needs to resolve a
// HelmRelease's sourceRef against the canonical object store.
//
// The interface exists so helm.Client doesn't have to maintain its own
// parallel registries of source CRs that the Store already owns — see
// NewStoreSourceResolver for the production-side adapter. Embedders
// rendering a single HelmRelease without an Orchestrator can implement
// this directly.
//
// A nil return from any method means "not found" — callers translate
// that into a typed manifest.ErrObjectNotFound error.
type SourceResolver interface {
	// HelmRepository returns the HelmRepository at the given (ns, name)
	// or nil. Used for index.yaml fetch + SecretRef/CertSecretRef.
	HelmRepository(namespace, name string) *manifest.HelmRepository
	// OCIRepository returns the OCIRepository at the given (ns, name)
	// or nil. The .url field is the chart-artifact root.
	OCIRepository(namespace, name string) *manifest.OCIRepository
	// LocalSourceArtifact returns the fetched on-disk artifact for a
	// GitRepository / Bucket / ExternalArtifact source, or nil. Charts
	// live at <artifact.LocalPath>/<hr.Chart.Name> for any of those
	// three sourceRef.kind values.
	LocalSourceArtifact(kind, namespace, name string) *store.SourceArtifact
}

// NewStoreSourceResolver returns a SourceResolver backed by the
// canonical object store. The orchestrator wires this into helm.Client
// so chart-source lookups read straight from the Store rather than
// through helm.Client's now-deprecated AddRepo/AddOCIRepo/AddLocalSource
// push API.
func NewStoreSourceResolver(s *store.Store) SourceResolver {
	return &storeResolver{store: s}
}

type storeResolver struct {
	store *store.Store
}

func (r *storeResolver) HelmRepository(namespace, name string) *manifest.HelmRepository {
	obj, _ := r.store.GetByName(manifest.KindHelmRepository, namespace, name).(*manifest.HelmRepository)
	return obj
}

func (r *storeResolver) OCIRepository(namespace, name string) *manifest.OCIRepository {
	obj, _ := r.store.GetByName(manifest.KindOCIRepository, namespace, name).(*manifest.OCIRepository)
	return obj
}

func (r *storeResolver) LocalSourceArtifact(kind, namespace, name string) *store.SourceArtifact {
	id := manifest.NamedResource{Kind: kind, Namespace: namespace, Name: name}
	art, _ := r.store.GetArtifact(id).(*store.SourceArtifact)
	return art
}
