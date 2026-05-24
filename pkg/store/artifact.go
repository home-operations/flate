package store

import (
	"reflect"

	"github.com/home-operations/flate/pkg/manifest"
)

// Artifact is a marker interface implemented by every artifact type.
// Controllers type-assert to the concrete type they expect.
type Artifact interface {
	artifact()
}

// RenderedArtifact is satisfied by artifacts that carry a rendered
// manifest set — KustomizationArtifact and HelmReleaseArtifact. CLI
// emitters use it to collect rendered output without caring which
// controller produced it.
type RenderedArtifact interface {
	Artifact
	RenderedManifests() []map[string]any
}

// SourceArtifact is the unified working-tree artifact produced by
// source fetchers (GitRepository, OCIRepository, Bucket, ExternalArtifact, …).
// Kind identifies which CR kind produced it so consumers that care
// (e.g. the helm-controller's local-git registration) can filter
// without the previous per-kind type union.
//
// Mirrors Flux's meta.Artifact contract: URL is the upstream address,
// Revision is the human-readable identifier (branch:main, tag:v1.2.3,
// commit sha), Digest is the content-addressed verification value,
// Size is the artifact size in bytes when known, and Metadata holds
// kind-specific annotations (OCI image annotations, bucket ETag…).
type SourceArtifact struct {
	Kind      string
	URL       string
	LocalPath string
	Revision  string
	Digest    string
	Size      int64
	Metadata  map[string]string
}

func (*SourceArtifact) artifact() {}

// KustomizationArtifact is the rendered output of a Kustomization build.
//
// Fingerprint mirrors HelmReleaseArtifact: a stable hash of the inputs
// that determine the rendered output (path, inline contents, spec,
// expanded substitutions, resolved source root). The KS controller
// compares it on every reconcile and skips re-running kustomize when
// a re-AddObject event arrives with the same effective spec — the
// same wasted-work pattern HR had before PR #219, just for KS.
type KustomizationArtifact struct {
	Path        string
	Manifests   []map[string]any
	Fingerprint string
}

func (*KustomizationArtifact) artifact() {}

// RenderedManifests implements RenderedArtifact.
func (a *KustomizationArtifact) RenderedManifests() []map[string]any { return a.Manifests }

// HelmReleaseArtifact is the rendered output of a HelmRelease template.
//
// Fingerprint is a stable hash of the inputs that determine the
// rendered output (chart identity, expanded values, install/upgrade
// flags). The HR controller compares it on every reconcile and
// skips the helm render — which is by far the hot path — when a
// re-AddObject event arrives with the same effective spec. Typical
// trigger: the parent Kustomization's render re-emits the HR with
// `kustomize.toolkit.fluxcd.io/{name,namespace}` ownership labels
// stamped on metadata, which fails AddObject's reflect-DeepEqual
// gate even though the rendered output would be byte-identical.
type HelmReleaseArtifact struct {
	Manifests   []map[string]any
	Fingerprint string
}

func (*HelmReleaseArtifact) artifact() {}

// RenderedManifests implements RenderedArtifact.
func (a *HelmReleaseArtifact) RenderedManifests() []map[string]any { return a.Manifests }

// --- Store operations on artifacts ---

// SetArtifact stores an artifact for id and dispatches an
// ArtifactUpdated event. Re-setting with a deep-equal value is a no-op.
func (s *Store) SetArtifact(id manifest.NamedResource, artifact Artifact) {
	s.mu.Lock()
	prev, exists := s.artifacts[id]
	if exists && reflect.DeepEqual(prev, artifact) {
		s.mu.Unlock()
		return
	}
	s.artifacts[id] = artifact
	s.mu.Unlock()
	s.fire(EventArtifactUpdated, id, artifact)
}

// GetArtifact returns the artifact for id, or nil if none was set.
func (s *Store) GetArtifact(id manifest.NamedResource) Artifact {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.artifacts[id]
}
