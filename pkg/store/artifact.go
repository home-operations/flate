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
// ArtifactUpdated event. Re-setting with a content-equal value is a
// no-op.
//
// Equality cascade, cheapest-first:
//
//  1. Pointer identity (prev == artifact): trivially equal, no-op.
//     Source-controller refresh loops cache their own SourceArtifact
//     and re-publish the same pointer on every tick; the short-
//     circuit avoids reflection entirely for that case (~3× faster
//     than the legacy reflect.DeepEqual-on-aliased-pointers fast
//     path, which still walks struct headers).
//  2. reflect.DeepEqual fallback for distinct-pointer dedup. Phase 1
//     evaluated hashing here (FNV64a via both json.Marshal and a
//     hand-rolled walker) and benched it against DeepEqual on the
//     realistic KS / HR re-emit shape (20-200 docs, fresh maps every
//     reconcile, no aliased sub-pointers). Result: DeepEqual was
//     ≥2× faster than either hash variant because the FNV walker
//     pays full per-leaf write cost on every call, while DeepEqual
//     short-circuits on the first leaf mismatch and is hand-tuned by
//     the runtime for nested map / slice shapes. The plan
//     acknowledged this outcome ("if JSON encode is slower than
//     DeepEqual on the artifact size you have, this is a wash. Bench
//     it."). The pointer-identity short-circuit is the residual win.
func (s *Store) SetArtifact(id manifest.NamedResource, artifact Artifact) {
	sh := s.shardFor(id)
	sh.mu.Lock()
	prev, exists := sh.artifacts[id]
	// Dedup cheapest-first: pointer identity (the hot path when a
	// fetcher caches its own SourceArtifact and re-publishes the same
	// pointer every refresh tick, skipping reflection entirely) before
	// the reflect.DeepEqual fallback for distinct-pointer equal content.
	if exists && (prev == artifact || reflect.DeepEqual(prev, artifact)) {
		sh.mu.Unlock()
		return
	}
	sh.artifacts[id] = artifact
	dispatch := s.fireUnderLock(EventArtifactUpdated, id, artifact)
	sh.mu.Unlock()
	dispatch()
}

// GetArtifact returns the artifact for id, or nil if none was set.
func (s *Store) GetArtifact(id manifest.NamedResource) Artifact {
	sh := s.shardFor(id)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.artifacts[id]
}
