package store

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
type KustomizationArtifact struct {
	Path      string
	Manifests []map[string]any
	Revision  string
}

func (*KustomizationArtifact) artifact() {}

// RenderedManifests returns the manifests rendered by the Kustomization.
func (a *KustomizationArtifact) RenderedManifests() []map[string]any { return a.Manifests }

// HelmReleaseArtifact is the rendered output of a HelmRelease template.
type HelmReleaseArtifact struct {
	ChartName string
	Manifests []map[string]any
	Values    map[string]any
}

func (*HelmReleaseArtifact) artifact() {}

// RenderedManifests returns the manifests rendered by the HelmRelease.
func (a *HelmReleaseArtifact) RenderedManifests() []map[string]any { return a.Manifests }
