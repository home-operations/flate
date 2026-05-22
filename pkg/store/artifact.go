package store

import "github.com/home-operations/flate/pkg/manifest"

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

// GitArtifact is the working tree produced by SourceController for a
// GitRepository.
type GitArtifact struct {
	URL       string
	LocalPath string
	Ref       manifest.GitRepositoryRef
	Revision  string // resolved commit SHA, when known
}

func (*GitArtifact) artifact() {}

// OCIArtifact is the working tree produced by SourceController for an
// OCIRepository.
type OCIArtifact struct {
	URL       string
	LocalPath string
	Ref       manifest.OCIRepositoryRef
	Digest    string
}

func (*OCIArtifact) artifact() {}

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
