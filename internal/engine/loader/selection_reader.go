package loader

import (
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type admissionReader interface {
	SourceAdmission(manifest.NamedResource, manifest.NamedResource) bool
	Lookup(manifest.NamedResource) (manifest.BaseManifest, store.DeclarationPolicy)
	GetObject(manifest.NamedResource) manifest.BaseManifest
	GetDeclaration(manifest.NamedResource) manifest.BaseManifest
	GetArtifact(manifest.NamedResource, store.ArtifactRead) store.Artifact
	Productions(manifest.NamedResource, *manifest.DiscoveryIndex) []manifest.Production
	ListProductionCandidates(
		string,
		string,
		*manifest.DiscoveryIndex,
		map[manifest.NamedResource]bool,
	) []manifest.BaseManifest
}
