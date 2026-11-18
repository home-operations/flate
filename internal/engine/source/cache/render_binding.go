package cache

import (
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

// RenderBinding MUST keep raw render identity separate from the acquisition family.
type RenderBinding[P any] struct {
	Key      string                 `json:"Key"`
	Source   manifest.NamedResource `json:"Source"`
	Artifact store.ArtifactHistory  `json:"Artifact"`
	Policy   P                      `json:"Policy"`
}

// Refresh MUST preserve retained policy slices and earlier request snapshots.
func (b *RenderBinding[P]) Refresh(key string, source manifest.NamedResource, artifact *store.SourceArtifact) {
	b.Key, b.Source, b.Artifact = key, source, artifact.HistoryInput()
}

// Equal MUST compare typed policy without hashing or reflection.
func (b *RenderBinding[P]) Equal(other *RenderBinding[P], equalPolicy func(*P, *P) bool) bool {
	return b.Key == other.Key && b.Source == other.Source && b.Artifact == other.Artifact &&
		equalPolicy(&b.Policy, &other.Policy)
}

// Fingerprint MUST derive history identity within the cache owner.
func (b *RenderBinding[P]) Fingerprint() string {
	return manifest.Fingerprint(b)
}
