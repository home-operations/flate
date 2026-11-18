// Package inputprojection binds transient content evidence to immutable data inputs.
package inputprojection

import (
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/engine/tree"
	"github.com/home-operations/flate/pkg/manifest"
)

// Projection binds effective content to the immutable data used to derive it.
type Projection struct {
	Content any
	Inputs  map[manifest.NamedResource]manifest.Data
}

// Overlay MUST keep render-only files outside the authored discovery inventory.
type Overlay struct {
	*tree.Tree

	Authored *store.SourceArtifact
}

// Authored retains the admitted source identity behind render-only content.
func Authored(input *store.SourceArtifact) *store.SourceArtifact {
	if input != nil {
		if overlay, ok := input.FS.(*Overlay); ok {
			return overlay.Authored
		}
	}
	return input
}
