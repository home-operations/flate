// Package inputprojection binds transient content evidence to immutable data inputs.
package inputprojection

import "github.com/home-operations/flate/pkg/manifest"

// Projection binds effective content to the immutable data used to derive it.
type Projection struct {
	Content any
	Inputs  map[manifest.NamedResource]manifest.Data
}
