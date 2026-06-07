package testutil

import (
	"github.com/home-operations/flate/pkg/manifest"
)

// DepRefs wraps NamedResources as bare DependencyRefs (no ReadyExpr),
// the shape dependency-ordering tests use to express dependsOn edges.
func DepRefs(ids ...manifest.NamedResource) []manifest.DependencyRef {
	out := make([]manifest.DependencyRef, len(ids))
	for i, id := range ids {
		out[i] = manifest.DependencyRef{NamedResource: id}
	}
	return out
}
