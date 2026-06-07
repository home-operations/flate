package diff

import (
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
)

// DocsFromManifests flattens an orchestrator render Result's per-parent
// rendered manifests into the flat []Doc that RenderDocs and Changes
// consume, tagging each document with the Flux Kustomization /
// HelmRelease that produced it. The input is exactly
// orchestrator.Result.Manifests (map[NamedResource][]map[string]any), so
// an SDK consumer wires two renders straight into a diff without
// re-implementing the walk.
//
// pathOf, when non-nil, supplies a parent's Flux Kustomization spec.path
// — it disambiguates two same-named Kustomizations rendered from
// different overlays (a real-world pairing collision); pass nil when the
// path isn't available (HelmRelease parents and most consumers don't need
// it). Documents are grouped and ordered by producing parent (each
// parent's documents keep their render emission order) so the result is
// deterministic across the input map's random iteration order.
func DocsFromManifests(manifests map[manifest.NamedResource][]map[string]any, pathOf func(manifest.NamedResource) string) []Doc {
	parents := make([]manifest.NamedResource, 0, len(manifests))
	for id := range manifests {
		parents = append(parents, id)
	}
	slices.SortFunc(parents, func(a, b manifest.NamedResource) int { return a.Compare(b) })

	out := make([]Doc, 0, len(manifests))
	for _, id := range parents {
		p := Parent{Kind: id.Kind, Namespace: id.Namespace, Name: id.Name}
		if pathOf != nil {
			p.Path = pathOf(id)
		}
		for _, m := range manifests[id] {
			out = append(out, Doc{Manifest: m, Parent: p})
		}
	}
	return out
}
