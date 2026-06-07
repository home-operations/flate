package diff_test

import (
	"fmt"

	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
)

// Example shows the SDK wiring an external consumer uses to turn two
// renders into a structured diff: orchestrator.Result.Manifests →
// DocsFromManifests → Changes. (Here the two manifest maps are hand-built
// for a self-contained example; in practice they're baseRes.Manifests and
// headRes.Manifests from two orchestrator.Render calls.)
func Example() {
	parent := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "media", Name: "sonarr"}
	deployment := func(chartVer, image string) map[string]any {
		return map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{
				"name": "sonarr", "namespace": "media",
				"labels": map[string]any{"helm.sh/chart": "app-template-" + chartVer},
			},
			"spec": map[string]any{
				"template": map[string]any{"spec": map[string]any{
					"containers": []any{map[string]any{"name": "main", "image": image}},
				}},
			},
		}
	}
	base := map[manifest.NamedResource][]map[string]any{parent: {deployment("4.0.0", "ghcr.io/home-operations/sonarr:4.0.0")}}
	head := map[manifest.NamedResource][]map[string]any{parent: {deployment("4.1.0", "ghcr.io/home-operations/sonarr:4.1.0")}}

	changes := diff.Changes(
		diff.DocsFromManifests(base, nil),
		diff.DocsFromManifests(head, nil),
		diff.Options{StripAttrs: diff.DefaultStripAttrs, StripFields: diff.DefaultStripFields},
	)

	for _, c := range changes {
		fmt.Printf("%s %s %s/%s (chart %s -> %s)\n", c.Status, c.Kind, c.Namespace, c.Name, c.OldChart, c.NewChart)
	}
	// Output:
	// changed Deployment media/sonarr (chart app-template-4.0.0 -> app-template-4.1.0)
}
