package diff_test

import (
	"fmt"

	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
)

// Example compares explicitly attributed documents and retains chart metadata
// even when normalization strips the chart label from the compared manifests.
func Example() {
	parentID := manifest.NamedResource{
		Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease,
		Namespace: "media", Name: "sonarr",
	}
	parent := diff.Parent{NamedResource: parentID}
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
	base := []diff.Doc{{Parent: parent, Manifest: deployment("4.0.0", "ghcr.io/home-operations/sonarr:4.0.0")}}
	head := []diff.Doc{{Parent: parent, Manifest: deployment("4.1.0", "ghcr.io/home-operations/sonarr:4.1.0")}}

	changes, _ := diff.Changes(
		base, head,
		diff.Options{StripAttrs: diff.DefaultStripAttrs, StripFields: diff.DefaultStripFields},
	)

	for _, c := range changes {
		fmt.Printf("%s %s %s/%s (chart %s -> %s)\n", c.Status, c.Kind, c.Namespace, c.Name, c.OldChart, c.NewChart)
	}
	// Output:
	// changed Deployment media/sonarr (chart app-template-4.0.0 -> app-template-4.1.0)
}
