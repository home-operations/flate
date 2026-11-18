package diff_test

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/home-operations/flate/pkg/diff"
	"github.com/home-operations/flate/pkg/manifest"
)

// Example compares explicitly attributed documents and retains chart metadata
// even when normalization strips the chart label from the compared manifests.
func Example() {
	parent := &manifest.HelmRelease{Namespace: "media", Name: "sonarr"}
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
	base := diff.Snapshot{Manifests: []manifest.DocumentBatch{{
		Parent: parent, Documents: []map[string]any{deployment("4.0.0", "ghcr.io/home-operations/sonarr:4.0.0")},
	}}}
	head := diff.Snapshot{Manifests: []manifest.DocumentBatch{{
		Parent: parent, Documents: []map[string]any{deployment("4.1.0", "ghcr.io/home-operations/sonarr:4.1.0")},
	}}}

	comparison, err := diff.Compare(
		[]diff.Group{{Orig: base, Current: head}},
		diff.Options{Format: diff.FormatJSON, StripAttrs: diff.DefaultStripAttrs, StripFields: diff.DefaultStripFields},
	)
	if err != nil {
		panic(err)
	}
	var output bytes.Buffer
	if err := comparison.Write(&output, nil); err != nil {
		panic(err)
	}
	var report struct {
		Changes []diff.Change `json:"changes"`
	}
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		panic(err)
	}
	{
		cValues := report.Changes
		for cIndex := range cValues {
			c := cValues[cIndex]
			fmt.Printf("%s %s %s/%s (chart %s -> %s)\n", c.Status, c.Kind, c.Namespace, c.Name, c.OldChart, c.NewChart)
		}
	}
	// Output:
	// changed Deployment media/sonarr (chart app-template-4.0.0 -> app-template-4.1.0)
}
