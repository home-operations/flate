package output

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

func BenchmarkEncoder_YAML(b *testing.B) {
	docs := make([]map[string]any, 64)
	resources := make([]map[string]any, len(docs))
	images := make([]string, len(docs))
	for i := range docs {
		name := fmt.Sprintf("application-%d", i)
		docs[i] = map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": name, "namespace": "apps"},
			"data":     map[string]any{"config": strings.Repeat("key=value\n", 32), "enabled": "true"},
		}
		resources[i] = map[string]any{
			"kind": "Kustomization", "name": name, "namespace": "apps", "prune": true,
			"sourceRef": map[string]string{"kind": "GitRepository", "name": "flux-system", "namespace": "flux-system"},
		}
		images[i] = "ghcr.io/example/" + name + ":v1.0.0"
	}
	e := Encoder{Writer: io.Discard, Format: FormatYAML}
	summary := map[string]any{"kustomizations": 64, "helmReleases": 32}
	for _, bc := range []struct {
		name string
		run  func() error
	}{
		{"Documents/ConfigMaps64", func() error { return e.Documents(docs) }},
		{"YAML/ConfigMap", func() error { return e.YAML(docs[0]) }},
		{"Resources/SourceRefs64", func() error { return e.Resources(nil, nil, resources) }},
		{"Images/List64", func() error { return e.Images(images) }},
		{"Cluster/Counts", func() error { return e.Cluster(summary) }},
	} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if err := bc.run(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
