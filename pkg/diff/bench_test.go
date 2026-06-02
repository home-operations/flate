package diff

import (
	"fmt"
	"testing"
)

// BenchmarkDiff_LargeTree measures RenderDocs against two 100-resource
// doc sets where most pairs are identical and a handful differ — the
// shape a real `flate diff` produces on a typical CI run.
func BenchmarkDiff_LargeTree(b *testing.B) {
	const n = 100
	left, right := buildDiffCorpus(n, 5)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		out, err := RenderDocs(left, right, Options{Format: FormatGitHub})
		if err != nil {
			b.Fatalf("RenderDocs: %v", err)
		}
		if len(out) == 0 {
			b.Fatalf("expected at least one diff")
		}
	}
}

// BenchmarkNormalizeDocs measures normalizeDocs against a 100-doc set
// with 5 strip attrs — the pre-diff sanitization pass that pulls
// chart-bump noise (helm.sh/chart, checksum/config, …) out of every
// resource's metadata and redacts ConfigMap.binaryData before dyff
// sees them. The corpus carries no binaryData, so this exercises the
// strip path plus the docsContainBinaryData short-circuit walk.
func BenchmarkNormalizeDocs(b *testing.B) {
	const n = 100
	docs := make([]Doc, 0, n)
	for i := range n {
		docs = append(docs, Doc{
			Manifest: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]any{
					"name":      fmt.Sprintf("app-%d", i),
					"namespace": "default",
					"labels": map[string]any{
						"app":                    fmt.Sprintf("app-%d", i),
						"helm.sh/chart":          fmt.Sprintf("chart-%d", i),
						"app.kubernetes.io/name": fmt.Sprintf("app-%d", i),
					},
					"annotations": map[string]any{
						"checksum/config":                   fmt.Sprintf("%d", i*31),
						"deployment.kubernetes.io/revision": "1",
					},
				},
				"spec": map[string]any{
					"template": map[string]any{
						"metadata": map[string]any{
							"labels": map[string]any{
								"app":           fmt.Sprintf("app-%d", i),
								"helm.sh/chart": fmt.Sprintf("chart-%d", i),
							},
							"annotations": map[string]any{
								"checksum/config": fmt.Sprintf("%d", i*31),
							},
						},
					},
				},
			},
			Parent: Parent{Kind: "HelmRelease", Namespace: "default", Name: fmt.Sprintf("hr-%d", i)},
		})
	}
	attrs := []string{
		"helm.sh/chart",
		"checksum/config",
		"deployment.kubernetes.io/revision",
		"control-plane.alpha.kubernetes.io/leader",
		"meta.helm.sh/release-name",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = normalizeDocs(docs, attrs)
	}
}

// buildDiffCorpus generates two doc sets of size n. The first
// changeCount entries differ in a single field; the rest are
// identical.
func buildDiffCorpus(n, changeCount int) (left, right []Doc) {
	left = make([]Doc, 0, n)
	right = make([]Doc, 0, n)
	for i := range n {
		name := fmt.Sprintf("cm-%d", i)
		parent := Parent{Kind: "HelmRelease", Namespace: "default", Name: fmt.Sprintf("hr-%d", i/10)}
		baseValue := fmt.Sprintf("value-%d", i)
		other := baseValue
		if i < changeCount {
			other = fmt.Sprintf("value-%d-changed", i)
		}
		left = append(left, Doc{
			Manifest: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      name,
					"namespace": "default",
				},
				"data": map[string]any{"k": baseValue},
			},
			Parent: parent,
		})
		right = append(right, Doc{
			Manifest: map[string]any{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]any{
					"name":      name,
					"namespace": "default",
				},
				"data": map[string]any{"k": other},
			},
			Parent: parent,
		})
	}
	return left, right
}
