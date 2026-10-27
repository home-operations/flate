package diffprep

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

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
			Parent: Parent{NamedResource: testutil.HelmReleaseID("default", fmt.Sprintf("hr-%d", i))},
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
		_, _ = normalizeDocs(docs, attrs, nil, nil)
	}
}
