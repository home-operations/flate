package diff

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
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
		out, _, err := RenderDocs(left, right, Options{Format: FormatGitHub})
		if err != nil {
			b.Fatalf("RenderDocs: %v", err)
		}
		if len(out) == 0 {
			b.Fatalf("expected at least one diff")
		}
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
		parent := Parent{NamedResource: testutil.HelmReleaseID("default", fmt.Sprintf("hr-%d", i/10))}
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
