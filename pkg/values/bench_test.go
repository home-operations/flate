package values

import (
	"fmt"
	"testing"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

// BenchmarkExpandValueReferences_ManyFromRefs measures ExpandValueReferences
// against an HR with 10 valuesFrom ConfigMap refs — each carrying a
// multi-key YAML document — backed by a SliceProvider. The DeepMerge
// chain dominates here; the lookupValueRef path runs once per ref.
func BenchmarkExpandValueReferences_ManyFromRefs(b *testing.B) {
	const n = 10
	cms := make([]*manifest.ConfigMap, 0, n)
	refs := make([]manifest.ValuesReference, 0, n)
	for i := range n {
		name := fmt.Sprintf("values-%d", i)
		// Each CM contributes a small map; ExpandValueReferences merges
		// them all into the final hr.Values.
		data := fmt.Sprintf(`layer-%d:
  k: v-%d
shared:
  nested-%d: %d
counts:
  total: %d
`, i, i, i, i, i)
		cms = append(cms, &manifest.ConfigMap{
			Name: name, Namespace: "default",
			Data: map[string]any{"values.yaml": data},
		})
		refs = append(refs, manifest.ValuesReference{Kind: "ConfigMap", Name: name})
	}
	provider := &SliceProvider{ConfigMaps: cms}
	// Shared Cache across iterations: same valuesFrom refs hit the
	// FNV-keyed memo and skip yaml.Unmarshal — matches the production
	// pattern where M HRs sharing a platform-wide values CM parse
	// exactly once.
	cache := NewCache()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		// Fresh HR per iteration — ExpandValueReferences writes hr.Values
		// in place; the merge result accumulates across runs otherwise.
		hr := &manifest.HelmRelease{
			Name: "demo", Namespace: "default",
			HelmReleaseSpec: helmv2.HelmReleaseSpec{ValuesFrom: refs},
			Values:          map[string]any{"image": map[string]any{"repository": "nginx"}},
		}
		if err := ExpandValueReferences(hr, provider, cache); err != nil {
			b.Fatalf("ExpandValueReferences: %v", err)
		}
	}
}

// BenchmarkDeepMerge_DeepTree measures DeepMerge against two 5-level
// nested maps — the cost the Helm prepare path pays per HR when
// layering inline values onto resolved valuesFrom output.
func BenchmarkDeepMerge_DeepTree(b *testing.B) {
	base := buildDeepTree(5, "base")
	override := buildDeepTree(5, "override")

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = DeepMerge(base, override)
	}
}

// buildDeepTree constructs a depth-deep map where every level is a
// fresh map[string]any containing a leaf "leaf" key plus a "next" key
// that recurses. Used so DeepMerge has to walk all the way down to
// merge a leaf — exercising the recursive-map branch end to end.
func buildDeepTree(depth int, tag string) map[string]any {
	if depth == 0 {
		return map[string]any{"leaf": tag}
	}
	return map[string]any{
		"leaf": tag,
		"key1": fmt.Sprintf("scalar-%d", depth),
		"key2": []any{1, 2, 3, "four", true},
		"next": buildDeepTree(depth-1, tag),
	}
}
