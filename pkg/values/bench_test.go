package values

import (
	"fmt"
	"strings"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/pkg/manifest"
)

// bigPlatformValuesYAML builds a large nested values document resembling a
// platform-wide values ConfigMap that many HelmReleases reference via a
// single whole-doc valuesFrom. Deep maps + lists, ~tens of KB, so the
// per-HR DeepCopyMap of the cached parse is a measurable cost.
func bigPlatformValuesYAML() string {
	var b strings.Builder
	b.WriteString("global:\n  domain: example.com\n  registry: ghcr.io\n")
	for app := range 30 {
		fmt.Fprintf(&b, "app%d:\n", app)
		fmt.Fprintf(&b, "  image:\n    repository: ghcr.io/org/app%d\n    tag: v1.%d.0\n    pullPolicy: IfNotPresent\n", app, app)
		fmt.Fprintf(&b, "  resources:\n    requests:\n      cpu: %dm\n      memory: %dMi\n    limits:\n      cpu: %dm\n      memory: %dMi\n", app*10, app*32, app*20, app*64)
		b.WriteString("  env:\n")
		for e := range 8 {
			fmt.Fprintf(&b, "    - name: VAR_%d\n      value: \"value-%d-%d\"\n", e, app, e)
		}
		b.WriteString("  ingress:\n    enabled: true\n    hosts:\n")
		for h := range 3 {
			fmt.Fprintf(&b, "      - host: app%d-%d.example.com\n        paths: [\"/\"]\n", app, h)
		}
	}
	return b.String()
}

// BenchmarkExpandValueReferences_SharedLargeCM is the gate for the
// copy-on-collision optimization: N HelmReleases each reference ONE shared
// large platform-values ConfigMap (whole-doc, no TargetPath), reusing a
// single *Cache. After the first iteration the parse is cached, so each
// subsequent iteration measures the per-HR cost the optimization targets —
// today a full manifest.DeepCopyMap of the cached tree.
func BenchmarkExpandValueReferences_SharedLargeCM(b *testing.B) {
	cm := &manifest.ConfigMap{
		Name: "platform-values", Namespace: "default",
		Data: map[string]any{"values.yaml": bigPlatformValuesYAML()},
	}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	ref := manifest.ValuesReference{Kind: "ConfigMap", Name: "platform-values"}
	cache := NewCache() // shared across all HRs, as in production (one per helm.Client)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		hr := &manifest.HelmRelease{
			Name: "app", Namespace: "default",
			ValuesFrom: []manifest.ValuesReference{ref},
			Values:     map[string]any{"replicaCount": 2},
		}
		if err := ExpandValueReferences(hr, provider, cache); err != nil {
			b.Fatalf("ExpandValueReferences: %v", err)
		}
	}
}

// BenchmarkExpandValueReferences_ManyFromRefs measures ExpandValueReferences
// against an HR with 10 valuesFrom ConfigMap refs — each carrying a
// multi-key YAML document — backed by a SliceProvider. The merge
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
	// content-keyed memo and skip yaml.Unmarshal — matches the production
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
			ValuesFrom: refs,
			Values:     map[string]any{"image": map[string]any{"repository": "nginx"}},
		}
		if err := ExpandValueReferences(hr, provider, cache); err != nil {
			b.Fatalf("ExpandValueReferences: %v", err)
		}
	}
}

func BenchmarkExpandValueReferences_TargetPaths(b *testing.B) {
	for _, count := range []int{1, 5} {
		b.Run(fmt.Sprintf("targets=%d", count), func(b *testing.B) {
			cm := &manifest.ConfigMap{Name: "platform", Namespace: "default", Data: map[string]any{"values.yaml": bigPlatformValuesYAML()}}
			inject := &manifest.ConfigMap{Name: "inject", Namespace: "default", Data: map[string]any{"value": "replacement"}}
			provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm, inject}}
			refs := []manifest.ValuesReference{{Kind: "ConfigMap", Name: cm.Name}}
			for i := range count {
				refs = append(refs, manifest.ValuesReference{Kind: "ConfigMap", Name: inject.Name, ValuesKey: "value", TargetPath: fmt.Sprintf("app%d.image.tag", i)})
			}
			cache := NewCache()
			b.ReportAllocs()
			for b.Loop() {
				hr := &manifest.HelmRelease{Name: "app", Namespace: "default", ValuesFrom: refs}
				if err := ExpandValueReferences(hr, provider, cache); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkExpandValueReferences_ManyKeyCM is the gate for the single-key
// valuesFrom lookup. An HR references ONE key of a ConfigMap whose data bag
// carries many keys (the shared-settings / shared-secret shape). The values
// Cache memoizes the yaml.Unmarshal of the referenced key after the first
// iteration, but it is keyed on the *extracted* string, so the bag access in
// lookupValueRef runs every iteration: the old path normalized (and, for a
// Secret, base64-decoded) all 30 keys to return one; the single-key path reads
// only the referenced key. The per-iteration delta is that wasted full-bag
// decode.
func BenchmarkExpandValueReferences_ManyKeyCM(b *testing.B) {
	data := make(map[string]any, 30)
	data["values.yaml"] = "replicaCount: 3\n"
	for i := range 29 {
		data[fmt.Sprintf("sibling-%d.yaml", i)] = fmt.Sprintf("key%d: value-%d\n", i, i)
	}
	cm := &manifest.ConfigMap{Name: "shared", Namespace: "default", Data: data}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}
	ref := manifest.ValuesReference{Kind: "ConfigMap", Name: "shared", ValuesKey: "values.yaml"}
	cache := NewCache()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		hr := &manifest.HelmRelease{
			Name: "app", Namespace: "default",
			ValuesFrom: []manifest.ValuesReference{ref},
		}
		if err := ExpandValueReferences(hr, provider, cache); err != nil {
			b.Fatalf("ExpandValueReferences: %v", err)
		}
	}
}

// BenchmarkExpandPostBuildSubstituteReference_SharedCM measures the
// per-KS cost of the kustomize substitute path against a large shared
// cluster-settings ConfigMap (50 flat vars) — the bjw-s/onedr0p pattern
// where one CM is referenced by substituteFrom across many KSes. Unlike
// the helm valuesFrom path, this does NO yaml.Unmarshal: lookupResourceData
// -> decodeBag is a cheap map[string]any -> map[string]string conversion,
// and the per-KS \n-strip + varname-regex validation run regardless of any
// lookup cache. Quantifies whether memoizing the lookup would pay for the
// threading complexity.
func BenchmarkExpandPostBuildSubstituteReference_SharedCM(b *testing.B) {
	const keys = 50
	data := make(map[string]any, keys)
	for i := range keys {
		data[fmt.Sprintf("VAR_%d", i)] = fmt.Sprintf("value-%d", i)
	}
	cm := &manifest.ConfigMap{Name: "cluster-settings", Namespace: "flux-system", Data: data}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{cm}}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		// Fresh KS per iteration mirrors a distinct KS reconcile sharing
		// the same substituteFrom CM.
		postBuild := kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{{Kind: "ConfigMap", Name: "cluster-settings"}}}
		ks := &manifest.Kustomization{Name: "app", Namespace: "flux-system"}
		ks.PostBuild = &postBuild
		if err := ExpandPostBuildSubstituteReference(ks, provider); err != nil {
			b.Fatalf("ExpandPostBuildSubstituteReference: %v", err)
		}
	}
}

func BenchmarkExpandPostBuildSubstituteReference_ParseSharedCM(b *testing.B) {
	const input = `apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: app, namespace: flux-system}
spec:
  sourceRef: {kind: GitRepository, name: apps}
  postBuild: {substituteFrom: [{kind: ConfigMap, name: cluster-settings}]}
`
	data := make(map[string]any, 50)
	for i := range 50 {
		data[fmt.Sprintf("VAR_%d", i)] = fmt.Sprintf("value-%d", i)
	}
	provider := &SliceProvider{ConfigMaps: []*manifest.ConfigMap{{Name: "cluster-settings", Namespace: "flux-system", Data: data}}}
	b.ReportAllocs()
	for b.Loop() {
		docs, err := manifest.DecodeDocs(strings.NewReader(input))
		if err != nil {
			b.Fatal(err)
		}
		obj, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{DefaultNamespace: ""})
		if err != nil {
			b.Fatal(err)
		}
		ks := obj.(*manifest.Kustomization)
		if err := ExpandPostBuildSubstituteReference(ks, provider); err != nil {
			b.Fatal(err)
		}
		if len(ks.PostBuildSubstitute) != len(data) || ks.PostBuildSubstitute["VAR_49"] != "value-49" {
			b.Fatal("shared ConfigMap substitutions were not expanded")
		}
	}
}
