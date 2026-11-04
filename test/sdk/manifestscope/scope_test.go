package manifestscope_test

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

type resolver struct {
	memo        map[yaml.TypeMeta]bool
	resolutions int
}

func (r *resolver) ResolveScope(meta yaml.TypeMeta) manifest.ScopeLookup {
	r.resolutions++
	value := meta.Kind != "ClusterRole"
	r.memo[meta] = value
	return manifest.ScopeLookup{Namespaced: value, Found: true}
}

func (r *resolver) LookupScope(meta yaml.TypeMeta) manifest.ScopeLookup {
	value, ok := r.memo[meta]
	return manifest.ScopeLookup{Namespaced: value, Found: ok}
}

//nolint:nonamedreturns,paralleltest // Panic recovery updates the result and scope registration changes process-wide state.
func TestScopeStartup(t *testing.T) {
	panics := func(f func()) (panicked bool) {
		defer func() { panicked = recover() != nil }()
		f()
		return false
	}
	assert.FatalIf(
		t,
		!panics(func() { manifest.KindOf(&manifest.RawObject{APIVersion: "v1", Kind: "ConfigMap"}).Namespaced() }),
		"%v",
		"missing scope resolver silently supplied an answer",
	)
	assert.FatalIf(t, (manifest.KindInfo{}).Namespaced(), "%v", "empty kind has namespace scope")
	assert.FatalIf(
		t,
		!panics(func() { manifest.RegisterScopeResolver(nil) }),
		"%v",
		"nil scope resolver was accepted at startup",
	)
	r := &resolver{memo: make(map[yaml.TypeMeta]bool)}
	manifest.RegisterScopeResolver(r)
	parsed, err := manifest.ParseDoc(
		map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm"}},
		manifest.ParseDocOptions{DefaultNamespace: "apps"},
	)
	obj := parsed.Object
	assert.FatalIf(t, err != nil || obj.Named().Namespace != "apps", "standalone namespace default = %v, %v", obj, err)
	parsed2, parsedErr := manifest.ParseDoc(map[string]any{
		"apiVersion": manifest.FluxOperatorDomain + "/v1",
		"kind":       manifest.KindResourceSet,
		"metadata":   map[string]any{"name": "set", "namespace": "authored"},
		"spec": map[string]any{
			"dependsOn": []any{
				map[string]any{
					"apiVersion": "rbac.authorization.k8s.io/v1",
					"kind":       "ClusterRole",
					"name":       "role",
					"namespace":  "explicit",
				},
			},
		},
	}, manifest.ParseDocOptions{})
	obj, err = parsed2.Object, parsedErr
	assert.NoError(t, err)
	before := r.resolutions
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		assert.FatalIf(t, ref.Namespace != "", "cluster dependency namespace = %q", ref.Namespace)
	}
	direct := &manifest.ResourceSet{
		DependsOn: []fluxopv1.Dependency{{APIVersion: "direct.example/v1", Kind: "Widget", Name: "widget"}},
	}
	assert.FatalIf(t, !panics(func() {
		for range manifest.References(direct, manifest.ReferenceRelations) {
			t.Error("unprepared dependency yielded a reference")
		}
	}) || r.resolutions != before, "%v", "memo miss did not preserve the preparation invariant")
	manifest.KindOf(&manifest.RawObject{APIVersion: "direct.example/v1", Kind: "Widget"}).Namespaced()
	count := 0
	for range manifest.References(direct, manifest.ReferenceRelations) {
		count++
	}
	assert.FatalIf(t, count != 1, "prepared direct references = %d; want 1", count)
	assert.FatalIf(
		t,
		!panics(func() { manifest.RegisterScopeResolver(r) }),
		"%v",
		"scope owner was replaceable after startup",
	)
}
