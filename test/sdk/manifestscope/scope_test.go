package manifestscope_test

import (
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

type resolver struct {
	memo        map[yaml.TypeMeta]bool
	resolutions int
}

func (r *resolver) ResolveScope(meta yaml.TypeMeta) bool {
	r.resolutions++
	value := meta.Kind != "ClusterRole"
	r.memo[meta] = value
	return value
}

func (r *resolver) LookupScope(meta yaml.TypeMeta) (bool, bool) {
	value, ok := r.memo[meta]
	return value, ok
}

func TestScopeStartup(t *testing.T) {
	panics := func(f func()) (panicked bool) {
		defer func() { panicked = recover() != nil }()
		f()
		return false
	}
	if !panics(func() { manifest.KindOf(&manifest.RawObject{APIVersion: "v1", Kind: "ConfigMap"}).Namespaced() }) {
		t.Fatal("missing scope resolver silently supplied an answer")
	}
	if (manifest.KindInfo{}).Namespaced() {
		t.Fatal("empty kind has namespace scope")
	}
	if !panics(func() { manifest.RegisterScopeResolver(nil) }) {
		t.Fatal("nil scope resolver was accepted at startup")
	}
	r := &resolver{memo: make(map[yaml.TypeMeta]bool)}
	manifest.RegisterScopeResolver(r)
	obj, _, _, err := manifest.ParseDoc(map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "cm"}}, manifest.ParseDocOptions{DefaultNamespace: "apps"})
	if err != nil || obj.Named().Namespace != "apps" {
		t.Fatalf("standalone namespace default = %v, %v", obj, err)
	}
	obj, _, _, err = manifest.ParseDoc(map[string]any{"apiVersion": manifest.FluxOperatorDomain + "/v1", "kind": manifest.KindResourceSet, "metadata": map[string]any{"name": "set", "namespace": "authored"},
		"spec": map[string]any{"dependsOn": []any{map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "name": "role", "namespace": "explicit"}}}}, manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	before := r.resolutions
	for ref := range manifest.References(obj, manifest.ReferenceRelations) {
		if ref.Namespace != "" {
			t.Fatalf("cluster dependency namespace = %q", ref.Namespace)
		}
	}
	direct := &manifest.ResourceSet{DependsOn: []fluxopv1.Dependency{{APIVersion: "direct.example/v1", Kind: "Widget", Name: "widget"}}}
	if !panics(func() {
		for range manifest.References(direct, manifest.ReferenceRelations) {
			t.Error("unprepared dependency yielded a reference")
		}
	}) || r.resolutions != before {
		t.Fatal("memo miss did not preserve the preparation invariant")
	}
	manifest.KindOf(&manifest.RawObject{APIVersion: "direct.example/v1", Kind: "Widget"}).Namespaced()
	count := 0
	for range manifest.References(direct, manifest.ReferenceRelations) {
		count++
	}
	if count != 1 {
		t.Fatalf("prepared direct references = %d; want 1", count)
	}
	if !panics(func() { manifest.RegisterScopeResolver(r) }) {
		t.Fatal("scope owner was replaceable after startup")
	}
}
