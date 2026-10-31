package manifest

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestSelection_Matches(t *testing.T) {
	t.Parallel()
	core := NamedResource{Kind: "ConfigMap", Namespace: "apps", Name: "values"}
	for _, tt := range []struct {
		name  string
		roots []NamedResource
		id    NamedResource
		want  bool
	}{
		{name: "universal", id: core, want: true},
		{name: "core", roots: []NamedResource{{Kind: "ConfigMap"}}, id: core, want: true},
		{name: "exact group", roots: []NamedResource{{Group: "example.com", Kind: "ConfigMap"}}, id: core},
		{name: "namespace", roots: []NamedResource{{Kind: "ConfigMap", Namespace: "other"}}, id: core},
		{name: "name", roots: []NamedResource{{Kind: "ConfigMap", Name: "other"}}, id: core},
		{name: "cluster scoped", roots: []NamedResource{{Group: "apiextensions.k8s.io", Kind: "CustomResourceDefinition", Namespace: "apps"}}, id: NamedResource{Group: "apiextensions.k8s.io", Kind: "CustomResourceDefinition", Name: "tests.example.com"}, want: true},
		{name: "union", roots: []NamedResource{{Kind: "Secret"}, {Kind: "ConfigMap"}}, id: core, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := (Selection{Roots: tt.roots}).Matches(tt.id); got != tt.want {
				t.Fatalf("Matches(%v) = %v, want %v", tt.id, got, tt.want)
			}
		})
	}
}

func TestSelection_CompleteKindsIncludeStandaloneNamespace(t *testing.T) {
	t.Parallel()
	intent := Selection{}
	for info := range Kinds() {
		if info.Render {
			intent.Roots = append(intent.Roots, NamedResource{Group: info.Group, Kind: info.Kind, Namespace: "apps"})
		}
	}
	assert.Equal(t, intent.Matches(NamedResource{Kind: KindConfigMap, Namespace: "apps", Name: "data"}), true)
	assert.Equal(t, intent.Matches(NamedResource{Kind: KindConfigMap, Namespace: "other", Name: "data"}), false)
	assert.Equal(t, intent.Matches(NamedResource{Group: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "cluster"}), true)
}
