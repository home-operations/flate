package selection

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"

	"github.com/home-operations/flate/internal/assert"
)

func TestSelection_Matches(t *testing.T) {
	t.Parallel()
	core := manifest.NamedResource{Kind: "ConfigMap", Namespace: "apps", Name: "values"}
	for _, tt := range []struct {
		name  string
		roots []manifest.NamedResource
		id    manifest.NamedResource
		want  bool
	}{
		{name: "universal", id: core, want: true},
		{name: "core", roots: []manifest.NamedResource{{Kind: "ConfigMap"}}, id: core, want: true},
		{name: "exact group", roots: []manifest.NamedResource{{Group: "example.com", Kind: "ConfigMap"}}, id: core},
		{name: "namespace", roots: []manifest.NamedResource{{Kind: "ConfigMap", Namespace: "other"}}, id: core},
		{name: "name", roots: []manifest.NamedResource{{Kind: "ConfigMap", Name: "other"}}, id: core},
		{name: "cluster scoped", roots: []manifest.NamedResource{{Group: "apiextensions.k8s.io", Kind: "CustomResourceDefinition", Namespace: "apps"}}, id: manifest.NamedResource{Group: "apiextensions.k8s.io", Kind: "CustomResourceDefinition", Name: "tests.example.com"}, want: true},
		{name: "union", roots: []manifest.NamedResource{{Kind: "Secret"}, {Kind: "ConfigMap"}}, id: core, want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			{
				got := (Request{Roots: tt.roots}).Matches(tt.id)
				assert.FatalIf(t, got != tt.want, "Matches(%v) = %v, want %v", tt.id, got, tt.want)
			}
		})
	}
}

func TestSelection_CompleteKindsIncludeStandaloneNamespace(t *testing.T) {
	t.Parallel()
	intent := Request{}
	for info := range manifest.Kinds() {
		if info.Render {
			intent.Roots = append(
				intent.Roots,
				manifest.NamedResource{Group: info.Group, Kind: info.Kind, Namespace: "apps"},
			)
		}
	}
	assert.Equal(
		t,
		intent.Matches(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "data"}),
		true,
	)
	assert.Equal(
		t,
		intent.Matches(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "other", Name: "data"}),
		false,
	)
	assert.Equal(
		t,
		intent.Matches(
			manifest.NamedResource{Group: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "cluster"},
		),
		true,
	)
}
