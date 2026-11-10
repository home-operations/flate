package values

import (
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestResolveStaticPath_ParentSubstitutions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, want string
		inline           map[string]string
		reference        bool
		missingReference bool
		missingKey       bool
		known            bool
	}{
		{name: "inline", path: "./${ROOT}", want: "./apps/red", inline: map[string]string{"ROOT": "apps/red"}, known: true},
		{name: "reference", path: "./${ROOT}", want: "./apps/blue", reference: true, known: true},
		{name: "precedence", path: "./${ROOT}", want: "./apps/red", inline: map[string]string{"ROOT": "apps/red"}, reference: true, known: true},
		{name: "missing reference", path: "./${ROOT}", reference: true, missingReference: true},
		{name: "inline with missing reference", path: "./${ROOT}", want: "./apps/red", inline: map[string]string{"ROOT": "apps/red"}, reference: true, missingReference: true, known: true},
		{name: "default", path: "./${ROOT:-apps/red}", want: "./apps/red", known: true},
		{name: "absent key default", path: "./${ROOT:-apps/red}", want: "./apps/red", reference: true, missingKey: true, known: true},
		{name: "absent key", path: "./${ROOT}", reference: true, missingKey: true},
		{name: "transform", path: "./${ROOT/red/blue}", want: "./apps/blue", inline: map[string]string{"ROOT": "apps/red"}, known: true},
		{name: "unknown", path: "./${ROOT}", known: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			inline := tc.inline
			if inline == nil {
				inline = map[string]string{"ENABLE": "1"}
			}
			parent.PostBuild = &kustomizev1.PostBuild{Substitute: inline}
			if tc.reference {
				parent.PostBuild.SubstituteFrom = []kustomizev1.SubstituteReference{
					{Kind: manifest.KindConfigMap, Name: "settings"},
				}
				if tc.missingReference {
					parent.PostBuild.SubstituteFrom[0].Name = "missing"
				}
			}
			provider := &sliceProvider{
				ConfigMaps: []*manifest.ConfigMap{
					{Name: "settings", Namespace: "apps", Data: map[string]any{"ROOT": "apps/blue"}},
				},
			}
			if tc.missingKey {
				provider.ConfigMaps[0].Data = nil
			}
			before := parent.Clone()
			got, known := ResolveStaticPath(tc.path, parent, provider)
			assert.Equal(t, known, tc.known)
			if known {
				assert.Equal(t, got, tc.want)
			}
			assert.Equal(t, manifest.Equal(parent, before), true)
		})
	}
}

func TestResolveStaticPath_Activation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		postBuild *kustomizev1.PostBuild
		want      string
		known     bool
	}{
		{name: "nil PostBuild", want: "./${ROOT:-apps/red}", known: true},
		{name: "empty substitutions", postBuild: &kustomizev1.PostBuild{Substitute: map[string]string{}}, want: "./${ROOT:-apps/red}", known: true},
		{name: "nonempty substitutions", postBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"ROOT": "apps/blue"}}, want: "./apps/blue", known: true},
		{name: "Always empty", postBuild: &kustomizev1.PostBuild{SubstituteStrategy: kustomizev1.SubstituteStrategyAlways}, want: "./apps/red", known: true},
		{
			name: "unknown inputs",
			postBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "missing"},
			}},
			want: "./apps/red",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", PostBuild: tc.postBuild}
			before := parent.Clone()
			got, known := ResolveStaticPath("./${ROOT:-apps/red}", parent, &sliceProvider{})
			assert.Equal(t, known, tc.known)
			assert.Equal(t, got, tc.want)
			assert.Equal(t, manifest.Equal(parent, before), true)
		})
	}
}
