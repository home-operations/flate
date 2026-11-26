package values

import (
	"cmp"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type countingProvider struct {
	lookups int
}

func (p *countingProvider) Lookup(manifest.NamedResource) store.ReferenceFacts {
	p.lookups++
	return store.ReferenceFacts{}
}

func TestResolveStaticValue_ParentSubstitutions(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, path, want string
		kind             string
		allowMissing     bool
		inline           map[string]string
		reference        bool
		missingReference bool
		missingKey       bool
		known            bool
		needsInput       bool
	}{
		{name: "inline", path: "./${ROOT}", want: "./apps/red", inline: map[string]string{"ROOT": "apps/red"}, known: true},
		{name: "reference", path: "./${ROOT}", want: "./apps/blue", reference: true, known: true},
		{name: "precedence", path: "./${ROOT}", want: "./apps/red", inline: map[string]string{"ROOT": "apps/red"}, reference: true, known: true},
		{name: "missing reference", path: "./${ROOT}", reference: true, missingReference: true, needsInput: true},
		{name: "inline with missing reference", path: "./${ROOT}", want: "./apps/red", inline: map[string]string{"ROOT": "apps/red"}, reference: true, missingReference: true, known: true},
		{name: "default", path: "./${ROOT:-apps/red}", want: "./apps/red", known: true},
		{name: "absent key default", path: "./${ROOT:-apps/red}", want: "./apps/red", reference: true, missingKey: true, known: true},
		{name: "absent key", path: "./${ROOT}", reference: true, missingKey: true},
		{name: "transform", path: "./${ROOT/red/blue}", want: "./apps/blue", inline: map[string]string{"ROOT": "apps/red"}, known: true},
		{name: "unknown", path: "./${ROOT}", known: false},
		{name: "selected missing ConfigMap", path: "./${ROOT}", reference: true, missingReference: true, allowMissing: true, needsInput: true},
		{name: "missing Secret", path: "./${ROOT}", kind: manifest.KindSecret, reference: true, missingReference: true, needsInput: true},
		{name: "selected missing Secret", path: "./${ROOT}", kind: manifest.KindSecret, reference: true, missingReference: true, allowMissing: true, needsInput: true},
		{name: "inline with missing Secret", path: "./${ROOT}", want: "./apps/red", inline: map[string]string{"ROOT": "apps/red"}, kind: manifest.KindSecret, reference: true, missingReference: true, known: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			kind := cmp.Or(tc.kind, manifest.KindConfigMap)
			inline := tc.inline
			if inline == nil {
				inline = map[string]string{"ENABLE": "1"}
			}
			parent.PostBuild = &kustomizev1.PostBuild{Substitute: inline}
			if tc.reference {
				parent.PostBuild.SubstituteFrom = []kustomizev1.SubstituteReference{
					{Kind: kind, Name: "settings"},
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
			var kinds []string
			if tc.allowMissing {
				kinds = []string{kind}
			}
			got, known, needsInput := ResolveStaticValue(tc.path, parent, provider, testPermissions(kinds), false)
			assert.Equal(t, known, tc.known)
			assert.Equal(t, needsInput, tc.needsInput)
			if known {
				assert.Equal(t, got, tc.want)
			}
			assert.Equal(t, manifest.Equal(parent, before), true)
		})
	}
	t.Run("repeated missing reference", func(t *testing.T) {
		t.Parallel()
		ref := kustomizev1.SubstituteReference{Kind: manifest.KindConfigMap, Name: "missing"}
		parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{
			SubstituteFrom: []kustomizev1.SubstituteReference{ref, ref},
		}}
		provider := &countingProvider{}
		_, known, needsInput := ResolveStaticValue("./${ROOT}", parent, provider, nil, false)
		assert.Equal(t, known, false)
		assert.Equal(t, needsInput, true)
		assert.Equal(t, provider.lookups, 2)
	})
}

func TestResolveStaticValue_Activation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		postBuild  *kustomizev1.PostBuild
		provider   *sliceProvider
		want       string
		known      bool
		needsInput bool
		unresolved bool
	}{
		{name: "nil PostBuild", want: "./${ROOT:-apps/red}", known: true},
		{name: "empty substitutions", postBuild: &kustomizev1.PostBuild{Substitute: map[string]string{}}, want: "./${ROOT:-apps/red}", known: true},
		{name: "nonempty substitutions", postBuild: &kustomizev1.PostBuild{Substitute: map[string]string{"ROOT": "apps/blue"}}, want: "./apps/blue", known: true},
		{name: "Always empty", postBuild: &kustomizev1.PostBuild{SubstituteStrategy: kustomizev1.SubstituteStrategyAlways}, want: "./apps/red", known: true},
		{
			name: "readable expanded reference",
			postBuild: &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "settings"},
			}},
			provider: &sliceProvider{ConfigMaps: []*manifest.ConfigMap{{
				Name: "settings", Namespace: "apps", Data: map[string]any{"ROOT": "apps/blue"},
			}}},
			want: "./apps/blue", known: true,
		},
		{
			name: "readable empty reference",
			postBuild: &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "settings"},
			}},
			provider: &sliceProvider{ConfigMaps: []*manifest.ConfigMap{{Name: "settings", Namespace: "apps"}}},
			want:     "./${ROOT:-apps/red}", known: true,
		},
		{
			name: "material error",
			postBuild: &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{
				{Kind: manifest.KindSecret, Name: "settings"},
			}},
			provider: &sliceProvider{Secrets: []*manifest.Secret{{
				Name: "settings", Namespace: "apps", Data: map[string]any{"ROOT": "!!!"},
			}}},
			want: "./apps/red",
		},
		{
			name: "unknown inputs",
			postBuild: &kustomizev1.PostBuild{SubstituteFrom: []kustomizev1.SubstituteReference{
				{Kind: manifest.KindConfigMap, Name: "missing"},
			}},
			want: "./apps/red", needsInput: true,
		},
		{
			name:       "unresolved reference name",
			postBuild:  &kustomizev1.PostBuild{},
			want:       "./apps/red",
			unresolved: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps", PostBuild: tc.postBuild}
			before := parent.Clone()
			provider := cmp.Or(tc.provider, &sliceProvider{})
			got, known, needsInput := ResolveStaticValue(
				"./${ROOT:-apps/red}", parent, provider, testPermissions(nil), tc.unresolved,
			)
			assert.Equal(t, known, tc.known)
			assert.Equal(t, needsInput, tc.needsInput)
			assert.Equal(t, got, tc.want)
			assert.Equal(t, manifest.Equal(parent, before), true)
		})
	}
}
