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
		known            bool
	}{
		{name: "inline", path: "./${ROOT}", want: "./apps/red", inline: map[string]string{"ROOT": "apps/red"}, known: true},
		{name: "reference", path: "./${ROOT}", want: "./apps/blue", reference: true, known: true},
		{name: "precedence", path: "./${ROOT}", want: "./apps/red", inline: map[string]string{"ROOT": "apps/red"}, reference: true, known: true},
		{name: "missing reference", path: "./${ROOT}", reference: true, missingReference: true},
		{name: "inline with missing reference", path: "./${ROOT}", want: "./apps/red", inline: map[string]string{"ROOT": "apps/red"}, reference: true, missingReference: true, known: true},
		{name: "default", path: "./${ROOT:-apps/red}", want: "./apps/red", known: true},
		{name: "transform", path: "./${ROOT/red/blue}", want: "./apps/blue", inline: map[string]string{"ROOT": "apps/red"}, known: true},
		{name: "unknown", path: "./${ROOT}", known: false},
		{name: "placeholder default", path: "./${ROOT:-apps/red}", inline: map[string]string{"ROOT": manifest.ValuePlaceholder("ROOT")}, known: false},
		{name: "placeholder", path: "./${ROOT}", inline: map[string]string{"ROOT": manifest.ValuePlaceholder("ROOT")}, known: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testResolveStaticPathParentSubstitutionsCase(t, &tc)
		},
		)
	}
}

func testResolveStaticPathParentSubstitutionsCase(
	t *testing.T,
	testCaseInput *struct {
		name             string
		path             string
		want             string
		inline           map[string]string
		reference        bool
		missingReference bool
		known            bool
	},
) {
	t.Helper()

	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	inline := testCaseInput.inline
	if inline == nil {
		inline = map[string]string{"ENABLE": "1"}
	}
	parent.PostBuild = &kustomizev1.PostBuild{Substitute: inline}
	if testCaseInput.reference {
		parent.PostBuild.SubstituteFrom = []kustomizev1.SubstituteReference{
			{Kind: manifest.KindConfigMap, Name: "settings"},
		}
		if testCaseInput.missingReference {
			parent.PostBuild.SubstituteFrom[0].Name = "missing"
		}
	}
	provider := &sliceProvider{
		ConfigMaps: []*manifest.ConfigMap{
			{Name: "settings", Namespace: "apps", Data: map[string]any{"ROOT": "apps/blue"}},
		},
	}
	before := parent.Clone()
	got, known := ResolveStaticPath(testCaseInput.path, parent, provider)
	assert.Equal(t, known, testCaseInput.known)
	if known {
		assert.Equal(t, got, testCaseInput.want)
	}
	assert.Equal(t, manifest.Equal(parent, before), true)
}
