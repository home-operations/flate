package resourceset_test

import (
	"testing"

	"github.com/home-operations/flate/internal/resourceset"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestRender_NullInput(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "null", Namespace: "apps",
		Inputs: []fluxopv1.ResourceSetInput{{"value": nil, "malformed": {Raw: []byte("invalid")}}},
		ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: null-input}
data:
  value: << if eq inputs.value nil >>"null"<< end >>
  malformed: << if hasKey inputs "malformed" >>present<< else >>omitted<< end >>`,
	}
	docs, _, err := renderProviders(rs, nil, nil)
	assert.NoError(t, err)
	assert.FatalIf(t, len(docs) != 1, "documents = %d, want 1", len(docs))
	assert.Diff(t, docs[0].Document["data"], map[string]any{"value": "null", "malformed": "omitted"})
}

func TestRender_EmptyInputRule(t *testing.T) {
	t.Parallel()
	for _, strategy := range []string{fluxopv1.InputStrategyFlatten, fluxopv1.InputStrategyPermute} {
		for _, referenced := range []bool{false, true} {
			name := strategy + "/inline-only"
			if referenced {
				name = strategy + "/empty-provider"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				rs := &manifest.ResourceSet{
					Name:              "empty",
					Namespace:         "default",
					InputStrategy:     &fluxopv1.InputStrategySpec{Name: strategy},
					ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: static}",
				}
				want := 1
				if referenced {
					rs.InputsFrom = []fluxopv1.InputProviderReference{{Name: "empty"}}
					want = 0
				}
				resolve := func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
					return []*manifest.ResourceSetInputProvider{
						{Name: "empty", Type: fluxopv1.InputProviderGitHubBranch},
					}, nil
				}
				docs, _, err := renderProviders(rs, resolve, nil)
				assert.NoError(t, err)
				assert.FatalIf(t, len(docs) != want, "docs = %d, want %d", len(docs), want)
			})
		}
	}
}

func TestRender_EvaluatedInputsRemainImmutable(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name:              "matrix",
		Namespace:         "apps",
		InputsFrom:        []fluxopv1.InputProviderReference{{Name: "images"}},
		ResourcesTemplate: "<< $_ := set inputs.nested \"value\" \"mutated\" >>apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.tag >>'}\n",
	}
	inputs := []resourceset.ProviderInputs{
		{
			ID: manifest.NamedResource{
				Group:     manifest.FluxOperatorDomain,
				Kind:      manifest.KindResourceSetInputProvider,
				Namespace: "apps",
				Name:      "images",
			},
			Inputs: []map[string]any{{"tag": "selected", "nested": map[string]any{"value": "captured"}}},
		},
	}
	for range 2 {
		docs, warnings, err := resourceset.Render(rs, inputs, nil, nil)
		assert.NoError(t, err)
		assert.Equal(t, len(docs), 1)
		assert.Equal(t, len(warnings), 0)
		assert.Equal(t, inputs[0].Inputs[0]["nested"].(map[string]any)["value"], any("captured"))
		{
			_, exists := inputs[0].Inputs[0]["provider"]
			assert.FatalIf(t, exists, "%v", "render mutated captured provider inputs")
		}
	}
	inputs[0].Inputs = nil
	docs, warnings, err := resourceset.Render(rs, inputs, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, len(docs), 0)
	assert.Equal(t, len(warnings), 0)
}
