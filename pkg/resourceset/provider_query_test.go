package resourceset_test

import (
	"fmt"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
)

func TestRender_UsesOneProviderQuery(t *testing.T) {
	t.Parallel()
	rs := &manifest.ResourceSet{
		Name: "apps", Namespace: "apps",
		InputsFrom:        []fluxopv1.InputProviderReference{{Name: "provider"}},
		ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.value >>'}\n",
	}
	provider := &manifest.ResourceSetInputProvider{
		Name: "provider", Namespace: "apps",
		DefaultValues: fluxopv1.ResourceSetInput{"value": jsonTmpl(t, `"captured"`)},
	}
	calls := 0
	docs, _, err := renderProviders(
		rs,
		func(fluxopv1.InputProviderReference, manifest.NamedResource) ([]*manifest.ResourceSetInputProvider, error) {
			calls++
			assert.FatalIf(t, calls != 1, "%s", "provider query repeated")
			return []*manifest.ResourceSetInputProvider{provider}, nil
		},
		nil,
	)
	assert.NoError(t, err)
	assert.FatalIf(
		t,
		calls != 1 || len(docs) != 1 || docs[0].Document["metadata"].(map[string]any)["name"] != "captured",
		"query calls=%d output=%v",
		calls,
		docs,
	)
}

func renderProviders(
	rs *manifest.ResourceSet,
	resolve resourceset.ProviderResolver,
	admit func(any) (bool, error),
) ([]manifest.DocOccurrence, []manifest.Warning, error) {
	inputs, err := resourceset.CollectProviders(rs, resolve)
	if err != nil {
		return nil, nil, err
	}
	for i := range inputs {
		input := &inputs[i]
		plan, err := input.Declaration.InputPlan()
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %w", manifest.ErrInput, err)
		}
		input.Inputs = plan.Inputs
		if plan.Kind == manifest.ProviderInputUnsupported {
			input.State = resourceset.ProviderInputUnsupported
		}
	}
	return resourceset.Render(rs, inputs, admit)
}
