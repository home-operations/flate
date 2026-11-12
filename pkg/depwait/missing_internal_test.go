package depwait

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/values"
)

func TestMissingPolicy_ReferenceRules(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                                 string
		ref                                  manifest.Reference
		input                                store.ReferenceFacts
		kinds                                []string
		allowed, unreadable, producerAbsence bool
	}{
		{name: "required values Secret", ref: manifest.Reference{Kind: manifest.KindSecret, Use: "valuesFrom"}},
		{name: "selected values Secret", ref: manifest.Reference{Kind: manifest.KindSecret, Use: "valuesFrom"}, kinds: []string{manifest.KindSecret}, allowed: true},
		{name: "Secret selection leaves values ConfigMap required", ref: manifest.Reference{Kind: manifest.KindConfigMap, Use: "valuesFrom"}, kinds: []string{manifest.KindSecret}},
		{name: "selected values ConfigMap", ref: manifest.Reference{Kind: manifest.KindConfigMap, Use: "valuesFrom"}, kinds: []string{manifest.KindConfigMap}, allowed: true},
		{name: "ConfigMap selection leaves values Secret required", ref: manifest.Reference{Kind: manifest.KindSecret, Use: "valuesFrom"}, kinds: []string{manifest.KindConfigMap}},
		{name: "foreign values kind", ref: manifest.Reference{Group: "example.test", Kind: manifest.KindSecret, Use: "valuesFrom"}, kinds: []string{manifest.KindSecret}},
		{name: "unsupported values kind", ref: manifest.Reference{Kind: "Widget", Use: "valuesFrom"}, kinds: []string{"Widget"}},
		{name: "optional values", ref: manifest.Reference{Kind: manifest.KindConfigMap, Use: "valuesFrom", Optional: true}, allowed: true},
		{name: "required substitution ConfigMap", ref: manifest.Reference{Kind: manifest.KindConfigMap, Use: "substituteFrom"}, kinds: []string{manifest.KindConfigMap}},
		{name: "optional substitution ConfigMap", ref: manifest.Reference{Kind: manifest.KindConfigMap, Use: "substituteFrom", Optional: true}, allowed: true},
		{name: "required substitution Secret", ref: manifest.Reference{Kind: manifest.KindSecret, Use: "substituteFrom"}, unreadable: true},
		{name: "selected substitution Secret remains required", ref: manifest.Reference{Kind: manifest.KindSecret, Use: "substituteFrom"}, kinds: []string{manifest.KindSecret}, unreadable: true},
		{name: "empty substitution Secret", ref: manifest.Reference{Kind: manifest.KindSecret, Use: "substituteFrom"}, input: store.ReferenceFacts{Data: &manifest.Secret{}}, unreadable: true},
		{name: "readable substitution Secret", ref: manifest.Reference{Kind: manifest.KindSecret, Use: "substituteFrom"}, input: store.ReferenceFacts{Data: &manifest.Secret{StringData: map[string]any{"VALUE": "input"}}}},
		{name: "optional substitution Secret", ref: manifest.Reference{Kind: manifest.KindSecret, Use: "substituteFrom", Optional: true}, allowed: true},
		{name: "foreign substitution Secret", ref: manifest.Reference{Group: "example.test", Kind: manifest.KindSecret, Use: "substituteFrom"}},
		{name: "generated substitution Secret", ref: manifest.Reference{Kind: manifest.KindSecret, Use: "substituteFrom"}, input: store.ReferenceFacts{Generated: true}, allowed: true, unreadable: true, producerAbsence: true},
		{name: "offline generated substitution Secret", ref: manifest.Reference{Kind: manifest.KindSecret, Use: "substituteFrom"}, input: store.ReferenceFacts{Generated: true, Offline: true}, allowed: true, unreadable: true, producerAbsence: true},
		{name: "private offline input", ref: manifest.Reference{Kind: manifest.KindSecret, Use: "substituteFrom"}, input: store.ReferenceFacts{Private: true, Offline: true}, allowed: true, producerAbsence: true},
		{name: "materializer alone requires data", input: store.ReferenceFacts{Materializers: []manifest.NamedResource{{Kind: manifest.KindKustomization, Name: "producer"}}}},
		{name: "selected CRD", ref: manifest.Reference{Group: "apiextensions.k8s.io", Kind: manifest.KindCustomResourceDefinition}, kinds: []string{manifest.KindCustomResourceDefinition}, allowed: true},
		{name: "unselected CRD", ref: manifest.Reference{Group: "apiextensions.k8s.io", Kind: manifest.KindCustomResourceDefinition}},
		{name: "foreign CRD", ref: manifest.Reference{Group: "example.test", Kind: manifest.KindCustomResourceDefinition}, kinds: []string{manifest.KindCustomResourceDefinition}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			policy := NewMissingPolicy(tc.kinds)
			got := policy.Admission(new(tc.ref), new(tc.input))
			want := selection.RequiredInput
			if tc.allowed {
				want = selection.OptionalInput
			}
			if tc.input.Data != nil {
				want = selection.AvailableInput
			}
			assert.Equal(t, got, want)
			if tc.ref.NamedResource.KindInfo().Data {
				result, err := values.Classify(new(tc.ref), new(tc.input))
				assert.NoError(t, err)
				assert.Equal(t, result.State() == values.Produced, tc.producerAbsence)
			}
		})
	}
}

func TestMissingPolicy_SelectionSnapshot(t *testing.T) {
	t.Parallel()
	kinds := []string{manifest.KindSecret}
	policy := NewMissingPolicy(kinds)
	kinds[0] = manifest.KindConfigMap
	for _, tc := range []struct {
		kind string
		want bool
	}{
		{manifest.KindSecret, true},
		{manifest.KindConfigMap, false},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			admission := policy.Admission(
				new(manifest.Reference{Kind: tc.kind, Use: "valuesFrom"}),
				new(store.ReferenceFacts{}),
			)
			assert.Equal(t, admission == selection.OptionalInput, tc.want)
			assert.Equal(t, policy.AllowsKind(tc.kind), tc.want)
		})
	}
}

func TestMissingPolicy_ZeroSelection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		policy *MissingPolicy
	}{
		{name: "nil", policy: NewMissingPolicy(nil)},
		{name: "zero", policy: &MissingPolicy{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			admission := tc.policy.Admission(
				new(manifest.Reference{Kind: manifest.KindSecret, Use: "substituteFrom"}), new(store.ReferenceFacts{}),
			)
			assert.Equal(t, admission, selection.RequiredInput)
			admission = tc.policy.Admission(
				new(manifest.Reference{
					Kind:     manifest.KindSecret,
					Use:      "valuesFrom",
					Optional: true,
				}),
				new(store.ReferenceFacts{}),
			)
			assert.Equal(t, admission, selection.OptionalInput)
		})
	}
}
