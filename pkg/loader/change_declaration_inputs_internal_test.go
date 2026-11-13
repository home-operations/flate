package loader

import (
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
)

func TestFilter_DeclarationInputs(t *testing.T) {
	t.Parallel()
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	consumer := testutil.KustomizationID("apps", "consumer")
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		input:    {File: "settings.yaml", Inputs: []string{"patch.yaml"}},
		consumer: {File: "consumer.yaml", Refs: []manifest.NamedResource{input}},
	}}
	objects := filtertest.MapLister{
		input: &manifest.ConfigMap{Name: input.Name, Namespace: input.Namespace},
		consumer: &manifest.Kustomization{
			Name: consumer.Name, Namespace: consumer.Namespace,
			PostBuild: &kustomizev1.PostBuild{SubstituteFrom: []manifest.SubstituteReference{
				{Kind: input.Kind, Name: input.Name},
			}},
		},
	}
	filter := newAdmission(
		t,
		admissionConfig{
			changes:   change.NewSet([]string{"patch.yaml"}),
			index:     index,
			inventory: objects,
			root:      "/repo",
		},
	)
	assert.Diff(t, filter.KeepNames(), []string{input.String(), consumer.String()})
	assert.Equal(t, filter.role(input), primary)
	assert.Equal(t, filter.role(consumer), primary)
}

func TestFilter_SelectorProviderScope(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"named", "selector"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			for _, inventory := range []string{"stored", "indexed"} {
				t.Run(inventory, func(t *testing.T) {
					t.Parallel()
					provider := &manifest.ResourceSetInputProvider{
						Name:      "images",
						Namespace: "apps",
						Type:      fluxopv1.InputProviderStatic,
						Labels:    map[string]string{"role": "images"},
					}
					ref := fluxopv1.InputProviderReference{
						Name: "images",
						Selector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "invalid"}},
						},
					}
					if route == "selector" {
						ref = fluxopv1.InputProviderReference{
							Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "images"}},
						}
					}
					set := &manifest.ResourceSet{
						Name:       "matrix",
						Namespace:  "apps",
						InputsFrom: []fluxopv1.InputProviderReference{ref},
					}
					objects := filtertest.MapLister{set.Named(): set}
					if inventory == "stored" {
						objects[provider.Named()] = provider
					}
					index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						provider.Named(): {File: "provider.yaml", Declaration: provider},
						set.Named():      {File: "set.yaml", Declaration: set},
					}}
					selected := newAdmission(
						t,
						admissionConfig{
							changes:   change.NewSet([]string{"provider.yaml"}),
							index:     index,
							inventory: objects,
						},
					)
					selected.loader.locations = map[manifest.NamedResource]authoredOccurrence{
						provider.Named(): compactOccurrence(provider, projectedProducerForTest(provider)),
					}
					selected.prepare(selection.PrepareRequest{Invocation: true}, false)
					assert.Equal(t, selected.Reconcile(provider.Named()), true)
					assert.Equal(t, selected.Reconcile(set.Named()), true)
				})
			}
		})
	}
}
