package selection

import (
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/store"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestSelection_SelectorCandidates(t *testing.T) {
	t.Parallel()
	provider := &manifest.ResourceSetInputProvider{
		Name:      "images",
		Namespace: "apps",
		Labels:    map[string]string{"role": "images"},
	}
	other := *provider
	other.Labels = map[string]string{"role": "other"}
	bare := *provider
	bare.Namespace = ""
	set := &manifest.ResourceSet{
		Name:       "matrix",
		Namespace:  "apps",
		InputsFrom: []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{MatchLabels: provider.Labels}}},
	}
	for _, tc := range []struct {
		name        string
		declaration manifest.BaseManifest
		current     manifest.BaseManifest
		incoming    manifest.BaseManifest
		want        bool
	}{
		{"unpublished", provider, nil, nil, true},
		{"effective_namespace", &bare, nil, nil, true},
		{"current_overrides_declaration", provider, &other, nil, false},
		{"incoming_overrides_current", &other, &other, provider, true},
		{"incoming_removes_match", provider, provider, &other, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testSelectionSelectorCandidatesCase(
				t,
				&testSelectionSelectorCandidatesCaseInputs{provider: &provider, set: &set, tc: &tc},
			)
		},
		)
	}
}

type testSelectionSelectorCandidatesCaseInputs struct {
	provider **manifest.ResourceSetInputProvider
	set      **manifest.ResourceSet
	tc       *struct {
		name        string
		declaration manifest.BaseManifest
		current     manifest.BaseManifest
		incoming    manifest.BaseManifest
		want        bool
	}
}

func testSelectionSelectorCandidatesCase(t *testing.T, testCaseInput *testSelectionSelectorCandidatesCaseInputs) {
	t.Helper()

	objects := filtertest.MapLister{(*testCaseInput.set).Named(): (*testCaseInput.set)}
	if testCaseInput.tc.current != nil {
		objects[(*testCaseInput.provider).Named()] = testCaseInput.tc.current
	}
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		(*testCaseInput.set).Named(): {
			File:        "set.yaml",
			Declaration: (*testCaseInput.set),
		},
		(*testCaseInput.provider).Named(): {File: "provider.yaml", Declaration: testCaseInput.tc.declaration},
	}}
	st := store.New()
	st.AddObjects(objects.Objects())
	facts := &solverFacts{index: *index, occurrences: map[manifest.NamedResource]OccurrenceFacts{
		(*testCaseInput.provider).Named(): {
			Identity:     (*testCaseInput.provider).Named(),
			Authored:     testCaseInput.tc.declaration.Named(),
			Catalog:      testCaseInput.tc.declaration,
			CatalogKnown: true,
			Labels:       testCaseInput.tc.declaration.(*manifest.ResourceSetInputProvider).Labels,
		},
	}}
	record := facts.index.Records[(*testCaseInput.provider).Named()]
	record.Declaration = nil
	facts.index.Records[(*testCaseInput.provider).Named()] = record
	selected := solverClosure(t, st, facts)
	selected.generation = []store.Child{{Obj: testCaseInput.tc.incoming}}
	selected.captureIncoming()
	var got []manifest.NamedResource
	for ref := range manifest.References((*testCaseInput.set), manifest.ReferenceRelations) {
		if ref.Use == "inputsFrom" {
			matches, err := selected.matchingInputs(new(ref), true)
			assert.NoError(t, err)
			got = append(got, matches...)
		}
	}
	var want []manifest.NamedResource
	if testCaseInput.tc.want {
		want = []manifest.NamedResource{(*testCaseInput.provider).Named()}
	}
	assert.Diff(t, got, want)
	assert.Equal(t, st.GetObject((*testCaseInput.provider).Named()), testCaseInput.tc.current)
	assert.Equal(t, objects.GetObject((*testCaseInput.provider).Named()), testCaseInput.tc.current)
}

func TestAdmission_CompactSelectorInputs(t *testing.T) {
	t.Parallel()
	t.Run("compact records", func(t *testing.T) {
		t.Parallel()
		st := store.New()
		facts := &solverFacts{
			occurrences: map[manifest.NamedResource]OccurrenceFacts{},
			index:       manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
		}
		for _, name := range []string{"z", "a"} {
			provider := &manifest.ResourceSetInputProvider{Name: name, Labels: map[string]string{"app": "selected"}}
			id := provider.Named()
			id.Namespace = "apps"
			facts.occurrences[id] = OccurrenceFacts{
				Identity:     id,
				Authored:     provider.Named(),
				Catalog:      provider,
				CatalogKnown: true,
				Labels:       provider.Labels,
			}
			facts.index.Records[id] = manifest.DiscoveryRecord{}
		}
		query := manifest.Reference{
			Group:     manifest.FluxOperatorDomain,
			Kind:      manifest.KindResourceSetInputProvider,
			Namespace: "apps",
			Selector:  &metav1.LabelSelector{MatchLabels: map[string]string{"app": "selected"}},
		}
		closure := solverClosure(t, st, facts)
		got, err := closure.matchingInputs(&query, false)
		assert.NoError(t, err)
		assert.Diff(t, got, []manifest.NamedResource{
			{
				Group:     manifest.FluxOperatorDomain,
				Kind:      manifest.KindResourceSetInputProvider,
				Namespace: "apps",
				Name:      "a",
			},
			{
				Group:     manifest.FluxOperatorDomain,
				Kind:      manifest.KindResourceSetInputProvider,
				Namespace: "apps",
				Name:      "z",
			},
		})
		assert.Equal(t, len(st.Objects()), 0)
	})
}
