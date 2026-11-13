package selection

import (
	"slices"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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
			matches, known, err := selected.matchingInputs(new(ref), true)
			assert.NoError(t, err)
			assert.Equal(t, known, true)
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
		got, known, err := closure.matchingInputs(&query, false)
		assert.NoError(t, err)
		assert.Equal(t, known, true)
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

func TestAdmission_SelectorMembership(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, value  string
		known, match bool
	}{
		{name: "unknown default", value: "${ROLE:-selected}"},
		{name: "known empty", value: "absent", known: true},
		{name: "known match", value: "selected", known: true, match: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			provider := &manifest.ResourceSetInputProvider{
				Name:      "input",
				Namespace: "apps",
				Labels:    map[string]string{"role": "selected"},
			}
			reader := &manifest.ResourceSet{
				Name:      "reader",
				Namespace: "apps",
				InputsFrom: []fluxopv1.InputProviderReference{{
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": tc.value}},
				}},
			}
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			st := store.New()
			st.AddObjects([]manifest.BaseManifest{reader, provider, owner})
			facts := &solverFacts{
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					reader.Named(): {Parent: owner.Named(), Productions: []manifest.Production{{Owner: owner.Named()}}},
				}},
			}
			closure := solverClosure(t, st, facts)
			closure.generation = []store.Child{
				{Obj: reader, Productions: []manifest.Production{{Owner: owner.Named()}}},
			}
			closure.captureIncoming()
			closure.states[owner.Named()] = admissionState{
				role: admissionRole{needed: true, ordinary: true, selected: true, dirty: true},
			}
			closure.states[provider.Named()] = admissionState{role: admissionRole{needed: true, dirty: true}}
			var ref manifest.Reference
			for candidate := range manifest.References(reader, manifest.ReferenceRelations) {
				if candidate.Use == "inputsFrom" {
					ref = candidate
				}
			}
			inputs, known, err := closure.matchingInputs(&ref, false)
			assert.NoError(t, err)
			assert.Equal(t, known, tc.known)
			assert.Equal(t, len(inputs) != 0, tc.match)

			searches := 0
			assert.NoError(
				t,
				closure.selectReferenceInputs(
					reader.Named(),
					&ref,
					func(materializerDemand, manifest.NamedResource) bool {
						searches++
						return false
					},
				),
			)
			wantSearches := 0
			if tc.known && !tc.match {
				wantSearches = 1
			}
			assert.Equal(t, searches, wantSearches)
			assert.Equal(t, closure.role(owner.Named()).needed, true)

			var seeds admissionSeeds
			links := 0
			seeds.scanReferences(&closure, reader.Named(), reader, func(from, to manifest.NamedResource) {
				assert.Equal(t, from, provider.Named())
				assert.Equal(t, to, reader.Named())
				links++
			})
			assert.Equal(t, links != 0, tc.match)
			assert.Equal(t, len(seeds), 0)
			assert.Equal(t, closure.reverseInputRole(reader.Named(), admissionRole{}, true).dirty, tc.match)
			assert.NoError(t, closure.selectInputReader(provider.Named(), reader.Named()))
			assert.Equal(t, closure.role(reader.Named()).dirty, tc.match)
			assert.Equal(t, closure.productionRole(reader.Named(), admissionRole{}, true).dirty, true)
		})
	}
}

func TestAdmission_SelectorCompleteness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		ref            manifest.Reference
		known, invalid bool
	}{
		{name: "unknown reference name", ref: manifest.Reference{Name: "${INPUT}", Namespace: "apps"}},
		{name: "unknown reference namespace", ref: manifest.Reference{Name: "provider", Namespace: "${NS}"}},
		{name: "unknown label key", ref: manifest.Reference{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"${KEY}": "x"}}}},
		{name: "unknown operator", ref: manifest.Reference{Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "${OP}", Values: []string{"x"}}}}}},
		{name: "unknown expression key", ref: manifest.Reference{Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "${KEY}", Operator: "In", Values: []string{"x"}}}}}},
		{name: "unknown expression value", ref: manifest.Reference{Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "NotIn", Values: []string{"${VALUE}"}}}}}},
		{name: "complete invalid", ref: manifest.Reference{Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "invalid"}}}}, known: true, invalid: true},
		{name: "named precedence", ref: manifest.Reference{Name: "provider", Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "${VALUE}"}}}, known: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			closure := solverClosure(t, st, &solverFacts{})
			inputs, known, err := closure.matchingInputs(&tc.ref, false)
			assert.Equal(t, known, tc.known)
			assert.Equal(t, err != nil, tc.invalid)
			if !tc.known {
				assert.Equal(t, len(inputs), 0)
			}
		})
	}
}

//nolint:gocognit // Authored and rendered candidates MUST share the same reference and label precedence assertions.
func TestSelection_PreparedProjection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, builder       string
		supported, authored bool
	}{
		{name: "authored", supported: true, authored: true},
		{name: "rendered", builder: "built", supported: true},
		{name: "no source support"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			provider := &manifest.ResourceSetInputProvider{
				Name:      "provider",
				Namespace: "apps",
				Labels:    map[string]string{"role": "${ROLE}"},
			}
			reader := &manifest.ResourceSet{
				Name:      "reader",
				Namespace: "apps",
				InputsFrom: []fluxopv1.InputProviderReference{{
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "${ROLE}"}},
				}},
			}
			var resolved manifest.Reference
			for ref := range manifest.References(reader, manifest.ReferenceRelations) {
				if ref.Use == "inputsFrom" {
					resolved = ref.Clone()
					resolved.Selector.MatchLabels["role"] = "selected"
				}
			}
			providerCatalog, readerCatalog := *provider, *reader
			facts := &solverFacts{
				index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					reader.Named(): {Builder: tc.builder}, provider.Named(): {Builder: tc.builder},
				}},
				occurrences: map[manifest.NamedResource]OccurrenceFacts{
					reader.Named(): {
						Identity:     reader.Named(),
						Catalog:      &readerCatalog,
						CatalogKnown: true,
						Refs:         []manifest.Reference{resolved},
					},
					provider.Named(): {
						Identity:     provider.Named(),
						Catalog:      &providerCatalog,
						CatalogKnown: true,
						Labels:       map[string]string{"role": "selected"},
					},
				},
			}
			if tc.builder != "" {
				facts.candidates = map[manifest.NamedResource]store.Child{reader.Named(): {Obj: reader}}
			}
			closure := solverClosure(t, store.New(), facts)
			var support manifest.NamedResource
			if tc.supported {
				support = manifest.BootstrapSourceID
			}
			closure.children = map[manifest.NamedResource]store.Child{
				reader.Named(): {
					Obj:           reader,
					SourceSupport: support,
				},
				provider.Named(): {Obj: provider, SourceSupport: support},
			}
			refs := 0
			for ref := range closure.references(reader.Named(), reader) {
				if ref.Use != "inputsFrom" {
					continue
				}
				refs++
				inputs, known, err := closure.matchingInputs(&ref, false)
				assert.NoError(t, err)
				assert.Equal(t, known, tc.authored)
				assert.Equal(t, slices.Contains(inputs, provider.Named()), tc.authored)
			}
			assert.Equal(t, refs, 1)
			if tc.builder != "" {
				closure.yieldIndexedReferences(reader.Named(), func(ref manifest.Reference) bool {
					if ref.Use == "inputsFrom" {
						assert.Equal(t, ref.Selector.MatchLabels["role"], "${ROLE}")
					}
					return true
				})
			}
			assert.Equal(t, reader.InputsFrom[0].Selector.MatchLabels["role"], "${ROLE}")
			assert.Equal(t, provider.Labels["role"], "${ROLE}")
		})
	}
}
