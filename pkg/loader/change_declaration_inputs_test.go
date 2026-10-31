package loader

import (
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/filtertest"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestFilter_DeclarationInputs(t *testing.T) {
	t.Parallel()
	input := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "settings"}
	consumer := testutil.KustomizationID("apps", "consumer")
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		input:    {File: "settings.yaml", Inputs: []string{"patch.yaml"}},
		consumer: {File: "consumer.yaml", Refs: []manifest.NamedResource{input}},
	}}
	filter := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"patch.yaml"}), index: index, inventory: nil, root: "/repo"})
	assert.Diff(t, filter.KeepNames(), []string{input.String(), consumer.String()})
	assert.Equal(t, filter.keep[input], primary)
	assert.Equal(t, filter.keep[consumer], primary)
}

func TestFilter_SelectorProviderScope(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"named", "selector"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			for _, inventory := range []string{"stored", "indexed"} {
				t.Run(inventory, func(t *testing.T) {
					t.Parallel()
					provider := &manifest.ResourceSetInputProvider{Name: "images", Namespace: "apps", Type: fluxopv1.InputProviderStatic, Labels: map[string]string{"role": "images"}}
					ref := fluxopv1.InputProviderReference{Name: "images", Selector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "role", Operator: "invalid"}}}}
					if route == "selector" {
						ref = fluxopv1.InputProviderReference{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"role": "images"}}}
					}
					set := &manifest.ResourceSet{Name: "matrix", Namespace: "apps", InputsFrom: []fluxopv1.InputProviderReference{ref}}
					objects := filtertest.MapLister{set.Named(): set}
					if inventory == "stored" {
						objects[provider.Named()] = provider
					}
					index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						provider.Named(): {File: "provider.yaml", Declaration: provider},
						set.Named():      {File: "set.yaml", Declaration: set},
					}}
					selected := newAdmission(t, admissionConfig{changes: change.NewSet([]string{"provider.yaml"}), index: index, inventory: objects})
					selected.loader.locations = map[manifest.NamedResource]Occurrence{provider.Named(): compactOccurrence(provider, manifest.ProjectProducer(provider))}
					selected.prepare(store.Child{}, nil, false)
					assert.Equal(t, selected.ShouldReconcile(provider.Named()), true)
					assert.Equal(t, selected.ShouldReconcile(set.Named()), true)
				})
			}
		})
	}
}

func TestSelection_SelectorCandidates(t *testing.T) {
	t.Parallel()
	provider := &manifest.ResourceSetInputProvider{Name: "images", Namespace: "apps", Labels: map[string]string{"role": "images"}}
	other := *provider
	other.Labels = map[string]string{"role": "other"}
	bare := *provider
	bare.Namespace = ""
	set := &manifest.ResourceSet{Name: "matrix", Namespace: "apps", InputsFrom: []fluxopv1.InputProviderReference{{Selector: &metav1.LabelSelector{MatchLabels: provider.Labels}}}}
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
			objects := filtertest.MapLister{set.Named(): set}
			if tc.current != nil {
				objects[provider.Named()] = tc.current
			}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				set.Named():      {File: "set.yaml", Declaration: set},
				provider.Named(): {File: "provider.yaml", Declaration: tc.declaration},
			}}
			st := store.New()
			st.AddObjects(objects.Objects())
			l := New(st)
			l.Index = index
			occurrence := compactOccurrence(tc.declaration, manifest.ProjectProducer(tc.declaration))
			occurrence.Identity = provider.Named()
			l.locations = map[manifest.NamedResource]Occurrence{provider.Named(): occurrence}
			record := l.Index.Records[provider.Named()]
			record.Declaration = nil
			l.Index.Records[provider.Named()] = record
			selected := admissionClosure{loader: l, objects: st, incoming: store.Child{Obj: tc.incoming}}
			var got []manifest.NamedResource
			for ref := range manifest.References(set, manifest.ReferenceRelations) {
				if ref.Use == "inputsFrom" {
					matches, err := selected.matchingInputs(ref, true)
					assert.NoError(t, err)
					got = append(got, matches...)
				}
			}
			var want []manifest.NamedResource
			if tc.want {
				want = []manifest.NamedResource{provider.Named()}
			}
			assert.Diff(t, got, want)
			assert.Equal(t, st.GetObject(provider.Named()), tc.current)
			assert.Equal(t, objects.GetObject(provider.Named()), tc.current)
		})
	}
}
