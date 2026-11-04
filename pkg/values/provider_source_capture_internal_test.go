package values

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

type countedDataReader struct {
	*store.ReadSet

	objects, productions int
}

func (r *countedDataReader) ReferenceFacts(
	id manifest.NamedResource,
	index *manifest.DiscoveryIndex,
) store.ReferenceFacts {
	r.objects++
	r.productions++
	return r.ReadSet.ReferenceFacts(id, index)
}

func TestStoreProvider_LookupRetainsOneSource(t *testing.T) {
	t.Parallel()
	s := store.New()
	supplied := &manifest.Secret{Name: "supplied", Namespace: "apps", StringData: map[string]any{"VALUE": "supplied"}}
	snapshot, err := store.NewSupplied([]store.SuppliedObject{{Object: supplied}})
	assert.NoError(t, err)
	s.SetSupplied(snapshot)
	first := &manifest.ConfigMap{Name: "settings", Namespace: "apps", Data: map[string]any{"VALUE": "first"}}
	s.AddObject(first)
	owner := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "producer",
	}
	generatedID := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: "apps", Name: "generated"}
	materializer := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "materializer",
	}
	declaration := manifest.NamedResource{
		Group:     "external-secrets.io",
		Kind:      "ExternalSecret",
		Namespace: "apps",
		Name:      "declared",
	}
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		first.Named(): {
			Productions: []manifest.Production{{Owner: owner, Path: "settings.yaml", Authored: first.Named()}},
		},
		generatedID: {Productions: []manifest.Production{{Owner: materializer, Declaration: declaration}}},
	}})
	s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	s.RecordEmitted(owner, &store.Emission{Children: []store.Child{{Obj: first}}, Complete: true}, nil)
	var reads store.ReadSet
	reader := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Namespace: "apps",
		Name:      "reader",
	}
	reads.Init(s, reader, nil)
	defer reads.Close()
	counted := &countedDataReader{ReadSet: &reads}
	provider := NewStoreProvider(counted, true)
	input := provider.Lookup(first.Named())
	{
		assert.FatalIf(
			t,
			input.Data != first || !input.Offline || len(input.Materializers) != 0,
			"captured input=%+v",
			input,
		)
	}
	absent := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: first.Namespace, Name: first.Name}
	other := &manifest.ConfigMap{Name: first.Name, Namespace: "other", Data: map[string]any{"VALUE": "other"}}
	s.AddObject(other)
	missing := provider.Lookup(absent)
	{
		assert.FatalIf(t, missing.Data != nil || missing.Generated || missing.Offline || missing.Private ||
			len(missing.Materializers) != 0, "missing input=%+v", missing)
	}
	generated := provider.Lookup(generatedID)
	{
		assert.FatalIf(t, generated.Data != nil || !generated.Generated || generated.Offline || generated.Private ||
			len(generated.Materializers) != 1 ||
			generated.Materializers[0] != materializer, "generated input=%+v", generated)
	}
	otherInput := provider.Lookup(other.Named())
	private := provider.Lookup(supplied.Named())
	{
		assert.FatalIf(
			t,
			otherInput.Data != other || private.Data == nil || private.Private,
			"distinct inputs=%+v %+v",
			otherInput,
			private,
		)
	}
	s.AddObject(
		&manifest.Secret{
			Name:       absent.Name,
			Namespace:  absent.Namespace,
			StringData: map[string]any{"VALUE": "inserted"},
		},
	)
	for _, captured := range []struct {
		id    manifest.NamedResource
		input Input
	}{{first.Named(), input}, {absent, missing}, {other.Named(), otherInput}, {supplied.Named(), private}, {generatedID, generated}} {
		assert.Diff(t, captured.input, provider.Lookup(captured.id))
		{
			assert.FatalIf(
				t,
				provider.Data(captured.id) != captured.input.Data,
				"%s",
				"cached data differs from the captured input",
			)
		}
	}
	next := *first
	next.Data = map[string]any{"VALUE": "next"}
	s.RecordEmitted(owner, &store.Emission{Children: []store.Child{{Obj: &next}}, Complete: true}, nil)
	if provider.Data(first.Named()) != first || provider.Lookup(first.Named()).Data != first || counted.objects != 5 ||
		counted.productions != 5 {
		t.Fatalf("expansion rescanned captured input: object=%d productions=%d", counted.objects, counted.productions)
	}
	{
		assert.FatalIf(t, reads.Valid(), "%s", "changed consumed data did not invalidate execution")
	}
	var fresh store.ReadSet
	fresh.Init(s, reader, nil)
	defer fresh.Close()
	{
		got := NewStoreProvider(&fresh, true).Data(first.Named())
		assert.FatalIf(t, got != &next, "next execution retained preceding value: %v", got)
	}
	{
		got := NewStoreProvider(&fresh, true).Data(absent)
		assert.FatalIf(t, got == nil, "%s", "fresh execution retained frozen absence")
	}
}
