package values

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
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
	consumer manifest.NamedResource,
) store.ReferenceFacts {
	r.objects++
	r.productions++
	return r.ReadSet.ReferenceFacts(id, index, consumer)
}

func TestStoreProvider_LookupRetainsOneSource(t *testing.T) {
	t.Parallel()
	s := store.New()
	supplied := &manifest.Secret{Name: "supplied", Namespace: "apps", StringData: map[string]any{"VALUE": "supplied"}}
	s.AddDeclaration(
		store.Declaration{Obj: supplied, Doc: testutil.ObjectDocument(supplied), Origin: manifest.SuppliedDeclaration},
	)
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
	provider := NewStoreProvider(counted)
	input := provider.Lookup(first.Named())
	assert.FatalIf(
		t,
		input.Data != first || !input.Offline || len(input.Materializers) != 1 || input.Materializers[0] != owner,
		"captured input=%+v",
		input,
	)
	absent := manifest.NamedResource{Kind: manifest.KindSecret, Namespace: first.Namespace, Name: first.Name}
	other := &manifest.ConfigMap{Name: first.Name, Namespace: "other", Data: map[string]any{"VALUE": "other"}}
	s.AddObject(other)
	missing := provider.Lookup(absent)
	assert.FatalIf(t, missing.Data != nil || missing.Generated || missing.Offline || missing.Private ||
		len(missing.Materializers) != 0, "missing input=%+v", missing)
	generated := provider.Lookup(generatedID)
	assert.FatalIf(t, generated.Data != nil || !generated.Generated || generated.Offline || generated.Private ||
		len(generated.Materializers) != 1 ||
		generated.Materializers[0] != materializer, "generated input=%+v", generated)
	otherInput := provider.Lookup(other.Named())
	private := provider.Lookup(supplied.Named())
	assert.FatalIf(
		t,
		otherInput.Data != other || private.Data == nil || private.Private,
		"distinct inputs=%+v %+v",
		otherInput,
		private,
	)
	s.AddObject(
		&manifest.Secret{
			Name:       absent.Name,
			Namespace:  absent.Namespace,
			StringData: map[string]any{"VALUE": "inserted"},
		},
	)
	for _, captured := range []struct {
		id    manifest.NamedResource
		input store.ReferenceFacts
	}{{first.Named(), input}, {absent, missing}, {other.Named(), otherInput}, {supplied.Named(), private}, {generatedID, generated}} {
		assert.Diff(t, captured.input, provider.Lookup(captured.id))
		assert.FatalIf(
			t,
			provider.Lookup(captured.id).Data != captured.input.Data,
			"%s",
			"cached data differs from the captured input",
		)
	}
	next := *first
	next.Data = map[string]any{"VALUE": "next"}
	s.RecordEmitted(owner, &store.Emission{Children: []store.Child{{Obj: &next}}, Complete: true}, nil)
	assert.FatalIf(
		t,
		provider.Lookup(first.Named()).Data != first || counted.objects != 5 ||
			counted.productions != 5,
		"expansion rescanned captured input: object=%d productions=%d",
		counted.objects,
		counted.productions,
	)

	assert.FatalIf(t, reads.Valid(), "%s", "changed consumed data did not invalidate execution")
	var fresh store.ReadSet
	fresh.Init(s, reader, nil)
	defer fresh.Close()
	{
		got := NewStoreProvider(&fresh).Lookup(first.Named()).Data
		assert.FatalIf(t, got != &next, "next execution retained preceding value: %v", got)
	}
	{
		got := NewStoreProvider(&fresh).Lookup(absent).Data
		assert.FatalIf(t, got == nil, "%s", "fresh execution retained frozen absence")
	}
}
