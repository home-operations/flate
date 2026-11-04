package values

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestReferenceFacts_DataOnlyProviderPolicy(t *testing.T) {
	t.Parallel()
	st := store.New()
	id := manifest.NamedResource{Kind: manifest.KindSecret, Name: "input", Namespace: "apps"}
	raw := &manifest.Secret{Name: id.Name, Namespace: id.Namespace, StringData: map[string]any{"VALUE": "visible"}}
	owner := manifest.NamedResource{
		Group:     manifest.FluxKustomizeDomain,
		Kind:      manifest.KindKustomization,
		Name:      "owner",
		Namespace: "apps",
	}
	st.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	proof := manifest.Production{
		Owner:    owner,
		Source:   manifest.BootstrapSourceID,
		Authored: id,
		Path:     "input.yaml",
		File:     "input.yaml",
	}
	st.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				id: {Productions: []manifest.Production{proof}},
			},
		},
	)
	st.AddObject(raw)
	facts := st.ReferenceFacts(id, nil, false, manifest.NamedResource{})

	assert.FatalIf(t, facts.Data != raw || facts.Accepted, "covered facts=%+v", facts)

	assert.FatalIf(
		t,
		NewStoreProvider(st, true).Data(id) != raw,
		"%s",
		"data-only provider rejected visible prediction",
	)

	var previous store.ReadSet
	previous.Init(st, owner, nil)
	defer previous.Retire()
	assert.FatalIf(
		t,
		NewStoreProvider(&previous, true).Data(id) != raw,
		"%s",
		"captured provider rejected visible prediction",
	)
	first, second := facts.Productions, facts.Productions
	assert.Diff(t, second, first)
	assert.Diff(t, st.Productions(id, nil), first)
	visits := 0
	for range facts.Productions {
		visits++
		break
	}
	assert.Equal(t, visits, 1)

	external := &manifest.RawObject{
		Kind:       "ExternalSecret",
		APIVersion: "external-secrets.io/v1",
		Name:       "external",
		Namespace:  id.Namespace,
		Spec: map[string]any{
			"target": map[string]any{"name": id.Name},
			"data":   []any{map[string]any{"secretKey": "VALUE"}},
		},
	}
	_, _, next := st.GetEmission(owner)
	defer next.Finish()
	next.Children, next.Complete = []store.Child{{Obj: external}}, true
	st.RecordEmitted(owner, &next, nil)
	assert.FatalIf(t, previous.Valid(), "%s", "replacement retained captured file authority")
	var current store.ReadSet
	current.Init(st, owner, nil)
	defer current.Retire()
	want := []manifest.Production{{Owner: owner, Source: manifest.BootstrapSourceID, Declaration: external.Named()}}
	for _, reader := range []dataReader{st, &current} {
		consumer := manifest.NamedResource{}
		if reader == &current {
			consumer = owner
		}
		facts := reader.ReferenceFacts(id, nil, false, consumer)
		wantFacts := store.ReferenceFacts{
			Generated:     true,
			Productions:   want,
			ReferenceData: []manifest.ReferenceData{{Production: want[0], DeclaredKeys: []string{"VALUE"}}},
		}
		if reader == &current {
			wantFacts.Private = true
		} else {
			wantFacts.Materializers = []manifest.NamedResource{owner}
		}
		assert.Diff(t, facts, wantFacts)
		assert.Diff(t, NewStoreProvider(reader, true).Data(id), &manifest.Secret{
			Name:       id.Name,
			Namespace:  id.Namespace,
			StringData: map[string]any{"VALUE": manifest.ValuePlaceholder("VALUE")},
		})
		assert.FatalIf(
			t,
			NewStoreProvider(reader, false).Data(id) != nil,
			"%s",
			"provider supplied declared placeholders with wiping disabled",
		)
	}
}
