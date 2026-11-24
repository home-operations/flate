package values

import (
	"errors"
	"testing"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReferenceFacts_CapturedProviderPolicy(t *testing.T) {
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
	facts := st.ReferenceFacts(id, nil, manifest.NamedResource{})
	assert.FatalIf(t, facts.Data != raw || facts.Accepted,
		"covered facts=%+v", facts)
	assert.FatalIf(
		t,
		NewStoreProvider(st, manifest.NamedResource{}).Lookup(id).Data != raw,
		"%s",
		"captured provider rejected visible prediction",
	)

	var previous store.ReadSet
	previous.Init(st, owner, nil)
	defer previous.Retire()
	assert.FatalIf(
		t,
		NewStoreProvider(&previous, previous.ID()).Lookup(id).Data != raw,
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
		facts := reader.ReferenceFacts(id, nil, consumer)
		wantFacts := store.ReferenceFacts{
			Generated:   true,
			Keys:        manifest.KeySet{Names: []string{"VALUE"}, Complete: true},
			Productions: want,
			ReferenceData: []manifest.ReferenceData{
				{Production: want[0], Keys: manifest.KeySet{Names: []string{"VALUE"}, Complete: true}},
			},
		}
		if reader == &current {
			wantFacts.Private = true
		} else {
			wantFacts.Materializers = []manifest.NamedResource{owner}
		}
		assert.Diff(t, facts, wantFacts)
		assert.Diff(t, NewStoreProvider(reader, consumer).Lookup(id), wantFacts)
		assert.FatalIf(
			t,
			NewStoreProvider(reader, consumer).Lookup(id).Data != nil,
			"%s",
			"provider fabricated declared data",
		)
	}
}

func TestStoreProvider_BareMetadataDoesNotAdmitNamespace(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			bare := manifest.NamedResource{Name: "settings"}
			data := map[string]any{"VALUE": "ENC[AES256_GCM,data:x,iv:y,tag:z,type:str]"}
			var raw manifest.BaseManifest = &manifest.ConfigMap{Name: bare.Name, Namespace: bare.Namespace, Data: data}
			if kind == manifest.KindSecret {
				raw = &manifest.Secret{Name: bare.Name, Namespace: bare.Namespace, Data: nil, StringData: data}
			}
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw.Named(): {Path: "settings.yaml", Declaration: raw},
			}})
			consumer := &manifest.Kustomization{Name: "consumer", Namespace: "apps", PostBuild: &kustomizev1.PostBuild{
				SubstituteFrom: []kustomizev1.SubstituteReference{{Kind: kind, Name: bare.Name}},
			}}
			var reads store.ReadSet
			reads.Init(st, consumer.Named(), nil)
			defer reads.Close()
			id := raw.Named()
			id.Namespace = consumer.Namespace
			facts := NewStoreProvider(&reads, reads.ID()).Lookup(id)
			assert.Equal(t, facts.Declared, false)
			assert.Equal(t, facts.Data == nil, true)
			assert.Diff(t, facts.Keys, manifest.KeySet{Names: []string{"VALUE"}, Complete: true})
			ref := manifest.Reference{NamedResource: id, Use: "substituteFrom"}
			result := Resolve(&ResolveRequest{Reference: ref, Facts: facts})
			assert.Equal(t, result.Effect(), Failed)
			assert.FatalIf(
				t,
				!errors.Is(result.Err(), manifest.ErrObjectNotFound) || !errors.Is(result.Err(), manifest.ErrFlux),
				"qualified absence lost: %v",
				result.Err(),
			)
			assert.Equal(t, ExpandPostBuildSubstituteReference(consumer, []Result{result}), result.Err())
			assert.Equal(t, consumer.PostBuild.Substitute == nil, true)
			for _, demand := range []Demand{ObjectDemand, HelmDemand} {
				selected := Resolve(&ResolveRequest{Reference: ref, Facts: facts, Demand: demand})
				assert.Equal(t, selected.Effect(), Failed)
				assert.Equal(t, errors.Is(selected.Err(), manifest.ErrObjectNotFound), true)
			}
		})
	}
}
