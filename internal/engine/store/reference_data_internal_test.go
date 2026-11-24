package store

import (
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReferenceFacts_ProjectsQualifiedNamespaceData(t *testing.T) {
	t.Parallel()
	st := New()
	bare := manifest.NamedResource{Kind: manifest.KindSecret, Name: "input"}
	target := bare
	target.Namespace = "apps"
	proof := manifest.Production{
		Authored: bare,
		Source:   manifest.BootstrapSourceID,
		Path:     "input.yaml",
		File:     "input.yaml",
	}
	data := &manifest.Secret{Name: bare.Name, Namespace: bare.Namespace, StringData: map[string]any{"VALUE": "offline"}}
	unrelated := proof
	unrelated.Source.Name = "other"
	candidate := manifest.ReferenceData{Production: proof, OfflineData: data}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		bare: {Productions: []manifest.Production{proof}, ReferenceData: []manifest.ReferenceData{candidate}},
		target: {
			Productions: []manifest.Production{proof},
			ReferenceData: []manifest.ReferenceData{
				{Production: unrelated, Keys: manifest.KeySet{Names: []string{"WRONG"}, Complete: true}},
			},
		},
	}})
	for _, facts := range []ReferenceFacts{st.ReferenceFacts(target, nil, manifest.NamedResource{}), func() ReferenceFacts {
		var reads ReadSet
		reads.Init(st, ownerID("consumer"), nil)
		defer reads.Close()
		return reads.ReferenceFacts(target, nil, reads.ID())
	}()} {
		assert.FatalIf(t, len(facts.ReferenceData) != 1, "candidates=%+v", facts.ReferenceData)
		got := facts.ReferenceData[0]
		assert.Equal(t, got.Production, proof)
		projected := got.OfflineData.(*manifest.Secret)
		assert.Equal(t, projected.Named(), target)
		assert.Equal(t, data.Named(), bare)

		assert.FatalIf(
			t,
			facts.Data == nil || facts.Accepted || st.GetObject(target) != nil,
			"%s",
			"namespace projection gained authority",
		)
	}
}

func TestReferenceFacts_SourceContextWithdrawal(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"same_source", "replaced_source", "owner_deleted"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			testReferenceFactsSourceContextWithdrawalCase(
				t,
				&mode,
			)
		},
		)
	}
}

func TestReferenceFacts_CurrentDeclarationSupersedesIndexedSource(t *testing.T) {
	t.Parallel()
	st := New()
	owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	st.AddObject(owner)
	target := manifest.NamedResource{Kind: manifest.KindSecret, Name: "input", Namespace: "apps"}
	declaration := &manifest.RawObject{
		Kind:       "ExternalSecret",
		APIVersion: "external-secrets.io/v1",
		Name:       "external",
		Namespace:  "apps",
		Spec: map[string]any{
			"target": map[string]any{"name": target.Name}, "data": []any{map[string]any{"secretKey": "CURRENT"}},
		},
	}
	source := manifest.BootstrapSourceID
	st.SetArtifact(source, &SourceArtifact{Content: "indexed"})
	proof := manifest.Production{Owner: owner.Named(), Source: source, Declaration: declaration.Named()}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {
			Productions: []manifest.Production{proof},
			ReferenceData: []manifest.ReferenceData{
				{Production: proof, Keys: manifest.KeySet{Names: []string{"INDEXED"}, Complete: true}},
			},
		},
	}})
	var before ReadSet
	before.Init(st, ownerID("consumer"), nil)
	defer before.Close()
	assert.Diff(
		t,
		before.ReferenceFacts(target, nil, before.ID()).ReferenceData,
		[]manifest.ReferenceData{
			{Production: proof, Keys: manifest.KeySet{Names: []string{"INDEXED"}, Complete: true}},
		},
	)
	st.SetArtifact(source, &SourceArtifact{Content: "current"})
	assert.FatalIf(t, before.Valid(), "%s", "source replacement retained indexed evidence")
	_, _, output := st.GetEmission(owner.Named())
	defer output.Finish()
	output.Children, output.Complete = []Child{{Obj: declaration}}, true
	st.RecordEmitted(owner.Named(), &output, nil)
	var current ReadSet
	current.Init(st, ownerID("current-consumer"), nil)
	defer current.Close()
	want := []manifest.ReferenceData{
		{Production: proof, Keys: manifest.KeySet{Names: []string{"CURRENT"}, Complete: true}},
	}
	assert.Diff(t, st.ReferenceFacts(target, nil, manifest.NamedResource{}).ReferenceData, want)
	assert.Diff(t, current.ReferenceFacts(target, nil, current.ID()).ReferenceData, want)

	assert.FatalIf(t, st.GetObject(target) != nil, "%s", "current keys entered canonical inventory")
}

func testReferenceFactsSourceContextWithdrawalCase(
	t *testing.T,
	testCaseInput *string,
) {
	t.Helper()

	st := New()
	owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	st.AddObject(owner)
	target := manifest.NamedResource{Kind: manifest.KindSecret, Name: "input", Namespace: "apps"}
	source := manifest.BootstrapSourceID
	artifact := &SourceArtifact{Content: "old"}
	st.SetArtifact(source, artifact)
	proof := manifest.Production{Owner: owner.Named(), Source: source, File: "generator.yaml"}
	data := &manifest.Secret{
		Name:       target.Name,
		Namespace:  target.Namespace,
		StringData: map[string]any{"VALUE": "offline"},
	}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {
			Productions: []manifest.Production{proof},
			ReferenceData: []manifest.ReferenceData{
				{Production: proof, OfflineData: data, Keys: manifest.KeySet{Names: []string{"VALUE"}, Complete: true}},
			},
		},
	}})
	supplied := &manifest.Secret{
		Name:       target.Name,
		Namespace:  target.Namespace,
		StringData: map[string]any{"VALUE": "supplied"},
	}
	st.AddDeclaration(Declaration{Obj: supplied, Role: SuppliedDeclaration})
	var captured ReadSet
	captured.Init(st, ownerID("consumer"), nil)
	defer captured.Retire()
	assert.Equal(t, captured.ReferenceFacts(target, nil, captured.ID()).Data.HasKeys(), true)

	assert.FatalIf(
		t,
		len(st.ReferenceFacts(target, nil, manifest.NamedResource{}).ReferenceData) != 1,
		"%s",
		"candidate unavailable before withdrawal",
	)

	if (*testCaseInput) == "owner_deleted" {
		st.DeleteObject(owner.Named())
	} else {
		if (*testCaseInput) == "replaced_source" {
			st.SetArtifact(source, &SourceArtifact{Content: "new"})

			assert.FatalIf(
				t,
				len(st.ReferenceFacts(target, nil, manifest.NamedResource{}).ReferenceData) != 0,
				"%s",
				"source replacement retained stale candidate",
			)
		}
		_, _, output := st.GetEmission(owner.Named())
		defer output.Finish()
		output.Complete = true
		st.RecordEmitted(owner.Named(), &output, nil)
	}
	facts := st.ReferenceFacts(target, nil, manifest.NamedResource{})
	assert.Equal(t, captured.Valid(), false)

	assert.FatalIf(
		t,
		len(facts.ReferenceData) != 0 || facts.Data != supplied || st.GetObject(target) != supplied ||
			len(st.ListObjects("", manifest.KindSecret)) != 1,
		"withdrawal retained lookup data: %+v",
		facts,
	)

	if (*testCaseInput) == "replaced_source" {
		declaration := &manifest.RawObject{
			Kind:       "ExternalSecret",
			APIVersion: "external-secrets.io/v1",
			Name:       "current",
			Namespace:  "apps",
			Spec: map[string]any{
				"target": map[string]any{"name": target.Name},
				"data":   []any{map[string]any{"secretKey": "CURRENT"}},
			},
		}
		_, _, current := st.GetEmission(owner.Named())
		defer current.Finish()
		current.Complete, current.Children = true, []Child{{Obj: declaration}}
		st.RecordEmitted(owner.Named(), &current, nil)
		facts = st.ReferenceFacts(target, nil, manifest.NamedResource{})

		assert.FatalIf(t, len(facts.ReferenceData) != 1, "current source lost declaration keys: %+v", facts)

		assert.Diff(t, facts.ReferenceData[0].Keys.Names, []string{"CURRENT"})
		assert.FatalIf(t, facts.ReferenceData[0].OfflineData != nil, "%s", "new source revived indexed data")
	}
}

func TestReferenceFacts_DeclarationAndCompleteEmptyKeys(t *testing.T) {
	t.Parallel()
	for _, indexed := range []bool{false, true} {
		name := "published"
		if indexed {
			name = "index_only"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := New()
			data := &manifest.ConfigMap{Name: "empty", Namespace: "apps"}
			if indexed {
				st.SetDiscoveryIndex(
					&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
						data.Named(): {Path: "empty.yaml", Declaration: data},
					}},
				)
			} else {
				st.AddObject(data)
			}
			facts := st.ReferenceFacts(data.Named(), nil, manifest.NamedResource{})
			assert.Equal(t, facts.Declared, true)
			assert.Diff(t, facts.Keys, manifest.KeySet{Complete: true})
			if indexed {
				assert.Equal(t, facts.Data == nil, true)
			}
		})
	}
}

func TestReferenceFacts_UnknownProducedCoverage(t *testing.T) {
	t.Parallel()
	st := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	st.AddObject(owner)
	target := manifest.NamedResource{Kind: manifest.KindSecret, Name: "unknown", Namespace: "apps"}
	declaration := &manifest.RawObject{
		APIVersion: "external-secrets.io/v1",
		Kind:       "ExternalSecret",
		Name:       "unknown",
		Namespace:  "apps",
		Spec: map[string]any{
			"dataFrom": []any{map[string]any{"extract": map[string]any{"key": "remote"}}},
		},
	}
	_, _, output := st.GetEmission(owner.Named())
	defer output.Finish()
	output.Children, output.Complete = []Child{{Obj: declaration}}, true
	st.RecordEmitted(owner.Named(), &output, nil)
	facts := st.ReferenceFacts(target, nil, manifest.NamedResource{})
	assert.Equal(t, facts.Generated, true)
	assert.Equal(t, facts.Declared, false)
	assert.Equal(t, facts.Data == nil, true)
	assert.Diff(t, facts.Keys, manifest.KeySet{})
	assert.Equal(t, len(facts.ReferenceData), 1)
	assert.Diff(t, facts.ReferenceData[0].Keys, manifest.KeySet{})
}

func TestReferenceFacts_ProducedSupplyKeepsUnknownCoverage(t *testing.T) {
	t.Parallel()
	st := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	st.AddObject(owner)
	target := manifest.NamedResource{Kind: manifest.KindSecret, Name: "input", Namespace: "apps"}
	producer := manifest.NamedResource{
		Group:     "external-secrets.io",
		Kind:      "ExternalSecret",
		Name:      "input",
		Namespace: "apps",
	}
	proof := manifest.Production{Owner: owner.Named(), Declaration: producer}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {
			Productions:   []manifest.Production{proof},
			ReferenceData: []manifest.ReferenceData{{Production: proof}},
		},
	}})
	st.AddDeclaration(
		Declaration{
			Obj: &manifest.Secret{
				Name:       target.Name,
				Namespace:  target.Namespace,
				StringData: map[string]any{"FIRST": "supplied"},
			},
			Role: SuppliedDeclaration,
		},
	)
	var reads ReadSet
	reads.Init(st, ownerID("consumer"), nil)
	defer reads.Close()
	facts := reads.ReferenceFacts(target, nil, reads.ID())
	assert.Equal(t, facts.Declared, true)
	assert.Equal(t, facts.Original == nil, true)
	assert.Diff(t, facts.Keys, manifest.KeySet{Names: []string{"FIRST"}})
	st.AddDeclaration(
		Declaration{
			Obj: &manifest.Secret{
				Name:       target.Name,
				Namespace:  target.Namespace,
				StringData: map[string]any{"SECOND": "replacement"},
			},
			Role: SuppliedDeclaration,
		},
	)
	assert.Equal(t, reads.Valid(), false)
	facts = st.ReferenceFacts(target, nil, manifest.NamedResource{})
	assert.Diff(t, facts.Keys, manifest.KeySet{Names: []string{"SECOND"}})
}

func TestReferenceFacts_SelectsOneProducerKeySet(t *testing.T) {
	t.Parallel()
	st := New()
	first, second := ownerID("a"), ownerID("b")
	st.AddObject(&manifest.Kustomization{Name: first.Name, Namespace: first.Namespace})
	st.AddObject(&manifest.Kustomization{Name: second.Name, Namespace: second.Namespace})
	target := manifest.NamedResource{Kind: manifest.KindSecret, Name: "input", Namespace: "apps"}
	a := manifest.Production{
		Owner: first,
		Declaration: manifest.NamedResource{
			Group:     "external-secrets.io",
			Kind:      "ExternalSecret",
			Name:      "a",
			Namespace: "apps",
		},
	}
	b := a
	b.Owner, b.Declaration.Name = second, "b"
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {Productions: []manifest.Production{b, a}, ReferenceData: []manifest.ReferenceData{
			{Production: b, Keys: manifest.KeySet{Names: []string{"SECOND"}, Complete: true}},
			{Production: a, Keys: manifest.KeySet{Names: []string{"FIRST"}}},
		}},
	}})
	facts := st.ReferenceFacts(target, nil, manifest.NamedResource{})
	assert.Diff(t, facts.Keys, manifest.KeySet{Names: []string{"FIRST"}})
	assert.Equal(t, facts.Data == nil, true)
}
