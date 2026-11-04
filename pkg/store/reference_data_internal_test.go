package store

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
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
	data := manifest.NewSecret(bare, nil, map[string]any{"VALUE": "offline"})
	unrelated := proof
	unrelated.Source.Name = "other"
	candidate := manifest.ReferenceData{Production: proof, OfflineData: data}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		bare: {Productions: []manifest.Production{proof}, ReferenceData: []manifest.ReferenceData{candidate}},
		target: {
			Productions:   []manifest.Production{proof},
			ReferenceData: []manifest.ReferenceData{{Production: unrelated, DeclaredKeys: []string{"WRONG"}}},
		},
	}})
	for _, facts := range []ReferenceFacts{st.ReferenceFacts(target, nil, false, manifest.NamedResource{}), func() ReferenceFacts {
		var reads ReadSet
		reads.Init(st, ownerID("consumer"), nil)
		defer reads.Close()
		return reads.ReferenceFacts(target, nil, false, reads.ID())
	}()} {
		{
			assert.FatalIf(t, len(facts.ReferenceData) != 1, "candidates=%+v", facts.ReferenceData)
		}
		got := facts.ReferenceData[0]
		assert.Equal(t, got.Production, proof)
		projected := got.OfflineData.(*manifest.Secret)
		assert.Equal(t, projected.Named(), target)
		assert.Equal(t, data.Named(), bare)
		{
			assert.FatalIf(
				t,
				facts.Data == nil || facts.Accepted || st.GetObject(target) != nil,
				"%s",
				"namespace projection gained authority",
			)
		}
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
			Productions:   []manifest.Production{proof},
			ReferenceData: []manifest.ReferenceData{{Production: proof, DeclaredKeys: []string{"INDEXED"}}},
		},
	}})
	var before ReadSet
	before.Init(st, ownerID("consumer"), nil)
	defer before.Close()
	assert.Diff(
		t,
		before.ReferenceFacts(target, nil, false, before.ID()).ReferenceData,
		[]manifest.ReferenceData{{Production: proof, DeclaredKeys: []string{"INDEXED"}}},
	)
	st.SetArtifact(source, &SourceArtifact{Content: "current"})
	{
		assert.FatalIf(t, before.Valid(), "%s", "source replacement retained indexed evidence")
	}
	_, _, output := st.GetEmission(owner.Named())
	defer output.Finish()
	output.Children, output.Complete = []Child{{Obj: declaration}}, true
	st.RecordEmitted(owner.Named(), &output, nil)
	var current ReadSet
	current.Init(st, ownerID("current-consumer"), nil)
	defer current.Close()
	want := []manifest.ReferenceData{{Production: proof, DeclaredKeys: []string{"CURRENT"}}}
	assert.Diff(t, st.ReferenceFacts(target, nil, false, manifest.NamedResource{}).ReferenceData, want)
	assert.Diff(t, current.ReferenceFacts(target, nil, false, current.ID()).ReferenceData, want)
	{
		assert.FatalIf(t, st.GetObject(target) != nil, "%s", "current keys entered canonical inventory")
	}
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
	data := manifest.NewSecret(target, nil, map[string]any{"VALUE": "offline"})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {
			Productions: []manifest.Production{proof},
			ReferenceData: []manifest.ReferenceData{
				{Production: proof, OfflineData: data, DeclaredKeys: []string{"VALUE"}},
			},
		},
	}})
	supplied := manifest.NewSecret(target, nil, map[string]any{"VALUE": "supplied"})
	st.AddDeclaration(Declaration{Obj: supplied, Origin: manifest.SuppliedDeclaration})
	var captured ReadSet
	captured.Init(st, ownerID("consumer"), nil)
	defer captured.Retire()
	assert.Equal(t, captured.ReferenceFacts(target, nil, false, captured.ID()).Data.HasKeys(), true)
	{
		assert.FatalIf(
			t,
			len(st.ReferenceFacts(target, nil, false, manifest.NamedResource{}).ReferenceData) != 1,
			"%s",
			"candidate unavailable before withdrawal",
		)
	}
	if (*testCaseInput) == "owner_deleted" {
		st.DeleteObject(owner.Named())
	} else {
		if (*testCaseInput) == "replaced_source" {
			st.SetArtifact(source, &SourceArtifact{Content: "new"})
			{
				assert.FatalIf(
					t,
					len(st.ReferenceFacts(target, nil, false, manifest.NamedResource{}).ReferenceData) != 0,
					"%s",
					"source replacement retained stale candidate",
				)
			}
		}
		_, _, output := st.GetEmission(owner.Named())
		defer output.Finish()
		output.Complete = true
		st.RecordEmitted(owner.Named(), &output, nil)
	}
	facts := st.ReferenceFacts(target, nil, false, manifest.NamedResource{})
	assert.Equal(t, captured.Valid(), false)
	{
		assert.FatalIf(
			t,
			len(facts.ReferenceData) != 0 || facts.Data != supplied || st.GetObject(target) != supplied ||
				len(st.ListObjects("", manifest.KindSecret)) != 1,
			"withdrawal retained lookup data: %+v",
			facts,
		)
	}
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
		facts = st.ReferenceFacts(target, nil, false, manifest.NamedResource{})
		{
			assert.FatalIf(t, len(facts.ReferenceData) != 1, "current source lost declaration keys: %+v", facts)
		}
		assert.Diff(t, facts.ReferenceData[0].DeclaredKeys, []string{"CURRENT"})
		{
			assert.FatalIf(t, facts.ReferenceData[0].OfflineData != nil, "%s", "new source revived indexed data")
		}
	}
}
