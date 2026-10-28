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
	proof := manifest.Production{Authored: bare, Source: manifest.BootstrapSourceID, Path: "input.yaml", File: "input.yaml"}
	data := manifest.NewSecret(bare, nil, map[string]any{"VALUE": "offline"})
	unrelated := proof
	unrelated.Source.Name = "other"
	candidate := manifest.ReferenceData{Production: proof, OfflineData: data}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		bare:   {Productions: []manifest.Production{proof}, ReferenceData: []manifest.ReferenceData{candidate}},
		target: {Productions: []manifest.Production{proof}, ReferenceData: []manifest.ReferenceData{{Production: unrelated, DeclaredKeys: []string{"WRONG"}}}},
	}})
	for _, facts := range []ReferenceFacts{st.ReferenceFacts(target, nil), func() ReferenceFacts {
		var reads ReadSet
		reads.Init(st, ownerID("consumer"), nil)
		defer reads.Close()
		return reads.ReferenceFacts(target, nil)
	}()} {
		if len(facts.ReferenceData) != 1 {
			t.Fatalf("candidates=%+v", facts.ReferenceData)
		}
		got := facts.ReferenceData[0]
		assert.Equal(t, got.Production, proof)
		projected := got.OfflineData.(*manifest.Secret)
		assert.Equal(t, projected.Named(), target)
		assert.Equal(t, data.Named(), bare)
		if facts.Data != nil || facts.Accepted || st.GetObject(target) != nil {
			t.Fatal("namespace projection gained authority")
		}
	}
}

func TestReferenceFacts_SourceContextWithdrawal(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"same_source", "replaced_source", "owner_deleted"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
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
				target: {Productions: []manifest.Production{proof}, ReferenceData: []manifest.ReferenceData{{Production: proof, OfflineData: data, DeclaredKeys: []string{"VALUE"}}}},
			}})
			if len(st.ReferenceFacts(target, nil).ReferenceData) != 1 {
				t.Fatal("candidate unavailable before withdrawal")
			}
			if mode == "owner_deleted" {
				st.DeleteObject(owner.Named())
			} else {
				if mode == "replaced_source" {
					st.SetArtifact(source, &SourceArtifact{Content: "new"})
					if len(st.ReferenceFacts(target, nil).ReferenceData) != 0 {
						t.Fatal("source replacement retained stale candidate")
					}
				}
				_, _, output := st.GetEmission(owner.Named())
				defer output.Finish()
				output.Complete = true
				st.RecordEmitted(owner.Named(), &output, nil)
			}
			facts := st.ReferenceFacts(target, nil)
			if len(facts.ReferenceData) != 0 || facts.Data != nil || st.GetObject(target) != nil {
				t.Fatalf("withdrawal retained lookup data: %+v", facts)
			}
			if mode == "replaced_source" {
				declaration := &manifest.RawObject{Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Name: "current", Namespace: "apps", Spec: map[string]any{"target": map[string]any{"name": target.Name}, "data": []any{map[string]any{"secretKey": "CURRENT"}}}}
				_, _, current := st.GetEmission(owner.Named())
				defer current.Finish()
				current.Complete, current.Children = true, []Child{{Obj: declaration}}
				st.RecordEmitted(owner.Named(), &current, nil)
				facts = st.ReferenceFacts(target, nil)
				if len(facts.ReferenceData) != 1 {
					t.Fatalf("current source lost declaration keys: %+v", facts)
				}
				assert.Diff(t, facts.ReferenceData[0].DeclaredKeys, []string{"CURRENT"})
				if facts.ReferenceData[0].OfflineData != nil {
					t.Fatal("new source revived indexed data")
				}
			}
		})
	}
}

func TestReferenceFacts_CurrentDeclarationSupersedesIndexedSource(t *testing.T) {
	t.Parallel()
	st := New()
	owner := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	st.AddObject(owner)
	target := manifest.NamedResource{Kind: manifest.KindSecret, Name: "input", Namespace: "apps"}
	declaration := &manifest.RawObject{Kind: "ExternalSecret", APIVersion: "external-secrets.io/v1", Name: "external", Namespace: "apps", Spec: map[string]any{
		"target": map[string]any{"name": target.Name}, "data": []any{map[string]any{"secretKey": "CURRENT"}},
	}}
	source := manifest.BootstrapSourceID
	st.SetArtifact(source, &SourceArtifact{Content: "indexed"})
	proof := manifest.Production{Owner: owner.Named(), Source: source, Declaration: declaration.Named()}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		target: {Productions: []manifest.Production{proof}, ReferenceData: []manifest.ReferenceData{{Production: proof, DeclaredKeys: []string{"INDEXED"}}}},
	}})
	var before ReadSet
	before.Init(st, ownerID("consumer"), nil)
	defer before.Close()
	assert.Diff(t, before.ReferenceFacts(target, nil).ReferenceData, []manifest.ReferenceData{{Production: proof, DeclaredKeys: []string{"INDEXED"}}})
	st.SetArtifact(source, &SourceArtifact{Content: "current"})
	if before.Valid() {
		t.Fatal("source replacement retained indexed evidence")
	}
	_, _, output := st.GetEmission(owner.Named())
	defer output.Finish()
	output.Children, output.Complete = []Child{{Obj: declaration}}, true
	st.RecordEmitted(owner.Named(), &output, nil)
	var current ReadSet
	current.Init(st, ownerID("current-consumer"), nil)
	defer current.Close()
	want := []manifest.ReferenceData{{Production: proof, DeclaredKeys: []string{"CURRENT"}}}
	assert.Diff(t, st.ReferenceFacts(target, nil).ReferenceData, want)
	assert.Diff(t, current.ReferenceFacts(target, nil).ReferenceData, want)
	if st.GetObject(target) != nil {
		t.Fatal("current keys entered canonical inventory")
	}
}
