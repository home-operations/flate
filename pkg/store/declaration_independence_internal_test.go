package store

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestGetDeclaration_IndependenceLifecycle(t *testing.T) {
	t.Parallel()
	for _, support := range []string{"uncovered", "independent", "covered"} {
		for _, route := range []string{"pre_index", "post_index", "document", "batch", "included", "preflight", "readset", "readmit", "anchor", "batch_anchor"} {
			t.Run(support+"/"+route, func(t *testing.T) {
				t.Parallel()
				testGetDeclarationIndependenceLifecycleCase(
					t,
					&testGetDeclarationIndependenceLifecycleCaseInputs{support: &support, route: &route},
				)
			},
			)
		}
	}
}

func TestRecordEmitted_IndependencePublication(t *testing.T) {
	t.Parallel()
	for _, independent := range []bool{false, true} {
		for _, route := range []string{"first", "stable", "replacement", "hidden_producer", "reachability", "target", "expanded_coverage", "expanded_inventory", "metadata"} {
			t.Run(fmt.Sprintf("independent_%t/%s", independent, route), func(t *testing.T) {
				t.Parallel()
				testRecordEmittedIndependencePublicationCase(
					t,
					&testRecordEmittedIndependencePublicationCaseInputs{independent: &independent, route: &route},
				)
			},
			)
		}
	}
}

type testGetDeclarationIndependenceLifecycleCaseInputs struct {
	support *string
	route   *string
}

func testGetDeclarationIndependenceLifecycleCase(
	t *testing.T,
	testCaseInput *testGetDeclarationIndependenceLifecycleCaseInputs,
) {
	t.Helper()

	st := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	raw := &manifest.OCIRepository{Name: "input", Namespace: "apps", URL: "oci://raw.example"}
	st.AddObject(owner)
	proofs := []manifest.Production{{Owner: owner.Named()}}
	independent := (*testCaseInput.support) == "independent"
	if independent {
		proofs = append(proofs, manifest.Production{})
	}
	if (*testCaseInput.support) == "uncovered" {
		proofs, independent = nil, true
	}
	index := &manifest.DiscoveryIndex{
		Records: map[manifest.NamedResource]manifest.DiscoveryRecord{raw.Named(): {Productions: proofs}},
	}
	if (*testCaseInput.route) == "anchor" || (*testCaseInput.route) == "batch_anchor" {
		index.Records[ownerID("descendant")] = manifest.DiscoveryRecord{
			Productions: []manifest.Production{{Owner: raw.Named()}},
		}
	}
	if (*testCaseInput.route) == "pre_index" {
		st.AddObject(raw)
		assert.FatalIf(
			t,
			st.GetDeclaration(raw.Named()) != raw,
			"%s",
			"pre-index declaration lost independent support",
		)
	}
	st.SetDiscoveryIndex(index)
	switch *testCaseInput.route {
	case "post_index":
		st.AddObject(raw)
	case "document":
		st.AddObject(raw, map[string]any{"apiVersion": manifest.SourceDomain + "/v1"})
	case "batch":
		st.AddObjects([]manifest.BaseManifest{raw})
	case "included", "preflight":
		if (*testCaseInput.route) == "included" {
			st.SetIncluded(raw.Named(), false)
		} else {
			st.SetPreflight(raw.Named(), "preflight")
		}
		st.AddObject(raw)
	case "readset":
		st.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{Key: "source"})
		var reads ReadSet
		reads.Init(st, owner.Named(), nil)
		defer reads.Retire()
		reads.Admit(Child{Obj: raw}, manifest.BootstrapSourceID, manifest.NamedResource{})
	case "readmit":
		st.AddObject(raw)
		assert.FatalIf(t, !st.DeleteObject(raw.Named()), "%s", "fixture did not delete its first admission")
		st.AddObject(raw)
	case "anchor", "batch_anchor":
		if (*testCaseInput.route) == "anchor" {
			st.AddObject(raw)
		} else {
			st.AddObjects([]manifest.BaseManifest{raw})
		}
	}
	testGetDeclarationIndependenceLifecycleCaseCompletion(
		t,
		&testGetDeclarationIndependenceLifecycleCaseCompletionState{
			testCaseInput: &testCaseInput,
			st:            &st,
			raw:           &raw,
			independent:   &independent,
		},
	)
}

type testRecordEmittedIndependencePublicationCaseInputs struct {
	independent *bool
	route       *string
}

func testRecordEmittedIndependencePublicationCase(
	t *testing.T,
	testCaseInput *testRecordEmittedIndependencePublicationCaseInputs,
) {
	t.Helper()

	st := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	child := emissionChild("output", "first")
	proofs := []manifest.Production{{Owner: owner.Named()}}
	if (*testCaseInput.route) == "expanded_coverage" {
		proofs[0].Owner = ownerID("nested")
	}
	if *testCaseInput.independent {
		proofs = append(proofs, manifest.Production{})
	}
	id := child.Obj.Named()
	if (*testCaseInput.route) == "hidden_producer" {
		id = owner.Named()
		proofs[0].Owner = ownerID("covering")
	} else {
		st.AddObject(owner)
	}
	records := map[manifest.NamedResource]manifest.DiscoveryRecord{id: {Productions: proofs}}
	if (*testCaseInput.route) == "reachability" || (*testCaseInput.route) == "expanded_coverage" {
		records[ownerID("descendant")] = manifest.DiscoveryRecord{
			Productions: []manifest.Production{{Owner: id}},
		}
	}
	if (*testCaseInput.route) == "target" {
		child = Child{
			Obj: &manifest.RawObject{
				APIVersion: "objectbucket.io/v1alpha1",
				Kind:       "ObjectBucketClaim",
				Name:       id.Name,
				Namespace:  id.Namespace,
			},
		}
	}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: records})
	sh := st.shardFor(id)
	sh.mu.RLock()
	_, present := sh.cells[id]
	sh.mu.RUnlock()
	assert.FatalIf(t, present, "%s", "index manufactured an absent object cell")
	if (*testCaseInput.route) == "metadata" {
		st.SetIncluded(id, false)
		st.SetPreflight(id, "preflight")
	}
	var absent ReadSet
	absent.Init(st, ownerID("absent"), nil)
	defer absent.Retire()
	assert.Equal(t, absent.GetDeclaration(id), nil)
	_, _, output := st.GetEmission(owner.Named())
	defer output.Finish()
	output.Complete = true
	if (*testCaseInput.route) != "reachability" {
		output.Children = []Child{child}
	}
	nested := &manifest.Kustomization{Name: "nested", Namespace: "apps"}
	if (*testCaseInput.route) == "expanded_coverage" || (*testCaseInput.route) == "expanded_inventory" {
		output.Children = []Child{{Obj: nested}}
	}
	testRecordEmittedIndependencePublicationCaseCompletion(
		t,
		&testRecordEmittedIndependencePublicationCaseCompletionState{
			testCaseInput: &testCaseInput,
			st:            &st,
			owner:         &owner,
			child:         &child,
			id:            &id,
			sh:            &sh,
			absent:        &absent,
			output:        &output,
			nested:        &nested,
		},
	)
}

type testGetDeclarationIndependenceLifecycleCaseCompletionState struct {
	testCaseInput **testGetDeclarationIndependenceLifecycleCaseInputs
	st            **Store
	raw           **manifest.OCIRepository
	independent   *bool
}
