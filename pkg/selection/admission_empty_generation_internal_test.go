package selection

import (
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_EmptyGenerationDropsPreviousObjects(t *testing.T) {
	t.Parallel()
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	st.AddObject(parent)
	session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{parent.Named()}})
	facts := session.facts.(*solverFacts)
	facts.roots = map[manifest.NamedResource]RootEvidence{child.Named(): {
		Identity: child.Named(), Captured: true,
		Scope: RootScopeFacts{NamespaceEligible: true, Candidate: true},
	}}
	_, retry := publishDecision(t, session, &Admission{
		Needed: []manifest.NamedResource{parent.Named()}, Selected: []manifest.NamedResource{parent.Named()},
		Dirty: []manifest.NamedResource{parent.Named()}, Ordinary: []manifest.NamedResource{parent.Named()},
	}, session.Snapshot().version())
	if retry {
		t.Fatal("unexpected publication retry")
	}
	var reads store.ReadSet
	reads.Init(st, parent.Named(), nil)
	defer reads.Close()
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	request := PrepareRequest{
		Preparation: &preparation, Owner: parent.Named(), Reads: &reads,
		Generation: []store.Child{{Obj: child, SourceSupport: manifest.BootstrapSourceID}},
	}
	first, err := session.Prepare(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Objects) != 1 {
		t.Fatalf("first proposal objects = %d, want 1", len(first.Objects))
	}
	_, committed, err := session.Commit(t.Context(), first, Execution)
	if err != nil {
		t.Fatal(err)
	}
	if !committed {
		t.Fatal("first proposal did not commit")
	}
	request.Generation = nil
	empty, err := session.Prepare(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Retire()
	if len(empty.Objects) != 0 {
		t.Errorf("empty generation republishes %d old objects, want 0", len(empty.Objects))
	}
	if _, retained := preparation.closure.states[child.Named()]; retained {
		t.Error("empty generation retains the withdrawn child's closure state")
	}
}
