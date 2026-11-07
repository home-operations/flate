package selection

import (
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_OwnershipReadCapture(t *testing.T) {
	t.Parallel()
	st := store.New()
	root := t.TempDir()
	input := &store.SourceArtifact{LocalPath: root, LocalRoot: root, FS: fstest.MapFS{}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	source := &manifest.GitRepository{Name: "local", Namespace: "sources"}
	st.AddObject(source)
	st.SetArtifact(source.Named(), input)
	factory := &manifest.ResourceSet{Name: "factory", Namespace: "apps"}
	st.AddObject(factory)
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "./declarations", TargetNamespace: "apps"}
	owner.SourceRef.Kind, owner.SourceRef.Name, owner.SourceRef.Namespace = source.Named().Kind, source.Name, source.Namespace
	declaration := &manifest.Kustomization{Name: "child", Namespace: "apps", Path: "./payload"}
	declaration.SourceRef = owner.SourceRef
	child := declaration.Named()
	record := manifest.DiscoveryRecord{
		File:        "declarations/child.yaml",
		Path:        filepath.Join(root, "declarations", "child.yaml"),
		Declaration: declaration,
	}
	occurrence := OccurrenceFacts{
		Identity:     child,
		Authored:     child,
		Object:       declaration,
		Catalog:      declaration,
		Record:       record,
		Declaration:  OccurrenceID{Source: root, File: record.File},
		Candidate:    store.Child{Obj: declaration},
		Mounted:      input,
		Located:      true,
		CatalogKnown: true,
		Source:       source.Named(),
		Path:         "./payload",
		HasPath:      true,
	}
	facts := &solverFacts{
		store: st,
		index: manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{child: record},
		},
		occurrences: map[manifest.NamedResource]OccurrenceFacts{child: occurrence},
		roots: map[manifest.NamedResource]RootEvidence{
			child: {
				Identity:   child,
				Record:     record,
				Occurrence: occurrence,
				Captured:   true,
				Scope: RootScopeFacts{
					Path:              record.Path,
					Eligibility:       RootEligible,
					Initial:           true,
					Bootstrap:         true,
					NamespaceEligible: true,
					Candidate:         true,
				},
			},
		},
	}
	facts.context.Root = root
	session, err := New(st, facts, Request{Roots: []manifest.NamedResource{child}})
	assert.NoError(t, err)
	unrelated := owner.Clone()
	unrelated.Path = "./elsewhere"
	_, _, outside := st.GetEmission(factory.Named())
	outside.Children, outside.Complete = []store.Child{{Obj: unrelated}}, true
	st.RecordEmitted(factory.Named(), &outside, nil)
	outside.Finish()
	var uncovered store.ReadSet
	uncovered.Init(st, factory.Named(), nil)
	defer uncovered.Retire()
	uncovered.RequireData(child)
	unowned, err := prepareSolver(t.Context(), t, session, PrepareRequest{Reads: &uncovered})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(unowned.Needed, owner.Named()), false)
	assert.Equal(
		t,
		slices.ContainsFunc(unowned.Objects, func(c store.Child) bool { return c.Obj.Named() == child }),
		true,
	)
	assert.Equal(t, unowned.reads.Valid(), true)
	_, _, next := st.GetEmission(factory.Named())
	next.Children, next.Complete = []store.Child{{Obj: owner}}, true
	st.RecordEmitted(factory.Named(), &next, nil)
	next.Finish()
	assert.Equal(t, unowned.reads.Valid(), false)
	var reads store.ReadSet
	reads.Init(st, factory.Named(), nil)
	defer reads.Retire()
	reads.RequireData(child)
	proposal, err := prepareSolver(t.Context(), t, session, PrepareRequest{Reads: &reads})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), true)
	assert.Equal(
		t,
		slices.ContainsFunc(proposal.Objects, func(c store.Child) bool { return c.Obj.Named() == child }),
		false,
	)
	assert.Equal(t, proposal.reads.Valid(), true)
	st.SetArtifact(source.Named(), &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
	if proposal.reads.Valid() {
		t.Error("ownership source replacement did not invalidate the captured proposal")
	}
	var fresh store.ReadSet
	fresh.Init(st, factory.Named(), nil)
	defer fresh.Retire()
	fresh.RequireData(child)
	proposal, err = prepareSolver(t.Context(), t, session, PrepareRequest{Reads: &fresh})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), false)
	assert.Equal(
		t,
		slices.ContainsFunc(proposal.Objects, func(c store.Child) bool { return c.Obj.Named() == child }),
		true,
	)
	assert.Equal(t, proposal.reads.Valid(), true)
}
