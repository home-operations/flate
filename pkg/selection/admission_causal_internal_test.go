package selection

import (
	"slices"
	"testing"
	"testing/fstest"

	helmv2 "github.com/fluxcd/helm-controller/api/v2"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_SourceObservationKeepsPreparedComparison(t *testing.T) {
	t.Parallel()
	stores := [2]*store.Store{store.New(), store.New()}
	releases := [2]*manifest.HelmRelease{}
	for side, st := range stores {
		st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}})
		releases[side] = &manifest.HelmRelease{
			Name:       "demo",
			Namespace:  "apps",
			Values:     map[string]any{"message": "before"},
			ValuesFrom: []helmv2.ValuesReference{{Kind: "Secret", Name: "extra-values"}},
		}
		st.AddObject(releases[side])
	}
	changes, err := change.Qualify(t.Context(), change.NewSet([]string{"hr.yaml"}), stores, manifest.WorkScope{})
	assert.NoError(t, err)
	changes, err = changes.WithDeclarations(t.Context(), change.DeclarationInputs{Stores: stores})
	assert.NoError(t, err)
	st, original := stores[0], releases[0]
	observed := original.Clone()
	document, err := manifest.ObjectDocument(observed.Named(), observed)
	assert.NoError(t, err)
	var previous, current store.ReadSet
	previous.Init(st, original.Named(), nil)
	defer previous.Retire()
	previous.GetDeclaration(original.Named())
	previous.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	current.Init(st, original.Named(), nil)
	defer current.Retire()
	current.GetDeclaration(original.Named())
	current.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
	facts := &solverFacts{
		store:   st,
		changes: changes,
		sources: map[manifest.NamedResource]SourceFacts{manifest.BootstrapSourceID: {Paired: true}},
		index: manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				original.Named(): {Declaration: observed, Builder: "kustomization.yaml", Inputs: []string{"hr.yaml"}},
			},
		},
	}
	facts.context.SideKnown, facts.context.Side = true, manifest.TreeBase
	previous.GetObject(original.Named())
	previous.Productions(original.Named(), &facts.index)
	previous.GetObject(manifest.BootstrapSourceID)
	previous.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent|store.ArtifactRevision)
	for ref := range manifest.References(observed, manifest.ReferenceRelations) {
		previous.GetObject(ref.NamedResource)
	}
	c := solverClosure(t, st, facts)
	c.context = facts.context
	c.objects = &current
	c.proposal.reads = &current
	c.proposal.Request.Changes = changes
	c.changes = changes
	c.children = map[manifest.NamedResource]store.Child{
		original.Named(): {Obj: observed, Doc: document, SourceSupport: manifest.BootstrapSourceID},
	}
	c.states[original.Named()] = admissionState{initialKnown: true, affectedKnown: true}
	ref := manifest.Reference{NamedResource: original.Named()}
	assert.Equal(t, manifest.Equal(observed, original), true)
	assert.Equal(
		t,
		changes.OutputChanged(manifest.TreeBase, manifest.BootstrapSourceID, ref, original),
		change.Comparison{Known: true},
	)
	assert.Equal(t, changes.OutputChanged(manifest.TreeBase, manifest.BootstrapSourceID, ref, observed).Known, false)
	assert.Equal(t, current.MatchesContent(&previous, manifest.BootstrapSourceID), true)
	assert.Equal(t, current.MatchesContent(&previous, original.Named()), true)
	assert.Equal(t, c.outputChanged(original.Named(), &ref, observed).Known, false)
	assert.Equal(
		t,
		c.matchesDeclarationContent(original.Named(), &admissionState{}, &previous, &PrepareRequest{Changes: changes}),
		false,
	)

	c.sideKnown, c.side = true, manifest.TreeBase
	c.states[original.Named()] = admissionState{
		initialKnown: true, affectedKnown: true, affected: true, directKnown: true, direct: true,
	}
	preparation := &Preparation{closure: c, evidence: &previous}
	defer func() { preparation.Retire() }()
	accepted, committed, err := c.session.admitTest(t.Context(), AdmissionOptions{
		Request: PrepareRequest{
			Changes:     changes,
			Preparation: &preparation,
			Missing:     &solverMissingPolicy{allowed: true},
		},
		Stage: Execution,
	})
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, slices.Contains(accepted.Dirty, original.Named()), false)
	assert.Equal(t, preparation.closure.children[original.Named()].Obj, nil)
}
