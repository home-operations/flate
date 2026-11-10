package selection

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestEntryRoute_InheritedAndGuarded(t *testing.T) {
	t.Parallel()
	for _, change := range []string{"source", "declaration", "route", "root", "withdrawal", "publication", "retired-declaration"} {
		t.Run(change, func(t *testing.T) {
			t.Parallel()
			session := newPublicationSession(t)
			st := session.store
			input := &store.SourceArtifact{LocalPath: t.TempDir()}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			owner := &manifest.Kustomization{Name: "application", Namespace: "apps", Path: "entry"}
			st.AddObject(owner)
			guard := &store.ReadSet{}
			guard.Init(st, manifest.BootstrapSourceID, nil)
			guard.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
			guard.GetDeclaration(owner.Named())
			defer st.Retire(guard)
			guard.Retire()
			defer guard.ReleaseCapture()
			binding := &store.ReadSet{}
			binding.Init(st, manifest.BootstrapSourceID, nil)
			binding.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
			binding.GetDeclaration(owner.Named())
			binding.ReleaseCapture()
			defer st.Retire(binding)
			assert.Equal(t, binding.Valid(), true)
			route := &EntryRoute{
				Application: &manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "entry"},
				Producer:    owner.Named(), Root: owner.Named(), Reads: guard, Bound: true,
			}
			session.PublishEntry(route)
			equal := *route
			session.PublishEntry(&equal)
			assert.Equal(t, session.entry.Load(), route)
			proposal, err := session.prepareTest(t.Context(), PrepareRequest{})
			assert.NoError(t, err)
			assert.Equal(t, proposal.entry, route)
			assert.Equal(t, proposal.entrySupported(), true)
			refreshed, err := session.refreshHistoryTest(t.Context(), new(proposal.result()), PrepareRequest{})
			assert.NoError(t, err)
			assert.Equal(t, refreshed.entry, route)
			refreshed.Retire()

			if change == "retired-declaration" {
				proposal.Retire()
				var committed bool
				_, committed, err = session.publishProposal(t.Context(), proposal, Construction)
				assert.NoError(t, err)
				assert.Equal(t, committed, true)
			}
			prepared, changed := make(chan struct{}), make(chan struct{})
			go func() {
				<-prepared
				switch change {
				case "publication":
					st.SetIncluded(owner.Named(), false, false)
					st.SetIncluded(owner.Named(), true, true)
				case "source":
					st.SetArtifact(
						manifest.BootstrapSourceID,
						&store.SourceArtifact{LocalPath: input.LocalPath + "-replacement"},
					)
				case "declaration", "retired-declaration":
					replacement := *owner
					replacement.Path = "elsewhere"
					st.AddObject(&replacement)
				case "route":
					replacement := *route
					replacement.Producer.Name = "other"
					session.PublishEntry(&replacement)
				case "root":
					replacement := *route
					replacement.Root.Name = "other"
					session.PublishEntry(&replacement)
				case "withdrawal":
					session.PublishEntry(nil)
				}
				close(changed)
			}()
			close(prepared)
			<-changed
			_, committed, err := session.publishProposal(t.Context(), proposal, Execution)
			assert.NoError(t, err)
			assert.Equal(t, committed, false)
			assert.Equal(t, session.Snapshot().version(), uint64(0))
			current, err := session.prepareTest(t.Context(), PrepareRequest{})
			assert.NoError(t, err)
			defer current.Retire()
			assert.Equal(t, current.entry, session.entry.Load())
			if change != "route" && change != "root" && change != "publication" {
				assert.Equal(t, current.entrySupported(), false)
			} else {
				assert.Equal(t, current.entrySupported(), true)
			}
			_, committed, err = session.publishProposal(t.Context(), current, Execution)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
		})
	}
}

func TestEntryRoute_BoundWithoutSupport(t *testing.T) {
	t.Parallel()
	var full *Session
	full.PublishEntry(&EntryRoute{Bound: true})
	assert.Equal(t, full.Snapshot(), (*Decision)(nil))
	session := newPublicationSession(t)
	session.PublishEntry(&EntryRoute{
		Application: &manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "entry"},
		Bound:       true,
	})
	proposal, err := session.prepareTest(t.Context(), PrepareRequest{})
	assert.NoError(t, err)
	defer proposal.Retire()
	assert.Equal(t, proposal.entrySupported(), false)
}
