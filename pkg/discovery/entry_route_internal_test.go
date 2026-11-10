package discovery

import (
	"context"
	"errors"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/kustomize"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/selection"
	"github.com/home-operations/flate/pkg/store"
)

func TestEntryRoute_RejectedProposalPreservesAcceptedSupport(t *testing.T) {
	t.Parallel()
	for _, rejection := range []string{"cancellation", "bind_cancellation", "arrival"} {
		t.Run(rejection, func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			bootstrap, _ := manifest.BootstrapSourceID.BindLocal(root)
			st.AddObject(bootstrap)
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root})
			l := loader.New(st)
			l.SourceRoot = root
			configureSelection(t, l, selection.Request{Changed: true})
			a := &Admission{
				store:          st,
				loader:         l,
				input:          &store.SourceArtifact{LocalPath: root},
				trees:          testTrees(t),
				requestedEntry: manifest.EntryApplication{Source: manifest.BootstrapSourceID, Path: "entry"},
			}
			capture := func() *store.ReadSet {
				reads := &store.ReadSet{}
				reads.Init(st, manifest.BootstrapSourceID, nil)
				reads.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
				t.Cleanup(func() { st.Retire(reads) })
				return reads
			}
			support := capture()
			support.Retire()
			owner := manifest.NewEntryKustomization(manifest.BootstrapSourceID, "entry", manifest.NamedResource{}).
				Named()
			acceptedRoot := manifest.NamedResource{
				Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "apps", Name: "accepted",
			}
			binding, err := a.AcceptEntry(t.Context(), Binding{
				Producer: owner, Reads: capture(), support: support,
				Entry: &kustomize.EntryEvidence{Root: acceptedRoot},
			})
			assert.NoError(t, err)
			assert.Equal(t, binding.support, (*store.ReadSet)(nil))
			accepted := a.acceptedEntry
			assert.FatalIf(t, accepted == nil, "accepted route was not retained")
			assert.Equal(t, accepted.Reads, support)
			assert.Equal(t, accepted.Root, acceptedRoot)
			proposal, err := l.Options.Session.Prepare(
				t.Context(),
				selection.PrepareRequest{Missing: l.Options.Missing},
			)
			assert.NoError(t, err)
			provisional := capture()
			provisional.Retire()
			replacement := owner
			replacement.Name = "replacement"
			matchedRoot := acceptedRoot
			matchedRoot.Name = "matched"
			a.qualification = &entryQualification{matches: []applicationEvidence{{root: matchedRoot}}}
			matched := a.publishEntry(provisional, replacement)
			assert.Equal(t, matched.Reads, provisional)
			assert.Equal(t, matched.Root, matchedRoot)
			receiptRoot := acceptedRoot
			receiptRoot.Name = "receipt"
			assert.Equal(t, a.qualification.matches[0].history, false)
			a.qualification.accepted = &kustomize.EntryEvidence{Root: receiptRoot}
			reused := a.publishEntry(provisional, replacement)
			assert.Equal(t, reused.Reads, provisional)
			assert.Equal(t, reused.Root, receiptRoot)
			assert.Equal(t, a.entryEvidence.Root, acceptedRoot)
			ctx := t.Context()
			if rejection != "arrival" {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			} else {
				a.qualification = &entryQualification{}
				a.applications.Store(
					&applicationEvidence{object: &manifest.ResourceSet{Name: "arrival", Namespace: "apps"}},
				)
			}
			if rejection == "bind_cancellation" {
				_, _, err = a.BindEntry(ctx, st)
			} else {
				_, err = a.AcceptEntry(ctx, Binding{Producer: replacement, Reads: capture(), support: provisional})
			}
			if rejection != "arrival" {
				assert.Equal(t, errors.Is(err, context.Canceled), true)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, a.acceptedEntry, accepted)
			assert.Equal(t, a.acceptedEntry.Root, acceptedRoot)
			committed, err := commitSelection(t.Context(), l, proposal, selection.Execution)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
		})
	}
}
