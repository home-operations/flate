package selection

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"testing"
	"testing/fstest"
	"weak"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestCommit_ProposalLifetime(t *testing.T) {
	t.Parallel()
	for _, outcome := range []string{"success", "retry", "cancel", "abandon", "stale"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{LocalPath: t.TempDir(), FS: fstest.MapFS{}},
			)
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			st.AddObject(parent)
			session := newSolverSession(t, st, nil, Request{Roots: []manifest.NamedResource{parent.Named()}})
			var preparation *Preparation
			defer func() { preparation.Retire() }()

			var guard store.ReadSet
			guard.Init(st, parent.Named(), nil)
			t.Cleanup(guard.Retire)
			guard.GetDeclaration(parent.Named())
			guard.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
			initial, err := prepareSolver(
				t.Context(),
				t,
				session,
				PrepareRequest{Preparation: &preparation, Generation: []store.Child{{Obj: parent}}},
			)
			assert.NoError(t, err)
			_, committed, err := session.commitSolver(t.Context(), t, initial, Construction)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
			before := session.Snapshot()
			assert.Equal(t, before != nil, true)
			assert.Diff(t, ordinaryIdentities(before), initial.Ordinary)
			var declaration weak.Pointer[manifest.Kustomization]
			proposal := func() *Admission {
				child := &manifest.Kustomization{Name: "entry", Namespace: "apps"}
				declaration = weak.Make(child)
				proposal, err := prepareSolver(t.Context(), t, session, PrepareRequest{
					Preparation: &preparation,
					Generation:  []store.Child{{Obj: child, Entry: &manifest.EntryApplication{}}}, Reads: &guard,
				})
				assert.NoError(t, err)
				return proposal
			}()
			t.Cleanup(proposal.Retire)
			needed := slices.Clone(proposal.Needed)
			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			switch outcome {
			case "cancel":
				cancel()
			case "stale":
				replacement := parent.Clone()
				replacement.Path = "./replacement"
				st.AddObject(replacement)
			case "retry":
				_, retry := publishDecision(
					t,
					session,
					new(Admission{Needed: []manifest.NamedResource{testutil.KustomizationID("apps", "other")}}),
					proposal.Revision,
				)
				assert.Equal(t, retry, false)
			}
			if outcome == "abandon" {
				proposal.Retire()
			} else {
				_, committed, err := session.commitSolver(ctx, t, proposal, Construction)
				assert.Equal(t, errors.Is(err, context.Canceled), outcome == "cancel")
				assert.Equal(t, committed, outcome == "success" || outcome == "retry")
			}
			assert.Equal(t, proposal.reads, nil)
			assert.Equal(t, len(proposal.Objects)+len(proposal.sources), 0)
			assert.Equal(t, len(proposal.Request.Generation), 0)
			assert.Equal(t, proposal.Request.Reads, nil)
			assert.Diff(t, proposal.Needed, needed)
			assert.Equal(t, guard.Valid(), outcome != "stale")
			if outcome == "success" || outcome == "retry" {
				assert.Equal(t, session.Snapshot().Reconcile(parent.Named()) || outcome == "retry", true)
				assert.Equal(t, before.Output(parent.Named()), false)
				empty, err := prepareSolver(
					t.Context(),
					t,
					session,
					PrepareRequest{Preparation: &preparation, Invocation: true},
				)
				assert.NoError(t, err)
				_, settled, err := session.commitSolver(t.Context(), t, empty, Execution)
				assert.NoError(t, err)
				assert.Equal(t, settled, true)
				assert.Equal(t, session.Snapshot().Output(parent.Named()), true)
			} else {
				assert.Equal(t, session.Snapshot(), before)
			}
			preparation.Retire()
			assert.Equal(t, preparation.evidence == nil && preparation.closure.session == nil, true)
			proposal.Retire()
			st.DeleteObject(testutil.KustomizationID("apps", "entry"))
			for range st.Sweep() {
			}
			runtime.GC()
			assert.Equal(t, declaration.Value(), nil)
			runtime.KeepAlive(proposal)
			runtime.KeepAlive(&guard)
		})
	}
}

func ordinaryIdentities(decision *Decision) []manifest.NamedResource {
	var ids []manifest.NamedResource
	for _, bucket := range decision.buckets {
		for id := range bucket {
			if decision.OrdinaryDemand(id) {
				ids = append(ids, id)
			}
		}
	}
	slices.SortFunc(ids, manifest.NamedResource.Compare)
	return ids
}
