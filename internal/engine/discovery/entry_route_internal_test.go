package discovery

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/home-operations/flate/internal/engine/kustomize"
	"github.com/home-operations/flate/internal/engine/loader"
	"github.com/home-operations/flate/internal/engine/selection"
	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
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
			st.SetArtifact(
				manifest.BootstrapSourceID,
				&store.SourceArtifact{Provider: store.ProviderInput, LocalPath: root},
			)
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
			acceptedIndex := l.Index
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
			a.qualification.matches[0].root = receiptRoot
			a.qualification.matches[0].receipt = &kustomize.EntryEvidence{Root: receiptRoot}
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
			assert.Equal(t, l.Index, acceptedIndex)
			assert.Equal(t, a.acceptedEntry.Root, acceptedRoot)
			committed, err := commitSelection(t.Context(), l, proposal, selection.Execution)
			assert.NoError(t, err)
			assert.Equal(t, committed, true)
		})
	}
}

func TestEntryRoute_AcceptedNamedWinnersReplaceEarlierRoute(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	st := store.New()
	input := &store.SourceArtifact{LocalPath: root}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	l := loader.New(st)
	l.SourceRoot = root
	target := selection.OccurrenceID{Source: root, File: "apps/workload.yaml"}
	workload := testutil.KustomizationID("apps", "workload")
	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{workload}})
	t.Cleanup(l.Retire)
	a := &Admission{store: st, loader: l, entryTargets: []entryTarget{{occurrence: target}}}
	for _, name := range []string{"first", "replacement"} {
		owner := testutil.KustomizationID("flux-system", name)
		a.qualification = &entryQualification{
			loader: l, winners: map[selection.OccurrenceID]manifest.NamedResource{target: owner},
		}
		var reads store.ReadSet
		reads.Init(st, manifest.BootstrapSourceID, nil)
		reads.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent)
		binding, err := a.AcceptEntry(t.Context(), Binding{Producer: owner, Key: owner, Reads: &reads})
		reads.ReleaseCapture()
		reads.Retire()
		assert.NoError(t, err)
		assert.Equal(t, binding.Producer, owner)
		route := a.acceptedEntry
		assert.FatalIf(t, route == nil || route.Winners == nil, "acceptance omitted named winners")
		assert.Diff(t, *route.Winners, map[selection.OccurrenceID]manifest.NamedResource{target: owner})
	}
}

func TestEntryRoute_AcceptedNamedWithdrawalClearsWinner(t *testing.T) {
	t.Parallel()
	repo := t.TempDir()
	testutil.WriteFile(t, repo, "apps/kustomization.yaml", "resources: [workload.yaml]\n")
	workload := testutil.KustomizationID("apps", "workload")
	testutil.WriteFile(
		t,
		repo,
		"apps/workload.yaml",
		testutil.KustomizationYAML(workload.Name, workload.Namespace, "  path: ./workload\n"),
	)

	st := store.New()
	bootstrap, _ := manifest.BootstrapSourceID.BindLocal(repo)
	st.AddObject(bootstrap)
	input := &store.SourceArtifact{Provider: store.ProviderInput, LocalPath: repo, FS: os.DirFS(repo)}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	l := loader.New(st)
	l.SourceRoot = repo
	configureSelection(t, l, selection.Request{Roots: []manifest.NamedResource{workload}})
	t.Cleanup(l.Retire)
	target := selection.OccurrenceID{Source: repo, File: "apps/workload.yaml"}
	a := &Admission{
		store: st, loader: l, input: input, path: filepath.Join(repo, "apps"),
		entryTargets: []entryTarget{{occurrence: target, origin: filepath.Join(repo, target.File)}},
	}
	producer := &manifest.Kustomization{Name: "factory", Namespace: "flux-system", Path: "./factory"}
	winner := &manifest.Kustomization{Name: "sync", Namespace: "flux-system", Path: "./apps"}
	st.AddObject(producer)
	st.RecordEmitted(producer.Named(), &store.Emission{
		Complete: true, Children: []store.Child{{Obj: winner, VisibleGeneration: true}},
	}, nil)
	a.AddApplication(st, winner)

	_, proposal, err := a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.FatalIf(t, proposal.Reads == nil, "named entry did not propose the published winner")
	accepted, err := a.AcceptEntry(t.Context(), proposal)
	assert.NoError(t, err)
	assert.Equal(t, accepted.Producer, winner.Named())
	assert.FatalIf(t, a.acceptedEntry == nil || a.acceptedEntry.Winners == nil, "the first binding omitted its winner")
	assert.Equal(t, (*a.acceptedEntry.Winners)[target], winner.Named())

	st.RecordEmitted(producer.Named(), &store.Emission{Complete: true}, nil)
	_, proposal, err = a.BindEntry(t.Context(), st)
	assert.NoError(t, err)
	assert.FatalIf(t, proposal.Reads == nil, "withdrawal did not propose the literal entry")
	assert.Equal(t, proposal.support, (*store.ReadSet)(nil))
	accepted, err = a.AcceptEntry(t.Context(), proposal)
	assert.NoError(t, err)
	assert.Equal(t, accepted.Producer.ReportHidden(), true)
	assert.FatalIf(t, a.acceptedEntry == nil || a.acceptedEntry.Winners == nil, "the fallback omitted the named route")
	assert.Equal(t, len(*a.acceptedEntry.Winners), 0)
}
