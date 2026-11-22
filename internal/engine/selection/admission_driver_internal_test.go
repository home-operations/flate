package selection

import (
	"bytes"
	"context"
	"iter"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestAdmit_HistoryAvailability(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		supplied  bool
		available bool
		complete  bool
		reader    bool
	}{
		{name: "absent"},
		{name: "unavailable", supplied: true},
		{name: "incomplete", supplied: true, available: true},
		{name: "complete empty", supplied: true, available: true, complete: true},
		{name: "reader unavailable", reader: true},
		{name: "reader incomplete", reader: true, available: true},
		{name: "reader complete empty", reader: true, available: true, complete: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			st.AddObject(owner)
			session := newSolverSession(t, st, nil, Request{Changed: true})
			previous := AdmissionResult{Needed: []manifest.NamedResource{owner.Named()}}
			request := PrepareRequest{}
			var records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			if tc.available {
				records = func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
					if !tc.complete {
						yield(testutil.KustomizationID("apps", "child"), manifest.DiscoveryRecord{File: "child.yaml"})
					}
				}
			}
			if tc.supplied {
				request.History = map[manifest.NamedResource]struct {
					Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
					Complete bool
				}{owner.Named(): {Records: records, Complete: tc.complete}}
			}
			reads := 0
			if tc.reader {
				request.ReadHistory = func(context.Context, *Demand) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
					reads++
					return records, tc.complete
				}
			}
			result, accepted, err := session.admitTest(
				t.Context(),
				AdmissionOptions{Request: request, Previous: &previous},
			)
			assert.NoError(t, err)
			assert.Equal(t, accepted, true)
			history, answered := result.Request.History[owner.Named()]
			assert.Equal(t, answered, true)
			assert.Equal(t, history.Records != nil, tc.available)
			assert.Equal(t, history.Complete, tc.complete)
			assert.Equal(t, len(result.Demands), 0)
			assert.Equal(t, result.Request.Preparation, nil)
			assert.Equal(t, reads == 1, tc.reader)
			assert.Equal(t, len(request.History) == 1, tc.supplied)
			if tc.available {
				assert.Equal(t, len(maps.Collect(history.Records)) == 0, tc.complete)
			}
			assert.Equal(t, session.Snapshot().Output(owner.Named()), false)
		})
	}
}

func TestAdmit_EntryHistory(t *testing.T) {
	t.Parallel()
	for _, present := range []bool{false, true} {
		t.Run(map[bool]string{false: "zero entry owner", true: "complete empty entry"}[present], func(t *testing.T) {
			t.Parallel()
			session := newSolverSession(t, store.New(), nil, Request{Changed: true})
			options := AdmissionOptions{}
			if present {
				options.EntryHistory.Owner = testutil.KustomizationID("apps", "entry")
			}
			options.EntryHistory.Records = func(func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {}
			result, accepted, err := session.admitTest(t.Context(), options)
			assert.NoError(t, err)
			assert.Equal(t, accepted, true)
			assert.Equal(t, len(result.Request.History) == 1, present)
			if present {
				history := result.Request.History[options.EntryHistory.Owner]
				assert.Equal(t, history.Records != nil && history.Complete, true)
				assert.Equal(t, len(maps.Collect(history.Records)), 0)
			}
		})
	}
}

func TestAdmit_HistorySurvivesPublication(t *testing.T) {
	t.Parallel()
	t.Run("restricted owner generation", testRestrictedHistoryPublication)
	for _, availability := range []string{"unavailable", "incomplete", "complete empty"} {
		t.Run(availability, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			input := &store.SourceArtifact{Provider: store.ProviderInput, Content: "before"}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "before"}
			st.AddObject(owner)
			st.SetIncluded(owner.Named(), false, false, false)
			session := newSolverSession(t, st, nil, Request{Changed: true})
			previous := AdmissionResult{
				Needed:   []manifest.NamedResource{owner.Named()},
				Ordinary: []manifest.NamedResource{owner.Named()},
			}
			borrowed := map[manifest.NamedResource]struct {
				Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
				Complete bool
			}{owner.Named(): {}}
			reads := 0
			request := PrepareRequest{History: borrowed}
			rejected := testutil.KustomizationID("apps", "rejected")
			acceptedChild := testutil.KustomizationID("apps", "accepted")
			acceptedRecords := map[manifest.NamedResource]manifest.DiscoveryRecord{}
			if availability == "incomplete" {
				acceptedRecords[acceptedChild] = manifest.DiscoveryRecord{File: "accepted.yaml"}
			}
			var records iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
			if availability != "unavailable" {
				records = maps.All(acceptedRecords)
			}
			request.ReadHistory = func(_ context.Context, demand *Demand) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
				reads++
				assert.Equal(t, demand.Reads.Valid(), true)
				if reads == 1 {
					return maps.All(map[manifest.NamedResource]manifest.DiscoveryRecord{
						rejected: {File: "rejected.yaml"},
					}), true
				}
				return records, availability == "complete empty"
			}
			published := false
			stop := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
				if published || id != owner.Named() {
					return
				}
				published = true
				replacement := owner.Clone()
				replacement.Path = "after"
				st.AddObject(replacement)
			}, false)
			defer stop()
			result, accepted, err := session.admitTest(
				t.Context(),
				AdmissionOptions{Request: request, Previous: &previous, Stage: Execution},
			)
			assert.NoError(t, err)
			assert.Equal(t, accepted, true)
			assert.Equal(t, published, true)
			assert.Equal(t, reads, 2)
			history := result.Request.History[owner.Named()]
			assert.Equal(t, history.Records != nil, availability != "unavailable")
			assert.Equal(t, history.Complete, availability == "complete empty")
			assert.Equal(t, borrowed[owner.Named()].Records == nil, true)
			assert.Equal(t, borrowed[owner.Named()].Complete, false)
			var captured map[manifest.NamedResource]manifest.DiscoveryRecord
			if history.Records != nil {
				captured = maps.Collect(history.Records)
			}
			_, rejectedPresent := captured[rejected]
			assert.Equal(t, rejectedPresent, false)
			assert.Equal(t, len(captured) == 1, availability == "incomplete")
			assert.Equal(t, result.Request.Preparation, nil)
			assert.Equal(t, len(result.Demands), 0)
			next, nextAccepted, nextErr := session.admitTest(
				t.Context(),
				AdmissionOptions{Request: result.Request, Previous: &result},
			)
			assert.NoError(t, nextErr)
			assert.Equal(t, nextAccepted, true)
			assert.Equal(t, reads, 3)
			assert.Equal(t, next.Request.History[owner.Named()].Complete, history.Complete)
			assert.Equal(t, result.Request.History[owner.Named()].Complete, history.Complete)
		})
	}
}

func testRestrictedHistoryPublication(t *testing.T) {
	t.Parallel()
	st := store.New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "before"}
	child := &manifest.HelmRelease{Name: "child", Namespace: "apps"}
	st.AddObject(owner)
	ownerID, childID := owner.Named(), child.Named()

	var scopes atomic.Pointer[EntryScopes]
	scopes.Store(&EntryScopes{ownerID: {}})
	facts := &solverFacts{
		store:   st,
		context: SourceContext{EntryPath: "/repo/leaf", Frozen: true},
		sources: map[manifest.NamedResource]SourceFacts{manifest.BootstrapSourceID: {EntryScopes: &scopes}},
		index: manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			childID: {Builder: "kustomization.yaml", BuilderOwner: ownerID, Declaration: child},
		}},
	}
	session, err := New(st, facts, Request{Roots: []manifest.NamedResource{childID}})
	assert.NoError(t, err)
	request := PrepareRequest{Invocation: true}
	pending, err := session.prepareTest(t.Context(), request)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(pending.Selected, childID), false)
	pending.Retire()

	proof := &store.ReadSet{}
	proof.Init(st, ownerID, nil)
	proof.GetDeclaration(ownerID)
	t.Cleanup(proof.Retire)
	scopes.Store(&EntryScopes{ownerID: {Support: proof, Children: map[manifest.NamedResource]bool{childID: true}}})
	result, committed, err := session.admitTest(t.Context(), AdmissionOptions{Request: request, Stage: Execution})
	assert.NoError(t, err)
	assert.Equal(t, committed, true)
	assert.Equal(t, slices.Contains(result.Selected, childID), true)
	assert.Equal(t, session.Snapshot().Output(childID), true)

	var preparation *Preparation
	t.Cleanup(func() { preparation.Retire() })
	next := PrepareRequest{Owner: ownerID, Generation: []store.Child{{Obj: child}}, Preparation: &preparation}
	_, prepared, err := session.admitTest(t.Context(), AdmissionOptions{Request: next, Stage: Execution})
	assert.NoError(t, err)
	assert.Equal(t, prepared, true)
	reused, err := session.prepareTest(t.Context(), next)
	assert.NoError(t, err)
	assert.Equal(t, reused.reused, true)
	reused.Retire()

	replacement := owner.Clone()
	replacement.Path = "after"
	st.AddObject(replacement)
	assert.Equal(t, proof.Valid(), false)
	stale, err := session.prepareTest(t.Context(), next)
	assert.NoError(t, err)
	assert.Equal(t, stale.reused, false)
	assert.Equal(t, slices.Contains(stale.Selected, childID), false)
	stale.Retire()

	refreshed, err := session.refreshHistoryTest(t.Context(), &result, request)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(refreshed.Selected, childID), false)
	refreshed.Retire()

	next.AwaitEmission = true
	_, published, err := session.admitTest(t.Context(), AdmissionOptions{Request: next, Stage: Execution})
	assert.Equal(t, published, false)
	assert.Equal(
		t,
		err != nil && err.Error() == EntryMembershipFailure(facts.context.EntryPath, ownerID, childID).Error(),
		true,
	)

	scopes.Store(&EntryScopes{ownerID: {Support: &store.ReadSet{}}})
	outside, err := session.prepareTest(t.Context(), next)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(outside.Selected, childID), false)
	outside.Retire()

	delete(facts.sources, manifest.BootstrapSourceID)
	newSession, err := New(st, facts, Request{Roots: []manifest.NamedResource{childID}})
	assert.NoError(t, err)
	full, err := newSession.prepareTest(t.Context(), request)
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(full.Selected, childID), true)
	full.Retire()
}

type retryAcquisitionFacts struct {
	*solverFacts

	rootsRead       int
	occurrencesRead int
	watched         manifest.NamedResource
	captured        *store.ReadSet
}

func (f *retryAcquisitionFacts) RootEvidence(id manifest.NamedResource, reads *store.ReadSet) RootEvidence {
	if f.watched.Name != "" {
		reads.GetObject(f.watched)
		f.captured = reads
	}
	return f.solverFacts.RootEvidence(id, reads)
}

//nolint:gocritic // The fixture MUST retain the census result signature.
func (f *retryAcquisitionFacts) ResolveRoot(
	ctx context.Context,
	reads *store.ReadSet,
	source manifest.NamedResource,
	root string,
) (string, bool, error) {
	f.rootsRead++
	return f.solverFacts.ResolveRoot(ctx, reads, source, root)
}

func (f *retryAcquisitionFacts) OccurrenceObject(
	ctx context.Context,
	occurrence *OccurrenceFacts,
	input *store.SourceArtifact,
) (store.Child, error) {
	f.occurrencesRead++
	return f.solverFacts.OccurrenceObject(ctx, occurrence, input)
}

//nolint:paralleltest // The occurrence trace uses the process logger.
func TestAdmit_PublicationRetryAcquiresDemand(t *testing.T) {
	t.Run("source replacement", testPublicationRetryReplacesSource)
	t.Run("base side", func(t *testing.T) { testPublicationRetryReusesOccurrence(t, manifest.TreeBase) })
	t.Run("head side", func(t *testing.T) { testPublicationRetryReusesOccurrence(t, manifest.TreeHead) })
}

func testPublicationRetryReusesOccurrence(t *testing.T, side manifest.TreeSide) {
	t.Helper()
	var trace bytes.Buffer
	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&trace, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(logger) })

	st := store.New()
	root := t.TempDir()
	input := &store.SourceArtifact{
		Provider:  store.ProviderInput,
		Content:   "source",
		LocalPath: root,
		FS:        fstest.MapFS{},
	}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "app"}
	facts := newRetryAcquisitionFacts(st, input, owner)
	facts.context.Side, facts.context.SideKnown = side, true
	reader := &manifest.Kustomization{Name: "reader", Namespace: "apps", Path: "reader"}
	facts.watched = reader.Named()
	st.AddObject(reader)
	session, err := New(st, facts, Request{Changed: true, Roots: []manifest.NamedResource{owner.Named()}})
	assert.NoError(t, err)
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	var reads store.ReadSet
	var work manifest.WorkRecorder
	reads.Init(st, manifest.NamedResource{}, nil)
	reads.SetWork(&work)
	defer reads.Retire()
	request := PrepareRequest{Preparation: &preparation, Reads: &reads}
	reads.RequireOrdering(&store.OrderingEdge{Target: owner.Named()})
	_, accepted, err := session.admitTest(t.Context(), AdmissionOptions{Request: request, Stage: Construction})
	assert.NoError(t, err)
	assert.Equal(t, accepted, true)
	first := preparation.occurrences[owner.Named()]
	assert.Equal(t, first != nil, true)
	assert.Equal(t, facts.occurrencesRead, 1)
	firstOwnerPrepares := work.Scope().Admission().Counts.OwnerPrepares
	assert.Equal(t, firstOwnerPrepares, uint64(1))
	st.DeleteObject(owner.Named())

	trigger := &manifest.ConfigMap{Name: "trigger", Namespace: "apps"}
	request.Invocation = true
	request.Generation = []store.Child{{Obj: trigger, Entry: &manifest.EntryApplication{}}}
	histories := 0
	request.HistoryOwners = map[manifest.NamedResource][]manifest.SourceEvidence{
		owner.Named(): {{Path: "app/child.yaml"}},
	}
	published := false
	request.ReadHistory = func(_ context.Context, demand *Demand) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
		histories++
		assert.Equal(t, demand.Identity, owner.Named())
		root := "app/../original"
		if published {
			root = "app/../added"
		}
		return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
			yield(owner.Named(), manifest.DiscoveryRecord{
				ContentRoots: map[manifest.NamedResource][]string{manifest.BootstrapSourceID: {root}},
			})
		}, false
	}
	stop := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if published || id != trigger.Named() {
			return
		}
		published = true
		replacement := reader.Clone()
		replacement.Path = "reader-after"
		st.AddObject(replacement)
		assert.Equal(t, st.GetObject(reader.Named()), manifest.BaseManifest(replacement))
		assert.Equal(t, facts.captured.Valid(), false)
	}, false)
	defer stop()
	result, accepted, err := session.admitTest(t.Context(), AdmissionOptions{Request: request, Stage: Execution})
	assert.NoError(t, err)
	assert.Equal(t, accepted, true)
	assert.Equal(t, published, true)
	assert.Equal(t, histories, 2)
	assert.Equal(t, facts.rootsRead, 2)
	assert.Equal(t, facts.occurrencesRead, 1)
	assert.Equal(t, strings.Count(trace.String(), "result=derived"), 1)
	assert.Equal(t, strings.Count(trace.String(), "result=reused") > 0, true)
	assert.Equal(t, preparation.occurrences[owner.Named()] == first, true)
	assert.Equal(t, work.Scope().Admission().Counts.OwnerPrepares > firstOwnerPrepares, true)
	assert.Equal(t, slices.Contains(result.Needed, owner.Named()), true)
	assert.Equal(t, len(result.Demands), 0)
}

func newRetryAcquisitionFacts(
	st *store.Store,
	input *store.SourceArtifact,
	owner *manifest.Kustomization,
) *retryAcquisitionFacts {
	root := input.LocalPath
	record := manifest.DiscoveryRecord{File: "owner.yaml", Path: filepath.Join(root, "owner.yaml"), Declaration: owner}
	occurrence := OccurrenceFacts{
		Identity: owner.Named(), Authored: owner.Named(), Object: owner, Catalog: owner,
		Declaration: OccurrenceID{Source: root, File: "owner.yaml"}, Mounted: input, Located: true, CatalogKnown: true,
	}
	return &retryAcquisitionFacts{solverFacts: &solverFacts{
		store:   st,
		context: SourceContext{Root: root},
		index: manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{owner.Named(): record},
		},
		candidates:  map[manifest.NamedResource]store.Child{owner.Named(): {Obj: owner}},
		occurrences: map[manifest.NamedResource]OccurrenceFacts{owner.Named(): occurrence},
		roots: map[manifest.NamedResource]RootEvidence{owner.Named(): {
			Identity:   owner.Named(),
			Record:     RootRecordFrom(&record),
			Occurrence: occurrence,
			Scope: RootScopeFacts{
				Path:              record.Path,
				Eligibility:       RootEligible,
				Initial:           true,
				Bootstrap:         true,
				NamespaceEligible: true,
				Candidate:         true,
			},
		}},
	}}
}

func testPublicationRetryReplacesSource(t *testing.T) {
	t.Helper()
	st := store.New()
	input := &store.SourceArtifact{Content: "before", LocalPath: t.TempDir(), FS: fstest.MapFS{}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "app"}
	facts := newRetryAcquisitionFacts(st, input, owner)
	session, err := New(st, facts, Request{Roots: []manifest.NamedResource{owner.Named()}})
	assert.NoError(t, err)
	var preparation *Preparation
	defer func() { preparation.Retire() }()
	var newer *store.SourceArtifact
	histories := 0
	trigger := &manifest.ConfigMap{Name: "trigger", Namespace: "apps"}
	published := false
	stop := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if published || id != trigger.Named() {
			return
		}
		published = true
		assert.Equal(t, histories > 0 && facts.rootsRead > 0 && facts.occurrencesRead > 0, true)
		assert.Equal(t, preparation.occurrences[owner.Named()], nil)
		replacement := *input
		replacement.Content = "after"
		newer = &replacement
		st.SetArtifact(manifest.BootstrapSourceID, newer)
		current := facts.occurrences[owner.Named()]
		current.Mounted = newer
		facts.occurrences[owner.Named()] = current
		rootEvidence := facts.roots[owner.Named()]
		rootEvidence.Occurrence = current
		facts.roots[owner.Named()] = rootEvidence
	}, false)
	defer stop()
	result, accepted, err := session.admitTest(t.Context(), AdmissionOptions{Request: PrepareRequest{
		Invocation:    true,
		Preparation:   &preparation,
		Generation:    []store.Child{{Obj: trigger, Entry: &manifest.EntryApplication{}}},
		HistoryOwners: map[manifest.NamedResource][]manifest.SourceEvidence{owner.Named(): {{Path: "app/child.yaml"}}},
		ReadHistory: func(context.Context, *Demand) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
			histories++
			return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
				yield(
					owner.Named(),
					manifest.DiscoveryRecord{
						ContentRoots: map[manifest.NamedResource][]string{
							manifest.BootstrapSourceID: {"app/../payload"},
						},
					},
				)
			}, false
		},
	}, Stage: Execution})
	assert.NoError(t, err)
	assert.Equal(t, accepted, true)
	assert.Equal(t, published, true)
	assert.Equal(t, histories, 2)
	assert.Equal(t, facts.rootsRead, 2)
	assert.Equal(t, facts.occurrencesRead, 2)
	assert.Equal(t, preparation.occurrences[owner.Named()].key.artifact, newer)
	assert.Equal(t, len(result.Demands), 0)
	assert.Equal(t, slices.Contains(result.Needed, owner.Named()), true)
	assert.Equal(t, st.GetDeclaration(owner.Named()) == owner, true)
}
