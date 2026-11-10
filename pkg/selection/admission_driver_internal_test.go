package selection

import (
	"context"
	"iter"
	"maps"
	"path/filepath"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
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

//nolint:gocognit // Publication retries MUST compare every history state and rejected observation.
func TestAdmit_HistorySurvivesPublication(t *testing.T) {
	t.Parallel()
	for _, availability := range []string{"unavailable", "incomplete", "complete empty"} {
		t.Run(availability, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			input := &store.SourceArtifact{Content: "before"}
			st.SetArtifact(manifest.BootstrapSourceID, input)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "before"}
			st.AddObject(owner)
			st.SetIncluded(owner.Named(), false, false)
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
			request.ReadHistory = func(_ context.Context, demand *Demand) (iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord], bool) {
				reads++
				assert.Equal(t, demand.Reads.Valid(), true)
				if reads == 1 {
					return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
						yield(rejected, manifest.DiscoveryRecord{File: "rejected.yaml"})
					}, true
				}
				if availability == "unavailable" {
					return nil, false
				}
				return func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
					if availability == "incomplete" {
						yield(acceptedChild, manifest.DiscoveryRecord{File: "accepted.yaml"})
					}
				}, availability == "complete empty"
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
			if history.Records != nil {
				captured := maps.Collect(history.Records)
				_, rejectedPresent := captured[rejected]
				assert.Equal(t, rejectedPresent, false)
				assert.Equal(t, len(captured) == 1, availability == "incomplete")
			}
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

type retryAcquisitionFacts struct {
	*solverFacts

	rootsRead       int
	occurrencesRead int
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

func TestAdmit_PublicationRetryAcquiresDemand(t *testing.T) {
	t.Parallel()
	st := store.New()
	root := t.TempDir()
	input := &store.SourceArtifact{Content: "before", LocalPath: root, FS: fstest.MapFS{}}
	st.SetArtifact(manifest.BootstrapSourceID, input)
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "app"}
	record := manifest.DiscoveryRecord{File: "owner.yaml", Path: filepath.Join(root, "owner.yaml"), Declaration: owner}
	occurrence := OccurrenceFacts{
		Identity: owner.Named(), Authored: owner.Named(), Object: owner, Catalog: owner,
		Declaration: OccurrenceID{Source: root, File: "owner.yaml"}, Mounted: input, Located: true, CatalogKnown: true,
	}
	facts := &retryAcquisitionFacts{solverFacts: &solverFacts{
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
	session, err := New(st, facts, Request{Roots: []manifest.NamedResource{owner.Named()}})
	assert.NoError(t, err)
	histories := 0
	trigger := &manifest.ConfigMap{Name: "trigger", Namespace: "apps"}
	published := false
	stop := st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if published || id != trigger.Named() {
			return
		}
		published = true
		assert.Equal(t, histories > 0 && facts.rootsRead > 0 && facts.occurrencesRead > 0, true)
		replacement := *input
		replacement.Content = "after"
		st.SetArtifact(manifest.BootstrapSourceID, &replacement)
		current := facts.occurrences[owner.Named()]
		current.Mounted = &replacement
		facts.occurrences[owner.Named()] = current
		rootEvidence := facts.roots[owner.Named()]
		rootEvidence.Occurrence = current
		facts.roots[owner.Named()] = rootEvidence
	}, false)
	defer stop()
	result, accepted, err := session.admitTest(t.Context(), AdmissionOptions{Request: PrepareRequest{
		Invocation:    true,
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
	assert.Equal(t, len(result.Demands), 0)
	assert.Equal(t, slices.Contains(result.Needed, owner.Named()), true)
	assert.Equal(t, st.GetDeclaration(owner.Named()) == owner, true)
}
