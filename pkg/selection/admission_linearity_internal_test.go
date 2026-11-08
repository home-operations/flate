package selection

import (
	"fmt"
	"iter"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestAdmission_ChangedDeclarationsPrepareLinearly(t *testing.T) {
	t.Parallel()
	for _, count := range []int{32, 128} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			testAdmissionChangedDeclarationsPrepareLinearlyCase(
				t,
				&count,
			)
		},
		)
	}
}

func TestAdmission_StableAncestorsPrepareLinearly(t *testing.T) {
	t.Parallel()
	for _, count := range []int{32, 128} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			facts := &solverFacts{
				store:   st,
				changes: change.NewSet([]string{"unrelated.yaml"}),
				index:   manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
			}
			facts.context.Root = root

			facts.occurrences = map[manifest.NamedResource]OccurrenceFacts{}
			var parent manifest.NamedResource
			for i := range count {
				object := &manifest.Kustomization{Name: fmt.Sprintf("item-%05d", i), Namespace: "apps"}
				object.Path = "./apps"
				id := object.Named()
				file := fmt.Sprintf("declarations/%05d.yaml", i)
				facts.index.Records[id] = manifest.DiscoveryRecord{File: file, Parent: parent, Declaration: object}
				facts.occurrences[id] = OccurrenceFacts{
					Identity:     id,
					Authored:     id,
					Object:       object,
					Catalog:      object,
					Declaration:  OccurrenceID{Source: root, File: file},
					Located:      true,
					CatalogKnown: true,
				}
				parent = id
			}
			reads := &admissionArtifactReads{admissionReader: st}
			c := solverClosure(t, st, facts)
			c.objects = reads
			proposal, err := evaluateSolver(t, &c, &PrepareRequest{Invocation: true})
			assert.NoError(t, err)
			assert.Equal(t, len(proposal.Needed), 0)
			t.Logf("%d stable ancestors: %d artifact reads", count, reads.calls)
			if reads.calls > 128*count+128 {
				t.Errorf(
					"%d stable ancestors caused %d artifact reads; want at most %d",
					count,
					reads.calls,
					128*count+128,
				)
			}
		})
	}
}

func TestAdmission_EmittedSourceVisitsItsPath(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		count   int
		history bool
	}{{32, false}, {128, false}, {32, true}, {128, true}} {
		t.Run(fmt.Sprintf("%d/history-%t", tc.count, tc.history), func(t *testing.T) {
			count := tc.count
			t.Parallel()
			root := t.TempDir()
			st := store.New()
			st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: fstest.MapFS{}})
			session := newSolverSession(t, st, change.NewSet(nil), Request{Changed: true})
			facts := session.facts.(*solverFacts)
			facts.context.Frozen = true
			facts.index.Records = map[manifest.NamedResource]manifest.DiscoveryRecord{}
			for i := range count {
				id := manifest.NamedResource{
					Kind:      manifest.KindConfigMap,
					Namespace: "apps",
					Name:      fmt.Sprintf("unrelated-%d", i),
				}
				file := fmt.Sprintf("owned-other/%d.yaml", i)
				facts.index.Records[id] = manifest.DiscoveryRecord{File: file, Path: filepath.Join(root, file)}
			}
			target := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "needed"}
			facts.index.Records[target] = manifest.DiscoveryRecord{
				File: "owned/child.yaml",
				Path: filepath.Join(root, "owned", "child.yaml"),
			}
			publishDecision(t, session, &Admission{Needed: []manifest.NamedResource{target}}, 0)
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "owned"}
			var preparation *Preparation
			defer func() { preparation.Retire() }()
			var reads store.ReadSet
			reads.Init(st, owner.Named(), nil)
			defer reads.Close()
			var work manifest.WorkRecorder
			reads.SetWork(&work)
			request := PrepareRequest{Generation: []store.Child{{Obj: owner}}, Reads: &reads, Preparation: &preparation}
			if tc.history {
				request.History = map[manifest.NamedResource]struct {
					Records  iter.Seq2[manifest.NamedResource, manifest.DiscoveryRecord]
					Complete bool
				}{owner.Named(): {Records: func(yield func(manifest.NamedResource, manifest.DiscoveryRecord) bool) {
					yield(target, manifest.DiscoveryRecord{Origin: manifest.RepositoryDeclaration})
				}, Complete: true}}
			}
			proposal, err := session.Prepare(t.Context(), request)
			assert.NoError(t, err)
			defer proposal.Retire()
			assert.Equal(t, slices.Contains(proposal.Needed, owner.Named()), true)
			assert.Equal(t, work.Scope().Admission().Counts.FileEntries, uint64(1))
			for id := range facts.index.Records {
				if id == target {
					continue
				}
				_, retained := preparation.closure.states[id]
				assert.Equal(t, retained, false)
			}
		})
	}
}

func testAdmissionChangedDeclarationsPrepareLinearlyCase(
	t *testing.T,
	testCaseInput *int,
) {
	t.Helper()

	l := publishedAdmissionDeclarations(t, *testCaseInput)
	st := l.store
	facts := l.facts.(*solverFacts)
	var required store.ReadSet
	required.Init(st, manifest.NamedResource{}, nil)
	defer required.Close()
	ids := make([]manifest.NamedResource, 0, (*testCaseInput))
	for _, object := range st.Objects() {
		ids = append(ids, object.Named())
		required.RequireData(object.Named())
	}
	_, retry := publishDecision(
		t,
		l,
		new(Admission{Needed: ids, Selected: ids, Dirty: ids, Ordinary: ids}),
		l.Snapshot().version(),
	)
	assert.Equal(t, retry, false)
	reads := &admissionArtifactReads{admissionReader: st}
	c := solverClosure(t, st, facts)
	c.session, c.objects = l, reads
	proposal, err := evaluateSolver(t, &c, &PrepareRequest{Reads: &required})
	assert.NoError(t, err)
	assert.Equal(t, len(proposal.Needed), 0)
	assert.Equal(t, len(proposal.Selected), 0)
	assert.Equal(t, len(proposal.Objects), 0)
	assert.Equal(t, reads.calls, 0)
	assert.Equal(t, reads.lists, 0)
	calls := 0
	for _, object := range st.Objects() {
		var proof store.ReadSet
		proof.Init(st, object.Named(), nil)
		readsResult := &admissionArtifactReads{admissionReader: &proof}
		cResult := solverClosure(t, st, facts)
		cResult.session, cResult.objects = l, readsResult
		cResult.generation = []store.Child{{Obj: object, Productions: st.Productions(object.Named(), &facts.index)}}
		evaluateProposal, evaluateErr := evaluateSolver(t, &cResult, &PrepareRequest{Reads: &proof})
		proof.Close()
		assert.NoError(t, evaluateErr)
		assert.Equal(t, len(evaluateProposal.Objects), 0)
		assert.Equal(t, readsResult.lists, 1)
		calls += readsResult.calls
	}
	t.Logf("%d dispatches: %d artifact reads", (*testCaseInput), calls)
	if calls > 32*(*testCaseInput)+32 {
		t.Errorf(
			"%d declarations reopened %d artifact reads during dispatch; want at most %d",
			(*testCaseInput),
			calls,
			32*(*testCaseInput)+32,
		)
	}
	child := store.Child{
		Obj:         &manifest.ConfigMap{Name: "emitted", Namespace: "apps"},
		Productions: []manifest.Production{{Owner: ids[0]}},
	}
	reads = &admissionArtifactReads{admissionReader: st}
	c = solverClosure(t, st, facts)
	c.session, c.objects, c.generation = l, reads, []store.Child{child}
	proposal, err = evaluateSolver(t, &c, &PrepareRequest{Invocation: true})
	assert.NoError(t, err)
	assert.Equal(t, slices.Contains(proposal.Needed, child.Obj.Named()), true)
	assert.Equal(t, slices.Contains(proposal.Selected, child.Obj.Named()), true)
	t.Logf("emitted data child with %d unrelated declarations: %d inventories", (*testCaseInput), reads.lists)
	assert.Equal(t, reads.lists, 0)
}

func publishedAdmissionDeclarations(t *testing.T, count int) *Session {
	t.Helper()
	root := t.TempDir()
	files := fstest.MapFS{}
	paths := make([]string, count)
	for i := range paths {
		name := fmt.Sprintf("item-%05d", i)
		paths[i] = name + ".yaml"
		files[paths[i]] = &fstest.MapFile{Data: []byte(testutil.KustomizationYAML(name, "apps", "  suspend: true\n"))}
	}
	st := store.New()
	st.SetArtifact(manifest.BootstrapSourceID, &store.SourceArtifact{LocalPath: root, FS: files})
	facts := &solverFacts{
		store:       st,
		changes:     change.NewSet(paths),
		index:       manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}},
		candidates:  map[manifest.NamedResource]store.Child{},
		occurrences: map[manifest.NamedResource]OccurrenceFacts{},
		roots:       map[manifest.NamedResource]RootEvidence{},
	}
	facts.context.Root = root
	for i, file := range paths {
		obj := &manifest.Kustomization{Name: fmt.Sprintf("item-%05d", i), Namespace: "apps", Suspend: true}
		record := manifest.DiscoveryRecord{File: file, Path: filepath.Join(root, file), Declaration: obj}
		facts.index.Records[obj.Named()] = record
		facts.roots[obj.Named()] = RootEvidence{
			Identity: obj.Named(),
			Record:   RootRecordFrom(&record),
			Captured: true,
			Scope:    RootScopeFacts{Candidate: true, NamespaceEligible: true, Initial: true},
		}
		facts.occurrences[obj.Named()] = OccurrenceFacts{
			Identity:     obj.Named(),
			Authored:     obj.Named(),
			Object:       obj,
			Catalog:      obj,
			Located:      true,
			CatalogKnown: true,
			Mounted:      st.GetArtifact(manifest.BootstrapSourceID, store.ArtifactContent).(*store.SourceArtifact),
			Declaration:  OccurrenceID{Source: root, File: file},
		}
		facts.candidates[obj.Named()] = store.Child{Obj: obj}
		rootFacts := facts.roots[obj.Named()]
		rootFacts.Occurrence = facts.occurrences[obj.Named()]
		facts.roots[obj.Named()] = rootFacts
	}
	session, err := New(st, facts, Request{Changed: true})
	assert.NoError(t, err)
	for _, published := range []bool{false, true} {
		reads := &admissionArtifactReads{admissionReader: st}
		c := solverClosure(t, st, facts)
		c.session = session
		c.objects = reads
		proposal, err := evaluateSolver(t, &c, &PrepareRequest{Invocation: true})
		assert.NoError(t, err)
		assert.Equal(t, len(proposal.Selected), count)
		wantObjects := count
		if published {
			wantObjects = 0
		}
		assert.Equal(t, len(proposal.Objects), wantObjects)
		t.Logf(
			"%d declarations, published %v: %d artifact reads, %d inventories",
			count,
			published,
			reads.calls,
			reads.lists,
		)
		if reads.calls > 32*count+32 {
			t.Errorf(
				"%d changed declarations caused %d artifact reads; want at most %d",
				count,
				reads.calls,
				32*count+32,
			)
		}
		assert.Equal(t, reads.lists, 1)
		for i := range proposal.Objects {
			child := &proposal.Objects[i]
			st.AddObject(child.Obj, child.Doc)
		}
	}
	return session
}
