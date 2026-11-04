package store

import (
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestStore_ProductionsRetainsAcceptedOwner(t *testing.T) {
	t.Parallel()

	t.Run("frozen_borrow", func(t *testing.T) { t.Parallel(); testStoreProductionsRetainsAcceptedOwnerFrozenBorrow(t) })
	t.Run(
		"hidden_owner_rooted_child",
		func(t *testing.T) { t.Parallel(); testStoreProductionsRetainsAcceptedOwnerHiddenOwnerRootedChild(t) },
	)

	for _, filtered := range []int{0, 1} {
		t.Run(fmt.Sprintf("filtered_frozen_%d", filtered), func(t *testing.T) {
			t.Parallel()
			testStoreProductionsRetainsAcceptedOwnerCase(
				t,
				&filtered,
			)
		},
		)
	}
	t.Run(
		"captured_concurrent",
		func(t *testing.T) { t.Parallel(); testStoreProductionsRetainsAcceptedOwnerCapturedConcurrent(t) },
	)
	for _, ownEvidence := range []bool{false, true} {
		t.Run(strconv.FormatBool(ownEvidence), func(t *testing.T) {
			t.Parallel()
			testStoreProductionsRetainsAcceptedOwnerCase2(
				t,
				&ownEvidence,
			)
		},
		)
	}
}

func TestRecordEmitted_ConflictingProducers(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/reverse=%t", workers, reverse), func(t *testing.T) {
				t.Parallel()
				testRecordEmittedConflictingProducersCase(
					t,
					&testRecordEmittedConflictingProducersCaseInputs{workers: &workers, reverse: &reverse},
				)
			},
			)
		}
	}
}

func TestRecordEmitted_SharedChildArrival(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		t.Run(strconv.Itoa(workers), func(t *testing.T) {
			t.Parallel()

			s := New()
			child := &manifest.HelmChartSource{Name: "shared", Namespace: "apps", Chart: "app-template"}
			var arrivals atomic.Int64
			unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
				if id == child.Named() {
					arrivals.Add(1)
				}
			}, false)
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := range workers {
				owner := ownerID(strconv.Itoa(i))
				s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
				wg.Go(func() {
					<-start
					synthetic := *child
					s.RecordEmitted(owner, &Emission{Children: []Child{{Obj: &synthetic}}}, nil)
					s.RecordEmitted(
						owner,
						&Emission{
							Children: []Child{{Obj: &synthetic}, emissionChild(owner.Name, "complete")},
							Complete: true,
						},
						nil,
					)
				})
			}
			close(start)
			wg.Wait()
			unsub()
			{
				got := arrivals.Load()
				assert.FatalIf(t, got != 1, "identical shared child arrivals=%d want 1", got)
			}
		})
	}
}

func TestRecordEmitted_NeverEmittedCoverageWithdrawal(t *testing.T) {
	t.Parallel()

	seedRetained := func(t *testing.T, s *Store, id manifest.NamedResource) {
		t.Helper()
		s.SetArtifact(id, &SourceArtifact{Key: "static-l"})
		s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: ""})
		{
			assert.FatalIf(
				t,
				s.GetObject(id) == nil || s.GetArtifact(id, ArtifactContent) == nil,
				"%s",
				"fixture L not initially live with an artifact",
			)
		}
		{
			info, ok := s.GetStatus(id)
			assert.FatalIf(t, !ok || info.Status != StatusReady, "%s", "fixture L not initially Ready")
		}
	}
	fixture := func(t *testing.T, kName, lName string, reverse bool) (*Store, manifest.NamedResource, manifest.NamedResource, Child) {
		t.Helper()
		s := New()
		p, k := ownerID("p"), ownerID(kName)
		parent := &manifest.Kustomization{Name: p.Name, Namespace: p.Namespace}
		l := emissionChild(lName, "value")
		if reverse {
			s.AddObject(l.Obj)
			s.AddObject(parent)
		} else {
			s.AddObject(parent)
			s.AddObject(l.Obj)
		}
		s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			l.Obj.Named(): {
				Productions: []manifest.Production{
					{
						Owner:    k,
						Source:   manifest.BootstrapSourceID,
						Authored: l.Obj.Named(),
						Path:     "l.yaml",
						File:     "l.yaml",
					},
				},
			},
		}})
		s.RecordEmitted(
			p,
			&Emission{
				Children: []Child{{Obj: &manifest.Kustomization{Name: k.Name, Namespace: k.Namespace}}},
				Complete: true,
			},
			nil,
		)
		{
			assert.FatalIf(t, s.GetObject(k) == nil ||
				!slices.ContainsFunc(
					s.ListObjects("", ""),
					func(obj manifest.BaseManifest) bool { return obj.Named() == l.Obj.Named() },
				), "%s", "fixture K/L not initially visible")
		}
		seedRetained(t, s, l.Obj.Named())
		{
			assert.FatalIf(t, s.shardFor(k).slots[k].current.Load() != nil, "%s", "fixture K unexpectedly emitted")
		}
		return s, p, k, l
	}
	checkHidden := func(t *testing.T, s *Store, id manifest.NamedResource) {
		t.Helper()
		{
			assert.ErrorIf(t, s.GetObject(id) != nil, "%v object stays visible after withdrawal", id)
		}
		{
			assert.ErrorIf(t, slices.ContainsFunc(
				s.ListObjects("", ""),
				func(obj manifest.BaseManifest) bool { return obj.Named() == id },
			), "%v stays listed after withdrawal", id)
		}
		{
			assert.ErrorIf(t, s.GetArtifact(id, ArtifactContent) != nil, "%v artifact stays live after withdrawal", id)
		}
		{
			_, ok := s.GetStatus(id)
			assert.ErrorIf(t, ok, "%v status stays live after withdrawal", id)
		}
		sh := s.shardFor(id)
		sh.mu.RLock()
		defer sh.mu.RUnlock()
		var view capture
		sh.captureLocked(id, new(sh.cells[id]), &view)
		child := view.child
		var owner manifest.NamedResource
		if view.owner != nil {
			owner = view.owner.id
		}
		{
			assert.ErrorIf(
				t,
				!sh.cells[id].hidden || !sh.cells[id].retired || child.Obj != nil ||
					owner != (manifest.NamedResource{}),
				"%v hidden, retirement and representative disagree",
				id,
			)
		}
		{
			slot := sh.slots[id]
			assert.ErrorIf(t, slot != nil && (slot.visible.Load() || !slot.hidden.Load()), "%v slot remains live", id)
		}
	}
	t.Run("simple", func(t *testing.T) {
		t.Parallel()
		testRecordEmittedNeverEmittedCoverageWithdrawalSimple(
			t,
			&testRecordEmittedNeverEmittedCoverageWithdrawalSimpleInputs{
				seedRetained: &seedRetained,
				fixture:      &fixture,
				checkHidden:  &checkHidden,
			},
		)
	},
	)
	for _, workers := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("workers=%d/reverse=%t", workers, reverse), func(t *testing.T) {
				t.Parallel()
				testRecordEmittedNeverEmittedCoverageWithdrawalCase(
					t,
					&testRecordEmittedNeverEmittedCoverageWithdrawalCaseInputs{
						fixture:     &fixture,
						checkHidden: &checkHidden,
						workers:     &workers,
						reverse:     &reverse,
					},
				)
			},
			)
		}
	}
}

func testStoreProductionsRetainsAcceptedOwnerFrozenBorrow(t *testing.T) {
	t.Helper()

	s := New()
	p, q := ownerID("p"), ownerID("q")
	for _, id := range []manifest.NamedResource{p, q} {
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	child := emissionChild("shared", "value")
	proof := manifest.Production{Owner: p, Source: manifest.BootstrapSourceID}
	backing := []manifest.Production{proof, {Owner: ownerID("sentinel")}, {}, {}}
	original := slices.Clone(backing)
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		child.Obj.Named(): {Productions: backing[:1]},
	}}
	s.SetDiscoveryIndex(index)
	s.AddObject(child.Obj)
	accept(
		t,
		s,
		q,
		&Emission{
			Children:  []Child{child},
			Manifests: testutil.DocumentOccurrences([]map[string]any{child.Doc}),
			Complete:  true,
		},
	)
	snapshot := s.Productions(child.Obj.Named(), nil)
	facts := s.ReferenceFacts(child.Obj.Named(), nil)
	assert.Diff(t, facts.Data, child.Obj.(manifest.Data))
	{
		assert.FatalIf(t, !facts.Accepted, "accepted selected facts=%+v", facts)
	}
	want := []manifest.Production{proof, {Owner: q}}
	accept(t, s, q, &Emission{Complete: true})
	{
		got := snapshot
		assert.FatalIf(t, !slices.Equal(got, want), "frozen/runtime snapshot changed: got %v, want %v", got, want)
	}
	assert.Diff(t, facts.Productions, want)
	parent, ok := s.RenderedParent(child.Obj.Named(), nil)
	{
		assert.FatalIf(t, !ok || parent != p, "frozen surviving parent=%v/%t, want %v/true", parent, ok, p)
	}
	{
		assert.FatalIf(
			t,
			!slices.Equal(backing, original),
			"query mutated borrowed discovery evidence: got %v, want %v",
			backing,
			original,
		)
	}
	owned := child
	owned.Productions = []manifest.Production{proof}
	s.RecordEmitted(p, &Emission{Children: []Child{owned}}, nil)
	borrowed := s.Productions(child.Obj.Named(), nil)
	{
		assert.FatalIf(
			t,
			len(borrowed) != 1 || &borrowed[0] != &backing[0],
			"%s",
			"duplicate accepted evidence copied frozen backing",
		)
	}

	for _, evidence := range [][]manifest.Production{
		{{Owner: q}, proof, proof},
		{proof, proof, {Owner: q}},
	} {
		index = &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			child.Obj.Named(): {Productions: evidence},
		}}
		original = slices.Clone(evidence)
		{
			got := s.Productions(child.Obj.Named(), index)
			assert.FatalIf(
				t,
				!slices.Equal(got, want),
				"query did not sort/deduplicate frozen evidence: got %v, want %v",
				got,
				want,
			)
		}
		{
			assert.FatalIf(
				t,
				!slices.Equal(evidence, original),
				"query reordered borrowed discovery evidence: got %v, want %v",
				evidence,
				original,
			)
		}
	}
}

func testStoreProductionsRetainsAcceptedOwnerHiddenOwnerRootedChild(t *testing.T) {
	t.Helper()

	s := New()
	p := ownerID("absent")
	child := emissionChild("rooted", "value")
	s.AddObject(child.Obj)
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		child.Obj.Named(): {Productions: []manifest.Production{{Owner: p, Source: manifest.BootstrapSourceID}}},
	}})
	s.RecordEmitted(p, &Emission{Children: []Child{child}}, nil)
	s.AddObject(child.Obj)
	{
		assert.FatalIf(
			t,
			s.GetObject(child.Obj.Named()) != child.Obj || s.GetObject(p) != nil,
			"%s",
			"fixture needs an independently rooted child and hidden owner",
		)
	}
	assert.Equal(t, len(s.Productions(child.Obj.Named(), nil)), 0)
	{
		parent, ok := s.RenderedParent(child.Obj.Named(), nil)
		assert.FatalIf(t, ok || parent != (manifest.NamedResource{}), "hidden owner parent=%v/%t", parent, ok)
	}
}

func testStoreProductionsRetainsAcceptedOwnerCase(
	t *testing.T,
	testCaseInput *int,
) {
	t.Helper()

	s := New()
	p, q, r := ownerID("p"), ownerID("q"), ownerID("r")
	owners := []manifest.NamedResource{p, q, r}
	for _, id := range owners {
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	}
	child := emissionChild("shared", "value")
	proofs := []manifest.Production{
		{Owner: p, Source: manifest.BootstrapSourceID},
		{Owner: q, Source: manifest.BootstrapSourceID},
		{Owner: r, Source: manifest.BootstrapSourceID},
	}
	original := slices.Clone(proofs)
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		child.Obj.Named(): {Productions: proofs},
	}})
	s.AddObject(child.Obj)
	_, _, attempt := s.GetEmission(owners[(*testCaseInput)])
	attempt.Complete = true
	accept(t, s, owners[(*testCaseInput)], &attempt)
	attempt.Finish()
	snapshot := s.Productions(child.Obj.Named(), nil)
	parent, ok := s.RenderedParent(child.Obj.Named(), nil)
	wantParent := p
	if (*testCaseInput) == 0 {
		wantParent = q
	}
	{
		assert.FatalIf(
			t,
			!ok || parent != wantParent,
			"filtered parent=%v/%t, want %v/true",
			parent,
			ok,
			wantParent,
		)
	}
	{
		assert.FatalIf(
			t,
			!slices.Equal(proofs, original),
			"filter mutated borrowed discovery evidence: got %v, want %v",
			proofs,
			original,
		)
	}
	want := slices.Delete(slices.Clone(original), (*testCaseInput), (*testCaseInput)+1)
	for _, id := range owners {
		_, _, attempt := s.GetEmission(id)
		attempt.Complete = true
		accept(t, s, id, &attempt)
		attempt.Finish()
	}
	{
		got := snapshot
		assert.FatalIf(
			t,
			!slices.Equal(got, want),
			"filtered snapshot changed after withdrawal: got %v, want %v",
			got,
			want,
		)
	}
	{
		got := s.Productions(child.Obj.Named(), nil)
		assert.FatalIf(t, len(got) != 0, "hidden frozen evidence remains visible: %v", got)
	}
}

func testStoreProductionsRetainsAcceptedOwnerCapturedConcurrent(t *testing.T) {
	t.Helper()

	s := New()
	child := emissionChild("shared", "value")
	owners := make([]manifest.NamedResource, 0, 4)
	want := make([]manifest.Production, 0, 4)
	for i := range 4 {
		id := ownerID(strconv.Itoa(3 - i))
		owners = append(owners, id)
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		proof := manifest.Production{Owner: id, Source: manifest.BootstrapSourceID}
		want = append(want, proof)
		owned := child
		owned.Productions = []manifest.Production{proof, proof}
		accept(
			t,
			s,
			id,
			&Emission{
				Children:  []Child{owned},
				Manifests: testutil.DocumentOccurrences([]map[string]any{owned.Doc}),
				Complete:  true,
			},
		)
	}
	slices.SortFunc(want, compareProduction)
	snapshot := s.Productions(child.Obj.Named(), nil)
	var wg sync.WaitGroup
	for _, id := range owners {
		wg.Go(func() {
			replacement := emissionChild(id.Name, "replacement")
			accept(
				t,
				s,
				id,
				&Emission{
					Children:  []Child{replacement},
					Manifests: testutil.DocumentOccurrences([]map[string]any{replacement.Doc}),
					Complete:  true,
				},
			)
			accept(t, s, id, &Emission{Complete: true})
		})
	}
	wg.Wait()
	got := snapshot
	{
		assert.FatalIf(
			t,
			!slices.Equal(got, want),
			"captured productions changed after concurrent replacement/withdrawal: got %v, want %v",
			got,
			want,
		)
	}
	{
		got := s.Productions(child.Obj.Named(), nil)
		assert.FatalIf(t, len(got) != 0, "withdrawn productions remain visible: %v", got)
	}
	for range snapshot {
		s.AddObject(child.Obj)
	}
}

func testStoreProductionsRetainsAcceptedOwnerCase2(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	s := New()
	p, q := ownerID("p"), ownerID("q")
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	child := emissionChild("child", "value")
	proof := manifest.Production{
		Owner:    q,
		Authored: child.Obj.Named(),
		File:     "proof.yaml",
		Source:   manifest.BootstrapSourceID,
	}
	if *testCaseInput {
		proof.Owner = p
	}
	child.Productions = []manifest.Production{proof}
	accept(
		t,
		s,
		p,
		&Emission{
			Children:  []Child{child},
			Manifests: testutil.DocumentOccurrences([]map[string]any{child.Doc}),
			Complete:  true,
		},
	)
	got := s.Productions(child.Obj.Named(), nil)
	want := 2
	if *testCaseInput {
		want = 1
	}
	{
		assert.FatalIf(t, len(got) != want || !slices.Contains(got, proof) ||
			!slices.ContainsFunc(
				got,
				func(v manifest.Production) bool { return v.Owner == p },
			), "accepted owner lost or redundant thin evidence added: %v", got)
	}
	parent, ok := s.RenderedParent(child.Obj.Named(), nil)
	{
		assert.FatalIf(
			t,
			ok == (*testCaseInput) || (!(*testCaseInput) && parent != p),
			"authored parent=%v/%t, own evidence=%t",
			parent,
			ok, (*testCaseInput),
		)
	}
	accept(t, s, p, &Emission{Complete: true})
	{
		got := s.Productions(child.Obj.Named(), nil)
		assert.FatalIf(
			t,
			len(got) != 0 || s.GetObject(child.Obj.Named()) != nil,
			"withdrawal retained accepted evidence: %v",
			got,
		)
	}
	withdrawn := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		child.Obj.Named(): {Productions: []manifest.Production{{Owner: q}}},
	}}
	assert.Equal(t, len(s.Productions(child.Obj.Named(), withdrawn)), 0)
	{
		parent, ok := s.RenderedParent(
			child.Obj.Named(),
			withdrawn,
		)
		assert.FatalIf(t, ok ||
			parent != (manifest.NamedResource{}), "hidden explicit-index parent=%v/%t", parent, ok)
	}
}

type testRecordEmittedConflictingProducersCaseInputs struct {
	workers *int
	reverse *bool
}

func testRecordEmittedConflictingProducersCase(
	t *testing.T,
	testCaseInput *testRecordEmittedConflictingProducersCaseInputs,
) {
	t.Helper()

	s := New()
	owners := make([]manifest.NamedResource, (*testCaseInput.workers))
	children := make([]Child, (*testCaseInput.workers))
	release := make([]chan struct{}, (*testCaseInput.workers))
	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := range *testCaseInput.workers {
		owners[i] = ownerID(strconv.Itoa(i))
		children[i] = emissionChild("shared", strconv.Itoa(i))
		s.AddObject(&manifest.Kustomization{Name: owners[i].Name, Namespace: owners[i].Namespace})
		release[i] = make(chan struct{})
		wg.Go(func() {
			defer func() { done <- struct{}{} }()
			<-release[i]
			accept(
				t,
				s,
				owners[i],
				&Emission{
					Children:  []Child{children[i]},
					Manifests: testutil.DocumentOccurrences([]map[string]any{children[i].Doc}),
					Complete:  true,
				},
			)
		})
	}
	for i := range *testCaseInput.workers {
		if *testCaseInput.reverse {
			i = (*testCaseInput.workers) - 1 - i
		}
		close(release[i])
		<-done
	}
	wg.Wait()
	winner := children[(*testCaseInput.workers)-1]
	id := winner.Obj.Named()
	check := func(child Child, owner manifest.NamedResource) {
		t.Helper()
		assert.Equal(t, s.EmissionOwner(id), owner)
		var observation Observation
		obj, observation := s.Snapshot(id, true)
		info := observation.StatusInfo
		conditions := observation.Conditions
		assert.Equal(t, obj, child.Obj)
		want, _ := s.GetStatus(id)
		assert.Equal(t, info, want)
		assert.Equal(t, conditions == nil, true)
		checkEffectiveObject(t, s, id, child)
	}
	check(winner, owners[(*testCaseInput.workers)-1])
	warnings := s.Warnings()
	want := make([]string, (*testCaseInput.workers))
	for i, owner := range owners {
		want[i] = owner.String()
	}
	{
		assert.FatalIf(
			t,
			len(warnings) != 1 || warnings[0].Resource != id || warnings[0].Category != "ConflictingProducers" ||
				!slices.Equal(warnings[0].Detail, want) ||
				warnings[0].Count != 1,
			"conflict warning: %+v",
			warnings,
		)
	}
	assert.Equal(t, warnings[0].Message, "conflicting producers; using "+owners[(*testCaseInput.workers)-1].String())
	accept(t, s, owners[(*testCaseInput.workers)-1], &Emission{Complete: true})
	check(children[(*testCaseInput.workers)-2], owners[(*testCaseInput.workers)-2])
	warnings = s.Warnings()
	{
		assert.FatalIf(t, (*testCaseInput.workers) == 2 && len(warnings) != 0 || (*testCaseInput.workers) ==
			4 &&
			(len(warnings) != 1 || !slices.Equal(warnings[0].Detail, want[:(*testCaseInput.workers)-1])), "withdrawn producer remained in warning: %+v", warnings)
	}
	if len(warnings) != 0 {
		assert.Equal(
			t,
			warnings[0].Message,
			"conflicting producers; using "+owners[(*testCaseInput.workers)-2].String(),
		)
	}
	for _, owner := range owners {
		accept(
			t,
			s,
			owner,
			&Emission{
				Children:  []Child{winner},
				Manifests: testutil.DocumentOccurrences([]map[string]any{winner.Doc}),
				Complete:  true,
			},
		)
	}
	check(winner, owners[(*testCaseInput.workers)-1])
	{
		warnings := s.Warnings()
		assert.FatalIf(t, len(warnings) != 0, "equal content warned: %+v", warnings)
	}
}

type testRecordEmittedNeverEmittedCoverageWithdrawalSimpleInputs struct {
	seedRetained *func(t *testing.T, s *Store, id manifest.NamedResource)
	fixture      *func(t *testing.T, kName string, lName string, reverse bool) (*Store, manifest.NamedResource, manifest.NamedResource, Child)
	checkHidden  *func(t *testing.T, s *Store, id manifest.NamedResource)
}
