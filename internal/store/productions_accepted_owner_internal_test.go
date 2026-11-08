package store

import (
	"fmt"
	"slices"
	"strconv"
	"sync"
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
	facts := s.ReferenceFacts(child.Obj.Named(), nil, manifest.NamedResource{})
	assert.Diff(t, facts.Data, child.Obj.(manifest.Data))
	assert.FatalIf(t, !facts.Accepted, "accepted selected facts=%+v", facts)
	want := []manifest.Production{proof, {Owner: q}}
	accept(t, s, q, &Emission{Complete: true})
	{
		got := snapshot
		assert.FatalIf(t, !slices.Equal(got, want), "frozen/runtime snapshot changed: got %v, want %v", got, want)
	}
	assert.Diff(t, facts.Productions, want)
	parent, ok := s.RenderedParent(child.Obj.Named(), nil)
	assert.FatalIf(t, !ok || parent != p, "frozen surviving parent=%v/%t, want %v/true", parent, ok, p)
	assert.FatalIf(
		t,
		!slices.Equal(backing, original),
		"query mutated borrowed discovery evidence: got %v, want %v",
		backing,
		original,
	)
	owned := child
	owned.Productions = []manifest.Production{proof}
	s.RecordEmitted(p, &Emission{Children: []Child{owned}}, nil)
	borrowed := s.Productions(child.Obj.Named(), nil)
	assert.FatalIf(
		t,
		len(borrowed) != 1 || &borrowed[0] != &backing[0],
		"%s",
		"duplicate accepted evidence copied frozen backing",
	)

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
		assert.FatalIf(
			t,
			!slices.Equal(evidence, original),
			"query reordered borrowed discovery evidence: got %v, want %v",
			evidence,
			original,
		)
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
	assert.FatalIf(
		t,
		s.GetObject(child.Obj.Named()) != child.Obj || s.GetObject(p) != nil,
		"%s",
		"fixture needs an independently rooted child and hidden owner",
	)
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
	assert.FatalIf(
		t,
		!ok || parent != wantParent,
		"filtered parent=%v/%t, want %v/true",
		parent,
		ok,
		wantParent,
	)
	assert.FatalIf(
		t,
		!slices.Equal(proofs, original),
		"filter mutated borrowed discovery evidence: got %v, want %v",
		proofs,
		original,
	)
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
	assert.FatalIf(
		t,
		!slices.Equal(got, want),
		"captured productions changed after concurrent replacement/withdrawal: got %v, want %v",
		got,
		want,
	)
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
	assert.FatalIf(t, len(got) != want || !slices.Contains(got, proof) ||
		!slices.ContainsFunc(
			got,
			func(v manifest.Production) bool { return v.Owner == p },
		), "accepted owner lost or redundant thin evidence added: %v", got)
	parent, ok := s.RenderedParent(child.Obj.Named(), nil)
	assert.FatalIf(
		t,
		ok == (*testCaseInput) || (!(*testCaseInput) && parent != p),
		"authored parent=%v/%t, own evidence=%t",
		parent,
		ok, (*testCaseInput),
	)
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
