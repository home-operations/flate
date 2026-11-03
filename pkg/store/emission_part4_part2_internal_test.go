package store

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func testStoreProductionsScopedFileBridgeCase4(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	s := New()
	p, q, r := ownerID("p"), ownerID("q"), ownerID("r")
	for _, owner := range []manifest.NamedResource{p, q, r} {
		s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	}
	target := emissionChild("bridged", "value").Obj.Named()
	bare := target
	bare.Namespace = ""
	source := manifest.BootstrapSourceID
	other := source
	other.Name = "other"
	proofs := []manifest.Production{
		{Owner: r, Source: other},
		{Owner: p, Source: source, Authored: bare, Path: "first.yaml"},
		{Owner: r, Source: other, Path: "other.yaml"},
		{Owner: q, Source: source, Path: "matching.yaml"},
		{Source: source, Path: "unbound.yaml"},
		{Owner: r, Source: source, Path: "later.yaml"},
	}
	s.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{bare: {Productions: proofs}},
		},
	)
	want := []manifest.Production{proofs[1]}
	if *testCaseInput {
		_, _, next := s.GetEmission(p)
		next.Complete = true
		s.RecordEmitted(p, &next, nil)
		next.Finish()
		want = nil
	}
	slices.SortFunc(want, compareProduction)
	assert.Diff(t, s.Productions(target, nil), want)
	parent, ok := s.RenderedParent(target, nil)
	{
		assert.FatalIf(
			t,
			ok || parent != (manifest.NamedResource{}),
			"authored bridge parent=%v/%t",
			parent,
			ok,
		)
	}
}

func testStoreRenderedParentAllocationsCase(t *testing.T, testCaseInput *string,
) {
	t.Helper()
	s := New()
	owner := ownerID("parent")
	s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	child := emissionChild("child", "value")
	id := child.Obj.Named()
	switch *testCaseInput {
	case "thin":
		s.RecordEmitted(owner, &Emission{Children: []Child{child}, Complete: true}, nil)
	case "frozen", "bridge":
		key := id
		proof := manifest.Production{Owner: owner}
		if (*testCaseInput) == "bridge" {
			key.Namespace = ""
			proof.Path = "child.yaml"
		}
		s.SetDiscoveryIndex(
			&manifest.DiscoveryIndex{
				Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					key: {Productions: []manifest.Production{proof}},
				},
			},
		)
	}
	var parent manifest.NamedResource
	var ok bool
	allocs := testing.AllocsPerRun(10, func() { parent, ok = s.RenderedParent(id, nil) })
	{
		assert.FatalIf(
			t,
			allocs != 0 || ok != ((*testCaseInput) != "missing") || (ok && parent != owner),
			"parent=%v/%t allocations=%v",
			parent,
			ok,
			allocs,
		)
	}
	if (*testCaseInput) == "thin" {
		var obj manifest.BaseManifest
		allocs = testing.AllocsPerRun(10, func() { obj = s.GetObject(id) })
		{
			assert.FatalIf(
				t,
				allocs != 0 || obj != child.Obj,
				"effective object=%v allocations=%v",
				obj,
				allocs,
			)
		}
		allocs = testing.AllocsPerRun(10, func() { parent = s.EmissionOwner(id) })
		{
			assert.FatalIf(t, allocs != 0 || parent != owner, "content owner=%v allocations=%v", parent, allocs)
		}
	}
}
