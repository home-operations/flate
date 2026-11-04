package store

import (
	"fmt"
	"testing"
	"testing/fstest"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestStore_QueryAndExecutionAllocations(t *testing.T) {
	t.Run("Store_ProductionsQueryAllocations", func(t *testing.T) {
		s := New()
		id := ownerID("known")
		s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
			id: {Productions: []manifest.Production{{Owner: ownerID("parent")}}},
		}})
		for _, target := range []manifest.NamedResource{ownerID("missing"), id} {
			t.Run(target.Name, func(t *testing.T) {
				{
					got := testing.AllocsPerRun(10, func() {
						s.Productions(target, nil)
					})
					assert.FatalIf(t, got != 0, "production query allocations=%v, want 0", got)
				}
				{
					got := testing.AllocsPerRun(10, func() {
						s.ReferenceFacts(target, nil, manifest.NamedResource{})
					})
					assert.FatalIf(t, got != 0, "reference facts allocations=%v, want 0", got)
				}
			})
		}
		obj := &manifest.Kustomization{Name: "reader", Namespace: "apps"}
		s.AddObject(obj)
		var reads ReadSet
		measure := func(productions bool) float64 {
			return testing.AllocsPerRun(10, func() {
				reads.Init(s, obj.Named(), nil)
				reads.GetObject(obj.Named())
				if productions {
					reads.Productions(obj.Named(), nil)
				}
				reads.Retire()
				for range s.Sweep() {
				}
			})
		}
		baseline := measure(false)
		{
			got := measure(true)
			assert.FatalIf(
				t,
				got != baseline,
				"empty captured production query allocations=%v, capture alone=%v",
				got,
				baseline,
			)
		}
	})
	t.Run("BeginExecution_Allocations", func(t *testing.T) {
		s := New()
		s.SetWorkTree(manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead}, (&manifest.WorkRecorder{}).Scope())
		owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
		s.AddObject(owner)
		var reads [2]ReadSet
		for i := range reads {
			reads[i].Init(s, owner.Named(), nil)
			reads[i].GetObject(owner.Named())
		}
		reads[0].BeginExecution(nil)
		i := 0
		allocations := testing.AllocsPerRun(1, func() {
			reads[i].BeginExecution(nil)
			reads[i].WorkScope().Admission().Add(&manifest.WorkCounts{OwnerPrepares: 1})
			i++
		})
		assert.Equal(t, allocations, float64(0))
	})
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
func TestStore_RenderedParentAllocations(t *testing.T) {
	for _, name := range []string{"thin", "frozen", "bridge", "missing"} {
		t.Run(name, func(t *testing.T) {
			testStoreRenderedParentAllocationsCase(t, &name)
		},
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

	assert.FatalIf(
		t,
		allocs != 0 || ok != ((*testCaseInput) != "missing") || (ok && parent != owner),
		"parent=%v/%t allocations=%v",
		parent,
		ok,
		allocs,
	)

	if (*testCaseInput) == "thin" {
		var obj manifest.BaseManifest
		allocs = testing.AllocsPerRun(10, func() { obj = s.GetObject(id) })

		assert.FatalIf(
			t,
			allocs != 0 || obj != child.Obj,
			"effective object=%v allocations=%v",
			obj,
			allocs,
		)

		allocs = testing.AllocsPerRun(10, func() { parent = s.EmissionOwner(id) })

		assert.FatalIf(t, allocs != 0 || parent != owner, "content owner=%v allocations=%v", parent, allocs)
	}
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestStore_AddObject_TypedDocumentFreeAllocations(t *testing.T) {
	s := New()
	versions := [2]*manifest.ResourceSetInputProvider{
		{Name: "provider", Namespace: "ns", Labels: map[string]string{"revision": "a"}},
		{Name: "provider", Namespace: "ns", Labels: map[string]string{"revision": "b"}},
	}
	s.AddObject(versions[0])
	i := 0
	got := testing.AllocsPerRun(100, func() {
		i++
		s.AddObject(versions[i%2])
	})

	assert.FatalIf(t, got > 6, "changed typed object allocations=%v, want at most 6", got)

	assert.FatalIf(
		t,
		s.GetObject(versions[0].Named()) != versions[i%2],
		"%v",
		"changed typed object was not admitted",
	)
}

//nolint:paralleltest // The test calls a helper that changes process-global state.
//nolint:paralleltest // This test changes process-global state.
func TestStore_PublicationAndWorkAllocations(t *testing.T) {
	t.Run("RecordEmitted_ReplacementAllocations", func(t *testing.T) {
		s := New()
		owner := &manifest.Kustomization{Name: "owner", Namespace: "ns"}
		s.AddObject(owner)
		versions := [2]Emission{{Complete: true}, {Complete: true}}
		for version := range versions {
			for i := range 100 {
				versions[version].Children = append(
					versions[version].Children,
					Child{
						Obj: &manifest.RawObject{
							Kind:       "Deployment",
							APIVersion: "apps/v1",
							Name:       fmt.Sprintf("app-%d-%d", version, i),
							Namespace:  "ns",
						},
					},
				)
			}
		}
		s.RecordEmitted(owner.Named(), &versions[0], nil)
		round := 0
		got := testing.AllocsPerRun(20, func() {
			round++
			s.RecordEmitted(owner.Named(), &versions[round%2], nil)
		})

		assert.FatalIf(t, got > 60, "100-output replacement allocations=%v, want at most 60", got)
	})
	t.Run("WorkSnapshot_LifetimeAllocations", func(t *testing.T) {
		lifetime := func(record bool) *Store {
			s := New()
			var root manifest.WorkRecorder
			s.SetWorkTree(manifest.TreeID{Cluster: "cluster", Side: manifest.TreeHead}, root.Scope())
			for _, name := range []string{
				"one", "two", "three", "four", "five", "six", "seven", "eight",
				"nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen",
			} {
				owner := &manifest.Kustomization{Name: name, Namespace: "apps"}
				s.AddObject(owner)
				execution := new(ReadSet)
				execution.Init(s, owner.Named(), nil)
				execution.GetObject(owner.Named())
				if record {
					execution.BeginExecution(nil)
					for _, input := range []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"} {
						execution.Work().
							Scope().
							Record(manifest.WorkEvaluation, input).
							Add(&manifest.WorkCounts{Attempts: 1, Completed: 1})
					}
					scope := execution.Work().Scope()
					scope.Record(manifest.WorkEvaluation, "a").Add(&manifest.WorkCounts{Attempts: 1, Failures: 1})
				}
				execution.Retire()
			}
			return s
		}
		baseline := testing.AllocsPerRun(10, func() { lifetime(false) })
		withStorage := testing.AllocsPerRun(10, func() { lifetime(true) })
		stores := [2]*Store{lifetime(true), lifetime(true)}
		index := 0
		var detachedWork manifest.WorkSummary
		output := testing.AllocsPerRun(1, func() { detachedWork = stores[index].WorkSnapshot(); index++ })
		t.Logf("counter-store growth allocations: %g", withStorage-baseline)
		assert.Equal(t, output, float64(1))
		assert.Equal(t, len(detachedWork), 16*10)
		assert.Equal(t, sumWork(detachedWork), manifest.WorkCounts{Attempts: 176, Completed: 160, Failures: 16})
	})

	t.Run("payload_lookup", func(t *testing.T) {
		s := New()
		owner := ownerID("producer")
		s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
		child := emissionChild("target", "same")
		target := child.Obj.Named()
		payload := &SourceArtifact{Kind: target.Kind, Key: "key", Content: "key", FS: fstest.MapFS{}}
		_, _, attempt := s.GetEmission(owner)
		defer attempt.Finish()
		attempt.Children = []Child{child}
		s.RecordEmitted(owner, &attempt, map[manifest.NamedResource]*SourceArtifact{target: payload})
		attempt.Complete = true
		s.RecordEmitted(owner, &attempt, nil)
		children := []Child{child}
		allocations := testing.AllocsPerRun(10, func() {
			attempt.Children = children
			s.RecordEmitted(owner, &attempt, nil)
		})
		assert.Equal(t, allocations, float64(1))
		assert.Equal(t, s.GetArtifact(target, ArtifactContent) == payload, true)
	})
}
