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

func TestRecordEmitted_ConflictingProducers_SyntheticOwners(t *testing.T) {
	t.Parallel()
	for _, static := range []bool{false, true} {
		t.Run(fmt.Sprintf("static=%t", static), func(t *testing.T) {
			t.Parallel()
			testRecordEmittedConflictingProducersSyntheticOwnersCase(
				t,
				&static,
			)
		},
		)
	}
}

func testRecordEmittedConflictingProducersSyntheticOwnersCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	s := New()
	chart := &manifest.HelmChartSource{Name: "chart", Namespace: "apps"}
	first := chart.OCIRepository(&manifest.HelmRepository{URL: "oci://registry.example/charts"})
	second := first.WithNamespace("other")
	s.AddObject(first)
	s.AddObject(second)
	target := &manifest.ConfigMap{Name: "target", Namespace: "apps"}
	if *testCaseInput {
		s.AddObject(target)
	}
	owners := []manifest.NamedResource{first.Named(), second.Named()}
	for i, owner := range owners {
		child := &manifest.ConfigMap{
			Name:      target.Name,
			Namespace: target.Namespace,
			Data:      map[string]any{"value": strconv.Itoa(i)},
		}
		s.RecordEmitted(owner, &Emission{Children: []Child{{Obj: child}}, Complete: true}, nil)
	}
	if *testCaseInput {
		s.AddObject(s.GetObject(target.Named()))
	}
	warnings := s.Warnings()
	assert.FatalIf(t, len(warnings) != 1, "warnings=%+v", warnings)
	warning := warnings[0]
	assert.FatalIf(
		t,
		warning.Count != 1 || warning.Resource != target.Named() || warning.Subject.Resource != target.Named() ||
			len(warning.Detail) != 2 ||
			warning.Detail[0] != chart.Named().String() ||
			warning.Detail[1] != chart.Named().String(),
		"conflict display=%+v",
		warning,
	)
	assert.FatalIf(
		t,
		len(warning.Related) < 2 || warning.Related[0] != owners[0] || warning.Related[1] != owners[1],
		"raw contributors=%v",
		warning.Related,
	)
	winner := chart.Named().String()
	if *testCaseInput {
		winner = "static object"
	}
	assert.FatalIf(t, warning.Message != "conflicting producers; using "+winner, "winner=%+v", warning)
}

func TestRecordEmitted_AbsentProducerStaysPrivate(t *testing.T) {
	t.Parallel()

	s := New()
	p := ownerID("absent")
	child := emissionChild("private", "value")
	s.AddObject(child.Obj)
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		child.Obj.Named(): {Productions: []manifest.Production{{Owner: p}}},
	}})
	s.SetArtifact(child.Obj.Named(), &SourceArtifact{Key: "static"})
	s.UpdateStatus(child.Obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
	assert.FatalIf(
		t,
		s.GetArtifact(child.Obj.Named(), ArtifactContent) == nil,
		"%s",
		"fixture has no retained static artifact",
	)
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
	s.UpdateFailure(p, manifest.NewError(manifest.ErrInput, nil, "private failure"))
	s.SetArtifact(p, &SourceArtifact{Key: "private"})
	assert.Equal(t, s.EmissionOwner(ownerID("missing")), manifest.NamedResource{})
	for _, id := range []manifest.NamedResource{p, child.Obj.Named()} {
		assert.Equal(t, s.EmissionOwner(id), manifest.NamedResource{})
		assert.FatalIf(
			t,
			s.GetObject(id) != nil || s.GetArtifact(id, ArtifactContent) != nil,
			"%s",
			"absent producer leaked live object or artifact",
		)
		{
			_, ok := s.GetStatus(id)
			assert.FatalIf(t, ok, "%s", "absent producer leaked terminal state")
		}
		var observation Observation
		obj, observation := s.Snapshot(id, true)
		info := observation.StatusInfo
		conditions := observation.Conditions
		assert.FatalIf(
			t,
			obj != nil || info != (StatusInfo{}) || len(conditions) != 0,
			"%s",
			"absent producer leaked snapshot",
		)
		{
			obj, _, attempt := s.GetEmission(
				id,
			)
			assert.FatalIf(t, obj != nil || len(attempt.Children) != 0 ||
				len(attempt.Manifests) != 0, "%s", "absent producer leaked accepted output")
		}
	}
	assert.FatalIf(
		t,
		len(s.FailedResources()) != 0 || len(s.ListObjects("", "")) != 0,
		"%s",
		"absent producer leaked inventory",
	)
	for event := EventObjectAdded; event <= EventArtifactUpdated; event++ {
		s.AddListener(
			event,
			func(manifest.NamedResource, any) { t.Error("absent producer leaked listener replay") },
			true,
		)()
	}
	s.AddObject(&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace})
	assert.FatalIf(
		t,
		s.GetObject(child.Obj.Named()) == nil,
		"%s",
		"external anchor failed to activate privately completed output",
	)
	assert.FatalIf(
		t,
		s.GetArtifact(child.Obj.Named(), ArtifactContent) != nil,
		"%s",
		"external anchor activated a retained static artifact",
	)
	{
		_, ok := s.GetStatus(child.Obj.Named())
		assert.FatalIf(t, ok, "%s", "external anchor activated retained static Ready")
	}
	assert.FatalIf(
		t,
		!s.shardFor(child.Obj.Named()).cells[child.Obj.Named()].retired,
		"%s",
		"actual membership restored shadowed static authority",
	)
	{
		_, ok := s.GetStatus(p)
		assert.FatalIf(t, ok, "%s", "external anchor activated private Ready/Failed state")
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
	assert.FatalIf(
		t,
		len(warnings) != 1 || warnings[0].Resource != id || warnings[0].Category != "ConflictingProducers" ||
			!slices.Equal(warnings[0].Detail, want) ||
			warnings[0].Count != 1,
		"conflict warning: %+v",
		warnings,
	)
	assert.Equal(t, warnings[0].Message, "conflicting producers; using "+owners[(*testCaseInput.workers)-1].String())
	accept(t, s, owners[(*testCaseInput.workers)-1], &Emission{Complete: true})
	check(children[(*testCaseInput.workers)-2], owners[(*testCaseInput.workers)-2])
	warnings = s.Warnings()
	assert.FatalIf(t, (*testCaseInput.workers) == 2 && len(warnings) != 0 || (*testCaseInput.workers) ==
		4 &&
		(len(warnings) != 1 || !slices.Equal(warnings[0].Detail, want[:(*testCaseInput.workers)-1])), "withdrawn producer remained in warning: %+v", warnings)
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
