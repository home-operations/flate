package store

import (
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/internal/testutil/assert"
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

func TestRecordEmitted_ConflictingProducers_GeneratedSync(t *testing.T) {
	t.Parallel()
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reverse=%t", reverse), func(t *testing.T) {
			t.Parallel()
			testGeneratedSyncConflict(t, reverse)
		})
	}
}

func testGeneratedSyncConflict(t *testing.T, reverse bool) {
	t.Helper()
	s := New()
	generator := &manifest.RawObject{
		APIVersion: manifest.FluxOperatorDomain + "/v1", Kind: "FluxInstance", Name: "instance", Namespace: "apps",
	}
	authored := &manifest.Kustomization{Name: "authored", Namespace: "apps"}
	syncRoot := &manifest.Kustomization{Name: "sync", Namespace: "apps"}
	s.AddObject(generator)
	s.AddObject(authored)
	accept(t, s, generator.Named(), &Emission{
		Children: []Child{{Obj: syncRoot, Generated: true, VisibleGeneration: true}},
		Complete: true,
	})
	producers := []manifest.BaseManifest{authored, syncRoot}
	if reverse {
		slices.Reverse(producers)
	}
	children := make(map[manifest.NamedResource]Child)
	for _, producer := range producers {
		id := producer.Named()
		docs := testutil.DocumentOccurrences([]map[string]any{{
			"apiVersion": "monitoring.coreos.com/v1",
			"kind":       "PrometheusRule",
			"metadata":   map[string]any{"namespace": "apps", "name": "shared"},
		}})
		producer.EmissionPolicy().Stamp(docs)
		parsed, err := manifest.ParseDoc(docs[0].Document, manifest.ParseDocOptions{})
		assert.NoError(t, err)
		child := Child{Obj: parsed.Object, Doc: parsed.Document}
		children[id] = child
		_, _, output := s.GetEmission(id)
		output.Children, output.Manifests, output.Complete = []Child{child}, docs, true
		output.Fingerprint = manifest.Fingerprint(docs)
		accept(t, s, id, &output)
		output.Finish()
	}
	winner := children[syncRoot.Named()]
	id := winner.Obj.Named()
	assert.Equal(t, semanticEqual(children[authored.Named()], winner), true)
	checkEffectiveObject(t, s, id, &winner)
	assert.Equal(t, s.EmissionOwner(id), syncRoot.Named())
	assessment := s.AssessProducers()
	assert.Diff(t, assessment.GeneratedOverlaps, map[manifest.NamedResource]map[manifest.NamedResource]struct{}{
		authored.Named(): {id: {}},
		syncRoot.Named(): {id: {}},
	})
	assert.Diff(t, assessment.Warnings, []manifest.Warning{{
		Resource: id, Subject: manifest.DiagnosticFor(id, winner.Obj), Category: manifest.WarnConflictingProducers,
		Message: "conflicting producers; using " + syncRoot.Named().String(), Count: 1,
		Detail:  []string{authored.Named().String(), syncRoot.Named().String()},
		Related: []manifest.NamedResource{authored.Named(), syncRoot.Named(), syncRoot.Named()},
	}})

	s.AddObject(&manifest.Kustomization{Name: syncRoot.Name, Namespace: syncRoot.Namespace})
	_, _, successor := s.GetEmission(syncRoot.Named())
	assert.Equal(t, successor.syncOwnerKeys, [2]string{})
	assert.Equal(t, successor.previous.syncOwnerKeys, syncRoot.EmissionPolicy().OwnerKeys)
	assert.Diff(t, successor.previous.documentOwners, []manifest.NamedResource{id})
	assert.Diff(t, s.AssessProducers(), assessment)
	accept(t, s, syncRoot.Named(), &successor)
	successor.Finish()
	s.UpdateFailure(syncRoot.Named(), manifest.NewError(manifest.ErrInput, nil, "successor failed"))
	assert.Diff(t, s.AssessProducers(), assessment)
	checkEffectiveObject(t, s, id, &winner)

	_, _, completed := s.GetEmission(syncRoot.Named())
	previous := completed.previous
	completed.Fingerprint, completed.Complete = previous.Fingerprint, true
	assert.FatalIf(t, completed.Fingerprint == "", "missing accepted fingerprint")
	accept(t, s, syncRoot.Named(), &completed)
	completed.Finish()

	_, _, current := s.GetEmission(syncRoot.Named())
	defer current.Finish()
	assert.FatalIf(t, current.previous == nil, "completed successor has no accepted output")
	assert.Equal(t, current.previous.syncOwnerKeys, [2]string{})
	assert.Equal(t, current.previous.Fingerprint, previous.Fingerprint)
	assert.Diff(t, current.Manifests, previous.Manifests)
	assert.Diff(t, s.AssessProducers(), ProducerAssessment{})
	checkEffectiveObject(t, s, id, &winner)

	accept(t, s, syncRoot.Named(), &Emission{Complete: true})
	survivor := children[authored.Named()]
	checkEffectiveObject(t, s, id, &survivor)
	assert.Equal(t, s.EmissionOwner(id), authored.Named())
	assert.Diff(t, s.AssessProducers(), ProducerAssessment{})
}

func TestRecordEmitted_SharedChildArrival(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/reverse=%t", workers, reverse), func(t *testing.T) {
				t.Parallel()

				testRecordEmittedSharedChildArrivalCase(t, workers, reverse)
			})
		}
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
		checkEffectiveObject(t, s, id, &child)
	}
	check(winner, owners[(*testCaseInput.workers)-1])
	for i, owner := range owners {
		_, _, output := s.GetEmission(owner)
		assert.Diff(t, testutil.DocumentMaps(output.Manifests), []map[string]any{children[i].Doc})
		output.Finish()
	}
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
	assert.Diff(t, warnings, []manifest.Warning{{
		Resource: id,
		Subject:  manifest.DiagnosticFor(id, winner.Obj),
		Related:  append(slices.Clone(owners), owners[(*testCaseInput.workers)-1]),
		Category: manifest.WarnConflictingProducers,
		Message:  "conflicting producers; using " + owners[(*testCaseInput.workers)-1].String(),
		Detail:   want,
		Count:    1,
	}})
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

func testRecordEmittedSharedChildArrivalCase(t *testing.T, workers int, reverse bool) {
	t.Helper()

	s := New()
	child := &manifest.HelmChartSource{Name: "shared", Namespace: "apps", Chart: "app-template"}
	var arrivals atomic.Int64
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == child.Named() {
			arrivals.Add(1)
		}
	}, false)
	defer unsub()
	var wg sync.WaitGroup
	release := make([]chan struct{}, workers)
	children := make([]*manifest.HelmChartSource, workers)
	arrived, complete := make(chan struct{}), make(chan struct{})
	for i := range workers {
		owner := ownerID(strconv.Itoa(i))
		s.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
		release[i] = make(chan struct{})
		wg.Go(func() {
			<-release[i]
			synthetic := *child
			children[i] = &synthetic
			s.RecordEmitted(owner, &Emission{Children: []Child{{Obj: &synthetic}}}, nil)
			arrived <- struct{}{}
			<-complete
			output := emissionChild(owner.Name, "complete")
			s.RecordEmitted(owner, &Emission{
				Children:  []Child{{Obj: &synthetic}, output},
				Manifests: testutil.DocumentOccurrences([]map[string]any{output.Doc}),
				Complete:  true,
			}, nil)
		})
	}
	for i := range workers {
		if reverse {
			i = workers - 1 - i
		}
		close(release[i])
		<-arrived
	}
	assert.Equal(t, s.EmissionOwner(child.Named()), ownerID(strconv.Itoa(workers-1)))
	close(complete)
	wg.Wait()
	assert.Equal(t, arrivals.Load(), int64(1))
	assert.Equal[manifest.BaseManifest](t, s.GetObject(child.Named()), children[workers-1])
	assert.Equal(t, s.EmissionOwner(child.Named()), ownerID(strconv.Itoa(workers-1)))
	assert.Diff(t, s.Warnings(), []manifest.Warning(nil))
	for i := range workers {
		owner := ownerID(strconv.Itoa(i))
		_, _, output := s.GetEmission(owner)
		assert.Diff(t, testutil.DocumentMaps(output.Manifests), []map[string]any{
			emissionChild(owner.Name, "complete").Doc,
		})
		output.Finish()
	}
}
