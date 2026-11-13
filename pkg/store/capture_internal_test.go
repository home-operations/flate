package store

import (
	"fmt"
	"runtime"
	"slices"
	"sync"
	"testing"
	"weak"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCapture_ProducerRecordLifetime(t *testing.T) {
	t.Parallel()
	for _, payload := range []bool{false, true} {
		t.Run(fmt.Sprintf("payload=%t", payload), func(t *testing.T) {
			t.Parallel()
			testCaptureProducerRecordLifetimeCase(t, &payload)
		},
		)
	}
}

func TestReadRegistration_CollectionLabelReplacement(t *testing.T) {
	t.Parallel()
	s := New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	provider := &manifest.ResourceSetInputProvider{
		Name:      "provider",
		Namespace: "apps",
		Labels:    map[string]string{"selected": "false"},
	}
	publish := func(obj *manifest.ResourceSetInputProvider) {
		s.RecordEmitted(producer.Named(), &Emission{Children: []Child{{Obj: obj}}, Complete: true}, nil)
	}
	publish(provider)
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	objects := reads.ListObjects(provider.Named().Group, provider.Named().Kind)
	assert.Equal(t, len(objects), 1)
	assert.Equal(t, objects[0].(*manifest.ResourceSetInputProvider).Labels["selected"], "false")
	reads.Accept(AssessmentReconciled)
	sh := s.shardFor(provider.Named())
	sh.mu.RLock()
	before := sh.byName[kindID{provider.Named().Group, provider.Named().Kind}].generation
	sh.mu.RUnlock()
	changed := *provider
	changed.Labels = map[string]string{"selected": "true"}
	publish(&changed)
	sh.mu.RLock()
	after := sh.byName[kindID{provider.Named().Group, provider.Named().Kind}].generation
	sh.mu.RUnlock()
	assert.FatalIf(t, after <= before, "%s", "same-identity label publication bypassed collection replacement")
	assert.Equal(t, reads.Valid(), false)
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.ID()})
	assert.Equal(t, reads.Valid(), false)
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	objects = s.ListObjects(provider.Named().Group, provider.Named().Kind)
	assert.Equal(t, objects[0].(*manifest.ResourceSetInputProvider).Labels["selected"], "true")
}

func TestReadRegistration_RawLabelDocuments(t *testing.T) {
	t.Parallel()
	labelDoc := func(value string) map[string]any {
		return map[string]any{"metadata": map[string]any{"labels": map[string]any{"app": value}}}
	}
	for _, tc := range []struct {
		name         string
		before, next map[string]any
		changed      bool
	}{
		{"nil to authoritative empty", nil, map[string]any{}, true},
		{"authoritative empty to nil", map[string]any{}, nil, true},
		{"nil to matching document", nil, labelDoc("same"), false},
		{"matching document to nil", labelDoc("same"), nil, false},
		{"changed document labels", labelDoc("same"), labelDoc("other"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := New()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			raw := &manifest.RawObject{
				Kind:       "Widget",
				APIVersion: "example.test/v1",
				Name:       "app",
				Namespace:  "apps",
				Spec:       labelDoc("same"),
			}
			s.AddObject(producer)
			s.RecordEmitted(
				producer.Named(),
				&Emission{Children: []Child{{Obj: raw, Doc: tc.before}}, Complete: true},
				nil,
			)
			id := raw.Named()
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			defer reads.Retire()
			assert.Equal(t, len(reads.ListObjects(id.Group, id.Kind)), 1)
			sh := s.shardFor(id)
			sh.mu.RLock()
			before := sh.byName[kindID{id.Group, id.Kind}].generation
			sh.mu.RUnlock()
			s.RecordEmitted(
				producer.Named(),
				&Emission{Children: []Child{{Obj: raw, Doc: tc.next}}, Complete: true},
				nil,
			)
			sh.mu.RLock()
			after := sh.byName[kindID{id.Group, id.Kind}].generation
			sh.mu.RUnlock()
			assert.Equal(t, after > before, tc.changed)
			assert.Equal(t, reads.Valid(), !tc.changed)
			assert.Diff(t, raw.Spec, labelDoc("same"))
		})
	}
}

func TestReadRegistration_CrossShardLostWakeup(t *testing.T) {
	t.Parallel()
	for _, payload := range []bool{false, true} {
		for _, phase := range []string{"before-link", "after-link", "after-validation"} {
			t.Run(fmt.Sprintf("payload=%t/%s", payload, phase), func(t *testing.T) {
				t.Parallel()
				testReadRegistrationCrossShardLostWakeupCase(
					t,
					&testReadRegistrationCrossShardLostWakeupCaseInputs{payload: &payload, phase: &phase},
				)
			},
			)
		}
	}
}

func TestDerivedCapture_UnselectedContributorStableCommit(t *testing.T) {
	t.Parallel()
	s := New()
	a, z := ownerID("a"), ownerID("z")
	s.AddObjects(
		[]manifest.BaseManifest{
			&manifest.Kustomization{Name: a.Name, Namespace: a.Namespace},
			&manifest.Kustomization{Name: z.Name, Namespace: z.Namespace},
		},
	)
	child := emissionChild("shared", "first")
	s.RecordEmitted(a, &Emission{Children: []Child{child}, Complete: true}, nil)
	s.RecordEmitted(z, &Emission{Children: []Child{child}, Complete: true}, nil)
	var reads ReadSet
	reads.Init(s, ownerID("reader"), make([]Input, 4))
	assert.Equal(t, reads.GetObject(child.Obj.Named()), child.Obj)
	_ = reads.Productions(child.Obj.Named(), nil)
	before := headsForTest(s, child.Obj.Named())
	s.RecordEmitted(a, &Emission{Children: []Child{emissionChild("shared", "second")}, Complete: true}, nil)
	assert.Equal(t, headsForTest(s, child.Obj.Named()).object, before.object)
	assert.Equal(t, s.GetObject(child.Obj.Named()), child.Obj)
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
	assert.FatalIf(t, reads.Valid(), "%s", "unselected contributor's old record remained valid")
	reads.Retire()
	for range s.Sweep() {
	}
}

func TestDerivedCapture_UnselectedContributorReplacement(t *testing.T) {
	t.Parallel()
	for _, invalidated := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "valid_predecessor", true: "invalid_predecessor"}[invalidated],
			func(t *testing.T) {
				t.Parallel()
				s := New()
				a, z := ownerID("a"), ownerID("z")
				input := emissionChild("input", "old")
				s.AddObjects(
					[]manifest.BaseManifest{
						&manifest.Kustomization{Name: a.Name, Namespace: a.Namespace},
						&manifest.Kustomization{Name: z.Name, Namespace: z.Namespace},
						input.Obj,
					},
				)
				var previous, content, contributors ReadSet
				previous.Init(s, a, nil)
				previous.GetObject(input.Obj.Named())
				content.Init(s, ownerID("content"), nil)
				contributors.Init(s, ownerID("contributors"), nil)
				defer func() {
					previous.Retire()
					content.Retire()
					contributors.Retire()
					for range s.Sweep() {
					}
				}()
				child := emissionChild("shared", "first")
				_, _, initial := previous.GetEmission(a)
				initial.Children, initial.Complete = []Child{child}, true
				s.RecordEmitted(a, &initial, nil)
				initial.Finish()
				s.RecordEmitted(z, &Emission{Children: []Child{child}, Complete: true}, nil)
				assert.Equal(t, content.GetObject(child.Obj.Named()), child.Obj)
				assert.Diff(
					t,
					contributors.Productions(child.Obj.Named(), nil),
					[]manifest.Production{{Owner: a}, {Owner: z}},
				)
				before := headsForTest(s, child.Obj.Named())
				if invalidated {
					s.AddObject(emissionChild("input", "new").Obj)
				}
				s.RecordEmitted(
					a,
					&Emission{Children: []Child{child, emissionChild("other", "new")}, Complete: true},
					nil,
				)
				assert.Equal(t, headsForTest(s, child.Obj.Named()).object, before.object)
				assert.Equal(t, s.GetObject(child.Obj.Named()), child.Obj)
				assert.Equal(t, content.Valid(), true)
				assert.Equal(t, contributors.Valid(), false)
			},
		)
	}
}

func TestCapture_StableSegmentLifetime(t *testing.T) {
	t.Parallel()
	t.Run(
		"named collection and readiness",
		func(t *testing.T) { t.Parallel(); testCaptureStableSegmentLifetimeNamedCollectionAndReadiness(t) },
	)
	s := New()
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	first := reads.input(ownerID("a"))
	for i := range 129 {
		reads.GetObject(ownerID(string(rune('a' + i))))
	}
	assert.FatalIf(t, reads.input(ownerID("a")) != first, "%s", "unreserved growth replaced the first capture")
	for i := 128; i >= 0; i-- {
		id := ownerID(string(rune('a' + i)))
		captured := reads.input(id)
		reads.GetObject(id)
		assert.FatalIf(t, reads.input(id) != captured, "%s", "repeated lookup replaced an unreserved capture")
	}
	assert.FatalIf(t, !reads.Valid(), "%s", "segment growth changed original registrations")
	target := ownerID("a")
	s.AddObject(&manifest.Kustomization{Name: target.Name, Namespace: target.Namespace})
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
	reads.Retire()
	for range s.Sweep() {
	}
	for _, sh := range s.shards {
		sh.mu.RLock()
		assert.ErrorIf(
			t,
			sh.absence.first.Load() != nil || sh.absence.watch != nil || sh.absence.pending.Load(),
			"%s",
			"retired absence remains reachable from the shard",
		)
		for _, cell := range sh.cells {
			assert.ErrorIf(t, cell.links != nil &&
				(cell.links.first.Load() != nil || cell.links.detached.Load() || cell.links.pending.Load()), "%s", "retired segment remains reachable from an authority")
		}
		sh.mu.RUnlock()
	}
	var successor ReadSet
	successor.Init(s, ownerID("successor"), nil)
	successor.GetObject(target)
	old := reads.input(target).object.head
	current := successor.input(target).object.head
	assert.FatalIf(
		t,
		old != &s.shardFor(target).absence || current != s.shardFor(target).cells[target].links,
		"%s",
		"successor did not capture the admitted authority",
	)
	published := make(chan struct{})
	go func() {
		s.AddObject(&manifest.Kustomization{Name: target.Name, Namespace: target.Namespace, Path: "new"})
		close(published)
	}()
	<-published
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{successor.id})
	successor.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
}

func TestReadRegistration_RetirementUnderSupersession(t *testing.T) {
	t.Parallel()
	s := New()
	id := ownerID("input")
	var previous, successor ReadSet
	previous.Init(s, ownerID("old"), nil)
	successor.Init(s, ownerID("new"), nil)
	previous.GetArtifact(id, ArtifactContent|ArtifactRevision|ArtifactOrigin)
	successor.GetArtifact(id, ArtifactContent|ArtifactRevision|ArtifactOrigin)
	var ready sync.WaitGroup
	ready.Add(2)
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	go func() { ready.Done(); <-start; previous.Retire(); done <- struct{}{} }()
	go func() {
		ready.Done()
		<-start
		s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
		done <- struct{}{}
	}()
	ready.Wait()
	close(start)
	<-done
	<-done
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{successor.id})
	successor.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
}

func TestReadRegistration_ActivationUnderReadLock(t *testing.T) {
	t.Parallel()
	for _, facet := range []string{"object", "condition", "artifact", "bucket"} {
		for _, present := range []bool{false, true} {
			name := "absent"
			if present {
				name = "present"
			}
			t.Run(facet+"/"+name, func(t *testing.T) {
				t.Parallel()
				testReadRegistrationActivationUnderReadLockCase(
					t,
					&testReadRegistrationActivationUnderReadLockCaseInputs{facet: &facet, present: &present},
				)
			},
			)
		}
	}
}

func TestReadRegistration_RepeatedFacetPreservesFirstStamp(t *testing.T) {
	t.Parallel()
	for _, conditions := range []bool{false, true} {
		t.Run(fmt.Sprintf("snapshot/conditions=%t", conditions), func(t *testing.T) {
			t.Parallel()
			s := New()
			obj := &manifest.Kustomization{Name: "input", Namespace: "apps"}
			s.AddObject(obj)
			s.UpdateStatus(obj.Named(), StatusInfo{Status: StatusPending, Reason: "", Message: "waiting"})
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			defer reads.Retire()
			captured, first := reads.Snapshot(obj.Named(), conditions)
			assert.Equal(t, captured, manifest.BaseManifest(obj))
			assert.Equal(t, first.Statusful, true)
			status, exists := reads.GetStatus(obj.Named())
			assert.Equal(t, exists, true)
			assert.Equal(t, status, first.StatusInfo)
			s.UpdateStatus(obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: "ready"})
			captured, again := reads.Snapshot(obj.Named(), conditions)
			assert.Equal(t, captured, manifest.BaseManifest(obj))
			assert.Equal(t, again.Statusful, first.Statusful)
			assert.Equal(t, again.StatusInfo, first.StatusInfo)
			assert.Diff(t, again.Conditions, first.Conditions)
			status, exists = reads.GetStatus(obj.Named())
			assert.Equal(t, exists, true)
			assert.Equal(t, status, first.StatusInfo)
			assert.FatalIf(t, reads.Valid(), "%s", "readiness replacement left the captured snapshot valid")
		})
	}
	s := New()
	var reads ReadSet
	id := ownerID("input")
	reads.Init(s, ownerID("reader"), nil)
	assert.Equal(t, reads.GetObject(id), manifest.BaseManifest(nil))
	s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	assert.Equal(t, reads.GetObject(id), manifest.BaseManifest(nil))
	assert.FatalIf(t, reads.Valid(), "%s", "a later lookup replaced consumed absence")
	reads.Retire()
	for range s.Sweep() {
	}
}

func TestReadRegistration_ReclamationOnlyAndCutoff(t *testing.T) {
	t.Parallel()
	s := New()
	id := ownerID("input")
	version := func(path string) *manifest.Kustomization {
		return &manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: path}
	}
	s.AddObject(version("zero"))
	var previous, successor, fresh ReadSet
	previous.Init(s, ownerID("previous"), nil)
	successor.Init(s, ownerID("successor"), nil)
	fresh.Init(s, ownerID("fresh"), nil)
	previous.GetObject(id)
	s.AddObject(version("one"))
	successor.GetObject(id)
	detached, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for invalid := range s.Sweep() {
			assert.ErrorIf(t, invalid != previous.id, "first cutoff invalidated %s, want %s", invalid, previous.id)
			close(detached)
			<-resume
		}
	}()
	<-detached
	previous.Retire()
	s.AddObject(version("two"))
	fresh.GetObject(id)
	close(resume)
	<-done
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{successor.id})
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	assert.FatalIf(t, !fresh.Valid(), "%s", "reattachment invalidated a new live registration")
	generation := headsForTest(s, id).object
	successor.Retire()
	fresh.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	assert.Equal(t, headsForTest(s, id).object, generation)
	sh := s.shardFor(id)
	sh.mu.RLock()
	head := sh.cells[id].authority
	sh.mu.RUnlock()
	assert.FatalIf(t, head.links != nil &&
		(head.links.first.Load() != nil || head.links.pending.Load() || head.links.detached.Load()), "%s", "reclamation left a live or pending registration")
}

func TestReadRegistration_AbsenceDoesNotAdmitObjects(t *testing.T) {
	t.Parallel()
	s := New()
	var reads [2]ReadSet
	var done sync.WaitGroup
	done.Add(len(reads))
	for i := range reads {
		go func() {
			defer done.Done()
			reads[i].Init(s, ownerID(string(rune('a'+i))), nil)
			reads[i].GetObject(ownerID("missing"))
		}()
	}
	done.Wait()
	for _, sh := range s.shards {
		sh.mu.RLock()
		count := len(sh.cells)
		sh.mu.RUnlock()
		assert.Equal(t, count, 0)
	}
	assert.Equal(t, len(s.ListObjects("", "")), 0)
	s.AddObject(&manifest.Kustomization{Name: "unrelated", Namespace: "apps"})
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	for i := range reads {
		assert.FatalIf(t, !reads[i].Valid(), "%s", "unrelated admission invalidated consumed absence")
	}
	s.AddObject(&manifest.Kustomization{Name: "missing", Namespace: "apps"})
	invalid := slices.Collect(s.Sweep())
	slices.SortFunc(invalid, manifest.NamedResource.Compare)
	assert.Diff(t, invalid, []manifest.NamedResource{reads[0].id, reads[1].id})
	for i := range reads {
		reads[i].Retire()
	}
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
}

func testCaptureProducerRecordLifetimeCase(t *testing.T, testCaseInput *bool,
) {
	t.Helper()

	s := New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	child := emissionChild("input", "old")
	var firstPayload map[manifest.NamedResource]*SourceArtifact
	if *testCaseInput {
		firstPayload = map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): {Key: "first"}}
	}
	s.RecordEmitted(producer.Named(), &Emission{Children: []Child{child}, Complete: false}, firstPayload)
	previous := weak.Make(s.GetArtifact(producer.Named(), ArtifactContent).(*Emission))
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	assert.Equal(t, reads.GetObject(child.Obj.Named()), child.Obj)
	if *testCaseInput {
		assert.Equal(
			t,
			reads.GetArtifact(child.Obj.Named(), ArtifactContent),
			Artifact(firstPayload[child.Obj.Named()]),
		)
		assert.FatalIf(t, !reads.Valid(), "%s", "current payload reader was invalid before supersession")
	}
	var nextPayload map[manifest.NamedResource]*SourceArtifact
	if *testCaseInput {
		nextPayload = map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): {Key: "next"}}
	}
	s.RecordEmitted(
		producer.Named(),
		&Emission{Children: []Child{emissionChild("input", "new")}, Complete: true},
		nextPayload,
	)
	runtime.GC()
	assert.FatalIf(
		t,
		previous.Value() != nil,
		"%s",
		"captured child retained the superseded producer's execution history",
	)
	assert.Equal(t, reads.GetObject(child.Obj.Named()), child.Obj)
	assert.Equal(t, reads.Valid(), false)
	runtime.KeepAlive(s)
	runtime.KeepAlive(&reads)
}

type testReadRegistrationCrossShardLostWakeupCaseInputs struct {
	payload *bool
	phase   *string
}

func testReadRegistrationCrossShardLostWakeupCase(
	t *testing.T,
	testCaseInput *testReadRegistrationCrossShardLostWakeupCaseInputs,
) {
	t.Helper()

	s := New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	old := emissionChild("input", "old")
	var firstPayload map[manifest.NamedResource]*SourceArtifact
	if *testCaseInput.payload {
		firstPayload = map[manifest.NamedResource]*SourceArtifact{old.Obj.Named(): {Key: "first"}}
	}
	s.RecordEmitted(producer.Named(), &Emission{Children: []Child{old}, Complete: false}, firstPayload)
	if *testCaseInput.payload {
		s.AddObject(old.Obj)
	}
	assert.FatalIf(
		t,
		kindShardIndex(producer.Named().Kind) == kindShardIndex(old.Obj.Named().Kind),
		"%s",
		"fixture must span distinct shards",
	)
	var reads ReadSet
	reads.Init(s, ownerID("reader"), make([]Input, 3))
	if (*testCaseInput.phase) == "before-link" {
		sh := s.shardFor(old.Obj.Named())
		sh.mu.RLock()
		var view capture
		sh.captureLocked(old.Obj.Named(), new(sh.cells[old.Obj.Named()]), &view)
		sh.mu.RUnlock()
		if *testCaseInput.payload {
			reads.sample(old.Obj.Named(), view.payloadOwner, view.payloadOwner.current.Load(), false)
		} else {
			reads.sample(old.Obj.Named(), view.owner, view.record, false)
		}
	} else {
		assert.Equal(t, reads.GetObject(old.Obj.Named()), old.Obj)
		if *testCaseInput.payload {
			reads.GetArtifact(old.Obj.Named(), ArtifactContent)
		}
		assert.FatalIf(
			t,
			(*testCaseInput.phase) == "after-validation" && !reads.Valid(),
			"%s",
			"unchanged captured input was invalid",
		)
	}
	published := make(chan struct{})
	go func() {
		var nextPayload map[manifest.NamedResource]*SourceArtifact
		if *testCaseInput.payload {
			nextPayload = map[manifest.NamedResource]*SourceArtifact{old.Obj.Named(): {Key: "next"}}
		}
		s.RecordEmitted(
			producer.Named(),
			&Emission{Children: []Child{emissionChild("input", "new")}, Complete: true},
			nextPayload,
		)
		close(published)
	}()
	<-published
	if (*testCaseInput.phase) == "before-link" {
		reads.activateProducers()
		assert.FatalIf(t, reads.Valid(), "%s", "activation certified an obsolete producer record")
	} else {
		assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
		assert.FatalIf(t, reads.Valid(), "%s", "publication was lost after activation")
	}
	reads.Retire()
	for range s.Sweep() {
	}
}

func testCaptureStableSegmentLifetimeNamedCollectionAndReadiness(t *testing.T) {
	t.Helper()

	s := New()
	target := ownerID("provider-00")
	for i := range 64 {
		obj := &manifest.Kustomization{Name: fmt.Sprintf("provider-%02d", i), Namespace: target.Namespace}
		s.AddObject(obj)
	}
	s.UpdateStatus(target, StatusInfo{Status: StatusReady, Reason: "", Message: "initial"})
	var reads ReadSet
	reads.Init(s, ownerID("collection-reader"), nil)
	defer reads.Close()
	first := reads.GetObject(target)
	captured := reads.input(target)
	objectCell := captured.cell(objectFacet)
	objects := reads.ListObjects(target.Group, target.Kind)
	assert.FatalIf(t, len(objects) != 64 || reads.GetObject(target) != first || reads.input(target) != captured ||
		captured.cell(objectFacet) != objectCell, "%s", "collection replaced the named capture or its registration")
	_, observed := reads.Snapshot(target, true)
	assert.FatalIf(
		t,
		observed.Status != StatusReady || len(observed.Conditions) != 1,
		"readiness after collection: %+v",
		observed,
	)
	conditionCell := captured.cell(conditionFacet)
	generation := conditionCell.generation.Load()
	bulkID := ownerID("provider-01")
	bulk := reads.input(bulkID)
	_, bulkBefore := reads.Snapshot(bulkID, true)
	bulkCell := bulk.cell(conditionFacet)
	for i := range 65 {
		reads.GetObject(ownerID(fmt.Sprintf("absent-%02d", i)))
	}
	occurrences := 0
	for input := reads.inputs; input != nil; input = input.next {
		if input.Ref.NamedResource == target {
			occurrences++
		}
	}
	assert.FatalIf(
		t,
		occurrences != 1 || reads.input(target) != captured || captured.cell(conditionFacet) != conditionCell ||
			!reads.Valid(),
		"%s",
		"growth duplicated or moved the captured identity or sparse facet",
	)
	assert.FatalIf(
		t,
		reads.input(bulkID) != bulk || bulk.cell(conditionFacet) != bulkCell,
		"%s",
		"growth moved the bulk capture's sparse facet",
	)
	s.UpdateStatus(bulkID, StatusInfo{Status: StatusReady, Reason: "", Message: "admitted"})
	assert.FatalIf(t, bulkCell.current(), "%s", "bulk readiness admission left its consumed absence valid")
	_, bulkAgain := reads.Snapshot(bulkID, true)
	assert.Equal(t, bulkAgain.Status, bulkBefore.Status)
	assert.Diff(t, bulkAgain.Conditions, bulkBefore.Conditions)
	s.UpdateStatus(target, StatusInfo{Status: StatusFailed, Reason: "", Message: "replacement"})
	_, again := reads.Snapshot(target, true)
	assert.FatalIf(
		t,
		again.Status != StatusReady || conditionCell.generation.Load() != generation,
		"%s",
		"repeated readiness lost its first stamp or invalidation",
	)
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
	assert.FatalIf(t, reads.Valid(), "%s", "readiness replacement did not invalidate the captured input")
}

type testReadRegistrationActivationUnderReadLockCaseInputs struct {
	facet   *string
	present *bool
}

func testReadRegistrationActivationUnderReadLockCase(
	t *testing.T,
	testCaseInput *testReadRegistrationActivationUnderReadLockCaseInputs,
) {
	t.Helper()

	s := New()
	id := ownerID("input")
	publish := func() {
		switch *testCaseInput.facet {
		case "object", "bucket":
			s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "new"})
		case "condition":
			s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: "new"})
		case "artifact":
			s.SetArtifact(id, &SourceArtifact{Revision: "new"})
		}
	}
	if *testCaseInput.present {
		publish()
	}
	var reads [2]ReadSet
	var ready, done sync.WaitGroup
	ready.Add(len(reads))
	done.Add(len(reads))
	start := make(chan struct{})
	sh := s.shardFor(id)
	sh.mu.RLock()
	for i := range reads {
		go func() {
			defer done.Done()
			reads[i].Init(s, ownerID(string(rune('a'+i))), nil)
			ready.Done()
			<-start
			switch *testCaseInput.facet {
			case "object":
				reads[i].GetObject(id)
			case "condition":
				_, _ = reads[i].Snapshot(id, true)
			case "artifact":
				reads[i].GetArtifact(id, ArtifactContent|ArtifactRevision)
			case "bucket":
				reads[i].ListObjects(id.Group, id.Kind)
			}
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	sh.mu.RUnlock()
	if *testCaseInput.present {
		switch *testCaseInput.facet {
		case "object", "bucket":
			s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "changed"})
		case "condition":
			s.UpdateStatus(id, StatusInfo{Status: StatusFailed, Reason: "", Message: "changed"})
		case "artifact":
			s.SetArtifact(id, &SourceArtifact{Revision: "changed"})
		}
	} else {
		publish()
	}
	invalid := slices.Collect(s.Sweep())
	slices.SortFunc(invalid, manifest.NamedResource.Compare)
	assert.Diff(t, invalid, []manifest.NamedResource{reads[0].id, reads[1].id})
	for i := range reads {
		reads[i].Retire()
	}
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
}

func TestReadRegistration_ProducerActivation(t *testing.T) {
	t.Parallel()
	for _, settled := range []bool{false, true} {
		t.Run(fmt.Sprintf("settled=%t", settled), func(t *testing.T) {
			t.Parallel()
			s := New()
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			t.Cleanup(reads.Retire)
			var selected capture
			for _, name := range []string{"first", "second", "third"} {
				producer := ownerID(name)
				s.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
				child := emissionChild(name, "old")
				s.RecordEmitted(producer, &Emission{Children: []Child{child}, Complete: settled}, nil)
				sh := s.shardFor(child.Obj.Named())
				sh.mu.RLock()
				var view capture
				sh.captureLocked(child.Obj.Named(), new(sh.cells[child.Obj.Named()]), &view)
				sh.mu.RUnlock()
				reads.sample(child.Obj.Named(), view.owner, view.record, false)
				reads.sample(child.Obj.Named(), view.owner, view.record, false)
				if name == "second" {
					selected = view
				}
			}
			reads.activateProducers()
			assert.Equal(t, reads.Valid(), true)
			reads.sample(emissionChild("second", "old").Obj.Named(), selected.owner, selected.record, true)
			reads.activateProducers()
			assert.Equal(t, reads.Valid(), true)
			s.RecordEmitted(
				ownerID("second"),
				&Emission{Children: []Child{emissionChild("second", "new")}, Complete: true},
				nil,
			)
			assert.Equal(t, reads.Valid(), false)
		})
	}
}
