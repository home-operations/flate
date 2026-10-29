package store

import (
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"weak"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestCapture_ProducerRecordLifetime(t *testing.T) {
	t.Parallel()
	for _, payload := range []bool{false, true} {
		t.Run(fmt.Sprintf("payload=%t", payload), func(t *testing.T) {
			t.Parallel()
			s := New()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			s.AddObject(producer)
			child := emissionChild("input", "old")
			var firstPayload map[manifest.NamedResource]*SourceArtifact
			if payload {
				firstPayload = map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): {Key: "first"}}
			}
			s.RecordEmitted(producer.Named(), &Emission{Children: []Child{child}, Complete: false}, firstPayload)
			previous := weak.Make(s.GetArtifact(producer.Named(), ArtifactContent).(*Emission))
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			defer reads.Retire()
			assert.Equal(t, reads.GetObject(child.Obj.Named()), child.Obj)
			if payload {
				assert.Equal(t, reads.GetArtifact(child.Obj.Named(), ArtifactContent), Artifact(firstPayload[child.Obj.Named()]))
				if !reads.Valid() {
					t.Fatal("current payload reader was invalid before supersession")
				}
			}
			var nextPayload map[manifest.NamedResource]*SourceArtifact
			if payload {
				nextPayload = map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): {Key: "next"}}
			}
			s.RecordEmitted(producer.Named(), &Emission{Children: []Child{emissionChild("input", "new")}, Complete: true}, nextPayload)
			runtime.GC()
			if previous.Value() != nil {
				t.Fatal("captured child retained the superseded producer's execution history")
			}
			assert.Equal(t, reads.GetObject(child.Obj.Named()), child.Obj)
			assert.Equal(t, reads.Valid(), false)
			runtime.KeepAlive(s)
			runtime.KeepAlive(&reads)
		})
	}
}

func TestReadRegistration_CollectionLabelReplacement(t *testing.T) {
	t.Parallel()
	s := New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	provider := &manifest.ResourceSetInputProvider{Name: "provider", Namespace: "apps", Labels: map[string]string{"selected": "false"}}
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
	if after <= before {
		t.Fatal("same-identity label publication bypassed collection replacement")
	}
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.ID()})
	assert.Equal(t, reads.Valid(), false)
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
			raw := &manifest.RawObject{Kind: "Widget", APIVersion: "example.test/v1", Name: "app", Namespace: "apps", Spec: labelDoc("same")}
			s.AddObject(producer)
			s.RecordEmitted(producer.Named(), &Emission{Children: []Child{{Obj: raw, Doc: tc.before}}, Complete: true}, nil)
			id := raw.Named()
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			defer reads.Retire()
			assert.Equal(t, len(reads.ListObjects(id.Group, id.Kind)), 1)
			sh := s.shardFor(id)
			sh.mu.RLock()
			before := sh.byName[kindID{id.Group, id.Kind}].generation
			sh.mu.RUnlock()
			s.RecordEmitted(producer.Named(), &Emission{Children: []Child{{Obj: raw, Doc: tc.next}}, Complete: true}, nil)
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
				s := New()
				producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
				s.AddObject(producer)
				old := emissionChild("input", "old")
				var firstPayload map[manifest.NamedResource]*SourceArtifact
				if payload {
					firstPayload = map[manifest.NamedResource]*SourceArtifact{old.Obj.Named(): {Key: "first"}}
				}
				s.RecordEmitted(producer.Named(), &Emission{Children: []Child{old}, Complete: false}, firstPayload)
				if payload {
					s.AddObject(old.Obj)
				}
				if kindShardIndex(producer.Named().Kind) == kindShardIndex(old.Obj.Named().Kind) {
					t.Fatal("fixture must span distinct shards")
				}
				var reads ReadSet
				reads.Init(s, ownerID("reader"), make([]Input, 3))
				if phase == "before-link" {
					sh := s.shardFor(old.Obj.Named())
					sh.mu.RLock()
					var view capture
					sh.captureLocked(old.Obj.Named(), sh.cells[old.Obj.Named()], &view)
					sh.mu.RUnlock()
					if payload {
						reads.sample(old.Obj.Named(), view.payloadOwner, view.payloadOwner.current.Load(), false)
					} else {
						reads.sample(old.Obj.Named(), view.owner, view.record, false)
					}
				} else {
					assert.Equal(t, reads.GetObject(old.Obj.Named()), old.Obj)
					if payload {
						reads.GetArtifact(old.Obj.Named(), ArtifactContent)
					}
					if phase == "after-validation" && !reads.Valid() {
						t.Fatal("unchanged captured input was invalid")
					}
				}
				published := make(chan struct{})
				go func() {
					var nextPayload map[manifest.NamedResource]*SourceArtifact
					if payload {
						nextPayload = map[manifest.NamedResource]*SourceArtifact{old.Obj.Named(): {Key: "next"}}
					}
					s.RecordEmitted(producer.Named(), &Emission{Children: []Child{emissionChild("input", "new")}, Complete: true}, nextPayload)
					close(published)
				}()
				<-published
				if phase == "before-link" {
					reads.activateProducers()
					if reads.Valid() {
						t.Fatal("activation certified an obsolete producer record")
					}
				} else {
					assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
					if reads.Valid() {
						t.Fatal("publication was lost after activation")
					}
				}
				reads.Retire()
				for range s.Sweep() {
				}
			})
		}
	}
}

func TestDerivedCapture_UnselectedContributorStableCommit(t *testing.T) {
	t.Parallel()
	s := New()
	a, z := ownerID("a"), ownerID("z")
	s.AddObjects([]manifest.BaseManifest{&manifest.Kustomization{Name: a.Name, Namespace: a.Namespace}, &manifest.Kustomization{Name: z.Name, Namespace: z.Namespace}})
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
	if reads.Valid() {
		t.Fatal("unselected contributor's old record remained valid")
	}
	reads.Retire()
	for range s.Sweep() {
	}
}

func TestDerivedCapture_UnselectedContributorReplacement(t *testing.T) {
	t.Parallel()
	for _, invalidated := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid_predecessor", true: "invalid_predecessor"}[invalidated], func(t *testing.T) {
			t.Parallel()
			s := New()
			a, z := ownerID("a"), ownerID("z")
			input := emissionChild("input", "old")
			s.AddObjects([]manifest.BaseManifest{&manifest.Kustomization{Name: a.Name, Namespace: a.Namespace}, &manifest.Kustomization{Name: z.Name, Namespace: z.Namespace}, input.Obj})
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
			assert.Diff(t, contributors.Productions(child.Obj.Named(), nil), []manifest.Production{{Owner: a}, {Owner: z}})
			before := headsForTest(s, child.Obj.Named())
			if invalidated {
				s.AddObject(emissionChild("input", "new").Obj)
			}
			s.RecordEmitted(a, &Emission{Children: []Child{child, emissionChild("other", "new")}, Complete: true}, nil)
			assert.Equal(t, headsForTest(s, child.Obj.Named()).object, before.object)
			assert.Equal(t, s.GetObject(child.Obj.Named()), child.Obj)
			assert.Equal(t, content.Valid(), true)
			assert.Equal(t, contributors.Valid(), false)
		})
	}
}

func TestCapture_StableSegmentLifetime(t *testing.T) {
	t.Parallel()
	t.Run("named collection and readiness", func(t *testing.T) {
		t.Parallel()
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
		if len(objects) != 64 || reads.GetObject(target) != first || reads.input(target) != captured || captured.cell(objectFacet) != objectCell {
			t.Fatal("collection replaced the named capture or its registration")
		}
		_, observed := reads.Snapshot(target, true)
		if observed.Status != StatusReady || len(observed.Conditions) != 1 {
			t.Fatalf("readiness after collection: %+v", observed)
		}
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
		if occurrences != 1 || reads.input(target) != captured || captured.cell(conditionFacet) != conditionCell || !reads.Valid() {
			t.Fatal("growth duplicated or moved the captured identity or sparse facet")
		}
		if reads.input(bulkID) != bulk || bulk.cell(conditionFacet) != bulkCell {
			t.Fatal("growth moved the bulk capture's sparse facet")
		}
		s.UpdateStatus(bulkID, StatusInfo{Status: StatusReady, Reason: "", Message: "admitted"})
		if bulkCell.current() {
			t.Fatal("bulk readiness admission left its consumed absence valid")
		}
		_, bulkAgain := reads.Snapshot(bulkID, true)
		assert.Equal(t, bulkAgain.Status, bulkBefore.Status)
		assert.Diff(t, bulkAgain.Conditions, bulkBefore.Conditions)
		s.UpdateStatus(target, StatusInfo{Status: StatusFailed, Reason: "", Message: "replacement"})
		_, again := reads.Snapshot(target, true)
		if again.Status != StatusReady || conditionCell.generation.Load() != generation {
			t.Fatal("repeated readiness lost its first stamp or invalidation")
		}
		assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
		if reads.Valid() {
			t.Fatal("readiness replacement did not invalidate the captured input")
		}
	})
	s := New()
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	for i := range 33 {
		reads.GetObject(ownerID(string(rune('a' + i))))
	}
	if !reads.Valid() {
		t.Fatal("segment growth changed original registrations")
	}
	target := ownerID("a")
	s.AddObject(&manifest.Kustomization{Name: target.Name, Namespace: target.Namespace})
	assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
	reads.Retire()
	for range s.Sweep() {
	}
	for _, sh := range s.shards {
		sh.mu.RLock()
		if sh.absence.first.Load() != nil || sh.absence.watch != nil || sh.absence.pending.Load() {
			t.Error("retired absence remains reachable from the shard")
		}
		for _, cell := range sh.cells {
			if cell.links != nil && (cell.links.first.Load() != nil || cell.links.detached.Load() || cell.links.pending.Load()) {
				t.Error("retired segment remains reachable from an authority")
			}
		}
		sh.mu.RUnlock()
	}
	var successor ReadSet
	successor.Init(s, ownerID("successor"), nil)
	successor.GetObject(target)
	old := reads.input(target).object.head
	current := successor.input(target).object.head
	if old != &s.shardFor(target).absence || current != s.shardFor(target).cells[target].links {
		t.Fatal("successor did not capture the admitted authority")
	}
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
				s := New()
				id := ownerID("input")
				publish := func() {
					switch facet {
					case "object", "bucket":
						s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace, Path: "new"})
					case "condition":
						s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: "new"})
					case "artifact":
						s.SetArtifact(id, &SourceArtifact{Revision: "new"})
					}
				}
				if present {
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
						switch facet {
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
				if present {
					switch facet {
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
			})
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
			if reads.Valid() {
				t.Fatal("readiness replacement left the captured snapshot valid")
			}
		})
	}
	s := New()
	var reads ReadSet
	id := ownerID("input")
	reads.Init(s, ownerID("reader"), nil)
	assert.Equal(t, reads.GetObject(id), manifest.BaseManifest(nil))
	s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
	assert.Equal(t, reads.GetObject(id), manifest.BaseManifest(nil))
	if reads.Valid() {
		t.Fatal("a later lookup replaced consumed absence")
	}
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
			if invalid != previous.id {
				t.Errorf("first cutoff invalidated %s, want %s", invalid, previous.id)
			}
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
	if !fresh.Valid() {
		t.Fatal("reattachment invalidated a new live registration")
	}
	generation := headsForTest(s, id).object
	successor.Retire()
	fresh.Retire()
	assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
	assert.Equal(t, headsForTest(s, id).object, generation)
	sh := s.shardFor(id)
	sh.mu.RLock()
	head := sh.cells[id].authority
	sh.mu.RUnlock()
	if head.links != nil && (head.links.first.Load() != nil || head.links.pending.Load() || head.links.detached.Load()) {
		t.Fatal("reclamation left a live or pending registration")
	}
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
		if !reads[i].Valid() {
			t.Fatal("unrelated admission invalidated consumed absence")
		}
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

func TestReadRegistration_AbsentAdmission(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"batch", "emission", "hidden-producer"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			s := New()
			child := emissionChild("input", "new")
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			if mode == "emission" {
				s.AddObject(producer)
			}
			id := child.Obj.Named()
			if mode == "hidden-producer" {
				id = producer.Named()
			}
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			reads.GetObject(id)
			done := make(chan struct{})
			go func() {
				defer close(done)
				if mode == "batch" {
					s.AddObjects([]manifest.BaseManifest{child.Obj})
				} else {
					s.RecordEmitted(producer.Named(), &Emission{Children: []Child{child}, Complete: true}, nil)
				}
			}()
			<-done
			assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.id})
			if reads.Valid() {
				t.Fatal("first authority admission left consumed absence valid")
			}
			reads.Retire()
			assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
		})
	}
}

func TestReadRegistration_AbsentDetachedAdmission(t *testing.T) {
	t.Parallel()
	for _, mask := range []ArtifactRead{0, ArtifactContent, ArtifactRevision, ArtifactOrigin, ArtifactContent | ArtifactRevision | ArtifactOrigin} {
		t.Run(fmt.Sprint(mask), func(t *testing.T) {
			t.Parallel()
			s := New()
			read := func(r *ReadSet, id manifest.NamedResource) {
				if mask == 0 {
					r.GetObject(id)
				} else {
					r.GetArtifact(id, mask)
				}
			}
			publish := func(id manifest.NamedResource) {
				if mask == 0 {
					s.AddObject(&manifest.Kustomization{Name: id.Name, Namespace: id.Namespace})
				} else {
					s.SetArtifact(id, &SourceArtifact{Revision: "present", Metadata: map[string]string{"org.opencontainers.image.revision": ""}})
				}
			}
			var first, later, retired, fresh ReadSet
			for _, reads := range []*ReadSet{&first, &later, &retired, &fresh} {
				reads.Init(s, ownerID("reader"), nil)
			}
			first.id, later.id = ownerID("first-reader"), ownerID("later-reader")
			firstID, laterID := ownerID("first-input"), ownerID("later-input")
			read(&later, laterID)
			read(&retired, ownerID("retired-input"))
			read(&first, firstID)
			retired.Retire()
			publish(firstID)
			detached, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				for id := range s.Sweep() {
					assert.Equal(t, id, first.id)
					close(detached)
					<-resume
				}
			}()
			<-detached
			first.Retire()
			read(&fresh, ownerID("still-absent"))
			publish(laterID)
			close(resume)
			<-done
			assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{later.id})
			if !fresh.Valid() {
				t.Fatal("new live absence registration was invalidated")
			}
			later.Retire()
			fresh.Retire()
			assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
			sh := s.shardFor(laterID)
			if sh.absence.first.Load() != nil || sh.absence.watch != nil || sh.absence.pending.Load() {
				t.Fatal("retired absence registrations survived reclamation")
			}
		})
	}
}

func TestReadRegistration_AbsenceExhaustionIsAtomic(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"object", "condition", "artifact", "revision", "origin", "union", "emission"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			s := New()
			obj := &manifest.ConfigMap{Name: "new", Namespace: "apps"}
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			s.AddObject(producer)
			var reads ReadSet
			reads.Init(s, producer.Named(), nil)
			switch operation {
			case "condition":
				reads.GetStatus(obj.Named())
			case "artifact", "revision", "origin", "union":
				reads.GetArtifact(obj.Named(), ArtifactContent|ArtifactRevision|ArtifactOrigin)
			default:
				reads.GetObject(obj.Named())
			}
			sh := s.shardFor(obj.Named())
			generation := ^uint64(0)
			if operation == "object" || operation == "emission" {
				generation--
			}
			if operation == "union" {
				generation -= 2
			}
			sh.absence.generation.Store(generation)
			panicked := false
			func() {
				defer func() { panicked = recover() != nil }()
				switch operation {
				case "object":
					s.AddObject(obj)
				case "condition":
					s.UpdateStatus(obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
				case "artifact":
					s.SetArtifact(obj.Named(), &SourceArtifact{})
				case "revision":
					s.SetArtifact(obj.Named(), &SourceArtifact{Revision: "next"})
				case "origin":
					s.SetArtifact(obj.Named(), &SourceArtifact{Metadata: map[string]string{"org.opencontainers.image.revision": ""}})
				case "union":
					s.SetArtifact(obj.Named(), &SourceArtifact{Revision: "next", Metadata: map[string]string{"org.opencontainers.image.revision": ""}})
				case "emission":
					s.RecordEmitted(producer.Named(), &Emission{Children: []Child{{Obj: obj}}, Complete: true}, nil)
				}
			}()
			if !panicked || sh.absence.generation.Load() != generation || !reads.Valid() {
				t.Fatalf("panic=%t generation=%d valid=%t", panicked, sh.absence.generation.Load(), reads.Valid())
			}
			if s.GetObject(obj.Named()) != nil || s.GetArtifact(obj.Named(), ArtifactContent) != nil {
				t.Fatal("exhaustion partially admitted target")
			}
			if _, ok := s.GetStatus(obj.Named()); ok {
				t.Fatal("exhaustion partially published status")
			}
			if s.GetArtifact(producer.Named(), ArtifactContent) != nil {
				t.Fatal("exhaustion partially published inventory")
			}
		})
	}
}

func TestProductions_ProjectedRawFamily(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                         string
		separateFile, separateSource bool
	}{
		{name: "shared file"}, {name: "different file", separateFile: true}, {name: "different source", separateSource: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New()
			raw := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings"}
			target := raw
			target.Namespace = "apps"
			first := testutil.KustomizationID("apps", "first")
			second := testutil.KustomizationID("other", "second")
			p := manifest.Production{Owner: first, Source: manifest.BootstrapSourceID, Authored: raw, File: "settings.yaml", Path: "/settings.yaml"}
			other := p
			other.Owner = second
			selected := p
			if tc.separateFile {
				selected.File, selected.Path = "another.yaml", "/another.yaml"
			}
			if tc.separateSource {
				selected.Source = manifest.NamedResource{Group: manifest.SourceDomain, Kind: manifest.KindGitRepository, Name: "foreign"}
			}
			index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw: {Productions: []manifest.Production{p, other}}, target: {Productions: []manifest.Production{selected}},
			}}
			st.SetDiscoveryIndex(index)
			var reads ReadSet
			reads.Init(st, testutil.KustomizationID("apps", "consumer"), nil)
			got := reads.Productions(target, index)
			want := []manifest.Production{p, other}
			if tc.separateFile || tc.separateSource {
				want = []manifest.Production{selected}
			}
			assert.Diff(t, got, want)
			assert.Diff(t, st.Productions(target, index), want)
		})
	}
}

func TestCapture_RawSupportExcludesReadiness(t *testing.T) {
	t.Parallel()
	st := New()
	owner := ownerID("producer")
	source := manifest.BootstrapSourceID
	id := manifest.NamedResource{Kind: manifest.KindConfigMap, Name: "settings", Namespace: "apps"}
	st.AddObject(&manifest.Kustomization{Name: owner.Name, Namespace: owner.Namespace})
	st.SetArtifact(source, &SourceArtifact{Key: "content"})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		id: {Productions: []manifest.Production{{Owner: owner, Source: source, Authored: id, File: "settings.yaml", Path: "/settings.yaml"}}},
	}})
	st.UpdateStatus(owner, StatusInfo{Status: StatusPending, Reason: "", Message: "pending"})
	var reads ReadSet
	reads.Init(st, ownerID("consumer"), nil)
	for range reads.Promotions(id, nil) {
	}
	reads.GetStatus(owner)
	reads.Admit(&manifest.ConfigMap{Name: id.Name, Namespace: id.Namespace, Data: map[string]any{"VALUE": "raw"}}, source, owner)
	st.UpdateStatus(owner, StatusInfo{Status: StatusFailed, Reason: "", Message: "render failed"})
	if reads.Valid() || st.GetObject(id) == nil {
		t.Fatal("readiness transition must invalidate its reader without withdrawing source bytes")
	}
	st.RecordEmitted(owner, &Emission{Complete: true}, nil)
	if st.GetObject(id) != nil {
		t.Fatal("producer replacement retained unsupported source bytes")
	}
}

func TestReadSet_MatchesMutableArtifact(t *testing.T) {
	t.Parallel()
	st := New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
	st.AddObject(source)
	artifact := &SourceArtifact{LocalPath: "input", FS: fstest.MapFS{}}
	st.SetArtifact(source.Named(), artifact)
	var before, current ReadSet
	before.Init(st, ownerID("consumer"), nil)
	before.GetArtifact(source.Named(), ArtifactContent)
	current.Init(st, ownerID("consumer"), nil)
	if !current.Matches(&before) {
		t.Fatal("the same mutable artifact was treated as different material")
	}
	st.SetArtifact(source.Named(), artifact.WithReads(&ReadSet{}))
	if !before.Valid() || !current.Matches(&before) {
		t.Fatal("refreshing registrations replaced the acquired mutable material")
	}
	st.SetArtifact(source.Named(), &SourceArtifact{LocalPath: artifact.LocalPath, FS: artifact.FS})
	var replacement ReadSet
	replacement.Init(st, ownerID("consumer"), nil)
	if replacement.Matches(&before) {
		t.Fatal("a new mutable artifact was inferred equal from its location")
	}
}

func TestReadSet_MatchesRawCollection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		value   string
		matches bool
	}{
		{"equal distinct content", "same", true},
		{"changed nested content", "changed", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first := &manifest.RawObject{Kind: "Widget", APIVersion: "example.test/v1", Name: "app", Spec: map[string]any{"items": []any{map[string]any{"value": "same"}}}}
			second := first.Clone()
			second.Spec["items"].([]any)[0].(map[string]any)["value"] = tc.value
			beforeStore, currentStore := New(), New()
			beforeStore.AddObject(first)
			currentStore.AddObject(second)
			var before, current ReadSet
			before.Init(beforeStore, ownerID("reader"), nil)
			current.Init(currentStore, ownerID("reader"), nil)
			defer before.Retire()
			defer current.Retire()
			id := first.Named()
			objects := before.ListObjects(id.Group, id.Kind)
			assert.Equal(t, len(objects), 1)
			assert.Equal(t, objects[0], manifest.BaseManifest(first))
			assert.Equal(t, current.Matches(&before), tc.matches)
			objects = current.ListObjects(id.Group, id.Kind)
			assert.Equal(t, len(objects), 1)
			assert.Equal(t, objects[0], manifest.BaseManifest(second))
			assert.Equal(t, objects[0] == first, false)
			assert.Diff(t, first.Spec, map[string]any{"items": []any{map[string]any{"value": "same"}}})
		})
	}
}

func TestReadSet_UnsettledProducerSnapshot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		known bool
	}{{"empty", false}, {"known", true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New()
			source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
			first := &manifest.Kustomization{Name: "first", Namespace: "apps"}
			second := &manifest.Kustomization{Name: "second", Namespace: "apps"}
			st.AddObjects([]manifest.BaseManifest{source, first, second})
			if tc.known {
				st.RecordEmitted(first.Named(), &Emission{Children: []Child{{Obj: source}}}, nil)
			}
			var reads ReadSet
			reads.Init(st, ownerID("reader"), nil)
			defer reads.Retire()
			before := reads.UnsettledProducers(source.Named())
			st.RecordEmitted(second.Named(), &Emission{Children: []Child{{Obj: source}}}, nil)
			if tc.known {
				st.RecordEmitted(first.Named(), &Emission{Children: []Child{{Obj: source}}, Complete: true}, nil)
			}
			if after := reads.UnsettledProducers(source.Named()); !slices.Equal(before, after) {
				t.Fatalf("availability query changed within one captured execution: before=%v after=%v", before, after)
			}
		})
	}
}

func TestRequire_RetainsPredicates(t *testing.T) {
	t.Parallel()
	for _, expressions := range [][]string{
		{"false", ""}, {"", "false"}, {"true", "false"},
		{"false", "true", "false", "", "true", "1 == 2", "1 == 2", ""},
	} {
		t.Run(strings.Join(expressions, "/"), func(t *testing.T) {
			t.Parallel()
			s := New()
			obj := &manifest.Kustomization{Name: "dependency", Namespace: "apps"}
			s.AddObject(obj)
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			defer reads.Retire()
			var want []Gate
			for _, expr := range expressions {
				ref := manifest.DependencyRef{NamedResource: obj.Named(), ReadyExpr: new(expr)}
				reads.Require(ref)
				if !slices.ContainsFunc(want, func(prior Gate) bool { return sameReadinessGate(prior.DependencyRef, ref) }) {
					want = append(want, Gate{DependencyRef: ref})
				}
			}
			assert.Diff(t, slices.Collect(reads.Gates()), want)
			assert.Diff(t, slices.Collect(reads.Required()), []manifest.NamedResource{obj.Named()})
			assert.Equal(t, reads.GetObject(obj.Named()), manifest.BaseManifest(obj))
			if reads.inputs != &reads.local || reads.inputs.next != nil {
				t.Fatal("predicates created multiple captured authorities for one identity")
			}
			for range reads.Gates() {
				break
			}
			got := 0
			for range reads.Gates() {
				got++
				if got == 2 {
					break
				}
			}
			assert.Equal(t, got, 2)
		})
	}
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprintf("facts/reverse=%t", reverse), func(t *testing.T) {
			t.Parallel()
			s := New()
			obj := &manifest.OCIRepository{Name: "source", Namespace: "apps"}
			s.AddObject(obj)
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			defer reads.Retire()
			refs := []manifest.DependencyRef{
				{NamedResource: obj.Named()},
				{NamedResource: obj.Named(), ReadyExpr: new("")},
				{NamedResource: obj.Named(), ReadyExpr: new("true")},
				{NamedResource: obj.Named(), ReadyExpr: new("true"), Artifact: true},
				{NamedResource: obj.Named(), ReadyExpr: new("true"), Artifact: true, UnknownArtifactRoot: true},
				{NamedResource: obj.Named(), ReadyExpr: new("true"), Artifact: true, ChartSource: true},
				{NamedResource: obj.Named(), ReadyExpr: new("true"), Artifact: true, ArtifactOnly: true},
			}
			if reverse {
				slices.Reverse(refs)
				reads.RequireMaterializer(obj.Named())
			}
			for _, ref := range refs {
				reads.Require(ref)
			}
			for _, ref := range refs {
				if ref.ReadyExpr != nil {
					ref.ReadyExpr = new(*ref.ReadyExpr)
				}
				reads.Require(ref)
			}
			reads.RequireMaterializer(obj.Named())
			want := make([]Gate, len(refs))
			for i, ref := range refs {
				want[i].DependencyRef = ref
			}
			materializer := Gate{NamedResource: obj.Named(), Materializer: true}
			if reverse {
				want = append([]Gate{materializer}, want...)
			} else {
				want = append(want, materializer)
			}
			assert.Diff(t, slices.Collect(reads.Gates()), want)
			assert.Diff(t, slices.Collect(reads.Required()), []manifest.NamedResource{obj.Named()})
			assert.Equal(t, reads.GetObject(obj.Named()), manifest.BaseManifest(obj))
			if reads.inputs != &reads.local || reads.inputs.next != nil {
				t.Fatal("gate facts created more than one captured authority")
			}
		})
	}

}

func TestRequire_ReleasesPredicates(t *testing.T) {
	t.Parallel()
	s := New()
	var reads ReadSet
	inputs := make([]Input, 1)
	for _, retirement := range []bool{false, true} {
		reads.Init(s, ownerID("reader"), inputs)
		for _, id := range []manifest.NamedResource{ownerID("first"), ownerID("second")} {
			reads.Require(manifest.DependencyRef{NamedResource: id, ReadyExpr: new("true")})
			reads.Require(manifest.DependencyRef{NamedResource: id, ReadyExpr: new("false")})
		}
		if retirement {
			reads.Retire()
			for input := reads.inputs; input != nil; input = input.next {
				if details := input.details.Load(); details != nil && details.gates != nil {
					t.Fatal("retirement retained overflow predicates")
				}
			}
		}
	}
	reads.Init(s, ownerID("reader"), inputs)
	assert.Equal(t, len(slices.Collect(reads.Gates())), 0)
	if reads.localDetails.gates != nil || inputs[0].details.Load() != nil {
		t.Fatal("reset retained overflow predicates")
	}
}

func TestRequire_CapturedIdentityDuringAdmission(t *testing.T) {
	t.Parallel()
	s := New()
	obj := &manifest.OCIRepository{Name: "source", Namespace: "apps"}
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	assert.Equal(t, reads.GetObject(obj.Named()), manifest.BaseManifest(nil))
	ref := manifest.DependencyRef{NamedResource: obj.Named(), ReadyExpr: new("true"), Artifact: true, UnknownArtifactRoot: true, ChartSource: true, ArtifactOnly: true}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Go(func() {
		<-start
		reads.Require(ref)
	})
	workers.Go(func() {
		<-start
		s.AddObject(obj)
	})
	close(start)
	workers.Wait()
	assert.Diff(t, slices.Collect(reads.Gates()), []Gate{{DependencyRef: ref}})
	assert.Equal(t, s.GetObject(obj.Named()), manifest.BaseManifest(obj))
	assert.Equal(t, reads.GetObject(obj.Named()), manifest.BaseManifest(nil))
}

func TestReadSet_DeclarationAuthority(t *testing.T) {
	t.Parallel()
	s := New()
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
	supplier := &manifest.Kustomization{Name: "supplier", Namespace: "apps"}
	raw := &manifest.OCIRepository{Name: "covered", Namespace: "apps", URL: "oci://same.example"}
	s.AddObjects([]manifest.BaseManifest{owner, supplier, raw})
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{raw.Named(): {Productions: []manifest.Production{{Owner: owner.Named()}}}}})
	var pending ReadSet
	pending.Init(s, ownerID("waiting"), nil)
	defer pending.Retire()
	if pending.GetDeclaration(raw.Named()) != nil || pending.GetObject(raw.Named()) != raw {
		t.Fatal("raw visibility supplied authority")
	}
	s.RecordEmitted(supplier.Named(), &Emission{Children: []Child{{Obj: raw}}, Complete: true}, nil)
	invalidated := slices.Collect(s.Sweep())
	if !slices.Contains(invalidated, pending.ID()) || pending.GetDeclaration(raw.Named()) != nil {
		t.Fatal("arrival failed to wake or changed the first captured view")
	}
	var accepted ReadSet
	accepted.Init(s, ownerID("accepted"), nil)
	defer accepted.Retire()
	if accepted.GetDeclaration(raw.Named()) != raw || accepted.Matches(&pending) {
		t.Fatal("same bytes concealed authority arrival")
	}
	s.RecordEmitted(supplier.Named(), &Emission{Complete: true}, nil)
	if accepted.Valid() || s.GetObject(raw.Named()) != raw || s.GetDeclaration(raw.Named()) != nil {
		t.Fatal("withdrawal lost raw identity or retained authority")
	}
	var withdrawn ReadSet
	withdrawn.Init(s, ownerID("withdrawn"), nil)
	defer withdrawn.Retire()
	if withdrawn.Matches(&accepted) || pending.Valid() {
		t.Fatal("same bytes concealed withdrawal or reused authority after ABA")
	}
	s.RecordEmitted(supplier.Named(), &Emission{Children: []Child{{Obj: raw}}, Complete: true}, nil)
	var supplied ReadSet
	supplied.Init(s, ownerID("supplied"), nil)
	defer supplied.Retire()
	supplied.GetDeclaration(raw.Named())
	s.AddObject(raw)
	if supplied.Valid() {
		t.Fatal("rooting failed to invalidate the supplier capture")
	}
	var rooted ReadSet
	rooted.Init(s, ownerID("rooted"), nil)
	defer rooted.Retire()
	if !rooted.Matches(&supplied) || rooted.GetDeclaration(raw.Named()) != raw {
		t.Fatal("equal effective content could not rebind rooted authority")
	}
}

func TestCapture_PayloadAuthority(t *testing.T) {
	t.Parallel()
	for _, static := range []bool{false, true} {
		t.Run(fmt.Sprintf("static=%t", static), func(t *testing.T) {
			t.Parallel()
			s := New()
			a, z := ownerID("a"), ownerID("z")
			s.AddObjects([]manifest.BaseManifest{&manifest.Kustomization{Name: a.Name, Namespace: a.Namespace}, &manifest.Kustomization{Name: z.Name, Namespace: z.Namespace}})
			child := emissionChild("input", "a")
			declaration := emissionChild("input", "z")
			target := child.Obj.Named()
			payload := &SourceArtifact{Key: "first", Digest: "first", FS: fstest.MapFS{"file": {Data: []byte("first")}}}
			s.RecordEmitted(a, &Emission{Children: []Child{child}, Complete: false}, map[manifest.NamedResource]*SourceArtifact{target: payload})
			s.RecordEmitted(z, &Emission{Children: []Child{declaration}, Complete: true}, nil)
			if static {
				s.AddObject(declaration.Obj)
			}
			var first, second ReadSet
			first.Init(s, ownerID("first-reader"), nil)
			defer first.Retire()
			assert.Equal(t, first.GetObject(target), declaration.Obj)
			assert.Equal(t, first.GetArtifact(target, ArtifactContent), Artifact(payload))
			if !first.Valid() {
				t.Fatal("capturing independent declaration and payload invalidated a current reader")
			}
			next := &SourceArtifact{Key: "next", Digest: "next", FS: fstest.MapFS{"file": {Data: []byte("next")}}}
			s.RecordEmitted(a, &Emission{Children: []Child{child}, Complete: true}, map[manifest.NamedResource]*SourceArtifact{target: next})
			assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{first.ID()})
			if first.Valid() {
				t.Fatal("payload-only replacement did not invalidate the registered reader")
			}
			assert.Equal(t, first.GetArtifact(target, ArtifactContent), Artifact(payload))
			second.Init(s, ownerID("second-reader"), nil)
			defer second.Retire()
			assert.Equal(t, second.GetArtifact(target, ArtifactContent), Artifact(next))
			if !second.Valid() {
				t.Fatal("new payload authority could not be captured")
			}
			s.RecordEmitted(a, &Emission{Complete: true}, nil)
			if second.Valid() {
				t.Fatal("last payload withdrawal retained registered artifact authority")
			}
			assert.Equal(t, s.GetObject(target), declaration.Obj)
			assert.Equal(t, s.GetArtifact(target, ArtifactContent), Artifact(nil))
		})
	}
}

func TestRecordEmitted_StalePayloadRejected(t *testing.T) {
	t.Parallel()
	s := New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(producer)
	input := ownerID("input")
	s.SetArtifact(input, &SourceArtifact{Key: "first"})
	var reads ReadSet
	reads.Init(s, producer.Named(), nil)
	defer reads.Retire()
	reads.GetArtifact(input, ArtifactContent)
	_, _, next := reads.GetEmission(producer.Named())
	defer next.Finish()
	child := emissionChild("output", "old")
	next.Children, next.Complete = []Child{child}, true
	reads.Bind(&next)
	s.SetArtifact(input, &SourceArtifact{Key: "changed"})
	s.RecordEmitted(producer.Named(), &next, map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): {Key: "stale"}})
	if s.GetObject(child.Obj.Named()) != nil || s.GetArtifact(child.Obj.Named(), ArtifactContent) != nil || s.HasInventory(producer.Named()) {
		t.Fatal("invalid captured inputs published stale declaration or bytes")
	}
}

func TestProductions_SelfPublication(t *testing.T) {
	t.Parallel()
	s := New()
	producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	child := emissionChild("output", "value")
	s.AddObject(producer)
	production := manifest.Production{Owner: producer.Named(), Source: manifest.BootstrapSourceID}
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		producer.Named():  {Productions: []manifest.Production{production}},
		child.Obj.Named(): {Productions: []manifest.Production{production}},
	}})
	var self, observer ReadSet
	self.Init(s, producer.Named(), nil)
	observer.Init(s, ownerID("observer"), nil)
	defer self.Close()
	defer observer.Close()
	assert.Diff(t, self.Productions(producer.Named(), nil), []manifest.Production{production})
	assert.Diff(t, observer.Productions(producer.Named(), nil), []manifest.Production{production})
	_, _, next := self.GetEmission(producer.Named())
	defer next.Finish()
	next.Children, next.Complete = []Child{child}, true
	s.RecordEmitted(producer.Named(), &next, nil)
	if !self.Valid() || observer.Valid() || s.GetObject(child.Obj.Named()) == nil {
		t.Fatalf("publication validity: self=%t observer=%t child=%v", self.Valid(), observer.Valid(), s.GetObject(child.Obj.Named()))
	}
	changed := producer.Clone()
	changed.Path = "changed"
	s.AddObject(changed)
	if self.Valid() {
		t.Fatal("self-publication exemption ignored replacement of the declaration")
	}
}

func TestReadSet_BoundDeclarationAuthority(t *testing.T) {
	t.Parallel()
	for _, support := range []string{"unindexed", "no_productions", "independent", "self", "covered"} {
		t.Run(support, func(t *testing.T) {
			t.Parallel()
			st := New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			raw := emissionChild("input", "raw")
			prospective := emissionChild("input", "prospective")
			st.AddObjects([]manifest.BaseManifest{owner, raw.Obj})
			proofs := []manifest.Production{{Owner: owner.Named()}}
			switch support {
			case "no_productions":
				proofs = nil
			case "independent":
				proofs = append(proofs, manifest.Production{})
			case "self":
				proofs = append(proofs, manifest.Production{Owner: raw.Obj.Named()})
			}
			if support != "unindexed" {
				st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{raw.Obj.Named(): {Productions: proofs}}})
			}
			var reads ReadSet
			reads.Init(st, owner.Named(), nil)
			defer reads.Retire()
			_, _, next := reads.GetEmission(owner.Named())
			defer next.Finish()
			next.Children = []Child{prospective}
			reads.Bind(&next)
			assert.Equal(t, reads.GetObject(raw.Obj.Named()), prospective.Obj)
			assert.Diff(t, reads.input(raw.Obj.Named()).view.child.Doc, prospective.Doc)
			var want manifest.BaseManifest
			if support != "covered" {
				want = prospective.Obj
			}
			assert.Equal(t, reads.GetDeclaration(raw.Obj.Named()), want)
			assert.Equal(t, st.GetObject(raw.Obj.Named()), raw.Obj)
			view := reads.input(raw.Obj.Named()).view
			if view.accepted || view.owner != nil {
				t.Fatal("prospective child manufactured accepted authority")
			}
			st.RecordEmitted(owner.Named(), &next, nil)
			assert.Equal(t, st.GetDeclaration(raw.Obj.Named()), prospective.Obj)
			assert.Equal(t, reads.GetDeclaration(raw.Obj.Named()), want)
		})
	}
}

func TestReadSet_RawOwnedSourceMaterializer(t *testing.T) {
	t.Parallel()
	st := New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	source := &manifest.HelmRepository{Name: "chart", Namespace: "apps", URL: "https://charts.example"}
	st.AddObjects([]manifest.BaseManifest{parent, source})
	p := manifest.Production{Owner: parent.Named(), Source: manifest.BootstrapSourceID, Authored: source.Named(), Path: "/input/source.yaml", File: "source.yaml"}
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		source.Named(): {Path: p.Path, File: p.File, Authored: p.Authored, Productions: []manifest.Production{p}},
	}})
	st.UpdateStatus(parent.Named(), StatusInfo{Status: StatusFailed, Message: "dependency unavailable"})
	var reads ReadSet
	reads.Init(st, source.Named(), nil)
	defer reads.Retire()
	if got := reads.Materializer(source.Named(), p); got != parent.Named() {
		t.Fatalf("raw owned source materializer=%s, want %s", got, parent.Named())
	}
	if reads.GetDeclaration(source.Named()) != nil {
		t.Fatal("raw source acquired declaration authority before its failed parent emitted it")
	}
}

func TestReadSet_BoundInventoryPublication(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"self_first", "self_last", "borrowed", "remapped"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			st := New()
			owner := &manifest.Kustomization{Name: "owner", Namespace: "apps"}
			raw, prospective := emissionChild("input", "raw"), emissionChild("input", "prospective")
			st.AddObjects([]manifest.BaseManifest{owner, raw.Obj})
			st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				raw.Obj.Named(): {Productions: []manifest.Production{{Owner: owner.Named()}}},
			}})
			if mode == "borrowed" {
				st.RecordEmitted(owner.Named(), &Emission{Children: []Child{prospective}, Complete: true}, nil)
			}
			var reads ReadSet
			reads.Init(st, owner.Named(), nil)
			defer reads.Retire()
			_, _, next := reads.GetEmission(owner.Named())
			defer next.Finish()
			children := []Child{{Obj: owner}, prospective}
			if mode == "self_last" {
				children[0], children[1] = children[1], children[0]
			}
			if mode != "borrowed" {
				next.Children = children
			}
			reads.Bind(&next)
			want := make([]manifest.NamedResource, len(reads.produced))
			for i, entry := range reads.produced {
				want[i] = entry.child.Obj.Named()
			}
			assert.Equal(t, next.entriesOwned, mode != "borrowed")
			published := prospective
			switch mode {
			case "borrowed":
				next.Children = children
			case "remapped":
				children = slices.Clone(children)
				published = emissionChild("input", "remapped")
				children[1] = published
				next.Children = children
			}
			next.Complete = true
			st.RecordEmitted(owner.Named(), &next, nil)
			got := make([]manifest.NamedResource, len(reads.produced))
			for i, entry := range reads.produced {
				got[i] = entry.child.Obj.Named()
			}
			assert.Diff(t, got, want)
			assert.Equal(t, reads.GetObject(raw.Obj.Named()), prospective.Obj)
			assert.Equal(t, reads.GetDeclaration(raw.Obj.Named()), nil)
			assert.Equal(t, st.GetDeclaration(raw.Obj.Named()), published.Obj)
			assert.Equal(t, st.GetObject(owner.Named()), manifest.BaseManifest(owner))
		})
	}
}

func TestReadSet_DeclarationFreeze(t *testing.T) {
	t.Parallel()
	st := New()
	raw := &manifest.OCIRepository{Name: "covered", Namespace: "apps"}
	st.AddObject(raw)
	var before, after ReadSet
	before.Init(st, ownerID("before"), nil)
	after.Init(st, ownerID("after"), nil)
	defer before.Retire()
	defer after.Retire()
	assert.Equal(t, before.GetDeclaration(raw.Named()), manifest.BaseManifest(raw))
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{raw.Named(): {Productions: []manifest.Production{{Owner: ownerID("owner")}}}}})
	if after.Matches(&before) {
		t.Fatal("matching reclassified the immutable pre-index declaration")
	}
	assert.Equal(t, before.GetDeclaration(raw.Named()), manifest.BaseManifest(raw))
	assert.Equal(t, after.GetDeclaration(raw.Named()), nil)
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{})
	assert.Equal(t, st.GetDeclaration(raw.Named()), nil)
}

func TestReadRegistration_SelectedArtifact(t *testing.T) {
	t.Parallel()
	for _, emitted := range []bool{false, true} {
		for _, tc := range []struct {
			name    string
			read    ArtifactRead
			change  func(*SourceArtifact)
			invalid bool
		}{
			{"ignored", ArtifactContent | ArtifactRevision | ArtifactOrigin, func(a *SourceArtifact) {
				a.URL = "next"
				a.Size = 12
				a.Metadata = map[string]string{"ignored": "next"}
			}, false},
			{"revision-only-content", ArtifactRevision, func(a *SourceArtifact) { a.Content = "next" }, false},
			{"origin-only-revision", ArtifactOrigin, func(a *SourceArtifact) { a.Revision = "next" }, false},
			{"content-only-revision", ArtifactContent, func(a *SourceArtifact) { a.Revision = "next" }, false},
			{"revision", ArtifactContent | ArtifactRevision, func(a *SourceArtifact) { a.Revision = "next" }, true},
			{"content-only-origin", ArtifactContent, func(a *SourceArtifact) { a.Metadata = map[string]string{"org.opencontainers.image.revision": ""} }, false},
			{"origin-presence", ArtifactContent | ArtifactOrigin, func(a *SourceArtifact) { a.Metadata = map[string]string{"org.opencontainers.image.revision": ""} }, true},
			{"content", ArtifactContent, func(a *SourceArtifact) { a.Content = "next" }, true},
			{"union", ArtifactContent | ArtifactRevision | ArtifactOrigin, func(a *SourceArtifact) {
				a.Content = "next"
				a.Revision = "next"
				a.Metadata = map[string]string{"org.opencontainers.image.revision": "next"}
			}, true},
		} {
			t.Run(fmt.Sprintf("emitted=%t/%s", emitted, tc.name), func(t *testing.T) {
				t.Parallel()
				s := New()
				child := emissionChild("selected", "same")
				producer := ownerID("producer")
				var direct *SourceArtifact
				if emitted {
					direct = &SourceArtifact{Key: "direct", Content: "direct", FS: fstest.MapFS{}}
					s.AddObject(child.Obj)
					s.SetArtifact(child.Obj.Named(), direct)
				}
				s.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
				publish := func(a *SourceArtifact) {
					if emitted {
						s.RecordEmitted(producer, &Emission{Children: []Child{child}, Complete: true}, map[manifest.NamedResource]*SourceArtifact{child.Obj.Named(): a})
					} else {
						s.SetArtifact(child.Obj.Named(), a)
					}
				}
				first := &SourceArtifact{Key: "family", Content: "generation", Revision: "first", FS: fstest.MapFS{"file": {Data: []byte("same")}}}
				publish(first)
				var reads ReadSet
				reads.Init(s, ownerID("reader"), nil)
				defer reads.Close()
				assert.Equal(t, reads.GetArtifact(child.Obj.Named(), tc.read), Artifact(first))
				next := *first
				tc.change(&next)
				publish(&next)
				assert.Equal(t, s.GetArtifact(child.Obj.Named(), ArtifactContent), Artifact(&next))
				assert.Equal(t, reads.GetArtifact(child.Obj.Named(), tc.read), Artifact(first))
				changes := slices.Collect(s.Sweep())
				if tc.invalid {
					assert.Diff(t, changes, []manifest.NamedResource{reads.ID()})
				} else {
					assert.Equal(t, len(changes), 0)
				}
				assert.Equal(t, reads.Valid(), !tc.invalid)
				assert.Equal(t, len(slices.Collect(s.Sweep())), 0)
				var replay ReadSet
				replay.Init(s, reads.ID(), nil)
				defer replay.Close()
				assert.Equal(t, replay.Matches(&reads), !tc.invalid)
				if emitted {
					s.RecordEmitted(producer, &Emission{Complete: true}, nil)
					assert.Equal(t, s.GetArtifact(child.Obj.Named(), ArtifactContent), Artifact(direct))
					assert.Equal(t, replay.Valid(), false)
				}
			})
		}
	}
	for _, read := range []ArtifactRead{ArtifactRevision, ArtifactOrigin} {
		t.Run(fmt.Sprintf("late-%d", read), func(t *testing.T) {
			t.Parallel()
			s := New()
			id := ownerID("source")
			first := &SourceArtifact{Key: "family", Content: "generation", Revision: "first", FS: fstest.MapFS{}}
			s.SetArtifact(id, first)
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			defer reads.Close()
			reads.GetArtifact(id, ArtifactContent)
			next := *first
			next.Revision = "next"
			next.Metadata = map[string]string{"org.opencontainers.image.revision": ""}
			s.SetArtifact(id, &next)
			assert.Equal(t, reads.Valid(), true)
			assert.Equal(t, reads.GetArtifact(id, ArtifactContent|read), Artifact(first))
			assert.Equal(t, reads.Valid(), false)
		})
	}
}

func TestReadRegistration_SelectedDeclaration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		initial, next any
		key           string
		invalid       bool
	}{
		{"uid", "first", "next", "uid", true}, {"non-string-uid", nil, 3, "uid", false},
		{"disable", nil, "disabled", "annotation", true}, {"normalized-disable", "disabled", "DiSaBlEd", "annotation", false},
		{"enable", "disabled", nil, "annotation", true}, {"ignored", nil, "next", "ignored", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New()
			object := &manifest.RawObject{APIVersion: "source.extensions.fluxcd.io/v1beta1", Kind: "ArtifactGenerator", Name: "generator", Namespace: "apps"}
			document := func(value any) map[string]any {
				metadata := map[string]any{}
				if tc.key == "annotation" {
					metadata["annotations"] = map[string]any{"source.extensions.fluxcd.io/reconcile": value}
				} else {
					metadata[tc.key] = value
				}
				return map[string]any{"apiVersion": object.APIVersion, "metadata": metadata}
			}
			st.AddObject(object, document(tc.initial))
			var reads ReadSet
			reads.Init(st, ownerID("reader"), nil)
			defer reads.Close()
			reads.GetObject(object.Named())
			next := document(tc.next)
			st.AddObject(object, next)
			assert.Equal(t, reads.Valid(), !tc.invalid)
			_, latest, emission := st.GetEmission(object.Named())
			defer emission.Finish()
			assert.Diff(t, latest, next)
		})
	}
}

func TestCapture_SupplierQueryIsolation(t *testing.T) {
	t.Parallel()
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete=%t", complete), func(t *testing.T) {
			t.Parallel()
			st := New()
			sourceOwner, queryOwner := ownerID("source-owner"), ownerID("query-owner")
			st.AddObjects([]manifest.BaseManifest{&manifest.Kustomization{Name: sourceOwner.Name, Namespace: sourceOwner.Namespace}, &manifest.Kustomization{Name: queryOwner.Name, Namespace: queryOwner.Namespace}})
			source, query := emissionChild("source", "same"), emissionChild("query", "same")
			payload := &SourceArtifact{Key: "family", Content: "generation", FS: fstest.MapFS{}}
			st.RecordEmitted(sourceOwner, &Emission{Children: []Child{source}, Complete: complete}, map[manifest.NamedResource]*SourceArtifact{source.Obj.Named(): payload})
			st.RecordEmitted(queryOwner, &Emission{Children: []Child{query}, Complete: complete}, nil)
			var reads ReadSet
			reads.Init(st, ownerID("reader"), nil)
			defer reads.Close()
			reads.GetArtifact(source.Obj.Named(), ArtifactContent)
			reads.UnsettledProducers(query.Obj.Named())
			reads.Productions(query.Obj.Named(), nil)
			next := *payload
			next.Metadata = map[string]string{"ignored": "next"}
			st.RecordEmitted(sourceOwner, &Emission{Children: []Child{source, emissionChild("unrelated", "new")}, Complete: complete}, map[manifest.NamedResource]*SourceArtifact{source.Obj.Named(): &next})
			assert.Equal(t, reads.Valid(), true)
			assert.Equal(t, len(slices.Collect(st.Sweep())), 0)
			st.RecordEmitted(queryOwner, &Emission{Children: []Child{query, emissionChild("query-sibling", "new")}, Complete: complete, Fingerprint: "query-publication"}, nil)
			assert.Diff(t, slices.Collect(st.Sweep()), []manifest.NamedResource{reads.ID()})
			assert.Equal(t, reads.Valid(), false)
		})
	}
}

func TestCapture_PartialSupplierInputStamp(t *testing.T) {
	t.Parallel()
	st := New()
	producer := ownerID("producer")
	st.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
	sibling := emissionChild("sibling", "same")
	child := &manifest.ResourceSet{Name: "consumer", Namespace: "apps"}
	st.RecordEmitted(producer, &Emission{Children: []Child{sibling}}, nil)
	var reads ReadSet
	reads.Init(st, child.Named(), nil)
	defer reads.Close()
	reads.GetObject(sibling.Obj.Named())
	st.RecordEmitted(producer, &Emission{Children: []Child{sibling, {Obj: child}}}, nil)
	_, _, captured := reads.GetEmission(child.Named())
	defer captured.Finish()
	_, _, current := st.GetEmission(child.Named())
	defer current.Finish()
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, captured.input, manifest.BaseManifest(child))
	assert.Equal(t, captured.inputHead, current.inputHead)
	st.RecordEmitted(producer, &Emission{Children: []Child{sibling, {Obj: child}, emissionChild("unrelated", "new")}}, nil)
	_, _, repeated := reads.GetEmission(child.Named())
	defer repeated.Finish()
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, repeated.inputHead, captured.inputHead)
	assert.Equal(t, len(slices.Collect(st.Sweep())), 0)
}

func TestReadSet_MatchesProducerAvailability(t *testing.T) {
	t.Parallel()
	st := New()
	producer := ownerID("producer")
	st.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
	child := emissionChild("input", "same")
	st.RecordEmitted(producer, &Emission{Children: []Child{child}, Complete: true}, nil)
	var before, current ReadSet
	before.Init(st, ownerID("before"), nil)
	defer before.Close()
	assert.Equal(t, len(before.UnsettledProducers(child.Obj.Named())), 0)
	st.RecordEmitted(producer, &Emission{Children: []Child{child}, Complete: true, Fingerprint: "next"}, nil)
	assert.Diff(t, slices.Collect(st.Sweep()), []manifest.NamedResource{before.ID()})
	assert.Equal(t, before.Valid(), false)
	current.Init(st, ownerID("current"), nil)
	defer current.Close()
	assert.Equal(t, current.Matches(&before), true)
	before.Retire()
	st.RecordEmitted(producer, &Emission{Children: []Child{child}, Complete: true, Fingerprint: "latest"}, nil)
	assert.Diff(t, slices.Collect(st.Sweep()), []manifest.NamedResource{current.ID()})
	assert.Equal(t, current.Valid(), false)
}

func TestReadRegistration_StableWithdrawalNoABA(t *testing.T) {
	t.Parallel()
	st := New()
	producer := ownerID("producer")
	st.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
	publish := func(child Child) {
		st.RecordEmitted(producer, &Emission{Children: []Child{child}}, nil)
	}
	first, second := emissionChild("input", "first"), emissionChild("input", "second")
	publish(first)
	var earlier, current ReadSet
	earlier.Init(st, ownerID("earlier"), nil)
	defer earlier.Close()
	earlier.GetObject(first.Obj.Named())
	publish(second)
	current.Init(st, ownerID("current"), nil)
	defer current.Close()
	assert.Equal(t, current.GetObject(first.Obj.Named()), second.Obj)
	assert.Equal(t, current.Valid(), true)
	st.RecordEmitted(producer, &Emission{Children: []Child{emissionChild("other", "unrelated")}}, nil)
	assert.Equal(t, current.Valid(), true)
	st.RecordEmitted(producer, &Emission{Complete: true}, nil)
	assert.Equal(t, st.GetObject(first.Obj.Named()), manifest.BaseManifest(nil))
	if current.Valid() {
		t.Fatal("withdrawal reused a captured stable-publication stamp")
	}
	assert.Equal(t, earlier.Valid(), false)
	notified := slices.Collect(st.Sweep())
	slices.SortFunc(notified, manifest.NamedResource.Compare)
	assert.Diff(t, notified, []manifest.NamedResource{current.id, earlier.id})
	assert.Equal(t, len(slices.Collect(st.Sweep())), 0)
}

func TestReadSet_ProducedArtifact(t *testing.T) {
	t.Parallel()
	for _, consumed := range []bool{false, true} {
		t.Run(fmt.Sprint(consumed), func(t *testing.T) {
			t.Parallel()
			s := New()
			parent := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			s.AddObject(parent)
			child := emissionChild("output", "first")
			id := child.Obj.Named()
			s.AddObject(child.Obj, child.Doc)
			previous := &SourceArtifact{Content: "first", Revision: "first", FS: fstest.MapFS{}}
			s.SetArtifact(id, previous)
			var reads ReadSet
			reads.Init(s, parent.Named(), nil)
			if consumed {
				reads.GetArtifact(id, ArtifactContent)
			}
			_, _, next := reads.GetEmission(parent.Named())
			defer next.Finish()
			next.Children, next.Complete = []Child{child}, true
			reads.Bind(&next)
			if got := reads.GetArtifact(id, ArtifactContent|ArtifactRevision|ArtifactOrigin); got != previous {
				t.Fatalf("prospective output lost the complete descriptor: %v", got)
			}
			s.RecordEmitted(parent.Named(), &next, map[manifest.NamedResource]*SourceArtifact{id: {Content: "second", Revision: "second", FS: fstest.MapFS{}}})
			if reads.Valid() == consumed {
				t.Fatalf("self-publication validity=%v with prior consumption=%v", reads.Valid(), consumed)
			}
		})
	}
}

func TestGetEmission_CrossExecutionAuthority(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"absent", "inventory", "declaration"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			s := New()
			producer := &manifest.Kustomization{Name: "producer", Namespace: "apps", Path: "."}
			s.AddObject(producer)
			child := emissionChild("source", "equal")
			if phase != "absent" {
				s.RecordEmitted(producer.Named(), &Emission{Children: []Child{child, emissionChild("root", "old")}, Complete: true}, nil)
			}
			var reads ReadSet
			reads.Init(s, ownerID("observer"), nil)
			defer reads.Close()
			obj, _, output := reads.GetEmission(producer.Named())
			defer output.Finish()
			assert.Equal(t, obj, manifest.BaseManifest(producer))
			assert.Equal(t, output.Complete, phase != "absent")
			assert.Equal(t, reads.Valid(), true)
			_ = slices.Collect(s.Sweep())
			if phase == "declaration" {
				next := producer.Clone()
				next.Path = "changed"
				s.AddObject(next)
			} else {
				s.RecordEmitted(producer.Named(), &Emission{Children: []Child{child}, Complete: true}, nil)
			}
			assert.Equal(t, reads.Valid(), false)
			assert.Diff(t, slices.Collect(s.Sweep()), []manifest.NamedResource{reads.ID()})
		})
	}
}

func TestCapture_AcquiringOwnSource(t *testing.T) {
	t.Parallel()
	s := New()
	source := &manifest.GitRepository{Name: "source", Namespace: "apps", URL: "file:///input"}
	root := &manifest.Kustomization{Name: "root", Namespace: "apps", Path: "."}
	s.AddObject(source)
	s.AddObject(root)
	s.SetArtifact(source.Named(), &SourceArtifact{Key: "old"})
	var admission, acquisition ReadSet
	admission.Init(s, root.Named(), nil)
	acquisition.Init(s, source.Named(), nil)
	defer admission.Close()
	defer acquisition.Close()
	admission.Admit(root, source.Named(), manifest.NamedResource{})
	assert.Equal(t, acquisition.GetDeclaration(root.Named()), manifest.BaseManifest(root))
	assert.Equal(t, acquisition.Valid(), true)
	s.SetArtifact(source.Named(), &SourceArtifact{Key: "new"})
	assert.Equal(t, acquisition.Valid(), true)
	changed := root.Clone()
	changed.Path = "changed"
	s.AddObject(changed)
	assert.Equal(t, acquisition.Valid(), false)
}

func TestIncomingProductions_OwnArtifact(t *testing.T) {
	t.Parallel()
	s := New()
	source := &manifest.GitRepository{Name: "self", Namespace: "apps", URL: "https://fixture.invalid/self"}
	owner := &manifest.Kustomization{Name: "owner", Namespace: "apps", Path: "."}
	s.AddObject(source)
	s.AddObject(owner)
	var old, current ReadSet
	old.Init(s, source.Named(), nil)
	current.Init(s, source.Named(), nil)
	defer old.Close()
	defer current.Close()
	old.GetDeclaration(owner.Named())
	previous := &SourceArtifact{Reads: &old, Key: "old"}
	s.SetArtifact(source.Named(), previous)
	var consumer ReadSet
	consumer.Init(s, ownerID("consumer"), nil)
	defer consumer.Close()
	assert.Equal(t, consumer.GetArtifact(source.Named(), ArtifactContent), Artifact(previous))
	changed := owner.Clone()
	changed.TargetNamespace = "changed"
	s.AddObject(changed)
	assert.Equal(t, old.Valid(), false)
	production := manifest.Production{Owner: owner.Named(), Source: source.Named()}
	child := Child{Obj: source, Productions: []manifest.Production{production}}
	s.RecordEmitted(owner.Named(), &Emission{Children: []Child{child}, Complete: true}, nil)
	current.GetDeclaration(owner.Named())
	assert.Diff(t, current.IncomingProductions(child, nil), []manifest.Production{production})
	assert.Equal(t, current.GetArtifact(source.Named(), ArtifactContent), Artifact(previous))
	assert.Equal(t, current.Valid(), true)
	assert.Equal(t, consumer.GetArtifact(source.Named(), ArtifactContent), Artifact(nil))
	assert.Equal(t, consumer.Valid(), false)
	changed = changed.Clone()
	changed.TargetNamespace = "again"
	s.AddObject(changed)
	assert.Equal(t, current.Valid(), false)
}

func TestReadSet_SourceAvailability(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"renewal", "changed", "unavailable", "withdrawal"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			s := New()
			source := &manifest.GitRepository{Name: "source", Namespace: "apps"}
			authority := &manifest.Kustomization{Name: "entry", Namespace: "apps", Path: "."}
			s.AddObjects([]manifest.BaseManifest{source, authority})
			var first, next, waiting, consumed, renewed ReadSet
			first.Init(s, source.Named(), nil)
			next.Init(s, source.Named(), nil)
			waiting.Init(s, ownerID("waiting"), nil)
			consumed.Init(s, ownerID("consumed"), nil)
			renewed.Init(s, ownerID("renewed"), nil)
			defer first.Close()
			defer next.Close()
			defer waiting.Close()
			defer consumed.Close()
			defer renewed.Close()
			first.GetObject(authority.Named())
			artifact := &SourceArtifact{Reads: &first, Key: "source", Content: "same", FS: fstest.MapFS{}}
			s.SetArtifact(source.Named(), artifact)
			assert.Equal(t, consumed.GetArtifact(source.Named(), ArtifactContent), Artifact(artifact))
			changed := authority.Clone()
			changed.Path = "changed"
			s.AddObject(changed)
			assert.Equal(t, consumed.GetArtifact(source.Named(), ArtifactContent), nil)
			assert.Equal(t, consumed.Valid(), false)
			assert.Equal(t, s.GetArtifact(source.Named(), ArtifactContent), Artifact(artifact))
			assert.Equal(t, waiting.Matches(&consumed), false)
			assert.Equal(t, waiting.Valid(), true)
			assert.Equal(t, waiting.GetArtifact(source.Named(), ArtifactContent), nil)
			assert.Equal(t, waiting.Valid(), true)
			assert.Equal(t, waiting.GetArtifact(source.Named(), ArtifactContent), nil)
			assert.Equal(t, waiting.Valid(), true)
			next.GetObject(authority.Named())
			replacement := artifact.WithReads(&next)
			switch phase {
			case "changed":
				replacement.Content = "changed"
			case "unavailable":
				replacement.FS = nil
			case "withdrawal":
				replacement = nil
			}
			s.SetArtifact(source.Named(), replacement)
			assert.Equal(t, first.retired.Load(), true)
			if replacement == nil {
				assert.Equal(t, waiting.GetArtifact(source.Named(), ArtifactContent), nil)
			} else {
				assert.Equal(t, waiting.GetArtifact(source.Named(), ArtifactContent), Artifact(replacement))
			}
			assert.Equal(t, waiting.Valid(), false)
			if replacement == nil {
				assert.Equal(t, renewed.GetArtifact(source.Named(), ArtifactContent), nil)
			} else {
				assert.Equal(t, renewed.GetArtifact(source.Named(), ArtifactContent), Artifact(replacement))
				assert.Equal(t, replacement.Reads.Valid(), true)
			}
			assert.Equal(t, renewed.Valid(), true)
			s.SetArtifact(source.Named(), &SourceArtifact{Reads: &next, Key: "source", Content: "after", FS: fstest.MapFS{}})
			assert.Equal(t, renewed.Valid(), false)
		})
	}
}
