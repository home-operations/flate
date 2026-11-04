package store

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

type authorityHeads struct {
	object, producer, condition, artifact uint64
}

func headsForTest(s *Store, id manifest.NamedResource) authorityHeads {
	sh := s.shardFor(id)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	h := authorityHeads{
		object:    sh.cells[id].generation,
		condition: sh.conditions[id].stamp().generation,
		artifact:  sh.artifacts[id].generation,
	}
	if slot := sh.slots[id]; slot != nil {
		h.producer = slot.generation
	}
	return h
}

func TestReadRegistration_SameByteAuthorityTransitions(t *testing.T) {
	t.Parallel()
	for _, batch := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "batch"}[batch], func(t *testing.T) {
			t.Parallel()
			testReadRegistrationSameByteAuthorityTransitionsCase(
				t,
				&batch,
			)
		},
		)
	}
}

func TestStore_CollectionAuthority(t *testing.T) {
	t.Parallel()
	for _, raw := range []bool{false, true} {
		t.Run(map[bool]string{false: "typed", true: "raw"}[raw], func(t *testing.T) {
			t.Parallel()
			s := New()
			versions := make([]manifest.BaseManifest, 3)
			for i, label := range []string{"a", "a", "b"} {
				if raw {
					versions[i] = &manifest.RawObject{
						APIVersion: "v1",
						Kind:       "ConfigMap",
						Name:       "input",
						Spec: map[string]any{
							"metadata": map[string]any{"labels": map[string]any{"selected": label}}, "data": i,
						},
					}
				} else {
					versions[i] = &manifest.Kustomization{
						Name:   "input",
						Labels: map[string]string{"selected": label},
						Path:   string(rune('a' + i)),
					}
				}
			}
			id := versions[0].Named()
			head := func() uint64 {
				sh := s.shardFor(id)
				sh.mu.RLock()
				defer sh.mu.RUnlock()
				return sh.byName[kindID{id.Group, id.Kind}].generation
			}
			s.AddDeclaration(Declaration{Obj: versions[0], Origin: manifest.SuppliedDeclaration})
			var before ReadSet
			before.Init(s, ownerID("before"), nil)
			defer before.Retire()
			assert.Equal(t, len(before.ListObjects(id.Group, id.Kind)), 1)
			s.AddObject(versions[0])
			assert.Equal(t, before.Valid(), false)
			first := head()
			s.AddObject(versions[1])
			assert.Equal(t, head(), first)
			assert.Equal(t, s.GetObject(id), versions[1])
			input, _, _ := s.GetEmission(id)
			assert.Equal(t, input, versions[1])
			s.AddObjects([]manifest.BaseManifest{versions[2]})
			assert.FatalIf(t, head() <= first, "%s", "label transition did not advance collection authority")
			objects := s.ListObjects(id.Group, id.Kind)
			assert.Equal(t, len(objects), 1)
			assert.Equal(t, objects[0], versions[2])
			var after ReadSet
			after.Init(s, ownerID("after"), nil)
			defer after.Retire()
			after.ListObjects(id.Group, id.Kind)
			s.DeleteObject(id)
			assert.Equal(t, after.Valid(), false)
			assert.Equal(t, len(s.ListObjects(id.Group, id.Kind)), 1)
			assert.Equal(t, s.GetObject(id), versions[0])
			assert.Equal(t, s.ReportHidden(id), true)
		})
	}
}

func TestReadRegistration_NoABA(t *testing.T) {
	t.Parallel()
	t.Run("object", func(t *testing.T) { t.Parallel(); testReadRegistrationNoABAObject(t) })
	t.Run("producer", func(t *testing.T) { t.Parallel(); testReadRegistrationNoABAProducer(t) })
	for _, kind := range []string{"condition", "artifact"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			testReadRegistrationNoABACase(t, &kind)
		},
		)
	}
	t.Run("condition-exhaustion", func(t *testing.T) { t.Parallel(); testReadRegistrationNoABAConditionExhaustion(t) })
	for _, kind := range []string{"artifact", "producer", "bucket"} {
		t.Run(kind+"-exhaustion", func(t *testing.T) {
			t.Parallel()
			testReadRegistrationNoABACase2(t, &kind)
		},
		)
	}

	t.Run("exhaustion", func(t *testing.T) { t.Parallel(); testReadRegistrationNoABAExhaustion(t) })
}

func TestEvidenceOwnerProof_StablePublicationCut(t *testing.T) {
	t.Parallel()
	s := New()
	p := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(p)
	old := emissionChild("cut", "old")
	fresh := emissionChild("cut", "new")
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{old}, Complete: true}, nil)
	sh := s.shardFor(old.Obj.Named())
	sh.mu.RLock()
	var before capture
	sh.captureLocked(old.Obj.Named(), new(sh.cells[old.Obj.Named()]), &before)
	sh.mu.RUnlock()
	paused, resume := make(chan struct{}), make(chan struct{})
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == fresh.Obj.Named() {
			close(paused)
			<-resume
		}
	}, false)
	defer unsub()
	var wg sync.WaitGroup
	wg.Go(func() { s.RecordEmitted(p.Named(), &Emission{Children: []Child{fresh}, Complete: true}, nil) })
	<-paused
	obj, _, accepted := s.GetEmission(p.Named())
	assert.Equal(t, obj, manifest.BaseManifest(p))
	assert.Equal(t, accepted.Children[0].Obj, fresh.Obj)
	assert.Equal(t, s.GetObject(fresh.Obj.Named()), fresh.Obj)
	sh.mu.RLock()
	var after capture
	sh.captureLocked(fresh.Obj.Named(), new(sh.cells[fresh.Obj.Named()]), &after)
	sh.mu.RUnlock()
	assert.ErrorIf(
		t,
		before.owner != after.owner || before.record == after.record || before.generation >= after.generation,
		"%s",
		"selected material did not advance its existing target authority",
	)
	assert.Equal(t, before.record.entries[0].child.Obj, old.Obj)
	assert.Equal(t, after.record.entries[0].child.Obj, fresh.Obj)
	assert.Diff(t, s.Productions(fresh.Obj.Named(), nil), []manifest.Production{{Owner: p.Named()}})
	close(resume)
	wg.Wait()
}

func TestFeasibility_PointerOnlyExistingSection(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"stable", "replacement"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			testFeasibilityPointerOnlyExistingSectionCase(
				t,
				&route,
			)
		},
		)
	}
}

func TestFamilyAuthority_CrossFamilyWithdrawalCut(t *testing.T) {
	t.Parallel()
	s := New()
	p := &manifest.Kustomization{Name: "p", Namespace: "apps", Path: "static"}
	q := &manifest.ResourceSet{Name: "q", Namespace: "apps"}
	assert.FatalIf(
		t,
		s.shardFor(p.Named()) == s.shardFor(q.Named()),
		"%s",
		"fixture requires different producer shards",
	)
	s.AddObjects([]manifest.BaseManifest{p, q})
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		p.Named(): {Productions: []manifest.Production{{Owner: q.Named(), Source: manifest.BootstrapSourceID}}},
		q.Named(): {Productions: []manifest.Production{{Owner: p.Named(), Source: manifest.BootstrapSourceID}}},
	}})
	s.AddObjects([]manifest.BaseManifest{p, q})
	fromP, fromQ := emissionChild("from-p", "p"), emissionChild("from-q", "q")
	emittedP := *p
	emittedP.Path = "emitted"
	_, _, a := s.GetEmission(p.Named())
	a.Children, a.Complete = []Child{{Obj: q}, fromP}, true
	s.RecordEmitted(p.Named(), &a, nil)
	_, _, b := s.GetEmission(q.Named())
	b.Children, b.Complete = []Child{{Obj: &emittedP}, fromQ}, true
	s.RecordEmitted(q.Named(), &b, nil)
	paused, resume := make(chan struct{}), make(chan struct{})
	var stopped atomic.Bool
	unsub := s.AddListener(EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == p.Named() && stopped.CompareAndSwap(false, true) {
			close(paused)
			<-resume
		}
	}, false)
	defer unsub()
	var wg sync.WaitGroup
	wg.Go(func() { s.DeleteObject(p.Named()) })
	<-paused
	assert.FatalIf(t, !s.DeleteObject(q.Named()), "%s", "second withdrawal did not remove its static authority")
	for _, id := range []manifest.NamedResource{p.Named(), q.Named(), fromP.Obj.Named(), fromQ.Obj.Named()} {
		assert.ErrorIf(
			t,
			s.GetObject(id) != nil || s.GetArtifact(id, ArtifactContent) != nil || len(s.Productions(id, nil)) != 0,
			"rootless family retained authority for %s",
			id,
		)
	}
	close(resume)
	wg.Wait()
}

func TestOfflockModel_ActivationAfterValidation(t *testing.T) {
	t.Parallel()
	s := New()
	p := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	seed := &manifest.ConfigMap{Name: "leaf"}
	s.AddObjects([]manifest.BaseManifest{p, seed})
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		seed.Named(): {
			Productions: []manifest.Production{
				{Owner: p.Named(), Source: manifest.BootstrapSourceID, Authored: seed.Named(), Path: "leaf.yaml"},
			},
		},
	}})
	paused, resume := make(chan struct{}), make(chan struct{})
	unsub := s.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
		if id == p.Named() {
			close(paused)
			<-resume
		}
	}, false)
	defer unsub()
	_, _, next := s.GetEmission(p.Named())
	next.Complete = true
	var wg sync.WaitGroup
	wg.Go(func() { s.RecordEmitted(p.Named(), &next, nil) })
	<-paused
	scoped := *seed
	scoped.Namespace = "late"
	{
		got := s.Productions(scoped.Named(), nil)
		assert.ErrorIf(t, len(got) != 0, "late family projected withdrawn coverage: %v", got)
	}
	s.AddObject(&scoped)
	{
		got := s.Productions(scoped.Named(), nil)
		assert.ErrorIf(t, len(got) != 0, "explicit admission reactivated withdrawn family: %v", got)
	}
	assert.Equal(t, s.GetObject(scoped.Named()), manifest.BaseManifest(&scoped))
	assert.Equal(t, s.GetObject(seed.Named()), manifest.BaseManifest(nil))
	close(resume)
	wg.Wait()
}

func TestFeasibility_RevisionBeforeSeparateInstall(t *testing.T) {
	t.Parallel()
	s := New()
	p := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(p)
	a, b := emissionChild("child", "a"), emissionChild("child", "b")
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{a}, Complete: true}, nil)
	target := s.shardFor(a.Obj.Named())
	assert.FatalIf(
		t,
		target == s.shardFor(p.Named()),
		"%s",
		"fixture requires different producer and target shards",
	)
	target.mu.Lock()
	done := make(chan struct{})
	go func() {
		s.RecordEmitted(p.Named(), &Emission{Children: []Child{b}, Complete: true}, nil)
		close(done)
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		target.mu.Unlock()
		<-done
		t.Fatal("stable publication visited the target shard after installation")
	}
	artifact := s.GetArtifact(p.Named(), ArtifactContent).(*Emission)
	assert.Equal(t, artifact.Children[0].Obj, b.Obj)
	target.mu.Unlock()
	assert.Equal(t, s.GetObject(b.Obj.Named()), b.Obj)
}

func TestStore_AddObjects_LastWrite(t *testing.T) {
	t.Parallel()
	s := New()
	a, b := emissionChild("duplicate", "a"), emissionChild("duplicate", "b")
	s.AddObject(a.Obj)
	s.AddObjects([]manifest.BaseManifest{b.Obj, a.Obj})
	assert.Equal(t, s.GetObject(a.Obj.Named()), a.Obj)
	s.AddObjects([]manifest.BaseManifest{a.Obj, b.Obj})
	assert.Equal(t, s.GetObject(a.Obj.Named()), b.Obj)
}

func TestStore_AnchorRechecksEffective(t *testing.T) {
	t.Parallel()
	s := New()
	p := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	seed, emitted := emissionChild("anchor", "static"), emissionChild("anchor", "emitted")
	s.AddObjects([]manifest.BaseManifest{p, seed.Obj})
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{emitted}, Complete: true}, nil)
	sh := s.shardFor(seed.Obj.Named())
	sh.mu.RLock()
	cell := sh.cells[seed.Obj.Named()]
	u := objectUpdate{obj: emitted.Obj, cell: cell}
	sh.captureLocked(seed.Obj.Named(), new(cell), &u.effective)
	sh.mu.RUnlock()
	u.compare()
	assert.Equal(t, u.effectiveEqual, true)
	var wg sync.WaitGroup
	wg.Go(func() {
		s.RecordEmitted(p.Named(), &Emission{Children: []Child{emissionChild("anchor", "new")}, Complete: true}, nil)
	})
	wg.Wait()
	assert.FatalIf(t, s.addAnchor(new(u)), "%s", "anchor accepted equality against a superseded effective record")
	s.AddObject(emitted.Obj)
	assert.Equal(t, s.GetObject(seed.Obj.Named()), emitted.Obj)
	s.RecordEmitted(p.Named(), &Emission{Complete: true}, nil)
	assert.Equal(t, s.GetObject(seed.Obj.Named()), emitted.Obj)

	t.Run("ignored_metadata", func(t *testing.T) {
		t.Parallel()
		st := New()
		producer := ownerID("producer")
		st.AddObject(&manifest.Kustomization{Name: producer.Name, Namespace: producer.Namespace})
		obj := &manifest.RawObject{
			APIVersion: "source.extensions.fluxcd.io/v1beta1",
			Kind:       "ArtifactGenerator",
			Name:       "generator",
			Namespace:  "apps",
		}
		doc := map[string]any{
			"apiVersion": obj.APIVersion,
			"kind":       obj.Kind,
			"metadata":   map[string]any{"name": obj.Name, "namespace": obj.Namespace},
		}
		st.RecordEmitted(producer, &Emission{Children: []Child{{Obj: obj, Doc: doc}}, Complete: true}, nil)
		shard := st.shardFor(obj.Named())
		shard.mu.RLock()
		update := objectUpdate{obj: obj, document: doc, cell: shard.cells[obj.Named()]}
		shard.captureLocked(obj.Named(), new(update.cell), &update.effective)
		shard.mu.RUnlock()
		update.compare()
		var reads ReadSet
		reads.Init(st, ownerID("reader"), nil)
		defer reads.Close()
		reads.GetObject(obj.Named())
		changed := manifest.DeepCopyMap(doc)
		changed["metadata"].(map[string]any)["annotations"] = map[string]any{"unrelated": "changed"}
		st.RecordEmitted(producer, &Emission{Children: []Child{{Obj: obj, Doc: changed}}, Complete: true}, nil)
		assert.FatalIf(t, !reads.Valid(), "%s", "ignored metadata changed the consumed declaration")
		assert.FatalIf(
			t,
			st.addAnchor(new(update)),
			"%s",
			"anchor accepted equality against a superseded full document",
		)
		_, retained, attempt := st.GetEmission(obj.Named())
		defer attempt.Finish()
		assert.FatalIf(
			t,
			!manifest.ValueEqual(retained, changed),
			"%s",
			"stale anchor replaced the latest authoritative document",
		)
	})
}

func TestGetEmission_InputAuthorityStamp(t *testing.T) {
	t.Parallel()
	s := New()
	p := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	q := &manifest.ResourceSet{Name: "consumer", Namespace: "apps"}
	s.AddObject(p)
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{{Obj: q}}, Complete: true, Fingerprint: "a"}, nil)
	_, _, before := s.GetEmission(q.Named())
	before.Complete, before.Fingerprint = true, "unchanged-output"
	s.RecordEmitted(q.Named(), &before, nil)
	slot := s.shardFor(q.Named()).slots[q.Named()]
	revision := slot.current.Load().revision
	var reads ReadSet
	reads.Init(s, q.Named(), nil)
	reads.GetObject(q.Named())
	reads.Productions(q.Named(), nil)
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{{Obj: q}}, Complete: true, Fingerprint: "b"}, nil)
	_, _, after := s.GetEmission(q.Named())
	assert.FatalIf(t, reads.Valid(), "%s", "producer authority transition failed to invalidate its reader")
	after.Complete = true
	s.RecordEmitted(q.Named(), &after, nil)
	assert.Equal(t, slot.current.Load().revision, revision)
	assert.Equal(t, after.inputHead.owner, before.inputHead.owner)
	assert.Equal(t, after.inputHead.generation, before.inputHead.generation)
	assert.FatalIf(
		t,
		after.inputHead.revision <= before.inputHead.revision,
		"%s",
		"stable publication reused its captured authority revision",
	)
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{{Obj: q}}, Complete: true, Fingerprint: "b"}, nil)
	_, _, repeated := s.GetEmission(q.Named())
	assert.Equal(t, repeated.inputHead, after.inputHead)
}

func TestReadRegistration_SeedReplacement(t *testing.T) {
	t.Parallel()
	s := New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	child := &manifest.HelmRelease{Name: "child", Namespace: "apps", Values: map[string]any{"value": "old"}}
	s.SetDiscoveryIndex(
		&manifest.DiscoveryIndex{
			Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
				child.Named(): {Productions: []manifest.Production{{Owner: parent.Named()}}},
			},
		},
	)
	s.AddObject(parent)
	s.AddObject(child)
	replacement := child.Clone()
	replacement.Values["value"] = "new"
	s.RecordEmitted(parent.Named(), &Emission{Children: []Child{{Obj: replacement}}, Complete: true}, nil)
	{
		actual := s.GetObject(child.Named())
		assert.FatalIf(t, actual != replacement, "%s", "seed admission masked its producer's replacement")
	}
}

func TestStore_ReadmissionExhaustionIsAtomic(t *testing.T) {
	t.Parallel()
	s := New()
	obj := &manifest.Kustomization{Name: "readmitted", Namespace: "apps"}
	id := obj.Named()
	s.AddObject(obj)
	s.SetIncluded(id, false)
	s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: ReasonUnchanged})
	sh := s.shardFor(id)
	sh.mu.Lock()
	sh.conditions[id].state.generation = math.MaxUint64
	sh.mu.Unlock()
	var reads ReadSet
	reads.Init(s, id, nil)
	defer reads.Retire()
	reads.GetObject(id)
	reads.GetStatus(id)
	before := headsForTest(s, id)
	func() {
		defer func() {
			assert.ErrorIf(t, recover() == nil, "%s", "readmission did not reject exhausted condition authority")
		}()
		s.SetIncluded(id, true)
	}()
	assert.Equal(t, headsForTest(s, id), before)
	assert.Equal(t, reads.Valid(), true)
	var current ReadSet
	current.Init(s, id, nil)
	defer current.Retire()
	assert.Equal(t, current.Admitted(), false)
	info, _ := current.GetStatus(id)
	assert.Equal(t, info.Outcome(), OutcomeUnchanged)
}

func testReadRegistrationSameByteAuthorityTransitionsCase(
	t *testing.T,
	testCaseInput *bool,
) {
	t.Helper()

	s := New()
	p, q := ownerID("a"), ownerID("z")
	s.AddObjects(
		[]manifest.BaseManifest{
			&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace},
			&manifest.Kustomization{Name: q.Name, Namespace: q.Namespace},
		},
	)
	child := emissionChild("shared", "equal")
	s.AddDeclaration(Declaration{Obj: child.Obj, Doc: child.Doc, Origin: manifest.SuppliedDeclaration})
	s.RecordEmitted(p, &Emission{Children: []Child{child}, Complete: true}, nil)
	h1 := headsForTest(s, child.Obj.Named())
	captureReads := func() *ReadSet {
		t.Helper()
		reads := &ReadSet{}
		reads.Init(s, ownerID("reader"), nil)
		reads.GetObject(child.Obj.Named())
		reads.GetStatus(child.Obj.Named())
		reads.GetArtifact(child.Obj.Named(), ArtifactContent)
		for range reads.Productions(child.Obj.Named(), nil) {
		}
		t.Cleanup(reads.Retire)
		return reads
	}
	first := captureReads()
	s.RecordEmitted(q, &Emission{Children: []Child{child}, Complete: true}, nil)
	h2 := headsForTest(s, child.Obj.Named())
	assert.FatalIf(t, h2.object <= h1.object, "%s", "equal contributor admission did not advance authority")
	assert.Equal(t, first.Valid(), false)
	second := captureReads()
	s.RecordEmitted(p, &Emission{Complete: true}, nil)
	h3 := headsForTest(s, child.Obj.Named())
	assert.FatalIf(
		t,
		h3.object <= h2.object || s.EmissionOwner(child.Obj.Named()) != q,
		"%s",
		"equal unselected contributor withdrawal lost authority",
	)
	assert.Equal(t, second.Valid(), false)
	third := captureReads()
	if *testCaseInput {
		s.AddObjects([]manifest.BaseManifest{child.Obj})
	} else {
		s.AddObject(child.Obj)
	}
	h4 := headsForTest(s, child.Obj.Named())
	assert.FatalIf(
		t,
		h4.object <= h3.object || s.EmissionOwner(child.Obj.Named()) != (manifest.NamedResource{}),
		"%s",
		"same pointer static admission did not root the object",
	)
	assert.Equal(t, third.Valid(), false)
	fourth := captureReads()
	if *testCaseInput {
		s.AddObjects([]manifest.BaseManifest{child.Obj})
	} else {
		s.AddObject(child.Obj)
	}
	assert.Equal(t, headsForTest(s, child.Obj.Named()), h4)
	assert.Equal(t, fourth.Valid(), true)
	s.UpdateStatus(child.Obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
	s.SetArtifact(child.Obj.Named(), &SourceArtifact{Key: "a"})
	h5 := headsForTest(s, child.Obj.Named())
	assert.Equal(t, fourth.Valid(), false)
	fifth := captureReads()
	s.UpdateStatus(child.Obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: ""})
	s.SetArtifact(child.Obj.Named(), &SourceArtifact{Key: "a"})
	assert.Equal(t, headsForTest(s, child.Obj.Named()), h5)
	assert.Equal(t, fifth.Valid(), true)
	s.DeleteObject(child.Obj.Named())
	h6 := headsForTest(s, child.Obj.Named())
	assert.FatalIf(
		t,
		h6.object <= h5.object || h6.condition <= h5.condition || h6.artifact <= h5.artifact,
		"%s",
		"cleared facets lost their tombstone identities",
	)
	assert.Equal(t, fifth.Valid(), false)
	sixth := captureReads()
	s.RecordEmitted(q, &Emission{Complete: true}, nil)
	assert.Equal(t, sixth.Valid(), false)
	assert.Equal(t, s.GetObject(child.Obj.Named()), child.Obj)
	assert.Equal(t, s.GetDeclaration(child.Obj.Named()), child.Obj)
	assert.Equal(t, s.ReportHidden(child.Obj.Named()), true)
	assert.Equal(t, len(s.ListObjects(child.Obj.Named().Group, child.Obj.Named().Kind)), 1)
}

func testReadRegistrationNoABAObject(t *testing.T) {
	t.Helper()

	s := New()
	a, b := emissionChild("aba", "a"), emissionChild("aba", "b")
	s.AddObject(a.Obj)
	h1 := headsForTest(s, a.Obj.Named())
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	reads.GetObject(a.Obj.Named())
	defer reads.Retire()
	s.AddObject(b.Obj)
	s.AddObject(a.Obj)
	h2 := headsForTest(s, a.Obj.Named())
	assert.FatalIf(t, h2.object <= h1.object, "%s", "A/B/A reused an object generation")
	assert.Equal(t, reads.Valid(), false)
	var readmitted ReadSet
	readmitted.Init(s, ownerID("readmitted-reader"), nil)
	readmitted.GetObject(a.Obj.Named())
	defer readmitted.Retire()
	s.DeleteObject(a.Obj.Named())
	s.AddObject(a.Obj)
	assert.FatalIf(
		t,
		headsForTest(s, a.Obj.Named()).object <= h2.object,
		"%s",
		"delete/readmit reset the object generation",
	)
	assert.Equal(t, readmitted.Valid(), false)
}

func testReadRegistrationNoABAProducer(t *testing.T) {
	t.Helper()

	s := New()
	p := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(p)
	child := emissionChild("aba", "a")
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{child}, Complete: true}, nil)
	h1 := headsForTest(s, p.Named())
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	reads.GetObject(child.Obj.Named())
	defer reads.Retire()
	s.DeleteObject(p.Named())
	s.AddObject(p)
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{emissionChild("aba", "a")}, Complete: true}, nil)
	assert.FatalIf(
		t,
		headsForTest(s, p.Named()).producer <= h1.producer,
		"%s",
		"producer readmission reused its authority generation",
	)
	assert.Equal(t, reads.Valid(), false)
}

func testReadRegistrationNoABACase(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	s := New()
	obj := &manifest.Kustomization{Name: "aba", Namespace: "apps"}
	s.AddObject(obj)
	publish := func(value string) {
		if (*testCaseInput) == "condition" {
			s.UpdateStatus(obj.Named(), StatusInfo{Status: StatusReady, Reason: "", Message: value})
		} else {
			s.SetArtifact(obj.Named(), &SourceArtifact{Key: value})
		}
	}
	publish("a")
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	defer reads.Retire()
	if (*testCaseInput) == "condition" {
		reads.GetStatus(obj.Named())
	} else {
		reads.GetArtifact(obj.Named(), ArtifactContent)
	}
	publish("b")
	publish("a")
	assert.Equal(t, reads.Valid(), false)
}

func testReadRegistrationNoABAConditionExhaustion(t *testing.T) {
	t.Helper()

	s := New()
	id := ownerID("condition-exhausted")
	s.UpdateStatus(id, StatusInfo{Status: StatusReady, Reason: "", Message: ""})
	sh := s.shardFor(id)
	sh.mu.Lock()
	sh.conditions[id].state.generation = math.MaxUint64
	sh.mu.Unlock()
	var reads ReadSet
	reads.Init(s, ownerID("reader"), nil)
	reads.GetStatus(id)
	defer reads.Retire()
	func() {
		defer func() {
			assert.ErrorIf(t, recover() == nil, "%s", "condition exhaustion did not fail before mutation")
		}()
		s.UpdateStatus(id, StatusInfo{Status: StatusFailed, Reason: "", Message: "changed"})
	}()
	info, ok := s.GetStatus(id)
	assert.Equal(t, ok, true)
	assert.Equal(t, info.Status, StatusReady)
	assert.Equal(t, headsForTest(s, id).condition, uint64(math.MaxUint64))
	assert.Equal(t, reads.Valid(), true)
}
