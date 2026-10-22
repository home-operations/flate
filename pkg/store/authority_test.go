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
	h := authorityHeads{object: sh.cells[id].generation, condition: sh.conditions[id].stamp().generation, artifact: sh.artifacts[id].generation}
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
			s := New()
			p, q := ownerID("a"), ownerID("z")
			s.AddObjects([]manifest.BaseManifest{&manifest.Kustomization{Name: p.Name, Namespace: p.Namespace}, &manifest.Kustomization{Name: q.Name, Namespace: q.Namespace}})
			child := emissionChild("shared", "equal")
			s.RecordEmitted(p, &Emission{Children: []Child{child}, Complete: true})
			h1 := headsForTest(s, child.Obj.Named())
			captureReads := func() *ReadSet {
				t.Helper()
				reads := &ReadSet{}
				reads.Init(s, ownerID("reader"), nil)
				reads.GetObject(child.Obj.Named())
				reads.GetStatus(child.Obj.Named())
				reads.GetArtifact(child.Obj.Named())
				for range reads.Productions(child.Obj.Named(), nil) {
				}
				t.Cleanup(reads.Retire)
				return reads
			}
			first := captureReads()
			s.RecordEmitted(q, &Emission{Children: []Child{child}, Complete: true})
			h2 := headsForTest(s, child.Obj.Named())
			if h2.object <= h1.object {
				t.Fatal("equal contributor admission did not advance authority")
			}
			assert.Equal(t, first.Valid(), false)
			second := captureReads()
			s.RecordEmitted(p, &Emission{Complete: true})
			h3 := headsForTest(s, child.Obj.Named())
			if h3.object <= h2.object || s.EmissionOwner(child.Obj.Named()) != q {
				t.Fatal("equal unselected contributor withdrawal lost authority")
			}
			assert.Equal(t, second.Valid(), false)
			third := captureReads()
			if batch {
				s.AddObjects([]manifest.BaseManifest{child.Obj})
			} else {
				s.AddObject(child.Obj)
			}
			h4 := headsForTest(s, child.Obj.Named())
			if h4.object <= h3.object || s.EmissionOwner(child.Obj.Named()) != (manifest.NamedResource{}) {
				t.Fatal("same pointer static admission did not root the object")
			}
			assert.Equal(t, third.Valid(), false)
			fourth := captureReads()
			if batch {
				s.AddObjects([]manifest.BaseManifest{child.Obj})
			} else {
				s.AddObject(child.Obj)
			}
			assert.Equal(t, headsForTest(s, child.Obj.Named()), h4)
			assert.Equal(t, fourth.Valid(), true)
			s.UpdateStatus(child.Obj.Named(), StatusReady, "", "")
			s.SetArtifact(child.Obj.Named(), &SourceArtifact{Key: "a"})
			h5 := headsForTest(s, child.Obj.Named())
			assert.Equal(t, fourth.Valid(), false)
			fifth := captureReads()
			s.UpdateStatus(child.Obj.Named(), StatusReady, "", "")
			s.SetArtifact(child.Obj.Named(), &SourceArtifact{Key: "a"})
			assert.Equal(t, headsForTest(s, child.Obj.Named()), h5)
			assert.Equal(t, fifth.Valid(), true)
			s.DeleteObject(child.Obj.Named())
			h6 := headsForTest(s, child.Obj.Named())
			if h6.object <= h5.object || h6.condition <= h5.condition || h6.artifact <= h5.artifact {
				t.Fatal("cleared facets lost their tombstone identities")
			}
			assert.Equal(t, fifth.Valid(), false)
		})
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
					versions[i] = &manifest.RawObject{APIVersion: "v1", Kind: "ConfigMap", Name: "input", Spec: map[string]any{
						"metadata": map[string]any{"labels": map[string]any{"selected": label}}, "data": i,
					}}
				} else {
					versions[i] = &manifest.Kustomization{Name: "input", Labels: map[string]string{"selected": label}, Path: string(rune('a' + i))}
				}
			}
			id := versions[0].Named()
			head := func() uint64 {
				sh := s.shardFor(id)
				sh.mu.RLock()
				defer sh.mu.RUnlock()
				return sh.byName[kindID{id.Group, id.Kind}].generation
			}
			s.AddObject(versions[0])
			first := head()
			s.AddObject(versions[1])
			assert.Equal(t, head(), first)
			assert.Equal(t, s.GetObject(id), versions[1])
			input, _ := s.GetEmission(id)
			assert.Equal(t, input, versions[1])
			s.AddObjects([]manifest.BaseManifest{versions[2]})
			if head() <= first {
				t.Fatal("label transition did not advance collection authority")
			}
			objects := s.ListObjects(id.Group, id.Kind)
			assert.Equal(t, len(objects), 1)
			assert.Equal(t, objects[0], versions[2])
		})
	}
}

func TestReadRegistration_NoABA(t *testing.T) {
	t.Parallel()
	t.Run("object", func(t *testing.T) {
		t.Parallel()
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
		if h2.object <= h1.object {
			t.Fatal("A/B/A reused an object generation")
		}
		assert.Equal(t, reads.Valid(), false)
		var readmitted ReadSet
		readmitted.Init(s, ownerID("readmitted-reader"), nil)
		readmitted.GetObject(a.Obj.Named())
		defer readmitted.Retire()
		s.DeleteObject(a.Obj.Named())
		s.AddObject(a.Obj)
		if headsForTest(s, a.Obj.Named()).object <= h2.object {
			t.Fatal("delete/readmit reset the object generation")
		}
		assert.Equal(t, readmitted.Valid(), false)
	})
	t.Run("producer", func(t *testing.T) {
		t.Parallel()
		s := New()
		p := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
		s.AddObject(p)
		child := emissionChild("aba", "a")
		s.RecordEmitted(p.Named(), &Emission{Children: []Child{child}, Complete: true})
		h1 := headsForTest(s, p.Named())
		var reads ReadSet
		reads.Init(s, ownerID("reader"), nil)
		reads.GetObject(child.Obj.Named())
		defer reads.Retire()
		s.DeleteObject(p.Named())
		s.AddObject(p)
		s.RecordEmitted(p.Named(), &Emission{Children: []Child{emissionChild("aba", "a")}, Complete: true})
		if headsForTest(s, p.Named()).producer <= h1.producer {
			t.Fatal("producer readmission reused its authority generation")
		}
		assert.Equal(t, reads.Valid(), false)
	})
	for _, kind := range []string{"condition", "artifact"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			s := New()
			obj := &manifest.Kustomization{Name: "aba", Namespace: "apps"}
			s.AddObject(obj)
			publish := func(value string) {
				if kind == "condition" {
					s.UpdateStatus(obj.Named(), StatusReady, "", value)
				} else {
					s.SetArtifact(obj.Named(), &SourceArtifact{Key: value})
				}
			}
			publish("a")
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			defer reads.Retire()
			if kind == "condition" {
				reads.GetStatus(obj.Named())
			} else {
				reads.GetArtifact(obj.Named())
			}
			publish("b")
			publish("a")
			assert.Equal(t, reads.Valid(), false)
		})
	}
	t.Run("condition-exhaustion", func(t *testing.T) {
		t.Parallel()
		s := New()
		id := ownerID("condition-exhausted")
		s.UpdateStatus(id, StatusReady, "", "")
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
				if recover() == nil {
					t.Error("condition exhaustion did not fail before mutation")
				}
			}()
			s.UpdateStatus(id, StatusFailed, "", "changed")
		}()
		info, ok := s.GetStatus(id)
		assert.Equal(t, ok, true)
		assert.Equal(t, info.Status, StatusReady)
		assert.Equal(t, headsForTest(s, id).condition, uint64(math.MaxUint64))
		assert.Equal(t, reads.Valid(), true)
	})
	for _, kind := range []string{"artifact", "producer", "bucket"} {
		t.Run(kind+"-exhaustion", func(t *testing.T) {
			t.Parallel()
			s := New()
			obj := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
			s.AddObject(obj)
			child := emissionChild("child", "old")
			s.RecordEmitted(obj.Named(), &Emission{Children: []Child{child}, Complete: true})
			s.SetArtifact(child.Obj.Named(), &SourceArtifact{Key: "old"})
			id := child.Obj.Named()
			if kind == "producer" {
				id = obj.Named()
			}
			sh := s.shardFor(id)
			sh.mu.Lock()
			switch kind {
			case "artifact":
				record := sh.artifacts[id]
				record.generation = math.MaxUint64
				sh.artifacts[id] = record
			case "producer":
				sh.slots[id].generation = math.MaxUint64
			case "bucket":
				bucket := sh.byName[kindID{id.Group, id.Kind}]
				bucket.generation = math.MaxUint64
				sh.byName[kindID{id.Group, id.Kind}] = bucket
			}
			sh.mu.Unlock()
			var reads ReadSet
			reads.Init(s, ownerID("reader"), nil)
			defer reads.Retire()
			switch kind {
			case "artifact":
				reads.GetArtifact(id)
			case "producer":
				reads.GetObject(child.Obj.Named())
			case "bucket":
				reads.ListObjects(id.Group, id.Kind)
			}
			func() {
				defer func() {
					if recover() == nil {
						t.Error("exhaustion did not fail before mutation")
					}
				}()
				switch kind {
				case "artifact":
					s.SetArtifact(id, &SourceArtifact{Key: "new"})
				case "producer":
					s.RecordEmitted(id, &Emission{Children: []Child{emissionChild("child", "new")}, Complete: true})
				case "bucket":
					s.AddObject(emissionChild("new", "new").Obj)
				}
			}()
			assert.Equal(t, reads.Valid(), true)
			assert.Equal(t, s.GetObject(child.Obj.Named()), child.Obj)
			assert.Equal(t, s.GetArtifact(child.Obj.Named()).(*SourceArtifact).Key, "old")
			assert.Equal(t, s.GetObject(emissionChild("new", "new").Obj.Named()) == nil, true)
		})
	}

	t.Run("exhaustion", func(t *testing.T) {
		t.Parallel()
		s := New()
		a, b := emissionChild("exhausted", "a"), emissionChild("exhausted", "b")
		s.AddObject(a.Obj)
		sh := s.shardFor(a.Obj.Named())
		sh.mu.Lock()
		cell := sh.cells[a.Obj.Named()]
		cell.generation = math.MaxUint64
		sh.cells[a.Obj.Named()] = cell
		sh.mu.Unlock()
		var reads ReadSet
		reads.Init(s, ownerID("reader"), nil)
		reads.GetObject(a.Obj.Named())
		defer reads.Retire()
		func() {
			defer func() {
				if recover() == nil {
					t.Error("exhaustion did not fail before mutation")
				}
			}()
			s.AddObject(b.Obj)
		}()
		assert.Equal(t, s.GetObject(a.Obj.Named()), a.Obj)
		assert.Equal(t, headsForTest(s, a.Obj.Named()).object, uint64(math.MaxUint64))
		assert.Equal(t, reads.Valid(), true)
	})
}

func TestEvidenceOwnerProof_StablePublicationCut(t *testing.T) {
	t.Parallel()
	s := New()
	p := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(p)
	old := emissionChild("cut", "old")
	fresh := emissionChild("cut", "new")
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{old}, Complete: true})
	sh := s.shardFor(old.Obj.Named())
	sh.mu.RLock()
	var before capture
	sh.captureLocked(old.Obj.Named(), sh.cells[old.Obj.Named()], &before)
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
	wg.Go(func() { s.RecordEmitted(p.Named(), &Emission{Children: []Child{fresh}, Complete: true}) })
	<-paused
	obj, accepted := s.GetEmission(p.Named())
	assert.Equal(t, obj, manifest.BaseManifest(p))
	assert.Equal(t, accepted.Children[0].Obj, fresh.Obj)
	assert.Equal(t, s.GetObject(fresh.Obj.Named()), fresh.Obj)
	sh.mu.RLock()
	var after capture
	sh.captureLocked(fresh.Obj.Named(), sh.cells[fresh.Obj.Named()], &after)
	sh.mu.RUnlock()
	if before.owner != after.owner || before.record == after.record || before.generation != after.generation {
		t.Error("stable cut did not retain producer authority without target repair")
	}
	assert.Equal(t, before.record.entries[0].child.Obj, old.Obj)
	assert.Equal(t, after.record.entries[0].child.Obj, fresh.Obj)
	assert.Diff(t, s.Productions(fresh.Obj.Named(), nil), []manifest.Production{{Owner: p.Named()}})
	close(resume)
	wg.Wait()
}

func TestFeasibility_PointerOnlyExistingSection(t *testing.T) {
	t.Parallel()
	s := New()
	p := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(p)
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{emissionChild("cut", "a")}, Complete: true})
	sh := s.shardFor(p.Named())
	sh.mu.RLock()
	slot := sh.slots[p.Named()]
	previous, generation := slot.current.Load(), slot.generation
	sh.mu.RUnlock()
	_, proposed := s.GetEmission(p.Named())
	proposed.Children = append(proposed.Children, emissionChild("new", "b"))
	prepared := make([]admission, len(proposed.Children))
	for i, child := range proposed.Children {
		prepared[i] = admission{child: child, targets: manifest.ProducerTargets(child.Obj)}
	}
	proposal := prepareEmission(&proposed, previous, prepared, make(map[manifest.NamedResource]int, len(prepared)))
	var wg sync.WaitGroup
	wg.Go(func() {
		s.RecordEmitted(p.Named(), &Emission{Children: []Child{emissionChild("cut", "changed")}, Complete: true})
	})
	wg.Wait()
	if s.commitReplacement(p.Named(), previous, proposal, generation) {
		t.Fatal("replacement accepted a stale protected predecessor")
	}
	if s.GetObject(proposed.Children[1].Obj.Named()) != nil {
		t.Fatal("rejected replacement installed a target")
	}
}

func TestFamilyAuthority_CrossFamilyWithdrawalCut(t *testing.T) {
	t.Parallel()
	s := New()
	p := &manifest.Kustomization{Name: "p", Namespace: "apps", Path: "static"}
	q := &manifest.ResourceSet{Name: "q", Namespace: "apps"}
	if s.shardFor(p.Named()) == s.shardFor(q.Named()) {
		t.Fatal("fixture requires different producer shards")
	}
	s.AddObjects([]manifest.BaseManifest{p, q})
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		p.Named(): {Productions: []manifest.Production{{Owner: q.Named(), Source: manifest.BootstrapSourceID}}},
		q.Named(): {Productions: []manifest.Production{{Owner: p.Named(), Source: manifest.BootstrapSourceID}}},
	}})
	s.AddObjects([]manifest.BaseManifest{p, q})
	fromP, fromQ := emissionChild("from-p", "p"), emissionChild("from-q", "q")
	emittedP := *p
	emittedP.Path = "emitted"
	_, a := s.GetEmission(p.Named())
	a.Children, a.Complete = []Child{{Obj: q}, fromP}, true
	s.RecordEmitted(p.Named(), &a)
	_, b := s.GetEmission(q.Named())
	b.Children, b.Complete = []Child{{Obj: &emittedP}, fromQ}, true
	s.RecordEmitted(q.Named(), &b)
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
	if !s.DeleteObject(q.Named()) {
		t.Fatal("second withdrawal did not remove its static authority")
	}
	for _, id := range []manifest.NamedResource{p.Named(), q.Named(), fromP.Obj.Named(), fromQ.Obj.Named()} {
		if s.GetObject(id) != nil || s.GetArtifact(id) != nil || len(s.Productions(id, nil)) != 0 {
			t.Errorf("rootless family retained authority for %s", id)
		}
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
		seed.Named(): {Productions: []manifest.Production{{Owner: p.Named(), Source: manifest.BootstrapSourceID, Authored: seed.Named(), Path: "leaf.yaml"}}},
	}})
	paused, resume := make(chan struct{}), make(chan struct{})
	unsub := s.AddListener(EventArtifactUpdated, func(id manifest.NamedResource, _ any) {
		if id == p.Named() {
			close(paused)
			<-resume
		}
	}, false)
	defer unsub()
	_, next := s.GetEmission(p.Named())
	next.Complete = true
	var wg sync.WaitGroup
	wg.Go(func() { s.RecordEmitted(p.Named(), &next) })
	<-paused
	scoped := *seed
	scoped.Namespace = "late"
	if got := s.Productions(scoped.Named(), nil); len(got) != 0 {
		t.Errorf("late family projected withdrawn coverage: %v", got)
	}
	s.AddObject(&scoped)
	if got := s.Productions(scoped.Named(), nil); len(got) != 0 {
		t.Errorf("explicit admission reactivated withdrawn family: %v", got)
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
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{a}, Complete: true})
	target := s.shardFor(a.Obj.Named())
	if target == s.shardFor(p.Named()) {
		t.Fatal("fixture requires different producer and target shards")
	}
	target.mu.Lock()
	done := make(chan struct{})
	go func() {
		s.RecordEmitted(p.Named(), &Emission{Children: []Child{b}, Complete: true})
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
	artifact := s.GetArtifact(p.Named()).(*Emission)
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
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{emitted}, Complete: true})
	sh := s.shardFor(seed.Obj.Named())
	sh.mu.RLock()
	cell := sh.cells[seed.Obj.Named()]
	u := objectUpdate{obj: emitted.Obj, cell: cell}
	sh.captureLocked(seed.Obj.Named(), cell, &u.effective)
	sh.mu.RUnlock()
	u.compare()
	assert.Equal(t, u.effectiveEqual, true)
	var wg sync.WaitGroup
	wg.Go(func() {
		s.RecordEmitted(p.Named(), &Emission{Children: []Child{emissionChild("anchor", "new")}, Complete: true})
	})
	wg.Wait()
	if s.addAnchor(u) {
		t.Fatal("anchor accepted equality against a superseded effective record")
	}
	s.AddObject(emitted.Obj)
	assert.Equal(t, s.GetObject(seed.Obj.Named()), emitted.Obj)
	s.RecordEmitted(p.Named(), &Emission{Complete: true})
	assert.Equal(t, s.GetObject(seed.Obj.Named()), emitted.Obj)
}

func TestGetEmission_InputAuthorityStamp(t *testing.T) {
	t.Parallel()
	s := New()
	p := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	q := &manifest.ResourceSet{Name: "consumer", Namespace: "apps"}
	s.AddObject(p)
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{{Obj: q}}, Complete: true, Fingerprint: "a"})
	_, before := s.GetEmission(q.Named())
	before.Complete, before.Fingerprint = true, "unchanged-output"
	s.RecordEmitted(q.Named(), &before)
	slot := s.shardFor(q.Named()).slots[q.Named()]
	revision := slot.current.Load().revision
	var reads ReadSet
	reads.Init(s, q.Named(), nil)
	reads.GetObject(q.Named())
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{{Obj: q}}, Complete: true, Fingerprint: "b"})
	_, after := s.GetEmission(q.Named())
	if reads.Valid() {
		t.Fatal("producer authority transition failed to invalidate its reader")
	}
	after.Complete = true
	s.RecordEmitted(q.Named(), &after)
	assert.Equal(t, slot.current.Load().revision, revision)
	assert.Equal(t, after.inputHead.owner, before.inputHead.owner)
	assert.Equal(t, after.inputHead.generation, before.inputHead.generation)
	if after.inputHead.revision <= before.inputHead.revision {
		t.Fatal("stable publication reused its captured authority revision")
	}
	s.RecordEmitted(p.Named(), &Emission{Children: []Child{{Obj: q}}, Complete: true, Fingerprint: "b"})
	_, repeated := s.GetEmission(q.Named())
	assert.Equal(t, repeated.inputHead, after.inputHead)
}

func TestReadRegistration_SeedReplacement(t *testing.T) {
	t.Parallel()
	s := New()
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	child := &manifest.HelmRelease{Name: "child", Namespace: "apps", Values: map[string]any{"value": "old"}}
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{child.Named(): {Productions: []manifest.Production{{Owner: parent.Named()}}}}})
	s.AddObject(parent)
	s.AddObject(child)
	replacement := child.Clone()
	replacement.Values["value"] = "new"
	s.RecordEmitted(parent.Named(), &Emission{Children: []Child{{Obj: replacement}}, Complete: true})
	if actual := s.GetObject(child.Named()); actual != replacement {
		t.Fatal("seed admission masked its producer's replacement")
	}
}
