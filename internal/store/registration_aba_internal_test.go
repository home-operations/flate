package store

import (
	"math"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func testReadRegistrationNoABACase2(t *testing.T, testCaseInput *string,
) {
	t.Helper()

	s := New()
	obj := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
	s.AddObject(obj)
	child := emissionChild("child", "old")
	s.RecordEmitted(obj.Named(), &Emission{Children: []Child{child}, Complete: true}, nil)
	s.SetArtifact(child.Obj.Named(), &SourceArtifact{Key: "old"})
	id := child.Obj.Named()
	if (*testCaseInput) == "producer" {
		id = obj.Named()
	}
	sh := s.shardFor(id)
	sh.mu.Lock()
	switch *testCaseInput {
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
	switch *testCaseInput {
	case "artifact":
		reads.GetArtifact(id, ArtifactContent)
	case "producer":
		reads.GetObject(child.Obj.Named())
	case "bucket":
		reads.ListObjects(id.Group, id.Kind)
	}
	func() {
		defer func() {
			assert.ErrorIf(t, recover() == nil, "%s", "exhaustion did not fail before mutation")
		}()
		switch *testCaseInput {
		case "artifact":
			s.SetArtifact(id, &SourceArtifact{Key: "new"})
		case "producer":
			s.RecordEmitted(
				id,
				&Emission{Children: []Child{emissionChild("child", "new")}, Complete: true},
				nil,
			)
		case "bucket":
			s.AddObject(emissionChild("new", "new").Obj)
		}
	}()
	assert.Equal(t, reads.Valid(), true)
	assert.Equal(t, s.GetObject(child.Obj.Named()), child.Obj)
	assert.Equal(t, s.GetArtifact(child.Obj.Named(), ArtifactContent).(*SourceArtifact).Key, "old")
	assert.Equal(t, s.GetObject(emissionChild("new", "new").Obj.Named()) == nil, true)
}

func testReadRegistrationNoABAExhaustion(t *testing.T) {
	t.Helper()

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
			assert.ErrorIf(t, recover() == nil, "%s", "exhaustion did not fail before mutation")
		}()
		s.AddObject(b.Obj)
	}()
	assert.Equal(t, s.GetObject(a.Obj.Named()), a.Obj)
	assert.Equal(t, headsForTest(s, a.Obj.Named()).object, uint64(math.MaxUint64))
	assert.Equal(t, reads.Valid(), true)
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
	assert.FatalIf(t, current.Valid(), "%s", "withdrawal reused a captured stable-publication stamp")
	assert.Equal(t, earlier.Valid(), false)
}
