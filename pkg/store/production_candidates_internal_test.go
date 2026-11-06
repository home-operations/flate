package store

import (
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReadSet_ListProductionCandidates(t *testing.T) {
	t.Parallel()
	s := New()
	producer := &manifest.ResourceSet{Name: "producer", Namespace: "apps"}
	child := &manifest.Kustomization{Name: "child", Namespace: "apps"}
	s.AddObject(producer)
	s.AddObject(child)
	var before ReadSet
	before.Init(s, manifest.NamedResource{}, nil)
	defer before.Retire()
	assert.Equal(
		t,
		len(before.ListProductionCandidates(manifest.FluxKustomizeDomain, manifest.KindKustomization, nil, nil)),
		0,
	)
	assert.Equal(t, before.Valid(), true)
	_, _, next := s.GetEmission(producer.Named())
	next.Children, next.Complete = []Child{{Obj: child}}, true
	s.RecordEmitted(producer.Named(), &next, nil)
	next.Finish()
	assert.Equal(t, before.Valid(), false)
	var current ReadSet
	current.Init(s, manifest.NamedResource{}, nil)
	defer current.Retire()
	objects := current.ListProductionCandidates(manifest.FluxKustomizeDomain, manifest.KindKustomization, nil, nil)
	if len(objects) != 1 || objects[0].Named() != child.Named() {
		t.Fatalf("emitted objects = %v, want child", objects)
	}
	assert.Equal(t, current.Valid(), true)
	var writer ReadSet
	writer.Init(s, producer.Named(), nil)
	defer writer.Retire()
	assert.Equal(
		t,
		len(writer.ListProductionCandidates(manifest.FluxKustomizeDomain, manifest.KindKustomization, nil, nil)),
		1,
	)
	_, _, repeated := s.GetEmission(producer.Named())
	repeated.Children, repeated.Complete = []Child{
		{Obj: child},
		{Obj: &manifest.ConfigMap{Name: "other", Namespace: "apps"}},
	}, true
	s.RecordEmitted(producer.Named(), &repeated, nil)
	repeated.Finish()
	assert.Equal(t, writer.Valid(), true)
	assert.Equal(t, current.Valid(), false)
	_, _, empty := s.GetEmission(producer.Named())
	empty.Children, empty.Manifests, empty.Complete = nil, nil, true
	s.RecordEmitted(producer.Named(), &empty, nil)
	empty.Finish()
	assert.Equal(t, current.Valid(), false)
	assert.Equal(
		t,
		len(s.ListProductionCandidates(manifest.FluxKustomizeDomain, manifest.KindKustomization, nil, nil)),
		0,
	)
	assert.Equal(t, s.GetObject(child.Named()) == child, true)
	_, _, self := s.GetEmission(child.Named())
	self.Children, self.Complete = []Child{{Obj: child}}, true
	s.RecordEmitted(child.Named(), &self, nil)
	self.Finish()
	assert.Equal(
		t,
		len(s.ListProductionCandidates(manifest.FluxKustomizeDomain, manifest.KindKustomization, nil, nil)),
		0,
	)
	bare := child.Named()
	bare.Namespace = ""
	s.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		bare: {Productions: []manifest.Production{{Owner: producer.Named()}}},
	}})
	var indexed ReadSet
	indexed.Init(s, manifest.NamedResource{}, nil)
	defer indexed.Retire()
	objects = indexed.ListProductionCandidates(manifest.FluxKustomizeDomain, manifest.KindKustomization, nil, nil)
	if len(objects) != 1 || objects[0].Named() != child.Named() {
		t.Fatalf("production candidates = %v, want indexed child", objects)
	}
	assert.Equal(t, indexed.Valid(), true)
	projected := child.Clone()
	projected.Namespace = "other"
	s.AddObject(projected)
	s.AddObject(&manifest.Kustomization{Name: "unrelated", Namespace: "apps"})
	var pending ReadSet
	pending.Init(s, manifest.NamedResource{}, nil)
	defer pending.Retire()
	objects = pending.ListProductionCandidates(
		manifest.FluxKustomizeDomain,
		manifest.KindKustomization,
		&manifest.DiscoveryIndex{},
		map[manifest.NamedResource]bool{bare: true},
	)
	assert.Equal(t, len(objects), 2)
	for _, object := range objects {
		assert.Equal(t, object.Named().Name, child.Name)
	}
	assert.Equal(t, pending.Valid(), true)
}
