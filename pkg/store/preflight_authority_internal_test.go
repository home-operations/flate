package store

import (
	"slices"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestReadSet_PreflightAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name              string
		emitted, accepted bool
	}{
		{name: "static capture"},
		{name: "static accepted", accepted: true},
		{name: "emitted capture", emitted: true},
		{name: "emitted accepted", emitted: true, accepted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := New()
			object := &manifest.Kustomization{Name: "member", Namespace: "apps"}
			peer := ownerID("peer")
			if tc.emitted {
				producer := &manifest.Kustomization{Name: "producer", Namespace: "apps"}
				st.AddObject(producer)
				st.RecordEmitted(producer.Named(), &Emission{Children: []Child{{Obj: object}}, Complete: true}, nil)
			} else {
				st.AddObject(object)
			}
			id := object.Named()
			var captured ReadSet
			captured.Init(st, id, nil)
			t.Cleanup(captured.Close)
			assert.Equal(t, captured.Preflight(), "")
			assert.Equal(t, captured.input(id).view.owner != nil, tc.emitted)
			if tc.accepted {
				captured.Accept(AssessmentReconciled)
			}
			head := captured.originHead
			previous := head.assessments[0].Load()
			assert.Equal(t, previous != nil, tc.accepted)
			peerPublished, complete := make(chan struct{}), make(chan struct{})
			var publishers sync.WaitGroup
			publishers.Go(func() {
				st.SetPreflight(peer, "peer cycle")
				close(peerPublished)
			})
			publishers.Go(func() {
				<-complete
				st.SetPreflight(id, "member cycle")
			})
			<-peerPublished
			partialValid, partialMessage := captured.Valid(), captured.Preflight()
			close(complete)
			publishers.Wait()
			assert.Equal(t, partialValid, true)
			assert.Equal(t, partialMessage, "")
			assert.Equal(t, captured.Preflight(), "")
			assert.Equal(t, captured.Valid(), false)
			captured.Accept(AssessmentReconciled)
			assert.Equal(t, head.assessments[0].Load(), previous)
			assert.Diff(t, slices.Collect(st.Sweep()), []manifest.NamedResource{id})
			var current ReadSet
			current.Init(st, id, nil)
			t.Cleanup(current.Close)
			assert.Equal(t, current.Preflight(), "member cycle")
			assert.Equal(t, current.ReplacesInvalid(), tc.accepted)
			current.Accept(AssessmentReconciled)
			assert.Equal(t, head.assessments[0].Load().reads, &current)
			st.SetPreflight(id, "")
			assert.Equal(t, current.Preflight(), "member cycle")
			assert.Equal(t, current.Valid(), false)
			assert.Diff(t, slices.Collect(st.Sweep()), []manifest.NamedResource{id})
			assert.Equal(t, st.GetObject(id), manifest.BaseManifest(object))
			var cleared ReadSet
			cleared.Init(st, id, nil)
			t.Cleanup(cleared.Close)
			assert.Equal(t, cleared.Preflight(), "")
			assert.Equal(t, cleared.ReplacesInvalid(), true)
			cleared.Accept(AssessmentReconciled)
			assert.Equal(t, cleared.Valid(), true)
			assert.Equal(t, head.assessments[0].Load().reads, &cleared)
		})
	}
}

func TestReadSet_PreflightWithConsumedAdmission(t *testing.T) {
	t.Parallel()
	st := New()
	object := &manifest.Kustomization{Name: "consumer", Namespace: "apps"}
	child := emissionChild("parked", "raw")
	st.AddObject(object)
	st.SetArtifact(manifest.BootstrapSourceID, &SourceArtifact{Content: "source"})
	st.SetDiscoveryIndex(&manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		child.Obj.Named(): {
			BuilderOwner: ownerID("builder"),
			File:         "kustomization.yaml",
			Builder:      "kustomization.yaml",
		},
	}})
	var initial ReadSet
	initial.Init(st, ownerID("initial"), nil)
	initial.Admit(emissionChild("parked", "previous"), manifest.BootstrapSourceID, manifest.NamedResource{})
	initial.Retire()
	var captured ReadSet
	captured.Init(st, object.Named(), nil)
	t.Cleanup(captured.Close)
	assert.Equal(t, captured.Preflight(), "")
	assert.Equal(t, captured.GetDeclaration(child.Obj.Named()), manifest.BaseManifest(nil))
	child.SourceSupport = manifest.BootstrapSourceID
	captured.Admit(child, manifest.BootstrapSourceID, manifest.NamedResource{})
	assert.Equal(t, captured.input(child.Obj.Named()).object.consumed.Load(), true)
	assert.Equal(t, captured.input(object.Named()).object.consumed.Load(), false)
	assert.Equal(t, captured.Valid(), true)
	captured.Accept(AssessmentReconciled)
	assert.Equal(t, captured.accepted.Load(), true)
	st.SetPreflight(object.Named(), "consumer cycle")
	assert.Equal(t, captured.Valid(), false)
	assert.Diff(t, slices.Collect(st.Sweep()), []manifest.NamedResource{object.Named()})
}
