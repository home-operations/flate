package store

import (
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestStore_EmittedParent_RecordsAndQueriesParent(t *testing.T) {
	r := New()
	parent := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster"}
	child := manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "default", Name: "demo"}

	if _, ok := r.EmittedParent(child); ok {
		t.Fatal("EmittedParent should return ok=false for unknown id")
	}

	r.RecordEmitted(parent, []manifest.NamedResource{child})
	r.AddObject(&manifest.HelmRelease{Name: child.Name, Namespace: child.Namespace})
	r.DeleteObject(child)

	got, ok := r.EmittedParent(child)
	if !ok {
		t.Fatal("EmittedParent should return ok=true after RecordEmitted")
	}
	if got != parent {
		t.Errorf("EmittedParent = %v, want %v", got, parent)
	}
}

func TestStore_EmittedParent_FirstWriterWins(t *testing.T) {
	r := New()
	child := manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "n", Name: "demo"}
	parentA := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "a"}
	parentB := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "b"}

	r.RecordEmitted(parentA, []manifest.NamedResource{child})
	r.RecordEmitted(parentB, []manifest.NamedResource{child})

	got, _ := r.EmittedParent(child)
	if got != parentA {
		t.Errorf("EmittedParent = %v, want first writer %v", got, parentA)
	}
}

func TestStore_RecordEmitted_AttributesEveryChild(t *testing.T) {
	for _, insertForeignFirst := range []bool{false, true} {
		for _, withdrawForeignFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("insertForeignFirst=%t/withdrawForeignFirst=%t", insertForeignFirst, withdrawForeignFirst), func(t *testing.T) {
				r := New()
				parent := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster"}
				other := parent
				other.Group = "example.test"
				a := &manifest.RawObject{APIVersion: manifest.HelmReleaseDomain + "/v2", Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "a"}
				b := a.Clone()
				b.APIVersion = "example.test/v1"
				c := a.Named()
				c.Name = "c"
				parents := map[manifest.NamedResource]manifest.NamedResource{a.Named(): parent, b.Named(): other}
				var events [numEventKinds + 1]map[manifest.NamedResource]int
				for kind := EventObjectAdded; int(kind) <= numEventKinds; kind++ {
					events[kind] = map[manifest.NamedResource]int{}
					r.AddListener(kind, func(id manifest.NamedResource, payload any) {
						events[kind][id]++
						got, ok := r.EmittedParent(id)
						assert.Equal(t, ok, true)
						assert.Equal(t, got, parents[id])
						switch kind {
						case EventObjectAdded:
							assert.Equal(t, payload.(manifest.BaseManifest).Named(), id)
						case EventStatusUpdated:
							assert.Equal(t, payload.(StatusInfo).Message, id.Group)
						case EventArtifactUpdated:
							assert.Equal(t, payload.(*SourceArtifact).URL, id.Group)
						}
					}, false)
				}
				pair := []*manifest.RawObject{a, b}
				if insertForeignFirst {
					pair[0], pair[1] = pair[1], pair[0]
				}
				for i, obj := range pair {
					id := obj.Named()
					children := []manifest.NamedResource{id}
					if id == a.Named() {
						children = append(children, c)
					}
					r.RecordEmitted(parents[id], children)
					r.AddObject(obj)
					r.UpdateStatus(id, StatusReady, id.Group)
					r.SetArtifact(id, &SourceArtifact{URL: id.Group})
					if i == 0 {
						absent := pair[1].Named()
						got, conds := r.Snapshot(absent)
						assert.Equal(t, got, nil)
						assert.Equal(t, len(conds), 0)
						assert.Equal(t, r.GetArtifact(absent), nil)
						for kind := EventObjectAdded; int(kind) <= numEventKinds; kind++ {
							assert.Equal(t, events[kind][absent], 0)
						}
					}
				}
				children := r.EmittedChildrenByParent()
				for _, ids := range children {
					slices.SortFunc(ids, manifest.NamedResource.Compare)
				}
				assert.Diff(t, children, map[manifest.NamedResource][]manifest.NamedResource{parent: {a.Named(), c}, other: {b.Named()}})
				for _, obj := range pair {
					id := obj.Named()
					got, conds := r.Snapshot(id)
					assert.Equal(t, got, manifest.BaseManifest(obj))
					assert.Diff(t, conds, []Condition{readyCondition(StatusReady, id.Group)})
					status, ok := r.GetStatus(id)
					assert.Equal(t, ok, true)
					assert.Equal(t, status.Status, StatusReady)
					assert.Equal(t, status.Message, id.Group)
					assert.Diff(t, r.GetArtifact(id), Artifact(&SourceArtifact{URL: id.Group}))
				}
				pair = []*manifest.RawObject{a, b}
				if withdrawForeignFirst {
					pair[0], pair[1] = pair[1], pair[0]
				}
				for i, obj := range pair {
					id := obj.Named()
					assert.Equal(t, r.DeleteObject(id), true)
					assert.Equal(t, r.GetObject(id), nil)
					assert.Equal(t, len(r.GetConditions(id)), 0)
					assert.Equal(t, r.GetArtifact(id), nil)
					got, ok := r.EmittedParent(id)
					assert.Equal(t, ok, true)
					assert.Equal(t, got, parents[id])
					if i == 0 {
						remaining := pair[1].Named()
						got, conds := r.Snapshot(remaining)
						assert.Equal(t, got, manifest.BaseManifest(pair[1]))
						assert.Diff(t, conds, []Condition{readyCondition(StatusReady, remaining.Group)})
						assert.Diff(t, r.GetArtifact(remaining), Artifact(&SourceArtifact{URL: remaining.Group}))
					}
				}
				for kind := EventObjectAdded; int(kind) <= numEventKinds; kind++ {
					assert.Diff(t, events[kind], map[manifest.NamedResource]int{a.Named(): 1, b.Named(): 1})
				}
			})
		}
	}
}

func TestStore_RecordEmitted_HonorsSelfEdgeAndFirstWriterWins(t *testing.T) {
	r := New()
	parent := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "cluster"}
	other := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "other"}
	child := manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "demo"}

	// Self-edge (parent == child) must be silently dropped.
	r.RecordEmitted(parent, []manifest.NamedResource{parent, child})
	if _, ok := r.EmittedParent(parent); ok {
		t.Error("self-edge should have been dropped, but EmittedParent(parent) returned ok=true")
	}
	if got, ok := r.EmittedParent(child); !ok || got != parent {
		t.Errorf("EmittedParent(child) = (%v, %v), want (%v, true)", got, ok, parent)
	}

	// Re-batching child under a different parent must not overwrite.
	r.RecordEmitted(other, []manifest.NamedResource{child})
	if got, _ := r.EmittedParent(child); got != parent {
		t.Errorf("EmittedParent(child) = %v after second batch, want first writer %v", got, parent)
	}
}

func TestStore_EmittedChildrenByParent_Concurrent(t *testing.T) {
	r := New()
	parent := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "apps", Name: "parent"}
	var wg sync.WaitGroup
	for worker := range 2 {
		wg.Go(func() {
			for i := range 100 {
				child := manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "apps", Name: fmt.Sprintf("%d-%d", worker, i)}
				r.RecordEmitted(parent, []manifest.NamedResource{child})
				r.EmittedChildrenByParent()
			}
		})
	}
	wg.Wait()
	snapshot := r.EmittedChildrenByParent()
	assert.Equal(t, len(snapshot[parent]), 200)
	snapshot[parent][0] = manifest.NamedResource{}
	delete(snapshot, parent)
	fresh := r.EmittedChildrenByParent()
	assert.Equal(t, len(fresh[parent]), 200)
	for _, child := range fresh[parent] {
		if child.Name == "" {
			t.Fatal("snapshot aliases rendered state")
		}
	}
}
