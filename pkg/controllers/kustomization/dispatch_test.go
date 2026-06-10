package kustomization

import (
	"testing"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestEmitRenderedChildrenBatchesLeafDispatch(t *testing.T) {
	s := store.New()
	c := &Controller{Controller: base.New(s, task.NewBounded(0), "kustomization")}
	idA := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "a"}
	idB := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "b"}
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "parent"}

	var sawBOnA bool
	s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == idA {
			sawBOnA = s.GetObject(idB) != nil
		}
	}, false)

	c.emitRenderedChildren(parent, []map[string]any{
		fluxKustomizationDoc("a", "b"),
		fluxKustomizationDoc("b", "a"),
	}, true)
	if !sawBOnA {
		t.Fatal("first leaf dispatch did not see later emitted sibling")
	}
}

// TestEmitRenderedChildren_DropsKustomizeBuildDirective regresses the phantom
// "Kustomization//" Store entry: a kustomization.yaml self-referenced in its
// own resources: makes `kustomize build` emit a kustomize.config.k8s.io
// Kustomization. That's a build input, not a cluster resource — it must never
// reach the Store (it arrives nameless and surfaces as "FAILED (no status
// reported)"). A real Flux Kustomization in the same batch is still emitted.
func TestEmitRenderedChildren_DropsKustomizeBuildDirective(t *testing.T) {
	s := store.New()
	c := &Controller{Controller: base.New(s, task.NewBounded(0), "kustomization")}
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "parent"}

	var leaked bool
	s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id.Kind == manifest.KindKustomization && id.Name == "" {
			leaked = true
		}
	}, false)

	c.emitRenderedChildren(parent, []map[string]any{
		kustomizeConfigDoc(),                 // build directive — must be dropped
		fluxKustomizationDoc("real", "real"), // real child — must be stored
	}, true)

	if leaked {
		t.Error("kustomize.config build directive was emitted to the store (phantom Kustomization//)")
	}
	if s.GetObject(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "real"}) == nil {
		t.Error("real Flux Kustomization child was not stored")
	}
}

// recordingTracker is a minimal base.RenderTracker that records the
// parent→children provenance MarkRenderedBatch is handed, so a test can
// assert the dedup-skip replay still reports provenance even when it no
// longer re-publishes the children to the store.
type recordingTracker struct {
	children map[manifest.NamedResource][]manifest.NamedResource
}

func (r *recordingTracker) MarkRenderedBatch(parent manifest.NamedResource, children []manifest.NamedResource) {
	if r.children == nil {
		r.children = map[manifest.NamedResource][]manifest.NamedResource{}
	}
	r.children[parent] = append(r.children[parent], children...)
}

// TestEmitRenderedChildren_DedupSkip_NoStoreWrites pins Part B of the
// re-emission-churn fix: the fingerprint-dedup replay (publish=false)
// must NOT re-AddObject the children — re-firing EventObjectAdded
// re-submits already-settled children, the churn that transiently
// un-Ready-s a parent KS and races quiescence ("parent ... not ready").
// The idempotent provenance side-effect (MarkRenderedBatch) MUST still
// fire so the parent index stays correct on every pass (#102). The
// fresh-render path (publish=true) still publishes.
func TestEmitRenderedChildren_DedupSkip_NoStoreWrites(t *testing.T) {
	s := store.New()
	c := &Controller{Controller: base.New(s, task.NewBounded(0), "kustomization")}
	rt := &recordingTracker{}
	c.SetRenderTracker(rt)

	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "parent"}
	child := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "real"}
	docs := []map[string]any{fluxKustomizationDoc("real", "real")}

	var events int
	s.AddListener(store.EventObjectAdded, func(_ manifest.NamedResource, _ any) {
		events++
	}, false)

	// Dedup-skip replay: no events, store untouched, provenance recorded.
	c.emitRenderedChildren(parent, docs, false)
	if events != 0 {
		t.Errorf("publish=false fired %d EventObjectAdded; want 0 (no republish churn)", events)
	}
	if s.GetObject(child) != nil {
		t.Error("publish=false AddObject'd a leaf child; want the store left untouched")
	}
	if got := rt.children[parent]; len(got) != 1 || got[0] != child {
		t.Errorf("publish=false provenance = %v; want [%v] (MarkRenderedBatch must still fire)", got, child)
	}

	// Fresh render: events fire and the child lands in the store.
	c.emitRenderedChildren(parent, docs, true)
	if events != 1 {
		t.Errorf("publish=true fired %d EventObjectAdded; want 1 (the leaf published)", events)
	}
	if s.GetObject(child) == nil {
		t.Error("publish=true did not store the leaf child")
	}
}

// kustomizeConfigDoc is the shape `kustomize build` emits when a
// kustomization.yaml lists itself in resources: a kustomize.config.k8s.io
// build directive (here with the helm-repos metadata seen in the wild).
func kustomizeConfigDoc() map[string]any {
	return map[string]any{
		"apiVersion": "kustomize.config.k8s.io/v1beta1",
		"kind":       "Kustomization",
		"metadata":   map[string]any{"name": "helm-repos", "namespace": "ns"},
		"resources":  []any{"a.yaml", "kustomization.yaml"},
	}
}

func fluxKustomizationDoc(name, dep string) map[string]any {
	return map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata": map[string]any{
			"name":      name,
			"namespace": "ns",
		},
		"spec": map[string]any{
			"interval": "10m",
			"path":     "./" + name,
			"sourceRef": map[string]any{
				"kind": "GitRepository",
				"name": "repo",
			},
			"dependsOn": []any{
				map[string]any{"name": dep},
			},
		},
	}
}
