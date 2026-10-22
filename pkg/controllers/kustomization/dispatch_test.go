package kustomization

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestEmitRenderedChildrenBatchesLeafDispatch(t *testing.T) {
	t.Parallel()
	s := store.New()
	c := &Controller{Controller: base.New(s, task.NewBounded(0))}
	idA := testutil.KustomizationID("ns", "a")
	idB := testutil.KustomizationID("ns", "b")
	parent := testutil.KustomizationID("ns", "parent")

	rs := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "ns", Name: "render-only"}
	var sawBOnA bool
	var sawRS bool
	s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id == rs {
			sawRS = true
			if owners := testutil.Owners(s.Productions(rs, nil)); !slices.Contains(owners, parent) {
				t.Fatalf("missing independent contributor: %v", owners)
			}
		}
		if id == idA {
			sawBOnA = s.GetObject(idB) != nil
		}
	}, false)

	publishDocuments(c, parent, []map[string]any{
		{"apiVersion": "fluxcd.controlplane.io/v1", "kind": "ResourceSet", "metadata": map[string]any{"name": rs.Name, "namespace": rs.Namespace}, "spec": map[string]any{}},
		fluxKustomizationDoc("a", "b"),
		fluxKustomizationDoc("b", "a"),
	}, manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: parent.Namespace})
	if !sawRS {
		t.Fatal("render-only ResourceSet was not published")
	}
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
	t.Parallel()
	s := store.New()
	c := &Controller{Controller: base.New(s, task.NewBounded(0))}
	parent := testutil.KustomizationID("ns", "parent")

	var leaked bool
	s.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
		if id.Kind == manifest.KindKustomization && id.Name == "" {
			leaked = true
		}
	}, false)

	publishDocuments(c, parent, []map[string]any{
		kustomizeConfigDoc(),                 // build directive — must be dropped
		fluxKustomizationDoc("real", "real"), // real child — must be stored
	}, manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: parent.Namespace})

	if leaked {
		t.Error("kustomize.config build directive was emitted to the store (phantom Kustomization//)")
	}
	if s.GetObject(testutil.KustomizationID("ns", "real")) == nil {
		t.Error("real Flux Kustomization child was not stored")
	}
}

func TestEmitRenderedChildren_EqualReplay(t *testing.T) {
	t.Parallel()

	s := store.New()
	c := New(s, nil, nil, false)
	parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
	s.AddObject(parent)
	docs := []map[string]any{{"apiVersion": "kustomize.toolkit.fluxcd.io/v1", "kind": "Kustomization", "metadata": map[string]any{"name": "child", "namespace": "apps"}, "spec": map[string]any{"path": "./child"}}}
	children, retained := parseChildrenForTest(parent.Named(), docs, manifest.ParseDocOptions{})
	next := &store.Emission{Children: children, Manifests: retained, Complete: true}
	c.Emit(parent.Named(), next, nil)
	arrivals := 0
	s.AddListener(store.EventObjectAdded, func(manifest.NamedResource, any) { arrivals++ }, false)
	c.Emit(parent.Named(), next, nil)
	if arrivals != 0 {
		t.Fatalf("equal accepted replay fired %d arrivals", arrivals)
	}
	if s.GetObject(children[0].Obj.Named()) == nil {
		t.Fatal("equal replay lost accepted child")
	}
}

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
