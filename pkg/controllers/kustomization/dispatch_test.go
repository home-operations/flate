package kustomization

import (
	"testing"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// TestShouldDispatchAsObject_KnownKinds documents which kinds the
// parent Kustomization controller routes through AddObject vs.
// AddRendered. Pinning this matrix prevents a future contributor
// from accidentally widening or narrowing the set — a regression
// here would either re-introduce the two-phase emission race (by
// excluding a data kind) or trigger spurious reconciles on
// non-reconcilable kinds.
func TestShouldDispatchAsObject_KnownKinds(t *testing.T) {
	cases := []struct {
		name string
		obj  manifest.BaseManifest
		want bool
	}{
		{"Kustomization", &manifest.Kustomization{}, true},
		{"HelmRelease", &manifest.HelmRelease{}, true},
		{"HelmRepository", &manifest.HelmRepository{}, true},
		{"OCIRepository", &manifest.OCIRepository{}, true},
		{"GitRepository", &manifest.GitRepository{}, true},
		{"Bucket", &manifest.Bucket{}, true},
		{"HelmChartSource", &manifest.HelmChartSource{}, true},
		{"ExternalArtifact", &manifest.ExternalArtifact{}, true},
		{"ConfigMap", &manifest.ConfigMap{}, true},
		{"Secret", &manifest.Secret{}, true},
		{"RawObject falls through", &manifest.RawObject{Kind: "Service"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldDispatchAsObject(tc.obj); got != tc.want {
				t.Errorf("shouldDispatchAsObject(%T) = %v, want %v", tc.obj, got, tc.want)
			}
		})
	}
}

// TestIsLeafReconcilable defines the pass-1 vs pass-2 split. Only
// Kustomization and HelmRelease are "leaf reconcilables" — their
// controllers fire AS SOON AS AddObject lands and immediately try
// to expand substituteFrom / resolve chart sources, so they must be
// emitted in pass 2 (after ConfigMap / Secret / sources from pass 1
// are already in the store).
func TestIsLeafReconcilable(t *testing.T) {
	cases := []struct {
		name string
		obj  manifest.BaseManifest
		want bool
	}{
		// Pass 2 — wait for pass-1 data first.
		{"Kustomization is leaf", &manifest.Kustomization{}, true},
		{"HelmRelease is leaf", &manifest.HelmRelease{}, true},

		// Pass 1 — supply data that pass-2 leaves consume.
		{"ConfigMap is data, not leaf", &manifest.ConfigMap{}, false},
		{"Secret is data, not leaf", &manifest.Secret{}, false},
		{"HelmRepository is source, not leaf", &manifest.HelmRepository{}, false},
		{"OCIRepository is source, not leaf", &manifest.OCIRepository{}, false},
		{"GitRepository is source, not leaf", &manifest.GitRepository{}, false},
		{"Bucket is source, not leaf", &manifest.Bucket{}, false},
		{"HelmChartSource is source, not leaf", &manifest.HelmChartSource{}, false},
		{"ExternalArtifact is source, not leaf", &manifest.ExternalArtifact{}, false},
		{"RawObject is neither", &manifest.RawObject{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isLeafReconcilable(tc.obj); got != tc.want {
				t.Errorf("isLeafReconcilable(%T) = %v, want %v", tc.obj, got, tc.want)
			}
		})
	}
}

// TestPass1Vs Pass2Categories — the two helpers must agree on the
// pass-1-vs-pass-2 contract. Every leaf reconcilable must also be
// dispatch-as-object (otherwise it'd be silently swallowed by
// AddRendered). Non-leaf reconcilables can be either.
func TestPass1Pass2Categories(t *testing.T) {
	kinds := []manifest.BaseManifest{
		&manifest.Kustomization{},
		&manifest.HelmRelease{},
		&manifest.HelmRepository{},
		&manifest.OCIRepository{},
		&manifest.GitRepository{},
		&manifest.Bucket{},
		&manifest.HelmChartSource{},
		&manifest.ExternalArtifact{},
		&manifest.ConfigMap{},
		&manifest.Secret{},
		&manifest.RawObject{},
	}
	for _, k := range kinds {
		if isLeafReconcilable(k) && !shouldDispatchAsObject(k) {
			t.Errorf("%T is a leaf reconcilable but not dispatch-as-object", k)
		}
	}
}

func TestEmitRenderedChildrenBatchesLeafDispatch(t *testing.T) {
	s := store.New()
	c := &Controller{Controller: base.New(s, task.New())}
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
	})
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
	c := &Controller{Controller: base.New(s, task.New())}
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
	})

	if leaked {
		t.Error("kustomize.config build directive was emitted to the store (phantom Kustomization//)")
	}
	if s.GetObject(manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "real"}) == nil {
		t.Error("real Flux Kustomization child was not stored")
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
