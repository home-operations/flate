package emit

import (
	"testing"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// TestShouldDispatchAsObject_KnownKinds documents which kinds the
// render-emission helper routes through AddObject vs. AddRendered.
// Pinning this matrix prevents a future contributor from accidentally
// widening or narrowing the set — a regression here would either
// re-introduce the two-phase emission race (by excluding a data kind)
// or trigger spurious reconciles on non-reconcilable kinds.
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
		{"ResourceSet", &manifest.ResourceSet{}, true},
		{"ResourceSetInputProvider", &manifest.ResourceSetInputProvider{}, true},
		{"RawObject falls through", &manifest.RawObject{Kind: "Service"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ShouldDispatchAsObject(tc.obj); got != tc.want {
				t.Errorf("ShouldDispatchAsObject(%T) = %v, want %v", tc.obj, got, tc.want)
			}
		})
	}
}

// TestIsLeafReconcilable defines the pass-1 vs pass-2 split. Only
// Kustomization and HelmRelease are "leaf reconcilables" — their
// controllers fire AS SOON AS AddObject lands and immediately try
// to expand substituteFrom / resolve chart sources, so they must be
// emitted in pass 2 (after ConfigMap / Secret / sources from pass 1
// are already in the store). A ResourceSet is pass-1 data so any
// children it re-emits land behind its own data.
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
		{"ResourceSet is data, not leaf", &manifest.ResourceSet{}, false},
		{"ResourceSetInputProvider is data, not leaf", &manifest.ResourceSetInputProvider{}, false},
		{"RawObject is neither", &manifest.RawObject{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsLeafReconcilable(tc.obj); got != tc.want {
				t.Errorf("IsLeafReconcilable(%T) = %v, want %v", tc.obj, got, tc.want)
			}
		})
	}
}

// TestPass1Pass2Categories — the two helpers must agree on the
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
		&manifest.ResourceSet{},
		&manifest.ResourceSetInputProvider{},
		&manifest.RawObject{},
	}
	for _, k := range kinds {
		if IsLeafReconcilable(k) && !ShouldDispatchAsObject(k) {
			t.Errorf("%T is a leaf reconcilable but not dispatch-as-object", k)
		}
	}
}

// TestChildren_DefaultsNamespace pins the fix for the unsubstituted-OCIRepository
// bug: a namespace-less rendered child must inherit the emitting parent's
// namespace, so it dedups against its namespace-inherited file-loaded copy
// instead of lingering at empty namespace. An explicit namespace is preserved.
func TestChildren_DefaultsNamespace(t *testing.T) {
	s := store.New()
	c := base.New(s, task.NewBounded(1), "test")
	parent := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "infra"}

	Children(c, true, parent, []map[string]any{
		{
			"apiVersion": "source.toolkit.fluxcd.io/v1",
			"kind":       "OCIRepository",
			"metadata":   map[string]any{"name": "certmanager"},
			"spec":       map[string]any{"url": "oci://quay.io/c", "provider": "generic"},
		},
		{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata":   map[string]any{"name": "cm", "namespace": "other"},
		},
	}, true)

	if s.GetObject(manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "flux-system", Name: "certmanager"}) == nil {
		t.Error("namespace-less OCIRepository was not defaulted to the parent namespace flux-system")
	}
	if obj := s.GetObject(manifest.NamedResource{Kind: manifest.KindOCIRepository, Namespace: "", Name: "certmanager"}); obj != nil {
		t.Errorf("empty-namespace OCIRepository phantom present: %v", obj.Named())
	}
	if s.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "other", Name: "cm"}) == nil {
		t.Error("explicit namespace not preserved for ConfigMap other/cm")
	}
}
