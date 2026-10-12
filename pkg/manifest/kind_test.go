package manifest

import (
	"slices"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestKinds_ImmutableFacts(t *testing.T) {
	want := slices.Collect(Kinds())
	for info := range Kinds() {
		info.Group = "foreign.example"
		info.Tracked = !info.Tracked
	}
	assert.Diff(t, slices.Collect(Kinds()), want)
	assert.Diff(t, KindOf(&ResourceSet{}), KindInfo{Group: FluxOperatorDomain, Kind: KindResourceSet, Namespaced: true, Tracked: true, Runnable: true})
	assert.Diff(t, KindOf(&ResourceSetInputProvider{}), KindInfo{Group: FluxOperatorDomain, Kind: KindResourceSetInputProvider, Namespaced: true, Tracked: true})
}

func TestKindOf_GroupAndScope(t *testing.T) {
	for _, tc := range []struct {
		name, api, kind string
		namespaced      bool
	}{
		{"builtin", "rbac.authorization.k8s.io/v1", "ClusterRole", false},
		{"upstream_scope", "admissionregistration.k8s.io/v1", "ValidatingAdmissionPolicy", false},
		{"ecosystem", "cert-manager.io/v1", "ClusterIssuer", false},
		{"foreign_scope_lookalike", "foreign.example/v1", "ClusterRole", true},
		{"foreign_flux_lookalike", "foreign.example/v1", KindBucket, true},
		{"unknown", "foreign.example/v1", "Widget", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := map[string]any{"apiVersion": tc.api, "kind": tc.kind, "metadata": map[string]any{"name": "object", "namespace": "authored"}}
			obj, _, _, err := ParseDoc(doc, ParseDocOptions{})
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, obj.Named().Namespace, "authored")
			assert.Diff(t, KindOf(obj), KindInfo{Group: apiGroup(tc.api), Kind: tc.kind, Namespaced: tc.namespaced})
		})
	}
}

func TestKindOf_TableSlots(t *testing.T) {
	for _, obj := range []BaseManifest{&Kustomization{}, &HelmRelease{}, &HelmRepository{}, &HelmChartSource{}, &GitRepository{}, &OCIRepository{}, &ExternalArtifact{}, &Bucket{}, &ResourceSet{}, &ResourceSetInputProvider{}, &ConfigMap{}, &Secret{}} {
		t.Run(obj.Named().Kind, func(t *testing.T) {
			assert.Diff(t, KindOf(obj), kindTable[kindIndex[obj.Named().Kind]].KindInfo)
			assert.Equal(t, testing.AllocsPerRun(100, func() { KindOf(obj) }), float64(0))
		})
	}
}
