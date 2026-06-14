package orchestrator

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestCollectManifests_FailedHelmReleaseEmitsNothing pins the cold-render-race
// fix: a HelmRelease that ends StatusFailed contributes zero docs to
// Result.Manifests even when an earlier transient render left it an artifact
// (the parent KS's postBuild.substituteFrom not-yet-applied window). A passing
// HelmRelease keeps its children, and a FAILED Kustomization's artifact is
// untouched — the gate is HelmRelease-scoped.
func TestCollectManifests_FailedHelmReleaseEmitsNothing(t *testing.T) {
	st := store.New()

	ready := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "app", Name: "ready"}
	st.AddObject(&manifest.HelmRelease{Name: ready.Name, Namespace: ready.Namespace})
	st.SetArtifact(ready, &store.HelmReleaseArtifact{Manifests: []map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "ready-cm"}},
	}})
	st.UpdateStatus(ready, store.StatusReady, "")

	// A stale artifact from a transient pre-substitution render that passed
	// schema; the canonical substituted render then failed and marked it Failed.
	failedHR := manifest.NamedResource{Kind: manifest.KindHelmRelease, Namespace: "app", Name: "broken"}
	st.AddObject(&manifest.HelmRelease{Name: failedHR.Name, Namespace: failedHR.Namespace})
	st.SetArtifact(failedHR, &store.HelmReleaseArtifact{Manifests: []map[string]any{
		{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "broken-deploy"}},
	}})
	st.UpdateStatus(failedHR, store.StatusFailed, "values don't meet the specifications of the schema(s)")

	// A FAILED Kustomization with an artifact must still surface — the gate is
	// HelmRelease-scoped (a KS render is a different lifecycle).
	failedKS := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	st.AddObject(&manifest.Kustomization{Name: failedKS.Name, Namespace: failedKS.Namespace})
	st.SetArtifact(failedKS, &store.KustomizationArtifact{Manifests: []map[string]any{
		{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "apps"}},
	}})
	st.UpdateStatus(failedKS, store.StatusFailed, "kustomize build boom")

	o := &Orchestrator{store: st}
	got := o.collectManifests(st.FailedResources())

	if _, ok := got[ready]; !ok {
		t.Errorf("passing HelmRelease %s must contribute its rendered children", ready)
	}
	if docs, ok := got[failedHR]; ok {
		t.Errorf("FAILED HelmRelease %s must emit nothing (stale transient artifact); got %d docs", failedHR, len(docs))
	}
	if _, ok := got[failedKS]; !ok {
		t.Errorf("FAILED Kustomization %s artifact must still surface (gate is HelmRelease-scoped)", failedKS)
	}
}
