package orchestrator

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestCollectManifests_FailedSuccessorRetainsCompleted(t *testing.T) {
	st := store.New()

	ready := manifest.NamedResource{Group: "helm.toolkit.fluxcd.io", Kind: manifest.KindHelmRelease, Namespace: "app", Name: "ready"}
	st.AddObject(&manifest.HelmRelease{Name: ready.Name, Namespace: ready.Namespace})
	st.RecordEmitted(ready, &store.Emission{Manifests: []map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "ready-cm"}},
	}})
	st.UpdateStatus(ready, store.StatusReady, "")

	failedHR := manifest.NamedResource{Group: "helm.toolkit.fluxcd.io", Kind: manifest.KindHelmRelease, Namespace: "app", Name: "broken"}
	hr := &manifest.HelmRelease{Name: failedHR.Name, Namespace: failedHR.Namespace}
	st.AddObject(hr)
	_, completed := st.GetEmission(failedHR)
	completed.Manifests = []map[string]any{
		{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "broken-deploy"}},
	}
	completed.Complete = true
	st.RecordEmitted(failedHR, &completed)
	completed.Finish()
	successor := hr.Clone()
	successor.Chart.Name = "different"
	st.AddObject(successor)
	if st.GetArtifact(failedHR) != nil {
		t.Fatal("successor exposed a current-input artifact")
	}
	failure := "values don't meet the specifications of the schema(s)"
	st.UpdateStatus(failedHR, store.StatusFailed, failure)

	failedKS := manifest.NamedResource{Group: "kustomize.toolkit.fluxcd.io", Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "apps"}
	st.AddObject(&manifest.Kustomization{Name: failedKS.Name, Namespace: failedKS.Namespace})
	st.RecordEmitted(failedKS, &store.Emission{Manifests: []map[string]any{
		{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "apps"}},
	}})
	st.UpdateStatus(failedKS, store.StatusFailed, "kustomize build boom")

	o := &Orchestrator{store: st}
	got := o.collectManifests()

	if _, ok := got[ready]; !ok {
		t.Errorf("passing HelmRelease %s must contribute its rendered children", ready)
	}
	if docs, ok := got[failedHR]; !ok || len(docs) != 1 {
		t.Errorf("failed HelmRelease %s lost completed output; got %d docs", failedHR, len(docs))
	}
	if _, ok := got[failedKS]; !ok {
		t.Errorf("failed Kustomization %s lost completed output", failedKS)
	}
	if info, ok := st.FailedResources()[failedHR]; !ok || !strings.Contains(info.Message, failure) {
		t.Errorf("failed successor lost diagnostics: %+v", info)
	}
	st.DeleteObject(failedHR)
	if _, ok := o.collectManifests()[failedHR]; ok {
		t.Fatal("hidden producer contributed completed output")
	}
}

func TestCollectManifests_ConflictingProducers(t *testing.T) {
	for _, workers := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/reverse=%t", workers, reverse), func(t *testing.T) {
				st := store.New()
				owners := make([]manifest.NamedResource, workers)
				release := make([]chan struct{}, workers)
				done := make(chan struct{})
				var wg sync.WaitGroup
				for i := range workers {
					obj := &manifest.Kustomization{Name: fmt.Sprint(i), Namespace: "apps"}
					owners[i] = obj.Named()
					st.AddObject(obj)
					release[i] = make(chan struct{})
					wg.Go(func() {
						<-release[i]
						doc := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "shared", "namespace": "apps"}, "data": map[string]any{"value": fmt.Sprint(i)}}
						children, docs := base.ParseChildren(owners[i], []map[string]any{doc}, manifest.ParseDocOptions{})
						st.RecordEmitted(owners[i], &store.Emission{Children: children, Manifests: docs, Complete: true})
						done <- struct{}{}
					})
				}
				order := slices.Clone(release)
				if reverse {
					slices.Reverse(order)
				}
				for _, ch := range order {
					close(ch)
					<-done
				}
				wg.Wait()
				o := &Orchestrator{store: st}
				got := o.collectManifests()
				if len(got) != workers {
					t.Fatalf("output lost producer documents: %v", got)
				}
				for i, owner := range owners {
					if len(got[owner]) != 1 || got[owner][0]["data"].(map[string]any)["value"] != fmt.Sprint(i) {
						t.Fatalf("output lost %s contribution: %v", owner, got[owner])
					}
				}
				for _, owner := range owners {
					children, docs := base.ParseChildren(owner, got[owners[workers-1]], manifest.ParseDocOptions{})
					st.RecordEmitted(owner, &store.Emission{Children: children, Manifests: docs, Complete: true})
				}
				if got := o.collectManifests(); len(got) != workers {
					t.Fatalf("equal ordinary producer groups changed: %v", got)
				}
			})
		}
	}
}
