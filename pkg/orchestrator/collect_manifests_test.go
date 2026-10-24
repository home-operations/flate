package orchestrator

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/helm"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestCollectManifests_FailedSuccessorRetainsCompleted(t *testing.T) {
	t.Parallel()

	st := store.New()

	ready := testutil.HelmReleaseID("app", "ready")
	st.AddObject(&manifest.HelmRelease{Name: ready.Name, Namespace: ready.Namespace})
	st.RecordEmitted(ready, &store.Emission{Manifests: []map[string]any{
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "ready-cm"}},
	}})
	st.UpdateStatus(ready, store.StatusReady, "", "")

	failedHR := testutil.HelmReleaseID("app", "broken")
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
	st.UpdateStatus(failedHR, store.StatusFailed, "", failure)

	failedKS := testutil.KustomizationID("flux-system", "apps")
	st.AddObject(&manifest.Kustomization{Name: failedKS.Name, Namespace: failedKS.Namespace})
	st.RecordEmitted(failedKS, &store.Emission{Manifests: []map[string]any{
		{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": "apps"}},
	}})
	st.UpdateStatus(failedKS, store.StatusFailed, "", "kustomize build boom")

	o := &Orchestrator{store: st}
	got := outputByParent(o.collectManifests())

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
	if _, ok := outputByParent(o.collectManifests())[failedHR]; ok {
		t.Fatal("hidden producer contributed completed output")
	}
	t.Run("ResourceSets", func(t *testing.T) {
		t.Parallel()

		st := store.New()
		owners := []*manifest.ResourceSet{{Name: "a", Namespace: "apps"}, {Name: "z", Namespace: "apps"}}
		parent := testutil.KustomizationID("apps", "parent")
		st.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace})
		index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
		var winningDocs []map[string]any
		var winningChild store.Child
		for _, owner := range owners {
			owner.ResourcesTemplate = "accepted-" + owner.Name
			st.AddObject(owner)
			index.Records[owner.Named()] = manifest.DiscoveryRecord{Parent: parent}
			_, completed := st.GetEmission(owner.Named())
			doc := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "shared", "namespace": "apps"}, "data": map[string]any{"value": owner.Name}}
			completed.Children, completed.Manifests = base.ParseChildren(owner.Named(), []map[string]any{doc}, manifest.ParseDocOptions{})
			completed.Complete = true
			st.RecordEmitted(owner.Named(), &completed)
			completed.Finish()
			st.UpdateStatus(owner.Named(), store.StatusReady, "", "")
			winningDocs, winningChild = completed.Manifests, completed.Children[0]
		}
		winner := owners[1].Named()
		successor := *owners[1]
		successor.ResourcesTemplate = "invalid successor"
		st.AddObject(&successor)
		assert.Equal(t, st.GetArtifact(winner), store.Artifact(nil))
		st.UpdateStatus(winner, store.StatusFailed, "", "invalid ResourceSet template")
		o := &Orchestrator{store: st, index: index}
		assert.Diff(t, outputByParent(o.collectManifests()), map[manifest.NamedResource][]map[string]any{parent: winningDocs})
		assert.Equal(t, st.GetObject(winningChild.Obj.Named()), winningChild.Obj)
		assert.Equal(t, st.EmissionOwner(winningChild.Obj.Named()), winner)
		assert.Diff(t, st.Warnings(), []manifest.Warning{{Resource: winningChild.Obj.Named(), Category: manifest.WarnConflictingProducers, Count: 1,
			Message: "conflicting producers; using " + winner.String(), Detail: []string{owners[0].Named().String(), winner.String()}}})
		info, ok := st.FailedResources()[winner]
		assert.Equal(t, ok, true)
		assert.Equal(t, info.Message, "invalid ResourceSet template")
	})

}

func TestCollectManifests_MissingParent(t *testing.T) {
	t.Parallel()

	st := store.New()
	parent := testutil.KustomizationID("apps", "missing")
	rs := &manifest.ResourceSet{Name: "resources", Namespace: "apps"}
	st.AddObject(rs)
	doc := map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "settings", "namespace": "apps"}}
	children, docs := base.ParseChildren(rs.Named(), []map[string]any{doc}, manifest.ParseDocOptions{})
	st.RecordEmitted(rs.Named(), &store.Emission{Children: children, Manifests: docs, Complete: true})
	o := &Orchestrator{store: st, index: &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
		rs.Named(): {Parent: parent},
	}}}
	if batches := o.collectManifests(); len(batches) != 0 {
		t.Fatalf("missing parent contributed output: %+v", batches)
	}
}

func TestCollectManifests_ConflictingProducers(t *testing.T) {
	t.Parallel()

	for _, workers := range []int{2, 4} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/reverse=%t", workers, reverse), func(t *testing.T) {
				t.Parallel()

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
				got := outputByParent(o.collectManifests())
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
				if got := outputByParent(o.collectManifests()); len(got) != workers {
					t.Fatalf("equal ordinary producer groups changed: %v", got)
				}
			})
		}
	}
}

func TestCollectManifests_ParentlessResourceSetSharedChild(t *testing.T) {
	t.Parallel()

	for _, parentlessWinner := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("parentlessWinner=%t/reverse=%t", parentlessWinner, reverse), func(t *testing.T) {
				t.Parallel()

				st := store.New()
				parent := &manifest.Kustomization{Name: "p", Namespace: "apps"}
				a := &manifest.ResourceSet{Name: "a", Namespace: "apps"}
				z := &manifest.ResourceSet{Name: "z", Namespace: "apps"}
				st.AddObject(parent)
				st.AddObject(a)
				st.AddObject(z)
				attributed := z.Named()
				if parentlessWinner {
					attributed = a.Named()
				}
				index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{
					attributed: {Parent: parent.Named()},
				}}
				o := &Orchestrator{store: st, index: index}
				ids := []manifest.NamedResource{a.Named(), z.Named()}
				if reverse {
					slices.Reverse(ids)
				}
				var winningDocs []map[string]any
				for _, id := range ids {
					batch := []map[string]any{
						{"apiVersion": "example.com/v1", "kind": "Widget", "metadata": map[string]any{"name": "shared", "namespace": "apps"}, "spec": map[string]any{"value": id.Name}},
						{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "shared", "namespace": "apps"}, "data": map[string]any{"key": id.Name}},
						{"apiVersion": "v1", "kind": "Secret", "metadata": map[string]any{"name": "shared", "namespace": "apps"}, "stringData": map[string]any{"key": id.Name}},
					}
					children, docs := base.ParseChildren(id, batch, manifest.ParseDocOptions{})
					if len(children) != len(batch) || len(docs) != len(batch) {
						t.Fatalf("%s must publish every child: %v, %v", id, children, docs)
					}
					st.RecordEmitted(id, &store.Emission{Children: children, Manifests: docs, Complete: true})
					if id == z.Named() {
						winningDocs = docs
					}
				}
				want := map[manifest.NamedResource][]map[string]any{}
				if !parentlessWinner {
					want[parent.Named()] = winningDocs
				}
				assert.Diff(t, outputByParent(o.collectManifests()), want)
				assert.Diff(t, outputByParent(o.collectManifests()), want)
				cm, ok := st.Get[*manifest.ConfigMap](manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: "shared"})
				if !ok {
					t.Fatal("missing shared ConfigMap")
				}
				assert.Diff(t, cm.Data, map[string]any{"key": "z"})
				warnings := st.Warnings()
				assert.Equal(t, len(warnings), 3)
				for _, w := range warnings {
					assert.Equal(t, st.EmissionOwner(w.Resource), z.Named())
					assert.Equal(t, w.Message, "conflicting producers; using "+z.Named().String())
					assert.Diff(t, w.Detail, []string{a.Named().String(), z.Named().String()})
				}
			})
		}
	}
}

func TestCollectManifests_ResourceSetConflictingProducers(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{manifest.KindConfigMap, manifest.KindSecret, "ServiceAccount"} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%t", kind, reverse), func(t *testing.T) {
				t.Parallel()

				st := store.New()
				owners := []*manifest.ResourceSet{{Name: "a", Namespace: "apps"}, {Name: "z", Namespace: "apps"}}
				parents := []manifest.NamedResource{testutil.KustomizationID("apps", "parent-a"), testutil.KustomizationID("apps", "parent-z")}
				index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{}}
				emissions := make([]store.Emission, 2)
				for i, owner := range owners {
					st.AddObject(&manifest.Kustomization{Name: parents[i].Name, Namespace: parents[i].Namespace})
					st.AddObject(owner)
					index.Records[owner.Named()] = manifest.DiscoveryRecord{Parent: parents[i]}
					doc := map[string]any{"apiVersion": "v1", "kind": kind,
						"metadata": map[string]any{"name": "shared", "namespace": "apps", "annotations": map[string]any{"value": owner.Name}}}
					if kind == manifest.KindConfigMap {
						doc["data"] = map[string]any{"value": owner.Name}
					}
					if kind == manifest.KindSecret {
						doc["stringData"] = map[string]any{"value": owner.Name}
					}
					children, docs := base.ParseChildren(owner.Named(), []map[string]any{doc}, manifest.ParseDocOptions{})
					emissions[i] = store.Emission{Children: children, Manifests: docs, Complete: true}
				}
				release, done := make(chan struct{}), make(chan struct{})
				first, second := 0, 1
				if reverse {
					first, second = second, first
				}
				var wg sync.WaitGroup
				wg.Go(func() {
					st.RecordEmitted(owners[first].Named(), &emissions[first])
					close(release)
				})
				wg.Go(func() {
					<-release
					st.RecordEmitted(owners[second].Named(), &emissions[second])
					close(done)
				})
				<-done
				wg.Wait()
				o := &Orchestrator{store: st, index: index}
				id := emissions[1].Children[0].Obj.Named()
				check := func(winner int) {
					t.Helper()
					want := map[manifest.NamedResource][]map[string]any{parents[winner]: emissions[winner].Manifests}
					assert.Diff(t, outputByParent(o.collectManifests()), want)
					assert.Diff(t, outputByParent(o.collectManifests()), want)
					assert.Equal(t, st.EmissionOwner(id), owners[winner].Named())
					assert.Equal(t, st.GetObject(id), emissions[winner].Children[0].Obj)
				}
				check(1)
				assert.Diff(t, st.Warnings(), []manifest.Warning{{Resource: id, Category: manifest.WarnConflictingProducers, Count: 1,
					Message: "conflicting producers; using " + owners[1].Named().String(), Detail: []string{owners[0].Named().String(), owners[1].Named().String()}}})
				st.RecordEmitted(owners[1].Named(), &store.Emission{Complete: true})
				check(0)
				assert.Equal(t, len(st.Warnings()), 0)
				st.RecordEmitted(owners[1].Named(), &emissions[1])
				st.RecordEmitted(owners[0].Named(), &emissions[1])
				check(1)
				assert.Equal(t, len(st.Warnings()), 0)
			})
		}
	}
}

func TestCollectManifests_ResourceSetChildEligibility(t *testing.T) {
	t.Parallel()

	st := store.New()
	parent := testutil.KustomizationID("apps", "parent")
	st.AddObject(&manifest.Kustomization{Name: parent.Name, Namespace: parent.Namespace})
	rs := &manifest.ResourceSet{Name: "mixed", Namespace: "apps"}
	st.AddObject(rs)
	batch, err := manifest.DecodeDocs(strings.NewReader(`apiVersion: v1
kind: ServiceAccount
metadata: {name: raw, namespace: apps}
---
apiVersion: v1
kind: ConfigMap
metadata: {name: values, namespace: apps}
data: {key: value}
---
apiVersion: v1
kind: Secret
metadata: {name: credentials, namespace: apps}
stringData: {key: value}
---
apiVersion: fluxcd.controlplane.io/v1
kind: ResourceSetInputProvider
metadata: {name: inputs, namespace: apps}
spec: {type: Static, defaultValues: {name: one}}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: nested, namespace: apps}
spec: {interval: 1m, path: ./nested, sourceRef: {kind: GitRepository, name: flux-system}}
---
apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: nested, namespace: apps}
spec:
  interval: 1m
  chart:
    spec: {chart: app, sourceRef: {kind: HelmRepository, name: charts}}
`))
	assert.NoError(t, err)
	children, docs := base.ParseChildren(rs.Named(), batch, manifest.ParseDocOptions{})
	if len(children) != len(batch) {
		t.Fatalf("parsed %d of %d mixed children", len(children), len(batch))
	}
	st.RecordEmitted(rs.Named(), &store.Emission{Children: children, Manifests: docs, Complete: true})
	ks := testutil.KustomizationID("apps", "nested")
	hr := testutil.HelmReleaseID("apps", "nested")
	ksDocs := []map[string]any{{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": map[string]any{"name": "from-ks", "namespace": "apps"}}}
	hrDocs := []map[string]any{{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": map[string]any{"name": "from-hr", "namespace": "apps"}}}
	st.RecordEmitted(ks, &store.Emission{Manifests: ksDocs, Complete: true})
	st.RecordEmitted(hr, &store.Emission{Manifests: hrDocs, Complete: true})
	index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{rs.Named(): {Parent: parent}}}
	_, before := st.GetEmission(rs.Named())
	before.Finish()
	for i, child := range before.Children {
		before.Children[i].Doc = manifest.DeepCopyMap(child.Doc)
	}
	for i, doc := range before.Manifests {
		before.Manifests[i] = manifest.DeepCopyMap(doc)
	}
	for _, skip := range [][]string{nil, {manifest.KindConfigMap, manifest.KindSecret}} {
		t.Run(fmt.Sprintf("skip=%v", skip), func(t *testing.T) {
			t.Parallel()

			o := &Orchestrator{store: st, index: index, cfg: Config{HelmOptions: helm.Options{SkipKinds: skip}}}
			parentDocs := docs[:4]
			if len(skip) > 0 {
				parentDocs = []map[string]any{docs[0], docs[3]}
			}
			assert.Diff(t, outputByParent(o.collectManifests()), map[manifest.NamedResource][]map[string]any{
				parent: parentDocs,
				ks:     ksDocs,
				hr:     hrDocs,
			})
			_, after := st.GetEmission(rs.Named())
			after.Finish()
			if diff := cmp.Diff(after.Children, before.Children, cmp.AllowUnexported(manifest.Kustomization{}), cmpopts.IgnoreFields(manifest.Kustomization{}, "projection")); diff != "" {
				t.Fatalf("retained children changed (-want +got): %s", diff)
			}
			assert.Diff(t, after.Manifests, before.Manifests)
			cm, ok := st.Get[*manifest.ConfigMap](children[1].Obj.Named())
			if !ok || cm.Data["key"] != "value" {
				t.Fatal("collection discarded ConfigMap values")
			}
			secret, ok := st.Get[*manifest.Secret](children[2].Obj.Named())
			if !ok || secret.StringData["key"] != "value" {
				t.Fatal("collection discarded Secret values")
			}
		})
	}
}

func outputByParent(batches []manifest.DocumentBatch) map[manifest.NamedResource][]map[string]any {
	out := make(map[manifest.NamedResource][]map[string]any, len(batches))
	for _, batch := range batches {
		if batch.Parent != nil {
			out[batch.Parent.Named()] = batch.Documents
		}
	}
	return out
}
