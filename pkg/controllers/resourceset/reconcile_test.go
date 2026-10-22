package resourceset

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil/controllertest"
	"github.com/home-operations/flate/pkg/change"
	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

func TestReconcile_ParentUpdateDuringRender(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			tasks := task.NewBounded(workers)
			c := New(st, tasks, false)
			c.Configure(base.Options{})
			c.StartLifecycle()
			started, resume, rendered := make(chan struct{}), make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			t.Cleanup(func() { release(); tasks.BlockTillDone(); c.StartLifecycle() })
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			st.AddObject(parent)
			provider := &manifest.ResourceSetInputProvider{Name: "dynamic", Namespace: "apps", Type: fluxopv1.InputProviderGitHubBranch}
			st.AddObject(provider)
			st.UpdateStatus(provider.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			docA := map[string]any{
				"apiVersion": "fluxcd.controlplane.io/v1", "kind": manifest.KindResourceSet,
				"metadata": map[string]any{"name": "binding", "namespace": "apps"},
				"spec": map[string]any{
					"inputs":            []any{map[string]any{"name": "from-a"}},
					"inputsFrom":        []any{map[string]any{"kind": manifest.KindResourceSetInputProvider, "name": provider.Name}},
					"resourcesTemplate": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: << inputs.name >>, namespace: apps}\n",
				},
			}
			docB := manifest.DeepCopyMap(docA)
			docB["spec"].(map[string]any)["inputs"].([]any)[0].(map[string]any)["name"] = "from-b"
			publish := func(doc map[string]any) {
				publishDocuments(c, parent.Named(), []map[string]any{doc}, manifest.ParseDocOptions{DefaultNamespace: "apps"})
			}
			publish(docA)
			st.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			id := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "binding"}
			rs, ok := st.Get[*manifest.ResourceSet](id)
			if !ok {
				t.Fatal("parent did not emit input A")
			}
			_, wantFingerprint, err := renderWithAdmission(rs, resourceset.StoreResolver(st), "")
			if err != nil {
				t.Fatal(err)
			}
			execution := c.newExecution(id)
			defer execution.run.Close()
			var accepted atomic.Pointer[store.Emission]
			unsubscribe := st.AddListener(store.EventArtifactUpdated, func(other manifest.NamedResource, payload any) {
				if emission, ok := payload.(*store.Emission); other == id && ok && emission.Complete {
					accepted.Store(emission)
				}
			}, false)
			t.Cleanup(unsubscribe)
			tasks.Go(t.Context(), "parent", func(context.Context) {
				defer release()
				reached := false
				tasks.YieldSlot(func() {
					select {
					case <-started:
						reached = true
					case <-rendered:
					}
				})
				if !reached {
					t.Error("render never reached admission")
					return
				}
				publish(docB)
			})
			tasks.Go(t.Context(), "render", func(context.Context) {
				defer close(rendered)
				var attempt store.Emission
				docs, warnings, err := resourceset.Render(rs, resourceset.StoreResolver(st), func(content any) (bool, error) {
					fp, hit, err := execution.run.FingerprintDedup(id, content)
					if hit || err != nil {
						return hit, err
					}
					_, attempt = st.GetEmission(id)
					attempt.Fingerprint = fp
					close(started)
					tasks.YieldSlot(func() { <-resume })
					return false, nil
				})
				defer attempt.Finish()
				if err != nil {
					t.Error(err)
					return
				}
				for _, warning := range warnings {
					st.AddWarning(id, warning)
				}
				attempt.Children, attempt.Manifests = c.ParseChildren(c.Store.GetObject(id), docs, manifest.ParseDocOptions{})
				attempt.Complete = true
				c.Emit(id, &attempt)
			})
			tasks.BlockTillDone()
			if tasks.Failures() != 0 {
				t.Fatalf("task failures = %d", tasks.Failures())
			}
			emission := accepted.Load()
			if emission == nil || !emission.Complete || len(emission.Manifests) != 1 || emission.Fingerprint != wantFingerprint {
				t.Fatalf("accepted A emission = %+v; fingerprint want %s", emission, wantFingerprint)
			}
			if !hasRenderedDoc(emission, manifest.KindConfigMap, "from-a") {
				t.Fatal("accepted emission did not render input A")
			}
			current, ok := st.Get[*manifest.ResourceSet](id)
			if !ok || string(current.Inputs[0]["name"].Raw) != `"from-b"` {
				t.Fatalf("current input is not B: %+v", current)
			}
			if st.GetArtifact(id) != nil {
				t.Error("input B incorrectly accepts the output rendered from A")
			}
			publish(docA)
			if st.GetArtifact(id) != emission {
				t.Error("restoring exact input A did not expose the captured emission")
			}
			publish(docB)
			if st.GetArtifact(id) != nil {
				t.Error("restored input B incorrectly accepts A's emission")
			}
			if string(rs.Inputs[0]["name"].Raw) != `"from-a"` {
				t.Fatal("render mutated input A")
			}
		})
	}
}

func TestReconcile_ProviderInventorySettlement(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			t.Parallel()
			st := store.New()
			tasks := task.NewBounded(workers)
			rs := selectorFixture("reader", "blue")
			rs.ResourcesTemplate = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '<< inputs.provider.name >>', namespace: ns}\ndata: {value: '<< inputs.value >>'}"
			owner := &manifest.Kustomization{Name: "providers", Namespace: "ns"}
			st.AddObjects([]manifest.BaseManifest{rs, owner})
			st.UpdateStatus(owner.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			a := staticRSIP("a", "ns", map[string]string{"color": "blue"}, map[string]string{"value": "first"})
			b := staticRSIP("b", "ns", map[string]string{"color": "blue"}, map[string]string{"value": "second"})
			publish := func(providers ...*manifest.ResourceSetInputProvider) {
				t.Helper()
				children := make([]store.Child, 0, len(providers))
				for _, p := range providers {
					children = append(children, store.Child{Obj: p})
				}
				st.RecordEmitted(owner.Named(), &store.Emission{Children: children, Complete: true})
			}
			check := func(count int, status store.Status) *store.Emission {
				t.Helper()
				index := &manifest.DiscoveryIndex{Records: map[manifest.NamedResource]manifest.DiscoveryRecord{rs.Named(): {File: "reader.yaml"}}}
				filter := change.NewFilter(change.NewSet([]string{"reader.yaml"}), index, st)
				_, before := st.GetEmission(rs.Named())
				for _, child := range before.Children {
					if !filter.ShouldReconcile(child.Obj.Named()) {
						t.Fatal("accepted child missing before replay")
					}
				}
				before.Finish()
				c := New(st, tasks, false)
				c.Configure(base.Options{Filter: filter, Index: index})
				c.StartLifecycle()

				info := controllertest.DispatchToFixpoint(t.Context(), t, controllertest.Dispatch{Tasks: tasks, Store: st, ID: rs.Named(), ReconcileNode: c.ReconcileNode})
				if info.Status != status {
					t.Fatalf("status=%+v, want %s", info, status)
				}
				_, current := st.GetEmission(rs.Named())
				defer current.Finish()
				if len(current.Manifests) != count || len(current.Children) != count {
					t.Fatalf("inventory=%+v, want %d documents and children", current, count)
				}
				if status == store.StatusReady {
					for _, child := range current.Children {
						if !filter.ShouldReconcile(child.Obj.Named()) {
							t.Fatalf("accepted child %s missing from changed-only selection", child.Obj.Named())
						}
					}
				}
				accepted, _ := st.GetArtifact(rs.Named()).(*store.Emission)
				return accepted
			}
			publish(a)
			initial := check(1, store.StatusReady)
			replayed := check(1, store.StatusReady)
			if initial.Fingerprint != replayed.Fingerprint || !hasRenderedDoc(replayed, manifest.KindConfigMap, "a") {
				t.Fatal("replay lost accepted inventory")
			}
			publish(b, a)
			accepted := check(2, store.StatusReady)
			changed := *a
			changed.Status.ExportedInputs = []fluxopv1.ResourceSetInput{{"value": {Raw: []byte(`"current"`)}}}
			publish(&changed, b)
			updated := check(2, store.StatusReady)
			if updated.Fingerprint == accepted.Fingerprint {
				t.Fatal("changed provider replayed stale content")
			}
			assert.Diff(t, updated.Manifests[0]["data"], map[string]any{"value": "current"})
			accepted = updated
			broken := *a
			broken.DefaultValues = fluxopv1.ResourceSetInput{"value": {Raw: []byte("invalid")}}
			publish(&broken, b)
			check(0, store.StatusFailed)
			if failed, ok := st.GetArtifact(rs.Named()).(*store.Emission); ok && (len(failed.Manifests) != 0 || len(failed.Children) != 0) {
				t.Fatal("failed successor exposed output with changed provider inputs")
			}
			publish(&changed, b)
			recovered := check(2, store.StatusReady)
			assert.Diff(t, recovered.Manifests, accepted.Manifests)
			publish()
			check(0, store.StatusReady)
			if st.GetObject(a.Named()) != nil || st.GetObject(manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "ns", Name: "a"}) != nil {
				t.Fatal("complete empty inventory retained a provider or output")
			}
			publish(&broken)
			check(0, store.StatusFailed)
		})
	}
}
