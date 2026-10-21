package resourceset

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"

	"github.com/home-operations/flate/pkg/controllers/base"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/resourceset"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

type renderBarrierHandler struct {
	slog.Handler
	tasks   *task.Service
	once    sync.Once
	started chan struct{}
	resume  <-chan struct{}
}

func (h *renderBarrierHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Message == "resourceset: dynamic input provider contributes no inputs offline" {
		var resourceSet, provider string
		record.Attrs(func(attr slog.Attr) bool {
			switch attr.Key {
			case "resource_set":
				resourceSet = attr.Value.String()
			case "provider":
				provider = attr.Value.String()
			}
			return true
		})
		if resourceSet == "apps/binding" && provider == "apps/dynamic" {
			h.once.Do(func() {
				close(h.started)
				h.tasks.YieldSlot(func() { <-h.resume })
			})
		}
	}
	return h.Handler.Handle(ctx, record)
}

func TestReconcile_ParentUpdateDuringRender(t *testing.T) {
	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers_%d", workers), func(t *testing.T) {
			st := store.New()
			tasks := task.NewBounded(workers)
			c := New(st, tasks, false)
			c.Configure(base.Options{})
			c.StartLifecycle()
			started, resume, rendered := make(chan struct{}), make(chan struct{}), make(chan struct{})
			release := sync.OnceFunc(func() { close(resume) })
			t.Cleanup(func() { release(); tasks.BlockTillDone(); c.Close() })
			old := slog.Default()
			slog.SetDefault(slog.New(&renderBarrierHandler{Handler: slog.NewTextHandler(io.Discard, nil), tasks: tasks, started: started, resume: resume}))
			t.Cleanup(func() { slog.SetDefault(old) })
			parent := &manifest.Kustomization{Name: "parent", Namespace: "apps"}
			st.AddObject(parent)
			provider := &manifest.ResourceSetInputProvider{Name: "dynamic", Namespace: "apps", Type: fluxopv1.InputProviderGitHubBranch}
			st.AddObject(provider)
			st.UpdateStatus(provider.Named(), store.StatusReady, "", "")
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
			st.UpdateStatus(parent.Named(), store.StatusReady, "", "")
			id := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Namespace: "apps", Name: "binding"}
			rs, ok := st.Get[*manifest.ResourceSet](id)
			if !ok {
				t.Fatal("parent did not emit input A")
			}
			wantFingerprint := manifest.Fingerprint(resourceSetContent(rs, resourceset.StoreResolver(st)))
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
					t.Error("render never reached dynamic provider warning")
					return
				}
				publish(docB)
			})
			tasks.Go(t.Context(), "render", func(ctx context.Context) {
				defer close(rendered)
				if blocked := c.ReconcileNode(ctx, id, manifest.DrainNone); len(blocked) != 0 {
					t.Errorf("blocked on %v", blocked)
				}
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
