package resourceset

import (
	"context"
	"fmt"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/controllers/base"
	"slices"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
	"github.com/google/go-cmp/cmp"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/task"
)

// newController wires a bare ResourceSet controller against a fresh
// store for synchronous unit dispatch (no orchestrator / scheduler).
func newController(t *testing.T) (*Controller, *store.Store) {
	t.Helper()
	s := store.New()
	tasks := task.NewBounded(0)
	c := New(s, tasks, true)
	c.Configure(base.Options{})
	c.StartLifecycle()
	t.Cleanup(func() {
		c.Close()
		tasks.BlockTillDone()
	})
	return c, s
}

// dispatchToFixpoint drives id through ReconcileNode with escalating
// drain levels (none→cascade→force), mirroring the scheduler's
// structural fixpoint, until the node terminalizes (no blocked deps) or
// drain is exhausted, then returns the final store status. Mirrors the
// kustomization controller test harness.
func dispatchToFixpoint(t *testing.T, c *Controller, s *store.Store, id manifest.NamedResource) store.StatusInfo {
	t.Helper()
	for _, drain := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade, manifest.DrainForce} {
		if blocked := c.ReconcileNode(context.Background(), id, drain); len(blocked) == 0 {
			break
		}
	}
	info, _ := s.GetStatus(id)
	return info
}

func staticRSIP(name, ns string, labels map[string]string, defaults map[string]string) *manifest.ResourceSetInputProvider {
	vals := fluxopv1.ResourceSetInput{}
	for k, v := range defaults {
		raw := apiextensionsv1.JSON{Raw: []byte(`"` + v + `"`)}
		vals[k] = &raw
	}
	return &manifest.ResourceSetInputProvider{
		Name: name, Namespace: ns,
		Type:          fluxopv1.InputProviderStatic,
		DefaultValues: vals,
		Labels:        labels,
	}
}

// findDoc reports whether the RS artifact carries a doc of the given kind
// and metadata.name.
func hasRenderedDoc(art *store.Emission, kind, name string) bool {
	if art == nil {
		return false
	}
	for _, doc := range art.Manifests {
		if k, _ := doc["kind"].(string); k != kind {
			continue
		}
		md, _ := doc["metadata"].(map[string]any)
		if n, _ := md["name"].(string); n == name {
			return true
		}
	}
	return false
}

// TestReconcile_NamedInputsFrom_ParksOnRSIP pins (a): a ResourceSet with
// a NAMED inputsFrom parks on the RSIP (blocked) until the RSIP is in the
// store, then renders the full input set once it arrives.
func TestReconcile_NamedInputsFrom_ParksOnRSIP(t *testing.T) {
	c, s := newController(t)
	rs := &manifest.ResourceSet{
		Name: "acl", Namespace: "flux-system",
		InputsFrom: []fluxopv1.InputProviderReference{
			{Name: "rsip"},
		},
		ResourcesTemplate: `---
apiVersion: example.com/v1
kind: Widget
metadata: {name: << inputs.user >>, namespace: flux-system}
`,
	}
	s.AddObject(rs)

	// First dispatch (no RSIP yet): the named dep is absent, so the node
	// blocks rather than terminalizing.
	blocked := c.ReconcileNode(context.Background(), rs.Named(), 0)
	if len(blocked) != 1 {
		t.Fatalf("first dispatch blocked = %v, want [rsip] (parked on the named RSIP)", blocked)
	}
	want := manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSetInputProvider, Namespace: "flux-system", Name: "rsip"}
	if blocked[0] != want {
		t.Fatalf("blocked on %v, want %v", blocked[0], want)
	}

	// RSIP arrives → re-dispatch renders the Widget from its inputs.
	s.AddObject(staticRSIP("rsip", "flux-system", nil, map[string]string{"user": "alice"}))
	s.UpdateStatus(want, store.StatusReady, "")
	info := dispatchToFixpoint(t, c, s, rs.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want Ready", info)
	}
	art, _ := s.GetArtifact(rs.Named()).(*store.Emission)
	if !hasRenderedDoc(art, "Widget", "alice") {
		t.Fatalf("expected rendered Widget/alice; artifact = %+v", art)
	}
	if art.Fingerprint == "" {
		t.Error("artifact missing fingerprint")
	}
}

func TestReconcile_SelectorInputsFrom_LateRSIP(t *testing.T) {
	s := store.New()
	tasks := task.NewBounded(2)
	c := New(s, tasks, true)
	c.Configure(base.Options{})
	c.StartLifecycle()
	defer c.Close()
	rs := selectorFixture("acl", "blue")
	s.AddObject(rs)
	if !c.WantsDrainRerun(rs.Named()) || len(c.collectDeps(rs)) != 0 {
		t.Fatal("selector-only input must request a rerun without an ID gate")
	}
	if info := dispatchToFixpoint(t, c, s, rs.Named()); info.Status != store.StatusReady {
		t.Fatalf("empty render status = %+v, want Ready", info)
	}
	if art, _ := s.GetArtifact(rs.Named()).(*store.Emission); art == nil || len(art.Manifests) != 0 {
		t.Fatalf("empty selection artifact = %+v", art)
	}
	for _, matches := range []bool{true, false} {
		started, written := make(chan struct{}), make(chan struct{})
		var blocked []manifest.NamedResource
		tasks.Go(t.Context(), "membership", func(context.Context) {
			tasks.YieldSlot(func() { <-started })
			color := "red"
			if matches {
				color = "blue"
			}
			s.AddObject(staticRSIP("rsip", "ns", map[string]string{"color": color}, nil))
			close(written)
		})
		tasks.Go(t.Context(), "selector", func(ctx context.Context) {
			close(started)
			tasks.YieldSlot(func() { <-written })
			blocked = c.ReconcileNode(ctx, rs.Named(), manifest.DrainNone)
		})
		tasks.BlockTillDone()
		art, _ := s.GetArtifact(rs.Named()).(*store.Emission)
		info, _ := s.GetStatus(rs.Named())
		if len(blocked) != 0 || tasks.Failures() != 0 || info.Status != store.StatusReady || hasRenderedDoc(art, "ConfigMap", "rsip") != matches {
			t.Fatalf("membership=%v: blocked=%v status=%+v artifact=%+v", matches, blocked, info, art)
		}
		if rs.InputsFrom[0].Selector.MatchLabels["color"] != "blue" {
			t.Fatal("selector mutation changed the authored spec")
		}
	}
}

// TestReconcile_EmitsFluxChildToStore pins (c): a Flux-kind child (a
// Kustomization) emitted by the RS render lands in the store as a
// reconcilable object (AddObject), not merely as rendered data.
func TestReconcile_EmitsFluxChildToStore(t *testing.T) {
	c, s := newController(t)
	rs := &manifest.ResourceSet{
		Name: "fleet", Namespace: "flux-system",
		ResourcesTemplate: `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child}
spec:
  path: ./child
  sourceRef: {kind: GitRepository, name: flux-system}
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: bare}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata: {name: bare}
`,
	}
	s.AddObject(rs)

	info := dispatchToFixpoint(t, c, s, rs.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want Ready", info)
	}
	childID := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "child"}
	child, ok := s.Get[*manifest.Kustomization](childID)
	if !ok || child == nil {
		t.Fatalf("expected RS-emitted Kustomization %s in store (AddObject), got ok=%v", childID, ok)
	}
	art := s.GetArtifact(rs.Named()).(*store.Emission)
	before := make([]map[string]any, len(art.Manifests))
	for i, doc := range art.Manifests {
		before[i] = manifest.DeepCopyMap(doc)
		name, ns := manifest.DocMetadata(doc)
		want := rs.Namespace
		if manifest.DocKind(doc) == "ClusterRole" {
			want = ""
		}
		if ns != want {
			t.Fatalf("rendered namespace = %q, want %q", ns, want)
		}
		if s.GetObject(manifest.NamedResource{Group: schema.FromAPIVersionAndKind(manifest.DocAPIVersion(doc), manifest.DocKind(doc)).Group, Kind: manifest.DocKind(doc), Namespace: want, Name: name}) == nil {
			t.Fatalf("missing effective child %s/%s", want, name)
		}
	}
	if info := dispatchToFixpoint(t, c, s, rs.Named()); info.Status != store.StatusReady {
		t.Fatalf("repeat reconcile: %+v", info)
	}
	if delta := cmp.Diff(before, art.Manifests); delta != "" {
		t.Fatalf("replay mutated output (-want +got):\n%s", delta)
	}

}

func TestReconcile_RenderError_MarksFailed(t *testing.T) {
	for _, fault := range []string{"template", "provider export", "provider resolution"} {
		t.Run(fault, func(t *testing.T) {
			c, s := newController(t)
			rs := &manifest.ResourceSet{
				Name: "broken", Namespace: "flux-system",
				ResourcesTemplate: "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output}\n",
			}
			provider := staticRSIP("input", rs.Namespace, nil, map[string]string{"value": "valid"})
			switch fault {
			case "provider export":
				rs.InputsFrom = []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSetInputProvider, Selector: &metav1.LabelSelector{}}}
			case "provider resolution":
				rs.InputsFrom = []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSetInputProvider, Name: provider.Name}}
				s.AddObject(provider)
			}
			s.AddObject(rs)
			if info := dispatchToFixpoint(t, c, s, rs.Named()); info.Status != store.StatusReady {
				t.Fatalf("initial render: %+v", info)
			}
			artifact := s.GetArtifact(rs.Named()).(*store.Emission)
			if artifact.Fingerprint == "" {
				t.Fatal("successful render lacks replay candidate")
			}
			switch fault {
			case "template":
				changed := *rs
				changed.ResourcesTemplate = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: << .nonexistent.field >>}\n"
				s.AddObject(&changed)
			case "provider export":
				changed := *provider
				changed.DefaultValues = fluxopv1.ResourceSetInput{"value": {Raw: []byte("invalid json")}}
				s.AddObject(&changed)
			case "provider resolution":
				changed := *rs
				changed.InputsFrom = []fluxopv1.InputProviderReference{{Kind: manifest.KindResourceSetInputProvider, Name: "missing"}}
				s.AddObject(&changed)
			}
			if info := dispatchToFixpoint(t, c, s, rs.Named()); info.Status != store.StatusFailed {
				t.Fatalf("status = %+v, want Failed", info)
			}
		})
	}
}

// TestWantsDrainRerun_NamedOnly pins that an RS whose inputsFrom are all
// NAMED (no selector) does NOT ask for a drain rerun — it parks on the
// named RSIP instead.
func TestWantsDrainRerun_NamedOnly(t *testing.T) {
	c, s := newController(t)
	rs := &manifest.ResourceSet{
		Name: "named", Namespace: "flux-system",
		InputsFrom: []fluxopv1.InputProviderReference{
			{Kind: manifest.KindResourceSetInputProvider, Name: "rsip"},
		},
	}
	s.AddObject(rs)
	if c.WantsDrainRerun(rs.Named()) {
		t.Error("WantsDrainRerun = true for a named-only inputsFrom RS, want false")
	}
	// Unknown id → false.
	if c.WantsDrainRerun(manifest.NamedResource{Group: manifest.FluxOperatorDomain, Kind: manifest.KindResourceSet, Name: "ghost"}) {
		t.Error("WantsDrainRerun = true for an absent RS, want false")
	}
}

func TestReconcile_AlreadyReady_NoTransientPending(t *testing.T) {
	c, s := newController(t)
	rs := &manifest.ResourceSet{Name: "ready", Namespace: "apps", ResourcesTemplate: `apiVersion: v1
kind: ConfigMap
metadata: {name: output}`}
	s.AddObject(rs)
	if info := dispatchToFixpoint(t, c, s, rs.Named()); info.Status != store.StatusReady {
		t.Fatalf("first status = %+v, want Ready", info)
	}
	artifact := s.GetArtifact(rs.Named())
	unsub := s.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
		if id == rs.Named() && payload.(store.StatusInfo).Status == store.StatusPending {
			t.Error("already-Ready ResourceSet transiently became Pending")
		}
	}, false)
	t.Cleanup(unsub)
	c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone)
	if info, _ := s.GetStatus(rs.Named()); info.Status != store.StatusReady {
		t.Errorf("final status = %+v, want Ready", info)
	}
	if s.GetArtifact(rs.Named()) != artifact {
		t.Error("no-op reconcile replaced artifact")
	}
}

func TestReconcile_DisabledUsesSuspensionGate(t *testing.T) {
	for _, value := range []string{fluxopv1.DisabledValue, "DISABLED"} {
		t.Run(value, func(t *testing.T) {
			c, s := newController(t)
			doc := map[string]any{
				"apiVersion": fluxopv1.GroupVersion.String(), "kind": manifest.KindResourceSet,
				"metadata": map[string]any{"name": "disabled", "namespace": "default", "annotations": map[string]any{fluxopv1.ReconcileAnnotation: value}},
				"spec":     map[string]any{"inputsFrom": []any{map[string]any{"name": "missing"}}, "resourcesTemplate": "<< inputs.missing >>"},
			}
			obj, _, _, err := manifest.ParseDoc(doc, manifest.ParseDocOptions{DefaultNamespace: ""})
			if err != nil {
				t.Fatal(err)
			}
			s.AddObject(obj)
			blocked := c.ReconcileNode(t.Context(), obj.Named(), manifest.DrainNone)
			if len(blocked) != 0 {
				t.Fatalf("disabled ResourceSet blocked on %v", blocked)
			}
			info, _ := s.GetStatus(obj.Named())
			if info.Status != store.StatusReady || info.Message != store.MsgSuspended {
				t.Fatalf("status = %+v, want Ready/suspended", info)
			}
			if art := s.GetArtifact(obj.Named()); art != nil {
				t.Fatalf("disabled ResourceSet rendered: %v", art)
			}
		})
	}
}

func TestReconcile_SuspensionAfterRefresh(t *testing.T) {
	for _, suspended := range []bool{false, true} {
		t.Run(fmt.Sprintf("suspended=%t", suspended), func(t *testing.T) {
			st := store.New()
			tasks := task.NewBounded(2)
			c := New(st, tasks, true)
			var deliveries atomic.Int64
			c.Configure(base.Options{})
			c.StartLifecycle()
			t.Cleanup(c.Close)
			parent := &manifest.Kustomization{Name: "parent", Namespace: "flux-system"}
			st.AddObject(parent)
			st.UpdateStatus(parent.Named(), store.StatusReady, "")
			provider := staticRSIP("provider", "flux-system", nil, map[string]string{"user": "rs-refresh-output"})
			st.AddObject(provider)
			st.UpdateStatus(provider.Named(), store.StatusReady, "")
			doc := map[string]any{
				"apiVersion": fluxopv1.GroupVersion.String(), "kind": manifest.KindResourceSet,
				"metadata": map[string]any{"name": "fleet", "namespace": "flux-system"},
				"spec": map[string]any{
					"inputsFrom": []any{map[string]any{"kind": manifest.KindResourceSetInputProvider, "name": provider.Name}},
					"resourcesTemplate": `apiVersion: example.com/v1
kind: Widget
metadata: {name: stale-output}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: rs-refresh-child}
spec: {path: ./child, sourceRef: {kind: GitRepository, name: flux-system}}
`,
				},
			}
			children, _ := publishDocuments(t, c, parent.Named(), []map[string]any{doc}, manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: parent.Named().Namespace})
			if len(children) != 1 {
				t.Fatalf("initial publication: %v", children)
			}
			rs := children[0].Obj.(*manifest.ResourceSet)
			original, _, _, err := manifest.ParseDoc(manifest.DeepCopyMap(doc), manifest.ParseDocOptions{DefaultNamespace: ""})
			if err != nil {
				t.Fatal(err)
			}
			st.UpdateStatus(rs.Named(), store.StatusPending, "")
			replacement := manifest.DeepCopyMap(doc)
			replacement["spec"].(map[string]any)["resourcesTemplate"] = `apiVersion: example.com/v1
kind: Widget
metadata: {name: << inputs.user >>}
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: rs-refresh-child}
spec: {path: ./child, sourceRef: {kind: GitRepository, name: flux-system}}
`
			if suspended {
				replacement["metadata"].(map[string]any)["annotations"] = map[string]any{fluxopv1.ReconcileAnnotation: fluxopv1.DisabledValue}
			}
			var observations atomic.Int64
			var fired, published atomic.Bool
			unsub := st.AddListener(store.EventStatusUpdated, func(id manifest.NamedResource, payload any) {
				info, ok := payload.(store.StatusInfo)
				if id != rs.Named() || !ok || info.Message != "resolving inputs" {
					return
				}
				observations.Add(1)
				if fired.Swap(true) {
					return
				}
				publishDocuments(t, c, parent.Named(), []map[string]any{replacement}, manifest.ParseDocOptions{WipeSecrets: false, DefaultNamespace: parent.Named().Namespace})
				published.Store(true)
			}, false)
			t.Cleanup(unsub)
			tasks.Go(t.Context(), "refresh", func(ctx context.Context) {
				if blocked := c.ReconcileNode(ctx, rs.Named(), manifest.DrainNone); len(blocked) != 0 {
					t.Errorf("blocked on %v", blocked)
				}
			})
			tasks.BlockTillDone()
			if observations.Load() != 1 || !published.Load() || tasks.Failures() != 0 {
				t.Fatalf("observations=%d published=%t panics=%d", observations.Load(), published.Load(), tasks.Failures())
			}
			fresh, ok := st.Get[*manifest.ResourceSet](rs.Named())
			if !ok || fresh == rs || fresh.Suspend != suspended || fresh.ResourcesTemplate != replacement["spec"].(map[string]any)["resourcesTemplate"] {
				t.Fatalf("canonical replacement = %+v", fresh)
			}
			if diff := cmp.Diff(original, rs); diff != "" {
				t.Fatalf("original mutated (-want +got):\n%s", diff)
			}
			if owners := testutil.Owners(st.Productions(rs.Named(), nil)); !slices.Contains(owners, parent.Named()) {
				t.Fatalf("missing independent contributor: %v", owners)
			}
			info, _ := st.GetStatus(rs.Named())
			art, _ := st.GetArtifact(rs.Named()).(*store.Emission)
			child := manifest.NamedResource{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "rs-refresh-child"}
			if suspended {
				if info.Status != store.StatusReady || info.Message != store.MsgSuspended {
					t.Errorf("status = %+v, want Ready/suspended", info)
				}
				_, inventory := st.GetEmission(rs.Named())
				if art != nil || st.GetObject(child) != nil || deliveries.Load() != 0 || len(inventory.Children) != 0 {
					t.Errorf("disabled replacement rendered: artifact=%+v child=%v raw deliveries=%d", art, st.GetObject(child), deliveries.Load())
				}
			} else if info.Status != store.StatusReady || !hasRenderedDoc(art, "Widget", "rs-refresh-output") || st.GetObject(child) == nil {
				t.Fatalf("active replacement output: status=%+v artifact=%+v child=%v raw deliveries=%d", info, art, st.GetObject(child), deliveries.Load())
			}
		})
	}
}

func TestReconcile_InputsFromPreservesAuthoredKind(t *testing.T) {
	c, s := newController(t)
	provider := staticRSIP("inputs", "apps", nil, map[string]string{"user": "alice"})
	s.AddObject(provider)
	obj, _, _, err := manifest.ParseDoc(map[string]any{
		"apiVersion": "fluxcd.controlplane.io/v1", "kind": manifest.KindResourceSet,
		"metadata": map[string]any{"name": "app", "namespace": "apps"},
		"spec": map[string]any{
			"inputsFrom":        []any{map[string]any{"kind": "Unsupported", "name": "inputs"}},
			"resourcesTemplate": "apiVersion: example.com/v1\nkind: Widget\nmetadata: {name: << inputs.user >>, namespace: apps}\n",
		},
	}, manifest.ParseDocOptions{DefaultNamespace: ""})
	if err != nil {
		t.Fatal(err)
	}
	rs := obj.(*manifest.ResourceSet)
	s.AddObject(rs)
	want := manifest.NamedResource{Kind: "Unsupported", Namespace: "apps", Name: provider.Name}
	deps := c.collectDeps(rs)
	if len(deps) != 1 || deps[0].NamedResource != want {
		t.Fatalf("input dependencies = %+v, want edge to %s", deps, want)
	}
	if blocked := c.ReconcileNode(t.Context(), rs.Named(), manifest.DrainNone); len(blocked) != 1 || blocked[0] != want {
		t.Fatalf("blocked = %v, want [%s]", blocked, want)
	}
	if info := dispatchToFixpoint(t, c, s, rs.Named()); info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want Failed", info)
	}
	if art := s.GetArtifact(rs.Named()); art != nil {
		t.Fatalf("unsupported input kind rendered: artifact = %+v", art)
	}
	if rs.InputsFrom[0].Kind != "Unsupported" {
		t.Fatal("authored input kind mutated")
	}
}
