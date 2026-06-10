package resourceset

import (
	"context"
	"testing"

	fluxopv1 "github.com/controlplaneio-fluxcd/flux-operator/api/v1"
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
	c.Configure(Options{})
	c.Start(context.Background())
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
	for _, drain := range []int{0, 1, 2} {
		if blocked, _ := c.ReconcileNode(context.Background(), id, drain); len(blocked) == 0 {
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
		ResourceSetInputProviderSpec: fluxopv1.ResourceSetInputProviderSpec{
			Type:          fluxopv1.InputProviderStatic,
			DefaultValues: vals,
		},
		Labels: labels,
	}
}

// findDoc reports whether the RS artifact carries a doc of the given kind
// and metadata.name.
func hasRenderedDoc(art *store.ResourceSetArtifact, kind, name string) bool {
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
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			InputsFrom: []fluxopv1.InputProviderReference{
				{Kind: manifest.KindResourceSetInputProvider, Name: "rsip"},
			},
			ResourcesTemplate: `---
apiVersion: example.com/v1
kind: Widget
metadata: {name: << inputs.user >>, namespace: flux-system}
`,
		},
	}
	s.AddObject(rs)

	// First dispatch (no RSIP yet): the named dep is absent, so the node
	// blocks rather than terminalizing.
	blocked, _ := c.ReconcileNode(context.Background(), rs.Named(), 0)
	if len(blocked) != 1 {
		t.Fatalf("first dispatch blocked = %v, want [rsip] (parked on the named RSIP)", blocked)
	}
	want := manifest.NamedResource{Kind: manifest.KindResourceSetInputProvider, Namespace: "flux-system", Name: "rsip"}
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
	art, _ := s.GetArtifact(rs.Named()).(*store.ResourceSetArtifact)
	if !hasRenderedDoc(art, "Widget", "alice") {
		t.Fatalf("expected rendered Widget/alice; artifact = %+v", art)
	}
	if art.Fingerprint == "" {
		t.Error("artifact missing fingerprint")
	}
}

// TestReconcile_SelectorInputsFrom_LateRSIP pins (b): a selector
// inputsFrom RS renders the complete set even when the matching RSIP
// arrives AFTER the first render. The first pass renders zero docs (no
// match); WantsDrainRerun reports true (so the scheduler re-runs at the
// fixpoint); a re-dispatch after the RSIP arrives renders the full set.
func TestReconcile_SelectorInputsFrom_LateRSIP(t *testing.T) {
	c, s := newController(t)
	rs := &manifest.ResourceSet{
		Name: "acl", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			InputsFrom: []fluxopv1.InputProviderReference{
				{Kind: manifest.KindResourceSetInputProvider, Selector: &metav1.LabelSelector{
					MatchLabels: map[string]string{"role": "db"},
				}},
			},
			// Guard on inputs so the no-match first pass (nil input set under
			// Flatten) renders nothing; once the RSIP arrives the input set is
			// populated and the Widget emits.
			ResourcesTemplate: `<< if inputs >>---
apiVersion: example.com/v1
kind: Widget
metadata: {name: << inputs.user >>, namespace: flux-system}
<< end >>`,
		},
	}
	s.AddObject(rs)

	// A selector-only RS always wants a drain rerun (no nameable dep).
	if !c.WantsDrainRerun(rs.Named()) {
		t.Fatal("WantsDrainRerun = false, want true for a selector-only inputsFrom RS")
	}

	// First dispatch: no matching RSIP, renders nothing, terminalizes Ready.
	info := dispatchToFixpoint(t, c, s, rs.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("first dispatch status = %+v, want Ready (empty render)", info)
	}
	if art, _ := s.GetArtifact(rs.Named()).(*store.ResourceSetArtifact); hasRenderedDoc(art, "Widget", "alice") {
		t.Fatal("first dispatch should render no Widget (RSIP not yet present)")
	}

	// RSIP arrives late → the drain-rerun re-dispatch renders the full set.
	s.AddObject(staticRSIP("rsip", "flux-system", map[string]string{"role": "db"}, map[string]string{"user": "alice"}))
	info = dispatchToFixpoint(t, c, s, rs.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("drain-rerun status = %+v, want Ready", info)
	}
	art, _ := s.GetArtifact(rs.Named()).(*store.ResourceSetArtifact)
	if !hasRenderedDoc(art, "Widget", "alice") {
		t.Fatalf("expected rendered Widget/alice after late RSIP; artifact = %+v", art)
	}
}

// TestReconcile_EmitsFluxChildToStore pins (c): a Flux-kind child (a
// Kustomization) emitted by the RS render lands in the store as a
// reconcilable object (AddObject), not merely as rendered data.
func TestReconcile_EmitsFluxChildToStore(t *testing.T) {
	c, s := newController(t)
	rs := &manifest.ResourceSet{
		Name: "fleet", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			ResourcesTemplate: `---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: child, namespace: flux-system}
spec:
  path: ./child
  sourceRef: {kind: GitRepository, name: flux-system}
`,
		},
	}
	s.AddObject(rs)

	info := dispatchToFixpoint(t, c, s, rs.Named())
	if info.Status != store.StatusReady {
		t.Fatalf("status = %+v, want Ready", info)
	}
	childID := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "flux-system", Name: "child"}
	child, ok := store.Get[*manifest.Kustomization](s, childID)
	if !ok || child == nil {
		t.Fatalf("expected RS-emitted Kustomization %s in store (AddObject), got ok=%v", childID, ok)
	}
}

// TestReconcile_RenderError_MarksFailed pins that a malformed RS template
// (referencing a nonexistent input) marks the RS Failed and surfaces the
// error — the failure path the deleted post-run pass used to own.
func TestReconcile_RenderError_MarksFailed(t *testing.T) {
	c, s := newController(t)
	rs := &manifest.ResourceSet{
		Name: "broken", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			ResourcesTemplate: `---
apiVersion: v1
kind: ConfigMap
metadata: {name: << .nonexistent.field >>}
`,
		},
	}
	s.AddObject(rs)

	info := dispatchToFixpoint(t, c, s, rs.Named())
	if info.Status != store.StatusFailed {
		t.Fatalf("status = %+v, want Failed", info)
	}
}

// TestWantsDrainRerun_NamedOnly pins that an RS whose inputsFrom are all
// NAMED (no selector) does NOT ask for a drain rerun — it parks on the
// named RSIP instead.
func TestWantsDrainRerun_NamedOnly(t *testing.T) {
	c, s := newController(t)
	rs := &manifest.ResourceSet{
		Name: "named", Namespace: "flux-system",
		ResourceSetSpec: fluxopv1.ResourceSetSpec{
			InputsFrom: []fluxopv1.InputProviderReference{
				{Kind: manifest.KindResourceSetInputProvider, Name: "rsip"},
			},
		},
	}
	s.AddObject(rs)
	if c.WantsDrainRerun(rs.Named()) {
		t.Error("WantsDrainRerun = true for a named-only inputsFrom RS, want false")
	}
	// Unknown id → false.
	if c.WantsDrainRerun(manifest.NamedResource{Kind: manifest.KindResourceSet, Name: "ghost"}) {
		t.Error("WantsDrainRerun = true for an absent RS, want false")
	}
}
