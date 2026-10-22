package depwait

import (
	"fmt"
	"testing"

	"github.com/home-operations/flate/pkg/schedule"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func TestClassify_CoreStates(t *testing.T) {
	s := store.New()
	ready := manifest.DependencyRef{NamedResource: testutil.KustomizationID("ns", "ready")}
	failed := manifest.DependencyRef{NamedResource: testutil.KustomizationID("ns", "failed")}
	pending := manifest.DependencyRef{NamedResource: testutil.KustomizationID("ns", "pending")}
	statusless := manifest.DependencyRef{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "ns", Name: "statusless"}
	absent := manifest.DependencyRef{NamedResource: testutil.KustomizationID("ns", "absent")}
	s.AddObject(&manifest.Kustomization{Name: ready.Name, Namespace: ready.Namespace})
	s.UpdateStatus(ready.NamedResource, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	s.UpdateStatus(failed.NamedResource, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "boom"})
	s.UpdateStatus(pending.NamedResource, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "working"})
	s.AddObject(&manifest.HelmChartSource{Name: "statusless", Namespace: "ns"})

	w := &Waiter{Store: s}

	if got := w.Classify(t.Context(), ready, manifest.DrainNone); got.Kind != schedule.Terminal {
		t.Errorf("ready: got %+v, want schedule.Terminal", got)
	}
	for _, reason := range []store.StatusReason{store.ReasonSkipped, store.ReasonUnchanged, store.ReasonSuspended} {
		t.Run(string(reason), func(t *testing.T) {
			s.UpdateStatus(ready.NamedResource, store.StatusInfo{Status: store.StatusReady, Reason: reason, Message: ""})
			if got := w.Classify(t.Context(), ready, manifest.DrainNone); got.Kind != schedule.Terminal {
				t.Fatalf("typed Ready reason %s blocked dependency: %+v", reason, got)
			}
		})
	}
	if got := w.Classify(t.Context(), failed, manifest.DrainNone); got.Kind != schedule.DependencyFailed || got.Message != "boom" {
		t.Errorf("failed: got %+v, want schedule.DependencyFailed 'boom'", got)
	}
	// Present-Pending: blocks at none/cascade, fails "not ready" only at force.
	if got := w.Classify(t.Context(), pending, manifest.DrainNone); got.Kind != schedule.Blocked {
		t.Errorf("pending@none: got %+v, want schedule.Blocked", got)
	}
	if got := w.Classify(t.Context(), pending, manifest.DrainCascade); got.Kind != schedule.Blocked {
		t.Errorf("pending@cascade: got %+v, want schedule.Blocked (cascade keeps Pending blocked)", got)
	}
	if got := w.Classify(t.Context(), pending, manifest.DrainForce); got.Kind != schedule.DependencyFailed || got.Message != "not ready" {
		t.Errorf("pending@force: got %+v, want schedule.DependencyFailed 'not ready'", got)
	}
	// Present with no Ready condition (a synthetic HelmChart before its source
	// worker's first write) classifies exactly like Pending.
	for level, want := range map[manifest.DrainLevel]Classification{
		manifest.DrainNone:    {Kind: schedule.Blocked},
		manifest.DrainCascade: {Kind: schedule.Blocked},
		manifest.DrainForce:   {Kind: schedule.DependencyFailed, Message: "not ready"},
	} {
		if got := w.Classify(t.Context(), statusless, level); got != want {
			t.Errorf("statusless@%v: got %+v, want %+v", level, got, want)
		}
	}
	// Absent: blocks at none, fails "dependency not found" once draining.
	if got := w.Classify(t.Context(), absent, manifest.DrainNone); got.Kind != schedule.Blocked {
		t.Errorf("absent@none: got %+v, want schedule.Blocked", got)
	}
	if got := w.Classify(t.Context(), absent, manifest.DrainCascade); got.Kind != schedule.DependencyFailed || got.Message != "dependency not found" {
		t.Errorf("absent@cascade: got %+v, want schedule.DependencyFailed 'dependency not found'", got)
	}
	for _, group := range []string{"", "example.test"} {
		t.Run("group="+group, func(t *testing.T) {
			foreign := ready
			foreign.Group = group
			if got := w.Classify(t.Context(), foreign, manifest.DrainNone); got.Kind != schedule.Blocked {
				t.Fatalf("absent lookalike borrowed Ready status: %+v", got)
			}
			if got := w.Classify(t.Context(), foreign, manifest.DrainCascade); got.Kind != schedule.DependencyFailed || got.Message != "dependency not found" {
				t.Fatalf("absent lookalike at drain: %+v", got)
			}
			s.UpdateStatus(foreign.NamedResource, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "foreign status"})
			if got := w.Classify(t.Context(), foreign, manifest.DrainNone); got.Kind != schedule.Terminal {
				t.Fatalf("status-only lookalike acquired Flux readiness: %+v", got)
			}
		})
	}
	for _, allow := range []bool{false, true} {
		w.AllowMissingCRDs = allow
		for _, group := range []string{"apiextensions.k8s.io", "example.test", ""} {
			t.Run(fmt.Sprintf("CRD/group=%s/allow=%t", group, allow), func(t *testing.T) {
				dep := manifest.DependencyRef{Group: group, Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.test"}
				for _, level := range []manifest.DrainLevel{manifest.DrainNone, manifest.DrainCascade, manifest.DrainForce} {
					want := Classification{Kind: schedule.DependencyFailed, Message: "dependency not found"}
					if level == manifest.DrainNone {
						want = Classification{Kind: schedule.Blocked}
					} else if allow && group == "apiextensions.k8s.io" {
						want = Classification{Kind: schedule.Terminal}
					}
					if got := w.Classify(t.Context(), dep, level); got != want {
						t.Fatalf("missing CRD at %v: %+v, want %+v", level, got, want)
					}
				}
			})
		}
	}
}

func TestClassify_ReplaceReadyExpr(t *testing.T) {
	t.Parallel()
	s := store.New()
	dep := manifest.DependencyRef{NamedResource: testutil.KustomizationID("ns", "infra")}
	s.UpdateStatus(dep.NamedResource, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	dep.ReadyExpr = new(`dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`) // not satisfied

	w := &Waiter{Store: s}
	if got := w.Classify(t.Context(), dep, manifest.DrainNone); got.Kind != schedule.Blocked {
		t.Errorf("replace ReadyExpr false@none: got %+v, want schedule.Blocked", got)
	}
	if got := w.Classify(t.Context(), dep, manifest.DrainCascade); got.Kind != schedule.DependencyFailed ||
		got.Message != "readyExpr timeout: context deadline exceeded" {
		t.Errorf("replace ReadyExpr false@cascade: got %+v, want schedule.DependencyFailed 'readyExpr timeout: context deadline exceeded'", got)
	}
}

func TestClassify_EdgeReadiness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		obj  manifest.BaseManifest
		expr string
		want schedule.Kind
	}{
		{name: "ready waits for pending", obj: &manifest.Kustomization{Name: "dependency", Namespace: "apps"}, want: schedule.Blocked},
		{name: "raw Flux group remains existence", obj: &manifest.RawObject{Kind: manifest.KindKustomization, APIVersion: manifest.FluxKustomizeDomain + "/v1", Name: "dependency", Namespace: "apps"}, want: schedule.Terminal},
		{name: "foreign kind remains existence", obj: &manifest.RawObject{Kind: manifest.KindKustomization, APIVersion: "example.com/v1", Name: "dependency", Namespace: "apps"}, want: schedule.Terminal},
		{name: "ConfigMap ignores pending and CEL", obj: &manifest.ConfigMap{Name: "dependency", Namespace: "apps"}, expr: "invalid [", want: schedule.Terminal},
		{name: "Secret ignores pending and CEL", obj: &manifest.Secret{Name: "dependency", Namespace: "apps"}, expr: "invalid [", want: schedule.Terminal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			s.AddObject(tc.obj)
			s.UpdateStatus(tc.obj.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "working"})
			w := &Waiter{Store: s}
			got := w.Classify(t.Context(), manifest.DependencyRef{NamedResource: tc.obj.Named(), ReadyExpr: new(tc.expr)}, manifest.DrainNone)
			if got.Kind != tc.want {
				t.Fatalf("classification = %+v, want %v", got, tc.want)
			}
		})
	}
}

func TestClassify_DependencyFailureRetainsCause(t *testing.T) {
	t.Parallel()
	s := store.New()
	a, b := testutil.KustomizationID("apps", "a"), testutil.KustomizationID("apps", "b")
	s.AddObject(&manifest.Kustomization{Name: a.Name, Namespace: a.Namespace})
	s.AddObject(&manifest.Kustomization{Name: b.Name, Namespace: b.Namespace})
	failure := &manifest.DependencyFailedError{Parent: a, Failed: []manifest.NamedResource{b}, Reasons: map[manifest.NamedResource]string{b: "not ready"}}
	s.UpdateFailure(a, failure)
	w := Waiter{Store: s, Parent: b}
	ref := manifest.DependencyRef{NamedResource: a}
	first := w.Classify(t.Context(), ref, manifest.DrainForce)
	if first != (Classification{Kind: schedule.DependencyFailed, Message: failure.Error()}) {
		t.Fatalf("derived failure: %+v", first)
	}
	nested := &manifest.DependencyFailedError{Parent: b, Failed: []manifest.NamedResource{a}, Reasons: map[manifest.NamedResource]string{a: first.Message}}
	s.UpdateFailure(b, nested)
	w.Parent = a
	next := w.Classify(t.Context(), manifest.DependencyRef{NamedResource: b}, manifest.DrainForce)
	if next != (Classification{Kind: schedule.DependencyFailed, Message: nested.Error()}) {
		t.Fatalf("nested failure lost its cause: %+v", next)
	}
	if got := w.Classify(t.Context(), manifest.DependencyRef{NamedResource: b}, manifest.DrainForce); got != next {
		t.Fatalf("unchanged failure changed: %+v", got)
	}
	s.UpdateStatus(a, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "leaf failure"})
	if got := w.Classify(t.Context(), ref, manifest.DrainForce); got.Message != "leaf failure" {
		t.Fatalf("direct failure lost its reason: %+v", got)
	}
}

func TestClassify_OrdinaryPromotion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		materialize bool
		withdraw    bool
		status      store.Status
		want        Classification
	}{
		{name: "absent", want: Classification{Kind: schedule.DependencyFailed, Message: "dependency not found"}},
		{name: "materialized", materialize: true, want: Classification{Kind: schedule.Terminal}},
		{name: "withdrawn", materialize: true, withdraw: true, want: Classification{Kind: schedule.DependencyFailed, Message: "dependency not found"}},
		{name: "ready", materialize: true, status: store.StatusReady, want: Classification{Kind: schedule.Terminal}},
		{name: "failed", materialize: true, status: store.StatusFailed, want: Classification{Kind: schedule.DependencyFailed, Message: "boom"}},
		{name: "pending", materialize: true, status: store.StatusPending, want: Classification{Kind: schedule.Blocked}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			var data manifest.BaseManifest = &manifest.ConfigMap{Name: "input", Namespace: "apps"}
			if tc.status != "" {
				data = &manifest.Kustomization{Name: "input", Namespace: "apps"}
				defer st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
					st.UpdateStatus(id, store.StatusInfo{Status: tc.status, Reason: "", Message: "boom"})
				}, false)()
			}
			promotion := &inputPromotion{store: st, data: data, materialize: tc.materialize}
			if tc.withdraw {
				defer st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
					st.DeleteObject(id)
				}, false)()
				promotion.before = func() {
					if promotion.calls > 1 {
						promotion.materialize = false
					}
				}
			}
			w := Waiter{Store: st, Existence: promotion}
			got := w.Classify(t.Context(), manifest.DependencyRef{NamedResource: data.Named()}, manifest.DrainCascade)
			if promotion.calls != 1 || got != tc.want {
				t.Fatalf("promotion calls=%d Classification=%+v, want %+v", promotion.calls, got, tc.want)
			}
		})
	}
}
