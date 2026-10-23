package depwait

import (
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
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
	s.UpdateStatus(ready.NamedResource, store.StatusReady, "", "")
	s.UpdateStatus(failed.NamedResource, store.StatusFailed, "", "boom")
	s.UpdateStatus(pending.NamedResource, store.StatusPending, "", "working")
	s.AddObject(&manifest.HelmChartSource{Name: "statusless", Namespace: "ns"})

	w := &Waiter{Store: s}

	if got := w.classify(ready, manifest.DrainNone, w.Existence); got.Kind != schedule.Terminal {
		t.Errorf("ready: got %+v, want ClassReady", got)
	}
	for _, reason := range []store.StatusReason{store.ReasonSkipped, store.ReasonUnchanged, store.ReasonSuspended} {
		t.Run(string(reason), func(t *testing.T) {
			s.UpdateStatus(ready.NamedResource, store.StatusReady, reason, "")
			if got := w.classify(ready, manifest.DrainNone, w.Existence); got.Kind != schedule.Terminal {
				t.Fatalf("typed Ready reason %s blocked dependency: %+v", reason, got)
			}
		})
	}
	if got := w.classify(failed, manifest.DrainNone, w.Existence); got.Kind != schedule.DependencyFailed || got.Message != "boom" {
		t.Errorf("failed: got %+v, want ClassFailed 'boom'", got)
	}
	// Present-Pending: blocks at none/cascade, fails "not ready" only at force.
	if got := w.classify(pending, manifest.DrainNone, w.Existence); got.Kind != schedule.Blocked {
		t.Errorf("pending@none: got %+v, want ClassBlocked", got)
	}
	if got := w.classify(pending, manifest.DrainCascade, w.Existence); got.Kind != schedule.Blocked {
		t.Errorf("pending@cascade: got %+v, want ClassBlocked (cascade keeps Pending blocked)", got)
	}
	if got := w.classify(pending, manifest.DrainForce, w.Existence); got.Kind != schedule.DependencyFailed || got.Message != "not ready" {
		t.Errorf("pending@force: got %+v, want ClassFailed 'not ready'", got)
	}
	// Present with no Ready condition (a synthetic HelmChart before its source
	// worker's first write) classifies exactly like Pending.
	for level, want := range map[manifest.DrainLevel]classification{
		manifest.DrainNone:    {Kind: schedule.Blocked},
		manifest.DrainCascade: {Kind: schedule.Blocked},
		manifest.DrainForce:   {Kind: schedule.DependencyFailed, Message: "not ready"},
	} {
		if got := w.classify(statusless, level, w.Existence); got != want {
			t.Errorf("statusless@%v: got %+v, want %+v", level, got, want)
		}
	}
	// Absent: blocks at none, fails "dependency not found" once draining.
	if got := w.classify(absent, manifest.DrainNone, w.Existence); got.Kind != schedule.Blocked {
		t.Errorf("absent@none: got %+v, want ClassBlocked", got)
	}
	if got := w.classify(absent, manifest.DrainCascade, w.Existence); got.Kind != schedule.DependencyFailed || got.Message != "dependency not found" {
		t.Errorf("absent@cascade: got %+v, want ClassFailed 'dependency not found'", got)
	}
	for _, group := range []string{"", "example.test"} {
		t.Run("group="+group, func(t *testing.T) {
			foreign := ready
			foreign.Group = group
			if got := w.classify(foreign, manifest.DrainNone, w.Existence); got.Kind != schedule.Blocked {
				t.Fatalf("absent lookalike borrowed Ready status: %+v", got)
			}
			if got := w.classify(foreign, manifest.DrainCascade, w.Existence); got.Kind != schedule.DependencyFailed || got.Message != "dependency not found" {
				t.Fatalf("absent lookalike at drain: %+v", got)
			}
			s.UpdateStatus(foreign.NamedResource, store.StatusPending, "", "foreign status")
			if got := w.classify(foreign, manifest.DrainNone, w.Existence); got.Kind != schedule.Terminal {
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
					want := classification{Kind: schedule.DependencyFailed, Message: "dependency not found"}
					if level == manifest.DrainNone {
						want = classification{Kind: schedule.Blocked}
					} else if allow && group == "apiextensions.k8s.io" {
						want = classification{Kind: schedule.Terminal}
					}
					if got := w.classify(dep, level, w.Existence); got != want {
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
	parent := &manifest.ResourceSet{Name: "parent", Namespace: "ns"}
	s.AddObject(parent)
	s.AddObject(&manifest.Kustomization{Name: dep.Name, Namespace: dep.Namespace})
	s.UpdateStatus(parent.Named(), store.StatusPending, "", "waiting on infra")
	s.UpdateStatus(dep.NamedResource, store.StatusReady, "", "")
	dep.ReadyExpr = `dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`

	w := &Waiter{Store: s, Parent: parent.Named()}
	if got := w.classify(dep, manifest.DrainNone, w.Existence); got.Kind != schedule.Blocked {
		t.Errorf("replace ReadyExpr false@none: got %+v, want ClassBlocked", got)
	}
	if got := w.classify(dep, manifest.DrainCascade, w.Existence); got.Kind != schedule.DependencyFailed ||
		got.Message != "readyExpr timeout: context deadline exceeded" {
		t.Errorf("replace ReadyExpr false@cascade: got %+v, want ClassFailed 'readyExpr timeout: context deadline exceeded'", got)
	}
	pending, _ := s.GetStatus(parent.Named())
	dep.ReadyExpr = fmt.Sprintf(`dep.status.conditions.exists(c, c.type == "Ready" && c.status == "True") && self.status.conditions.exists(c, c.type == "Ready" && c.status == %q && c.reason == %q && c.message == %q)`, metav1.ConditionUnknown, pending.Reason, pending.Message)
	for _, tc := range []struct {
		name  string
		level manifest.DrainLevel
	}{
		{"ordinary", manifest.DrainNone},
		{"cascade", manifest.DrainCascade},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := w.classify(dep, tc.level, w.Existence); got.Kind != schedule.Terminal {
				t.Errorf("ResourceSet self condition: got %+v, want Terminal", got)
			}
		})
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
			s.UpdateStatus(tc.obj.Named(), store.StatusPending, "", "working")
			w := &Waiter{Store: s}
			got := w.classify(manifest.DependencyRef{NamedResource: tc.obj.Named(), ReadyExpr: tc.expr}, manifest.DrainNone, w.Existence)
			if got.Kind != tc.want {
				t.Fatalf("classification = %+v, want %v", got, tc.want)
			}
		})
	}
}

func TestClassify_OrdinaryPromotion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		materialize bool
		withdraw    bool
		status      store.Status
		want        classification
	}{
		{name: "absent", want: classification{Kind: schedule.DependencyFailed, Message: "dependency not found"}},
		{name: "materialized", materialize: true, want: classification{Kind: schedule.Terminal}},
		{name: "withdrawn", materialize: true, withdraw: true, want: classification{Kind: schedule.DependencyFailed, Message: "dependency not found"}},
		{name: "ready", materialize: true, status: store.StatusReady, want: classification{Kind: schedule.Terminal}},
		{name: "failed", materialize: true, status: store.StatusFailed, want: classification{Kind: schedule.DependencyFailed, Message: "boom"}},
		{name: "pending", materialize: true, status: store.StatusPending, want: classification{Kind: schedule.Blocked}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.New()
			var data manifest.BaseManifest = &manifest.ConfigMap{Name: "input", Namespace: "apps"}
			if tc.status != "" {
				data = &manifest.Kustomization{Name: "input", Namespace: "apps"}
				defer st.AddListener(store.EventObjectAdded, func(id manifest.NamedResource, _ any) {
					st.UpdateStatus(id, tc.status, "", "boom")
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
			got := w.classify(manifest.DependencyRef{NamedResource: data.Named()}, manifest.DrainCascade, w.Existence)
			if promotion.calls != 1 || got != tc.want {
				t.Fatalf("promotion calls=%d classification=%+v, want %+v", promotion.calls, got, tc.want)
			}
		})
	}
}
