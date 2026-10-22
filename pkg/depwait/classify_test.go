package depwait

import (
	"fmt"
	"testing"

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
	s.UpdateStatus(ready.NamedResource, store.StatusReady, "", "")
	s.UpdateStatus(failed.NamedResource, store.StatusFailed, "", "boom")
	s.UpdateStatus(pending.NamedResource, store.StatusPending, "", "working")
	s.AddObject(&manifest.HelmChartSource{Name: "statusless", Namespace: "ns"})

	w := &Waiter{Store: s}

	if got := w.classify(ready, manifest.DrainNone, w.Existence); got.Kind != classReady {
		t.Errorf("ready: got %+v, want ClassReady", got)
	}
	for _, reason := range []store.StatusReason{store.ReasonSkipped, store.ReasonUnchanged, store.ReasonSuspended} {
		t.Run(string(reason), func(t *testing.T) {
			s.UpdateStatus(ready.NamedResource, store.StatusReady, reason, "")
			if got := w.classify(ready, manifest.DrainNone, w.Existence); got.Kind != classReady {
				t.Fatalf("typed Ready reason %s blocked dependency: %+v", reason, got)
			}
		})
	}
	if got := w.classify(failed, manifest.DrainNone, w.Existence); got.Kind != classFailed || got.Message != "boom" {
		t.Errorf("failed: got %+v, want ClassFailed 'boom'", got)
	}
	// Present-Pending: blocks at none/cascade, fails "not ready" only at force.
	if got := w.classify(pending, manifest.DrainNone, w.Existence); got.Kind != classBlocked {
		t.Errorf("pending@none: got %+v, want ClassBlocked", got)
	}
	if got := w.classify(pending, manifest.DrainCascade, w.Existence); got.Kind != classBlocked {
		t.Errorf("pending@cascade: got %+v, want ClassBlocked (cascade keeps Pending blocked)", got)
	}
	if got := w.classify(pending, manifest.DrainForce, w.Existence); got.Kind != classFailed || got.Message != "not ready" {
		t.Errorf("pending@force: got %+v, want ClassFailed 'not ready'", got)
	}
	// Present with no Ready condition (a synthetic HelmChart before its source
	// worker's first write) classifies exactly like Pending.
	for level, want := range map[manifest.DrainLevel]classification{
		manifest.DrainNone:    {Kind: classBlocked},
		manifest.DrainCascade: {Kind: classBlocked},
		manifest.DrainForce:   {Kind: classFailed, Message: "not ready"},
	} {
		if got := w.classify(statusless, level, w.Existence); got != want {
			t.Errorf("statusless@%v: got %+v, want %+v", level, got, want)
		}
	}
	// Absent: blocks at none, fails "dependency not found" once draining.
	if got := w.classify(absent, manifest.DrainNone, w.Existence); got.Kind != classBlocked {
		t.Errorf("absent@none: got %+v, want ClassBlocked", got)
	}
	if got := w.classify(absent, manifest.DrainCascade, w.Existence); got.Kind != classFailed || got.Message != "dependency not found" {
		t.Errorf("absent@cascade: got %+v, want ClassFailed 'dependency not found'", got)
	}
	for _, group := range []string{"", "example.test"} {
		t.Run("group="+group, func(t *testing.T) {
			foreign := ready
			foreign.Group = group
			if got := w.classify(foreign, manifest.DrainNone, w.Existence); got.Kind != classBlocked {
				t.Fatalf("absent lookalike borrowed Ready status: %+v", got)
			}
			if got := w.classify(foreign, manifest.DrainCascade, w.Existence); got.Kind != classFailed || got.Message != "dependency not found" {
				t.Fatalf("absent lookalike at drain: %+v", got)
			}
			s.UpdateStatus(foreign.NamedResource, store.StatusPending, "", "foreign status")
			if got := w.classify(foreign, manifest.DrainNone, w.Existence); got.Kind != classReady {
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
					want := classification{Kind: classFailed, Message: "dependency not found"}
					if level == manifest.DrainNone {
						want = classification{Kind: classBlocked}
					} else if allow && group == "apiextensions.k8s.io" {
						want = classification{Kind: classReady}
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
	s.UpdateStatus(dep.NamedResource, store.StatusReady, "", "")
	dep.ReadyExpr = `dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")` // not satisfied

	w := &Waiter{Store: s}
	if got := w.classify(dep, manifest.DrainNone, w.Existence); got.Kind != classBlocked {
		t.Errorf("replace ReadyExpr false@none: got %+v, want ClassBlocked", got)
	}
	if got := w.classify(dep, manifest.DrainCascade, w.Existence); got.Kind != classFailed ||
		got.Message != "readyExpr timeout: context deadline exceeded" {
		t.Errorf("replace ReadyExpr false@cascade: got %+v, want ClassFailed 'readyExpr timeout: context deadline exceeded'", got)
	}
}

func TestClassify_EdgeReadiness(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		obj  manifest.BaseManifest
		expr string
		want classKind
	}{
		{name: "ready waits for pending", obj: &manifest.Kustomization{Name: "dependency", Namespace: "apps"}, want: classBlocked},
		{name: "raw Flux group remains existence", obj: &manifest.RawObject{Kind: manifest.KindKustomization, APIVersion: manifest.FluxKustomizeDomain + "/v1", Name: "dependency", Namespace: "apps"}, want: classReady},
		{name: "foreign kind remains existence", obj: &manifest.RawObject{Kind: manifest.KindKustomization, APIVersion: "example.com/v1", Name: "dependency", Namespace: "apps"}, want: classReady},
		{name: "ConfigMap ignores pending and CEL", obj: &manifest.ConfigMap{Name: "dependency", Namespace: "apps"}, expr: "invalid [", want: classReady},
		{name: "Secret ignores pending and CEL", obj: &manifest.Secret{Name: "dependency", Namespace: "apps"}, expr: "invalid [", want: classReady},
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
		{name: "absent", want: classification{Kind: classFailed, Message: "dependency not found"}},
		{name: "materialized", materialize: true, want: classification{Kind: classReady}},
		{name: "withdrawn", materialize: true, withdraw: true, want: classification{Kind: classFailed, Message: "dependency not found"}},
		{name: "ready", materialize: true, status: store.StatusReady, want: classification{Kind: classReady}},
		{name: "failed", materialize: true, status: store.StatusFailed, want: classification{Kind: classFailed, Message: "boom"}},
		{name: "pending", materialize: true, status: store.StatusPending, want: classification{Kind: classBlocked}},
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
