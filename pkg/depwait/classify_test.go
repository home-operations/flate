package depwait

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func ksDep(name string) manifest.DependencyRef {
	return manifest.DependencyRef{Group: manifest.FluxKustomizeDomain, Kind: manifest.KindKustomization, Namespace: "ns", Name: name}
}

func TestClassify_CoreStates(t *testing.T) {
	s := store.New()
	ready := ksDep("ready")
	failed := ksDep("failed")
	pending := ksDep("pending")
	statusless := manifest.DependencyRef{Group: manifest.SourceDomain, Kind: manifest.KindHelmChart, Namespace: "ns", Name: "statusless"}
	absent := ksDep("absent")
	s.AddObject(&manifest.Kustomization{Name: ready.Name, Namespace: ready.Namespace})
	s.UpdateStatus(ready.NamedResource, store.StatusReady, "")
	s.UpdateStatus(failed.NamedResource, store.StatusFailed, "boom")
	s.UpdateStatus(pending.NamedResource, store.StatusPending, "working")
	s.AddObject(&manifest.HelmChartSource{Name: "statusless", Namespace: "ns"})

	w := &Waiter{Store: s}

	if got := w.Classify(ready, manifest.DrainNone); got.Kind != ClassReady {
		t.Errorf("ready: got %+v, want ClassReady", got)
	}
	if got := w.Classify(failed, manifest.DrainNone); got.Kind != ClassFailed || got.Message != "boom" {
		t.Errorf("failed: got %+v, want ClassFailed 'boom'", got)
	}
	// Present-Pending: blocks at none/cascade, fails "not ready" only at force.
	if got := w.Classify(pending, manifest.DrainNone); got.Kind != ClassBlocked {
		t.Errorf("pending@none: got %+v, want ClassBlocked", got)
	}
	if got := w.Classify(pending, manifest.DrainCascade); got.Kind != ClassBlocked {
		t.Errorf("pending@cascade: got %+v, want ClassBlocked (cascade keeps Pending blocked)", got)
	}
	if got := w.Classify(pending, manifest.DrainForce); got.Kind != ClassFailed || got.Message != "not ready" {
		t.Errorf("pending@force: got %+v, want ClassFailed 'not ready'", got)
	}
	// Present with no Ready condition (a synthetic HelmChart before its source
	// worker's first write) classifies exactly like Pending.
	for level, want := range map[manifest.DrainLevel]Classification{
		manifest.DrainNone:    {Kind: ClassBlocked},
		manifest.DrainCascade: {Kind: ClassBlocked},
		manifest.DrainForce:   {Kind: ClassFailed, Message: "not ready"},
	} {
		if got := w.Classify(statusless, level); got != want {
			t.Errorf("statusless@%v: got %+v, want %+v", level, got, want)
		}
	}
	// Absent: blocks at none, fails "dependency not found" once draining.
	if got := w.Classify(absent, manifest.DrainNone); got.Kind != ClassBlocked {
		t.Errorf("absent@none: got %+v, want ClassBlocked", got)
	}
	if got := w.Classify(absent, manifest.DrainCascade); got.Kind != ClassFailed || got.Message != "dependency not found" {
		t.Errorf("absent@cascade: got %+v, want ClassFailed 'dependency not found'", got)
	}
	for _, group := range []string{"", "example.test"} {
		t.Run("group="+group, func(t *testing.T) {
			foreign := ready
			foreign.Group = group
			if got := w.Classify(foreign, manifest.DrainNone); got.Kind != ClassBlocked {
				t.Fatalf("absent lookalike borrowed Ready status: %+v", got)
			}
			if got := w.Classify(foreign, manifest.DrainCascade); got.Kind != ClassFailed || got.Message != "dependency not found" {
				t.Fatalf("absent lookalike at drain: %+v", got)
			}
			s.UpdateStatus(foreign.NamedResource, store.StatusPending, "foreign status")
			if got := w.Classify(foreign, manifest.DrainNone); got.Kind != ClassReady {
				t.Fatalf("status-only lookalike acquired Flux readiness: %+v", got)
			}
		})
	}
	w.AllowMissingCRDs = true
	for _, group := range []string{"apiextensions.k8s.io", "example.test", ""} {
		t.Run("CRD/group="+group, func(t *testing.T) {
			dep := manifest.DependencyRef{Group: group, Kind: manifest.KindCustomResourceDefinition, Name: "widgets.example.test"}
			want := ClassFailed
			if group == "apiextensions.k8s.io" {
				want = ClassReady
			}
			if got := w.Classify(dep, manifest.DrainCascade); got.Kind != want {
				t.Fatalf("missing CRD: %+v, want %v", got, want)
			}
		})
	}
}

func TestClassify_ReplaceReadyExpr(t *testing.T) {
	s := store.New()
	dep := ksDep("infra")
	s.UpdateStatus(dep.NamedResource, store.StatusReady, "")
	dep.ReadyExpr = `dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")` // not satisfied

	w := &Waiter{Store: s}
	if got := w.Classify(dep, manifest.DrainNone); got.Kind != ClassBlocked {
		t.Errorf("replace ReadyExpr false@none: got %+v, want ClassBlocked", got)
	}
	if got := w.Classify(dep, manifest.DrainCascade); got.Kind != ClassFailed ||
		got.Message != "readyExpr timeout: context deadline exceeded" {
		t.Errorf("replace ReadyExpr false@cascade: got %+v, want ClassFailed 'readyExpr timeout: context deadline exceeded'", got)
	}
}

func TestClassify_EdgeReadiness(t *testing.T) {
	for _, tc := range []struct {
		name string
		obj  manifest.BaseManifest
		expr string
		want ClassKind
	}{
		{name: "ready waits for pending", obj: &manifest.Kustomization{Name: "dependency", Namespace: "apps"}, want: ClassBlocked},
		{name: "raw Flux group remains existence", obj: &manifest.RawObject{Kind: manifest.KindKustomization, APIVersion: manifest.FluxKustomizeDomain + "/v1", Name: "dependency", Namespace: "apps"}, want: ClassReady},
		{name: "foreign kind remains existence", obj: &manifest.RawObject{Kind: manifest.KindKustomization, APIVersion: "example.com/v1", Name: "dependency", Namespace: "apps"}, want: ClassReady},
		{name: "ConfigMap ignores pending and CEL", obj: &manifest.ConfigMap{Name: "dependency", Namespace: "apps"}, expr: "invalid [", want: ClassReady},
		{name: "Secret ignores pending and CEL", obj: &manifest.Secret{Name: "dependency", Namespace: "apps"}, expr: "invalid [", want: ClassReady},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := store.New()
			s.AddObject(tc.obj)
			s.UpdateStatus(tc.obj.Named(), store.StatusPending, "working")
			w := &Waiter{Store: s}
			got := w.Classify(manifest.DependencyRef{NamedResource: tc.obj.Named(), ReadyExpr: tc.expr}, manifest.DrainNone)
			if got.Kind != tc.want {
				t.Fatalf("classification = %+v, want %v", got, tc.want)
			}
		})
	}
}
