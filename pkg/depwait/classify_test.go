package depwait

import (
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func classifyDep(t *testing.T, w *Waiter, ref manifest.DependencyRef, drain manifest.DrainLevel) Classification {
	t.Helper()
	return w.Classify(ref, drain)
}

func ksDep(name string) manifest.DependencyRef {
	return manifest.DependencyRef{
		Kind: manifest.KindKustomization, Namespace: "ns", Name: name}
}

// TestClassify_CoreStates exercises the non-ReadyExpr branches that drive the
// dag engine's gate, asserting the canonical event-engine messages per drain
// level.
func TestClassify_CoreStates(t *testing.T) {
	s := store.New()
	ready := ksDep("ready")
	failed := ksDep("failed")
	pending := ksDep("pending")
	statusless := manifest.DependencyRef{
		Kind: manifest.KindHelmChart, Namespace: "ns", Name: "statusless"}
	absent := ksDep("absent")
	s.UpdateStatus(ready.NamedResource, store.StatusReady, "")
	s.UpdateStatus(failed.NamedResource, store.StatusFailed, "boom")
	s.UpdateStatus(pending.NamedResource, store.StatusPending, "working")
	s.AddObject(&manifest.HelmChartSource{Name: "statusless", Namespace: "ns"})

	w := &Waiter{Store: s}

	if got := classifyDep(t, w, ready, manifest.DrainNone); got.Kind != ClassReady {
		t.Errorf("ready: got %+v, want ClassReady", got)
	}
	if got := classifyDep(t, w, failed, manifest.DrainNone); got.Kind != ClassFailed || got.Message != "boom" {
		t.Errorf("failed: got %+v, want ClassFailed 'boom'", got)
	}
	// Present-Pending: blocks at none/cascade, fails "not ready" only at force.
	if got := classifyDep(t, w, pending, manifest.DrainNone); got.Kind != ClassBlocked {
		t.Errorf("pending@none: got %+v, want ClassBlocked", got)
	}
	if got := classifyDep(t, w, pending, manifest.DrainCascade); got.Kind != ClassBlocked {
		t.Errorf("pending@cascade: got %+v, want ClassBlocked (cascade keeps Pending blocked)", got)
	}
	if got := classifyDep(t, w, pending, manifest.DrainForce); got.Kind != ClassFailed || got.Message != "not ready" {
		t.Errorf("pending@force: got %+v, want ClassFailed 'not ready'", got)
	}
	// Present with no Ready condition (a synthetic HelmChart before its source
	// worker's first write) classifies exactly like Pending.
	for level, want := range map[manifest.DrainLevel]Classification{
		manifest.DrainNone:    {Kind: ClassBlocked},
		manifest.DrainCascade: {Kind: ClassBlocked},
		manifest.DrainForce:   {Kind: ClassFailed, Message: "not ready"},
	} {
		if got := classifyDep(t, w, statusless, level); got != want {
			t.Errorf("statusless@%v: got %+v, want %+v", level, got, want)
		}
	}
	// Absent: blocks at none, fails "dependency not found" once draining.
	if got := classifyDep(t, w, absent, manifest.DrainNone); got.Kind != ClassBlocked {
		t.Errorf("absent@none: got %+v, want ClassBlocked", got)
	}
	if got := classifyDep(t, w, absent, manifest.DrainCascade); got.Kind != ClassFailed || got.Message != "dependency not found" {
		t.Errorf("absent@cascade: got %+v, want ClassFailed 'dependency not found'", got)
	}
}

// TestClassify_ReplaceReadyExpr: in the default (replace) mode the CEL is the
// readiness check; a never-true CEL blocks until the fixpoint, then fails with
// the event engine's "readyExpr timeout" string.
func TestClassify_ReplaceReadyExpr(t *testing.T) {
	s := store.New()
	dep := ksDep("infra")
	s.UpdateStatus(dep.NamedResource, store.StatusReady, "")
	dep.ReadyExpr = `dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")` // not satisfied

	w := &Waiter{Store: s}
	if got := classifyDep(t, w, dep, manifest.DrainNone); got.Kind != ClassBlocked {
		t.Errorf("replace ReadyExpr false@none: got %+v, want ClassBlocked", got)
	}
	if got := classifyDep(t, w, dep, manifest.DrainCascade); got.Kind != ClassFailed ||
		got.Message != "readyExpr timeout: context deadline exceeded" {
		t.Errorf("replace ReadyExpr false@cascade: got %+v, want ClassFailed 'readyExpr timeout: context deadline exceeded'", got)
	}
}
