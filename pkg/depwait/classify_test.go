package depwait

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

func classifyDep(t *testing.T, w *Waiter, ref manifest.DependencyRef, drain int) Classification {
	t.Helper()
	return w.Classify(ref, drain)
}

func ksDep(name string) manifest.DependencyRef {
	return manifest.DependencyRef{NamedResource: manifest.NamedResource{
		Kind: manifest.KindKustomization, Namespace: "ns", Name: name,
	}}
}

// TestClassify_CoreStates exercises the non-ReadyExpr branches that drive the
// dag engine's gate, asserting the canonical event-engine messages per drain
// level.
func TestClassify_CoreStates(t *testing.T) {
	s := store.New()
	ready := ksDep("ready")
	failed := ksDep("failed")
	pending := ksDep("pending")
	absent := ksDep("absent")
	s.UpdateStatus(ready.NamedResource, store.StatusReady, "")
	s.UpdateStatus(failed.NamedResource, store.StatusFailed, "boom")
	s.UpdateStatus(pending.NamedResource, store.StatusPending, "working")

	w := &Waiter{Store: s}

	if got := classifyDep(t, w, ready, drainNone); got.Kind != ClassReady {
		t.Errorf("ready: got %+v, want ClassReady", got)
	}
	if got := classifyDep(t, w, failed, drainNone); got.Kind != ClassFailed || got.Message != "boom" {
		t.Errorf("failed: got %+v, want ClassFailed 'boom'", got)
	}
	// Present-Pending: blocks at none/cascade, fails "not ready" only at force.
	if got := classifyDep(t, w, pending, drainNone); got.Kind != ClassBlocked {
		t.Errorf("pending@none: got %+v, want ClassBlocked", got)
	}
	if got := classifyDep(t, w, pending, drainCascade); got.Kind != ClassBlocked {
		t.Errorf("pending@cascade: got %+v, want ClassBlocked (cascade keeps Pending blocked)", got)
	}
	if got := classifyDep(t, w, pending, drainForce); got.Kind != ClassFailed || got.Message != "not ready" {
		t.Errorf("pending@force: got %+v, want ClassFailed 'not ready'", got)
	}
	// Absent: blocks at none, fails "dependency not found" once draining.
	if got := classifyDep(t, w, absent, drainNone); got.Kind != ClassBlocked {
		t.Errorf("absent@none: got %+v, want ClassBlocked", got)
	}
	if got := classifyDep(t, w, absent, drainCascade); got.Kind != ClassFailed || got.Message != "dependency not found" {
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

	w := &Waiter{Store: s} // AdditiveReadyExpr=false (default/reachable mode)
	if got := classifyDep(t, w, dep, drainNone); got.Kind != ClassBlocked {
		t.Errorf("replace ReadyExpr false@none: got %+v, want ClassBlocked", got)
	}
	if got := classifyDep(t, w, dep, drainCascade); got.Kind != ClassFailed ||
		got.Message != "readyExpr timeout: context deadline exceeded" {
		t.Errorf("replace ReadyExpr false@cascade: got %+v, want ClassFailed 'readyExpr timeout: context deadline exceeded'", got)
	}
}

// TestClassify_AdditiveReadyExpr is the defensive byte-equivalence guard: in
// additive mode (currently unreachable — no production Waiter sets it) Classify
// must match watchOne, failing a present-Ready dep immediately with
// "readyExpr returned false" on a clean-false CEL rather than blocking/timing
// out. Locks the fix so the divergence cannot return if the feature gate is
// ever wired.
func TestClassify_AdditiveReadyExpr(t *testing.T) {
	s := store.New()
	dep := ksDep("infra")
	s.UpdateStatus(dep.NamedResource, store.StatusReady, "") // built-in Ready holds
	dep.ReadyExpr = `dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`

	w := &Waiter{Store: s, AdditiveReadyExpr: true}
	// CEL is clean-false (no Healthy condition) → immediate fail, any drain level.
	for _, drain := range []int{drainNone, drainCascade, drainForce} {
		got := classifyDep(t, w, dep, drain)
		if got.Kind != ClassFailed || got.Message != "readyExpr returned false" {
			t.Errorf("additive clean-false@%d: got %+v, want ClassFailed 'readyExpr returned false'", drain, got)
		}
	}
	// Now satisfy the CEL: built-in Ready AND CEL true → ClassReady.
	s.SetCondition(dep.NamedResource, store.Condition{Type: "Healthy", Status: metav1.ConditionTrue})
	if got := classifyDep(t, w, dep, drainNone); got.Kind != ClassReady {
		t.Errorf("additive both-true: got %+v, want ClassReady", got)
	}
}
