package depwait

import (
	"context"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// TestReadyExpr_NonAdditive_PassesOnTrue: in the default (Flux)
// mode, ReadyExpr replaces the built-in check. When the CEL is true
// against current state, the dep is Ready regardless of the built-in
// Ready condition. The store carries Ready=False here to prove the
// replacement.
func TestReadyExpr_NonAdditive_PassesOnTrue(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "infra"}
	s.UpdateStatus(dep, store.StatusFailed, "still rolling out") // built-in says Failed
	s.SetCondition(dep, store.Condition{
		Type:   "Healthy",
		Status: metav1.ConditionTrue,
		Reason: "MyCustomCheck",
	})

	w := &Waiter{Store: s, Timeout: time.Second}
	sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{
		NamedResource: dep,
		ReadyExpr:     `object.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`,
	}}))
	if !sum.AllReady() {
		t.Errorf("non-additive ReadyExpr should override Failed Ready: %+v", sum)
	}
}

// TestReadyExpr_NonAdditive_FlipsOnStatusUpdate: when initial state
// doesn't satisfy the CEL, the Waiter subscribes to status updates
// and re-evaluates. A later SetCondition that makes the CEL true
// closes the wait.
func TestReadyExpr_NonAdditive_FlipsOnStatusUpdate(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "infra"}
	// Initial state: no Healthy condition.
	s.UpdateStatus(dep, store.StatusPending, "reconciling")

	go func() {
		time.Sleep(50 * time.Millisecond)
		s.SetCondition(dep, store.Condition{
			Type: "Healthy", Status: metav1.ConditionTrue, Reason: "AllGood",
		})
	}()

	w := &Waiter{Store: s, Timeout: 2 * time.Second}
	sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{
		NamedResource: dep,
		ReadyExpr:     `object.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`,
	}}))
	if !sum.AllReady() {
		t.Errorf("expected to satisfy after status update: %+v", sum)
	}
}

// TestReadyExpr_NonAdditive_TimesOutOnFalse: when the CEL never
// returns true and no status update flips it, the wait times out
// with a DepTimeout event (not DepFailed).
func TestReadyExpr_NonAdditive_TimesOutOnFalse(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "infra"}
	s.UpdateStatus(dep, store.StatusReady, "ok") // built-in says Ready, but CEL needs Healthy

	w := &Waiter{Store: s, Timeout: 100 * time.Millisecond}
	sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{
		NamedResource: dep,
		ReadyExpr:     `object.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`,
	}}))
	if sum.AllReady() {
		t.Fatalf("expected timeout: %+v", sum)
	}
	if !strings.Contains(sum.Messages[dep], "readyExpr timeout") {
		t.Errorf("expected readyExpr-prefixed timeout; got %q", sum.Messages[dep])
	}
}

// TestReadyExpr_Additive_BothMustPass: with AdditiveReadyExpr=true,
// Ready=True AND CEL=true → DepReady.
func TestReadyExpr_Additive_BothMustPass(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "infra"}
	s.UpdateStatus(dep, store.StatusReady, "ok")
	s.SetCondition(dep, store.Condition{
		Type: "Healthy", Status: metav1.ConditionTrue, Reason: "AllGood",
	})

	w := &Waiter{Store: s, Timeout: time.Second, AdditiveReadyExpr: true}
	sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{
		NamedResource: dep,
		ReadyExpr:     `object.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`,
	}}))
	if !sum.AllReady() {
		t.Errorf("expected AllReady when both checks pass: %+v", sum)
	}
}

// TestReadyExpr_Additive_FalseFails: with AdditiveReadyExpr=true,
// Ready=True but CEL=false → DepFailed immediately (no waiting on
// status updates — both must agree on the current state).
func TestReadyExpr_Additive_FalseFails(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "infra"}
	s.UpdateStatus(dep, store.StatusReady, "ok")
	// No Healthy condition.

	w := &Waiter{Store: s, Timeout: time.Second, AdditiveReadyExpr: true}
	sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{
		NamedResource: dep,
		ReadyExpr:     `object.status.conditions.exists(c, c.type == "Healthy")`,
	}}))
	if !sum.AnyFailed() {
		t.Fatalf("expected failure: %+v", sum)
	}
	if !strings.Contains(sum.Messages[dep], "readyExpr returned false") {
		t.Errorf("reason: %q", sum.Messages[dep])
	}
}

// TestReadyExpr_CompileError: malformed CEL produces a clear
// readyExpr-prefixed error in both modes.
func TestReadyExpr_CompileError(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "x"}
	s.UpdateStatus(dep, store.StatusReady, "ok")

	w := &Waiter{Store: s, Timeout: 100 * time.Millisecond}
	sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{
		NamedResource: dep,
		ReadyExpr:     `this isn't valid CEL`,
	}}))
	if !sum.AnyFailed() {
		t.Fatalf("expected failure for invalid CEL: %+v", sum)
	}
	if !strings.HasPrefix(sum.Messages[dep], "readyExpr:") {
		t.Errorf("message should be prefixed 'readyExpr:', got %q", sum.Messages[dep])
	}
}

// TestReadyExpr_NonBoolResult: a non-bool result surfaces a clear
// type error rather than treating it as truthy.
func TestReadyExpr_NonBoolResult(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "x"}
	s.UpdateStatus(dep, store.StatusReady, "ok")

	w := &Waiter{Store: s, Timeout: 100 * time.Millisecond}
	sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{
		NamedResource: dep,
		ReadyExpr:     `object.metadata.name`, // string, not bool
	}}))
	if !sum.AnyFailed() {
		t.Fatalf("expected failure for non-bool result: %+v", sum)
	}
	if !strings.Contains(sum.Messages[dep], "must return bool") {
		t.Errorf("expected non-bool error; got %q", sum.Messages[dep])
	}
}

// TestReadyExpr_Empty_UsesBuiltin: when ReadyExpr is empty the
// built-in Ready check is used unchanged (no CEL involvement).
func TestReadyExpr_Empty_UsesBuiltin(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "x"}
	s.UpdateStatus(dep, store.StatusReady, "ok")

	w := &Waiter{Store: s, Timeout: time.Second}
	sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{NamedResource: dep}}))
	if !sum.AllReady() {
		t.Errorf("plain dep with Ready status should pass: %+v", sum)
	}
}
