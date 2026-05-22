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

// TestWaiter_ReadyExprSatisfied: the dep is Ready by the built-in
// check AND its CEL expression returns true → DepReady.
func TestWaiter_ReadyExprSatisfied(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "infra"}
	s.UpdateStatus(dep, store.StatusReady, "ok")
	s.SetCondition(dep, store.Condition{
		Type:   "InfraInitialized",
		Status: metav1.ConditionTrue,
		Reason: "Done",
	})

	w := &Waiter{Store: s, Timeout: time.Second}
	sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{
		NamedResource: dep,
		ReadyExpr:     `object.status.conditions.exists(c, c.type == "InfraInitialized" && c.status == "True")`,
	}}))
	if !sum.AllReady() {
		t.Errorf("expected AllReady: %+v", sum)
	}
}

// TestWaiter_ReadyExprFailsCheck: the dep is Ready by the built-in
// check but its CEL expression returns false → DepFailed.
func TestWaiter_ReadyExprFailsCheck(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "infra"}
	s.UpdateStatus(dep, store.StatusReady, "ok")
	// No "InfraInitialized" condition.

	w := &Waiter{Store: s, Timeout: time.Second}
	sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{
		NamedResource: dep,
		ReadyExpr:     `object.status.conditions.exists(c, c.type == "InfraInitialized")`,
	}}))
	if !sum.AnyFailed() {
		t.Fatalf("expected failure: %+v", sum)
	}
	if !strings.Contains(sum.Messages[dep], "readyExpr returned false") {
		t.Errorf("reason: %q", sum.Messages[dep])
	}
}

// TestWaiter_ReadyExprCompileError: malformed CEL produces a clear
// readyExpr-prefixed error rather than silently passing.
func TestWaiter_ReadyExprCompileError(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "x"}
	s.UpdateStatus(dep, store.StatusReady, "ok")

	w := &Waiter{Store: s, Timeout: time.Second}
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

// TestWaiter_ReadyExprNonBoolResult: an expression that returns a
// non-bool value surfaces a clear type error rather than treating it
// as truthy.
func TestWaiter_ReadyExprNonBoolResult(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "x"}
	s.UpdateStatus(dep, store.StatusReady, "ok")

	w := &Waiter{Store: s, Timeout: time.Second}
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

// TestWaiter_NoReadyExprUsesBuiltin: when ReadyExpr is empty the
// built-in Ready check is used unchanged (no CEL involvement).
func TestWaiter_NoReadyExprUsesBuiltin(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "x"}
	s.UpdateStatus(dep, store.StatusReady, "ok")

	w := &Waiter{Store: s, Timeout: time.Second}
	sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{NamedResource: dep}}))
	if !sum.AllReady() {
		t.Errorf("plain dep with Ready status should pass: %+v", sum)
	}
}
