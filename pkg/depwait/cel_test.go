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
func TestReadyExpr_ProjectsObservedGeneration(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "infra"}
	s.UpdateStatus(dep, store.StatusReady, "")

	w := &Waiter{Store: s, Timeout: time.Second}
	sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{
		NamedResource: dep,
		// Common Flux readiness idiom.
		ReadyExpr: `dep.status.observedGeneration == dep.metadata.generation`,
	}}))
	if !sum.AllReady() {
		t.Errorf("observedGeneration projection should match metadata.generation: %+v", sum)
	}
}

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
		ReadyExpr:     `dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`,
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
		ReadyExpr:     `dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`,
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
		ReadyExpr:     `dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`,
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
		ReadyExpr:     `dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`,
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
		ReadyExpr:     `dep.status.conditions.exists(c, c.type == "Healthy")`,
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
		ReadyExpr:     `dep.metadata.name`, // string, not bool
	}}))
	if !sum.AnyFailed() {
		t.Fatalf("expected failure for non-bool result: %+v", sum)
	}
	if !strings.Contains(sum.Messages[dep], "must return bool") {
		t.Errorf("expected non-bool error; got %q", sum.Messages[dep])
	}
}

// TestReadyExpr_TransientEvalErrorPolls covers the "eval error against
// the projection is transient" contract: cel-go's runtime can fail
// when the expression touches a field that isn't yet present
// (e.g. an empty conditions slice). The waiter must re-poll on the
// next store event instead of failing terminally, so a dep that
// becomes ready a moment later still goes Ready.
func TestReadyExpr_TransientEvalErrorPolls(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "infra"}
	// Status is set but conditions is empty — `conditions[0]` triggers
	// a runtime eval error inside cel-go.
	s.UpdateStatus(dep, store.StatusReady, "")

	w := &Waiter{Store: s, Timeout: time.Second}
	ch := w.Watch(context.Background(), []manifest.DependencyRef{{
		NamedResource: dep,
		ReadyExpr:     `dep.status.conditions[0].type == "Ready"`,
	}})
	// Now land the condition the expr is looking for — this should
	// wake the polling loop.
	go func() {
		time.Sleep(50 * time.Millisecond)
		s.SetCondition(dep, store.Condition{Type: "Ready", Status: metav1.ConditionTrue})
	}()
	if sum := WaitAll(ch); !sum.AllReady() {
		t.Errorf("dep should go Ready once conditions[0] populates: %+v", sum)
	}
}

// TestReadyExpr_ReadsLabelsAndAnnotations locks the upstream Flux
// contract that readyExpr can read dep.metadata.{labels,annotations}.
// The HR/KS manifest types now carry these maps and the CEL projection
// surfaces them under metadata.labels / metadata.annotations.
func TestReadyExpr_ReadsLabelsAndAnnotations(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "child"}
	// AddObject the typed manifest so projectObject can read labels.
	s.AddObject(&manifest.Kustomization{
		Name: "child", Namespace: "ns",
		Labels:      map[string]string{"tier": "data"},
		Annotations: map[string]string{"app.kubernetes.io/version": "1.2.3"},
	})
	s.UpdateStatus(dep, store.StatusReady, "")

	w := &Waiter{Store: s, Timeout: time.Second}
	cases := []string{
		`dep.metadata.labels["tier"] == "data"`,
		`dep.metadata.annotations["app.kubernetes.io/version"] == "1.2.3"`,
	}
	for _, expr := range cases {
		sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{
			NamedResource: dep,
			ReadyExpr:     expr,
		}}))
		if !sum.AllReady() {
			t.Errorf("expr %q: not Ready: %+v", expr, sum)
		}
	}
}

// TestReadyExpr_BindsSelfAndDep locks the upstream Flux contract:
// readyExpr sees both `self` (the consumer / Waiter.Parent) and `dep`
// (the current dependency). The kustomize/helm controller idiom
// `dep.status.lastAppliedRevision == self.spec.sourceRef.revision`
// must compile and have access to both names.
func TestReadyExpr_BindsSelfAndDep(t *testing.T) {
	s := store.New()
	self := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "parent"}
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "child"}
	s.UpdateStatus(self, store.StatusReady, "")
	s.UpdateStatus(dep, store.StatusReady, "")

	w := &Waiter{Store: s, Parent: self, Timeout: time.Second}
	sum := WaitAll(w.Watch(context.Background(), []manifest.DependencyRef{{
		NamedResource: dep,
		// Reads both self and dep — would fail to compile under the old
		// `object`-only binding.
		ReadyExpr: `self.metadata.namespace == dep.metadata.namespace`,
	}}))
	if !sum.AllReady() {
		t.Errorf("expected self/dep CEL to evaluate true: %+v", sum)
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
