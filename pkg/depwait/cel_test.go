package depwait

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// These tests pin the CEL ReadyExpr machinery (evaluateReadyExpr + projectObject)
// that the dag engine's Classify reuses. They drive the kept non-blocking surface
// — evaluateReadyExpr directly for projection/binding correctness, and Classify
// for the readiness-gate semantics — rather than the deleted blocking Watch loop.

func celDep(name string) manifest.NamedResource {
	return manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: name}
}

// TestReadyExpr_ProjectsObservedGeneration: the common Flux readiness idiom
// `status.observedGeneration == metadata.generation` projects and compares.
func TestReadyExpr_ProjectsObservedGeneration(t *testing.T) {
	s := store.New()
	dep := celDep("infra")
	s.UpdateStatus(dep, store.StatusReady, "")

	ok, err := evaluateReadyExpr(`dep.status.observedGeneration == dep.metadata.generation`, s, manifest.NamedResource{}, dep)
	if err != nil || !ok {
		t.Errorf("observedGeneration projection: ok=%v err=%v, want true/nil", ok, err)
	}
}

// TestReadyExpr_ReplaceOverridesBuiltin: in the default (replace) mode a
// satisfied ReadyExpr makes the dep Ready even when the built-in status is
// Failed — Classify must return ClassReady (readyNow short-circuits on the CEL).
func TestReadyExpr_ReplaceOverridesBuiltin(t *testing.T) {
	s := store.New()
	dep := celDep("infra")
	s.UpdateStatus(dep, store.StatusFailed, "still rolling out") // built-in says Failed
	s.SetCondition(dep, store.Condition{Type: "Healthy", Status: metav1.ConditionTrue, Reason: "MyCustomCheck"})

	w := &Waiter{Store: s} // AdditiveReadyExpr=false (default replace mode)
	ref := manifest.DependencyRef{
		NamedResource: dep,
		ReadyExpr:     `dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`,
	}
	if got := w.Classify(ref, drainNone); got.Kind != ClassReady {
		t.Errorf("replace ReadyExpr true over Failed built-in: got %+v, want ClassReady", got)
	}
}

// TestReadyExpr_CompileError: malformed CEL is a terminal compile error —
// Classify fails it immediately (any drain level) with a "readyExpr:"-prefixed
// message, and evaluateReadyExpr wraps it as *celCompileErr.
func TestReadyExpr_CompileError(t *testing.T) {
	s := store.New()
	dep := celDep("x")
	s.UpdateStatus(dep, store.StatusReady, "ok")

	if _, err := evaluateReadyExpr(`this isn't valid CEL`, s, manifest.NamedResource{}, dep); err == nil {
		t.Fatal("expected compile error")
	}
	w := &Waiter{Store: s}
	ref := manifest.DependencyRef{NamedResource: dep, ReadyExpr: `this isn't valid CEL`}
	got := w.Classify(ref, drainNone)
	if got.Kind != ClassFailed || !strings.HasPrefix(got.Message, "readyExpr:") {
		t.Errorf("compile error: got %+v, want ClassFailed with 'readyExpr:' prefix", got)
	}
}

// TestReadyExpr_NonBoolResult: an expr that returns a non-bool (a string) is a
// terminal type-shape problem, surfaced as a compile-class error.
func TestReadyExpr_NonBoolResult(t *testing.T) {
	s := store.New()
	dep := celDep("x")
	s.UpdateStatus(dep, store.StatusReady, "ok")

	_, err := evaluateReadyExpr(`dep.metadata.name`, s, manifest.NamedResource{}, dep) // string, not bool
	if err == nil || !strings.Contains(err.Error(), "must return bool") {
		t.Errorf("non-bool result: err=%v, want a 'must return bool' error", err)
	}
}

// TestReadyExpr_TransientEvalErrorNotTerminal: a runtime eval error (indexing
// past the conditions slice) is transient, NOT terminal. tryReadyExpr reports
// it as not-ready with no failMsg (distinct from a compile error), and Classify
// blocks the dep — it may still become ready — rather than failing it.
func TestReadyExpr_TransientEvalErrorNotTerminal(t *testing.T) {
	s := store.New()
	dep := celDep("infra")
	s.UpdateStatus(dep, store.StatusReady, "")

	w := &Waiter{Store: s}
	const expr = `dep.status.conditions[100].type == "Ready"` // index out of range → runtime eval error
	if ready, failMsg := w.tryReadyExpr(expr, dep); ready || failMsg != "" {
		t.Fatalf("transient eval error: ready=%v failMsg=%q, want false/empty (not terminal)", ready, failMsg)
	}
	ref := manifest.DependencyRef{NamedResource: dep, ReadyExpr: expr}
	if got := w.Classify(ref, drainNone); got.Kind != ClassBlocked {
		t.Errorf("transient eval error@none: got %+v, want ClassBlocked (re-runnable, not failed)", got)
	}
}

// TestReadyExpr_ReadsLabelsAndAnnotations locks the upstream Flux contract that
// readyExpr can read dep.metadata.{labels,annotations} via the CEL projection.
func TestReadyExpr_ReadsLabelsAndAnnotations(t *testing.T) {
	s := store.New()
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "child"}
	s.AddObject(&manifest.Kustomization{
		Name: "child", Namespace: "ns",
		Labels:      map[string]string{"tier": "data"},
		Annotations: map[string]string{"app.kubernetes.io/version": "1.2.3"},
	})
	s.UpdateStatus(dep, store.StatusReady, "")

	for _, expr := range []string{
		`dep.metadata.labels["tier"] == "data"`,
		`dep.metadata.annotations["app.kubernetes.io/version"] == "1.2.3"`,
	} {
		ok, err := evaluateReadyExpr(expr, s, manifest.NamedResource{}, dep)
		if err != nil || !ok {
			t.Errorf("expr %q: ok=%v err=%v, want true/nil", expr, ok, err)
		}
	}
}

// TestReadyExpr_BindsSelfAndDep locks the upstream Flux contract: readyExpr sees
// both `self` (the consumer / Waiter.Parent) and `dep` (the dependency).
func TestReadyExpr_BindsSelfAndDep(t *testing.T) {
	s := store.New()
	self := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "parent"}
	dep := manifest.NamedResource{Kind: manifest.KindKustomization, Namespace: "ns", Name: "child"}
	s.UpdateStatus(self, store.StatusReady, "")
	s.UpdateStatus(dep, store.StatusReady, "")

	ok, err := evaluateReadyExpr(`self.metadata.namespace == dep.metadata.namespace`, s, self, dep)
	if err != nil || !ok {
		t.Errorf("self/dep binding: ok=%v err=%v, want true/nil", ok, err)
	}
}

// TestCompileReadyExpr_MemoizesErrors pins the compile-error cache: a known-bad
// expression should not recompile (and re-error) on every fire. Two compiles
// return the same error instance — sync.Map.LoadOrStore guarantees pointer
// identity for the cached entry.
func TestCompileReadyExpr_MemoizesErrors(t *testing.T) {
	const bad = `this is not valid CEL ((` // unclosed paren = compile error
	_, err1 := compileReadyExpr(bad)
	_, err2 := compileReadyExpr(bad)
	if err1 == nil || err2 == nil {
		t.Fatal("expected compile error on both calls")
	}
	if err1 != err2 {
		t.Errorf("compile error not memoized: %p vs %p", err1, err2)
	}
}

// TestTimeoutFromSpec mirrors Flux KS/HR's `*metav1.Duration` shape: nil and
// zero fall back to DefaultTimeout; user-supplied values win.
func TestTimeoutFromSpec(t *testing.T) {
	if got := TimeoutFromSpec(nil); got != DefaultTimeout {
		t.Errorf("nil → %v, want DefaultTimeout (%v)", got, DefaultTimeout)
	}
	if got := TimeoutFromSpec(&metav1.Duration{Duration: 0}); got != DefaultTimeout {
		t.Errorf("zero → %v, want DefaultTimeout", got)
	}
	if got := TimeoutFromSpec(&metav1.Duration{Duration: 5 * time.Minute}); got != 5*time.Minute {
		t.Errorf("5m → %v", got)
	}
}
