package depwait

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/schedule"
	"github.com/home-operations/flate/pkg/store"
)

// These tests pin the CEL ReadyExpr machinery (evaluateReadyExpr + projectObject)
// that the dag engine's Classify reuses. They drive the kept non-blocking surface
// — evaluateReadyExpr directly for projection/binding correctness, and Classify
// for the readiness-gate semantics — rather than the deleted blocking Watch loop.

// TestReadyExpr_ProjectsObservedGeneration: the common Flux readiness idiom
// `status.observedGeneration == metadata.generation` projects and compares.
func TestReadyExpr_ProjectsObservedGeneration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ group, kind, version string }{
		{manifest.FluxKustomizeDomain, manifest.KindKustomization, "kustomize.toolkit.fluxcd.io/v1"},
		{manifest.HelmReleaseDomain, manifest.KindHelmRelease, "helm.toolkit.fluxcd.io/v2"},
		{manifest.SourceDomain, manifest.KindGitRepository, "source.toolkit.fluxcd.io/v1"},
		{manifest.SourceDomain, manifest.KindOCIRepository, "source.toolkit.fluxcd.io/v1"},
		{manifest.SourceDomain, manifest.KindHelmRepository, "source.toolkit.fluxcd.io/v1"},
		{manifest.SourceDomain, manifest.KindHelmChart, "source.toolkit.fluxcd.io/v1"},
		{manifest.SourceDomain, manifest.KindBucket, "source.toolkit.fluxcd.io/v1"},
		{manifest.SourceDomain, manifest.KindExternalArtifact, "source.toolkit.fluxcd.io/v1"},
		{manifest.FluxOperatorDomain, manifest.KindResourceSet, ""},
		{"", manifest.KindConfigMap, ""},
		{"", "Unknown", ""},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			dep := manifest.NamedResource{Group: tc.group, Kind: tc.kind, Namespace: "ns", Name: "infra"}
			s.UpdateStatus(dep, store.StatusReady, "", "")
			expr := fmt.Sprintf(`dep.status.observedGeneration == dep.metadata.generation && dep.apiVersion == %q`, tc.version)
			ok, err := evaluateReadyExpr(expr, s, manifest.NamedResource{}, dep)
			if err != nil || !ok {
				t.Errorf("readiness projection: ok=%v err=%v, want true/nil", ok, err)
			}
		})
	}
}

// TestReadyExpr_ReplaceOverridesBuiltin: in the default (replace) mode a
// satisfied ReadyExpr makes the dep Ready even when the built-in status is
// Failed — Classify must return ClassReady (readyNow short-circuits on the CEL).
func TestReadyExpr_ReplaceOverridesBuiltin(t *testing.T) {
	t.Parallel()
	s := store.New()
	dep := testutil.KustomizationID("ns", "infra")
	s.UpdateStatus(dep, store.StatusFailed, "", "still rolling out") // built-in says Failed
	s.SetCondition(dep, store.Condition{Type: "Healthy", Status: metav1.ConditionTrue, Reason: "MyCustomCheck"})

	w := &Waiter{Store: s}
	ref := manifest.DependencyRef{NamedResource: dep,
		ReadyExpr: `dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`,
	}
	if got := w.classify(ref, manifest.DrainNone, w.Existence); got.Kind != schedule.Terminal {
		t.Errorf("replace ReadyExpr true over Failed built-in: got %+v, want ClassReady", got)
	}
}

func TestReadyExpr_CompileError(t *testing.T) {
	t.Parallel()
	for _, present := range []bool{false, true} {
		t.Run(fmt.Sprint(present), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			dep := testutil.KustomizationID("ns", "x")
			if present {
				s.UpdateStatus(dep, store.StatusReady, "", "ok")
			}

			if _, err := evaluateReadyExpr(`this isn't valid CEL`, s, manifest.NamedResource{}, dep); err == nil {
				t.Fatal("expected compile error")
			}
			w := &Waiter{Store: s}
			ref := manifest.DependencyRef{NamedResource: dep, ReadyExpr: `this isn't valid CEL`}
			got := w.classify(ref, manifest.DrainNone, w.Existence)
			if got.Kind != schedule.DependencyFailed || !strings.HasPrefix(got.Message, "readyExpr:") {
				t.Errorf("compile error: got %+v, want ClassFailed with 'readyExpr:' prefix", got)
			}
		})
	}
}

// TestReadyExpr_NonBoolResult: an expr that returns a non-bool (a string) is a
// terminal type-shape problem, surfaced as a compile-class error.
func TestReadyExpr_NonBoolResult(t *testing.T) {
	t.Parallel()
	s := store.New()
	dep := testutil.KustomizationID("ns", "x")
	s.UpdateStatus(dep, store.StatusReady, "", "ok")

	_, err := evaluateReadyExpr(`dep.metadata.name`, s, manifest.NamedResource{}, dep) // string, not bool
	if err == nil || !strings.Contains(err.Error(), "must return bool") {
		t.Errorf("non-bool result: err=%v, want a 'must return bool' error", err)
	}
	if _, terminal := errors.AsType[*celCompileErr](err); !terminal {
		t.Errorf("non-bool result: err=%T, want terminal *celCompileErr", err)
	}
}

// TestReadyExpr_TransientEvalErrorNotTerminal: a runtime eval error (indexing
// past the conditions slice) is transient, NOT terminal. tryReadyExpr reports
// it as not-ready with no failMsg (distinct from a compile error), and Classify
// blocks the dep — it may still become ready — rather than failing it.
func TestReadyExpr_TransientEvalErrorNotTerminal(t *testing.T) {
	t.Parallel()
	s := store.New()
	dep := testutil.KustomizationID("ns", "infra")
	s.UpdateStatus(dep, store.StatusReady, "", "")

	w := &Waiter{Store: s}
	const expr = `dep.status.conditions[100].type == "Ready"` // index out of range → runtime eval error
	var observation store.Observation
	obj := s.Snapshot(&dep, true, &observation)
	conds := observation.Conditions
	if ready, failMsg := w.tryReadyExpr(manifest.DependencyRef{NamedResource: dep, ReadyExpr: expr}, obj, conds); ready || failMsg != "" {
		t.Fatalf("transient eval error: ready=%v failMsg=%q, want false/empty (not terminal)", ready, failMsg)
	}
	ref := manifest.DependencyRef{NamedResource: dep, ReadyExpr: expr}
	if got := w.classify(ref, manifest.DrainNone, w.Existence); got.Kind != schedule.Blocked {
		t.Errorf("transient eval error@none: got %+v, want ClassBlocked (re-runnable, not failed)", got)
	}
}

// TestReadyExpr_ReadsLabelsAndAnnotations locks the upstream Flux contract that
// readyExpr can read dep.metadata.{labels,annotations} via the CEL projection.
func TestReadyExpr_ReadsLabelsAndAnnotations(t *testing.T) {
	t.Parallel()
	s := store.New()
	dep := testutil.KustomizationID("ns", "child")
	s.AddObject(&manifest.Kustomization{
		Name: "child", Namespace: "ns",
		Labels:      map[string]string{"tier": "data"},
		Annotations: map[string]string{"app.kubernetes.io/version": "1.2.3"},
	})
	s.UpdateStatus(dep, store.StatusReady, "", "")
	var observation store.Observation
	obj := s.Snapshot(&dep, true, &observation)
	conds := observation.Conditions
	fresh := obj.(*manifest.Kustomization).Clone()
	fresh.Labels = map[string]string{"tier": "changed"}
	fresh.Annotations = map[string]string{"app.kubernetes.io/version": "2.0.0"}
	s.AddObject(fresh)
	s.UpdateStatus(dep, store.StatusFailed, "", "replacement")
	w := Waiter{Store: s}

	for _, expr := range []string{
		`dep.metadata.labels["tier"] == "data"`,
		`dep.metadata.annotations["app.kubernetes.io/version"] == "1.2.3"`,
	} {
		expr += ` && dep.status.conditions.exists(c, c.type == "Ready" && c.status == "True")`
		ok, err := w.evaluateReadyExpr(expr, dep, obj, conds)
		if err != nil || !ok {
			t.Errorf("expr %q: ok=%v err=%v, want true/nil", expr, ok, err)
		}
	}
}

// TestReadyExpr_BindsSelfAndDep locks the upstream Flux contract: readyExpr sees
// both `self` (the consumer / Waiter.Parent) and `dep` (the dependency).
func TestReadyExpr_BindsSelfAndDep(t *testing.T) {
	t.Parallel()
	s := store.New()
	self := testutil.KustomizationID("ns", "parent")
	dep := testutil.KustomizationID("ns", "child")
	s.UpdateStatus(self, store.StatusReady, "", "")
	s.UpdateStatus(dep, store.StatusReady, store.ReasonSkipped, "changed diagnostic")

	ok, err := evaluateReadyExpr(`self.metadata.namespace == dep.metadata.namespace && self.status.conditions.exists(c, c.type == "Ready" && c.reason == "Succeeded") && dep.status.conditions.exists(c, c.type == "Ready" && c.reason == "Skipped" && c.status == "True")`, s, self, dep)
	if err != nil || !ok {
		t.Errorf("self/dep binding: ok=%v err=%v, want true/nil", ok, err)
	}
	var observation store.Observation
	obj := s.Snapshot(&dep, true, &observation)
	conds := observation.Conditions
	s.UpdateStatus(dep, store.StatusFailed, "", "replacement")
	for _, tc := range []struct {
		name   string
		self   manifest.NamedResource
		reason string
	}{
		{"same identity", dep, "Skipped"},
		{"distinct identity", self, "Succeeded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			w := Waiter{Store: s, Parent: tc.self}
			expr := `self.status.conditions.exists(c, c.type == "Ready" && c.reason == "` + tc.reason + `") && dep.status.conditions.exists(c, c.type == "Ready" && c.reason == "Skipped" && c.status == "True")`
			if ok, err := w.evaluateReadyExpr(expr, dep, obj, conds); err != nil || !ok {
				t.Fatalf("retained observation: ok=%v err=%v", ok, err)
			}
		})
	}
	for _, group := range []string{"", "example.test"} {
		t.Run("group="+group, func(t *testing.T) {
			t.Parallel()
			foreignSelf, foreignDep := self, dep
			foreignSelf.Group, foreignDep.Group = group, group
			expr := `self.apiVersion == '' && dep.apiVersion == '' && !dep.status.conditions.exists(c, c.type == 'Ready' && c.status == 'True')`
			if ok, err := evaluateReadyExpr(expr, s, foreignSelf, foreignDep); err != nil || !ok {
				t.Fatalf("foreign projection borrowed Flux identity or conditions: ok=%v err=%v", ok, err)
			}
			s.UpdateStatus(foreignDep, store.StatusPending, "", "foreign")
			if ok, err := evaluateReadyExpr(expr, s, foreignSelf, foreignDep); err != nil || !ok {
				t.Fatalf("status-only projection borrowed Flux identity: ok=%v err=%v", ok, err)
			}
		})
	}
	t.Run("ResourceSet pending self", func(t *testing.T) {
		t.Parallel()
		s := store.New()
		parent := &manifest.ResourceSet{Name: "parent", Namespace: "ns"}
		dependency := &manifest.Kustomization{Name: "child", Namespace: "ns"}
		s.AddObject(parent)
		s.AddObject(dependency)
		s.UpdateStatus(parent.Named(), store.StatusPending, "", "waiting on child")
		s.UpdateStatus(dependency.Named(), store.StatusReady, "", "")
		pending, _ := s.GetStatus(parent.Named())
		expr := fmt.Sprintf(`dep.status.conditions.exists(c, c.type == "Ready" && c.status == "True") && self.status.conditions.exists(c, c.type == "Ready" && c.status == %q && c.reason == %q && c.message == %q)`, metav1.ConditionUnknown, pending.Reason, pending.Message)
		if ok, err := evaluateReadyExpr(expr, s, parent.Named(), dependency.Named()); err != nil || !ok {
			t.Fatalf("ResourceSet self condition: ok=%v err=%v, want true/nil", ok, err)
		}
	})
}

// TestCompileReadyExpr_MemoizesErrors pins the compile-error cache: a known-bad
// expression should not recompile (and re-error) on every fire. Two compiles
// return the same error instance — sync.Map.LoadOrStore guarantees pointer
// identity for the cached entry.
func TestCompileReadyExpr_MemoizesErrors(t *testing.T) {
	t.Parallel()
	const bad = `this is not valid CEL ((` // unclosed paren = compile error
	_, err1 := compileReadyExpr(bad)
	_, err2 := compileReadyExpr(bad)
	if err1 == nil || err2 == nil {
		t.Fatal("expected compile error on both calls")
	}
	if !errors.Is(err1, err2) {
		t.Errorf("compile error not memoized: %p vs %p", err1, err2)
	}
}

func evaluateReadyExpr(expr string, s *store.Store, self, dep manifest.NamedResource) (bool, error) {
	var observation store.Observation
	obj := s.Snapshot(&dep, true, &observation)
	conds := observation.Conditions
	w := Waiter{Store: s, Parent: self}
	return w.evaluateReadyExpr(expr, dep, obj, conds)
}
