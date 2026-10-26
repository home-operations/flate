package depwait

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/home-operations/flate/pkg/schedule"

	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/manifest"

	"github.com/home-operations/flate/pkg/store"
)

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
			s.UpdateStatus(dep, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			expr := fmt.Sprintf(`dep.status.observedGeneration == dep.metadata.generation && dep.apiVersion == %q`, tc.version)
			ok, err := evaluateReadyExpr(expr, s, manifest.NamedResource{}, dep)
			if err != nil || !ok {
				t.Errorf("readiness projection: ok=%v err=%v, want true/nil", ok, err)
			}
		})
	}
}

func TestReadyExpr_DesiredSpec(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		id       manifest.NamedResource
		spec     func(string) map[string]any
		expr     string
		presence string
	}{
		{
			name: "Kustomization path",
			id:   testutil.KustomizationID("ns", "dep"),
			spec: func(value string) map[string]any {
				return map[string]any{"path": "./" + value}
			},
			expr:     `dep.spec.path == self.spec.path`,
			presence: `has(dep.spec) && has(self.spec) && has(dep.spec.path) && has(self.spec.path) && !has(dep.spec.postBuild) && !has(self.spec.postBuild)`,
		},
		{
			name: "Kustomization postBuild",
			id:   testutil.KustomizationID("ns", "dep"),
			spec: func(value string) map[string]any {
				return map[string]any{
					"path": "./apps",
					"postBuild": map[string]any{
						"substitute": map[string]any{"releaseVersion": value},
					},
				}
			},
			expr:     `dep.spec.postBuild.substitute.releaseVersion == self.spec.postBuild.substitute.releaseVersion`,
			presence: `has(dep.spec) && has(self.spec) && has(dep.spec.postBuild.substitute.releaseVersion) && has(self.spec.postBuild.substitute.releaseVersion) && !has(dep.spec.postBuild.substituteFrom) && !has(self.spec.postBuild.substituteFrom)`,
		},
		{
			name: "HelmRelease values",
			id:   manifest.NamedResource{Group: manifest.HelmReleaseDomain, Kind: manifest.KindHelmRelease, Namespace: "ns", Name: "dep"},
			spec: func(value string) map[string]any {
				return map[string]any{
					"chartRef": map[string]any{"kind": "OCIRepository", "name": "chart"},
					"values":   map[string]any{"releaseVersion": value},
				}
			},
			expr:     `dep.spec.values.releaseVersion == self.spec.values.releaseVersion`,
			presence: `has(dep.spec) && has(self.spec) && has(dep.spec.values.releaseVersion) && has(self.spec.values.releaseVersion) && !has(dep.spec.chart) && !has(self.spec.chart)`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, equal := range []bool{true, false} {
				t.Run(fmt.Sprintf("equal=%v", equal), func(t *testing.T) {
					t.Parallel()
					s := store.New()
					self := tc.id
					self.Name = "self"
					for _, id := range []manifest.NamedResource{tc.id, self} {
						value := "v1"
						if id == self && !equal {
							value = "v2"
						}
						obj, _, _, err := manifest.ParseDoc(map[string]any{
							"apiVersion": id.KindInfo().APIVersion(),
							"kind":       id.Kind,
							"metadata":   map[string]any{"name": id.Name, "namespace": id.Namespace},
							"spec":       tc.spec(value),
						}, manifest.ParseDocOptions{})
						if err != nil {
							t.Fatal(err)
						}
						s.AddObject(obj)
						s.UpdateStatus(id, store.StatusInfo{Status: store.StatusReady})
					}
					if ok, err := evaluateReadyExpr(tc.expr, s, self, tc.id); err != nil || ok != equal {
						t.Errorf("desired-spec comparison: ok=%v err=%v, want %v/nil", ok, err, equal)
					}
					if ok, err := evaluateReadyExpr(tc.presence, s, self, tc.id); err != nil || !ok {
						t.Errorf("authored field presence: ok=%v err=%v, want true/nil", ok, err)
					}
					w := Waiter{Store: s, Parent: self}
					ref := manifest.DependencyRef{NamedResource: tc.id, ReadyExpr: new(tc.expr)}
					want := schedule.Blocked
					if equal {
						want = schedule.Terminal
					}
					if got := w.Classify(t.Context(), ref, manifest.DrainNone); got.Kind != want || got.Err != nil || got.Message != "" {
						t.Errorf("desired-spec readiness: got %+v, want kind=%v without error or message", got, want)
					}
				})
			}
		})
	}
}

// TestReadyExpr_ReplaceOverridesBuiltin: in the default (replace) mode a
// satisfied ReadyExpr makes the dep Ready even when the built-in status is
// Failed — Classify must return schedule.Terminal (readyNow short-circuits on the CEL).
func TestReadyExpr_ReplaceOverridesBuiltin(t *testing.T) {
	t.Parallel()
	s := store.New()
	dep := testutil.KustomizationID("ns", "infra")
	s.UpdateStatus(dep, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "still rolling out"}) // built-in says Failed
	s.SetCondition(dep, store.Condition{Type: "Healthy", Status: metav1.ConditionTrue, Reason: "MyCustomCheck"})

	w := &Waiter{Store: s}
	ref := manifest.DependencyRef{NamedResource: dep,
		ReadyExpr: new(`dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")`),
	}
	if got := w.Classify(t.Context(), ref, manifest.DrainNone); got.Kind != schedule.Terminal {
		t.Errorf("replace ReadyExpr true over Failed built-in: got %+v, want schedule.Terminal", got)
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
				s.UpdateStatus(dep, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "ok"})
			}

			if _, err := evaluateReadyExpr(`this isn't valid CEL`, s, manifest.NamedResource{}, dep); err == nil {
				t.Fatal("expected compile error")
			}
			w := &Waiter{Store: s}
			ref := manifest.DependencyRef{NamedResource: dep, ReadyExpr: new(`this isn't valid CEL`)}
			got := w.Classify(t.Context(), ref, manifest.DrainNone)
			if got.Kind != schedule.DependencyFailed || !strings.HasPrefix(got.Message, "readyExpr:") {
				t.Errorf("compile error: got %+v, want schedule.DependencyFailed with 'readyExpr:' prefix", got)
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
	s.UpdateStatus(dep, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: "ok"})

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
	s.UpdateStatus(dep, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})

	w := &Waiter{Store: s}
	const expr = `dep.status.conditions[100].type == "Ready"` // index out of range → runtime eval error
	var observation store.Observation
	obj, observation := s.Snapshot(dep, true)
	conds := observation.Conditions
	if ready, failMsg := w.tryReadyExpr(manifest.DependencyRef{NamedResource: dep, ReadyExpr: new(expr)}, obj, conds); ready || failMsg != "" {
		t.Fatalf("transient eval error: ready=%v failMsg=%q, want false/empty (not terminal)", ready, failMsg)
	}
	ref := manifest.DependencyRef{NamedResource: dep, ReadyExpr: new(expr)}
	if got := w.Classify(t.Context(), ref, manifest.DrainNone); got.Kind != schedule.Blocked {
		t.Errorf("transient eval error@none: got %+v, want schedule.Blocked (re-runnable, not failed)", got)
	}
}

// TestReadyExpr_ReadsLabelsAndAnnotations locks the upstream Flux contract that
// readyExpr can read dep.metadata.{labels,annotations} via the CEL projection.
func TestReadyExpr_ReadsLabelsAndAnnotations(t *testing.T) {
	t.Parallel()
	s := store.New()
	dep := testutil.KustomizationID("ns", "child")
	s.AddObject(&manifest.Kustomization{
		Name: "child", Namespace: "ns", Path: "./old",
		Labels:      map[string]string{"tier": "data"},
		Annotations: map[string]string{"app.kubernetes.io/version": "1.2.3"},
	})
	s.UpdateStatus(dep, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	var observation store.Observation
	obj, observation := s.Snapshot(dep, true)
	conds := observation.Conditions
	fresh := obj.(*manifest.Kustomization).Clone()
	fresh.Path = "./new"
	fresh.Labels = map[string]string{"tier": "changed"}
	fresh.Annotations = map[string]string{"app.kubernetes.io/version": "2.0.0"}
	s.AddObject(fresh)
	s.UpdateStatus(dep, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "replacement"})
	w := Waiter{Store: s}

	for _, expr := range []string{
		`dep.metadata.labels["tier"] == "data"`,
		`dep.metadata.annotations["app.kubernetes.io/version"] == "1.2.3"`,
	} {
		expr += ` && dep.spec.path == "./old" && dep.status.conditions.exists(c, c.type == "Ready" && c.status == "True")`
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
	s.UpdateStatus(self, store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	s.UpdateStatus(dep, store.StatusInfo{Status: store.StatusReady, Reason: store.ReasonSkipped, Message: "changed diagnostic"})

	ok, err := evaluateReadyExpr(`self.metadata.namespace == dep.metadata.namespace && self.status.conditions.exists(c, c.type == "Ready" && c.reason == "Succeeded") && dep.status.conditions.exists(c, c.type == "Ready" && c.reason == "Skipped" && c.status == "True")`, s, self, dep)
	if err != nil || !ok {
		t.Errorf("self/dep binding: ok=%v err=%v, want true/nil", ok, err)
	}
	var observation store.Observation
	obj, observation := s.Snapshot(dep, true)
	conds := observation.Conditions
	s.UpdateStatus(dep, store.StatusInfo{Status: store.StatusFailed, Reason: "", Message: "replacement"})
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
			s.UpdateStatus(foreignDep, store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "foreign"})
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
		s.UpdateStatus(parent.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "waiting on child"})
		s.UpdateStatus(dependency.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
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
	obj, observation := s.Snapshot(dep, true)
	conds := observation.Conditions
	w := Waiter{Store: s, Parent: self}
	return w.evaluateReadyExpr(expr, dep, obj, conds)
}

func TestReadyExpr_SelfCapture(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		expr   string
		status bool
		same   bool
	}{
		{name: "dot metadata", expr: `self.metadata.labels.tier == 'data'`},
		{name: "literal index", expr: `self['metadata']['labels']['tier'] == 'data'`},
		{name: "presence", expr: `has(self.metadata.labels)`},
		{name: "spec presence", expr: `has(self.spec)`},
		{name: "spec value", expr: `self.spec.path == './apps'`},
		{name: "status dot", expr: `self.status.conditions.exists(c, c.status == 'True')`, status: true},
		{name: "status index", expr: `self['status'].conditions.exists(c, c.status == 'True')`, status: true},
		{name: "status presence", expr: `has(self.status)`, status: true},
		{name: "whole object", expr: `self == self`, status: true},
		{name: "alias", expr: `[self].all(s, s.status.conditions.exists(c, c.status == 'True'))`, status: true},
		{name: "dynamic index", expr: `self[dep.metadata.labels.field].labels.tier == 'data'`, status: true},
		{name: "computed index", expr: `self[['metadata'][0]].labels.tier == 'data'`, status: true},
		{name: "macro range", expr: `self.metadata.labels.all(self, self != '')`},
		{name: "shadowed status", expr: `[{'status': true}].all(self, self.status) && self.metadata.labels.tier == 'data'`},
		{name: "nested shadow", expr: `[1].all(self, [self].all(x, x == 1)) && self.metadata.labels.tier == 'data'`},
		{name: "outer self in range", expr: `[self.status].all(self, self.conditions.exists(c, c.status == 'True'))`, status: true},
		{name: "global after shadow", expr: `[1].all(self, self == 1) && self.status.conditions.exists(c, c.status == 'True')`, status: true},
		{name: "status text", expr: `'self.status' == 'self.status' && self.metadata.labels.tier == 'data'`},
		{name: "constant", expr: `true`},
		{name: "lazy branch", expr: `true || self.status.conditions.size() > 0`},
		{name: "same identity", expr: `self.metadata.labels.tier == 'data'`, status: true, same: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			self := &manifest.Kustomization{Name: "parent", Namespace: "ns", Labels: map[string]string{"tier": "data"}, Path: "./apps"}
			dep := &manifest.Kustomization{Name: "dep", Namespace: "ns", Labels: map[string]string{"field": "metadata"}}
			if tc.same {
				dep = self
			}
			s.AddObject(self)
			s.AddObject(dep)
			s.UpdateStatus(self.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			s.UpdateStatus(dep.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			var reads store.ReadSet
			reads.Init(s, self.Named(), nil)
			defer reads.Retire()
			obj, info := reads.Snapshot(dep.Named(), true)
			w := Waiter{Store: &reads, Parent: self.Named()}
			ok, err := w.evaluateReadyExpr(tc.expr, dep.Named(), obj, info.Conditions)
			if err != nil || !ok {
				t.Fatalf("evaluation: ok=%v err=%v, want true/nil", ok, err)
			}
			if !reads.Valid() {
				t.Fatal("fresh read set invalid")
			}
			s.UpdateStatus(self.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "rendering"})
			if got := reads.Valid(); got == tc.status {
				t.Fatalf("valid after self status change = %v, want %v", got, !tc.status)
			}
		})
	}
}

func TestReadyExpr_DeclarationChanges(t *testing.T) {
	t.Parallel()
	for _, field := range []string{"label", "spec", "constant"} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			s := store.New()
			self := &manifest.Kustomization{Name: "parent", Namespace: "ns", Labels: map[string]string{"tier": "data"}, Path: "./apps"}
			dep := &manifest.Kustomization{Name: "dep", Namespace: "ns"}
			s.AddObject(self)
			s.AddObject(dep)
			s.UpdateStatus(dep.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
			var reads store.ReadSet
			reads.Init(s, self.Named(), nil)
			defer reads.Retire()
			obj, info := reads.Snapshot(dep.Named(), true)
			w := Waiter{Store: &reads, Parent: self.Named()}
			expr := `self.metadata.labels.tier == 'data'`
			switch field {
			case "spec":
				expr = `self.spec.path == './apps'`
			case "constant":
				expr = `true`
			}
			if ok, err := w.evaluateReadyExpr(expr, dep.Named(), obj, info.Conditions); err != nil || !ok {
				t.Fatalf("evaluation: ok=%v err=%v, want true/nil", ok, err)
			}
			changed := self.Clone()
			if field == "spec" {
				changed.Path = "./changed"
			} else {
				changed.Labels = map[string]string{"tier": "changed"}
			}
			s.AddObject(changed)
			if got, want := reads.Valid(), field == "constant"; got != want {
				t.Fatalf("valid after declaration replacement = %v, want %v", got, want)
			}
		})
	}
}

func TestReadyExpr_DependencyConditions(t *testing.T) {
	t.Parallel()
	s := store.New()
	dep := &manifest.Kustomization{Name: "dep", Namespace: "ns"}
	s.AddObject(dep)
	s.UpdateStatus(dep.Named(), store.StatusInfo{Status: store.StatusReady, Reason: "", Message: ""})
	var reads store.ReadSet
	reads.Init(s, testutil.KustomizationID("ns", "parent"), nil)
	defer reads.Retire()
	w := Waiter{Store: &reads, Parent: testutil.KustomizationID("ns", "parent")}
	ref := manifest.DependencyRef{NamedResource: dep.Named(), ReadyExpr: new(`dep.status.conditions.exists(c, c.status == 'True')`)}
	if got := w.Classify(t.Context(), ref, manifest.DrainNone); got.Kind != schedule.Terminal {
		t.Fatalf("dependency: %+v", got)
	}
	s.UpdateStatus(dep.Named(), store.StatusInfo{Status: store.StatusPending, Reason: "", Message: "rendering"})
	if reads.Valid() {
		t.Fatal("dependency conditions did not invalidate reads")
	}
}

func TestReadyExpr_CapturedSpec(t *testing.T) {
	t.Parallel()
	for _, same := range []bool{true, false} {
		t.Run(fmt.Sprintf("same=%v", same), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			docs, err := manifest.SplitDocs([]byte(`apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata: {name: dep, namespace: ns, labels: {version: old}, annotations: {revision: old}}
spec: {path: ./old}
`))
			if err != nil {
				t.Fatal(err)
			}
			obj, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
			if err != nil {
				t.Fatal(err)
			}
			dep := obj.(*manifest.Kustomization)
			s.AddObject(dep)
			s.UpdateStatus(dep.Named(), store.StatusInfo{Status: store.StatusReady})
			parent := dep.Named()
			if !same {
				self := dep.Clone()
				self.Name, self.Path = "self", "./consumer"
				s.AddObject(self)
				parent = self.Named()
			}
			captured, published := make(chan struct{}), make(chan struct{})
			var wg sync.WaitGroup
			wg.Go(func() {
				var reads store.ReadSet
				reads.Init(s, parent, nil)
				defer reads.Retire()
				old, info := reads.Snapshot(dep.Named(), true)
				close(captured)
				<-published
				w := Waiter{Store: &reads, Parent: parent}
				expr := `dep.spec.path == './old' && dep.metadata.labels.version == 'old' && dep.metadata.annotations.revision == 'old' && dep.status.conditions.exists(c, c.type == 'Ready' && c.status == 'True') && dep.metadata.generation == dep.status.observedGeneration`
				if same {
					expr += ` && self.spec == dep.spec && self.status == dep.status`
				} else {
					expr += ` && self.spec.path == './consumer'`
				}
				if ok, err := w.evaluateReadyExpr(expr, dep.Named(), old, info.Conditions); err != nil || !ok {
					t.Errorf("old capture: ok=%v err=%v", ok, err)
				}
				if reads.Valid() {
					t.Error("replaced observation remained valid")
				}
			})
			wg.Go(func() {
				<-captured
				fresh := dep.Clone()
				fresh.Path, fresh.Labels["version"], fresh.Annotations["revision"] = "./new", "new", "new"
				s.AddObject(fresh)
				s.UpdateStatus(dep.Named(), store.StatusInfo{Status: store.StatusFailed, Message: "new publication"})
				close(published)
				var reads store.ReadSet
				reads.Init(s, parent, nil)
				defer reads.Retire()
				current, info := reads.Snapshot(dep.Named(), true)
				w := Waiter{Store: &reads, Parent: parent}
				expr := `dep.spec.path == './new' && dep.metadata.labels.version == 'new' && dep.metadata.annotations.revision == 'new' && dep.status.conditions.exists(c, c.type == 'Ready' && c.status == 'False')`
				if same {
					expr += ` && self.spec == dep.spec && self.status == dep.status`
				}
				if ok, err := w.evaluateReadyExpr(expr, dep.Named(), current, info.Conditions); err != nil || !ok {
					t.Errorf("new capture: ok=%v err=%v", ok, err)
				}
				if !reads.Valid() {
					t.Error("fresh observation invalid")
				}
			})
			wg.Wait()
		})
	}
}

type countedDocument struct {
	*manifest.HelmRelease
	calls int
}

func (d *countedDocument) RenderDocument() (map[string]any, error) {
	d.calls++
	return d.HelmRelease.RenderDocument()
}

func TestReadyExpr_ProjectionError(t *testing.T) {
	t.Parallel()
	docs, err := manifest.SplitDocs([]byte(`apiVersion: helm.toolkit.fluxcd.io/v2
kind: HelmRelease
metadata: {name: dep, namespace: ns}
spec: {chartRef: {kind: OCIRepository, name: chart}, values: {version: old}}
`))
	if err != nil {
		t.Fatal(err)
	}
	obj, _, _, err := manifest.ParseDoc(docs[0], manifest.ParseDocOptions{})
	if err != nil {
		t.Fatal(err)
	}
	broken := obj.(*manifest.HelmRelease).Clone()
	broken.HelmReleaseSpec.Values = &apix.JSON{Raw: []byte("{")}
	dep := &countedDocument{HelmRelease: broken}
	s := store.New()
	s.AddObject(dep)
	s.UpdateStatus(dep.Named(), store.StatusInfo{Status: store.StatusReady})
	var reads store.ReadSet
	reads.Init(s, dep.Named(), nil)
	defer reads.Retire()
	captured, info := reads.Snapshot(dep.Named(), true)
	w := Waiter{Store: &reads, Parent: dep.Named()}
	for _, expr := range []string{`true`, `true || self.spec.values.version == dep.spec.values.version`} {
		if ok, err := w.evaluateReadyExpr(expr, dep.Named(), captured, info.Conditions); err != nil || !ok {
			t.Fatalf("lazy evaluation: ok=%v err=%v", ok, err)
		}
	}
	if dep.calls != 0 {
		t.Fatal("short circuit converted an unused object")
	}
	expr := `self.spec == dep.spec`
	if ok, err := w.evaluateReadyExpr(expr, dep.Named(), captured, info.Conditions); ok || !errors.Is(err, manifest.ErrInput) {
		t.Fatalf("projection failure: ok=%v err=%v, want input error", ok, err)
	} else if _, terminal := errors.AsType[*celCompileErr](err); terminal {
		t.Fatalf("projection failure classified as compile error: %v", err)
	}
	if dep.calls != 1 {
		t.Fatalf("same identity projected %d times, want 1", dep.calls)
	}
	ref := manifest.DependencyRef{NamedResource: dep.Named(), ReadyExpr: new(expr)}
	if ready, failMsg := w.tryReadyExpr(ref, captured, info.Conditions); ready || failMsg != "" {
		t.Fatalf("projection failure readiness: ready=%v failMsg=%q", ready, failMsg)
	}
	if got := w.Classify(t.Context(), ref, manifest.DrainNone); got.Kind != schedule.Blocked {
		t.Fatalf("projection failure classification: %+v", got)
	}
	if !reads.Valid() {
		t.Fatal("projection failure invalidated unchanged observations")
	}
	s.UpdateStatus(dep.Named(), store.StatusInfo{Status: store.StatusPending})
	if reads.Valid() {
		t.Fatal("projection failure lost captured condition observation")
	}
}

func TestReadyExpr_AbsentSpec(t *testing.T) {
	t.Parallel()
	for _, statusOnly := range []bool{false, true} {
		t.Run(fmt.Sprint(statusOnly), func(t *testing.T) {
			t.Parallel()
			s := store.New()
			dep := testutil.KustomizationID("", "dep")
			if statusOnly {
				s.UpdateStatus(dep, store.StatusInfo{Status: store.StatusPending})
			}
			if ok, err := evaluateReadyExpr(`!has(dep.spec) && !has(dep.metadata.labels) && dep.metadata.namespace == ''`, s, dep, dep); err != nil || !ok {
				t.Fatalf("absent desired fields: ok=%v err=%v", ok, err)
			}
			w := Waiter{Store: s, Parent: dep}
			ref := manifest.DependencyRef{NamedResource: dep, ReadyExpr: new(`has(dep.spec)`)}
			if got := w.Classify(t.Context(), ref, manifest.DrainNone); got.Kind != schedule.Blocked {
				t.Fatalf("unavailable at normal evaluation: %+v", got)
			}
			if got := w.Classify(t.Context(), ref, manifest.DrainCascade); got.Kind != schedule.DependencyFailed {
				t.Fatalf("unavailable at cascade: %+v", got)
			}
		})
	}
}
