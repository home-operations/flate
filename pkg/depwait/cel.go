package depwait

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// celCache memoizes compileReadyExpr results keyed by source text.
// dependsOn typically references the same expression many times (one
// per consumer), so compiling once per process saves the parse + check
// pass that cel-go does internally. The cache also memoizes *errors*
// so a malformed expression doesn't recompile on every fire of
// watchReadyExpr — every re-evaluation against a known-bad expression
// reuses the cached error.
//
// sync.Map (rather than map+Mutex) was chosen because the cache is
// read-heavy and rarely-mutated: thousands of evaluations against a
// stable corpus of a few dozen expressions. sync.Map's read path is
// lock-free for hits, which removes the per-evaluation Mutex
// contention multiple parallel waiters previously paid.
var celCache sync.Map // map[string]celCacheEntry

// celCacheEntry caches one of (program, error). Exactly one field is
// non-nil; readers branch on err first.
type celCacheEntry struct {
	prog cel.Program
	err  error
}

// celEnv is the singleton CEL environment used by all ReadyExpr
// evaluations. The declared variables `self` and `dep` mirror what
// Flux's kustomize/helm controllers expose (cel.WithStructVariables
// in upstream evalReadyExpr): the consumer (self) and the dependency
// (dep), each as a generic JSON-shaped view. We use map[string]any
// (DynType) rather than typed Kubernetes proto descriptors so user
// expressions remain stable across Kind changes and avoid pulling in
// k8s.io/api OpenAPI schemas.
var celEnv = mustCELEnv()

func mustCELEnv() *cel.Env {
	env, err := cel.NewEnv(
		cel.Variable("self", cel.DynType),
		cel.Variable("dep", cel.DynType),
	)
	if err != nil {
		panic("depwait: build CEL env: " + err.Error())
	}
	return env
}

// evaluateReadyExpr compiles (memoized) and evaluates expr against the
// projected views of self (consumer) and dep (dependency). Returns true
// iff the program produces a bool true.
//
// Errors are split: a compile/program error is wrapped in *celCompileErr
// (terminal — the expression itself is broken); an eval error against
// the projected object is wrapped in *celEvalErr (transient — the dep's
// status may not yet be populated, so callers polling on store events
// should keep waiting).
func evaluateReadyExpr(expr string, s *store.Store, self, dep manifest.NamedResource) (bool, error) {
	prog, err := compileReadyExpr(expr)
	if err != nil {
		return false, &celCompileErr{err}
	}
	val, _, err := prog.Eval(map[string]any{
		"self": projectObject(s, self),
		"dep":  projectObject(s, dep),
	})
	if err != nil {
		return false, &celEvalErr{fmt.Errorf("eval: %w", err)}
	}
	// asBool errors are type-shape problems with the expression itself
	// (e.g. `dep.metadata.name` returns a string). The user's expr is
	// broken regardless of dep state, so surface as terminal.
	ok, err := asBool(val)
	if err != nil {
		return false, &celCompileErr{err}
	}
	return ok, nil
}

// celCompileErr signals an unrecoverable expression problem (parse,
// type-check, or program assembly failure). Treated as terminal by
// every caller.
type celCompileErr struct{ err error }

func (e *celCompileErr) Error() string { return e.err.Error() }
func (e *celCompileErr) Unwrap() error { return e.err }

// celEvalErr signals a runtime evaluation failure against the current
// projection — typically a "no such attribute" because the dep's
// status hasn't landed yet. Polling callers should treat this as
// transient and re-evaluate on the next store event.
type celEvalErr struct{ err error }

func (e *celEvalErr) Error() string { return e.err.Error() }
func (e *celEvalErr) Unwrap() error { return e.err }

func compileReadyExpr(expr string) (cel.Program, error) {
	if v, ok := celCache.Load(expr); ok {
		e := v.(celCacheEntry)
		return e.prog, e.err
	}
	entry := celCacheEntry{}
	ast, issues := celEnv.Compile(expr)
	if issues != nil && issues.Err() != nil {
		entry.err = fmt.Errorf("compile: %w", issues.Err())
	} else {
		prog, err := celEnv.Program(ast)
		if err != nil {
			entry.err = fmt.Errorf("program: %w", err)
		} else {
			entry.prog = prog
		}
	}
	// LoadOrStore: a concurrent compile of the same expr both lose
	// races; the loser's prog/err is discarded and we return the
	// winner's. cel-go's compilation is deterministic so the two
	// results are equivalent.
	actual, _ := celCache.LoadOrStore(expr, entry)
	e := actual.(celCacheEntry)
	return e.prog, e.err
}

// projectObject builds the unstructured-shaped value the CEL
// expression sees for `self` / `dep`. Includes:
//   - apiVersion + kind
//   - metadata.{name,namespace,generation,labels,annotations}
//   - status.{observedGeneration,conditions}
//
// Labels and annotations are surfaced from the typed manifest in the
// store when available — common upstream Flux readiness idioms like
// `dep.metadata.annotations['app.kubernetes.io/component'] == 'cache'`
// rely on these being populated. The full spec is not yet projected;
// CEL expressions touching spec.* read undefined (documented gap).
func projectObject(s *store.Store, id manifest.NamedResource) map[string]any {
	// Snapshot the object AND its conditions atomically. Independent
	// GetObject + GetConditions calls would each take their own
	// s.mu.RLock; between them a writer can land an AddObject and/or
	// SetCondition, mixing the freshly-projected object with stale
	// conditions (or vice versa). For correlation-style CEL like
	//   dep.metadata.labels['component'] == 'cache' &&
	//   dep.status.conditions.exists(c, c.type == 'Ready' && c.status == 'True')
	// the mixed snapshot can render a false positive/negative until
	// the next event triggers re-evaluation.
	obj, conds := s.Snapshot(id)
	if obj == nil {
		// Status entry without an object — only the metadata/status
		// derived from id is reachable; labels and annotations are
		// absent. A CEL expression like dep.metadata.labels['x']
		// evaluating against this view sees no labels (has() returns
		// false), which is correct: the dep isn't fully visible yet.
		// Logged at Debug so an operator chasing a "readyExpr never
		// satisfied" can see whether the dep had reached the store as
		// an object or only as a phantom status entry.
		slog.Debug("depwait: projectObject sees status without object",
			"id", id.String())
	}
	condsAny := make([]any, 0, len(conds))
	for _, c := range conds {
		condsAny = append(condsAny, conditionToMap(c))
	}
	meta := map[string]any{
		"name":      id.Name,
		"namespace": id.Namespace,
		// flate has no apiserver, so there is no monotonically-
		// increasing generation count to model. The single-snapshot
		// render pins both metadata.generation and
		// status.observedGeneration to the same value so CEL
		// expressions like
		//   dep.status.observedGeneration == dep.metadata.generation
		// — a common Flux readiness idiom — never spuriously fail.
		"generation": int64(1),
	}
	labels, annotations := labelsAndAnnotations(obj)
	if labels != nil {
		meta["labels"] = labels
	}
	if annotations != nil {
		meta["annotations"] = annotations
	}
	return map[string]any{
		"kind":       id.Kind,
		"apiVersion": kindAPIVersion[id.Kind],
		"metadata":   meta,
		"status": map[string]any{
			"observedGeneration": int64(1),
			"conditions":         condsAny,
		},
	}
}

// labelsAndAnnotations extracts metadata.labels / metadata.annotations
// from the typed manifest when the type carries them. Returns nil maps
// when missing (so CEL's `has(dep.metadata.labels)` evaluates false
// rather than seeing an empty map). The conversion to map[string]any
// is needed because the CEL DynType resolver doesn't deep-walk
// map[string]string.
func labelsAndAnnotations(obj manifest.BaseManifest) (labels, annotations map[string]any) {
	type withMeta interface {
		GetLabels() map[string]string
		GetAnnotations() map[string]string
	}
	if m, ok := obj.(withMeta); ok {
		return stringMapToAny(m.GetLabels()), stringMapToAny(m.GetAnnotations())
	}
	return nil, nil
}

func stringMapToAny(in map[string]string) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func conditionToMap(c metav1.Condition) map[string]any {
	return map[string]any{
		"type":               c.Type,
		"status":             string(c.Status),
		"reason":             c.Reason,
		"message":            c.Message,
		"observedGeneration": c.ObservedGeneration,
	}
}

// kindAPIVersion maps each tracked Kind to its canonical apiVersion.
// Used by projectObject so CEL expressions inspecting object.apiVersion
// behave sensibly. Kinds absent from the map return "" — most ReadyExpr
// formulations don't read it, so the empty value is a safe default.
var kindAPIVersion = map[string]string{
	manifest.KindKustomization:    manifest.FluxKustomizeDomain + "/v1",
	manifest.KindHelmRelease:      manifest.HelmReleaseDomain + "/v2",
	manifest.KindGitRepository:    manifest.SourceDomain + "/v1",
	manifest.KindOCIRepository:    manifest.SourceDomain + "/v1",
	manifest.KindHelmRepository:   manifest.SourceDomain + "/v1",
	manifest.KindHelmChart:        manifest.SourceDomain + "/v1",
	manifest.KindBucket:           manifest.SourceDomain + "/v1",
	manifest.KindExternalArtifact: manifest.SourceDomain + "/v1",
}

func asBool(v ref.Val) (bool, error) {
	if v == nil {
		return false, fmt.Errorf("readyExpr returned nil")
	}
	if b, ok := v.Value().(bool); ok {
		return b, nil
	}
	if v.Type() == types.BoolType {
		return v.Equal(types.True).Value().(bool), nil
	}
	return false, fmt.Errorf("readyExpr must return bool; got %s", v.Type().TypeName())
}
