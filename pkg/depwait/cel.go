package depwait

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"cel.dev/cel-go/cel"
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

func (w *Waiter) evaluateReadyExpr(expr string, dep manifest.NamedResource, obj manifest.BaseManifest, conds []store.Condition) (bool, error) {
	prog, err := compileReadyExpr(expr)
	if err != nil {
		return false, &celCompileErr{err}
	}
	var dependency, self func() any
	parent, st := w.Parent, w.Store
	if parent == dep {
		var projected map[string]any
		dependency = func() any {
			if projected == nil {
				projected = projectObject(dep, obj, conds)
			}
			return projected
		}
		self = dependency
	} else {
		dependency = func() any { return projectObject(dep, obj, conds) }
		self = func() any {
			var info store.Observation
			obj := st.Snapshot(new(parent), true, &info)
			return projectObject(parent, obj, info.Conditions)
		}
	}
	val, _, err := prog.Eval(map[string]any{"self": self, "dep": dependency})
	if err != nil {
		return false, fmt.Errorf("eval: %w", err)
	}
	// A non-bool result is an expression error, independent of dependency state.
	if val == nil {
		return false, &celCompileErr{errors.New("readyExpr returned nil")}
	}
	if b, ok := val.Value().(bool); ok {
		return b, nil
	}
	return false, &celCompileErr{fmt.Errorf("readyExpr must return bool; got %s", val.Type().TypeName())}
}

// celCompileErr signals an unrecoverable expression problem (parse,
// type-check, or program assembly failure). Treated as terminal by
// every caller.
type celCompileErr struct{ err error }

func (e *celCompileErr) Error() string { return e.err.Error() }
func (e *celCompileErr) Unwrap() error { return e.err }

func compileReadyExpr(expr string) (cel.Program, error) {
	v, ok := celCache.Load(expr)
	if !ok {
		// LoadOrStore: concurrent compiles of the same expr both race; the
		// loser's entry is discarded and we return the winner's. cel-go's
		// compilation is deterministic so the two results are equivalent.
		v, _ = celCache.LoadOrStore(expr, buildCacheEntry(expr))
	}
	e := v.(celCacheEntry)
	return e.prog, e.err
}

// buildCacheEntry compiles expr into the program (or terminal error)
// that compileReadyExpr memoizes.
func buildCacheEntry(expr string) celCacheEntry {
	ast, issues := celEnv.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return celCacheEntry{err: fmt.Errorf("compile: %w", issues.Err())}
	}
	prog, err := celEnv.Program(ast)
	if err != nil {
		return celCacheEntry{err: fmt.Errorf("program: %w", err)}
	}
	return celCacheEntry{prog: prog}
}

// projectObject keeps metadata and conditions from the same observation.
func projectObject(id manifest.NamedResource, obj manifest.BaseManifest, conds []store.Condition) map[string]any {
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
		"apiVersion": id.KindInfo().APIVersion(),
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
