package depwait

import (
	"fmt"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// celCache memoizes compiled CEL programs keyed by their source text.
// dependsOn typically references the same expression many times (one
// per consumer), so compiling once per process saves the parse + check
// pass that cel-go does internally.
var (
	celCacheMu sync.Mutex
	celCache   = map[string]cel.Program{}
)

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
// iff the program produces a bool true. Any compile, eval, or type-
// shape error is returned verbatim.
func evaluateReadyExpr(expr string, s *store.Store, self, dep manifest.NamedResource) (bool, error) {
	prog, err := compileReadyExpr(expr)
	if err != nil {
		return false, err
	}
	val, _, err := prog.Eval(map[string]any{
		"self": projectObject(s, self),
		"dep":  projectObject(s, dep),
	})
	if err != nil {
		return false, fmt.Errorf("eval: %w", err)
	}
	return asBool(val)
}

func compileReadyExpr(expr string) (cel.Program, error) {
	celCacheMu.Lock()
	defer celCacheMu.Unlock()
	if prog, ok := celCache[expr]; ok {
		return prog, nil
	}
	ast, issues := celEnv.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, fmt.Errorf("compile: %w", issues.Err())
	}
	prog, err := celEnv.Program(ast)
	if err != nil {
		return nil, fmt.Errorf("program: %w", err)
	}
	celCache[expr] = prog
	return prog, nil
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
	if labels, annotations := labelsAndAnnotations(obj); labels != nil || annotations != nil {
		if labels != nil {
			meta["labels"] = labels
		}
		if annotations != nil {
			meta["annotations"] = annotations
		}
	}
	return map[string]any{
		"kind":       id.Kind,
		"apiVersion": apiVersionFor(id.Kind),
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

// apiVersionFor returns the well-known apiVersion for kinds flate
// tracks. Used so CEL expressions inspecting `object.apiVersion`
// behave sensibly. Unknown kinds get an empty apiVersion — that's
// fine; most ReadyExpr formulations don't read it.
func apiVersionFor(kind string) string {
	switch kind {
	case manifest.KindKustomization:
		return manifest.FluxKustomizeDomain + "/v1"
	case manifest.KindHelmRelease:
		return manifest.HelmReleaseDomain + "/v2"
	case manifest.KindGitRepository,
		manifest.KindOCIRepository,
		manifest.KindHelmRepository,
		manifest.KindHelmChart,
		manifest.KindBucket,
		manifest.KindExternalArtifact:
		return manifest.SourceDomain + "/v1"
	}
	return ""
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
