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
// evaluations. The single declared variable `object` mirrors what
// Flux's kustomize/helm controllers expose — a generic JSON-shaped
// view of the dep's resource. We use map[string]any (DynType) rather
// than the typed Kubernetes proto descriptors so user expressions
// remain stable across Kind changes and avoid pulling in
// k8s.io/api OpenAPI schemas.
var celEnv = mustCELEnv()

func mustCELEnv() *cel.Env {
	env, err := cel.NewEnv(
		cel.Variable("object", cel.DynType),
	)
	if err != nil {
		panic("depwait: build CEL env: " + err.Error())
	}
	return env
}

// evaluateReadyExpr compiles (memoized) and evaluates expr against the
// projected view of id. Returns true iff the program produces a bool
// true. Any compile, eval, or type-shape error is returned verbatim.
func evaluateReadyExpr(expr string, s *store.Store, id manifest.NamedResource) (bool, error) {
	prog, err := compileReadyExpr(expr)
	if err != nil {
		return false, err
	}
	object := projectObject(s, id)
	val, _, err := prog.Eval(map[string]any{"object": object})
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

// projectObject builds the unstructured-shaped `object` value the CEL
// expression sees. Includes metadata + status.conditions derived from
// the Store. We deliberately do NOT include the full BaseManifest
// payload — ReadyExpr should evaluate against the live status, not
// the spec. Mirrors how Flux's CEL env exposes only the runtime view.
func projectObject(s *store.Store, id manifest.NamedResource) map[string]any {
	conds := s.GetConditions(id)
	condsAny := make([]any, 0, len(conds))
	for _, c := range conds {
		condsAny = append(condsAny, conditionToMap(c))
	}
	return map[string]any{
		"kind":       id.Kind,
		"apiVersion": apiVersionFor(id.Kind),
		"metadata": map[string]any{
			"name":      id.Name,
			"namespace": id.Namespace,
		},
		"status": map[string]any{
			"conditions": condsAny,
		},
	}
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
