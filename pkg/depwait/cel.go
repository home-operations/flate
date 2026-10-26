package depwait

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/ast"
	"cel.dev/cel-go/common/operators"
	"cel.dev/cel-go/common/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

var celCache sync.Map // map[string]celCacheEntry

type celCacheEntry struct {
	prog       cel.Program
	err        error
	selfStatus bool
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
	compiled, err := compileReadyExpr(expr)
	if err != nil {
		return false, &celCompileErr{err}
	}
	var dependency, self func() any
	parent, st := w.Parent, w.Store
	if parent == dep {
		var projected any
		dependency = func() any {
			if projected == nil {
				projected = projectObject(dep, obj, conds)
			}
			return projected
		}
		self = dependency
	} else {
		dependency = func() any { return projectObject(dep, obj, conds) }
		if compiled.selfStatus {
			self = func() any {
				obj, info := st.Snapshot(parent, true)
				return projectObject(parent, obj, info.Conditions)
			}
		} else {
			self = func() any { return projectObject(parent, st.GetObject(parent), nil) }
		}
	}
	val, _, err := compiled.prog.Eval(map[string]any{"self": self, "dep": dependency})
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

func compileReadyExpr(expr string) (celCacheEntry, error) {
	v, ok := celCache.Load(expr)
	if !ok {
		v, _ = celCache.LoadOrStore(expr, buildCacheEntry(expr))
	}
	e := v.(celCacheEntry)
	return e, e.err
}

func buildCacheEntry(expr string) celCacheEntry {
	ast, issues := celEnv.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return celCacheEntry{err: fmt.Errorf("compile: %w", issues.Err())}
	}
	prog, err := celEnv.Program(ast)
	if err != nil {
		return celCacheEntry{err: fmt.Errorf("program: %w", err)}
	}
	return celCacheEntry{prog: prog, selfStatus: readsSelfStatus(ast)}
}

func readsSelfStatus(checked *cel.Ast) bool {
	for _, ident := range ast.MatchDescendants(ast.NavigateAST(checked.NativeRep()), ast.KindMatcher(ast.IdentKind)) {
		if ident.AsIdent() != "self" || shadowsSelf(ident) {
			continue
		}
		parent, ok := ident.Parent()
		if ok {
			switch parent.Kind() {
			case ast.SelectKind:
				if parent.AsSelect().FieldName() != "status" {
					continue
				}
			case ast.CallKind:
				call := parent.AsCall()
				args := call.Args()
				if call.FunctionName() == operators.Index && len(args) == 2 && args[0].ID() == ident.ID() && args[1].Kind() == ast.LiteralKind {
					if key, ok := args[1].AsLiteral().Value().(string); ok && key != "status" {
						continue
					}
				}
			}
		}
		return true
	}
	return false
}

func shadowsSelf(ident ast.NavigableExpr) bool {
	for child := ident; ; {
		parent, ok := child.Parent()
		if !ok {
			return false
		}
		if parent.Kind() == ast.ComprehensionKind {
			comp := parent.AsComprehension()
			inLoop := child.ID() == comp.LoopCondition().ID() || child.ID() == comp.LoopStep().ID()
			if inLoop && (comp.IterVar() == "self" || comp.IterVar2() == "self") {
				return true
			}
			if comp.AccuVar() == "self" && (inLoop || child.ID() == comp.Result().ID()) {
				return true
			}
		}
		child = parent
	}
}

func projectObject(id manifest.NamedResource, obj manifest.BaseManifest, conds []store.Condition) any {
	doc, err := manifest.ObjectDocument(id, obj)
	if err != nil {
		return types.WrapErr(err)
	}
	if obj == nil {
		slog.Debug("depwait: projectObject sees status without object", "id", id.String())
	}
	condsAny := make([]any, 0, len(conds))
	for _, c := range conds {
		condsAny = append(condsAny, conditionToMap(c))
	}
	// Offline snapshots have no apiserver generation; normalize both sides together.
	doc["metadata"].(map[string]any)["generation"] = int64(1)
	doc["status"] = map[string]any{
		"observedGeneration": int64(1),
		"conditions":         condsAny,
	}
	return doc
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
