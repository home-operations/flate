// Package depwait resolves a controller's NamedResource dependencies
// for the dag scheduler: Classify decides, WITHOUT blocking, whether
// each dep is Ready, terminally Failed, or still blocked (parkable).
// The shared readiness predicates (readyNow, the ReadyExpr
// evaluator, discovery-record promotion) live here so the gate semantics
// are single-sourced.
package depwait

import (
	"errors"
	"slices"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Waiter holds the parameters for one dependency-classification operation.
type Waiter struct {
	Store  *store.Store
	Parent manifest.NamedResource

	// AllowMissingCRDs accepts CRD dependencies absent from the offline inputs
	// because the cluster installs them at runtime.
	AllowMissingCRDs bool

	// Existence, when non-nil, lets Classify lazy-promote a missing dep
	// from the loader's discovery records before deciding it is
	// absent. The orchestrator wires this against the discovery index;
	// tests supply stubs. When nil, an absent dep is simply absent.
	//
	// See ExistenceLookup for the decision matrix.
	Existence ExistenceLookup
}

// ExistenceLookup is the seam Classify uses to resolve missing
// dependencies. Bundled into one interface so the orchestrator
// (and any future embedder) wires a single object through the
// controller Options pipe instead of two parallel closures.
//
// The orchestrator's implementation reads from the loader's
// discovery index — file-indexed objects (CMs/Secrets/HRs the
// DiscoveryOnly loader kept out of the Store) get lazy-promoted
// the moment a depwait edge needs them, while render-only ids
// (no file record) stay absent until a parent render's
// emitRenderedChildren chain produces them.
type ExistenceLookup interface {
	// IsFileIndexed reports whether id has a raw-file discovery record.
	IsFileIndexed(id manifest.NamedResource) bool
	// Promote attempts to materialize id into the Store from the
	// discovery index. Returns true when promotion succeeded
	// and id is now reachable via store.GetObject.
	Promote(id manifest.NamedResource) bool
}

// Bucket names by their first byte to avoid hashing statusless dependencies.
var statusKinds = func() [256][]string {
	var kinds [256][]string
	for kind := range manifest.Kinds() {
		if kind.Status {
			first := kind.Kind[0]
			kinds[first] = append(kinds[first], kind.Kind)
		}
	}
	return kinds
}()

type dependencyState uint8

const (
	depAbsent dependencyState = iota
	depPending
	depReady
)

func (w *Waiter) readyNow(dep manifest.DependencyRef) dependencyState {
	id := dep.NamedResource
	obj := w.Store.GetObject(id)
	if obj == nil {
		if _, exists := w.Store.GetStatus(id); !exists {
			return depAbsent
		}
	}
	if !supportsStatus(id.Kind, obj) {
		return depReady
	}
	if dep.ReadyExpr != "" {
		if ready, _ := w.tryReadyExpr(dep.ReadyExpr, id); ready {
			return depReady
		}
		return depPending
	}
	info, ok := w.Store.GetStatus(id)
	if ok && info.Status == store.StatusReady {
		return depReady
	}
	return depPending
}

// Status-only entries have no API group; preserve their kind-based readiness.
func supportsStatus(kind string, obj manifest.BaseManifest) bool {
	if kind == "" || !slices.Contains(statusKinds[kind[0]], kind) {
		return false
	}
	if obj != nil {
		return manifest.KindOf(obj).Status
	}
	return true
}

// tryReadyExpr evaluates expr once against id's projected state and reports:
//   - ready=true            → the CEL produced a definitive true.
//   - failMsg!=""           → a compile error that no amount of polling will
//     fix (the dep can never satisfy); the string is the byte-exact reason.
//   - ready=false,failMsg="" → a clean false OR a transient runtime eval error
//     (typically a missing attribute because the dep's status isn't populated
//     yet) — the dep is not yet satisfied but may still become so.
func (w *Waiter) tryReadyExpr(expr string, id manifest.NamedResource) (ready bool, failMsg string) {
	ok, err := evaluateReadyExpr(expr, w.Store, w.Parent, id)
	if err != nil {
		if _, isCompile := errors.AsType[*celCompileErr](err); isCompile {
			return false, "readyExpr: " + err.Error()
		}
		// Eval error: transient, treat as not-yet-ready.
		return false, ""
	}
	return ok, ""
}
