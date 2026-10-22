// Package depwait resolves a controller's NamedResource dependencies
// for the dag scheduler: Classify decides, WITHOUT blocking, whether
// each dep is Ready, terminally Failed, or still blocked (parkable).
// The shared readiness predicates (ordinary readiness, the ReadyExpr
// evaluator, discovery-record promotion) live here so the gate semantics
// are single-sourced.
package depwait

import (
	"errors"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// Waiter holds the parameters for one dependency-classification operation.
type Waiter struct {
	Dependencies []manifest.DependencyRef
	Declaration  manifest.BaseManifest
	Supplied     map[manifest.NamedResource]manifest.BaseManifest
	Store        *store.Store
	Parent       manifest.NamedResource

	// AllowMissingCRDs accepts CRD dependencies absent from the offline inputs
	// because the cluster installs them at runtime.
	AllowMissingCRDs bool
	// WipeSecrets permits declared-key placeholder interpretation.
	WipeSecrets bool
	// AllowMissingSecrets permits required Helm values omission at the fixpoint.
	AllowMissingSecrets bool

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
// discovery loader kept out of the Store) get lazy-promoted
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
