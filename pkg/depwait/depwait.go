// Package depwait resolves a controller's NamedResource dependencies
// for the dag scheduler: Classify decides, WITHOUT blocking, whether
// each dep is Ready, terminally Failed, or still blocked (parkable).
// The shared readiness predicates (readyNow, depExists, the ReadyExpr
// evaluator, Existence-index promotion) live here so the gate semantics
// are single-sourced.
package depwait

import (
	"errors"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// DefaultTimeout is the per-dep timeout when not specified. The
// upstream Flux controllers default to several minutes since they
// wait for in-cluster reconciliation; flate is purely offline, so
// waits past a few seconds almost always indicate a misconfigured
// reference. Keep this short so typos in dependsOn / sourceRef
// surface immediately instead of stalling a render.
const DefaultTimeout = 30 * time.Second

// TimeoutFromSpec resolves a Flux spec.timeout (`*metav1.Duration` —
// the shape used by both Kustomization and HelmRelease) into the
// effective per-dep wait. Honors a user-supplied value when set; falls
// back to flate's offline-tuned DefaultTimeout otherwise. Matches the
// principle that a real Flux reconcile would respect spec.timeout.
func TimeoutFromSpec(d *metav1.Duration) time.Duration {
	if d == nil || d.Duration <= 0 {
		return DefaultTimeout
	}
	return d.Duration
}

// Summary tallies the failed dependencies of a Require gate. base.Require
// appends each terminally-failed dep and its reason; base.DepFailed folds the
// result into a manifest.DependencyFailedError.
type Summary struct {
	Failed   []manifest.NamedResource
	Messages map[manifest.NamedResource]string
}

// AnyFailed reports whether at least one dependency ended in failure.
func (s Summary) AnyFailed() bool { return len(s.Failed) > 0 }

// Waiter holds the parameters for one dependency-classification operation.
type Waiter struct {
	Store   *store.Store
	Parent  manifest.NamedResource
	Timeout time.Duration

	// AdditiveReadyExpr toggles Flux's AdditiveCELDependencyCheck
	// feature gate. When false (the default, matching Flux), a
	// dep's ReadyExpr REPLACES the built-in Ready check — flate
	// treats the dep as Ready when the expression returns true. When
	// true, the dep must satisfy both the built-in Ready check AND
	// the ReadyExpr (additive mode).
	AdditiveReadyExpr bool

	// Existence, when non-nil, lets Classify lazy-promote a missing dep
	// from the loader's file-existence index before deciding it is
	// absent. The orchestrator wires this against the ExistenceIndex;
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
// ExistenceIndex — file-indexed objects (CMs/Secrets/HRs the
// DiscoveryOnly loader kept out of the Store) get lazy-promoted
// the moment a depwait edge needs them, while render-only ids
// (no file record) stay absent until a parent render's
// emitRenderedChildren chain produces them.
type ExistenceLookup interface {
	// IsFileIndexed reports whether id has a file-existence record.
	IsFileIndexed(id manifest.NamedResource) bool
	// Promote attempts to materialize id into the Store from the
	// file-existence index. Returns true when promotion succeeded
	// and id is now reachable via store.GetObject.
	Promote(id manifest.NamedResource) bool
}

func (w *Waiter) readyNow(dep manifest.DependencyRef) bool {
	id := dep.NamedResource
	if !w.depExists(id) {
		return false
	}
	if !store.SupportsStatus(id.Kind) {
		return true
	}
	if dep.ReadyExpr != "" && !w.AdditiveReadyExpr {
		return w.readyExprSatisfied(dep.ReadyExpr, id)
	}
	info, ok := w.Store.GetStatus(id)
	if !ok || info.Status != store.StatusReady {
		return false
	}
	if dep.ReadyExpr != "" {
		return w.readyExprSatisfied(dep.ReadyExpr, id)
	}
	return true
}

// readyExprSatisfied reports whether expr produced a definitive Ready
// result for id right now (no waiting). A clean false, a transient eval
// error, or a compile failure all read as "not ready".
func (w *Waiter) readyExprSatisfied(expr string, id manifest.NamedResource) bool {
	ready, _ := w.tryReadyExpr(expr, id)
	return ready
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

// depExists reports whether a dep is known to the store via either an
// added object or a recorded status entry.
func (w *Waiter) depExists(dep manifest.NamedResource) bool {
	if w.Store.GetObject(dep) != nil {
		return true
	}
	_, ok := w.Store.GetStatus(dep)
	return ok
}
