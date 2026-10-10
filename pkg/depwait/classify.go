package depwait

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// ClassKind is the non-blocking resolution of one dependency for the dag engine.
type ClassKind int

const (
	// ClassReady means the dependency is satisfied; the consumer may proceed.
	ClassReady ClassKind = iota
	// ClassFailed means the dependency is terminally unsatisfiable; Message
	// carries the canonical terminal reason for the failure.
	ClassFailed
	// ClassBlocked means the dependency is not yet satisfied but possibly
	// producible; the scheduler parks the consumer keyed on it.
	ClassBlocked
)

// Classification is the result of Classify.
type Classification struct {
	Kind    ClassKind
	Message string // populated only for ClassFailed
}

// Classify resolves dep for the dag engine WITHOUT blocking, reusing the shared
// readiness predicates (readyNow, depExists, the ReadyExpr evaluator,
// Existence.Promote). level escalates how an unsatisfied dependency is
// treated at the structural fixpoint and selects the canonical terminal
// message: notFound ("dependency not found"), the raw stored Failed message,
// "not ready" (a present-Pending dep at the force level), and the readyExpr
// strings.
//
// MUST run on a worker goroutine (it reads the store) — never under the
// scheduler's mutex.
func (w *Waiter) Classify(dep manifest.DependencyRef, level manifest.DrainLevel) Classification {
	id := dep.NamedResource

	// 1. Already satisfied: status Ready, a status-less kind that exists, or a
	//    satisfied ReadyExpr (replace or additive).
	if w.readyNow(dep) {
		return Classification{Kind: ClassReady}
	}

	// 2. A ReadyExpr that COMPILE-fails is permanently unsatisfiable regardless
	//    of dependency state or drain level. A transient eval error (missing
	//    attribute) returns no failMsg and falls through.
	if dep.ReadyExpr != "" {
		if _, failMsg := w.tryReadyExpr(dep.ReadyExpr, id); failMsg != "" {
			return Classification{Kind: ClassFailed, Message: failMsg}
		}
	}

	// 3. Absent: try a lazy promote from the file-existence index (the bjw-s
	//    same-path substituteFrom-CM pattern), else block — or fail
	//    "dependency not found" once draining, since an absent dep with no
	//    in-flight producer can never appear.
	if !w.depExists(id) {
		if w.Existence != nil && w.Existence.Promote(id) && w.readyNow(dep) {
			return Classification{Kind: ClassReady}
		}
		if !w.depExists(id) {
			if level >= manifest.DrainCascade {
				if w.AllowMissingCRDs && id.Kind == manifest.KindCustomResourceDefinition {
					return Classification{Kind: ClassReady}
				}
				return Classification{Kind: ClassFailed, Message: "dependency not found"}
			}
			return Classification{Kind: ClassBlocked}
		}
	}

	// 4. Present, status-less kind (ConfigMap/Secret): existence is readiness.
	if !store.SupportsStatus(id.Kind) {
		return Classification{Kind: ClassReady}
	}

	// 5. Present and Failed: cascade immediately with the raw stored message —
	//    Require wraps it into a DependencyFailedError.
	if info, ok := w.Store.GetStatus(id); ok && info.Status == store.StatusFailed {
		return Classification{Kind: ClassFailed, Message: info.Message}
	}

	// 6. Present with a ReadyExpr that readyNow rejected.
	if dep.ReadyExpr != "" {
		// Additive mode (Flux AdditiveCELDependencyCheck=true): the built-in
		// Ready check AND the CEL must both hold. When the built-in status is
		// already Ready, the CEL is the blocker, so evaluate it now and return
		// the byte-exact Flux result rather than blocking forever.
		if w.AdditiveReadyExpr {
			if info, ok := w.Store.GetStatus(id); ok && info.Status == store.StatusReady {
				okExpr, evalErr := evaluateReadyExpr(dep.ReadyExpr, w.Store, w.Parent, id)
				if evalErr != nil {
					return Classification{Kind: ClassFailed, Message: "readyExpr: " + evalErr.Error()}
				}
				if !okExpr {
					return Classification{Kind: ClassFailed, Message: "readyExpr returned false"}
				}
				return Classification{Kind: ClassReady}
			}
			// Built-in not yet Ready: treat like a plain present-Pending dep.
			if level >= manifest.DrainForce {
				return Classification{Kind: ClassFailed, Message: "not ready"}
			}
			return Classification{Kind: ClassBlocked}
		}
		// Replace mode (the default): the CEL IS the readiness check, so at the
		// fixpoint a never-true CEL fails with the canonical "readyExpr timeout".
		if level >= manifest.DrainCascade {
			return Classification{Kind: ClassFailed, Message: "readyExpr timeout: context deadline exceeded"}
		}
		return Classification{Kind: ClassBlocked}
	}

	// 7. Present and Pending: block. A parked producer will terminalize and the
	//    cascade carries its real message upward; only a cross-kind cycle
	//    reaches DrainForce, where it fails "not ready".
	if level >= manifest.DrainForce {
		return Classification{Kind: ClassFailed, Message: "not ready"}
	}
	return Classification{Kind: ClassBlocked}
}
