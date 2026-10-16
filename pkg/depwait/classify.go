package depwait

import (
	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

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

// Classify resolves readiness gates without blocking; level escalates at the fixpoint.
// CEL compile errors precede missing-dependency, failed-status and drain classification.
// MUST run on a worker, never under the scheduler mutex.
func (w *Waiter) Classify(dep manifest.DependencyRef, level manifest.DrainLevel) Classification {
	id := dep.NamedResource

	state := w.readyNow(dep)
	if state == depReady {
		return Classification{Kind: ClassReady}
	}

	// Compile errors are permanent; missing projected attributes may become available.
	if dep.ReadyExpr != "" {
		if _, failMsg := w.tryReadyExpr(dep.ReadyExpr, id); failMsg != "" {
			return Classification{Kind: ClassFailed, Message: failMsg}
		}
	}

	if state == depAbsent {
		if w.Existence != nil && w.Existence.Promote(id) {
			state = w.readyNow(dep)
			if state == depReady {
				return Classification{Kind: ClassReady}
			}
		}
		if state == depAbsent {
			if level >= manifest.DrainCascade {
				if w.AllowMissingCRDs && id.Group == apix.GroupName && id.Kind == manifest.KindCustomResourceDefinition {
					return Classification{Kind: ClassReady}
				}
				return Classification{Kind: ClassFailed, Message: "dependency not found"}
			}
			return Classification{Kind: ClassBlocked}
		}
	}

	if info, ok := w.Store.GetStatus(id); ok && info.Status == store.StatusFailed {
		return Classification{Kind: ClassFailed, Message: info.Message}
	}

	if dep.ReadyExpr != "" {
		// CEL replaces built-in readiness; false expressions fail at the fixpoint.
		if level >= manifest.DrainCascade {
			return Classification{Kind: ClassFailed, Message: "readyExpr timeout: context deadline exceeded"}
		}
		return Classification{Kind: ClassBlocked}
	}

	// Pending status fails only at DrainForce, which breaks cross-kind cycles.
	if level >= manifest.DrainForce {
		return Classification{Kind: ClassFailed, Message: "not ready"}
	}
	return Classification{Kind: ClassBlocked}
}
