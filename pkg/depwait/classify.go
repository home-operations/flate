package depwait

import (
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
)

// classKind is the non-blocking resolution of one dependency for the dag engine.
type classKind int

const (
	// classReady means the dependency is satisfied; the consumer may proceed.
	classReady classKind = iota
	// classFailed means the dependency is terminally unsatisfiable; Message
	// carries the canonical terminal reason for the failure.
	classFailed
	// classBlocked means the dependency is not yet satisfied but possibly
	// producible; the scheduler parks the consumer keyed on it.
	classBlocked
)

// classification describes one ordinary readiness decision.
type classification struct {
	Kind    classKind
	Message string // populated only for classFailed
}

func (w *Waiter) classify(dep manifest.DependencyRef, level manifest.DrainLevel, existence ExistenceLookup) classification {
	id := dep.NamedResource
	for {
		obj := w.Store.GetObject(id)
		present := obj != nil
		if !present {
			_, present = w.Store.GetStatus(id)
		}
		if obj != nil && !manifest.KindOf(obj).Status || obj == nil && present && !id.KindInfo().Status {
			return classification{Kind: classReady}
		}
		if dep.ReadyExpr != "" {
			ready, failMsg := w.tryReadyExpr(dep.ReadyExpr, id)
			if failMsg != "" {
				return classification{Kind: classFailed, Message: failMsg}
			}
			if present && ready {
				return classification{Kind: classReady}
			}
		}
		if !present {
			if existence != nil && existence.Promote(id) {
				existence = nil
				continue
			}
			if level < manifest.DrainCascade {
				return classification{Kind: classBlocked}
			}
			if w.AllowMissingCRDs && id.IsCRD() {
				return classification{Kind: classReady}
			}
			return classification{Kind: classFailed, Message: "dependency not found"}
		}
		info, _ := w.Store.GetStatus(id)
		if dep.ReadyExpr == "" && info.Status == store.StatusReady {
			return classification{Kind: classReady}
		}
		if info.Status == store.StatusFailed {
			return classification{Kind: classFailed, Message: info.Message}
		}
		if dep.ReadyExpr != "" && level >= manifest.DrainCascade {
			return classification{Kind: classFailed, Message: "readyExpr timeout: context deadline exceeded"}
		}
		if dep.ReadyExpr == "" && level >= manifest.DrainForce {
			return classification{Kind: classFailed, Message: "not ready"}
		}
		return classification{Kind: classBlocked}
	}
}
